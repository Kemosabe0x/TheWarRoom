import asyncio
import json
from decimal import Decimal
from pathlib import Path

import httpx
import pytest
from fastmcp import Client
from pydantic import ValidationError

from server import create_server
from war_room.betting.service import evaluate_combo
from war_room.contracts import ComboScenario, Dataset, DatasetQuery
from war_room.fantasy.sleeper import SleeperAdapter
from war_room.storage import DataUnavailable, LegacySnapshots


@pytest.fixture
def data_dir(tmp_path):
    for dataset in Dataset:
        (tmp_path / f"agent_{dataset.value}_kb.csv").write_text(
            "player_display_name,week,season,fantasy_points\nTest Player,1,2026,10.5\nOther Player,2,2026,8.0\n",
            encoding="utf-8")
    return tmp_path


def scenario(**overrides):
    return ComboScenario.model_validate({
        "legs": [{"venue": "kalshi", "market_id": f"m{i}", "outcome_id": "yes"} for i in [1, 2]],
        "joint_probability": "0.4", "probability_basis": "Caller hypothetical joint distribution",
        "total_cost": "30", "total_payout_if_all_win": "100", "total_fees": "2", **overrides})


def test_ev_uses_joint_probability_and_includes_fees():
    result = evaluate_combo(scenario())
    assert result.expected_profit == Decimal("8")
    assert result.maximum_loss == Decimal("32")
    assert result.return_on_cost == Decimal("0.25")


@pytest.mark.parametrize("overrides", [
    {"joint_probability": "1.01"}, {"joint_probability": "NaN"},
    {"total_cost": "0"}, {"total_fees": "-1"},
    {"legs": [{"venue": "kalshi", "market_id": "a", "outcome_id": "yes"}] * 2},
    {"invented": 1},
])
def test_invalid_scenarios_rejected(overrides):
    with pytest.raises(ValidationError):
        scenario(**overrides)


def test_snapshots_provenance_filter_and_change_detection(data_dir):
    store = LegacySnapshots(data_dir)
    page = store.query(DatasetQuery(dataset="fantasy", limit=1))
    assert page.status == "legacy_unverified" and page.source_updated_at is None
    assert page.data.next_offset == 1
    assert store.query(DatasetQuery(dataset="fantasy", week=2)).data.rows[0]["fantasy_points"] == "8.0"
    (data_dir / "agent_fantasy_kb.csv").write_text("week\n3\n", encoding="utf-8")
    with pytest.raises(DataUnavailable, match="Snapshot changed"):
        store.query(DatasetQuery(dataset="fantasy", expected_snapshot_id=page.snapshot_id))


def test_dataset_path_and_page_bounds():
    for params in [{"dataset": "../secret"}, {"dataset": "fantasy", "limit": 101},
                   {"dataset": "fantasy", "offset": -1}]:
        with pytest.raises(ValidationError):
            DatasetQuery(**params)


def test_mcp_protocol(data_dir):
    async def run():
        async with Client(create_server(data_dir)) as client:
            tool_names = {t.name for t in await client.list_tools()}
            assert tool_names == {"query_nfl_snapshot", "get_sleeper_league", "get_live_markets",
                                  "evaluate_combo_bet", "execute_combo_bet", "prepare_content_brief"}
            result = await client.call_tool("query_nfl_snapshot", {"query": {"dataset": "fantasy", "week": 1}})
            assert result.structured_content["status"] == "legacy_unverified"
            assert len(result.structured_content["data"]["rows"]) == 1
            market = await client.call_tool("get_live_markets", {"query": {"venue": "kalshi"}})
            assert market.structured_content["status"] == "unavailable"
            assert market.structured_content["data"] is None
            brief = await client.call_tool("prepare_content_brief", {"request": {"topic": "NFL Week 1"}})
            assert brief.structured_content["publication_allowed"] is False
            ev = await client.call_tool("evaluate_combo_bet", {"scenario": scenario().model_dump(mode="json")})
            assert Decimal(ev.structured_content["expected_profit"]) == 8
            for uri in ["warroom://status", "fantasy://datasets", "fantasy://stats/nfl/fantasy/1",
                        "fantasy://projections/nhl/player1", "betting://markets/polymarket/BTC",
                        "betting://oracle/kalshi/market1", "news://latest"]:
                contents = await client.read_resource(uri)
                payload = json.loads(contents[0].text)
                assert isinstance(payload, dict)
            execution = await client.call_tool("execute_combo_bet", {"request": {
                "plan_id": "plan1", "idempotency_key": "key1", "max_total_cost": "10",
                "expires_at": "2099-01-01T00:00:00Z"}}, raise_on_error=False)
            assert execution.is_error
            assert "EXECUTION_UNAVAILABLE" in execution.content[0].text
            invalid = await client.call_tool("get_live_markets", {"query": {"venue": "invented"}}, raise_on_error=False)
            assert invalid.is_error
    asyncio.run(run())


def test_sleeper_cached_and_validated():
    async def run():
        calls = []
        def respond(request):
            calls.append(request)
            return httpx.Response(200, json={"league_id": "123", "name": "League", "sport": "nfl",
                "season": "2026", "status": "in_season", "scoring_settings": {"rec": 1}, "unneeded": "drop"})
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            adapter = SleeperAdapter(client)
            first = await adapter.league("123")
            second = await adapter.league("123")
            assert len(calls) == 1 and first.retrieved_at == second.retrieved_at
            assert second.data.scoring_settings["rec"] == 1
            with pytest.raises(DataUnavailable):
                await adapter.league("999")
    asyncio.run(run())


@pytest.mark.parametrize("status,payload", [(429, {}), (500, {}), (200, None), (200, {"league_id": "123"})])
def test_sleeper_failure_is_explicit(status, payload):
    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(
            lambda request: httpx.Response(status, json=payload))) as client:
            with pytest.raises(DataUnavailable):
                await SleeperAdapter(client).league("123")
    asyncio.run(run())


def test_repository_data_smoke():
    store = LegacySnapshots(Path(__file__).resolve().parents[1] / "data")
    catalog = store.catalog()
    assert {item.dataset for item in catalog} == set(Dataset)
    for item in catalog:
        assert item.rows > 0
        assert len(store.query(DatasetQuery(dataset=item.dataset, limit=2)).data.rows) == 2


def test_sleeper_tool_uses_lifespan_client(data_dir, monkeypatch):
    async def fake_league(self, league_id):
        assert isinstance(self.client, httpx.AsyncClient)
        assert not self.client.is_closed
        from war_room.contracts import Snapshot
        from war_room.fantasy.sleeper import League
        return Snapshot[League](status="observed", source="fixture:sleeper", data=League(
            league_id=league_id, name="Test", sport="nfl", season="2026", status="in_season"))
    monkeypatch.setattr(SleeperAdapter, "league", fake_league)
    async def run():
        async with Client(create_server(data_dir)) as client:
            result = await client.call_tool("get_sleeper_league", {"league_id": "123"})
            assert result.structured_content["data"]["league_id"] == "123"
    asyncio.run(run())


def test_stdio_from_different_working_directory(tmp_path):
    import sys
    from fastmcp.client.transports import StdioTransport
    async def run():
        transport = StdioTransport(command=sys.executable,
            args=[str(Path(__file__).resolve().parents[1] / "server.py")], cwd=str(tmp_path))
        async with Client(transport) as client:
            response = await client.call_tool("query_nfl_snapshot", {"query": {"dataset": "fantasy", "limit": 1}})
            assert response.structured_content["status"] == "legacy_unverified"
            assert len(response.structured_content["data"]["rows"]) == 1
    asyncio.run(run())
