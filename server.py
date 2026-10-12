"""War Room MCP composition root. Run: python server.py (local stdio only).

All transports call domain services. No import-time downloads, orders, or publishing.
Remote HTTP deployment needs the authentication/authorization layer in docs/BLUEPRINT.md.
"""
import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated

import httpx
from fastmcp import Context, FastMCP
from fastmcp.exceptions import ToolError
from pydantic import Field

from war_room.betting.service import BettingService, evaluate_combo
from war_room.content.service import build_brief
from war_room.contracts import (
    ComboEvaluation, ComboExecution, ComboScenario, ContentBrief, ContentRequest,
    Dataset, DatasetInfo, DatasetPage, DatasetQuery, ExecutionReceipt, Identifier,
    MarketQuery, MarketQuote, NewsItem, OracleObservation, Projection, Snapshot, Sport, Venue,
)
from war_room.fantasy.sleeper import League, SleeperAdapter
from war_room.storage import DataUnavailable, LegacySnapshots

READ = {"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True}


def create_server(data_dir: Path | None = None) -> FastMCP:
    snapshots = LegacySnapshots(data_dir or Path(__file__).resolve().parent / "data")
    betting = BettingService()

    @asynccontextmanager
    async def lifespan(app: FastMCP):
        async with httpx.AsyncClient(timeout=httpx.Timeout(10),
            limits=httpx.Limits(max_connections=10, max_keepalive_connections=5),
            follow_redirects=False) as client:
            yield {"sleeper": SleeperAdapter(client)}

    mcp = FastMCP("The War Room", lifespan=lifespan,
        instructions="Read status and provenance. Legacy snapshots are not live data. "
                     "Combo evaluations use caller assumptions. Execution is unavailable.")

    async def query_snapshot(query: DatasetQuery) -> Snapshot[DatasetPage]:
        try:
            return await asyncio.to_thread(snapshots.query, query)
        except DataUnavailable as exc:
            raise ToolError(str(exc)) from exc

    @mcp.tool(annotations=READ, tags={"fantasy"})
    async def query_nfl_snapshot(query: DatasetQuery) -> Snapshot[DatasetPage]:
        """Read a bounded legacy NFL page; pin snapshot_id when paginating."""
        return await query_snapshot(query)

    @mcp.tool(annotations=READ, tags={"fantasy"})
    async def get_sleeper_league(
        league_id: Annotated[str, Field(pattern=r"^[0-9]{1,30}$")], ctx: Context,
    ) -> Snapshot[League]:
        """Fetch public NFL league/scoring settings, cached for 60 seconds."""
        try:
            return await ctx.lifespan_context["sleeper"].league(league_id)
        except DataUnavailable as exc:
            raise ToolError(str(exc)) from exc

    @mcp.tool(annotations=READ, tags={"betting"})
    def get_live_markets(query: MarketQuery) -> Snapshot[list[MarketQuote]]:
        """Live market contract; reports unavailable until venue collectors exist."""
        return betting.markets(query)

    @mcp.tool(annotations=READ, tags={"betting", "analysis"})
    def evaluate_combo_bet(scenario: ComboScenario) -> ComboEvaluation:
        """Calculate hypothetical all-legs-win EV using an explicit joint probability."""
        return evaluate_combo(scenario)

    @mcp.tool(annotations={"readOnlyHint": False, "destructiveHint": True,
                           "idempotentHint": False, "openWorldHint": True}, tags={"betting", "execution"})
    def execute_combo_bet(request: ComboExecution) -> ExecutionReceipt:
        """Reserved action schema. Always fails closed; no venue orders are submitted."""
        raise ToolError("EXECUTION_UNAVAILABLE: requires a persisted approved plan, risk reservation, "
                        "venue adapter, idempotency ledger and reconciliation worker.")

    @mcp.tool(annotations=READ, tags={"content"})
    async def prepare_content_brief(request: ContentRequest) -> ContentBrief:
        """Return evidence and writing constraints for an external agent; does not publish."""
        try:
            return await asyncio.to_thread(build_brief, request, snapshots)
        except DataUnavailable as exc:
            raise ToolError(str(exc)) from exc

    @mcp.resource("warroom://status", mime_type="application/json")
    def status() -> dict:
        """Implementation capabilities, not a live provider health probe."""
        return {"schema_version": "1.0", "mode": "development_scaffold", "transport": "stdio",
                "legacy_csv": "implemented_unverified_freshness", "sleeper_league": "implemented_on_demand",
                "kalshi": "unavailable", "polymarket": "unavailable", "nhl": "unavailable",
                "projections": "unavailable", "news": "unavailable", "execution": "disabled"}

    @mcp.resource("fantasy://datasets", mime_type="application/json")
    async def dataset_catalog() -> str:
        """List legacy row counts, columns, observed seasons and content digests."""
        try:
            catalog = await asyncio.to_thread(snapshots.catalog)
            return Snapshot[list[DatasetInfo]](status="legacy_unverified", source="repository:data",
                data=catalog, warnings=["Observed season evidence is absent in some exports."]).model_dump_json()
        except DataUnavailable as exc:
            return Snapshot(status="unavailable", source="repository:data", warnings=[str(exc)]).model_dump_json()

    @mcp.resource("fantasy://stats/nfl/{dataset}/{week}", mime_type="application/json")
    async def nfl_stats(dataset: Dataset, week: int) -> str:
        """Read up to 25 rows of a legacy weekly snapshot; use the tool to paginate."""
        return (await query_snapshot(DatasetQuery(dataset=dataset, week=week))).model_dump_json()

    @mcp.resource("fantasy://projections/{sport}/{player_id}", mime_type="application/json")
    def player_projections(sport: Sport, player_id: Identifier) -> str:
        """Projection boundary: no trained, validated model is wired yet."""
        return Snapshot[Projection](status="unavailable", source=f"model:{sport.value}:{player_id}",
            warnings=["No projection model or point-in-time feature store is configured."]).model_dump_json()

    @mcp.resource("betting://markets/{venue}/{asset}", mime_type="application/json")
    def market_snapshot(venue: Venue, asset: str) -> str:
        """Readable market snapshot boundary, not a WebSocket stream."""
        return betting.markets(MarketQuery(venue=venue, asset=asset)).model_dump_json()

    @mcp.resource("betting://oracle/{venue}/{market_id}", mime_type="application/json")
    def oracle_snapshot(venue: Venue, market_id: Identifier) -> str:
        """Oracle observation boundary; arrival lag alone is not a price advantage."""
        return Snapshot[OracleObservation](status="unavailable", source=f"{venue.value}:{market_id}",
            warnings=["No settlement-rule-linked oracle collector is configured."]).model_dump_json()

    @mcp.resource("news://latest", mime_type="application/json")
    def news() -> str:
        """News snapshot boundary; missing ingestion is explicit."""
        return Snapshot[list[NewsItem]](status="unavailable", source="news",
            warnings=["No licensed news feed or ingestion worker is configured."]).model_dump_json()

    return mcp


mcp = create_server()

if __name__ == "__main__":
    mcp.run(transport="stdio")
