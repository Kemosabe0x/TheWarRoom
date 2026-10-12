# The War Room MCP overhaul

This repository now includes a runnable **development scaffold** and a production architecture/migration plan. The original exporter, wrappers, draft and CSV files remain available for comparison. The new entry point is **`server.py`**, not `draft/server.py`.

Start with [the repository audit](docs/REPOSITORY_AUDIT.md), then [the architecture blueprint](docs/BLUEPRINT.md) and [the ordered implementation roadmap](docs/ROADMAP.md).

The [validation record](docs/VALIDATION.md) states what passed and what remains unverified. [Exported MCP schemas](docs/mcp-contract.json) contain the exact advertised input/output contracts for client integration.

## Run locally

Python 3.11+ is required; development verification used Python 3.13.14 on Windows.

```powershell
python -m venv .venv
.\.venv\Scripts\python -m pip install -e ".[nfl,test]"
.\.venv\Scripts\python -m pytest -q
.\.venv\Scripts\python server.py
```

The last command starts a stdio MCP server and waits for an MCP client. Configure a client to launch the absolute path of `.venv/Scripts/python.exe` with the absolute path of `server.py` as its argument. CSV paths resolve relative to `server.py`, not the client's working directory. Startup does not download data.

`pyproject.toml` is the new scaffold dependency definition. `requirements.txt` continues to describe the legacy daily job; do not combine the two environments. `requirements-scaffold-win-py313.lock.txt` records the exact tested Windows environment, including platform-specific packages, and is not a portable Linux lock. Generate a platform-specific hash-locked production environment in CI before deployment.

## What works now

| Interface | Behavior |
| --- | --- |
| `query_nfl_snapshot` | Bounded, filtered pages from real legacy CSVs, with SHA-256 snapshot IDs |
| `get_sleeper_league` | On-demand public NFL league/scoring read, timeout, bounded 60-second cache |
| `evaluate_combo_bet` | Decimal EV arithmetic for caller-supplied all-legs-win assumptions |
| `prepare_content_brief` | Persona data sample with provenance and writing constraints |
| `fantasy://datasets` | Real schema, row-count and season-evidence catalog |
| `fantasy://stats/nfl/{dataset}/{week}` | Bounded weekly legacy data resource |
| `warroom://status` | Explicit implementation capability report |

Market, projection and news interfaces return `unavailable` until their providers are built. `execute_combo_bet` exposes the action contract but always returns an MCP error. There is no live execution, trained predictive model, news collector, publishing integration, NHL adapter, or worker scheduler in this scaffold. The Sleeper adapter was verified with HTTP fixtures, not a user league.

Legacy observations are labeled `legacy_unverified`; reading a file today does not establish its source freshness. The architecture makes that distinction mandatory for future real-time feeds.

## Example tool arguments

```json
{
  "query": {
    "dataset": "fantasy",
    "week": 5,
    "limit": 25
  }
}
```

Pass the returned `snapshot_id` as `expected_snapshot_id` on subsequent pages. The legacy bridge deliberately preserves CSV values as text and truncates long cells to 1,000 characters; normalized typed statistics belong in the next migration phase.

```json
{
  "scenario": {
    "legs": [
      {"venue": "kalshi", "market_id": "example-a", "outcome_id": "yes"},
      {"venue": "kalshi", "market_id": "example-b", "outcome_id": "yes"}
    ],
    "joint_probability": "0.40",
    "probability_basis": "Hypothetical caller-supplied joint distribution",
    "total_cost": "30",
    "total_payout_if_all_win": "100",
    "total_fees": "2",
    "currency": "USD"
  }
}
```

This returns hypothetical expected profit of 8 and maximum loss of 32. It does not discover a tradable combo or recommend an order.
