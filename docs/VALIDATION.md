# Validation record — October 11, 2026

Environment: Windows, Python 3.13.14, FastMCP 3.4.8, nflreadpy 0.1.5, Polars 1.36.1. Dependencies were installed in the repository-local ignored `.venv`; the global Python environment was not modified.

## Passed

- `python -m pytest -q`: **18 passed**. Tests cover protocol discovery, structured tool results, all seven resource/resource-template routes, unavailable providers, disabled execution, EV/fee arithmetic, invalid probabilities/amounts/duplicate legs, snapshot filters and pagination version conflicts, path/page constraints, actual repository CSV reads, fixture-based Sleeper responses/errors/caching, and lifespan-managed client access.
- A subprocess stdio MCP client launched `server.py` from a different working directory and successfully queried the repository data. This checks actual transport startup and path independence, beyond in-process function calls.
- `python -m pip check`: **No broken requirements found.**
- `scripts/audit_data.py`: parsed all five CSVs and emitted `docs/data-audit.json`.
- `scripts/export_mcp_contract.py`: connected through MCP and emitted actual advertised schemas to `docs/mcp-contract.json` (six tools, three concrete resources, four resource templates).
- Verified installed nflreadpy signatures without downloading datasets; confirmed the old wrapper incompatibilities and `wrap.py` import failure.

`requirements-scaffold-win-py313.lock.txt` is a tested-environment freeze, not a hash-verified or cross-platform deployment lock. Use `pyproject.toml` for editable installation; use the freeze to reproduce this Windows/Python environment, then install the local project with `--no-deps` if exact replay is desired.

## Not performed

No live Sleeper league request, full nflreadpy download, NHL API contract test, Kalshi/Polymarket account connection, live order, publishing action, remote HTTP deployment, trained model evaluation, database migration, load test or collector soak test was performed. These components are either absent or explicitly deferred to the roadmap.

The existing CSV files, legacy scripts, old draft and scheduled workflow were preserved. This validation establishes that the new local scaffold is runnable and its advertised initial behavior is tested; it does not certify production data freshness or trading readiness.
