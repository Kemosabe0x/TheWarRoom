# Repository audit — October 11, 2026

Scope: all 11 originally tracked files at `fe7aeee`, including all rows and schemas in the five CSVs. The working tree was clean before this work. There were no repository `AGENTS.md` files, application tests, packaging metadata, or hidden application modules in the tracked inventory. Git history was sampled to establish the daily export workflow; this is not an audit of every historical commit.

## Current execution graph

```text
GitHub Actions daily 08:00 UTC / manual dispatch
  -> install requirements.txt (Python 3.10)
  -> execute update_data.py at module top level
  -> nflreadpy -> Polars -> pandas -> four CSV writes
  -> commit data/*.csv -> push

nfl_wrapper.py -> independent wrapper/example, not used by updater
wrap.py        -> broken duplicate, not used by updater
draft/server.py -> independent MCP mock, not connected to any real pipeline
```

08:00 UTC corresponds to 04:00 Eastern daylight time and 03:00 Eastern standard time. Scheduled workflows can be delayed; this is not a live feed. The original requirements do not declare either `fastmcp` or `mcp`.

## File and symbol migration map

| Existing file / exact symbols | Assessment | Destination / action |
| --- | --- | --- |
| `update_data.py:7-10`, `current_date`, `current_year`, `DATA_DIR` | Refactor: host-local date, March rollover, relative storage path | `war_room/settings.py` plus explicit worker `season` and `as_of`; inject season for replay, use library season helper only as a CLI default |
| `update_data.py:21-38`, `pbp`, `game_date`, `day_of_week`, `gruden_cols`, `gruden_df` | Reuse projection and date enrichment concept | `fantasy/transforms/personas.py`; preserve season, play_id and game_id, date type and nulls |
| `update_data.py:48-54`, `ngs` and season filter | Port with explicit stat category | `fantasy/nfl.py` and `workers/nfl_refresh.py`; passing is currently the default, add rushing/receiving datasets separately |
| `update_data.py:63-69`, `contracts`, `is_active` | Reuse active-contract view | `fantasy/transforms/contracts.py`; retain source units and IDs; normalize nested histories into child records |
| `update_data.py:78-93`, `player_stats`, `fantasy_cols`, `existing_cols`, `fantasy_df` | Refactor schema handling | Required-column validation with explicit aliases; never silently drop identity/season/team fields |
| `update_data.py:41,56,71,95`, broad exception handlers | Replace | Job results per dataset, nonzero required-stage failure, preserved previous good snapshot, alert on lag |
| `nfl_wrapper.py:24`, `cache_dir`, `use_cache` | Keep settings intent; replace mechanism | Configure nflreadpy once per worker process, not per call |
| `nfl_wrapper.py:41-79`, `load_play_by_play`, `load_player_stats`, `load_team_stats`, `load_schedules`, `load_players`, `load_rosters`, `load_rosters_weekly`, `load_snap_counts` | Keep loader coverage; rewrite thin calls | `fantasy/nfl.py`; bounded explicit seasons, supported function signatures, no arbitrary kwargs |
| `nfl_wrapper.py:81-99`, `load_nextgen_stats`, `load_ftn_charting`, `load_participation` | Port after coverage and availability checks | Separate NGS categories, charting/participation feature jobs; record source licensing/availability |
| `nfl_wrapper.py:101-144`, `load_injuries`, `load_contracts`, `load_officials`, `load_combine`, `load_depth_charts`, `load_trades`, `load_draft_picks`, `load_ff_playerids`, `load_ff_rankings`, `load_ff_opportunity` | Keep domain intent; refactor calls and output contracts | Reference/dimension jobs, identity bridge, availability service, baseline comparison features; isolate missing upstream sources |
| `nfl_wrapper.py:147-158`, `clear_cache`, season/week helpers | Port selectively | Admin worker commands; avoid granting agents an unrestricted global cache purge |
| `nfl_wrapper.py:161-190`, `fantasy_rules`, `calculate_fantasy_points` | Reuse coefficients only | `fantasy/scoring.py`; versioned scoring applied to normalized player-game stats, including receptions and all configured penalties |
| `nfl_wrapper.py:192-209`, `aggregate_player_stats` | Full refactor | Attribute PBP events to passer/rusher/receiver IDs first, aggregate at defined grain, join dimensions with cardinality checks |
| `nfl_wrapper.py:213-228`, example | Replace | Small fixture-backed demonstration; current PBP assumptions do not hold |
| `wrap.py` | Retire after migration | Duplicates wrapper but imports only `nflreadpy as nfl`; `pl` and unqualified loader functions are undefined |
| `draft/server.py` | Retain as historical draft; replace runtime | New root `server.py` and domain services; none of the mock outputs are portable business logic |
| `draft/dir-structure.md` | Expand | Blueprint adds shared contracts, storage, workers, execution accounting, tests and operations |
| `.github/workflows/schedule.yml` | Refactor after new exporter parity | CI validates code; runtime workers collect data. Move mutable data out of Git, preserve compatibility exports during cutover |
| `requirements.txt` | Split environments | New `pyproject.toml`; legacy pandas/pyarrow/fastparquet remain only until exporter migration |

The repeated wrappers pass `cache=` and `cache_dir=` to loaders that do not accept them. The pinned `nflreadpy==0.1.5` signatures were inspected locally: player/team stats take `summary_level`, not `stat_type`; `clear_cache` takes an optional pattern, not a cache directory. `wrap.py` was imported and failed with `NameError: name 'pl' is not defined`. `nfl_wrapper.py` imports successfully but its loader arguments are incompatible. [nflreadpy loading API](https://nflreadpy.nflverse.com/api/load_functions/), [configuration](https://nflreadpy.nflverse.com/api/configuration/).

The fantasy formula uses player-summary names while claiming to accept raw PBP. PBP has role-specific player identities, and the generic `player_id`/`player_name` group-by is not a valid universal play grain. Null arithmetic can nullify an entire score, receptions are omitted from PPR, and all fumbles cannot be indiscriminately assigned to every role. Build against the upstream player-stat dictionary and golden scoring fixtures.

## Data inventory

These counts were computed with a CSV parser, not physical line counts; GM contract cells contain embedded newlines. The machine-readable inventory in `data-audit.json` includes SHA-256 digests, every column, week coverage and date coverage. Reproduce with `python scripts/audit_data.py`.

| File | Rows | Bytes | Observed coverage | Key issue |
| --- | ---: | ---: | --- | --- |
| `agent_analytics_kb.csv` | 35,482 | 3,123,181 | 2025 game IDs; weeks 1–15 | Never written by current updater; missing play_id, explicit season and dates |
| `agent_gruden_kb.csv` | 11,327 | 1,704,928 | 2026 game IDs; weeks 1–5; dates Sept 9–Oct 8 | No play_id; cannot reliably join to analytics at play grain |
| `agent_scout_kb.csv` | 170 | 53,589 | Season 2026; REG; weeks 0–5 | Passing only; week 0 aggregates must be separated from weekly observations |
| `agent_fantasy_kb.csv` | 4,517 | 215,762 | Weeks 1–5; season cannot be proven from file | No stable player ID, season, game ID, season type or team; 3 duplicated name/week keys |
| `agent_gm_kb.csv` | 2,510 | 7,687,340 | Active-contract snapshot; source as-of absent | Has GSIS/OTC IDs; histories serialized as Python-like text, not reliable JSON |

All five lack trustworthy ingestion manifests. Filesystem modification time and Git commit time are not upstream event time. Do not label any of these CSVs live, infer the fantasy season from another file, or join coach and analytics by row order. Contract values need source-unit metadata before comparison; nested histories must be refetched/normalized, never evaluated as Python code.

## Failure paths to remove

1. Importing `update_data.py` starts downloads/writes. Move work under a CLI entry point and injectable job function.
2. Partial writes can mix different refresh generations. Publish immutable files plus a transactional manifest pointer.
3. Exceptions are printed, then `Update Complete` is printed and the process can exit successfully. Existing CSVs can satisfy the workflow's existence check even when every refresh failed.
4. `existing_cols` hides upstream column drift. `recent_team` is already absent from the committed fantasy file.
5. The workflow's `git commit ... || echo ...` conflates no changes with unrelated commit failures. Add required-stage exit status and concurrency control before the eventual runtime-worker cutover.
6. Draft MCP prices, EV, projections, processed-row counts and news are invented constants; `EXECUTE` is emitted without an execution or risk model. Root `server.py` never returns those placeholders.
7. No current persistence schema, entity resolution, live collectors, predictive training, backtests, order ledger, authentication or deployment manifests exist. Those are new modules, not porting tasks.

## What was implemented in this pass

An additive bridge preserves the original artifacts while making them queryable through typed MCP tools/resources. `server.py` owns registration and lifespan; `war_room/contracts.py` owns shared Pydantic schemas; `storage.py` handles bounded legacy reads; `fantasy/nfl.py` contains corrected worker adapter calls; `fantasy/sleeper.py` is a real optional public read adapter; betting arithmetic and content evidence are separate services.

This is an initial implementation, not the completed overhaul. Live order execution is deliberately unimplemented, and unavailable data is reported as unavailable. See the roadmap for the production gates and precise next files.
