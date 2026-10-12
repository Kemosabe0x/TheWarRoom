# Ordered execution roadmap

Review order: repository audit, blueprint, root `server.py`, shared schemas, then this checklist. The architecture is additive so the current daily exports continue until a replacement proves parity. No existing script or CSV was overwritten in the initial pass.

## 0. Establish the baseline — delivered

- Inspected every originally tracked source/config file and all five CSV schemas/rows.
- Added root FastMCP factory, domain services, typed requests/results and explicit unavailable-provider contracts.
- Added the bounded legacy bridge, hypothetical combo arithmetic, content evidence packets, a public Sleeper league adapter and corrected worker-only NFL loader calls.
- Added installation/run instructions, a reproducible CSV audit and MCP/validation/provider-failure tests.

This is the initial draft implementation, not a deployed live data or trading platform. Root `server.py` is the intended entry point; the original draft remains illustrative.

## 1. Repair the NFL export foundation

**Create:** `war_room/settings.py`, `war_room/workers/nfl_refresh.py`, `war_room/fantasy/transforms/personas.py`, `war_room/fantasy/transforms/contracts.py`, `tests/test_nfl_refresh.py`.

**Port next, in order:**

1. Move `update_data.py` configuration and top-level work into an explicit `refresh(season, datasets)` job. Make its CLI return nonzero when required datasets fail. Dependency-inject loaders for tests.
2. Use the new `fantasy/nfl.py` methods; expand with schedules, teams, players, IDs, rosters and contracts from `nfl_wrapper.py`. Remove unsupported `cache/cache_dir` kwargs and switch player/team calls to `summary_level`.
3. Port `gruden_cols` and `game_date`/`day_of_week` enrichment. Preserve `season`, `season_type`, `game_id` and `play_id` before persona projection.
4. Restore an analytics export using the current analytics CSV's `epa`, `wpa`, `cpoe`, `success` columns plus play identifiers. This repairs the 2025/2026 split.
5. Port `fantasy_cols`, replacing `existing_cols` with required/optional schema sets and a documented `recent_team` alias decision. Retain `player_id`, season, season type, game and team. Reject ambiguous duplicates.
6. Expand NGS into three named categories; separate week 0 summaries. Port active-contract filtering with explicit units and properly structured nested histories.
7. Add staging files and immutable manifest publication. Publish all required persona views as one generation. Preserve the previous generation if a required stage fails; optional datasets may be marked partial with their own source times.

**Acceptance:** fixture data reproduces existing useful columns; all five views agree on declared coverage; schema drift and fetch failure do not replace good data; no import-time network; season replay works; analytics is actually refreshed.

**Then modify:** `update_data.py` into a thin compatibility CLI. Adjust `.github/workflows/schedule.yml` to call it, honor failures and prevent overlapping refreshes. Keep CSV compatibility until downstream agents migrate; remove Git-based data delivery only after storage cutover. Retire `wrap.py` and the old wrapper only after callers and tests migrate.

## 2. Introduce durable snapshots and identities

**Create:** `repositories/protocols.py`, `repositories/postgres.py`, `repositories/objects.py`, `common/identities.py`, `common/provenance.py`, and SQL migrations for manifests, dataset heads, IDs, observations, jobs and outbox.

Load the audited CSVs as explicitly unverified legacy snapshots. Do not fill missing fantasy IDs/season from guesses. Backfill normalized player/game data directly from the provider with explicit season. Store raw immutable objects, validated normalized partitions and transactional head pointers.

Join `load_ff_playerids()` to GSIS/Sleeper identities with cardinality checks and a quarantine report. Track valid-time team changes. Replace the in-process CSV repository behind the existing query interface, then add typed player/event queries and stable opaque cursors tied to snapshot IDs.

**Acceptance:** concurrent readers see one generation; duplicate ingestion is idempotent; backup/restore reproduces hashes; unresolved identities cannot enter predictive joins; no queries scan entire seasons in an MCP request.

## 3. Add Sleeper context and scoring

**Expand:** `fantasy/sleeper.py`; create `workers/sleeper_refresh.py`, `fantasy/scoring.py`, scoring fixtures and provider contract tests.

Retain the implemented `/v1/league/{league_id}` read, then add documented rosters, weekly matchups and transactions. Schedule player dictionary refresh at most daily and reuse it centrally. Implement bounded retry with jitter for retryable failures, Retry-After handling, per-provider quotas, shared caching and circuit breaking. The current small read adapter has a timeout/cache and reports failures; it is not that full collector.

Port the original scoring coefficients into a versioned rule set, then apply league `scoring_settings`, including receptions, penalties and supported bonuses. Preserve raw scores and recomputed scores separately. Validate provider-owned IDs before joins.

**Acceptance:** one real target league (supplied at integration time) matches known scoring totals; 429/timeout/schema changes fail clearly; player fetch frequency remains within source guidance; stale settings are visible.

## 4. Build the NHL provider independently

**Create:** `fantasy/nhl.py`, `workers/nhl_live.py`, NHL schemas/fixtures and ID mapping tests.

First verify the provider, endpoint schemas, use permissions and required latency. Capture versioned schedule/boxscore/PBP/roster samples; define game status, corrections and NHL season IDs. Wire adaptive polling or provider streaming around game state, and reconcile postgame corrections. Implement goalie/line availability only from a verified feed.

**Acceptance:** complete-game replay matches final totals, postponements/overtime/shootouts are handled, time-on-ice units are explicit, player ID mappings are stable, freshness is measurable. Unsupported next-gen fields remain unavailable.

## 5. Add Kalshi and Polymarket read collectors

**Create:** `betting/adapters/kalshi.py`, `betting/adapters/polymarket.py`, `betting/discovery.py`, `betting/orderbooks.py`, `workers/market_streams.py`, `repositories/redis.py`.

Capture live catalog payloads using appropriate accounts/API access; discover actual 15-minute contracts and retain rules/currencies. Normalize IDs, precision, timestamps and settlement definitions. Build the snapshot/delta state machine with recorded replay fixtures before wiring the current `get_live_markets` and market resource to it.

Use API-version-specific authentication and signing libraries after contract verification. Do not carry an older Polymarket RTDS or SDK example forward without checking the current migration guide. Source endpoints/fees/ticks belong in provider adapters and persisted metadata.

**Acceptance:** reconnects, sequence gaps, duplicate/out-of-order frames, market rollover and API 429s recover; invalid books are not tradable; subscription ownership survives worker restart; quotes include depth/age/fee provenance. Production clocks and transport latency are measured.

## 6. Build projections and oracle pricing

**Create:** `fantasy/features.py`, `fantasy/projections.py`, `fantasy/models/`, `workers/projections.py`, `betting/oracle.py`, `betting/pricing.py` and replay/calibration tests.

For sports, build point-in-time feature tables from the normalized stats/availability/schedule data. Train a baseline and compare held-out chronological metrics before promoting artifacts. Expose explicit target, event, horizon, scoring version and uncertainty.

For oracle pricing, map actual market rules to reference feeds; capture source/arrival/clock observations. Replay decision-time information and execution delay. Derive calibrated settlement probabilities and executable-size EV after fees/slippage, with a no-quote state when input quality is insufficient.

**Acceptance:** no future-data leakage; models beat stated baselines on adequate held-out samples or remain unpromoted; uncertainty and drift reported; source-specific oracle lag is measured; latency alone never yields an automatic trade recommendation.

## 7. Implement combo planning and paper execution

**Create:** `betting/combos.py`, `betting/risk.py`, `betting/execution.py`, `workers/reconcile_orders.py`, order/position/reservation migrations, and execution fault tests.

Build native combo discovery/quote plans separately from baskets and cross-venue hedges. Persist exact plan content, joint probability provenance, quote IDs, expiry, principal/account binding and limits. Add `price_combo` only when it can return a server-owned quote/plan. Introduce transactional reservations, idempotent command outbox and per-venue order IDs.

Exercise the execution state machine in paper mode and a supported venue demo environment. Simulate lost acknowledgements, duplicate calls, partial fills, cancellation races, filled-before-cancel, worker crashes and restart. Reconcile balances/positions/orders independently from the client request.

**Acceptance:** repeated requests cannot duplicate orders; conflicting keys fail; reserved+filled exposure remains within policy under concurrency; ambiguous submit is reconciled before retry; cross-venue partial fills have an explicit bounded-risk policy.

Only then replace the always-disabled `execute_combo_bet` implementation and expose it to an authorized trading role. Keep the server's current fail-closed behavior until these gates pass.

## 8. Build news, content relevance and triggers

**Create:** `content/news.py`, `content/briefs.py`, `content/triggers.py`, `workers/outbox.py` and news/event tables.

Select permitted news sources; ingest canonical URL, publication time, revisions and entity links. Replace persona sampling with topic/entity/time-window queries against coherent stats/projection/market manifests. Return citations and explicit evidence gaps. Emit deduplicated change events after commits; support bounded cursor polling first, then signed webhook dispatch if needed by external workflows.

Keep prose generation in external agents. Add a publishing adapter only when destinations and publication policy are configured. Its command ledger must prevent duplicate posts and support source corrections.

**Acceptance:** correcting a story updates its evidence lineage; stale facts cannot be labeled breaking; injected source instructions stay data; duplicate events do not duplicate publications; each generated claim can be traced to a snapshot/source/model.

## 9. Harden and cut over

**Create:** authenticated HTTP deployment, environment-specific lockfiles/images, schema migrations, service roles, dashboards and backup/restore runbooks.

Add account-scoped authorization before any remote exposure, worker secrets isolation, transport protection and quotas. Use migrations with backward-compatible schema changes. Run CI on supported Python/OS targets, MCP protocol tests, load tests and collector soak/replay tests. Set measured freshness/error objectives per provider.

Migrate one consumer at a time from persona CSVs to resources/tools. Shadow-compare outputs; retain compatibility export and snapshot rollback. After parity and reliability gates pass, stop Git data commits, remove unused wrappers and archive the hard-coded draft. Never use the legacy draft as a production fallback.

## Decisions required before the corresponding phase

| Decision | Proposed default | Needed before |
| --- | --- | --- |
| Hosting and persistence | One managed runtime, PostgreSQL, object storage; Redis for live views | Phase 2 deployment |
| NFL/NHL live-data latency and budget | Published NFL data initially; qualify a real-time provider separately | Phases 4 and 6 |
| First Sleeper league/scoring policy | One league as the reconciliation fixture | Phase 3 integration |
| Market products and accounts | Read-only eligible 15-minute markets first | Phase 5 |
| Combo meaning | Distinguish native combo, independent basket, cross-venue hedge | Phase 7 |
| Risk authority and limits | Server policy enforced independently of agents | Phase 7 |
| News sources and publishing destinations | Evidence preparation first | Phase 8 |

These decisions do not block reviewing or running the current scaffold. They prevent a future implementation from silently choosing financial, data-provider or publishing policy on your behalf.
