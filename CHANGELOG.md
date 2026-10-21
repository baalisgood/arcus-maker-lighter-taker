# Change list

## v12 — Arcus fill rows no longer overwrite order side

- Root cause of the 14:18 SOL halt (`observation_mismatch side: observed=1 ledger=-1`): when a `userFills` row arrived with a newer sequence than the cached order, it was merged over the order and its `side` (trade side, not order side) replaced the order's side.
- `userFills` merge now drops `side`/`orderSide`; order side only comes from the order channel / REST. Ledger side validation is unchanged (not relaxed).
- Test: `test_fill_row_side_never_overrides_order_side`.

## Lighter write gate shared across markets: wait instead of burning retries
- Root cause of the 13:41 SOL `retry_exhausted` halt (and the earlier SOL 1521 UNKNOWN): the Lighter adapter admits one unresolved write at a time across all markets. A SOL hedge issued while a HYPE hedge (2726) was still in flight was blocked before signing; v10 counted each block as a zero-fill failure, so four retries were spent within one second.
- `hedge` now waits up to `gate_wait_seconds` (2s) for another market's in-flight hedge to settle, clearing journal-terminal gate entries each poll. If still busy, it defers (`lighter_hedge_gate_busy`, no retry consumed, nothing sent) and the engine re-drains that symbol after 1s (`_redrain_later`) instead of waiting for the next reconcile cycle.
- Operator: `tools/operator_resolve_hedge.py --release-retry-exhausted [--confirm]` releases a `retry_exhausted` manual-intervention latch only when every symbol's ledger equals actual positions, no attempts/makers are active and the failure market has no open Lighter orders. Prior zero-fill attempts of the residual obligations stop counting (`retry_reset:<id>` meta), the latch is removed and `operator_release_retry_exhausted` is recorded; the runner re-hedges the residual on start.

## Lighter hedges that never reached the venue no longer halt as UNKNOWN
- Root cause of the SOL 1521 / HYPE 2658 halts (no `writes` row): the Lighter adapter keeps a write gate (`_uncertain_orders`) and refuses new orders while any entry remains. Hedges resolved through inactive history (`find_order_by_client`) never cleared their entry, so the next hedge failed with OrderStateUnknownError before signing and was then treated as UNKNOWN.
- Terminal Lighter observations from cache, REST or inactive history now clear the gate entry; before each hedge, entries whose attempt the journal already holds terminal are cleared (`lighter_uncertain_cleared`).
- Any hedge failure before the audited send (gate block, nonce fetch, signing, audit) is a definitive no-send: `lighter_hedge_not_sent` (error type, code frames, blocking client; no raw exception text) and a normal zero-fill retry.
- Recovery: a Lighter attempt without an audit row is resolved as rejected/0 (`lighter_attempt_never_sent`); the audit row is committed before `send_tx` under the write lock, so its absence proves nothing was sent.

## Cancel results and startup lookups adopt ledger identity
- `observe_cancel` and startup maker recovery now apply the same ledger adoption as live pushes before validation. A cancel can return a cached sparse or zero-size terminal Arcus row (no side/size); previously this raised ValueError and halted with `CRITICAL cancellation unresolved` (seen live: amlt-2637).

## Arcus zero-size REJECTED pushes treated as status-only
- Seen live (amlt-1733, HYPE): a REJECTED push reported `originalSize` 0 (unresolved market `MARKET-0`), giving requested 0 / cumulative 0.13 and halting with `Fill processing failed`. A zero or negative reported size is no longer quantity evidence: requested/cumulative are dropped and the known order (matched by exchange id) inherits size and filled quantity from the ledger, like other status-only pushes. Position reconciliation still catches any real fill.

## Arcus clientId reuse when the ledger row has no exchange id
- A push matched to a ledger maker only by clientId (row has no stored exchange id) is now ours only if it does not contradict the row's durable intent (symbol, side, size). Contradicting pushes are handled as foreign orders (`client_id_reused_by_foreign_order`; terminal startup snapshots → `foreign_order_ignored`) instead of halting with `Observation does not match durable intent/account`.
- Seen live: old HYPE sell orders reusing amlt-1169 (ledger: BTC buy) and amlt-1360 (ledger: HYPE buy) halted startup.

## Unknown Lighter hedge: longer evidence-based lookup before halting
- When a Lighter hedge is not terminal within 3s, recovery now polls both the active order lookup and inactive order history (`find_order_by_client`) for up to 12s (`LiveTransport.lookup_poll_seconds`); the engine allows `lookup_timeout`=25s for Lighter lookups instead of the 5s request timeout that previously cut recovery off before any history or tx evidence was read.
- Tx evidence (`transaction_api.tx(by='hash')` + next nonce) is recorded before and after polling as `unknown_transaction_evidence` (now including `signed_hash`). Missing audit rows or hashes are logged as `unknown_transaction_evidence_unavailable` with `NoAuditedWrite` / `NoSignedHash`; non-ValueError lookup failures record only the exception type.
- Evidence is audit-only: fills are still taken only from order observations; an order that cannot be found still halts as UNKNOWN.

## Externally closed positions

- `tools/operator_resolve_hedge.py --accept-flat --confirm` resets a hedged ledger pair (net 0, no pending obligation/maker) to flat when both venues are verifiably flat (positions closed manually); dry run by default.
- Startup position mismatches record a `startup_position_mismatch` event with per-venue actual/ledger values.

## Reused clientId from historical orders; Lighter trades sort

- Fix startup halt `symbol: observed='BTC-USDC-PERP' ledger='HYPE-USDC-PERP'`: an Arcus history order reusing the ledger's `amlt-N` clientId (older/reset ledger on the same account) was matched by clientId alone. A clientId match is now ours only if the exchange order id agrees or is not yet known; otherwise the order is handled as foreign and a `client_id_reused_by_foreign_order` event is recorded. Maker pushes resolve the ledger row by exchange order id first.
- Unknown observations without side/size halt as unknown instead of raising in the socket callback.
- Lighter `/trades` uses `sort_dir=desc` (mainnet rejects `asc` with code 20001).

## Status-only Arcus pushes and worker diagnostics

- Fix live halt `Fill processing failed` (HYPE): a late Arcus push for a cache-evicted order carried no side/size, and the adapter's BUY/0 defaults were validated as `side 1 vs -1, requested 0 vs qty`. Such pushes now mark side/size unknown and inherit them from the ledger row matched by exchange order id (FILLED = full ledger size); unmatched pushes still fail closed.
- Worker failures record a sanitized `worker_error` event (code frames, project ValueError message, observation fields); raw exception payloads are still never persisted.
- Add read-only `tools/probe_lighter_trades.py` to find which Lighter `/trades` parameters this account accepts.

## Lighter send rejection handling and terminal monitor

- `send_tx` HTTP 4xx (except 429) and non-200 response codes without a tx hash are now definitive rejections (`OrderSendRejectedError`): the nonce is released and hard-refreshed from the exchange (key retired if refresh fails), a `lighter_send_rejected` event records status/body/nonce/key, and the engine retries the hedge under `max_retries` instead of halting as UNKNOWN.
- Ambiguous send failures still halt as UNKNOWN but now record a `lighter_send_error` event with the underlying error.
- Add `tools/monitor.py`: read-only rich terminal dashboard (positions, net exposure, resting makers, pending/unresolved hedges, 24h fills/hedges, latency p50, daily statistics, recent events, halt/manual-intervention banner).

## Unknown hedge recovery and diagnostics

- Add `tools/operator_resolve_hedge.py`: read-only dry run by default; with `--confirm` it closes a single unresolved Lighter hedge attempt only when live positions prove its fill amount (Arcus equals ledger, Lighter differs by 0..remaining in hedge direction, no open Lighter order, attempt older than 120s). Remaining exposure is re-hedged by normal recovery on next start.
- Arcus pushes for known orders that omit `clientId` or the market name adopt identity from the ledger row matched by exchange order id; conflicting values still fail closed.
- `Observation does not match durable intent/account` now lists each mismatching field (observed vs ledger) and records an `observation_mismatch` event; leverage halts include per-symbol leverage.
- Lighter client-id history search parses only the requested identity; unknown-transaction evidence records nonce, key index and transaction event info.
- Lighter fills retry once with market scope on request rejection and record the API error; funding rows for unconfigured markets (shared account) are skipped and reported instead of failing coverage.

## Received-frame liveness and execution diagnostics

- Separate book continuity from socket liveness, observe real data/control frames on public/private maker sockets and actively ping; static books retain their identity and consumed paper depth.
- Use official Arcus full-snapshot and Lighter prior-nonce linkage rules, preserve EMA only for short same-socket timeouts, and recheck Lighter admission after lock/nonce awaits before signing.
- Materialize close-intent mode and exact confirmed FIFO gross tranches with atomic diagnostic-only event enrichment/cursor/audit; retain unknown actual price/time coverage.
- Add complete observed-runtime resting quote uptime, stale reason/episode durations, per-period roundtrips, gross PnL/notional ratios, actual holding time and weighted edge/slippage coverage without a schema or dependency change.
- Replace SQLite-version-specific plan wording assertions with measured incremental work, keep metadata prefix checks indexable and preserve archive-aware diagnostic attribution.
- Preserve EMA and pause incomplete warmup for short same-socket private timeouts while retaining global cancellation; reset after long private gaps or a socket/generation change, including a long gap recovered between checks.
- Count floor/timeout closes by completed unique obligation, expose stable gross PnL/stale event fields and retain independent stale intervals across process boots.
- Recheck all normal Arcus makers after write-lock/authentication waits before signing, and reuse a successful close cancellation’s reserved cycle for its same-direction replacement without increasing the requote rate.

## FIFO estimated close-profit floor

- Add per-symbol merged `min_close_profit` (default 0.01%) and `close_floor_timeout` (default 600 seconds), with finite range validation.
- Protect normal close quotes using actual two-leg FIFO opening fills; each lot ages independently, expired portions do not subsidize fresh portions, and partial closes revalue the remaining FIFO quantity.
- Replay committed events into current lots and unsettled restoration undo without a schema change; preserve original price/time on restoration and use persistent conservative observation time only when attribution is unavailable.
- Recheck closing bounds on market changes, partial fills, expiry, capital admission and immediately before Arcus signing; protective cancellations bypass replacement throttles while re-posting respects them.

## Paper hedge depth and progress diagnostics

- Retain immutable public depth and match paper Lighter IOC fills across eligible levels using the live native price bound, lot precision, VWAP and per-snapshot liquidity consumption.
- Count only confirmed zero-filled terminal Lighter attempts against the existing initial-plus-retries failure budget; positive partial progress and deferred attempts do not consume or reset it.
- Persist the first halt cause and retain it in runtime/final output even when later cancellation, emergency or latency alarms occur.

## Current-state performance and market readiness

- Upgrade strictly validated v1/v2 journals atomically to v3 exact current inventory and pending IDs; index active lifecycle and relationship queries and remove full-history reads from trading callbacks.
- Move bounded, completely closed FIFO components into archive tables in transactional maintenance batches; restore complete components for late maker deltas, preserve history/finance coverage and reject inconsistent derived/archive state.
- Yield finance history processing and run hourly statistics on a dedicated read-only snapshot in a fresh interpreter without inherited credentials or socket descriptors.
- Require 60 continuous fresh seconds before new opening/closing quotes, separately from EMA tau and startup subscription readiness; replay now simulates the same warmup.
- Isolate stale/generation-invalid symbols for cancellation and admission while retaining global venue/private disconnect, UNKNOWN, cancellation and capital protections.

## Basis center and residual accumulation

- Quote opening and closing around a fresh per-symbol time-weighted EMA basis center; crossing prices use the nearest legal passive tick across tiers.
- Upgrade validated v1 journals transactionally to v2 FIFO attempt allocations. Keep original obligation attribution, retry budgets and UNKNOWN identity recovery.
- Accumulate compatible residuals until precision, metadata minBase and the conservative max(10 USDC, minQuote) policy pass; deferred dust remains in risk accounting and permits reconciled restart.
- Recheck policy before live signing and in paper mode; size each small-live flat quote for both venue minimums. Report pending residuals and flatness separately at shutdown.
- Attribute aggregated actual execution slippage using filled allocations; retain unknown cumulative economics as unknown.


## Maker strategy and accounting

- `core/services/arbitrage_monitor_v2/config/maker_taker_config.py`: typed, validated maker/hedge/risk profiles; strict disabled symbols and mode selection.
- `core/services/arbitrage_monitor_v2/execution/maker_taker_engine.py`: quotes, fill workers, independent risk cancellation, portfolio reservations, reconciliation with fresh capital checks before every corrective send, recovery and bounded shutdown.
- `core/services/arbitrage_monitor_v2/execution/maker_taker_journal.py`: SQLite intent/identity/fill/hedge durability, schema checks and execution/funding coverage records.
- `core/services/arbitrage_monitor_v2/execution/maker_taker_quotes.py`: percentage formulas, tick-tier rounding and common quantity precision.
- `core/services/arbitrage_monitor_v2/execution/maker_taker_paper.py`: public-only market streams and deterministic conservative paper execution.
- `core/services/arbitrage_monitor_v2/execution/maker_taker_live.py`: explicit live bridge, authoritative observations, known-ID recovery and account-scoped finance reads.
- `core/services/arbitrage_monitor_v2/history/maker_taker_finance.py`: actual trade/funding backfill, pagination, deduplication and explicit unverified Lighter semantics.
- `core/services/arbitrage_monitor_v2/history/maker_taker_statistics.py`: hourly/daily volume, fees, funding, latency, slippage and cost-coverage projections.

## Adapter and transport boundaries

- `core/adapters/exchanges/official_transport.py`: official API destinations, verified TLS before signer/token creation, disabled HTTP/WebSocket redirects and disabled environment proxies for authenticated clients.
- `core/adapters/exchanges/adapters/arcus.py`: quantity-independent observations, client-ID lookup, safe known-order cancellation and redirected-write UNKNOWN handling.
- `core/adapters/exchanges/adapters/arcus_websocket.py`: socket generations and nonblocking quantity callbacks/recovery.
- `core/adapters/exchanges/adapters/lighter.py`: public runtime settings with explicit credential injection and validation before signer construction.
- `core/adapters/exchanges/adapters/lighter_base.py`: transport protection for signer-owned API clients.
- `core/adapters/exchanges/adapters/lighter_rest.py`: durable pre-send audit hooks, strict size precision, equity validation and paginated order recovery.
- `core/adapters/exchanges/adapters/lighter_websocket.py`: account index zero, socket-bound heartbeat, receive freshness, immediate private-reader readiness invalidation and protected WebSocket connections.

## Entry and configuration

- `run_maker_taker.py`, `start.sh`: explicit paper/monitor/live/replay/legacy modes, separate account locks and signal handling.
- `main_unified.py`, `scripts/start_lighter_arcus.sh`: retain explicit legacy execution and prevent legacy ownership of a maker profile.
- `core/services/arbitrage_monitor_v2/config/symbol_config.py`: no default fallback for disabled/unconfigured maker symbols.
- `core/services/arbitrage_monitor_v2/core/orchestrator_bootstrap.py`: public Arcus/Lighter runtime settings with environment-backed authentication for the legacy entry.
- `config/arbitrage/arbitrage_segmented.yaml`: maker defaults, four enabled symbols, quantities and portfolio limits.
- `config/arbitrage/monitor_lighter_arcus.yaml`: BTC, ETH, SOL and HYPE subscriptions.
- `config/arbitrage/arbitrage_taker_taker.yaml`: separate preserved legacy strategy profile.
- `config/exchanges/arcus_runtime.yaml`, `config/exchanges/lighter_runtime.yaml`: public settings without credentials.
- `.gitignore`: exclude private/runtime material while retaining public runtime samples; obsolete unprefixed `gitignore`/`gitattributes` files were removed.

## Verification and operator documentation

- `tests/test_maker_taker_engine.py`: duplicate/partial/unknown fills, crash boundaries, cancellation races, reservations, reconciliation and dispatch timing.
- `tests/test_maker_taker_boundaries.py`: configuration, quotes, public protocol fixtures, paper/monitor entry, equity, locks and environment-backed legacy bootstrap.
- `tests/test_maker_taker_finance.py`: actual Arcus money, raw Lighter semantics, pagination, deduplication and incomplete-cost behavior.
- `tests/test_maker_taker_security.py`: account lock overlap, corrupt journal rejection, destination validation and redirect protection.
- `tests/fixtures/maker_taker_replay.json`: synthetic offline replay, explicitly separate from live public-market evidence.
- `tests/legacy_fixtures.py`: self-contained retained executor fixtures.
- `tests/test_arcus_adapter.py`, `tests/test_arcus_lighter_live_guards.py`, `tests/test_lighter_strict_state.py`, `tests/test_arbitrage_regressions.py`: update configuration/fixture expectations for the independent profiles and strict validation.
- `requirements-test.txt`: pinned development test dependency; production dependencies remain in `requirements-lighter-arcus.txt`.
- `README.md`: setup, configuration units, paper/live/tmux commands, stop/recovery behavior and unverified live/economic acceptance boundaries.
