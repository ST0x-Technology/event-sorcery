# Migrating a manual / apalis job system to event-sorcery durable jobs

This guide is for a consumer that already runs background work through a manual
or apalis-backed job system (e.g. `st0x.liquidity`'s `conductor::job` over
`apalis-sqlite`) and wants to move to event-sorcery's cqrs-native durable jobs
(ADR-0006,
[`docs/event-sourced-job-backend-spec.md`](event-sourced-job-backend-spec.md)).

It is grounded in a full audit of `st0x.liquidity`'s 17 job kinds. The headline:
event-sorcery **partially** replaces such a system — it owns the _execution_
machinery cleanly, but a real consumer needs a few enqueue-side capabilities
that event-sorcery does not ship yet (§2), and the cutover must be done in a
specific order to avoid losing or double-running in-flight work (§3).

## 1. What event-sorcery already covers

If your job system uses these, event-sorcery is a clean (often stronger) drop-in
for the _execution_ side:

- **Durable at-least-once execution** — each job is its own event stream
  (`Enqueued`) + a `job_queue` projection row, replacing apalis `SqliteStorage`
  rows.
- **Retry with exponential backoff** — `max_attempts` + `Backoff` via
  `RetryScheduled`; `attempt` is bumped only by a recorded failure, so a crash
  is not a failed attempt (matches apalis's contract).
- **Serialized execution per kind** — `max_concurrency = 1` reproduces apalis
  `.concurrency(1)`; the `ConcurrencyLimit` layer gates the _claim_, so a job is
  claimed only when a slot is free.
- **Crash / orphan recovery** — the claim/lease/`renew` with a projection-only
  `lease_until` column re-claims a crashed worker's job once its lease expires.
  This **replaces `requeue_orphaned`** and sidesteps the apalis
  deterministic-worker-name heartbeat bug — but note it recovers after the lease
  expires (~`lease_duration`), not instantly at startup (§4).
- **Dead-letter with audit retention** — `JobState::Dead { reason }` rows are
  retained (`status = 'dead'`, never re-run), like apalis
  Failed-for-reconciliation.
- **Crash-loop protection** — `max_claims` dead-letters as `Abandoned` a job
  that keeps claiming but never records an outcome (apalis lacked this).
- **Execution timeout** — `tokio::time::timeout(execution_timeout)` per
  `perform`.
- **Sub-second pickup** — a fixed 250ms poll cadence (no apalis-style
  exponential idle backoff), so a low-latency SLO is met out of the box.

Not load-bearing either way: apalis-workflow DAG `Output` composition (liquidity
sets `Output = ()` everywhere) and the `FailureInjector` (test-only).

## 2. What event-sorcery does NOT cover yet (prerequisites)

event-sorcery's enqueue model is the **inverse** of a typical job system. ES
enqueue is **handler-only**: `JobQueue::push` buffers into the `PENDING_JOBS`
scope that `Store::send` opens around a command, so the job commits atomically
with the command's events. A push outside that scope is dropped with a warning.

But in `st0x.liquidity`, **all 17 jobs are enqueued from reactors, polling
loops, sibling jobs (chains), self-reschedules, or startup recovery — zero from
command handlers.** So the current ES job feature cannot enqueue any of them.
These capabilities must land in event-sorcery first; each is independently
shippable and a no-op until used:

| Prerequisite                                                                                                                                                                   | Why liquidity needs it                                                                                                                                                                                                                              |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Standalone durable-enqueue API** (append `Enqueued` + seed the row in its own transaction, outside a command)                                                                | The central unblocker — reactor/poller/chain/startup enqueues. Reshaping all of these into command→handler→push is artificial for pollers and chains and should be rejected; keep handler-push as the default only for genuinely command-born jobs. |
| **Defer / snooze success outcome** (e.g. `Ok(JobOutcome::Defer(delay))` → a `Rescheduled` event that re-arms `run_at` _without_ incrementing `attempt` or recording a failure) | 6 jobs self-reschedule as a success "come back later" (poll-while-pending, CCTP attestation waits, recovery guard contention). On the failure-only path they would exhaust `max_attempts` and dead-letter. Distinct from `RetryScheduled`.          |
| **Enqueue-time dedup** (optional `Job::dedup_key`) + **`has_in_flight` / pending-count query**                                                                                 | Backfill overlap-skip, per-symbol recovery dedup, direction-independent transfer dedup. ES mints a fresh ULID per push with no unique key.                                                                                                          |
| **Per-kind terminal-failure (fail-stop) hook** fired on `Dead` (including `DeadReason::Abandoned`)                                                                             | Liquidity crashes the process on a terminal trading/transfer failure and lets systemd restart clean. ES currently dead-letters and keeps idling. A kind that does _not_ install the hook is the best-effort tier — one mechanism covers both.       |
| **Cancel-by-kind / cancel-by-dedup-key** (a terminal `Cancelled`)                                                                                                              | Discard stale pending work (rebalancing checks captured against pre-rebalance inventory; stale resume rows).                                                                                                                                        |
| **Lease/timeout sizing guidance** + steer long external waits to _defer_                                                                                                       | A multi-minute CCTP wait inside `perform` would outlive a 30s lease (→ double-claim → double burn) or hit the 300s timeout.                                                                                                                         |
| **`prune_terminal_jobs`** (already an ADR-0006 follow-up)                                                                                                                      | Replace apalis `spawn_finished_job_cleanup`; bound `job_queue` growth while retaining `Dead` rows for audit.                                                                                                                                        |

Until at least the standalone-enqueue API, defer, dedup, and the fail-stop hook
land, a liquidity migration cannot start.

## 3. The safe cutover (per kind)

Both backends can coexist on the **same SQLite file** during the migration
(apalis `Jobs` table + event-sorcery `events`/`snapshots`/`job_queue`). Run both
migrators with `set_ignore_missing(true)` against the shared `_sqlx_migrations`.

For each job kind, migrate one kind at a time, leaf-first, with **both workers
running concurrently**:

1. The apalis worker keeps draining pre-existing apalis `Jobs` rows; the ES
   worker for that kind runs the newly-enqueued ES work.
2. **Flip the enqueuer atomically** for that kind: every site that enqueues it
   switches from `apalis push` to the ES standalone-enqueue API in one deploy.
3. Because a single logical enqueue lands in **exactly one backend (one
   table)**, running both workers concurrently does **not** double-execute —
   each worker only sees its own table's rows.
4. The apalis worker is removed for that kind only once apalis `has_in_flight()`
   for it is false (all pre-cutover rows reached a terminal state).

> **Why not "drain first, then spawn the ES worker"?** That ordering looks safer
> but isn't: if you flip the enqueuer to ES while only the apalis worker runs,
> the new ES rows have _no_ worker until you spawn it — a live processing outage
> (and an unhedged-exposure window) for any continuously-arriving kind, and a
> forced rollback if an apalis row can't drain. Run both workers; the
> no-double-execution property comes from one-enqueue-one-backend, not from
> sequencing.

### Cutover order (liquidity)

Leaf-first, lowest-risk first:

1. **`ResumeTokenizationAggregate`** — its work is re-derived from the event
   store, so even total loss of its apalis rows is recovered. Validates the dual
   stack + standalone-enqueue + recovery end to end. Best-effort (no fail-stop
   hook).
2. **Chained / idempotent trading queues** — `AccountForDexTrade`,
   `PollOrderStatus` (needs _defer_), `ReconcileOrderFill`,
   `HandleOrderRejection`, `PlaceHedge`. Pollers (`BackfillRange`,
   `CheckPositions`) re-drive naturally.
3. **`AccountForDexTrade` is the one cutover-critical queue** — its non-terminal
   row is the _sole_ record of an observed fill once the backfill checkpoint
   passes that block. **Never** remove its apalis worker, recreate the DB file,
   or drop the `Jobs` table while any non-terminal trade-accounting row exists.
   A permanently-failing row is a hard STOP-and-reconcile, not a skip.
4. **Money-movement + self-rescheduling queues** — USDC/equity transfers,
   wrapped/unwrapped recovery, `CheckPositions`. Need defer + dedup + cancel.
5. **Wire the fail-stop hook** to `ConductorExit` for supervised kinds; leave
   `ResumeTokenizationAggregate` unhooked. Port the e2e fail-stop assertions.
6. **Decommission apalis** (final, irreversible) — only once every queue is
   drained to zero non-terminal rows: remove the second pool, the apalis
   migrations, `JobQueue`/`JobKind`/`FailureInjector`, and the finished-job
   cleanup.

## 4. Safety invariants (audit each migrated job against these)

- **Idempotency keys on domain identity, NOT `job_id`.** ES is at-least-once for
  _every_ `perform`, and re-claim/re-enqueue mints a fresh job identity. The
  ADR-0006 idempotency contract's `job_id` key does **not** dedupe a re-enqueued
  or cross-stack duplicate. Liquidity's guards correctly key on
  `tx_hash`/`log_index`, `symbol`, transfer direction — keep them. The new queue
  `dedup_key` is _additive_ defense, not a replacement for the
  event-reconstructed fail-closed `usdc_in_progress` / `equity_in_progress`
  guards (those are the authoritative re-burn defense for real money).
- **Cross-stack dedup is a no-op during the dual-stack window.** ES
  `has_in_flight` queries only `job_queue`, never the apalis `Jobs` table. A
  kind whose only overlap defense is `has_in_flight` (e.g. `BackfillRange`) has
  no in-memory backstop — migrate it in a single flip and confirm no overlap can
  occur across the cutover.
- **`max_concurrency = 1` does NOT halt after a dead-letter** the way apalis
  fail-stop does — the ES poll loop moves straight to the next pending
  candidate. The halt depends entirely on the fail-stop hook crashing the
  process _before_ the next poll claims. Make the hook synchronous/abort-fast,
  and treat the Dead-to-exit window as a real race for kinds that must not run
  with stale state.
- **Model multi-minute external waits as _defer_, not a long `perform`.** A long
  blocking `perform` can outlive the lease (second worker claims → double
  effect) or hit `execution_timeout` → dead-letter. Defer returns fast and
  re-arms, so no lease is held across the wait.
- **Route `DeadReason::Abandoned` (claim-budget) through the fail-stop hook**
  for critical kinds, so a restart-heavy crash-loop escalates loudly instead of
  dying silently.
- **Single process owner during cutover.** ES startup/lease recovery assumes one
  fresh process; systemd must ensure the old process exits before the new starts
  (the ES claim CAS is strictly safer here than apalis's heartbeat).
- **`SeedVaultRegistry` runs inline at startup, not queued** — do not give it an
  ES worker that could double-run it.

## 5. Schema migration

A consumer owns its migrations. event-sorcery ships the canonical schema; copy
it into your migration directory (see `examples/simple/migrations/`):

- `…_init.sql` — the `events` + `snapshots` tables (the event-sourcing core).
- `…_job_queue.sql` — the `job_queue` view table (generated
  `kind`/`status`/`run_at` columns + the projection-only `lease_until`).

`StoreBuilder::build()` and `JobRuntime::build()` wire over an already-migrated
pool; neither migrates for you (so they compose with your own view migrations
instead of conflicting). Apply all migrations at startup before building either.
