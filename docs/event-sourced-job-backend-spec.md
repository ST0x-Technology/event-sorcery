# Durable jobs over `EventBackend` — implementation spec

Status: **Implemented.** This is the worker/claim/ack blueprint for the
cqrs-native durable-job backend. The authoritative design record is
[ADR-0006](../adrs/0006-cqrs-native-durable-jobs.md); this doc is the concrete
implementation map (types, SQL, the concurrency rationale). It supersedes the
old `JobStore` spec (ADR-0005).

## 0. Scope, deployment model, the guarantee

**Hard deployment constraint:** one SQLite database, **one process**, N
in-process worker instances sharing the host wall clock. SQLite single-writer
CAS and `lease_until` comparisons are NOT sound across machines / network FS,
and cross-host skew drives premature lease steals. **Multi-node is out of
scope.** The lease math assumes a roughly-monotonic clock; a large backward NTP
step can briefly hide a crashed job or expire a live one (bounded, accepted).

**Guarantees, precisely:**

1. **Exactly-once commit of every lifecycle transition** (Claimed / Succeeded /
   RetryScheduled / Dead), enforced by the events
   `(aggregate_type, aggregate_id, sequence)` UNIQUE constraint. The ledger is
   the financial system of record; never double-written.
2. **Single executor while healthy** via a renewed lease; a slow-but-live job is
   never stolen.
3. **At-least-once execution across crashes** (irreducible) -> closed to
   **exactly-once effect** only by the consumer idempotency contract (§12).

## 1. The shape: jobs are a cqrs/es aggregate

A job is its own event stream (`aggregate_type = "job"`). `JobState` is an
[`EventSourced`] aggregate (`JobEvent` = Enqueued / Claimed / Succeeded /
RetryScheduled / Dead; `JobCommand` = Succeed / RetrySchedule / Kill). The whole
durable-jobs capability is built from one consumer-supplied **`EventBackend`** —
a cqrs-es event repository plus two job-shaped primitives, `claim` and `renew`.
`JobRuntime::build(pool)` wires the job `Store` (cqrs/es) and the `job_queue`
[`Projection`] via `StoreBuilder`; nothing else is required of the consumer.

The ack rides cqrs-es `execute` (a `JobCommand`); only the **claim** is a
backend transaction, because its runnable check must read the projection-only
lease column, which cqrs-es cannot express.

## 2. Keystone decisions (each closes a specific adversarial finding)

- **D1 Lease renewal is a projection-only `UPDATE`, never an event.**
  `UPDATE job_queue SET lease_until=? WHERE view_id=? AND version=claim_seq AND
  status='claimed'`.
  The renewal is invisible to the event stream, so the ack (an `execute`)
  commits at `claim_seq+1` regardless of how many times the lease was renewed.
  After the ack advances the row, a late renewal tick matches 0 rows and stops
  -> the **post-ack resurrection bug cannot happen**. Because the live lease is
  not folded from events, `JobState::Claimed` carries **no** `lease_until`
  field; the claim re-reads the column in-txn (D2). On a projection rebuild the
  column is NULL (reclaimable; rebuild only runs with no live workers).
- **D2 The claim is one `BEGIN IMMEDIATE` transaction that re-reads the
  `job_queue` row, lets a crate-side closure (`plan_claim`) decide, and enacts
  the decision** (events-UNIQUE compare-and-swap append + row write). The in-txn
  re-read of the renewed lease **column** is why a live lease cannot be stolen —
  reading a folded event lease would miss intervening renewals.
- **D3 Concurrency bound is a mandatory `ConcurrencyLimit` layer in
  `middleware()`.** apalis pulls the next stream item only after `poll_ready` is
  `Ready`, and the claim happens inside the stream's `poll_next` -> **a job is
  claimed only when a free execution slot exists** (no claim storm, no
  pre-execution steal). Not a tuning knob.
- **D4 Per-claim execution timeout:**
  `tokio::time::timeout(execution_timeout,
  perform)`; a timeout is `Err` ->
  normal failure path (counts an attempt).
- **D5 Claim-budget ceiling:** a crash-before-ack never increments `attempt`, so
  `claims` is folded onto `JobState` directly (incremented by every `Claimed`).
  If `claims >= max_claims`, `plan_claim` returns `Abandon` -> the claim
  transaction appends `Dead{Abandoned}` instead of `Claimed`. No extra query.

## 3. The fence (the correctness heart)

Each claim mints a fresh `ClaimId` (a ULID, per-claim, never per-worker),
carried on the `Claimed` event and folded onto `JobState::Claimed`. The ack
(`JobCommand` carrying that `ClaimId`) is fenced **two ways**:

1. **State fence:** `JobState::transition` returns `JobError::Fenced` unless the
   live state is `Claimed` with a matching `claim_id`. A re-claim folds a new
   `claim_id`, so the prior runner's ack is rejected before any event is
   written.
2. **CAS backstop:** even if two acks pass the state fence in a race, `execute`
   commits at `last_sequence+1`; the events UNIQUE rejects the loser
   (`AggregateError::AggregateConflict`).

Either fence on a **successful** job is logged as a DOUBLE-EXECUTION alarm (a
re-claimer owns the outcome; never clobber it). The alarm is monitoring, not the
safeguard — that is the consumer idempotency contract (§12).

## 4. SQLite pool contract (REQUIRED — without it the CAS does not occur)

- `journal_mode = WAL` (poll readers don't block the writer).
- `busy_timeout >= 5000ms` — **critical**: with `busy_timeout=0` the CAS loser
  gets `SQLITE_BUSY` before the UNIQUE check, so the "loser sees UNIQUE -> skip"
  mechanism never fires. With a positive timeout the loser blocks, the winner
  commits, the loser then sees the real UNIQUE violation.
- `synchronous = FULL` (outcome event fsync-durable before "done").

In-memory tests use a single shared connection (`max_connections=1`); a multi-DB
pool over `:memory:` gives each connection a separate database.

## 5. `job_queue` view table

The standard `(view_id, version, payload)` cqrs-es projection of
`Lifecycle<JobState>`, plus the **projection-only** `lease_until` column (the
only column the reactor never touches — written by `claim`/`renew` alone). Poll
keys are generated columns derived from the payload:

```sql
kind   TEXT    GENERATED ALWAYS AS (COALESCE(json_extract(payload,'$.Live.Pending.kind'),
                                             json_extract(payload,'$.Live.Claimed.kind'))) STORED,
status TEXT    GENERATED ALWAYS AS (CASE WHEN json_type(payload,'$.Live.Pending') IS NOT NULL THEN 'pending'
                                         WHEN json_type(payload,'$.Live.Claimed') IS NOT NULL THEN 'claimed'
                                         WHEN json_extract(payload,'$.Live')='Done'           THEN 'done'
                                         WHEN json_type(payload,'$.Live.Dead')    IS NOT NULL THEN 'dead' END) STORED,
run_at INTEGER GENERATED ALWAYS AS (COALESCE(json_extract(payload,'$.Live.Pending.run_at'),
                                             json_extract(payload,'$.Live.Claimed.run_at'))) STORED
```

Indexed by `(kind, status, run_at)` and `(kind, status, lease_until)`. Terminal
rows (`done`/`dead`) are retained, not deleted (the full history is the audit).

## 6. Poll -> claim -> run -> ack

**Poll** (`EventStoreBackend::poll`): a generic `Projection::find` (the
sqlite-es `IndexedView` DNF) over the runnable predicate:

```
(kind=K AND status='pending' AND run_at <= now)
  OR (kind=K AND status='claimed' AND lease_until <  now)   -- expired lease
  OR (kind=K AND status='claimed' AND lease_until IS NULL)  -- rebuilt, reclaimable
```

ordered by `run_at ASC`. Every transient DB condition is logged and downgraded
to `Ok(None)` (a stream `Err` stops the worker); the claim is rate-bounded by
D3.

**Claim** (`EventBackend::claim(job_id, plan_claim)`): the backend opens
`BEGIN IMMEDIATE`, re-reads the row into a
`ClaimRead{version, payload,
lease_until_ms}`, and runs `plan_claim`, which
decides on the **state variant** (not on whether `lease_until` is set — a
retried pending row keeps a stale lease): a runnable Pending/Claimed -> `Claim`
(append `Claimed`, write the row with the new lease), a budget-exhausted job ->
`Abandon` (append `Dead`), else `Skip`. The backend enacts: CAS-append via the
events UNIQUE (`OptimisticLock` -> `Contended`), then write
`(version=claim_seq, payload,
lease_until)`. The won claim carries `claim_seq`
(renew/ack key), `claim_id` (fence key), `attempt`, and the decoded `args`.

**Ack** (`AckService`): spawn a `renew_loop` (D1, projection-only UPDATE keyed
on `claim_seq`) bound to the whole op; run `perform` under `timeout` (D4); ack
while the lease is still held, then cancel renewal. If the renewal saw 0 rows
(`lost`), skip the ack — a re-claimer owns it. The ack is
`jobs.send(job_id, JobCommand::{Succeed | RetrySchedule | Kill})`, fenced per
§3; transient DB errors retry within the lease.

## 7. Retry / Dead / abandon

`attempt` advances ONLY via `RetryScheduled`. Honest guarantee: **at most
`max_attempts` recorded failures; physical executions may exceed that under a
lease steal** (bounded by `max_claims`). Durable retry only; apalis
`RetryPolicy` is not installed (one execution per claim).

## 8. Enqueue (atomic with the triggering events)

A handler holds a `JobQueue` (in its `Services`) and calls `push` /
`push_with_delay` synchronously. `Store::send` opens a `PENDING_JOBS` scope
around `execute`; `push` buffers into that scope; the event repository's
`persist` drains it (`take_pending`), appending each `Enqueued` event AND
seeding its `job_queue` row (version 1, pending) in the **same transaction**
that commits the triggering events. A job is therefore enqueued (and pollable)
iff that transaction commits. A `push` outside a command scope is a programming
error, dropped with a warning.

## 9. Worker wiring

```rust
let runtime = JobRuntime::build(pool).await?;            // one per process
let worker = EventStoreBackend::<MyJob>::new(runtime.clone(), "my-worker", cfg, Clock::system());
WorkerBuilder::new(name).backend(worker).data(Arc::new(input)).build(run::<MyJob>);
```

NO user retry/timeout layer; NO buffering layer outside the ack layer (must
propagate `poll_ready` backpressure). Multi-threaded runtime required (the
spawned renewal must run during a CPU-heavy `perform`; CPU-bound work uses
`spawn_blocking`).

## 12. Consumer idempotency contract (first-class)

Exactly-once _effect_ needs consumer cooperation. Inject `job_id` into `perform`
as the external idempotency key. **Key on `job_id` ALONE, never
`job_id+attempt`** (the latter fails to dedup the crash-after-effect duplicate).
The external boundary must be success-equivalent under that key: a re-issue of
an already-succeeded op must replay success, not error. The in-library
fenced-ack alarm is monitoring, not the safeguard.

## 13. Residual risks (accepted / documented)

At-least-once physical effect across a real crash between effect and ack is
irreducible (mitigated by §12). A non-monotonic clock can delay recovery or
cause a detected steal (bounded). Multi-node is unsupported. `max_claims` can
abandon an environmentally-crash-looping but recoverable job (tune
`max_claims >> max_attempts`; surface `Dead{Abandoned}`). A claimed row whose
payload no longer deserializes is skipped at claim time (logged), not silently
dropped.

[`EventSourced`]: ../crates/event-sorcery/src/lib.rs
[`Projection`]: ../crates/event-sorcery/src/projection.rs
