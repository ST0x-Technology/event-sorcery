# ADR-0006: cqrs-native durable jobs over a single `EventBackend`

## Status

Accepted. Revises ADR-0005 (which introduced the 12-method `JobStore`).

## Context

ADR-0005 made the durable-job worker generic over a `JobStore` trait — 12
per-backend methods (`begin_claim`/`append_event`/`read_head`/`upsert_row`/
`renew_lease`/`fetch_candidates`/…) plus `Tx`/`Conn`/`Connection` associated
types. A consumer wanting a non-SQLite backend had to re-implement the entire
job state machine in SQL.

The goal is stronger: a consumer supplies **only an `EventBackend`** (a cqrs-es
persistence backend) and the durable-jobs capability — claim, run, ack, retry,
dead-letter, the `job_queue`, the worker — is **provided automatically by
event-sorcery's own cqrs/es machinery** over that backend. The job lifecycle is
modeled as an event-sourced aggregate; claim/ack are commands; the `job_queue`
is a projection; the worker drives the commands. The CAS and the ack-fence then
come **from cqrs-es for free**, not from hand-rolled SQL.

**Grounded facts** (verified against cqrs-es 0.5.0 + this repo):

- `CqrsFramework::execute` is **non-retrying**: `load_aggregate` → `handle` →
  `commit` at `last_sequence + 1`; an optimistic-lock conflict at commit
  propagates as `AggregateError::AggregateConflict` (no retry). So two workers
  issuing the same command at the same version produce **exactly one winner**;
  the loser gets a recoverable `AggregateConflict`.
- The conflict chain: events `(aggregate_type, aggregate_id, sequence)` UNIQUE →
  `SqliteAggregateError::OptimisticLock` →
  `PersistenceError::OptimisticLockError` → `AggregateError::AggregateConflict`.
- `ViewRepository::update_view` is an optimistic CAS: `version 0` → INSERT;
  otherwise
  `UPDATE … SET version = ctx.version+1, payload = ? WHERE view_id = ?
  AND version = ctx.version`
  (it writes **only** `version` + `payload`, +1 per applied event).
- The generic `Projection` reactor folds **one** event onto the loaded view per
  dispatch; `catch_up` heals any "event committed, react skipped" gap and runs
  in `build()` **before** any worker polls.

**The lease decision (D1, chosen by the user — "not worth one event"):** lease
renewal stays a **projection-only update**, never an event. This is the one
place that forces a job-shaped backend primitive: because `execute`'s fold reads
the **events** only, it cannot see a projection-only renewed lease, so a pure
`execute(Claim)` would steal a live job. The claim must therefore re-read the
renewed lease **column** inside a write-locked transaction. (The alternative —
lease as a `Heartbeat` event — would make the projection non-load-bearing and
eliminate the primitive entirely, but costs a bounded stream of operational
events per execution; rejected.)

## Decision

### 1. `EventBackend` — exactly two job primitives

`JobStore` (12 methods) and its neutral types (`CasOutcome`/`Severity`/
`LeaseRenewal`/`QueueStatus`/`Candidate`/`QueueRow`/`JobRow`) are **deleted**.

```rust
pub trait EventBackend: Clone + Send + Sync + 'static {
    type EventRepo: PersistedEventRepository + Send + Sync + 'static;
    type Error: std::error::Error + Send + Sync + 'static;

    /// Flush-aware event repository. `persist` flushes buffered `Enqueued` events
    /// AND seeds their `job_queue` rows in the same transaction that commits the
    /// triggering events (atomic enqueue, unchanged).
    fn event_repo(&self, compaction_policy: CompactionPolicy) -> Self::EventRepo;

    /// Apply the canonical events + snapshots + job_queue schema.
    fn migrate(&self) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// THE claim primitive: a GENERIC transactional envelope. In ONE write-locked
    /// transaction (SQLite `BEGIN IMMEDIATE`): re-read the job_queue row, hand the
    /// RAW row to `decide`, enact the decision (CAS-append the event via the events
    /// UNIQUE, write the row). The backend NEVER names `JobEvent`/`JobState`;
    /// `decide` (crate-side `plan_claim`) owns that knowledge.
    fn claim<Decide>(&self, job_id: &str, decide: Decide)
        -> impl Future<Output = Result<ClaimOutcome, Self::Error>> + Send
    where
        Decide: FnOnce(Option<ClaimRead>) -> ClaimDecision + Send;

    /// THE renew primitive (D1, projection-only): bump `lease_until` on the claimed
    /// row, keyed on its `version` (== claim_seq). Zero rows -> `Lost`.
    fn renew(&self, job_id: &str, claim_seq: i64, new_lease_until_ms: i64)
        -> impl Future<Output = Result<LeaseRenewal, Self::Error>> + Send;
}
```

Backend-neutral types (primitives / `SerializedEvent` / `serde_json::Value` only
— never the `pub(crate)` `JobEvent`/`JobState`):

```rust
pub struct ClaimRead { pub version: i64, pub payload: String, pub lease_until_ms: Option<i64> }
pub enum ClaimDecision {
    Claim { event: SerializedEvent, payload: String, lease_until_ms: i64, won: WonClaim },
    Abandon { event: SerializedEvent, payload: String },
    Skip,
}
pub struct WonClaim { pub claim_seq: i64, pub claim_id: String, pub kind: String, pub attempt: u32, pub args: serde_json::Value }
pub enum ClaimOutcome { Won(WonClaim), Abandoned, Contended, Skip }
pub enum LeaseRenewal { Held, Lost }
```

The claim **decision** (fold, D5 budget, build the `Claimed`/`Dead` event,
compute the new `Lifecycle<JobState>` payload) varies with the domain and is
written **once** crate-side in `job::plan_claim`. The claim **transaction**
(begin/re-read/CAS-append/write-row/commit) varies with the engine and is the
only thing a backend implements. `renew` cannot be `update_view` (which bumps
`version` and never writes `lease_until`), so it is a second method — but
carries zero `JobState` knowledge. **Net job-shaped surface: `claim` + `renew`
(2), down from 12.** RPITIT `+ Send` on every method; used only as a generic
bound, never `dyn`.

### 2. The claim primitive — steal & contention closed

`plan_claim(read, worker, now_ms, lease_ms, max_claims)`: deserialize
`Lifecycle<JobState>` from `read.payload`; the **runnable check reads
`read.lease_until_ms` (the renewed column), never a folded event lease** — this
is the entire reason claim is a primitive. `claim_seq = read.version + 1`. If
the folded `claims >= max_claims` → `Abandon` (Dead{Abandoned}); else mint a
**fresh `ClaimId` per attempt**, build `Claimed{worker, claim_id, lease_until}`,
the new `Live(Claimed)` payload (`claims+1`, `claim_id` set), and `Claim`.

SQLite envelope (`?`-free between begin and the closer): `BEGIN IMMEDIATE` →
`SELECT version,payload,lease_until WHERE view_id=?` → `decide` → on `Claim`:
`INSERT events(Claimed@claim_seq)` (UNIQUE violation → `ROLLBACK` →
`Contended`),
`UPDATE job_queue SET version=claim_seq, payload=?, lease_until=? WHERE view_id=?`,
`COMMIT` → `Won`. The row UPDATE needs no version CAS (the `BEGIN IMMEDIATE`
write-lock makes read==write); the **events UNIQUE is the sole claim arbiter**;
`version` is set directly to the event sequence.

- **Live-lease steal is structurally impossible:** `JobState::Claimed` carries
  **no `lease_until` field** (the _event_ keeps it for audit, but `evolve` drops
  it from state), so the live lease is **only** the column. A claimer takes
  `BEGIN IMMEDIATE`, reads the column as committed at lock time (a concurrent
  renew either committed first → sees the fresh lease → skips, or blocks on the
  writer lock). No foldable lease to misread.
- **Contention → one winner:** the second `BEGIN IMMEDIATE` blocks on
  `busy_timeout`, re-reads the committed claimed row → not runnable → `Skip`
  before any append; the events UNIQUE is the backstop.

### 3. View schema, column ownership, version chain

`job_queue` becomes a cqrs-es view table — generated poll columns + **one real
projection-only `lease_until`** (forced: `update_view` writes only
`version`/`payload`, and the lease changes without an event):

```sql
CREATE TABLE job_queue (
  view_id TEXT NOT NULL PRIMARY KEY, version INTEGER NOT NULL, payload TEXT NOT NULL,
  lease_until INTEGER,                          -- REAL, projection-only (claim + renew only)
  kind   TEXT    GENERATED ALWAYS AS (COALESCE(json_extract(payload,'$.Live.Pending.kind'),
                                               json_extract(payload,'$.Live.Claimed.kind'))) STORED,
  status TEXT    GENERATED ALWAYS AS (CASE WHEN json_type(payload,'$.Live.Pending') IS NOT NULL THEN 'pending'
                                           WHEN json_type(payload,'$.Live.Claimed') IS NOT NULL THEN 'claimed'
                                           WHEN json_extract(payload,'$.Live')='Done'           THEN 'done'
                                           WHEN json_type(payload,'$.Live.Dead') IS NOT NULL     THEN 'dead' END) STORED,
  run_at INTEGER GENERATED ALWAYS AS (COALESCE(json_extract(payload,'$.Live.Pending.run_at'),
                                               json_extract(payload,'$.Live.Claimed.run_at'))) STORED
);
CREATE INDEX job_queue_runnable ON job_queue (kind, status, run_at);
CREATE INDEX job_queue_reclaim  ON job_queue (kind, status, lease_until);
```

`JobState` time fields serialize as epoch-millis
(`#[serde(with = "chrono::serde::ts_milliseconds")]`) so `run_at` is an integer
comparable to `now_ms`. The `(status='claimed')=(lease_until IS NOT NULL)` CHECK
is **dropped** (a done/pending row keeps a stale inert lease; a rebuilt claimed
row has NULL — see invariants).

**Column ownership:** `version`/`payload` ← seed, claim, ack-reactor, catch_up
(`= committed event sequence`). `lease_until` ← claim + renew only (NULL on
seed/dead-letter; reactor/catch_up never touch it). Generated columns ← auto.

**Version chain** (`version == max(event sequence)`; only event-committers bump
it): seed inserts `version=1` (Enqueued@1, in the trigger's persist tx); claim
`UPDATE … version=2` (Claimed@2); renew `UPDATE … lease_until WHERE version=2`
(no event → version unchanged); ack `execute(Succeed)` → reactor
`load_with_context` (version=2) → `apply(Succeeded@3)` →
`update_view … version=3 WHERE version=2` (matches; renew never moved version).
The ack reactor never clobbers the lease (its SET is `version,payload` only).
Crash between ack-commit and react is healed by `catch_up` before any poll.

### 4. Renew, ack-fence, poll

- **Renew (D1):**
  `UPDATE job_queue SET lease_until=? WHERE view_id=? AND
  version=? AND status='claimed'`
  (keyed on `version` == claim_seq, which subsumes claim_id: a re-claim or ack
  bumps version → 0 rows → `Lost`). The worker's renew loop sets `lost` on
  `Lost` and **skips the post-perform ack** (no post-ack resurrection).
- **Ack via `execute(JobCommand)`** (`JobState` becomes `EventSourced`,
  `AGGREGATE_TYPE="job"`, `Materialized=Table("job_queue")`,
  `COMPACTION_POLICY=
  Retain`). Commands: `Succeed{claim_id}`,
  `RetrySchedule{claim_id,run_at,
  attempt,error}`,
  `Kill{claim_id,reason,error}`. `transition` fences:
  `JobState::Claimed{claim_id: held}` and `held == cmd.claim_id` → emit the
  outcome event; else `Err(Fenced)` **before any write**. Two fence layers, both
  required: the **state check** (a visible re-claim folds to a different
  claim_id → Fenced) and the **events-UNIQUE backstop** (a re-claim landing
  between `load_aggregate` and commit → `AggregateConflict`). The worker treats
  `Fenced` and `AggregateConflict` identically (never success; a fenced ack of a
  succeeded `perform` raises a DOUBLE-EXECUTION alarm). Claim is **not** a
  command.
- **Poll via generic `IndexedView::find`** (a new capability on the view repo, a
  DNF predicate over generated columns; reusable by any projection, not a job
  method). The job DNF:
  `(kind=? AND status='pending' AND run_at<=now) OR (kind=?
  AND status='claimed' AND lease_until IS NULL) OR (kind=? AND status='claimed'
  AND lease_until<now)`
  ORDER BY run_at LIMIT n. The `lease_until IS NULL` branch makes a rebuilt
  claimed row (rebuild writes NULL lease) reclaimable — sound only because
  claim/renew always write a non-NULL lease atomically with `status='claimed'`.

### 5. The generic worker, avoiding the SQLite-bound `build()`

`JobRuntime<B: EventBackend>` is provisioned once per process from one backend:
one `Arc<Store<JobState, B>>` (ack via `execute`) + one `Projection<JobState>`
(poll via `find`). `JobRuntime::<SqliteBackend>::build(pool)` reuses
`StoreBuilder::<JobState>::with_backend(backend).build(())` wholesale —
`JobState` is just another `Table` entity, so reconcile + catch_up/rebuild +
reactor registration come for free. `build()` stays SQLite-bound (the
`Reconciler` is raw SQLite, per ADR-0005); the worker **type**
`EventStoreBackend<J, B>` is generic over `B`, only its construction is
SQLite-bound — same scope boundary as ADR-0005.

Poll (`stream::unfold`, gated by the mandatory `ConcurrencyLimitLayer`, D3):
`find` → per candidate `backend.claim(id, |read| plan_claim(read, …))` →
`Won(w)` → decode `w.args` into `J` → yield one `Task<J>` with
`JobContext{job_id, kind, claim_seq, claim_id, attempt}`. Ack (`AckService`, D4
`timeout` unchanged): spawn `renew_loop` (`backend.renew`; `Lost` → set `lost`,
stop) → run `perform` under timeout → if `!lost`, build the outcome command
(worker owns `max_attempts`/`backoff`) and `runtime.jobs.send(id, cmd)`
(transient-only retry within lease) → cancel renew after the ack.

## Consequences

- **Deleted:** `job_store.rs` (`JobStore` + neutral types); the hand-rolled
  worker state machine (`try_claim`/`persist_outcome`/`classify`/`ClaimOutcome`/
  …); `job.rs` raw-SQL helpers `append_job_event`/`apply_to_job_queue`/
  `project_event`; `delete_row`/`load_enqueued_payload`/`fetch_candidates`.
- **Two behavior deltas (only):** terminal rows are **retained** (the reactor
  never deletes), so a finished job's `job_queue` row stays as `status='done'`/
  `'dead'` rather than vanishing; the poll's `status IN ('pending','claimed')`
  gate preserves the safety contract (a terminal job is never re-run).
  Follow-up: a generic `prune_terminal_jobs` (deletes the row **and** its events
  together, keyed on the ULID `job_id`'s embedded time, so `catch_up` can't
  resurrect it).
- **Scope boundary (unchanged from ADR-0005):** `build()`/`Reconciler` and view
  recovery stay SQLite-bound; a non-SQLite `EventBackend` is type-expressible
  but not yet constructible until those are lifted off `SqlitePool`.

## Rework plan (down the stack, each PR green)

**PR #31 — generic `IndexedView::find` (additive, isolated).** `Predicate`/
`Conjunction`/`Term`/`Cmp`/`Value`/`Order` + `IndexedView` trait; impl for
`SqliteViewRepository` (parameterized SELECT, reuse `validate_column`) + the two
in-memory test doubles; tighten the `ViewBackend::Repo` GAT bound with
`+ IndexedView`; surface `Projection::find`. No job changes; the JobStore-based
backend keeps running. Green.

**PR #32 — the D1 job rework (stacked on #31).** Order: (1)
`JobState:
EventSourced` +
`ClaimId`/`claims`/`claim_id`/`JobCommand`/`JobError::Fenced` + epoch-ms serde +
the fold arms; (2) migration `job_queue_event_sourced_view` (DROP+recreate with
generated columns + real `lease_until`); (3) delete `JobStore`, define the new
`EventBackend` + neutral types, implement `SqliteBackend::{claim, renew}` +
`job::plan_claim` + the enqueue-seed change; (4) rewrite
`EventStoreBackend<J,B>` + `JobRuntime`; (5) rewrite the 7 worker tests (TTDD)
preserving every assertion except the two terminal-row deltas; (6) follow-up
`prune_terminal_jobs`. Then full-workspace check/nextest/clippy/fmt; update
SPEC.md + replace this ADR's predecessor references.

**Test checklist** (7 tests + D1–D5): claim marks row; contention→one winner; no
re-claim before expiry; expired re-claim w/o attempt bump (folded `claims+1`);
retry→dead at cap (now via real claim+execute; row retained `status='dead'`);
ack fenced by re-claimer (`Fenced`/`AggregateConflict`); claim-budget
dead-letters (folded `claims>=max`, row retained `'dead'`). D1 projection-only
renew; D2 `BEGIN IMMEDIATE` in-txn column re-read (ack reinterprets D2 via the
event-fold + UNIQUE — documented deviation); D3 `ConcurrencyLimitLayer`; D4
`timeout`; D5 folded `claims`.

## Residual risks / invariants to hold

1. **Stale-but-inert lease:** a pending/done/dead row may keep a non-NULL
   `lease_until`; harmless only because the poll's pending branch ignores it and
   the claimed branches gate on `status='claimed'`. Any poll-predicate change
   must preserve this.
2. **NULL-lease-reclaimable:** claim/renew never write NULL on a live claimed
   lease; NULL on a claimed row arises **solely** from rebuild/catch_up and
   means "no live holder → reclaimable." Keep the `lease_until IS NULL` poll
   branch.
3. **Generated-column JSON-path drift** (financial): if `JobState`/`Lifecycle`
   serialization changes, the `json_extract` paths silently go NULL → jobs stall
   silently. Mitigate: bump `JobState::SCHEMA_VERSION` on any serialization
   change (triggers rebuild) + validate `kind`/`status`/`run_at` columns loudly
   at worker startup.
4. **`claim` reads the column lease, never the folded lease** — enforced
   structurally by `JobState::Claimed` having no `lease_until` field. Keep it
   so.
5. **Fence needs both the state-check and the UNIQUE backstop.** **`execute`
   stays non-retrying.** Distinguish `Fenced`/`AggregateConflict` (never
   success, never retry) from `DatabaseConnectionError` (transient, retry within
   lease).
6. **Reactor fires only for acks** (Enqueued/Claimed are raw appends), so it
   never double-writes the seed/claim rows; routing Claimed through `execute`
   would make the reactor and the claim primitive collide — keep claim raw.
7. **One framework per aggregate:** `JobRuntime::build` runs once; every
   job-kind worker shares the one `Store<JobState>` + `Projection<JobState>`,
   filtering `find` by `J::KIND`.
8. **Two `Job`s:** the public `Job` perform-trait is distinct from the
   `EventSourced` `JobState`; `JobState` stays `pub(crate)`; its
   `AGGREGATE_TYPE` resolves to `"job"` so existing streams match.
