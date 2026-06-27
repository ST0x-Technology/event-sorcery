# EventStoreBackend — vetted implementation spec (worker side)

Status: **Vetted, ready to implement.** Produced by a 3-design panel +
adversarial concurrency stress-test + synthesis (ultracode). This is the
blueprint for the worker/execute/ack side of the event-sourced durable jobs
(ADR-0004); the enqueue side is built and green on `feat/jobs-replace-services`
(#11). Implement on `feat/job-backend`. Every decision below resolves a specific
adversarial finding; do not "simplify" one away without re-checking the finding
it closes.

## 0. Scope, deployment model, the guarantee

**Hard deployment constraint:** one SQLite database, **one process**, N
in-process worker instances sharing the host wall clock. SQLite single-writer
CAS and `lease_until` comparisons are NOT sound across machines / network FS,
and cross-host skew drives premature lease steals. **Multi-node is out of scope
— document in SPEC.md.** The lease math assumes a roughly-monotonic clock; a
large backward NTP step/sleep can briefly hide a crashed job or expire a live
one (bounded, accepted, documented).

**Guarantees, precisely:**

1. **Exactly-once commit of every lifecycle transition** (Claimed/Succeeded/
   RetryScheduled/Dead), enforced by the events
   `(aggregate_type, aggregate_id,
   sequence)` UNIQUE constraint, surfaced as
   `SqliteAggregateError::OptimisticLock`. The ledger is the financial system of
   record; never double-written.
2. **Single executor while healthy** via a renewed lease; a slow-but-live job is
   never stolen.
3. **At-least-once execution across crashes** (irreducible) -> closed to
   **exactly-once effect** only by the consumer idempotency contract (§12).

## 1. Keystone decisions (each closes a specific adversarial finding)

- **D1 Lease renewal is a projection-only UPDATE, never an event.**
  `UPDATE job_queue SET lease_until=? WHERE job_id=? AND sequence=claim_seq AND
  status='claimed'`.
  The ack CAS target is therefore always `claim_seq+1`, fixed at claim time.
  After the ack (Succeeded deletes the row; RetryScheduled sets it pending at
  `claim_seq+1`), a late renewal tick matches 0 rows and stops -> the **post-ack
  resurrection bug cannot happen**. Also kills heartbeat-event bloat and
  per-renewal writer contention. On a projection rebuild, `lease_until` falls
  back to the last `Claimed` event's lease (conservative; rebuild only happens
  with no live workers).
- **D2 Every worker write (claim/ack) is `BEGIN IMMEDIATE`, re-reads the
  job_queue head in-txn, validates the expected prior, then writes.** The
  event-CAS at the expected sequence is the arbiter; the in-txn re-read prevents
  a stale snapshot from contradicting the stream (fixes the fold-bypass root
  cause).
- **D3 Concurrency bound is a mandatory `ConcurrencyLimit` layer in
  `middleware()`.** apalis `CallAllUnordered` pulls the next stream item only
  after `poll_ready` is `Ready`, and the claim happens inside the stream's
  `poll_next` --> **a job is claimed only when a free execution slot exists**
  (no claim storm, no pre-execution steal). Not a tuning knob.
- **D4 Per-claim execution timeout:**
  `tokio::time::timeout(execution_timeout,
  perform)`; a timeout is `Err` ->
  normal failure path (counts an attempt). Bounds a hung `perform`.
- **D5 Claim-budget ceiling:** a crash-before-ack never increments `attempt`. At
  claim time, claims-so-far `= sequence - 1 - attempt` (stream = 1 Enqueued +
  #Claimed + #RetryScheduled; attempt == #RetryScheduled). If `>= max_claims`,
  dead-letter `Dead{Abandoned}` instead of claiming. No extra query.

## 2. SQLite pool contract (REQUIRED — without it the CAS does not occur)

- `journal_mode = WAL` (poll readers don't block the writer).
- `busy_timeout >= 5000ms` — **critical**: with `busy_timeout=0` the CAS loser
  gets `SQLITE_BUSY` (mapped to `SqliteAggregateError::Connection`) _before_ the
  UNIQUE check, so the "loser sees UNIQUE -> skip" mechanism never fires. With a
  positive timeout the loser blocks, the winner commits, the loser then sees the
  real UNIQUE violation -> `OptimisticLock`.
- `synchronous = FULL` (outcome event fsync-durable before "done").
- `foreign_keys = ON`.

Error classification in claim/ack:

- `Append(OptimisticLock)` -> genuine CAS loss / fence (contended or stolen).
- `Append(Connection(SQLITE_BUSY/transient))` -> transient; retry within lease
  (ack) or skip-candidate (claim). Never "success", never "fence".
- other `Connection`/`Project` -> transient DB error, same handling.

## 3. Types

```rust
// crates/event-sorcery/src/job/backend.rs (split job.rs into a job/ module)
pub struct EventStoreBackend<J: Job> {
    pool: SqlitePool, worker_id: WorkerId, config: JobWorkerConfig,
    clock: Clock, _job: PhantomData<fn() -> J>,
}
#[derive(Clone)] pub struct JobWorkerConfig {
    poll_interval: Duration,   // 250ms idle cadence
    scan_limit: i64,           // 16 candidates/tick
    max_concurrency: usize,    // D3 bound (MANDATORY)
    lease_duration: Duration,  // 30s
    renew_interval: Duration,  // 10s (= lease/3)
    execution_timeout: Duration, // D4
    max_attempts: u32,         // recorded-failure budget
    max_claims: i64,           // D5 lifetime-claim ceiling (>> max_attempts)
    backoff: Backoff, decode_grace: Duration, // §10
}
#[derive(Clone)] pub struct Backoff { base: Duration, factor: f64, cap: Duration, jitter: f64 }
#[derive(Clone)] pub struct Clock(Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>); // injectable for tests
#[derive(Clone, Default)] pub struct JobContext { job_id: String, kind: JobKind, claim_seq: i64, attempt: u32 }
#[derive(Debug, thiserror::Error)] pub enum BackendError { // UNRECOVERABLE only
    #[error("job store error")] Store(#[from] JobStoreError),
    #[error("job poll query failed")] Poll(#[source] sqlx::Error),
}
```

`Backend` impl:
`Args=J; IdType=String; Context=JobContext; Error=BackendError;
Stream=BoxStream<Result<Option<Task<J,JobContext,String>>,BackendError>>;
Beat=BoxStream<Result<(),BackendError>>; Layer=Stack<AckLayer<J>,
ConcurrencyLimitLayer>`
(ConcurrencyLimit OUTER of AckService so `poll_ready` gates the claim while
AckService still observes the inner `perform` Result — **verify the Stack arg
order against apalis worker/mod.rs:386-400 at build time**). `heartbeat` = an
interval ticker mapped to `Ok(())`. `poll` = §5. Handler:
`async fn run<J:Job>(job:J, input:Data<Arc<J::Input>>, id:Data<JobId>) -> Result<J::Output,J::Error> { job.perform(&input).await }`.

## 4. Poll SQL + CAS claim

Candidate query (snapshot, no locks):

```sql
SELECT job_id, run_at, attempt, sequence FROM job_queue
WHERE kind = ?1
  AND ( (status='pending' AND run_at      <= ?2)
     OR (status='claimed' AND lease_until <  ?2) )
ORDER BY run_at ASC LIMIT ?3;   -- ?2 = now_ms = clock().timestamp_millis()
```

`try_claim(cand, now)` in ONE `BEGIN IMMEDIATE` txn: (a) re-read the job_queue
row; gone -> Skip. (b) if `row.sequence != cand.sequence || !runnable(row, now)`
-> rollback, Skip. (c) D5: if `row.sequence - 1 - attempt >= max_claims` ->
append `Dead{Abandoned}` at `sequence+1` + project + commit -> Skip. (d)
`claim_seq =
row.sequence + 1`; CAS-append
`Claimed{worker, lease_until=now+lease_duration}` at `claim_seq`:
`OptimisticLock` -> Contended; `Connection` -> Transient; ok -> continue. (e)
project the claimed row (`project_claim` helper, NOT apply_to_job_queue with
dummy payload). commit -> `Won{claim_seq, kind, attempt,
run_at}`. **Decode the
Enqueued payload AFTER the claim wins**
(`SELECT payload FROM
events WHERE aggregate_type='job' AND aggregate_id=?1 AND sequence=1`),
then `from_value::<J>`.
`runnable(row,now) = (pending && run_at<=now) || (claimed &&
lease_until<now)`.

## 5. poll Stream

`stream::unfold` over a `poll_interval` ticker. Each tick: candidate query, then
`try_claim` per candidate until one `Won` (buffer extra wins), decode, yield one
`Task`. **Every transient DB condition is logged and downgraded to `Ok(None)`**
(a stream `Err` stops the worker — `call_all.rs:455`); only a permanently-closed
pool yields `Err`. Claiming is rate-bounded by D3.

## 6. Middleware ack flow (correctness heart)

`AckLayer<J>` is the backend middleware (inside user middleware, outside
Readiness/Tracker), so `AckService` observes the raw `perform` Result. `call`:
spawn a `renew_loop` (D1 UPDATE-only, fixed claim_seq) bound to the whole op
incl. the ack write; run `perform` under `timeout` (D4); **ack while the lease
is still held, THEN cancel renewal** (cancel AFTER persist, so the lease covers
the ack commit window); if `lost` is set (renewal saw rows==0) skip the ack
(re-claimer owns it). `renew_loop`: every `renew_interval`,
`UPDATE ... WHERE sequence=claim_seq
AND status='claimed'`; `rows==0` -> set
`lost`, stop; transient err -> retry next tick. `persist_outcome`: CAS the
outcome at `claim_seq+1` in a retry-within-lease loop — Ok -> `Succeeded`+delete
row; Err & `attempt+1<max_attempts` ->
`RetryScheduled{run_at=now+backoff(attempt), attempt+1, error}`+pending row;
else `Dead{RetriesExhausted, error}`+delete. `OptimisticLock` on the ack ->
**fenced**: log the alarm (if `result.is_ok()` it's a DOUBLE-EXECUTION RISK),
never clobber the re-claimer. `Connection`/transient -> retry within lease. Wrap
the op in `worker.track(..)` so shutdown drains in-flight acks. `error` field =
Display + source chain (audit data, not an opaque-String error variant).

## 7. Retry / Dead / abandon

`attempt` advances ONLY via `RetryScheduled`. Honest guarantee: **at most
`max_attempts` recorded failures; physical executions may exceed that under a
lease steal** (bounded by `max_claims`). Durable retry only; apalis
`RetryPolicy`/`Reschedule`/`Update` NOT installed (one execution per claim).

## 8. Worker wiring

`WorkerBuilder::new(name).backend(EventStoreBackend::new(pool, worker_id, cfg,
Clock::system())).data(Arc::new(input)).build(run::<MyJob>)`.
NO user retry/timeout layer; **NO buffering layer outside the ack layer** (must
propagate `poll_ready` backpressure). Multi-threaded runtime required (the
spawned renewal must run during a CPU-heavy perform; CPU-bound work uses
`spawn_blocking`).

## 10. Decode failure — rolling-deploy-safe

Decode failure of this KIND's payload is usually version skew, not corruption.
If `now - enqueued_at < decode_grace` -> release the claim (re-queue to pending
without incrementing attempt, bump run_at slightly to avoid a hot loop), warn.
Else -> `Dead{Undecodable}` at `claim_seq+1`, delete row, error. (`enqueued_at`
= the Enqueued event's run_at at sequence 1.) Never silently drop a valid job
mid-deploy; never loop forever on poison.

## 11. Required job.rs / migration additions

1. `WorkerId::new(name) -> format!("{name}:{ulid}")` (process-run-unique).
2. `DeadReason::{Undecodable, Abandoned}` (additive, serde-stable).
3. `project_claim(tx, job_id, kind, run_at, attempt, lease_until, seq)` + reuse
   `apply_to_job_queue` for terminal/pending (avoid dummy `payload: Null`).
4. Claim/ack txns use `BEGIN IMMEDIATE`.
5. Migration `job_queue_reclaim (kind, status, lease_until)` — DONE
   (20260627171021).
6. deps `tokio-util` (CancellationToken), `tokio-stream` (IntervalStream) —
   DONE.

## 12. Consumer idempotency contract (first-class)

Exactly-once _effect_ needs consumer cooperation. Inject `job_id` into `perform`
(`Data<JobId>`) as the external idempotency key. **Key on `job_id` ALONE, never
`job_id+attempt`** (the latter fails to dedup the crash-after-effect duplicate).
The external boundary must be success-equivalent under that key: a re-issue of
an already-succeeded op must replay success (`Ok`), not error. For airtight
effect-once, derive the key from a domain/business key at enqueue (in the
payload). The in-library fenced-ack `error!` is a monitoring alarm, not the
safeguard.

## 13. Test plan (TTDD, deterministic via injected Clock)

**Harness fix FIRST:** `create_test_pool` uses bare `:memory:` where each pooled
connection is a separate DB -> a two-writer CAS race hits two empty DBs and both
"win", making concurrency tests meaningless. Add a concurrency pool: shared DB
(`file:<rand>?mode=memory&cache=shared` or a tempfile), WAL, `busy_timeout>=5s`,
`synchronous=FULL`, `max_connections>=4`, migrations run once.

Cases: 1 single-claim-under-contention (exactly one Won); 2 re-claim only after
expiry; 3 no re-claim before expiry; 4 renewal extends lease without an event
(D1); 5 post-ack renewal is a no-op (Finding #1 regression); 6 ack fence
(re-claim then original ack -> OptimisticLock, alarm); 7 ack covers the commit
window (H1); 8 retry->Dead boundary; 9 backoff run_at; 10 execution timeout
(D4); 11 claim budget (D5); 12 decode skew vs poison (§10); 13 concurrency bound
(D3); 14 SQLITE_BUSY classification (H3); 15 crash mid-run (exactly one
Succeeded, at-least-once exec).

## 14. Residual risks (accepted / documented)

At-least-once physical effect across a real crash between effect and ack is
irreducible (mitigated by §12; consider a `Job::IDEMPOTENT` const the worker
logs loudly if unset). Non-monotonic clock can delay recovery or cause a
detected steal (bounded). Multi-node unsupported (doc-enforced). `max_claims`
can abandon an environmentally-crash-looping but recoverable job (tune
`max_claims >>
max_attempts`; surface `Dead{Abandoned}`). Short `decode_grace`
during a slow rollout could lose valid jobs (set
`decode_grace >> rollout duration`). Verify the `Stack`/`Layer` ordering
(ConcurrencyLimit outer of AckService) at build time.
