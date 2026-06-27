# Design draft: event-sourced job backend

Status: **Draft** (tracking
[ADR-0004](../adrs/0004-event-sourced-job-backend.md)). This is the detailed
design behind ADR-0004's decision to implement `apalis_core::Backend` over an
event-sourced job model and drop apalis-sqlite. Open questions are called out
inline; nothing here is committed until the ADR is accepted.

## Shape

apalis splits into the **execution engine** (`apalis-core`: the worker loop,
concurrency, retry layer, circuit breaker, `Monitor`) and **storage backends**
(`apalis-sqlite`, `apalis-postgres`, `apalis-libsql`) that implement the
`Backend` trait. We keep the engine and replace the storage with our own
`Backend` impl over the event store. apalis never sees a `Jobs` table; it sees a
stream of claimed jobs we hand it.

```
command handler --push--> JobQueue buffer --flush(Enqueued event)--> events table
                                                   (same tx as aggregate events)
                                                          |
event-store poll/claim <---- job_queue projection <-------+
        |
        v
apalis worker (perform) --ack--> Succeeded | Failed -> RetryScheduled | Dead
```

## Job as an event-sourced aggregate

Each job **instance** is an aggregate (id = a ULID minted at enqueue), with its
own stream. One aggregate per job, not one shared queue stream: a shared stream
serialises every claim through one sequence and does not scale; per-instance
streams give natural isolation and per-job optimistic concurrency.

```rust
enum JobEvent {
    Enqueued { kind: JobKind, payload: serde_json::Value, run_at: Timestamp },
    Claimed { worker: WorkerId, lease_until: Timestamp },
    Succeeded,
    Failed { error: String, attempt: u32 },
    RetryScheduled { run_at: Timestamp, attempt: u32 },
    Dead { reason: DeadReason },
}

enum JobStatus { Pending, Claimed, Done, Retrying, Dead }

struct JobState {
    kind: JobKind,
    payload: serde_json::Value,
    status: JobStatus,
    attempt: u32,
    run_at: Timestamp,
    lease_until: Option<Timestamp>,
}
```

`kind` is the stable `Job::KIND` (already in the codebase) and selects the
worker that runs it. `payload` is the serialized job. State is folded from the
events exactly like any `EventSourced` aggregate.

Open question: `payload` as `serde_json::Value` vs a per-kind typed event. A
typed event per kind is more in keeping with the library but multiplies event
types; a single `Enqueued` carrying an opaque encoded payload keeps the job
aggregate generic. Leaning generic (opaque payload) so the job aggregate is one
type, not one-per-job-kind.

## Enqueue (atomic, reusing the task-local buffer)

The handler-facing `JobQueue::push` stays as it is today: synchronous, buffering
onto the per-command `PENDING_JOBS` task-local. What changes is the **flush**.
Instead of `insert_pending` writing apalis-format rows, the flush appends an
`Enqueued` event to a fresh job stream **through the event store's own
connection, inside the triggering aggregate's commit transaction**.

Because job events live in the same `events` table as domain events, one SQLite
transaction covers both the triggering aggregate's events and the jobs'
`Enqueued` events -- atomic enqueue with no second store and no cross-driver
coordination. This requires the event repository's commit path to append events
for **multiple aggregate ids** in one transaction (the triggering aggregate plus
one new job aggregate per push); the current flush already runs in that
transaction, so this is a change of what it writes, not where.

This also closes the empty-event gap (ADR-0004): the enqueue _is_ an event, so a
command that only schedules work is no longer eventless, and the flush is our
code in our transaction rather than something gated by cqrs-es `persist_events`.

## Polling projection

The `Backend` must cheaply find runnable jobs. A `job_queue` projection,
maintained from `JobEvent`s like any other projection, is the index:

```sql
CREATE TABLE job_queue (
    job_id      TEXT PRIMARY KEY,
    kind        TEXT NOT NULL,
    status      TEXT NOT NULL,
    run_at      INTEGER NOT NULL,
    lease_until INTEGER,
    attempt     INTEGER NOT NULL,
    sequence    INTEGER NOT NULL   -- last job-stream sequence, for the claim CAS
);
CREATE INDEX job_queue_runnable ON job_queue (kind, status, run_at);
```

A job is **runnable** when `status IN ('Pending','Retrying') AND run_at <= now`,
or `status = 'Claimed' AND lease_until < now` (a crashed worker's lease
expired). The poll is
`SELECT ... WHERE kind = ? AND runnable ORDER BY run_at LIMIT n`.

## Claim, lease, and concurrency

The projection is eventually consistent, so it only _nominates_ candidates; the
**claim is authoritative on the job's own stream**. To claim, append
`Claimed { worker, lease_until }` with expected version = the candidate's
`sequence`. The event store's `(aggregate_id, sequence)` uniqueness means that
if two workers race, only one append at the expected sequence succeeds; the
loser gets a version-conflict and moves to the next candidate. This is the
double-claim safety apalis-sqlite gave for free, now expressed in our own append
semantics -- and the reason this must be event-sourced rather than a status
column updated in place.

`lease_until` bounds how long a claim is valid. A worker crash leaves the lease
to expire, after which the job is re-nominated and re-claimable.

Open questions:

- **Lease renewal for long jobs.** A job outliving its lease could be claimed by
  a second worker -> double side effect. Options: a generous fixed lease (>= max
  expected duration), or periodic `Claimed`-renewal appends (which bump the
  sequence and must not race the terminal append). Needs resolution before this
  is safe for financial side effects.
- **Projection lag.** A just-enqueued job is runnable only once the projection
  catches up. Acceptable for async jobs; quantify the lag and whether the
  `Backend` should also tail recent events directly.

## Execution and durable retry

On a successful claim the `Backend` decodes `payload` and hands the job to
apalis's worker, which runs `Job::perform`. The outcome is appended to the job
stream:

- success -> `Succeeded`
- failure with attempts remaining -> `Failed { error, attempt }` then
  `RetryScheduled { run_at = now + backoff(attempt), attempt + 1 }`
- failure with attempts exhausted -> `Failed` then `Dead { reason }`

Retry is **owned by this model**, not apalis's retry layer: apalis executes a
claimed job exactly once, and re-execution comes only from a later poll seeing a
`RetryScheduled` job become runnable. apalis's in-process `RetryPolicy` is
therefore disabled (or set to no retries) to avoid a second, non-durable attempt
counter. Backoff and the circuit breaker (`build_supervised_worker!`) stay as
they are today.

## Worker wiring

`build_supervised_worker!` keeps its shape but `.backend(...)` takes our
`EventStoreBackend<J>` instead of `JobBackend<J>` (apalis-sqlite storage). The
backend needs the event-store pool (sqlx 0.9), the `job_queue` projection
handle, and the job `KIND` it serves. `Job::Input` / `Job::perform` are
unchanged, so consumer job definitions do not move.

Open question: the exact `apalis_core::Backend` surface to implement (the poll
`Stream<Request>`, the `Poller`/heartbeat, the layer stack). Mirror
`apalis-postgres` / `apalis-libsql`, which implement the same trait over a SQL
store; `apalis-libsql` does it on a non-sqlx driver, confirming the trait is
storage-agnostic.

## What gets removed

- `apalis-sqlite` dependency and the `=rc.8` pin (`Cargo.toml`).
- `JobBackend`, `Storage<J>` (the apalis `SqliteStorage` alias),
  `insert_pending`'s apalis-format raw SQL (`job.rs`).
- The sqlx-0.8 surface entirely; enqueue and worker both run on sqlx 0.9.

## Build order (follow-up stack)

1. Job aggregate (`JobEvent`/`JobState`) + `job_queue` projection + migration.
2. Flush change: `Enqueued` events in the commit transaction (replaces
   `insert_pending`); multi-aggregate append in one tx.
3. `EventStoreBackend` implementing `apalis_core::Backend` (poll + CAS claim).
4. Execution/ack + durable retry; disable apalis in-process retry.
5. Rewire `build_supervised_worker!`; remove apalis-sqlite + `JobBackend`.
6. Concurrency / crash-recovery / retry-exhaustion tests (the correctness
   surface ADR-0004 calls out).

## Test plan (sketch)

- Atomic enqueue: a failing command commits no `Enqueued` event; a succeeding
  command commits exactly one, in the same tx as its domain events.
- Double-claim: two backends polling the same runnable job -> exactly one
  `Claimed` wins; the other skips.
- Crash recovery: a `Claimed` job with an expired lease is re-claimed and runs
  once to completion.
- Retry: a failing job emits `RetryScheduled`, is not runnable before `run_at`,
  runs again after, and reaches `Dead` after the attempt cap.
- Empty-event command: a command that only `push`es a job still commits the
  `Enqueued` event.
