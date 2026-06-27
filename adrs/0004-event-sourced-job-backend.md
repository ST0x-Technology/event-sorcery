# ADR-0004: Own the durable-job backend as an event-sourced apalis Backend

## Status

Proposed. The direction (own the backend; jobs are event-sourced) is agreed;
this records the detailed design for review before any branch is reshaped.

## Context

ADR-0001 replaced cqrs-es `Services` with durable, retryable jobs so command
handlers stay pure `(state, command) -> Vec<Event>` and a job is enqueued iff
its triggering events commit. The point of coupling jobs to cqrs/es was to get
that atomicity and durability **from the event store itself**.

The current implementation does not. It uses apalis-sqlite (apalis's own
`SqliteStorage`) as the durable backend and reaches atomicity by **hand-writing
rows into apalis's internal `Jobs` table** from the event store's connection:

- `insert_pending` (`job.rs:470`) runs
  `INSERT INTO Jobs (job, id, job_type,
  run_at) ...` as raw SQL replicating
  apalis-sqlite's storage format, through the event store's sqlx-0.9 connection,
  inside the event-commit transaction.
- The worker side reads that same table through apalis-sqlite's `SqliteStorage`
  on **sqlx 0.8**.

Consequences visible in review:

- **Two sqlx majors against one table.** The enqueue path (0.9) and the worker
  path (0.8) share the `Jobs` table by coincidence of schema, not contract.
- **Coupling to a churning private schema.** `Cargo.toml` pins
  `apalis-sqlite =
  "=1.0.0-rc.8"` precisely because "apalis's Jobs-table
  schema churns between release". A schema change is a silent data/runtime
  break, not a compile error.
- **Atomic enqueue is a contortion**, described inconsistently across ADR-0001
  (the `JobBackend`-borrows-pool variant breaks atomicity; the task-local-buffer
  variant is what actually ships).
- **Empty-event commands silently drop jobs** (HIGH, #8/#15): the job drain
  rides inside cqrs-es `persist_events`, which `Lifecycle::handle` skips when a
  handler returns no events.

apalis-core separates the `Backend` trait (worker loop, polling, retry layer,
circuit breaker) from the storage implementations. Custom backends are a
first-class extension point: `apalis-postgres`, `apalis-redis`, and
`apalis-libsql` are independent backends, and `apalis-libsql` runs on the libsql
driver -- not sqlx -- proving the worker side is decoupled from any specific SQL
driver.

## Decision

Implement `apalis_core::Backend` over an **event-sourced job model owned by
event-sorcery**, and drop the `apalis-sqlite` dependency. apalis keeps the
execution engine (worker loop, concurrency, circuit breaker, `Monitor`,
`build_supervised_worker!`); event-sorcery owns storage, durability, and the job
lifecycle. Job state transitions are **events in the same event store** as the
domain aggregates.

This is the `Lifecycle` pattern one layer up: `Lifecycle<Entity>` adapts
`EventSourced` to cqrs-es's `Aggregate`; a job-lifecycle adapter adapts the
event-sourced job model to apalis's `Backend`.

### Job as an event-sourced entity

Each job instance is an aggregate with its own stream. Events:

- `Enqueued { kind, payload, run_at }`
- `Claimed { worker, lease_until }`
- `Succeeded`
- `Failed { error, attempt }`
- `RetryScheduled { run_at, attempt }`
- `Dead { reason }`

State is the current status, attempt count, payload, and active lease -- the
same shape apalis-sqlite tracks in columns, but reconstructed from events.

### Atomic enqueue (native, no contortion)

Enqueue appends an `Enqueued` event to the job's stream. The existing
per-command `PENDING_JOBS` task-local buffer is reused, but the flush writes
`Enqueued` **events** into the event store inside the same transaction that
commits the triggering aggregate's events. Both writes hit the one `events`
table in one SQLite transaction, so the job is enqueued iff the triggering
events commit -- the ADR-0001 guarantee, now mechanical instead of replicated.

This also **dissolves the empty-event gap**: the job-enqueue is itself an event,
so a "job-only" command is no longer eventless, and the flush is event-sorcery's
own code in its own transaction rather than something gated by cqrs-es
`persist_events`.

### Polling, claiming, and concurrency

A `job_queue` projection (current status + `run_at` per job, indexed) is
maintained from the job events so the `Backend` poll is a cheap indexed query:
jobs whose latest state is `Enqueued`, or `RetryScheduled`/`Claimed` with an
elapsed deadline.

Claiming appends `Claimed { worker, lease_until }` with **optimistic
concurrency** on the job stream: the event store's `(aggregate_id, sequence)`
uniqueness means that under concurrent workers only one `Claimed` append at the
expected sequence wins; losers see the conflict and move to the next candidate.
This is the double-claim safety that apalis-sqlite gives for free and that we
now own.

### Visibility timeout and crash recovery

`Claimed` carries `lease_until`. A job claimed but without a terminal event past
its lease is re-claimable -- a crashed worker's job is picked up by another. The
race (a worker completing just as its lease expires) is resolved by the
expected-version guard on the terminal append.

### Retry ownership

Durable retries are owned by the event-sourced model: on failure, append
`Failed` and, if attempts remain,
`RetryScheduled { run_at = backoff(attempt) }`; the poll re-surfaces it after
`run_at`; on exhaustion, `Dead`. apalis executes once per claim; its in-process
retry layer is disabled to avoid a second, non-durable attempt counter.
Backoff/circuit-breaker policy stays as today.

## Consequences

- **One driver, one version.** Enqueue and worker both go through sqlx 0.9; the
  `apalis-sqlite` dependency and its `=rc.8` pin are removed. Remaining apalis
  trait churn surfaces as compile errors, not silent schema/data drift.
- **Atomicity and the empty-event guarantee become structural**, not patched.
- **Full audit and recovery for free**: a job's entire lifecycle is an event
  stream, queryable and replayable like any other aggregate -- the original
  reason to couple jobs to cqrs/es.
- **We own the hard parts**: atomic claim under concurrency, visibility
  timeouts, durable retry bookkeeping. For a financial library this is a real
  correctness surface (a double-claimed job is a double side effect) and demands
  thorough concurrency, crash-recovery, and retry-exhaustion tests.
  apalis-postgres / apalis-libsql are reference implementations to mirror.
- **More code owned by event-sorcery** -- but code we control, on our schema, in
  our test suite, instead of a pinned private dependency.

## Alternatives considered

- **Keep apalis-sqlite + raw-SQL flush (status quo).** Rejected: the sqlx-major
  split, the pinned churning schema, the inconsistent atomic-enqueue story, and
  the empty-event gap all stem from not owning the table.
- **Own a plain (non-event-sourced) `jobs` table** with status columns, polled
  by a custom `Backend`. Resolves the driver/version/schema coupling but not the
  point of this library: it forgoes the audit/replay/atomic-with-events coupling
  that motivated jobs-on-cqrs/es. Rejected in favor of the event-sourced model.
- **Apply apalis's retry layer for durability.** Rejected: in-process retries
  are not durable across restarts and would double-count attempts against the
  event-sourced record.

## Impact on in-flight work

This reshapes the foundation of the jobs stack, not a fix folded into a branch:

- **ADR-0001** is partially superseded (the `JobBackend`/apalis-sqlite storage
  decision; the `Job` trait and pure-handler shape stand).
- **#11 (jobs-replace-services), #14 (job-enqueue), #15 (jobs-eventsourced-
  reshape)** are built on apalis-sqlite + the raw-SQL flush and would be
  reworked: the `Backend` impl, the job event model + projection, and the
  flush-writes-events change replace `JobBackend`/`insert_pending`.
- Open question for review: land this by **reworking the existing stack** before
  merge, or merge the current stack and land the event-sourced backend as a
  **follow-up** that removes apalis-sqlite. The former keeps history honest; the
  latter unblocks the in-flight PRs sooner.
