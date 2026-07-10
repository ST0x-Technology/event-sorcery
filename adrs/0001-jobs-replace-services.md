# ADR-0001: Replace `EventSourced::Services` with `EventSourced::Jobs`

## Status

Proposed.

## Context

`EventSourced` lets command handlers call external systems via an associated
`type Services`:

```rust
async fn transition(&self, cmd: Self::Command, services: &Self::Services)
    -> Result<Vec<Self::Event>, Self::Error>;
```

The handler is the only place where state transitions are computed _and_ side
effects happen. The handler returns `Vec<Event>`; cqrs-es persists at the end.
Crash between the side effect and the event write = action happened in the
outside world, no event records it, restart cannot suppress a retry, no failure
to react to.

Handlers must become pure `(state, command) -> Vec<Event>`. All side effects
move to durable, retryable apalis-backed jobs whose enqueue commits in the same
SQLite transaction as the events that trigger them.

The shape of the job machinery is not a new design; it already exists, battle-
tested, in the `st0x.liquidity` consumer at `src/conductor/job.rs`:

- `trait Job<Ctx>` with `perform(&self, ctx: &Ctx)`, `WORKER_NAME`, `label()`,
  `Output`, `Error`. (This ADR reshapes `Ctx` into an associated `Input` type on
  the trait — see "`Job` trait" below.)
- `JobQueue<Task>` newtype around
  `apalis_sqlite::SqliteStorage<Task, JsonCodec,
  SqliteFetcher>` with `push`,
  `push_with_delay`, `cancel_all_pending`.
- `build_supervised_worker!` macro that wires a `WorkerBuilder` with retry
  policy + `ExponentialBackoff` + circuit breaker (`FAIL_STOP_RECOVERY_TIMEOUT`)
  - terminal-failure notifier.
- `work::<Ctx, J>` apalis handler (test-support and production variants).
- `FailureInjector` keyed by a `JobKind` enum for end-to-end fault testing.
- `Label` newtype for structured logging.
- Tuned poll-interval config to meet a sub-second pickup SLO.

This ADR lifts that machinery into `event-sorcery`, generalizes the
consumer-specific bits (the `JobKind` enum's variants, anything liquidity-
specific), wires job dispatch into the framework so events and job enqueue
commit atomically, and removes `Services`.

## Decision

### `EventSourced` shape (after)

```rust
pub trait EventSourced: /* existing bounds */ {
    type Id; type Event; type Command; type Error; type Materialized;

    /// Type-level list of job types this entity can dispatch. `Nil`
    /// if none; `jobs![SendEmail, ChargeCard]` for several, so a
    /// command can invoke whichever job(s) it needs. Each member
    /// declares its own `Input`, `Output`, and `Error` via the
    /// [`Job`] trait -- the aggregate carries no context type.
    type Jobs: JobList;

    const AGGREGATE_TYPE: &'static str;
    const PROJECTION: Self::Materialized;
    const SCHEMA_VERSION: u64;

    fn originate(event: &Self::Event) -> Option<Self>;
    fn evolve(entity: &Self, event: &Self::Event) -> Result<Option<Self>, Self::Error>;

    /// Handlers receive a [`JobQueue`] and enqueue jobs through it directly,
    /// mirroring the reference module's `JobQueue::push` ergonomics. Pushes
    /// are buffered on the queue handle and flushed by the framework in the
    /// same SQL transaction that commits the returned events — so jobs are
    /// enqueued iff their triggering events commit, regardless of crash
    /// timing.
    ///
    /// Handlers are sync. Reads they used to do via `Services` (clock,
    /// config, idempotency keys) are now supplied through `Command`.
    fn initialize(
        command: Self::Command,
        jobs: &mut JobQueue<Self::Jobs>,
    ) -> Result<Vec<Self::Event>, Self::Error>;

    fn transition(
        &self,
        command: Self::Command,
        jobs: &mut JobQueue<Self::Jobs>,
    ) -> Result<Vec<Self::Event>, Self::Error>;
}
```

Handlers are sync. `Services` is gone. Reads the handler used to do via services
(clock, config, idempotency keys) move into the `Command` itself — the caller
supplies them at dispatch time.

### `Job` trait — lifted from `conductor::job::Job` into event-sorcery

The reference trait carries its context as a generic parameter (`Job<Ctx>`).
This ADR instead makes the context an associated `Input` type on the trait, so a
`Job` impl is fully self-describing and `EventSourced` does not need to forward
a separate context type.

```rust
pub trait Job: Serialize + DeserializeOwned + Send + 'static {
    /// Dependency bundle injected into `perform`. The consumer's worker
    /// wiring constructs and owns this; the framework only forwards a
    /// reference.
    type Input: Send + Sync + 'static;
    type Output: Send + 'static;
    type Error: std::error::Error + Send + Sync + 'static;

    /// Worker name prefix; registered name becomes `format!("{WORKER_NAME}-{i}")`.
    const WORKER_NAME: &'static str;
    /// Stable identifier for this job kind (used by the failure-injection
    /// registry and structured logs). Distinct from `WORKER_NAME` because
    /// multiple workers can share a kind.
    const KIND: &'static str;
    /// Logged when retries exhaust.
    const TERMINAL_FAILURE_MSG: &'static str = "Job failed after retries";

    fn label(&self) -> Label;
    fn perform(&self, input: &Self::Input)
        -> impl Future<Output = Result<Self::Output, Self::Error>> + Send;
}
```

`perform` is spelled `-> impl Future<..> + Send` rather than `async fn` because
apalis workers require the returned future to be `Send`, which a bare `async fn`
in a trait does not guarantee.

`JOB_KIND` (which references a liquidity-specific `JobKind` enum used by
`FailureInjector`) collapses into the generic `KIND: &'static str` so the
failure-injection registry can key on it without naming consumer variants.

### `JobList` / `HasJob` -- one aggregate, many job types

`type Jobs` is a type-level list built from the existing `Cons`/`Nil` cells --
the same machinery `deps!` uses for reactor dependencies -- with a `jobs!` macro
for sugar:

```rust
pub trait JobList {}
impl JobList for Nil {}
impl<Head: Job, Tail: JobList> JobList for Cons<Head, Tail> {}

/// Membership: `J` is one of the job types in this list.
pub trait HasJob<J: Job>: JobList {}
```

`jobs![A, B]` expands to `Cons<A, Cons<B, Nil>>`, and a `register_jobs!` helper
generates the per-member `HasJob` impls (mirroring `register_entities!`), which
avoids the overlapping blanket impls a recursive `HasJob` would produce.
Consumers invoke it once, at module level, with the same members as the list:

```rust
type Jobs = jobs![SendEmail, ChargeCard];
register_jobs!(SendEmail, ChargeCard);
```

A single-element list needs no impls -- a blanket impl covers `Cons<J, Nil>` --
and `register_jobs!(OnlyJob)` is accepted as a no-op for uniformity.

`JobQueue<Self::Jobs>::push::<J>` is bounded `where Jobs: HasJob<J>`, so a
handler can enqueue a job _only_ when its type is in the aggregate's declared
list -- checked at compile time. A `Nil` list admits no pushes.

### Machinery moved into `event-sorcery`

- `Label`, `JobError`, `QueuePushError`, `ExponentialBackoff`, `RETRY_BACKOFF`,
  `FAIL_STOP_RECOVERY_TIMEOUT`, `build_poll_config`.
- Two related types share the `JobQueue` name in the reference; this ADR splits
  them. `JobQueue<J>` is what the handler sees; `JobBackend<J>` is what the
  worker macro consumes.

```rust
/// Handler-facing job handle. `push` / `push_with_delay` are sync and
/// **buffer** onto the handle; the framework drains the buffer inside the
/// event-commit transaction (see "Atomic enqueue"). Constructed fresh per
/// command by `Store::send` and dropped at the end of the call.
pub struct JobQueue<Jobs: JobList> {
    buffered: Vec<PendingPush>,
    _jobs: PhantomData<Jobs>,
}

/// Type-erased buffered push. The concrete job type is recovered only to
/// write its `job_type` (= `type_name::<J>()`) and JSON payload at flush.
struct PendingPush {
    job: Box<dyn ErasedJob>,
    delay: Option<Duration>,
}

impl<Jobs: JobList> JobQueue<Jobs> {
    pub fn push<J: Job>(&mut self, job: J)
    where
        Jobs: HasJob<J>,
    {
        self.buffered
            .push(PendingPush { job: Box::new(job), delay: None });
    }
    pub fn push_with_delay<J: Job>(&mut self, job: J, delay: Duration)
    where
        Jobs: HasJob<J>,
    {
        self.buffered.push(PendingPush {
            job: Box::new(job),
            delay: Some(delay),
        });
    }
}

/// Apalis-backed storage for a single job type. Built once at startup;
/// owned by the worker side. `Store::send` borrows the pool/storage from
/// this when flushing a `JobQueue` at commit time. Mirrors the reference
/// module's `JobQueue<Task>` surface minus the handler-facing `push`.
pub struct JobBackend<J: Job> {
    storage: apalis_sqlite::SqliteStorage<
        J,
        apalis_codec::json::JsonCodec<apalis_sqlite::CompactType>,
        apalis_sqlite::fetcher::SqliteFetcher,
    >,
}

impl<J: Job> JobBackend<J> {
    pub fn new(pool: &SqlitePool) -> Self { /* SqliteStorage::new_with_config */ }
    pub fn into_storage(self) -> /* SqliteStorage<...> */ { self.storage }
    pub fn pool(&self) -> &SqlitePool { self.storage.pool() }
    pub async fn cancel_all_pending(&self) { /* same query as reference */ }
}
```

- `work::<J>` apalis handler (production + test-support variants); it pulls
  `Data<Arc<J::Input>>` directly, no separate `Ctx` type parameter.
- `on_terminal_failure` event handler.
- `build_supervised_worker!` macro.
- `FailureInjector` made generic over `KIND: &'static str`. The internals (a
  `HashMap` keyed by kind behind one mutex) are invisible to consumers: the
  public surface is `arm(kind)` plus the worker-side hookup, and the type only
  exists under `test-support`.

`event-sorcery` takes a hard dependency on `apalis`, `apalis-sqlite`,
`apalis-core`, `apalis-codec`. No feature flag: this is the queue backend the
library ships.

### Atomic enqueue + event commit

Apalis `SqliteStorage` writes to a `Jobs` table in the same SQLite database the
event store uses. The framework's write path becomes:

1. Construct a fresh, empty `JobQueue<J>` handle bound to the entity's
   `JobBackend<J>`.
2. Load aggregate, route to `initialize`/`transition`, get `Vec<Event>`. The
   handler buffers any pushes onto the `JobQueue` handle (still sync, no I/O).
3. The event repository opens its usual commit transaction.
4. Persist events through the unchanged cqrs-es write path.
5. Drain the buffered pushes and write them into apalis's `Jobs` table through
   the same transaction.
6. Commit. On rollback, neither events nor jobs are visible.

This is why the handler-facing `JobQueue::push` is sync and buffered rather than
the reference module's async direct push: a direct `INSERT INTO Jobs` inside the
handler races with the event commit and reopens the crash-safety hole this ADR
exists to close. Buffering keeps the handler I/O-free and lets the framework own
atomicity.

The crash-safety guarantee requires the buffer to flush inside the same
transaction that writes the events. The implementation threads this through a
per-command task-local buffer rather than passing a `&mut Transaction` through
cqrs-es's `EventStore`: `Store::send` runs the handler inside a
`with_pending_jobs` scope, the `Lifecycle` handler buffers the handler's
`JobQueue` into that scope, and the event repository drains it with a
transaction-bound `INSERT` while committing the events. `persist_events` keeps
its signature -- the repository owns the transaction and does both writes inside
it -- so cqrs-es's `EventStore` interface stays untouched while preserving the
all-or-nothing commit.

The task-local buffer is safe by construction rather than by convention.
Handlers are sync, so they cannot call `Store::send` re-entrantly or spawn a
task that observes the buffer; the buffer is populated by the `Lifecycle`
handler (framework code) after the handler returns, never by consumer code. Each
command future gets a fresh scope, so concurrent commands never share a buffer.
A push outside any scope (a framework bug -- not reachable from consumer code)
is dropped with a warning rather than lost silently.

### Worker wiring

Consumers call
`build_supervised_worker!(::<MyJob>, index, queue, input, fail_stop, failure_notify)`
inside their `Monitor::register` callback:

- `::<MyJob>` — the job type the worker drains.
- `index` — numeric suffix; the worker registers as `{WORKER_NAME}-{index}`.
- `queue` — the job's `JobBackend<MyJob>`, consumed via `into_storage()`.
- `input` — `Arc<MyJob::Input>`, the only context handed in; no separate `Ctx`
  parameter.
- `fail_stop` — the `CircuitBreakerConfig` for the fail-stop breaker.
- `failure_notify` — `Arc<tokio::sync::Notify>` signalled on terminal
  (retries-exhausted) failure; the worker also stops itself.

Under `test-support` an optional trailing `FailureInjector` routes execution
through the fault-testing registry. The macro lives in event-sorcery so every
consumer gets the same retry policy, circuit breaker, and terminal-failure
behavior without re-deriving it.

`Monitor`, `WorkerBuilder`, and friends remain apalis types — event-sorcery
re-exports the minimal subset consumers need.

## Consequences

**Positive**

- Crash safety. Job enqueue and event persistence commit in the same SQLite
  transaction, so a crash can never leave a side effect without the event that
  records it (or vice versa).
- One opinionated queue/worker stack across consumers; the liquidity bot's
  `conductor::job` module collapses to consumer-specific `Job` impls + the
  `Input` struct.
- Sync, pure handlers.

**Negative / migration cost**

- Breaking change across every `impl EventSourced` (workspace + downstream):
  - drop `type Services`, add `type Jobs` (`Nil` if no jobs),
  - remove `async fn` / `&services`; accept `&mut JobQueue<Self::Jobs>`,
  - move external calls into `Job` variants and declare each job's `Input`,
  - return `Vec<Self::Event>` (jobs are pushed through the queue handle).
- `send_command`, `TestHarness::with(services)`, `wire::sqlite_snapshot_cqrs`
  lose their services parameter and gain (where relevant) job-queue plumbing.
- Runtime context moves from handler-time reads to command construction: every
  call site that leaned on `Services` to fetch the clock, config values, or
  idempotency keys on demand now gathers them and puts them on the `Command`
  before dispatch.
- apalis is now part of event-sorcery's public surface for the worker macro and
  `JobQueue` type. Version bumps in apalis are breaking for us.
- `Job` enums need a serde-stability story matching events: add variants, never
  mutate. Renaming a `WORKER_NAME` or `KIND` orphans queued rows.
- Jobs are delivered **at-least-once**: the worker retries `perform` on failure
  and re-runs it after a crash, so every `Job::perform` must be idempotent.
  Consumers carry any dedup key in the job payload or `Input`.

**SPEC.md / docs**

- SPEC.md: replace the `Services` bullet under `EventSourced` with `Jobs`. Add a
  "Job dispatch" subsection under "Behavior" describing the atomic-commit
  semantics (handler receives a buffered `JobQueue<Self::Jobs>`; framework
  flushes the buffer inside the event-commit transaction) and the at-least-once
  delivery contract (`perform` must be idempotent). Note apalis as a
  non-negotiable backend.
- docs/cqrs.md: rewrite the services pattern as the jobs pattern; show the `Job`
  impl, the `&mut JobQueue<Self::Jobs>` handler signature, and worker wiring via
  `build_supervised_worker!`. Call out the buffered-push semantics explicitly so
  consumers don't expect to observe a queued row mid-handler, and the
  at-least-once contract so they make `perform` idempotent.

## Alternatives considered

- **Single `type Job` per aggregate.** Rejected: an aggregate's commands often
  dispatch unrelated side effects (email vs. charge vs. webhook), each wanting
  its own worker, retry policy, and `Input`. One enum-shaped job per aggregate
  couples them and bloats a single worker's `Input`. A type-level `JobList` lets
  each command invoke whichever job(s) it needs -- per-job workers -- without
  losing the compile-time membership guarantee (`HasJob<J>`).
- **Marker `Job` trait, no `perform`, consumer brings the queue.** Rejected:
  this was the previous version of this ADR and it ignored the reference module
  entirely. The library ships the queue _and_ the worker contract.
- **Enqueue from a reactor instead of inside the event-write transaction.**
  Rejected: reintroduces the crash-safety window this whole change exists to
  close.
- **Keep `Services` alongside `Job`.** Rejected: leaves I/O reachable from
  handlers and defeats the invariant.

## Out of scope

- `Input` construction (consumer concern; same as today in `conductor::job`
  tests).
- Schema migrations for the `Jobs` table beyond what apalis ships.
- Multi-queue prioritization across aggregates.
