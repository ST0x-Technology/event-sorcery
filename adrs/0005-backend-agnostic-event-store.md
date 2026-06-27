# ADR-0005: Backend-agnostic event store and durable-job backend

## Status

Accepted.

## Context

ADR-0004 replaced `apalis-sqlite` with an event-sourced durable-job backend: a
job is its own event stream in the same event store as every other aggregate,
and an `apalis_core::Backend` (`EventStoreBackend`) polls a `job_queue`
projection, claims jobs by a compare-and-swap event append, and durably records
each outcome.

A side effect of that change is that **nothing about the job backend is
fundamentally tied to SQLite anymore** — it is event sourcing over a relational
store. The same is true of `event-sorcery`'s core: it wraps `cqrs-es`, whose
`PersistedEventRepository` is the backend abstraction, and which already has
`sqlite-es`, `postgres-es`, and `mysql-es` implementations. Yet `event-sorcery`
hardwires `SqliteEventRepository` + `SqlitePool` throughout (`wire.rs`,
`lib.rs`, the job module).

Two facts shape the design:

1. **`cqrs_es::persist::PersistedEventRepository` is aggregate-agnostic** — it
   is `Send + Sync` and every method is generic `<A: Aggregate>`. So the core
   needs no new trait and no higher-kinded type to generalize over it: a plain
   type parameter `R: PersistedEventRepository` suffices.
2. **The job backend needs operations cqrs-es does not provide** — the
   `job_queue` projection, an append-at-expected-sequence compare-and-swap whose
   conflict is observable (not a hard error), `BEGIN IMMEDIATE`/`FOR UPDATE`
   write locking, a lease-renewal `UPDATE`, and a runnable-candidate poll. These
   are SQL-dialect-specific and must be abstracted behind a trait with one
   implementation per backend.

Rust lacks higher-kinded types, so a backend cannot be passed as an
unparametrized type constructor and then saturated. This codebase already
resolves that for view storage with the **`ViewBackend` GAT**
(`view_backend.rs`) — a backend trait whose associated type supplies the
concrete repository. The new abstraction follows that _idiom_ (a backend trait
naming concrete capabilities as associated types), but needs **no GAT**: the job
aggregate is mono-typed (`aggregate_type = "job"`) and a pooled
connection/transaction is owned and `'static`, so plain associated types are
enough.

## Decision

Introduce two traits. One backend struct (`SqliteBackend`, the default)
implements both; the SQL is the current job-backend code moved verbatim.

### `JobStore` — per-backend durable-job storage (what the worker uses)

Names the operations cqrs-es does not cover. Every method returns
`impl Future + Send` (RPITIT, **not** `async fn`/`#[async_trait]`): the worker
boxes the poll stream (`BoxStream<'static>`) and `tokio::spawn`s the renewal,
both of which require `Send` futures.

```rust
pub trait JobStore: Clone + Send + Sync + 'static {
    type Connection: Send;                                 // SqliteConnection / PgConnection
    type Tx: DerefMut<Target = Self::Connection> + Send;   // owned guard, no lifetime -> no GAT
    type Error: std::error::Error + Send + Sync + 'static; // per-backend enum

    // write-locked claim transaction
    fn begin_claim(&self) -> impl Future<Output = Result<Self::Tx, Self::Error>> + Send;
    fn commit(&self, tx: Self::Tx) -> impl Future<Output = Result<(), Self::Error>> + Send;
    fn rollback(&self, tx: Self::Tx) -> impl Future<Output = ()> + Send;
    fn acquire(&self) -> impl Future<Output = Result<Self::Connection, Self::Error>> + Send;

    // CAS append + projection (on a tx or an acquired connection)
    fn append_event(&self, conn: &mut Self::Connection, event: &SerializedEvent)
        -> impl Future<Output = Result<CasOutcome, Self::Error>> + Send;
    fn read_head(&self, conn: &mut Self::Connection, job_id: &str)
        -> impl Future<Output = Result<Option<QueueRow>, Self::Error>> + Send;
    fn upsert_row(&self, conn: &mut Self::Connection, job_id: &str, row: &JobRow)
        -> impl Future<Output = Result<(), Self::Error>> + Send;
    fn delete_row(&self, conn: &mut Self::Connection, job_id: &str)
        -> impl Future<Output = Result<(), Self::Error>> + Send;

    // snapshot reads / lease renewal (on the pool)
    fn fetch_candidates(&self, kind: &str, now_ms: i64, scan_limit: i64)
        -> impl Future<Output = Result<Vec<Candidate>, Self::Error>> + Send;
    fn renew_lease(&self, job_id: &str, claim_seq: i64, new_lease_until_ms: i64)
        -> impl Future<Output = Result<LeaseRenewal, Self::Error>> + Send;
    fn load_enqueued_payload(&self, job_id: &str)
        -> impl Future<Output = Result<serde_json::Value, Self::Error>> + Send;

    fn classify(error: &Self::Error) -> Severity;

    // atomic enqueue flush -- DEFAULT impl over append_event + upsert_row, so no
    // backend reimplements it and external backends never build pub(crate) JobEvent.
    fn flush_pending(&self, conn: &mut Self::Connection, pending: &[EnqueueRequest])
        -> impl Future<Output = Result<(), EnqueueError<Self::Error>>> + Send { /* default */ }
}
```

### `EventBackend: JobStore` — adds the flush-aware event repository (what the core uses)

```rust
pub trait EventBackend: JobStore {
    type EventRepo: PersistedEventRepository + Send + Sync + 'static;
    fn event_repo(&self, compaction_policy: CompactionPolicy) -> Self::EventRepo;
    fn migrate(&self) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
```

The supertrait edge couples the two so the `job_queue` the worker polls and the
events the enqueue flush appends agree on one backend/schema. The core obtains
its repository **only** via `event_repo()` — a consumer hands the core a
_backend_, never a bare repository, so a non-flushing repository (one that would
silently drop the atomic-enqueue guarantee of ADR-0001) is unrepresentable.

### Backend-neutral types and the conflict/severity contract

The public traits carry primitives/`String`, never the `pub(crate)` domain
newtypes (`JobKind`/`WorkerId`/`JobEvent`/`JobState` stay `pub(crate)`; the
crate converts at the boundary). The two backend-neutral control-flow channels:

- **`CasOutcome { Committed, Conflict }`** — a CAS conflict is an _expected
  result on the Ok channel_, never an error (a stream `Err` stops the worker).
  `Conflict` is the engine-neutral image of a UNIQUE violation on
  `events (aggregate_type, aggregate_id, sequence)` (SQLite) / `23505` or
  `40001` (Postgres). The UNIQUE index — not the isolation level — is the
  arbiter, so a plain `BEGIN` + the index works on every engine.
- **`Severity { Transient, Fatal }`** via `classify(&Error)` — the worker
  branches on a neutral classification (claim: skip; ack: retry within lease;
  poll: idle) instead of decoding driver codes.

Plus `LeaseRenewal { Held, Lost }`, `QueueStatus { Pending, Claimed }`,
`Candidate`, `QueueRow`, `JobRow`, `EnqueueRequest`.

### Generic wiring with SQLite defaults

```rust
pub struct EventStoreBackend<J: Job, Q: JobStore = SqliteBackend> { /* ... */ }
pub struct Store<Entity: EventSourced, B: EventBackend = SqliteBackend> { /* ... */ }
pub struct StoreBuilder<Entity, B: EventBackend = SqliteBackend,
                        Materialized = <Entity as EventSourced>::Materialized> { /* ... */ }
```

The `SqliteBackend` defaults keep every existing call site
(`StoreBuilder::<E>::new(pool)`, `EventStoreBackend::<J>::new(pool, ...)`)
compiling and behaving identically.

## Scope boundary

"Generic over any cqrs-es backend" currently means the **event store + durable
jobs + `Nil`-entity wiring**. Out of scope (stays SQLite-bound, tracked
follow-up):

- The `Materialized = Table` build path — its `build()` uses
  `Projection::sqlite`, `Reconciler::new`, and `rebuild_all`/`catch_up`, all raw
  `SqlitePool` SQL.
- Materialized-view reconciliation in `projection.rs`.
- The `lib.rs` maintenance/enumeration free functions (`compact_events`,
  `vacuum`, `load_all_ids`, `send_command`, etc.) on `&SqlitePool`.

Generalizing those is a larger, separate effort (it requires lifting view
recovery off `SqlitePool`) and is deferred to the Postgres PR.

## Consequences

- **No behavior change on SQLite.** The SQL moves verbatim behind trait methods;
  the ack stays autocommit (two statements, not a transaction); the claim stays
  `BEGIN IMMEDIATE`. The 7 worker tests + the enqueue/repository tests stay
  green.
- **Postgres/MySQL can plug in** by implementing the two traits: only
  `begin_claim`, `read_head`'s `FOR UPDATE`, the `23505`/`40001` mapping, `$n`
  placeholders, `EXCLUDED` upsert, and `classify` differ; the entire
  claim/ack/budget/fence/lease/poll state machine in `EventStoreBackend` is
  reused.
- **Migrations** stay flat at `migrations/` for now; when Postgres lands they
  split into `migrations/{sqlite,postgres}/` and every `sqlx::migrate!` site
  (~12, incl. `sqlite-es`'s test helper) is repointed atomically. Consumers
  apply via `backend.migrate()`, never picking a directory.

### Residual risks (verify at build time)

1. **RPITIT `+ Send` on every trait method** — omitting it surfaces as an
   inscrutable "Stream is not Send" far from the method. Do not use `async fn`.
2. **SQLite `Tx` drop-safety** — a `SqliteTx` (a pooled connection + manual
   `BEGIN IMMEDIATE`) dropped without `commit`/`rollback` leaks an open
   transaction (sqlx does not track manual `BEGIN`s). The generic claim path
   stays `?`-free between `begin_claim` and `commit`/`rollback` (it already is —
   a `ClaimOutcome`, never a `Result`, flows to the closer).
3. **`read_head` locking is a documented contract**, not type-enforced; a
   Postgres impl that omits `FOR UPDATE` is silently racy.
4. **Ack stays autocommit** — do not "improve" it to transactional here.
5. **Non-object-safe** (RPITIT) — used only as generic parameters, like
   `ViewBackend`/`PersistedEventRepository`. No `Box<dyn JobStore>`.

## Implementation plan (each step compiles green)

0. Split `job.rs` into a `job/` module (`mod`, `store`, `sqlite`, `backend`).
1. Extract pure es helpers (`take_pending`, `enqueued_serialized_event`,
   `pending_row`) from `flush_pending_jobs`/`enqueue_job`.
2. Define `JobStore` + `EventBackend` + the neutral types; implement
   `SqliteBackend` (SQL verbatim); route `SqliteEventRepository::persist`
   through `take_pending` + `flush_pending`.
3. Generify the worker over `Q: JobStore` (default `SqliteBackend`); rewrite the
   7 tests' helpers to the trait, preserving every assertion.
4. Generify the core over `B: EventBackend` (default `SqliteBackend`); `Nil`
   build path generic, `Table` build path stays `SqliteBackend`.
5. Add a mock `JobStore` in `#[cfg(test)]` so the generic bounds are exercised
   without a real DB.
6. (Separate future PR) `PostgresBackend` + the `migrations/{sqlite,postgres}/`
   split; generalize views/maintenance.
