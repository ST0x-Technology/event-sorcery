# Event Sourcing with event-sorcery

Quick reference for event-sourcing patterns in this codebase. The
`event-sorcery` crate provides the primary interface; cqrs-es is an
implementation detail hidden behind it.

## Core Principle: Events Are Immutable

**Events are the source of truth and can NEVER be changed or deleted.**
Everything else -- entities, commands, projections -- can be freely modified
because they're derived from events.

- **Commands**: Can add, remove, or change freely
- **Entities**: Can restructure, add fields, change logic freely
- **Projections**: Can add, drop, restructure freely (just replay from events)
- **Events**: PERMANENT. Think carefully before adding new event types.

## Architecture

```text
Domain type          Adapter             cqrs-es (hidden)
+--------------+     +----------------+  +------------+
| impl         | --> | Lifecycle      |  | Aggregate  |
| EventSourced |     | (blanket impl) |--| trait      |
+--------------+     +----------------+  +------------+
                            |
                     +------+------+
                     | Store       |
                     | (typed IDs, |
                     |  send())    |
                     +-------------+
```

Consumers implement `EventSourced`. `Lifecycle` bridges to cqrs-es
automatically. `Store` provides type-safe command dispatch with strongly-typed
IDs.

## Implementing a New Entity

### 1. Define the Domain Type

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MyEntity {
    // domain state
}
```

### 2. Define Events and Commands

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MyEntityEvent {
    Created { /* fields */ },
    Updated { /* fields */ },
}

impl DomainEvent for MyEntityEvent {
    fn event_type(&self) -> String { /* e.g., "MyEntityEvent::Created" */ }
    fn event_version(&self) -> String { "1.0".to_string() }
}

pub enum MyEntityCommand {
    Create { /* fields */ },
    Update { /* fields */ },
}
```

### 3. Implement EventSourced

```rust
impl EventSourced for MyEntity {
    type Id = MyEntityId;       // strongly-typed, Display + FromStr
    type Event = MyEntityEvent;
    type Command = MyEntityCommand;
    type Error = Never;         // or a thiserror type
    type Jobs = Nil;            // or jobs![SendEmail, ChargeCard]
    type Materialized = Table;  // Table for projected, Nil for non-projected

    const AGGREGATE_TYPE: &'static str = "MyEntity";
    const PROJECTION: Table = Table("my_entity_view");
    const SCHEMA_VERSION: u64 = 1;

    // Event-side: reconstruct state from events
    fn originate(event: &Self::Event) -> Option<Self> { /* ... */ }
    fn evolve(entity: &Self, event: &Self::Event)
        -> Result<Option<Self>, Self::Error> { /* ... */ }

    // Command-side: sync, pure -- produce events, enqueue jobs
    fn initialize(command: Self::Command, jobs: &mut JobQueue<Self::Jobs>)
        -> Result<Vec<Self::Event>, Self::Error> { /* ... */ }
    fn transition(&self, command: Self::Command, jobs: &mut JobQueue<Self::Jobs>)
        -> Result<Vec<Self::Event>, Self::Error> { /* ... */ }
}
```

**Method naming conventions:**

| Method       | Purpose                                | Theme         |
| ------------ | -------------------------------------- | ------------- |
| `originate`  | Create initial state from first event  | Evolution     |
| `evolve`     | Derive new state from subsequent event | Evolution     |
| `initialize` | Handle command when no state exists    | State machine |
| `transition` | Handle command against existing state  | State machine |

## Key Types

| Type                   | Purpose                                      |
| ---------------------- | -------------------------------------------- |
| `EventSourced`         | Core trait -- implement on domain types      |
| `Store<Entity>`        | Type-safe command dispatch                   |
| `StoreBuilder<Entity>` | Wires reactors/projections, builds Store     |
| `Projection<Entity>`   | Read-side materialized view                  |
| `Reactor<Entity>`      | Event side-effect handler                    |
| `SendError<Entity>`    | Error from `Store::send()`                   |
| `LifecycleError<E>`    | Errors from event application                |
| `Never`                | Error type for infallible entities           |
| `DomainEvent`          | Trait for event serialization (from cqrs-es) |
| `Table`                | Newtype for projection table name            |
| `Nil`                  | Marker for entities without projections      |

## Sending Commands

```rust
let store: Store<Position> = /* built by StoreBuilder */;

let symbol = Symbol::new("AAPL").unwrap();
store.send(&symbol, PositionCommand::AcknowledgeFill { /* ... */ }).await?;
```

`Store::send()` routes based on lifecycle state:

- Uninitialized -> `Entity::initialize`
- Live -> `Entity::transition`
- Failed -> returns the stored error

## Reading State via Projections

Production code reads entity state through `Projection`, never by loading
aggregates directly:

```rust
// Projection is returned by StoreBuilder::build() for Table entities
let (store, projection) = StoreBuilder::<Position>::new(pool)
    .build(())
    .await?;

// Load by typed ID
let position: Option<Position> = projection.load(&symbol).await?;
```

Projections are materialized views stored in SQLite tables (named by
`PROJECTION` constant). `StoreBuilder::build()` automatically creates and wires
projections for entities with `type Materialized = Table`.

### Filtered Queries with Columns

```rust
const STATUS: Column = Column("status");

let pending_orders: Vec<OffchainOrder> = projection
    .load_where(STATUS, "Pending")
    .await?;
```

## Wiring: StoreBuilder

`StoreBuilder` wires reactors to a `Store` at startup and auto-wires projections
based on the entity's `Materialized` type. It uses type-level linked lists
(`Cons`/`Nil`) to ensure all required processors are wired at compile time.

For **projected entities** (`type Materialized = Table`), `build()` returns
`(Arc<Store>, Arc<Projection>)`:

```rust
let (store, projection) = StoreBuilder::<Position>::new(pool)
    .with(rebalancing_trigger)  // wire a reactor
    .build(())
    .await?;
```

For **non-projected entities** (`type Materialized = Nil`), `build()` returns
`Arc<Store>`:

```rust
let store = StoreBuilder::<OnChainTrade>::new(pool)
    .build(())
    .await?;
```

Projections are created and wired automatically - no manual
`Projection::sqlite()` or `.with(projection)` calls needed. This eliminates a
class of bugs where forgetting to wire a projection causes silent data
staleness.

The `QueryManifest` pattern in `conductor/manifest.rs` ensures exhaustive wiring
by destructuring all processors.

## Reactors

Multi-entity event handlers with compile-time exhaustiveness. Declare
dependencies once with `deps!`, then handle each entity in the `.on()` /
`.exhaustive()` chain:

```rust
deps!(RebalancingTrigger, [Position, TokenizedEquityMint]);

#[async_trait]
impl Reactor for RebalancingTrigger {
    type Error = TriggerError;

    async fn react(
        &self,
        event: <Self::Dependencies as EntityList>::Event,
    ) -> Result<(), Self::Error> {
        event
            .on(|symbol, event| async move {
                self.on_position(symbol, event).await
            })
            .on(|id, event| async move {
                self.on_mint(id, event).await
            })
            .exhaustive()
            .await
    }
}
```

Use `.on_with_fallback(handler, fallback)` instead of `.on()` when a handler
needs a recovery path. If the primary handler returns `Err(error)`, the fallback
receives `(error, id, event)` and can reprocess the event from the errored state
(e.g., force-applying a snapshot that the normal path rejected):

```rust
.on_with_fallback(
    |id, event| async move { self.on_snapshot(event).await },
    |error, id, event| async move {
        self.on_snapshot_recovery(error, event).await
    },
)
```

Wire reactors via `Unwired` + `StoreBuilder::wire()`.

## Jobs Pattern

Handlers are sync and pure: they must not call external systems. Side-effecting
work is enqueued as a durable job and runs in a separate worker. The enqueue
commits in the same SQLite transaction as the events that triggered it, so a job
exists iff its events do.

The worker retries `perform` on failure and re-runs it after a crash, so jobs
are delivered **at-least-once** -- `perform` must be idempotent. Carry any dedup
key in the job payload or `Input`.

Declare a job and the entity's job list:

```rust
#[derive(Serialize, Deserialize)]
struct PlaceOrder { order_id: OrderId, quantity: u32 }

impl Job for PlaceOrder {
    type Input = Arc<dyn OrderPlacer>;   // dependency bundle, injected by the worker
    type Output = ();
    type Error = PlaceOrderError;

    const WORKER_NAME: &'static str = "place-order";
    const KIND: &'static str = "PlaceOrder";

    fn label(&self) -> Label { Label(format!("place-order:{}", self.order_id)) }

    async fn perform(&self, placer: &Self::Input) -> Result<(), Self::Error> {
        placer.place_order(self.order_id, self.quantity).await
    }
}
```

```rust
type Jobs = jobs![PlaceOrder];   // Nil if the entity dispatches none
```

Enqueue from a handler through the `jobs` handle. `push` is synchronous and
buffers; the framework flushes the buffer at commit. `push::<J>` only compiles
when `J` is in the entity's `Jobs` list (`HasJob<J>`):

```rust
fn transition(
    &self,
    command: Self::Command,
    jobs: &mut JobQueue<Self::Jobs>,
) -> Result<Vec<Self::Event>, Self::Error> {
    jobs.push(PlaceOrder { order_id: self.id, quantity: command.quantity });
    Ok(vec![MyEvent::OrderRequested { /* ... */ }])
}
```

A queued row is never observable mid-handler -- the push is buffered, not a live
`INSERT`. Reads a handler once did via `Services` (clock, config, idempotency
keys) move into the `Command`; the caller supplies them at dispatch time.

Run the jobs in a worker, wired with the shared retry/backoff/circuit-breaker
policy. `Input` is the only context handed in:

```rust
let placer: Arc<dyn OrderPlacer> = Arc::new(/* ... */);
let backend = JobBackend::<PlaceOrder>::new(&pool);

Monitor::new().register(build_supervised_worker!(
    ::<PlaceOrder>, 0, backend.into_storage(), placer, fail_stop, failure_notify,
));
```

## Schema Versioning

Bump `SCHEMA_VERSION` when the entity's state, event, or projection schema
changes. On startup, the wiring infrastructure (via `StoreBuilder::build()`)
detects version mismatches and automatically clears stale snapshots.

### Adding Optional Fields to Events

When adding a new field to an existing event variant that has a sensible default
(zero, `None`, etc.), use `#[serde(default)]` on the field instead of writing an
upcaster. Old persisted events that lack the field will deserialize with the
default value.

```rust
#[derive(Serialize, Deserialize)]
enum MyEvent {
    Snapshot {
        existing_field: i64,
        #[serde(default)]
        new_optional_field: i64,  // old events -> 0
    },
}
```

**Pitfall**: `#[serde(default)]` only works when the default is semantically
correct for old events. If old events _must_ be distinguished from "field not
present" (e.g., `None` vs `Some(0)`), use `Option<T>` with `#[serde(default)]`
instead. For fields where the default would be misleading, use an upcaster (see
below).

Always bump `SCHEMA_VERSION` after adding the field so view projections are
rebuilt with the new field populated from the first event that carries it.

## Testing

### replay -- reconstruct state from events

```rust
use event_sorcery::replay;

let position = replay::<Position>(vec![
    PositionEvent::Initialized { /* ... */ },
    PositionEvent::FillAcknowledged { /* ... */ },
]).unwrap().unwrap();

assert_eq!(position.net, dec!(100));
```

### TestHarness -- BDD-style command testing

```rust
use event_sorcery::TestHarness;

TestHarness::<Position>::with(())
    .given(vec![PositionEvent::Initialized { /* ... */ }])
    .when(PositionCommand::AcknowledgeFill { /* ... */ })
    .await
    .then_expect_events(&[PositionEvent::FillAcknowledged { /* ... */ }]);
```

### TestStore -- in-memory command dispatch

```rust
use event_sorcery::TestStore;

let store = TestStore::<MyEntity>::new(vec![], ());
store.send(&id, MyCommand::Create { /* ... */ }).await.unwrap();

let entity = store.load(&id).await.unwrap().unwrap();
assert_eq!(entity.field, expected);
```

### test_store -- SQLite-backed store without reactors

```rust
use event_sorcery::test_store;

let store = test_store::<VaultRegistry>(pool.clone(), ());
store.send(&id, command).await.unwrap();
```

Use `test_store` when you need SQLite persistence but don't care about
projections or reactors. If you need projection data visible after commands, use
`StoreBuilder` with the projection wired.

### load_aggregate -- test-only aggregate loading

```rust
use event_sorcery::load_aggregate;

let entity: Option<Position> = load_aggregate::<Position>(pool, &symbol)
    .await.unwrap();
```

Gated behind `#[cfg(test)]` / `feature = "test-support"`. Bypasses the CQRS
framework (no reactors dispatched). Production code reads through `Projection`.

## Event Upcasters

When you MUST change event structure (e.g., adding required fields to existing
events), use upcasters to transform old events to the new format at load time:

```rust
use cqrs_es::persist::{EventUpcaster, SemanticVersionEventUpcaster};

fn upcast_v1_to_v2(mut payload: Value) -> Value {
    payload["new_field"] = json!("default");
    payload
}

pub fn create_my_upcaster() -> Box<dyn EventUpcaster> {
    Box::new(SemanticVersionEventUpcaster::new(
        "MyAggregate::MyEvent",  // event_type to match
        "2.0",                    // target version
        Box::new(upcast_v1_to_v2),
    ))
}
```

Register upcasters on the event store:

```rust
let event_store = PersistedEventStore::new(event_repo)
    .with_upcasters(vec![create_my_upcaster()]);
```

Update `event_version()` in your event enum to return the new version for new
events.

## Forbidden Patterns

- **NEVER write directly to the `events` table** -- use `Store::send()` (or
  `CqrsFramework::execute()` in test code) to emit events through commands
  - **FORBIDDEN**: direct INSERTs, manual sequence number management, any path
    that bypasses the framework
  - **WHY**: direct writes break aggregate consistency, event ordering, and
    violate the CQRS pattern. The framework owns persistence, sequence numbers,
    aggregate loading, and consistency guarantees
- **NEVER query the `events` table with raw SQL** -- use framework APIs
- **NEVER query view tables with raw SQL** -- use `GenericQuery::load()`
- **NEVER modify events** -- they're immutable historical facts
- **NEVER delete retained events** -- only explicit compactable observational
  aggregates may prune pre-snapshot events through event-sorcery compaction
- **NEVER add events you don't need yet** -- YAGNI applies especially to events
- **NEVER implement `Aggregate` directly** -- implement `EventSourced`
- **NEVER construct `Lifecycle` in application code** -- it's an internal
  adapter
- **NEVER call `sqlite_cqrs()` or `CqrsFramework::new()` in production code** --
  use `StoreBuilder`. Direct construction is allowed in test helpers, CLI code,
  and migration code
- **NEVER create multiple `Store<Entity>` for the same entity type** -- each
  entity type must have exactly ONE `Store<Entity>` instance, constructed once
  in `Conductor::start` via `StoreBuilder`, then shared
  - **WHY**: multiple instances cause silent production bugs -- events persist
    but query processors registered on other instances never see them, so views
    and projections go stale without warnings
  - Wire all query processors via `StoreBuilder::wire()` before calling
    `build()`; the builder tracks wired queries at the type level so a missing
    required query is a compile-time error

## cqrs-es / sqlite-es Internals Reference

These details are hidden by event-sorcery but documented here for debugging and
migration authoring.

### sqlite-es Table Schemas

All three tables are created in the `event_store` migration.

**Events table:**

```sql
CREATE TABLE IF NOT EXISTS events (
    aggregate_type TEXT NOT NULL,
    aggregate_id   TEXT NOT NULL,
    sequence       BIGINT NOT NULL,
    event_type     TEXT NOT NULL,
    event_version  TEXT NOT NULL,
    payload        JSON NOT NULL,
    metadata       JSON NOT NULL,
    PRIMARY KEY (aggregate_type, aggregate_id, sequence)
);
```

- **aggregate_type**: From `EventSourced::AGGREGATE_TYPE` (e.g., `"Position"`)
- **aggregate_id**: Caller-provided ID string
- **sequence**: Auto-incremented per aggregate instance (1, 2, 3, ...)
- **event_type**: From `DomainEvent::event_type()` (e.g.,
  `"PositionEvent::Initialized"`)
- **event_version**: From `DomainEvent::event_version()` (e.g., `"1.0"`)
- **payload**: Event serialized via `serde_json::to_value(&event)`
- **metadata**: Arbitrary JSON metadata passed via `execute_with_metadata()`

**NEVER** write to this table directly. Use `CqrsFramework::execute()`.

### Snapshots Table

```sql
CREATE TABLE IF NOT EXISTS snapshots (
    aggregate_type TEXT NOT NULL,
    aggregate_id   TEXT NOT NULL,
    last_sequence  BIGINT NOT NULL,
    snapshot_version BIGINT NOT NULL DEFAULT 0,
    payload        JSON NOT NULL,
    timestamp      TEXT NOT NULL,
    PRIMARY KEY (aggregate_type, aggregate_id)
);
```

- **payload**: Aggregate state serialized via `serde_json::to_value(&aggregate)`
- **last_sequence**: The event sequence at the time of the snapshot
- **snapshot_version**: The cqrs-es snapshot version used to determine the next
  snapshot update
- **timestamp**: ISO 8601 timestamp of when the snapshot was taken

Snapshots are enabled for all aggregates through `StoreBuilder`. They are used
as a replay starting point so hot aggregates do not reload their full event
history on every command.

After changing aggregate struct layout, bump `SCHEMA_VERSION` so startup clears
stale snapshots through the schema reconciler. Manual snapshot deletion is safe
only for retained event streams where the full event history remains available
for replay.

```sql
-- Retained streams only: reset snapshots when full event replay is available.
DELETE FROM snapshots WHERE aggregate_type = 'Mint';
```

Do not manually delete snapshots for aggregates using
`CompactionPolicy::CompactAfterSnapshot`. For compacted streams, the snapshot
may be the only durable pre-snapshot state. Those aggregates need a
snapshot-aware rebuild path or an external source before snapshots can be
discarded.

### Event Compaction

Financial audit aggregates retain every event indefinitely. Observational
aggregates may opt into `CompactionPolicy::CompactAfterSnapshot`, which deletes
events already represented by an aggregate snapshot. This is currently intended
for high-frequency external-state snapshots such as `InventorySnapshot`.

Only compact aggregates when historical events have no long-term audit value and
the aggregate can be reconstructed from the `snapshots` table plus newer events.
Do not enable compaction for projections that must support full rebuild from the
`events` table unless the projection also has a snapshot-aware rebuild path.

### View Tables (Projections)

```sql
CREATE TABLE IF NOT EXISTS my_view (
    view_id TEXT PRIMARY KEY,
    version BIGINT NOT NULL,
    payload JSON NOT NULL
);
```

- **view_id**: The aggregate ID string
- **version**: Event sequence number (used for optimistic locking)
- **payload**: The view serialized via `serde_json::to_value(&view)`

`SqliteViewRepository` stores views with `serde_json::to_value()` and loads them
with `serde_json::from_value()`.

### Lifecycle Serialization in View Payloads

All our aggregates use `Lifecycle<Entity>` (where `Entity: EventSourced`) as
both the aggregate and its own view (via the blanket `View` impl). Serde's
default externally-tagged enum representation means:

- `Lifecycle::Uninitialized` -> `"Uninitialized"`
- `Lifecycle::Live(data)` -> `{"Live": <data>}`
- `Lifecycle::Failed { error, last_valid_state }` -> `{"Failed": {...}}`

When `T` is a **struct** (e.g., `Position`, `OnChainTrade`), `<data>` is a flat
JSON object: `{"Live": {"symbol": "AAPL", "net": "0", ...}}`. JSON paths:
`$.Live.symbol`, `$.Live.net`.

When `T` is an **enum** (e.g., `OffchainOrder`, `UsdcRebalance`), `<data>` is
another tagged enum: `{"Live": {"Pending": {"symbol": "AAPL", ...}}}`. JSON
paths depend on the active variant and are unsuitable for generated columns. Use
`GenericQuery::load()` and deserialize in Rust instead.

### Generated Columns on Views

SQLite generated columns can extract fields from `payload` for indexing and
querying. Only appropriate for **struct-typed views** where the JSON path is
stable:

```sql
CREATE TABLE IF NOT EXISTS position_view (
    view_id TEXT PRIMARY KEY,
    version BIGINT NOT NULL,
    payload JSON NOT NULL,
    symbol TEXT GENERATED ALWAYS AS (
        json_extract(payload, '$.Live.symbol')
    ) STORED
);
```

Generated columns on enum-typed views (the path changes per variant) should be
avoided in favor of using native cqrs-es tooling, e.g.`GenericQuery::load()`.

## Views and GenericQuery

Views are read-optimized projections built from events. **Never query view
tables directly with raw SQL** -- use `GenericQuery`.

For `EventSourced` entities, `Lifecycle<Entity>` has a blanket `View` impl that
delegates to `originate` and `evolve`, so the entity itself serves as its own
view. Use the `SqliteQuery<Entity>` type alias (defined in `event_sourced.rs`)
for the query type:

```rust
use crate::event_sourced::SqliteQuery;

// SqliteQuery<Position> wraps
// GenericQuery<SqliteViewRepository<Lifecycle<Position>,
//     Lifecycle<Position>>>
let query: Arc<SqliteQuery<Position>> = /* built by StoreBuilder */;

// Load view by aggregate ID
let view: Option<Lifecycle<Position>> =
    query.load(&symbol.to_string()).await;
```

For custom views (not the entity itself), implement the `View` trait on the
cqrs-es `Aggregate` type (`Lifecycle<Entity>`):

```rust
impl View<Lifecycle<MyEntity>> for MyCustomView {
    fn update(&mut self, event: &EventEnvelope<Lifecycle<MyEntity>>) {
        match &event.payload {
            MyEvent::Created { .. } => { /* update view */ }
            MyEvent::Updated { .. } => { /* update view */ }
        }
    }
}
```

## Re-projecting Views with QueryReplay

When you add a new view or need to rebuild an existing one from events, use
`QueryReplay`:

```rust
use cqrs_es::persist::QueryReplay;

pub async fn replay_my_view(pool: Pool<Sqlite>) -> Result<(), MyError> {
    let view_repo = Arc::new(SqliteViewRepository::<MyView, MyAggregate>::new(
        pool.clone(),
        "my_view".to_string(),
    ));
    let query = GenericQuery::new(view_repo);
    let event_repo = SqliteEventRepository::new(pool);

    let replay = QueryReplay::new(event_repo, query);
    replay.replay_all().await?;

    Ok(())
}
```

This replays ALL events through the view's `update()` method, rebuilding the
entire view from scratch. It's idempotent - running it multiple times produces
the same result.

**Call replay at startup** to ensure views are up-to-date with any schema
changes.

## Jobs and Crash Safety

The reason handlers enqueue jobs instead of calling out directly: a job's
enqueue and its triggering events commit in one SQLite transaction. There is no
window where a side effect fires but no event records it, and none where an
event commits but the follow-up work is lost.

```rust
fn transition(
    &self,
    command: Self::Command,
    jobs: &mut JobQueue<Self::Jobs>,
) -> Result<Vec<Self::Event>, Self::Error> {
    jobs.push(SettlePayment { id: self.id });   // buffered, not yet persisted
    Ok(vec![MyEvent::PaymentRequested { /* ... */ }])
}
```

`push` only buffers; the framework drains the buffer with a transaction-bound
`INSERT` into the apalis `Jobs` table alongside the event write. On rollback,
neither is visible. A worker (`build_supervised_worker!`) then polls the table
and runs `Job::perform` with retries and a circuit breaker. See
[Jobs Pattern](#jobs-pattern) for the full `Job` impl and worker wiring.

For entities that dispatch no jobs, use `type Jobs = Nil`.

## Testing Aggregates

Prefer the public `TestHarness` for command tests — it hides the cqrs-es
plumbing (sink construction, event collection) behind a BDD-style interface:

```rust
use event_sorcery::testing::TestHarness;

#[tokio::test]
async fn test_my_command() {
    TestHarness::<MyEntity>::with(())
        .given(vec![MyEvent::Created { /* ... */ }])
        .when(MyCommand::Update { /* ... */ })
        .await
        .then_expect_events(&[MyEvent::Updated { /* ... */ }]);
}
```

When testing `Lifecycle` directly (inside this crate), the cqrs-es 0.5
`Aggregate::handle` signature takes `&mut self`, the services, and an
`EventSink`; emitted events are collected from the sink rather than returned:

```rust
use cqrs_es::Aggregate;
use cqrs_es::event_sink::EventSink;

#[tokio::test]
async fn test_my_command() {
    let mut aggregate = Lifecycle::<MyEntity>::default();
    aggregate.apply(MyEvent::Created { /* ... */ });

    let sink = EventSink::default();
    aggregate
        .handle(MyCommand::Update { /* ... */ }, &(), &sink)
        .await
        .unwrap();

    let events = sink.collect().await;
    assert!(matches!(events[0], MyEvent::Updated { .. }));
}
```

Note the cqrs-es 0.5 commit contract: `PersistedEventStore::commit` rebuilds the
snapshot by re-applying the sink's events to the aggregate it received from
`handle`. `Lifecycle::handle` therefore leaves `self` at its pre-command state
(events are routed through a throwaway scratch copy) — tests that call `handle`
directly should assert this invariant where it matters (see the `handle_*` tests
in `lifecycle.rs`).

Two consequences of the scratch-copy design worth knowing:

- **Command-time validation.** The scratch copy is checked after each event is
  applied to it, so a command whose events the entity's own `originate`/`evolve`
  rejects now fails at command time with the root-cause `LifecycleError` and
  nothing is persisted. (Under cqrs-es 0.4 such events were committed and
  poisoned the aggregate on its next load.)
- **Scope of the exactly-once guarantee.** The pre-command-state invariant
  guarantees exactly-once application through the
  `handle -> commit ->
  snapshot-rebuild` sequence of the Lifecycle adapter. It
  does NOT cover the snapshot `last_sequence` bookkeeping in the repositories: a
  multi-event command that crosses a snapshot boundary with `SNAPSHOT_SIZE > 1`
  records a `last_sequence` past the state the snapshot payload actually folded
  (pre-existing upstream limitation -- cqrs-es's persist API does not expose the
  covered sequence), which can drop tail events from rehydrated state. Until
  that is fixed, keep `SNAPSHOT_SIZE = 1` or emit single-event commands for
  snapshotted aggregates.

For view tests, use `View::update()` with `EventEnvelope`:

```rust
#[test]
fn test_view_updates() {
    let mut view = Lifecycle::<MyEntity>::default();
    view.update(&make_envelope("id", 1, MyEvent::Created { /* ... */ }));

    let Lifecycle::Live(entity) = view else {
        panic!("Expected Live state");
    };
    assert_eq!(entity.field, expected_value);
}
```

For error cases, verify the exact `LifecycleError` variant:

```rust
let sink = EventSink::default();
let error = aggregate.handle(command, &(), &sink).await.unwrap_err();
assert!(matches!(error, LifecycleError::Apply(MyError::SpecificVariant)));
```
