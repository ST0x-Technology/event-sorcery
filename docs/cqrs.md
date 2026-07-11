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
#[async_trait]
impl EventSourced for MyEntity {
    type Id = MyEntityId;       // strongly-typed, Display + FromStr
    type Event = MyEntityEvent;
    type Command = MyEntityCommand;
    type Error = Never;         // or a thiserror type
    type Services = ();         // or Arc<dyn SomeService>
    type Materialized = Table;  // Table for projected, Nil for non-projected

    const AGGREGATE_TYPE: &'static str = "MyEntity";
    const PROJECTION: Table = Table("my_entity_view");
    const SCHEMA_VERSION: u64 = 1;

    // Event-side: reconstruct state from events
    fn originate(event: &Self::Event) -> Option<Self> { /* ... */ }
    fn evolve(entity: &Self, event: &Self::Event)
        -> Result<Option<Self>, Self::Error> { /* ... */ }

    // Command-side: process commands to produce events
    async fn initialize(command: Self::Command, services: &Self::Services)
        -> Result<Vec<Self::Event>, Self::Error> { /* ... */ }
    async fn transition(&self, command: Self::Command, services: &Self::Services)
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

### Same-aggregate ordering guarantee and the reentrancy rule

`Store::send` serializes commands per aggregate ID: the whole load -> handle ->
commit -> reactor/projection-dispatch cycle of one command completes before the
next command on the _same_ aggregate begins, so `RebalancingTrigger`-style
reactors and `Projection` observe events in commit order even when a reactor's
dispatch is slow (e.g. retrying under `RetryOnBusy`). See ADR-0004 for the full
rationale.

**Reentrancy rule: a reactor calling `Store::send()` back onto the same
`(entity type, aggregate ID)` it is currently reacting to -- directly, or
transitively through a chain of other reactors' inline-awaited dispatches within
the same inline await-chain -- would deadlock against the held per-aggregate
lock.** `Store::send` detects this via a task-local set of in-flight aggregate
keys, inherited down that await-chain, and fails fast with
`LifecycleError::ReentrantCommand` instead of hanging. Commanding a _different_
aggregate ID -- or a different entity type entirely, as `RebalancingTrigger`
above does -- is the normal reactor-orchestration pattern and is what the guard
is designed to leave alone.

**The guard tracks inline await-chain ancestry, not task identity.** A `send()`
rejects `id` only if a call somewhere up its own chain of awaiters already holds
it. This means two sibling `send()` calls to the same aggregate -- e.g. issued
via `tokio::join!`/`select!` from inside a reactor -- each inherit the same
ancestor snapshot and so neither sees the other's in-flight key: as long as that
aggregate is absent from the snapshot they inherited, they queue on the
per-aggregate lock normally, exactly like two unrelated calls would, rather than
one spuriously failing as reentrant.

Siblings aimed at the aggregate the reactor is _currently reacting to_ are a
different story. That key is already in the inherited snapshot, so `Store::send`
sees it held and rejects each with `LifecycleError::ReentrantCommand` -- they do
not queue. Being siblings buys them nothing: each is an ancestor self-cycle in
its own right, which is exactly what the guard exists to catch.

Two failure modes fall outside the guard entirely:

- **Cross-aggregate cycles.** Two _separate_, concurrently in-flight command
  chains that reference each other -- e.g. aggregate A's reactor commands B
  while, concurrently, B's reactor commands A -- can still deadlock: each chain
  holds its own aggregate's lock while blocking on the other's. This applies
  whether the two chains run on separate tasks or as sibling futures
  joined/selected within one task -- the guard only ever sees a single chain's
  own ancestry, never a sibling's.
- **Same-aggregate commands moved off-task.** A reactor that spawns a task for a
  _same_-aggregate `send()` and awaits it (e.g.
  `tokio::spawn(async move {
  store.send(&id, cmd).await }).await`) also
  escapes the guard: the spawned task starts with an empty task-local scope of
  its own, so it blocks on the per-aggregate mutex the outer task is still
  holding across the spawn-and-await -- the exact same deadlock class as the
  cross-aggregate cycle above.

Both are inherent to holding a lock across dispatch at all, not a gap the guard
can close from inside a single `Store::send` call. Avoid bidirectional
cross-aggregate command cycles between reactors, and never move a same-aggregate
command onto a different task from within a reactor -- defer either kind of
follow-up `send()` (e.g. via a channel/queue drained outside the reacting
command) until after `react()` returns, so it no longer runs under the held
lock.

**Latency:** a command to an aggregate now waits for any in-flight command on
the _same_ aggregate to finish its whole `execute`, including slow reactor
retries (worst case ~4.3s per reacted event under sustained `SQLITE_BUSY`, see
below). Commands on _different_ aggregates are not serialized by this lock --
though they can still be delayed by the shared SQLite busy contention described
below, so "not serialized" is not the same as "unaffected". In exchange,
concurrent same-aggregate commands queue instead of one spuriously failing with
an optimistic-lock error.

**A reactor's nested `send()` is its own to handle.** `ReactorBridge::dispatch`
only logs when `react()` itself returns `Err`, so a reactor that ignores the
`Result` of an inner `send()` swallows a `ReentrantCommand` rejection silently:
the outer command still reports success while the orchestrated command never
ran. Propagate the `Result` (or handle it explicitly) -- outer success does not
imply the nested send happened.

### Retrying on transient SQLite busy errors

`ReactorBridge::dispatch` logs and swallows any `react()` error -- it does not
retry. `Projection` gets its own retry-with-backoff for free (see
`Projection::react`), covering both optimistic-lock conflicts and transient
SQLite busy errors, but a bespoke `Reactor` does not: under SQLite WAL
contention, a `SQLITE_BUSY`/`SQLITE_BUSY_SNAPSHOT` failure logs and drops the
reactor's update unless it opts in.

Opt in by wrapping the reactor in `RetryOnBusy` and implementing the
`IdempotentReactor` marker trait, which declares that `react()` performs solely
SQLite writes with no side effect (HTTP/RPC call, `Store::send` to another
aggregate, message-queue publish) that would double-fire on retry:

```rust
impl IdempotentReactor for MyReactor {}

let store = StoreBuilder::<MyEntity>::new(pool)
    .with(Arc::new(RetryOnBusy { inner: my_reactor }))
    // ...
```

Only implement `IdempotentReactor` for reactors that are provably pure DB writes
end-to-end -- a reactor that orchestrates across aggregates (like
`RebalancingTrigger` above) must not, unless the downstream command handler is
independently confirmed idempotent under re-invocation. For a reactor whose
`react()` has side effects before its write, call `retry_with_backoff` and
`is_retryable_sqlite_busy` directly around just the write instead, leaving the
earlier side effects outside the retry boundary and outside `IdempotentReactor`
entirely. A `react()` that issues two or more separate, non-transactional SQLite
statements is unsafe to mark too: if the first commits and a later one hits
`SQLITE_BUSY`, the retry replays the whole `react()`, re-running the
already-committed statement. A conforming `react()` must be atomic as a whole --
a single statement, a single transaction, or written so replaying it is safe
(upserts, not bare inserts).

A reactor that does neither still logs and drops its update on a busy error,
exactly as before this exists -- opting in is a per-reactor decision, not a
blanket fix.

#### Reactors that call `Store::send`: unwrap the error yourself

A reactor that orchestrates across aggregates is the one case the fail-closed
default leaves unprotected, so it needs the most care. `Store::send` returns
`SendError<Entity>` (= `cqrs_es::AggregateError<LifecycleError<Entity>>`), whose
`DatabaseConnectionError` variant boxes its inner error _without_ wiring it as
`#[source]`. `is_retryable_sqlite_busy` walks `source()` chains, so it cannot
see into that box -- and because `AggregateError<T>` is generic over the
aggregate, it cannot be downcast from generic code either. A busy error raised
inside `Store::send` is therefore sealed, and `is_retryable_sqlite_busy` returns
`false` for it (see `docs/sqlx.md`).

At _your_ call site `Entity` is concrete, so you can open the box yourself.
Classify the error, then retry only the write -- never the whole `react()`,
which would re-fire the preceding side effects:

```rust
use cqrs_es::AggregateError;
use event_sorcery::{
    RETRY_MAX_ATTEMPTS, RETRY_SCHEDULE, SendError, is_retryable_sqlite_busy,
    retry_with_backoff,
};

/// Reaches the busy error that `AggregateError`'s unsourced box hides from the
/// generic classifier.
fn send_error_is_busy(error: &SendError<Position>) -> bool {
    match error {
        AggregateError::DatabaseConnectionError(inner) => {
            is_retryable_sqlite_busy(inner.as_ref())
        }
        // A rejected command, a lost optimistic-lock race, or a corrupt payload
        // is not a transient lock conflict -- retrying re-runs the command.
        AggregateError::UserError(_)
        | AggregateError::AggregateConflict
        | AggregateError::DeserializationError(_)
        | AggregateError::UnexpectedError(_) => false,
    }
}

async fn react(&self, event: /* ... */) -> Result<(), MyReactorError> {
    let quote = self.pricing.fetch_quote().await?; // side effect: outside the retry

    retry_with_backoff(
        RETRY_MAX_ATTEMPTS,
        RETRY_SCHEDULE,
        || self.store.send(position_id.clone(), Rebalance { quote }),
        send_error_is_busy,
    )
    .await?;

    Ok(())
}
```

This reactor must **not** implement `IdempotentReactor`: the retry boundary is
the write, not `react()`, and the HTTP call above it must not double-fire. Note
that `Store::send` is only replay-safe here if the downstream command handler is
idempotent under re-invocation -- retrying a command that appends an event
re-appends it. Confirm that before wrapping the send at all.

**Latency tradeoff:** `CqrsFramework::execute_with_metadata` awaits every
registered reactor's `dispatch()` synchronously before returning to the command
caller, once per event, so a `RetryOnBusy`-wrapped reactor blocks that caller
_per reacted event_ whenever a busy/busy-snapshot conflict occurs -- including
conflicts caused by an unrelated writer committing anywhere in the same database
file, not just writes to the aggregate the command touched. A single command
that emits multiple events dispatches the reactor once per event, so the
worst-case block for that command is the per-event cost multiplied by the event
count.

Since `Store::send` serializes commands per aggregate ID (see "Same-aggregate
ordering guarantee" above), the block is not confined to the caller that
triggered it: any other command queued behind it on the _same_ aggregate also
waits it out before it can even begin.

The per-event cost is **the sleep budget plus up to one `busy_timeout` per
attempt**:

- **Sleep budget: ~4.3s.** The sum of the backoff delays between the 11 attempts
  (see `RETRY_MAX_ATTEMPTS` / `RETRY_SCHEDULE`).
- **Plus up to `busy_timeout` per attempt.** A plain `SQLITE_BUSY` does not
  surface to the application immediately: SQLite first waits out the
  connection's `busy_timeout`, which sqlx defaults to **5s**. Under persistent
  contention each attempt can burn that timeout _before_ returning the error
  that triggers the next retry, so the worst case is on the order of a minute
  per reacted event -- more than 10x the sleep budget alone.

The ~4.3s figure is accurate only for busy errors that surface immediately:
`SQLITE_BUSY_SNAPSHOT` (which `busy_timeout` cannot absorb -- rolling back and
re-attempting is the only fix, and the reason this retry exists) or a connection
configured with `busy_timeout` = 0. Size caller timeouts against the full bound,
not the sleep budget. This is a real behavioral tradeoff (latency vs. lost
updates), not just an idempotency question -- weigh it before wrapping a reactor
in `RetryOnBusy`.

## Services Pattern

Inject external dependencies into command handlers:

```rust
type Services = Arc<dyn OrderPlacer>;

async fn transition(
    &self,
    command: Self::Command,
    services: &Self::Services,
) -> Result<Vec<Self::Event>, Self::Error> {
    let result = services.place_order(/* ... */).await?;
    Ok(vec![MyEvent::OrderPlaced { /* ... */ }])
}
```

Pass services when building the `Store`:

```rust
let store = StoreBuilder::<MyEntity>::new(pool)
    .build(services)
    .await?;
```

For entities that don't need services, use `type Services = ()`.

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

## Services Pattern

Domain types can depend on external services (APIs, blockchain, etc.) via the
`Services` associated type on `EventSourced`:

```rust
#[async_trait]
impl EventSourced for MyEntity {
    type Services = Arc<dyn MyService>;  // or () if none needed

    async fn transition(
        &self,
        command: Self::Command,
        services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        let result = services.do_something().await?;
        Ok(vec![MyEvent::SomethingDone { result }])
    }
    // ...
}
```

Pass services when building the `Store`:

```rust
let services: Arc<dyn MyService> = Arc::new(MyServiceImpl::new());
let store = StoreBuilder::<MyEntity>::new(pool)
    .build(services)
    .await?;
```

For entities that don't need services, use `type Services = ()`.

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
