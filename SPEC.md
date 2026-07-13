# SPEC.md

System specification for `event-sorcery`. Covers what the library is, what it
guarantees, and the design decisions behind it at a level sufficient to
understand the system without prescribing exact code. For terminology and naming
conventions, see [docs/domain.md](docs/domain.md). For usage examples and
patterns, see [docs/cqrs.md](docs/cqrs.md).

## Background

`cqrs-es` is the de-facto Rust crate for CQRS / event-sourcing. It provides the
basic vocabulary (`Aggregate`, `DomainEvent`, `View`, `Query`, `CqrsFramework`)
and a pluggable persistence layer. In production, it has several sharp edges
that have caused real bugs:

- **Infallible `apply`.** `Aggregate::apply(&mut self, event)` returns nothing.
  Aggregates that need to fail an `apply` (overflow, invariant break) have to
  either panic or stuff the error inside the aggregate state by hand. Every
  aggregate ends up with the same boilerplate.
- **Stringly-typed aggregate IDs.** `cqrs.execute("some-id", cmd)` takes `&str`,
  so passing the wrong ID type compiles fine and fails in production.
- **No schema versioning.** When the shape of an aggregate's state, events, or
  views changes, stale snapshots and views silently mis-deserialize or drift out
  of sync with the event log. There's no built-in way to detect the drift, so
  manual database surgery is required.
- **Flat command handling.** A single `handle` method receives all commands
  regardless of lifecycle state. Implementors hand-match on `(state, command)`
  tuples, making it easy to forget a case or accidentally reference state during
  initialization.

`event-sorcery` is a thin, opinionated layer that closes those gaps without
forking `cqrs-es`. The underlying framework remains the engine; we just present
a safer, more ergonomic public surface.

## Goals

- **Capture invariants in types.** Aggregate identity, lifecycle state, schema
  version — all encoded so the compiler enforces correctness.
- **Make `apply` fallible.** Domain logic that can fail at apply time (overflow,
  invariant violation) returns `Result` and the failure becomes part of the
  persisted lifecycle.
- **Detect schema drift on startup.** Bumping `SCHEMA_VERSION` is enough to
  invalidate stale snapshots/views without touching the database by hand.
- **Don't reinvent persistence.** SQLite-backed event/view repositories live in
  their own crate (`sqlite-es`) so non-event-sorcery consumers can use them too.
- **Stay backend-pluggable.** Projections take a `ViewBackend` parameter
  (defaulting to SQLite) so tests can swap in in-memory storage and future
  callers can add Postgres without touching the projection logic.

## Non-goals

- A distributed event store. The library is built around a single-writer SQLite
  database.
- A general-purpose CQRS framework. We bridge to `cqrs-es` rather than competing
  with it.
- Multi-tenancy at the framework level. Tenancy is the consumer's concern.
- Online schema migrations of the events table. Once an event type is emitted,
  its shape is permanent — version up via `event_version()` and add new variants
  instead.

## Workspace

Two crates, no application binaries:

- **`crates/sqlite-es`** — SQLite implementations of `cqrs-es`'s persistence
  traits (`PersistedEventRepository`, `ViewRepository`, plus the `SqliteCqrs<A>`
  glue type). Standalone. Usable wherever a `cqrs-es` backend is needed.
- **`crates/event-sorcery`** — the higher-level layer on top of `sqlite-es`.
  Owns the `EventSourced` trait, the `Lifecycle` adapter that bridges it to
  `cqrs-es`, the typed `Store`, projections, the schema registry, the reactor,
  and the `ViewBackend` GAT.

The canonical SQLite schema for the event/snapshot tables lives at
`migrations/20251016210348_init.sql` at the workspace root. Tests apply it
in-memory via `sqlite_es::testing::create_test_pool()`. Consumers apply the same
migration in their application database.

## Components

### `EventSourced` trait

The core abstraction consumers implement on a domain type. It collects
everything needed to event-source the type:

- `Id` — strongly-typed aggregate identifier (`Display + FromStr`)
- `Event` — domain event type (must implement `DomainEvent`)
- `Command` — input that drives state transitions
- `Error` — domain failure type (`Never` if everything is infallible)
- `Services` — external dependencies passed into command handlers
- `Materialized` — `Table` if the entity has a SQLite-backed projection, `Nil`
  otherwise
- `AGGREGATE_TYPE` — stable string identifier used in the event store
- `SCHEMA_VERSION` — bumped when the entity, event, or view shape changes

It splits behavior across two pairs:

- Event-side: `originate` creates initial state from a genesis event; `evolve`
  derives new state from subsequent events. Both are fallible.
- Command-side: `initialize` handles a command when no state exists yet;
  `transition` handles a command against existing state. The split prevents
  accidentally reading "current state" while bootstrapping.

### `Lifecycle` adapter

`Lifecycle<Entity>` is the `pub(crate)` enum that bridges `EventSourced` to
`cqrs-es::Aggregate`. It encodes the four states an aggregate can be in:
`Uninitialized`, `Live(Entity)`, `Failed { error, last_valid_entity }`, and
intermediate transitions. A blanket
`impl Aggregate for Lifecycle<E> where
E: EventSourced` ties everything
together.

`Lifecycle` never appears in the public API. Where its presence is forced by
type-system mechanics (e.g., `ViewRepository<Lifecycle<E>, Lifecycle<E>>`), it's
hidden behind a higher-kinded-type emulation — see `ViewBackend`.

### `Store<Entity>`

The typed front door for command dispatch. Takes a strongly-typed `Id` and a
`Command`, routes through `Lifecycle` based on current state, and returns a
typed `SendError<Entity>`. Hides `cqrs-es::CqrsFramework` entirely.

`Store::send` serializes commands per aggregate ID: the full load -> handle ->
commit -> reactor/projection-dispatch cycle of one command completes before the
next command on the _same_ aggregate begins, so reactors and projections observe
events in commit order. Commands to different aggregate IDs still run
concurrently. Two consequences: concurrent same-aggregate commands now queue
rather than one failing with an optimistic-lock error, and a command can wait
behind an in-flight same-aggregate command's slow reactor retries. A reactor
commanding the same `(entity type, aggregate ID)` it is currently reacting to --
directly, or transitively through a chain of other reactors' inline-awaited
dispatches within the same command -- would deadlock against the held lock;
`Store::send` detects this within a single task and fails fast with
`LifecycleError::ReentrantCommand` instead. That guard only catches same-task
inline self-commands, so it does not reach across tasks: two separate,
concurrently in-flight command chains that reference each other (aggregate A's
reactor commands B while B's reactor concurrently commands A) can still
deadlock, and so can a same-aggregate command a reactor moves onto a different
task (e.g. `tokio::spawn(...).await`) -- that spawned task starts with an empty
guard scope of its own, so it blocks on the mutex the spawning task is still
holding. Avoid cross-aggregate cycles, and never move a same-aggregate command
off-task within a reactor; defer such work until after `react()` returns. See
ADR-0004.

The lock table backing this serialization is self-evicting: an aggregate's lock
exists only while a command on it is in flight, so the table's memory is bounded
by how many commands are in flight concurrently, not by how many distinct
aggregate IDs the process has ever commanded. Per-event and per-request ID
schemes (an ID per fill, per order, per API request) are therefore safe. See
ADR-0005.

### `Projection<Entity, Backend>`

The read-side. A SQLite-backed materialized view that consumers query for entity
state. Operations:

- `load(id)` — single entity by ID
- `load_all()` — every live entity
- `filter(column, value)` — typed filter on a generated column
- `catch_up()` — replay any events missed between persistence and view update
  (crash recovery)
- `rebuild(id)` / `rebuild_all()` — re-derive views from scratch

### `Reactor`

Side-effect handler keyed off events. Used for cross-aggregate orchestration
(e.g., one aggregate's event triggers a command on another). Automatic
retry-with-backoff is a property of `Projection` specifically (its own internal
retry loop, covering both optimistic-lock conflicts and transient SQLite
busy/busy-snapshot errors), not of reactors in general. A bespoke `Reactor` that
never opts in still logs and drops its update on a transient SQLite busy error,
exactly as it always has. A reactor can opt into an equivalent
retry-with-backoff for transient SQLite busy/busy-snapshot errors two ways: wrap
it in `RetryOnBusy` (retries the whole `react()` call, gated by implementing the
`IdempotentReactor` marker trait, which is a declaration that `react()` performs
solely idempotent SQLite writes with no side effect that would double-fire on
retry), or call `retry_with_backoff` / `is_retryable_sqlite_busy` directly
around just the write, leaving any prior side effects outside the retry
boundary.

### `SchemaRegistry`

Tracks `(aggregate_type, schema_version)` tuples in a `schema_registry` table.
On startup, the wiring layer compares the persisted version against the current
`SCHEMA_VERSION` constant and, on mismatch, clears stale snapshots and replays
projections from events. No manual database intervention.

### `ViewBackend` (GAT)

A higher-kinded-type emulation that makes `Projection` generic over its storage
backend without leaking `Lifecycle` into any public bound. The default backend
is `SqliteViewBackend`. Custom backends plug in alternative storage (in-memory
for tests, Postgres in the future).

### `StoreBuilder<Entity>`

Wires `Store` + `Projection` + reactors at startup using a typed-list encoding
(`Cons` / `Nil`) to enforce exhaustive wiring at compile time. Forgetting a
projection becomes a type error, not silent data staleness.

## Behavior

### Write path

1. Caller invokes `Store::send(&id, command)`, which acquires a per-aggregate-
   ID lock before proceeding (steps 2-5 all run under that lock; see
   `Store<Entity>` above and ADR-0004).
2. `Store` looks up the aggregate, loads its `Lifecycle`, applies any relevant
   snapshot, replays uncached events.
3. `Lifecycle::handle` routes to `EventSourced::initialize` (no state) or
   `EventSourced::transition` (has state) and produces events.
4. `cqrs-es::CqrsFramework` persists events with monotonic sequence numbers in
   the same SQL transaction as any side-effects implemented via cqrs-es
   `Service`s.
5. Reactors registered on this aggregate are notified, in commit order, before
   the lock is released and `send` returns.

### Read path

Consumers query via `Projection::load(...)`, never by replaying events
themselves. Projections are kept up to date in the same transaction as event
persistence (where possible) or asynchronously via a reactor.

### Schema drift

On startup, `SchemaRegistry::reconcile()` compares the persisted
`schema_version` for every registered aggregate against its current
`SCHEMA_VERSION` constant:

- **Match**: nothing to do.
- **Mismatch**: snapshots are cleared (forces full event replay) and projection
  tables are truncated (rebuilt from events on first read or via `catch_up`).

### Compaction

Per-aggregate `CompactionPolicy` controls whether old events are deleted once
captured by a snapshot:

- `Retain` — events are kept indefinitely. Default; safe.
- `CompactAfterSnapshot` — events at or before the current snapshot sequence may
  be deleted, trading replay latency for storage.

Compaction never deletes events past a snapshot, and snapshots always include
the `last_sequence` they captured so partial replay still works.

## Strictness contract

The library is consumed by financial systems where silent corruption is
catastrophic. Strict invariants:

- **Events are immutable.** Once an event type ships, its shape is permanent.
  Add a new variant or version (`event_version()`); never mutate an existing
  one. Migrations on the `events` table beyond the initial creation are
  forbidden.
- **No direct writes to the events table.** All event emission goes through
  `CqrsFramework::execute` so sequence numbers, ordering, and consistency are
  framework-enforced.
- **Numeric integrity.** Arithmetic in `apply` and projections uses checked
  operations; precision loss surfaces as an error, never a silent truncation.
- **Single framework instance per aggregate** in the consuming application,
  constructed at startup; never per request.
