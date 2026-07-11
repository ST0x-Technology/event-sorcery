# Domain Model

This document is the source of truth for terminology and naming conventions in
the `event-sorcery` codebase. Code names must be consistent with this document.
The first half is a glossary of the CQRS/event-sourcing concepts the library is
built around; the second half codifies the conventions the library imposes on
top.

For an overview of the library itself, see [SPEC.md](../SPEC.md). For usage
patterns, see [docs/cqrs.md](cqrs.md).

## CQRS/ES Glossary

### Event

An immutable record of something that happened in the domain. Past tense,
declarative (`OrderPlaced`, `BalanceCredited`). Once persisted, an event is the
source of truth — it cannot be edited, deleted, or rewritten. To "undo" an event
you emit a compensating event.

In code: a variant of an `Event` enum on an `EventSourced` type, paired with a
`DomainEvent` impl that supplies a stable `event_type()` string and an
`event_version()` for schema versioning.

### Event Store

The append-only log of all events, indexed by
`(aggregate_type,
aggregate_id, sequence)`. The library's event store is the
`events` table in SQLite, accessed through `cqrs-es`'s
`PersistedEventRepository` trait (implemented by `sqlite-es`).

### Aggregate

A consistency boundary: a cluster of state that's loaded, mutated, and persisted
atomically. All events for a single aggregate are totally ordered; events across
different aggregates are not. `Store::send` extends that total order past commit
into reactor/projection _application_: it serializes commands per
`(entity type, aggregate ID)`, so that aggregate's events are also applied to
every reactor and projection in commit order (see
[docs/cqrs.md](cqrs.md#reactors) and ADR-0004) -- previously the store-level
ordering did not extend past commit into dispatch. The lock table backing this
lives on the `Store` itself, so the guarantee holds for commands sent through
the _same_ `Store` instance, not process-wide: two `Store`s over the same entity
would each serialize against their own table and not against each other. Hold
exactly one `Store` per entity, as the single-framework-instance rule already
requires. In `cqrs-es` vocabulary, `trait Aggregate` is the bridge between
domain logic and the event store.

In `event-sorcery`, consumers don't implement `Aggregate` directly. They
implement `EventSourced` on their domain type, and a blanket impl on
`Lifecycle<Entity>` provides `Aggregate`. The aggregate type's stable identifier
is `EventSourced::AGGREGATE_TYPE`.

### Aggregate ID

The strongly-typed identifier for a single aggregate instance
(`EventSourced::Id`). Must be `Display + FromStr` so `cqrs-es` can stringify it
at the storage boundary. Use a newtype, not a raw `String` or `Uuid`, so the
compiler can prevent passing the wrong identifier.

### Command

The input that drives an aggregate's state transition (`PlaceOrder`, `Credit`).
Commands express intent, in imperative form. They produce events or fail; they
never mutate state directly.

`event-sorcery` splits command handling into two methods:

- `EventSourced::initialize(command, services)` — for aggregates that don't yet
  exist. No `&self`, so handlers can't accidentally read state during creation.
- `EventSourced::transition(&self, command, services)` — for live aggregates.
  Receives the domain type, never the wrapping `Lifecycle`.

### Event Application

The pure function that derives new aggregate state from old state plus an event.
Split into:

- `EventSourced::originate(event)` — create initial state from a genesis event.
  Returns `Some(state)` for events that bootstrap the aggregate, `None` for
  events that require existing state.
- `EventSourced::evolve(&self, event)` — derive new state from an event applied
  to existing state. `Ok(Some(new))` on success, `Ok(None)` if the event doesn't
  apply, `Err` for domain failures (overflow, invariant break).

### Projection

A read model derived from events. The library's `Projection<Entity,
Backend>` is
a SQLite-backed materialized view: a denormalized table that mirrors live
aggregate state for fast queries (`load`, `load_all`, `filter`).

A projection is fully derived — it can be dropped and rebuilt from the event log
at any time without data loss.

### View

`cqrs-es` calls the per-aggregate row in a projection's table a "view". The
library uses `View` only inside the `view_backend` GAT bound; consumers work
with `Projection` directly.

### View Backend

Pluggable storage for projections. `trait ViewBackend` is a higher-kinded type
emulation: it supplies, per `(View, Aggregate)` pair, a concrete
`ViewRepository` implementation. The default `SqliteViewBackend` maps every pair
to `SqliteViewRepository`. Tests use bespoke in-memory backends. See
`crates/event-sorcery/src/view_backend.rs`.

### Snapshot

A periodic checkpoint of an aggregate's state, stored separately from the event
log so reload doesn't always replay every event. Snapshots are serialized with a
`snapshot_version` so a schema bump can invalidate them without touching the
event log.

### Compaction

Optional deletion of events at or before a snapshot's sequence number. Trades
replay latency for storage. Per-aggregate `CompactionPolicy`: `Retain` (default,
safe) or `CompactAfterSnapshot`. Compaction never crosses snapshot boundaries.

### Reactor

A side-effect handler keyed off events. Reads from one or more aggregates'
streams and produces effects (commands on other aggregates, external calls).
Retry with exponential backoff is `Projection::react`'s own internal behavior --
covering both optimistic-lock conflicts and transient SQLite busy/busy-snapshot
errors -- not a property of reactors in general. A bespoke reactor opts into an
equivalent retry for transient SQLite busy errors via
`RetryOnBusy`/`IdempotentReactor` or `retry_with_backoff` (see
[docs/cqrs.md](cqrs.md#reactors)); one that does neither still logs and drops
its update on a busy error.

Reactors run inside the per-aggregate lock `Store::send` holds for the duration
of a command (see the Aggregate entry above and ADR-0004). A reactor that calls
`Store::send()` back onto the same `(entity type, aggregate ID)` it is currently
reacting to -- directly, or transitively through a chain of other reactors'
inline-awaited dispatches within the same inline await-chain -- would deadlock
on that held lock; `Store::send` detects this by tracking await-chain ancestry
and fails fast with `LifecycleError::ReentrantCommand` instead of hanging.
Commanding a _different_ aggregate ID, or a different entity type entirely, from
an inline-awaited reactor is the normal reactor-orchestration pattern.

The guard tracks inline await-chain ancestry, not task identity: it rejects `id`
only if a call somewhere up its own chain of awaiters already holds it, so two
sibling `send()` calls to the same aggregate issued via `tokio::join!`/`select!`
from inside a reactor queue on the lock normally instead of one spuriously
failing as reentrant -- provided that aggregate is absent from the ancestor set
they inherited. If the siblings target the very aggregate the reactor is
currently reacting to, that key _is_ in the inherited set, so each is rejected
as reentrant rather than queued. It only catches ancestor self-cycles, so it is
not immune to deadlock in general: two separate, concurrently in-flight command
chains that reference each other (A's reactor commands B while, concurrently,
B's reactor commands A) can still deadlock, since each chain holds one
aggregate's lock while blocking on the other's -- whether the two chains run on
separate tasks or as sibling futures joined/selected within one task. The same
is true if a reactor moves a _same_-aggregate `send()` onto a different task and
awaits it (e.g. `tokio::spawn(...).await`): that spawned task starts with an
empty guard scope of its own and blocks on the mutex the outer task is still
holding, exactly like the cross-aggregate case. Both are inherent to the
per-aggregate ordering guarantee -- avoid bidirectional cross-aggregate command
cycles, and never move a same-aggregate command off-task within a reactor; defer
either kind of follow-up `send()` until after `react()` returns.

The guarantee is a property of `Store::send`, not of the events table. The
crate's own `send_command()` free function calls `cqrs.execute()` directly, with
no lock and no query processors, so it sits outside the guarantee on both
counts: run concurrently against a live `Store` on the same aggregate, it
interleaves with that `Store`'s commands instead of queueing behind them, and
its events never reach that `Store`'s reactors or projections. It is for CLI,
migration, and test contexts where no `Store` for the entity is concurrently
live.

Nested `send()` calls a reactor issues are its own to handle: `ReactorBridge`
only logs when `react()` itself returns `Err`, so a reactor that ignores the
`Result` of an inner `send()` swallows a `ReentrantCommand` rejection silently
and the outer command still reports success. Outer success does not imply the
nested command ran -- propagate the `Result` or handle it explicitly.

### Schema Version

Per-aggregate `u64` (`EventSourced::SCHEMA_VERSION`) bumped whenever the shape
of state, events, or projections changes. Compared against the persisted version
in the `schema_registry` table on startup; mismatches clear stale snapshots and
trigger view rebuilds.

### Service

External dependency injected into command handlers (`EventSourced::Services`).
Used so a handler can produce side-effects (e.g., enqueue work) atomically with
event persistence. `()` when no services are needed.

### Lifecycle

The `pub(crate)` enum that wraps `EventSourced` so it satisfies
`cqrs-es::Aggregate`. Has variants for `Uninitialized`, `Live(Entity)`, and
`Failed { error, last_valid_entity }`. Implementation detail — `Lifecycle` is
not part of the library's public API and must not appear in any public bound.

### Store / StoreBuilder

`Store<Entity>` is the typed front door for sending commands.
`StoreBuilder<Entity>` wires a `Store` together with its projection and reactors
at startup, using a typed-list encoding (`Cons`/`Nil`) so forgetting a wiring
step is a compile error.

## Naming Conventions

### Public API hides cqrs-es names

cqrs-es names (`Aggregate`, `Query`, `View`, `DomainEvent`, `CqrsFramework`) are
deliberately avoided in our public surface. Consumers work with `EventSourced`,
`Store`, `Projection`, `Reactor`. When in doubt, ask: "does the name hint that
the consumer is using cqrs-es directly?" If yes, rename.

### Aggregate type identifier

`EventSourced::AGGREGATE_TYPE` is a stable string written into the `events`
table. **Once set, never change it** — the same string must forever resolve to
the same aggregate. Convention: PascalCase matching the Rust type name
(`"Position"`, `"OffchainOrder"`).

### Event types

`event_type()` returns a stable string identifying the variant. Convention:
`"{EventEnumName}::{Variant}"`. Bump `event_version()` when the variant's
payload shape changes; never edit a shipped variant.

### Projection table name

For entities with `type Materialized = Table`,
`Entity::PROJECTION =
Table("...")` declares the SQLite table name. Convention:
snake_case matching the entity, suffixed `_view` (`"position_view"`,
`"offchain_order_view"`).

### Domain types in errors

Error variants store domain types, not opaque strings. Prefer
`InvalidSymbol(Symbol)` over `InvalidSymbol(String)`. The compiler then prevents
the caller from accidentally formatting the symbol away too early.

### CQRS Aggregate Services

When an aggregate needs to perform side-effects atomically with persistence, the
cqrs-es Service pattern applies. Naming convention used across consumer code:

- **`{Action}er`** — the trait describing the capability (`OrderPlacer`).
- **`{Domain}Service`** — the implementation (`OrderPlacementService`).
- **`{Domain}Manager`** — the orchestration layer that drives commands through
  the framework (`OrderManager`).

This convention is library-imposed, not framework-enforced, but the `cqrs-es`
`Service` parameter on aggregate `handle` is what makes it work.

### Refactoring completeness

When renaming a type, **all** related names must change: variable names,
function names, parameters, test helpers. Zero mentions of the old name may
remain. A type rename without updating the surrounding vocabulary is incomplete
and confusing.
