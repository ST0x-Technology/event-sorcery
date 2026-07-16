# ADR-0004: Per-aggregate command serialization at `Store::send`

## Status

Accepted. Amended by [ADR-0005](0005-bounded-per-aggregate-lock-table.md): the
"No eviction" tradeoff below is superseded -- the lock table now evicts entries
once no in-flight command holds them.

## Context

`CqrsFramework::execute_with_metadata` (cqrs-es 0.5) persists a command's events
transactionally, then sequentially awaits every registered reactor/projection's
`dispatch()` before returning to the caller. `Store::send` is a thin wrapper
around `self.cqrs.execute(...)` that takes `&self`, so two concurrent `send`
calls for the _same_ aggregate ID run concurrently. cqrs-es's optimistic
concurrency check serializes the _commits_ (via the events table's
`(aggregate_type, aggregate_id, sequence)` primary key), but nothing serializes
the full load -> handle -> commit -> dispatch cycle.

This lets a slow dispatch (e.g. `RetryOnBusy` in `reactor.rs`, which retries for
up to ~4.3s on transient `SQLITE_BUSY`) be overtaken by a faster concurrent
command on the same aggregate: command N commits event N and begins a slow
dispatch; command N+1 (which could only have been produced after loading state
that includes committed event N) commits event N+1 and dispatches immediately,
updating reactors/projections before N's dispatch completes. Event N lands after
event N+1 in every reactor and projection, silently corrupting read models built
from the store's real commit order. `SqliteViewRepository::
update_view`'s
`version + 1` counter is an opaque per-write counter, not the event sequence, so
it cannot detect this: applying N+1 then N both look like valid increments.

Because the events table already enforces total order for a single aggregate's
_commits_ (see `docs/domain.md`'s Aggregate entry), the gap is specifically
between commit order and dispatch order -- something the storage layer's
constraint does not reach.

## Decision

Serialize `Store::send` per aggregate ID with a keyed async mutex
(`PerAggregateLocks`), held across the entire `self.cqrs.execute(...)` call.
Concurrent `send` calls for the _same_ aggregate ID queue in acquisition order;
calls for _different_ aggregate IDs proceed concurrently. Because an aggregate
ID belongs to exactly one `Store<Entity>` (the "single framework instance per
aggregate" invariant in SPEC's strictness contract), this yields true
per-aggregate linearization: for any aggregate, command N's events are fully
applied to every reactor and projection before command N+1 even loads.

The lock lives in `lib.rs` next to `Store` (its owning feature), as a private
`PerAggregateLocks` type: an outer
`std::sync::Mutex<HashMap<String,
Arc<tokio::sync::Mutex<()>>>>` that
get-or-inserts a per-aggregate `tokio::sync::Mutex`, clones its `Arc`, drops the
outer guard, then `.lock_owned().await`s the per-aggregate mutex. The returned
`OwnedMutexGuard` is held by the caller across `execute()` and dropped by RAII
-- no manual release, so a cancelled `send` future still releases the guard.

`tokio::sync::Mutex`, not `std::sync::Mutex`, guards the per-aggregate slot
itself, because the guard is held across an `.await` point (`execute()`); a
`std` mutex guard held across `await` blocks the executing worker thread and
trips clippy's `await_holding_lock`. The outer map mutex is `std::sync::Mutex`
because it is only ever held for the synchronous get-or-insert-and-clone.

**No eviction.** The per-aggregate lock map grows monotonically with aggregate-
ID cardinality. This library's consumers have bounded business-entity
cardinality (not per-request keys), so unbounded growth at that scale is an
accepted tradeoff. A `strong_count`-based cleanup was considered and rejected:
the map holds one `Arc` and any in-flight caller holds another, so a correct
eviction threshold is `== 2`, not `<= 1` -- subtle, and adds release-race
surface for no benefit at this cardinality. Revisit with a dedicated design if
growth ever becomes real.

> **Superseded by [ADR-0005](0005-bounded-per-aggregate-lock-table.md).** Growth
> became real: the bounded-cardinality premise is false for both production
> consumers, which key their highest-volume aggregates per fill, per order, and
> per API request. The lock table now holds `Weak` handles and evicts dead
> entries. The `strong_count` cleanup was rejected rightly -- the adopted design
> tests lock interest directly and needs no threshold at all.

## Alternatives considered

- **Serialize `ReactorBridge::dispatch` instead of the whole `execute()`.**
  Rejected: on a multi-threaded tokio runtime, after command N's commit future
  resolves there is a scheduling gap before N's task is polled to run its
  post-commit dispatch. During that gap, command N+1 can complete its entire
  load/handle/commit and reach the _same_ bridge-level lock first, inverting
  order. The race is not closed, only narrowed. Serializing the whole
  `execute()` removes it by construction: N+1 cannot even begin loading until
  N's `execute` -- including every reactor and projection dispatch -- has fully
  completed.
- **Sequence-number gating through the `Reactor` trait.** Rejected as invasive:
  `Reactor::react` sees only `(Id, Event)`; the sequence lives on
  `ReactorBridge::dispatch`'s `EventEnvelope`. Plumbing it through
  `Dependent`/`EntityList`/`OneOf`/`HasEntity`/`deps!` would touch the
  `.on()`/`.exhaustive()` ergonomics used by every reactor and example, and
  would still need a `Projection`-specific special case for its opaque view
  version. The `Store::send` lock achieves in-order application without touching
  the reactor event shape.
- **Document-only, optionally with a debug assertion.** Rejected as the primary
  fix: detection after the fact is not the "guarantee" this fix aims for, does
  nothing in release builds, and leaves bespoke raw-write reactors -- the
  worst-case scenario -- with no guard at all.

## Consequences

- **Reentrancy hazard, guarded against within an inline await-chain.** The lock
  is held across `execute()`, and reactors are dispatched inside `execute()`,
  inline and awaited directly (not spawned) -- so a reactor's `react()` is
  polled as part of the _same inline await-chain_ as the outer `execute()` that
  triggered it. If `react()` calls `Store::send()` for the **same**
  `(entity type, aggregate ID)` it is currently reacting to -- directly, or
  transitively through a chain of other reactors' inline-awaited dispatches
  within that same chain -- it would deadlock: `tokio::sync::Mutex` is not
  reentrant. `Store::send` closes this specific case with a task-local set of
  in-flight aggregate keys (`HELD_AGGREGATE_LOCKS` in `lib.rs`), inherited down
  the await-chain via a nested scope per call rather than mutated in place:
  before acquiring the per-aggregate lock, it checks whether the key is already
  held by an ancestor call in its own chain and, if so, fails fast with
  `LifecycleError::ReentrantCommand` instead of blocking. Commanding a
  **different** aggregate (different ID, or a different entity type -- a
  different `Store`, a different lock table) is left alone by this guard; that
  is the normal reactor-orchestration pattern.
- **The guard tracks inline await-chain ancestry, not task identity: it only
  catches ancestor self-cycles, not sibling concurrency.** Because each
  `Store::send` call installs its own nested scope built from a snapshot of what
  it inherited (rather than mutating a shared set), two sibling `send()` calls
  to the same aggregate -- e.g. issued via `tokio::join!`/`select!` from inside
  a reactor -- each inherit the same ancestor snapshot and neither sees the
  other's in-flight key, so they queue on the per-aggregate lock normally
  instead of one spuriously failing as reentrant. This holds only while that
  aggregate is absent from the snapshot they inherited. Siblings aimed at the
  aggregate the reactor is _currently reacting to_ do **not** queue: that key is
  already in the inherited snapshot, so each is rejected with
  `LifecycleError::ReentrantCommand` in its own right, being siblings buys them
  nothing. Two failure modes fall outside the guard entirely, both inherent to
  holding a lock across dispatch at all rather than a gap specific to this
  guard:
  - **Cross-aggregate cycles are not, and cannot be, caught.** Two _separate_,
    concurrently in-flight command chains that reference each other -- e.g.
    aggregate A's reactor commands B while, concurrently, B's reactor commands A
    -- can still deadlock: each chain holds one aggregate's lock while blocking
    on the other's (classic AB-BA lock-order inversion). This applies whether
    the two chains run on separate tasks or as sibling futures joined/selected
    within one task -- the guard only ever sees a single chain's own ancestry,
    never a sibling's.
  - **A same-aggregate command a reactor moves onto a different task also
    escapes it.** If `react()` does
    `tokio::spawn(async move {
    store.send(&id, cmd).await }).await` for the
    _same_ aggregate it is reacting to, the spawned task starts with an empty
    task-local scope of its own -- it has no ancestor call in this chain to see
    -- so it blocks on the per-aggregate mutex the outer task is still holding
    across the spawn-and-await. Same deadlock, same class as the cross-aggregate
    case above, just triggered by a same-aggregate command instead of a
    different one.

  A global, cross-task cycle detector or an acquire-timeout were considered and
  rejected for the same reason the "Document-only, optionally with a debug
  assertion" alternative above was rejected for the _original_ ordering bug
  (timeouts conflict with the "queue instead of fail" behavior this ADR is built
  around) and because a correct distributed-deadlock detector is significant,
  currently unjustified complexity. Consumers must avoid bidirectional
  cross-aggregate command cycles between reactors, and must never move a
  same-aggregate command onto a different task from within a reactor; defer
  either kind of follow-up `send()` until after `react()` returns (e.g. via a
  channel/queue drained outside the reacting command). Documented inline on
  `Store`/`Store::send` and in `docs/cqrs.md`/`docs/domain.md`.
- **Behavior change: no more spurious same-aggregate optimistic-lock errors.**
  Today, two concurrent same-aggregate commands can race to commit sequence N+1,
  and one fails with `OptimisticLockError`. With serialization they queue
  instead, so this failure mode disappears. This is an improvement, and it is a
  documented contract change (see `SPEC.md`).
- **Latency.** A command to an aggregate now waits for any in-flight command on
  the _same_ aggregate to finish its whole `execute`, including slow reactor
  retries (worst case ~4.3s per reacted event under sustained `SQLITE_BUSY`).
  Different aggregates are unaffected.
- **Unbounded lock-table growth**, accepted per above -- retracted by
  [ADR-0005](0005-bounded-per-aggregate-lock-table.md), which bounds the table.
