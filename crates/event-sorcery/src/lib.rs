//! A safer, more ergonomic interface for event-sourced entities
//! on top of cqrs-es.
//!
//! # Why this exists
//!
//! cqrs-es provides the `Aggregate` trait, but it has several
//! sharp edges that have caused production bugs:
//!
//! - **Infallible `apply`**: `Aggregate::apply(&mut self, event)`
//!   returns nothing. Financial applications cannot panic on
//!   arithmetic overflow, so every aggregate needs a wrapper to
//!   capture errors without panicking. Every aggregate in the
//!   codebase had identical boilerplate for this.
//!
//! - **Stringly-typed aggregate IDs**: `cqrs.execute("some-id",
//!   cmd)` takes `&str`, making it trivial to pass the wrong ID.
//!   This has caused production bugs.
//!
//! - **Manual schema versioning**: When aggregate or view schemas
//!   change, a stale `SCHEMA_VERSION` leaves snapshots and views
//!   in the old shape. Bumping it is the operator's responsibility;
//!   [`EventSourced::SCHEMA_VERSION`] plus startup reconciliation
//!   clears version-mismatched snapshots and rebuilds views. On load,
//!   an incompatible snapshot for a [`CompactionPolicy::Retain`]
//!   aggregate is ignored and the entity rebuilt from its
//!   always-present event history; for a
//!   [`CompactionPolicy::CompactAfterSnapshot`] aggregate it instead
//!   surfaces an error, since its events may be gone and the snapshot
//!   is the only durable state -- so state is never silently lost.
//!
//! - **Flat command handling**: A single `handle` method receives
//!   all commands regardless of lifecycle state. Implementors
//!   must manually match on (lifecycle_state, command) tuples,
//!   making it easy to accidentally reference state during
//!   initialization or forget to handle a case.
//!
//! # Design
//!
//! [`EventSourced`] replaces direct `Aggregate` usage. Domain
//! types implement `EventSourced`, and [`Lifecycle`] provides a
//! blanket `Aggregate` impl that bridges to cqrs-es. Consumers
//! interact through [`Store`], which enforces typed IDs and hides
//! cqrs-es internals.
//!
//! ```text
//! Domain type          Adapter             cqrs-es
//! +--------------+     +----------------+  +------------+
//! | impl         | --> | Lifecycle      |  | Aggregate  |
//! | EventSourced |     | (blanket impl) |--| trait      |
//! +--------------+     +----------------+  +------------+
//!                             |
//!                      +------+------+
//!                      | Store       |
//!                      | (typed IDs, |
//!                      |  send())    |
//!                      +-------------+
//! ```
//!
//! # Naming
//!
//! Method names follow two themes to distinguish their purpose:
//!
//! **Event-side** (replaying events to reconstruct state) uses
//! evolution-themed names:
//! - [`originate`](EventSourced::originate) -- create initial
//!   state from the first event
//! - [`evolve`](EventSourced::evolve) -- derive new state from
//!   subsequent events
//!
//! **Command-side** (processing commands to produce events) uses
//! state-machine names:
//! - [`initialize`](EventSourced::initialize) -- handle a
//!   command when no state exists yet
//! - [`transition`](EventSourced::transition) -- handle a
//!   command against existing state
//!
//! The asymmetry is intentional: commands express intent,
//! events express facts. Different verbs for different
//! semantics.
//!
//! cqrs-es names (`Aggregate`, `Query`, `View`, `DomainEvent`)
//! are deliberately avoided in our public API to make it
//! immediately obvious whether code belongs to this crate or
//! to cqrs-es.

pub(crate) mod dependency;
mod lifecycle;
mod projection;
mod reactor;
mod schema_registry;
mod sqlite_event_repository;
#[cfg(any(test, feature = "test-support"))]
mod testing;
mod view_backend;
mod wire;

use async_trait::async_trait;
pub use cqrs_es::AggregateError;
use cqrs_es::CqrsFramework;
pub use cqrs_es::DomainEvent;
use cqrs_es::EventStore;
use cqrs_es::persist::PersistedEventStore;
use serde::Serialize;
use serde::de::DeserializeOwned;
use sqlx::AssertSqlSafe;
use sqlx::SqlitePool;
use std::any::TypeId;
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Display};
use std::str::FromStr;
use std::sync::{Arc, Mutex as SyncMutex, PoisonError};
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};
use tokio::task_local;
use tracing::{error, warn};

#[doc(hidden)]
pub use dependency::Cons;
pub use dependency::Nil;
pub use dependency::{Dependent, EntityList, Fold, HasEntity, OneOf};
use lifecycle::Lifecycle;
pub use lifecycle::{LifecycleError, Never};
pub use projection::{Column, Projection, ProjectionError, Table};
pub use reactor::{
    IdempotentReactor, RETRY_BASE_DELAY_MS, RETRY_MAX_ATTEMPTS, RETRY_MAX_DELAY_MS, Reactor,
    RetryOnBusy, is_retryable_sqlite_busy, retry_with_backoff,
};
pub use schema_registry::{ReconcileError, Reconciler, SchemaReconciliation, SchemaRegistry};
use sqlite_event_repository::SqliteEventRepository;
#[cfg(any(test, feature = "test-support"))]
pub use testing::{
    ReactorHarness, SpyReactor, TestHarness, TestResult, TestStore, replay, test_store,
};
pub use view_backend::{SqliteViewBackend, ViewBackend};
pub use wire::StoreBuilder;

pub(crate) type SqliteCqrs<Entity> =
    CqrsFramework<Lifecycle<Entity>, PersistedEventStore<SqliteEventRepository, Lifecycle<Entity>>>;

/// Whether old events may be deleted after they are captured in a snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionPolicy {
    /// Keep all events indefinitely.
    Retain,
    /// Delete events at or before the current snapshot sequence.
    CompactAfterSnapshot,
}

/// The core abstraction for event-sourced domain entities.
///
/// Implement this trait on your domain type (e.g., `Position`,
/// `OffchainOrder`) to get a complete event-sourcing setup:
/// [`Lifecycle`] provides a blanket `Aggregate` impl, and
/// [`Store`] provides type-safe command dispatch.
///
/// # Associated types
///
/// - `Id`: The strongly-typed aggregate identifier. Prevents
///   mixing up IDs between different entity types at compile
///   time. Converted to string at the cqrs-es boundary only.
/// - `Event`: Domain events that drive state changes. Must be
///   `Eq` so lifecycle error states can carry typed events.
/// - `Command`: Instructions that produce events. A single
///   command type is used for both initialization and
///   transitions -- the lifecycle routes based on state.
/// - `Error`: Domain-specific errors from command handling or
///   event application (e.g., arithmetic overflow). For
///   entities with infallible operations, use [`Never`].
/// - `Services`: External dependencies injected into command
///   handlers (e.g., `Arc<dyn OrderPlacer>`). Use `()` when
///   no services are needed.
///
/// # Constants
///
/// - `AGGREGATE_TYPE`: Stable identifier for the event store.
///   Must not change after events are persisted.
/// - `SCHEMA_VERSION`: Bump when the entity's state, event, or
///   view schema changes. On startup, the wiring infrastructure
///   detects version mismatches and automatically clears stale
///   snapshots and replays views.
///
/// # Event-side methods
///
/// These reconstruct state from the event log during replay.
/// They are called by the blanket `Aggregate::apply` impl on
/// [`Lifecycle`], never by application code directly.
///
/// - `originate`: Attempt to create initial state from an
///   event. Returns `Some(state)` for genesis events, `None`
///   for events that require existing state.
/// - `evolve`: Attempt to derive new state from an event
///   applied to existing state. Returns `Ok(Some(new_state))`
///   on success, `Ok(None)` if the event doesn't apply to the
///   current state (mismatch), or `Err` for domain failures
///   like arithmetic overflow.
///
/// # Command-side methods
///
/// These process commands to produce events. They are called by
/// the blanket `Aggregate::handle` impl on [`Lifecycle`], which
/// routes commands based on lifecycle state.
///
/// - `initialize`: Handle a command when the entity doesn't
///   exist yet. Has no `&self` parameter, preventing accidental
///   reference to existing state during creation.
/// - `transition`: Handle a command against existing state.
///   Receives `&self` (the domain type, not `Lifecycle`), so
///   the handler only deals with live state.
#[async_trait]
pub trait EventSourced:
    Clone + Debug + Send + Sync + Sized + Serialize + DeserializeOwned + 'static
{
    /// Aggregate identity type, used as the key in the event store.
    type Id: Debug + Display + FromStr + Clone + Send + Sync;
    /// Domain event type emitted by commands and applied during replay.
    type Event: DomainEvent;
    /// Command type that drives state transitions.
    type Command: Send + Sync;
    /// Domain error type returned by command handlers and event
    /// application.
    type Error: DomainError;
    /// External dependencies injected into command handlers (e.g.
    /// API clients, order placers).
    type Services: Send + Sync;
    /// Whether this entity has a materialized view.
    ///
    /// Set to `Table` with `PROJECTION = Table("view_name")` for
    /// entities with materialized views. Set to `Nil` with
    /// `PROJECTION = Nil` for entities without views.
    ///
    /// [`StoreBuilder::build()`] uses this to auto-wire projections:
    /// `Table` entities return `(Store, Projection)`, `Nil` entities
    /// return just `Store`.
    type Materialized;

    /// Unique string identifying this aggregate type in the event
    /// store. Must be stable across deployments.
    const AGGREGATE_TYPE: &'static str;
    /// Projection table name (for `Table` entities) or `Nil`.
    const PROJECTION: Self::Materialized;
    /// Schema version for migration reconciliation. Bump when the
    /// event schema changes.
    const SCHEMA_VERSION: u64;
    /// Event retention policy for this entity.
    ///
    /// Financial audit aggregates must use the default
    /// [`CompactionPolicy::Retain`]. Only observational aggregates
    /// whose old events have no audit value should opt into compaction.
    const COMPACTION_POLICY: CompactionPolicy = CompactionPolicy::Retain;
    /// How many commands between automatic snapshots.
    ///
    /// A snapshot of `1` means every command triggers a snapshot
    /// write -- ideal for compactable aggregates where the snapshot
    /// is the durable source of pre-compaction state. Retained
    /// aggregates with low event counts per instance benefit from a
    /// larger value (e.g., 10-50) to reduce write amplification.
    const SNAPSHOT_SIZE: usize = 10;

    /// Create initial state from a genesis event.
    ///
    /// Returns `Some(state)` if this event creates the entity,
    /// `None` if it requires existing state. Returning `None`
    /// causes [`Lifecycle`] to enter a `Failed` state with a
    /// [`LifecycleError::EventCantOriginate`].
    fn originate(event: &Self::Event) -> Option<Self>;

    /// Derive new entity from an event applied to the current one.
    ///
    /// - `Ok(Some(new_entity))` -- event applied successfully
    /// - `Ok(None)` -- event doesn't apply to current entity
    ///   (becomes [`LifecycleError::UnexpectedEvent`])
    /// - `Err(error)` -- domain error during application
    ///   (becomes [`LifecycleError::Apply`])
    fn evolve(entity: &Self, event: &Self::Event) -> Result<Option<Self>, Self::Error>;

    /// Handle a command when the entity doesn't exist yet.
    ///
    /// No `&self` -- impossible to accidentally reference
    /// existing state during creation.
    async fn initialize(
        command: Self::Command,
        services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error>;

    /// Handle a command against existing state.
    ///
    /// `&self` is the domain type directly, not `Lifecycle`.
    /// The handler only deals with live state; lifecycle routing
    /// is handled by the blanket `Aggregate` impl.
    async fn transition(
        &self,
        command: Self::Command,
        services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error>;
}

/// Type-safe command dispatch for an event-sourced entity.
///
/// Wraps `SqliteCqrs<Lifecycle<Entity>>` and enforces that
/// commands are addressed to the correct entity type via
/// strongly-typed IDs. This prevents a class of bugs where
/// string aggregate IDs are mixed up between different entity
/// types.
///
/// # Usage
///
/// ```ignore
/// let positions: Store<Position> = /* built by StoreBuilder */;
///
/// // Typed ID -- can't accidentally pass an OffchainOrderId
/// let symbol = Symbol::new("AAPL").unwrap();
/// positions.send(&symbol, PositionCommand::AcknowledgeFill { .. }).await?;
/// ```
///
/// Produced by [`StoreBuilder::build()`] during conductor
/// startup. The builder handles CQRS framework construction,
/// query wiring, and schema reconciliation, returning a
/// ready-to-use `Store`.
///
/// `send` serializes commands per aggregate ID (see
/// [`PerAggregateLocks`]): the whole load -> handle -> commit ->
/// reactor/projection-dispatch cycle of one command completes
/// before the next command on the *same* aggregate begins, so
/// reactors and projections observe events in commit order.
/// Commands to different aggregate IDs still run concurrently.
///
/// **Reentrancy hazard:** because the lock is held across the
/// whole command (including reactor dispatch), a reactor calling
/// `Store::send()` back onto the same `(Entity, Entity::Id)` it is
/// currently reacting to -- directly, or transitively through a
/// chain of other reactors' inline-awaited dispatches within the
/// *same inline await-chain* -- would deadlock against the held
/// lock. `Store::send` detects this via a task-local set of
/// in-flight aggregate keys, inherited down that await-chain, and
/// fails fast with [`LifecycleError::ReentrantCommand`] instead of
/// hanging.
///
/// **The guard catches ancestor self-cycles, not sibling
/// concurrency.** It tracks *inline await-chain ancestry*, not task
/// identity: a `send()` rejects `id` only if a call somewhere up its
/// own chain of awaiters already holds it. Two sibling `send()`
/// calls to the same aggregate -- e.g. issued via `tokio::join!` from
/// inside a reactor -- each inherit the same ancestor snapshot and so
/// neither sees the other's in-flight key. Provided that aggregate is
/// not itself in the snapshot they inherited, they queue on
/// [`PerAggregateLocks`] normally, exactly like two unrelated calls
/// would, rather than one spuriously failing as reentrant. Siblings
/// aimed at the aggregate the reactor is *currently reacting to* are
/// the other case entirely: that key is already in the inherited
/// snapshot, so each is rejected with
/// [`LifecycleError::ReentrantCommand`] -- being siblings buys them
/// nothing, because each one is an ancestor self-cycle in its own
/// right.
///
/// A reactor that moves a same-aggregate `send()` onto a *different*
/// task -- e.g. `tokio::spawn(async move { store.send(&id, cmd).await
/// }).await` -- escapes the guard entirely. The spawned task starts
/// with an empty task-local scope, so it blocks on the per-aggregate
/// mutex the outer task is still holding across that spawn-and-
/// await, exactly like the cross-aggregate cycle below. Never move a
/// same-aggregate command off-task from within a reactor; defer such
/// work until after `react()` returns.
///
/// **Cross-aggregate cycles are not detected.** The reentrancy guard
/// only catches a self-cycle within a single inline await-chain. Two
/// *separate*, concurrently in-flight command chains that reference
/// each other -- e.g. aggregate A's reactor commands B while,
/// concurrently, B's reactor commands A, whether on separate tasks or
/// via sibling futures joined/selected within one task -- can still
/// deadlock: each chain holds one aggregate's lock while blocking on
/// the other's. This is inherent to the per-aggregate ordering
/// guarantee (see ADR-0004); avoid bidirectional cross-aggregate
/// command cycles, or defer the cross-aggregate `send()` until after
/// `react()` returns (e.g. via a channel/queue drained outside the
/// reacting command) so it does not run under the held lock.
///
/// **Aggregate IDs must be of bounded cardinality.** The per-aggregate
/// lock map is never evicted: it holds one mutex per distinct
/// `Entity::Id` this `Store` has ever commanded, for the lifetime of
/// the `Store`. That is sized for business-entity cardinality (see
/// ADR-0004), so a `Store` fed an unbounded stream of fresh IDs -- one
/// per request, say -- grows its lock map without bound. Do not use
/// `Entity::Id`s that are effectively unique per command.
pub struct Store<Entity: EventSourced> {
    cqrs: SqliteCqrs<Entity>,
    persisted_events: PersistedEventStore<SqliteEventRepository, Lifecycle<Entity>>,
    locks: PerAggregateLocks,
}

impl<Entity: EventSourced> Store<Entity> {
    /// Wrap an existing `SqliteCqrs` framework.
    ///
    /// Prefer using `StoreBuilder::build()` which handles wiring
    /// and reconciliation. This constructor exists for cases
    /// where direct construction is needed (e.g., tests).
    pub(crate) fn new(cqrs: SqliteCqrs<Entity>, pool: SqlitePool) -> Self {
        let repo = SqliteEventRepository::new(pool, Entity::COMPACTION_POLICY);
        let persisted_events = PersistedEventStore::new_snapshot_store(repo, Entity::SNAPSHOT_SIZE);
        Self {
            cqrs,
            persisted_events,
            locks: PerAggregateLocks::default(),
        }
    }

    /// Send a command to the entity identified by `id`.
    ///
    /// The command is routed based on the entity's lifecycle
    /// state:
    /// - Uninitialized -> `Entity::initialize`
    /// - Live -> `Entity::transition`
    /// - Failed -> returns the stored error
    ///
    /// Serializes per aggregate ID -- see the type-level doc
    /// comment on [`Store`] for the ordering guarantee, the
    /// fail-fast same-aggregate reentrancy guard, and the
    /// cross-aggregate cycle caveat this introduces.
    ///
    /// # Errors
    ///
    /// Returns [`LifecycleError::ReentrantCommand`] if this call is
    /// already commanding `id` somewhere up its own inline await-chain
    /// (directly, or transitively through inline-awaited reactor dispatch)
    /// instead of blocking forever on the already-held lock. This only
    /// catches ancestor self-cycles -- see the [`Store`] type-level doc
    /// comment for the escape hatches (spawned-and-awaited same-aggregate
    /// `send()`, and cross-aggregate cycles) it does not close.
    pub async fn send(
        &self,
        id: &Entity::Id,
        command: Entity::Command,
    ) -> Result<(), SendError<Entity>> {
        let lock_key = AggregateLockKey {
            entity: TypeId::of::<Entity>(),
            id: id.to_string(),
        };

        // The extended set is only ever installed in a *new*, nested
        // `HELD_AGGREGATE_LOCKS` scope wrapping this call's own
        // `send_under_lock` future below, so it becomes visible only while
        // code within this call's own await-chain is being polled -- true
        // descendants (an inline-awaited reactor this call's command
        // dispatches) see it, but a sibling does not.
        let admission = HELD_AGGREGATE_LOCKS
            .try_with(|held| Admission::for_inherited_scope(held, &lock_key))
            // No scope yet at all (e.g. a brand-new task a reactor spawned
            // via `tokio::spawn`) -- deliberately treated as "nothing to
            // detect" rather than "possibly reentrant". A spawned task has
            // no ancestor call in this chain and cannot see the spawning
            // task's held keys; that is the documented escape hatch a
            // spawned-and-awaited same-aggregate `send()` opens -- see the
            // [`Store`] type-level doc comment.
            .unwrap_or_else(|_task_local_not_yet_scoped| {
                Admission::Proceed(HashSet::from([lock_key.clone()]))
            });

        let held = match admission {
            Admission::Reject => {
                error!(
                    aggregate_id = %lock_key.id,
                    entity = std::any::type_name::<Entity>(),
                    "reentrant same-aggregate command rejected",
                );
                return Err(LifecycleError::ReentrantCommand.into());
            }
            Admission::Proceed(held) => held,
        };

        HELD_AGGREGATE_LOCKS
            .scope(held, self.send_under_lock(&lock_key.id, command))
            .await
    }

    /// Acquires the per-aggregate lock and runs `command` through `cqrs-es`.
    /// Called only after `send` has confirmed `id_string`'s key is not
    /// already held by an ancestor call in this call's own await-chain (and
    /// has installed a nested [`HELD_AGGREGATE_LOCKS`] scope reflecting
    /// that), so this never contends with itself.
    async fn send_under_lock(
        &self,
        id_string: &str,
        command: Entity::Command,
    ) -> Result<(), SendError<Entity>> {
        let _lock_guard = self.locks.acquire(id_string).await;
        self.cqrs.execute(id_string, command).await
    }

    /// Load an entity's current state directly from the event store.
    ///
    /// Replays events to reconstruct aggregate state. No query
    /// processors are dispatched.
    ///
    /// Returns:
    /// - `Ok(Some(entity))` if the entity is live
    /// - `Ok(None)` if the entity has not been initialized
    /// - `Err` if the entity is in a failed lifecycle state or on infrastructure error
    pub async fn load(&self, id: &Entity::Id) -> Result<Option<Entity>, SendError<Entity>> {
        let context = self
            .persisted_events
            .load_aggregate(&id.to_string())
            .await?;

        Ok(context.aggregate.into_result()?)
    }

    /// Reconstruct an entity's state from events without needing
    /// a full `Store` (no services or CQRS framework required).
    ///
    /// Useful in test/CLI contexts where you only need to read
    /// aggregate state and never send commands.
    #[cfg(any(test, feature = "test-support"))]
    pub async fn load_from_pool(
        pool: SqlitePool,
        id: &Entity::Id,
    ) -> Result<Option<Entity>, SendError<Entity>> {
        let repo = SqliteEventRepository::new(pool, Entity::COMPACTION_POLICY);
        let persisted_events =
            PersistedEventStore::<SqliteEventRepository, Lifecycle<Entity>>::new_snapshot_store(
                repo,
                Entity::SNAPSHOT_SIZE,
            );
        let context = persisted_events.load_aggregate(&id.to_string()).await?;

        Ok(context.aggregate.into_result()?)
    }
}

/// Identifies one aggregate instance (an `Entity` type plus its ID) for the
/// reentrancy guard and per-aggregate lock table below.
///
/// Keyed by `TypeId` plus `String` rather than a formatted string so that two
/// different `(Entity, id)` pairs can never collide on a shared key, which
/// `std::any::type_name` (a diagnostic aid, not a type-identity primitive)
/// plus string concatenation could not guarantee.
#[derive(Clone, PartialEq, Eq, Hash)]
struct AggregateLockKey {
    entity: TypeId,
    id: String,
}

/// Outcome of the reentrancy check a `Store::send` runs against the lock keys
/// it inherited from its own await-chain.
///
/// Names the reject path instead of leaving it as a shape (a `None` inside an
/// `Ok`), so the two ways a `send` may proceed -- an inherited scope that does
/// not already hold this key, and no inherited scope at all -- cannot be
/// confused with the one way it must not.
enum Admission {
    /// An ancestor call in this await-chain already holds this key, so
    /// proceeding would deadlock on a lock this chain itself is holding.
    Reject,
    /// Safe to proceed. Carries the key set to install for this call's own
    /// nested scope: everything inherited, plus this call's key.
    Proceed(HashSet<AggregateLockKey>),
}

impl Admission {
    /// Snapshot-and-extend, never mutate-in-place: reads the inherited scope
    /// without touching it, so a sibling `send()` joined/selected alongside
    /// this one -- which inherited the very same scope -- is unaffected by the
    /// key this call goes on to add.
    fn for_inherited_scope(held: &HashSet<AggregateLockKey>, lock_key: &AggregateLockKey) -> Self {
        if held.contains(lock_key) {
            return Self::Reject;
        }

        let mut extended = held.clone();
        extended.insert(lock_key.clone());
        Self::Proceed(extended)
    }
}

task_local! {
    /// Aggregate lock keys held by the inline await-chain currently being
    /// polled, i.e. the in-flight `Store::send` call this scope was
    /// installed for, plus every ancestor call it inherited a snapshot from.
    ///
    /// `CqrsFramework::execute_with_metadata` dispatches every registered
    /// reactor inline, awaited directly -- so a reactor's own `Store::send()`
    /// is polled as part of the same await-chain as the outer `execute()`
    /// that is reacting to it, and this task-local value is visible to it.
    /// Each `Store::send` call installs its own *nested* scope (see
    /// `Store::send`) containing a snapshot of whatever it inherited plus its
    /// own key, rather than mutating the inherited value in place -- a
    /// `tokio::task_local!` scope is only ever visible while the future it
    /// wraps is actually being polled, so this nested scope is visible to
    /// true descendants (an inline-awaited reactor this call's command
    /// dispatches) but not to a sibling `send()` joined/selected alongside
    /// this one, which still only sees what it itself inherited. `Store::send`
    /// checks the inherited snapshot before acquiring [`PerAggregateLocks`] so
    /// a same-aggregate self-command -- direct or transitive -- fails fast
    /// with [`LifecycleError::ReentrantCommand`] instead of deadlocking on a
    /// lock an ancestor call already holds. It does not, and cannot, catch a
    /// cycle formed by two *separate* concurrently in-flight tasks -- nor a
    /// same-aggregate `send()` a reactor moves onto a new task via
    /// `tokio::spawn` and then awaits, since that spawned task starts with
    /// an empty scope of its own -- see the [`Store`] type-level doc
    /// comment.
    static HELD_AGGREGATE_LOCKS: HashSet<AggregateLockKey>;
}

/// Keyed async mutex serializing `Store::send` per aggregate ID.
///
/// See ADR-0004 for the full rationale. An outer `std::sync::Mutex` guards a
/// map from aggregate ID to a per-aggregate `tokio::sync::Mutex`; callers
/// clone the `Arc` for their ID out of the map and then hold the per-aggregate
/// lock across the async command execution. The per-aggregate lock is async
/// (not `std::sync::Mutex`) because the guard is held across an `.await`
/// point -- a `std` guard held there would block the executing worker thread.
///
/// The map is never evicted: it grows with aggregate-ID cardinality for the
/// lifetime of the `Store`. Accepted tradeoff for this library's bounded
/// business-entity cardinality; see ADR-0004.
#[derive(Default)]
struct PerAggregateLocks {
    locks: SyncMutex<HashMap<String, Arc<AsyncMutex<()>>>>,
}

impl PerAggregateLocks {
    /// Acquire the lock for `aggregate_id`, creating it on first use.
    ///
    /// The returned guard is released by RAII when dropped -- including on
    /// cancellation of the future holding it -- so there is no separate
    /// release step.
    async fn acquire(&self, aggregate_id: &str) -> OwnedMutexGuard<()> {
        let per_aggregate_lock = {
            let mut locks = self.locks.lock().unwrap_or_else(PoisonError::into_inner);
            // Look up by borrowed `&str` first so the steady-state case (the
            // aggregate's lock already exists, which is the overwhelmingly
            // common case once a `Store` has warmed up) never allocates a
            // `String` just to discard it. Only the first-use miss path pays
            // for the owned key `entry()` requires.
            locks.get(aggregate_id).cloned().unwrap_or_else(|| {
                Arc::clone(
                    locks
                        .entry(aggregate_id.to_string())
                        .or_insert_with(|| Arc::new(AsyncMutex::new(()))),
                )
            })
        };

        per_aggregate_lock.lock_owned().await
    }
}

/// Error returned by [`Store::send`] and [`Store::load`].
///
/// Wraps the cqrs-es `AggregateError` containing a
/// `LifecycleError` so that consumers don't import from cqrs-es
/// or lifecycle directly.
pub type SendError<Entity> = AggregateError<LifecycleError<Entity>>;

impl<Entity: EventSourced> From<LifecycleError<Entity>> for SendError<Entity> {
    fn from(error: LifecycleError<Entity>) -> Self {
        Self::UserError(error)
    }
}

/// Bounds required for domain error types used with
/// [`EventSourced`].
///
/// [`LifecycleError`] stores the entity's error in its `Apply`
/// variant and derives `Clone`, `Serialize`, `Deserialize`,
/// `PartialEq`, and `Eq`. This trait captures those bounds in
/// one place so implementors see a single meaningful name
/// instead of a long bound list.
pub trait DomainError:
    std::error::Error + Clone + Serialize + DeserializeOwned + Send + Sync
{
}

impl<T> DomainError for T where
    T: std::error::Error + Clone + Serialize + DeserializeOwned + Send + Sync
{
}

/// Load a single entity by replaying events from the store.
///
/// Creates a lightweight, temporary event store - no CQRS framework, no
/// query processors. Suitable for read-only access from contexts that
/// don't own a [`Store`] (e.g., dashboard transfer loading).
///
/// # Errors
///
/// Returns `SendError` if event store loading or lifecycle
/// reconstruction fails.
pub async fn load_entity<Entity: EventSourced>(
    pool: &SqlitePool,
    id: &Entity::Id,
) -> Result<Option<Entity>, SendError<Entity>> {
    let repo = SqliteEventRepository::new(pool.clone(), Entity::COMPACTION_POLICY);
    let persisted_events =
        PersistedEventStore::<SqliteEventRepository, Lifecycle<Entity>>::new_snapshot_store(
            repo,
            Entity::SNAPSHOT_SIZE,
        );

    let context = persisted_events.load_aggregate(&id.to_string()).await?;

    Ok(context.aggregate.into_result()?)
}

/// Execute a single command against an aggregate without a pre-built
/// [`Store`].
///
/// Creates a temporary CQRS framework with no query processors,
/// executes the command, and discards the framework. Useful in CLI
/// contexts where you need to send a command but don't have (or need)
/// a full server-lifetime Store.
///
/// The caller must provide `services` matching the aggregate's
/// `Services` type. For commands that never invoke services (e.g.,
/// failure commands), a panicking stub is safe.
///
/// # Bypasses the per-aggregate ordering guarantee
///
/// This calls `cqrs.execute()` directly: it takes no per-aggregate lock and
/// registers no query processors. Two consequences, both of which put it
/// outside the guarantee [`Store::send`] provides (see ADR-0004):
///
/// - **No serialization.** Run concurrently against a live [`Store`] on the
///   same aggregate, it interleaves with that `Store`'s commands rather than
///   queueing behind them.
/// - **No dispatch.** Its events never reach the `Store`'s reactors or
///   projections, so read models do not see them until something else replays
///   the event log (e.g. `Projection::catch_up`).
///
/// Use it only where no `Store` for the entity is concurrently live -- CLI,
/// migration, and test contexts. For anything server-lifetime, use
/// [`Store::send`].
pub async fn send_command<Entity: EventSourced>(
    pool: &SqlitePool,
    id: &Entity::Id,
    command: Entity::Command,
    services: Entity::Services,
) -> Result<(), SendError<Entity>> {
    let repo = SqliteEventRepository::new(pool.clone(), Entity::COMPACTION_POLICY);
    let store = PersistedEventStore::<SqliteEventRepository, Lifecycle<Entity>>::new_snapshot_store(
        repo,
        Entity::SNAPSHOT_SIZE,
    );

    #[allow(clippy::disallowed_methods)]
    let cqrs = CqrsFramework::new(store, vec![], services);

    cqrs.execute(&id.to_string(), command).await
}

/// Delete compactable events that are already represented by snapshots.
///
/// This is a no-op for entities with [`CompactionPolicy::Retain`]. For
/// compactable entities, events with `sequence <= snapshots.last_sequence` are
/// removed. Snapshot-backed loading can still reconstruct the aggregate from the
/// snapshot and replay any newer events.
///
/// # Errors
///
/// Returns database errors from the delete query.
pub async fn compact_events<Entity: EventSourced>(pool: &SqlitePool) -> Result<u64, sqlx::Error> {
    if Entity::COMPACTION_POLICY == CompactionPolicy::Retain {
        return Ok(0);
    }

    let result = sqlx::query(
        "DELETE FROM events \
         WHERE aggregate_type = ?1 \
           AND sequence <= COALESCE( \
               (SELECT last_sequence FROM snapshots \
                WHERE snapshots.aggregate_type = events.aggregate_type \
                  AND snapshots.aggregate_id = events.aggregate_id), \
               0 \
           )",
    )
    .bind(Entity::AGGREGATE_TYPE)
    .execute(pool)
    .await?;

    Ok(result.rows_affected())
}

/// Reclaim SQLite file space after event compaction.
///
/// Full `VACUUM` can take an exclusive database lock and should be reserved for
/// explicit maintenance windows, not hot runtime loops.
///
/// # Errors
///
/// Returns database errors from SQLite `VACUUM`.
pub async fn vacuum(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    sqlx::query("VACUUM").execute(pool).await?;
    Ok(())
}

/// Reclaim a bounded number of SQLite freelist pages.
///
/// This is intended for periodic background cleanup on databases configured
/// with `auto_vacuum = INCREMENTAL`.
///
/// # Errors
///
/// Returns database errors from SQLite `PRAGMA incremental_vacuum`.
pub async fn incremental_vacuum(pool: &SqlitePool, pages: u32) -> Result<(), sqlx::Error> {
    sqlx::query(AssertSqlSafe(format!("PRAGMA incremental_vacuum({pages})")))
        .execute(pool)
        .await?;
    Ok(())
}

/// Load all aggregate IDs for a given entity type.
///
/// Queries events and snapshots for distinct aggregate IDs. Used by dashboard
/// transfer loading to enumerate all transfer aggregates without requiring
/// access to a [`Store`].
///
/// Returns an error if any stored aggregate ID fails to parse,
/// since that indicates data corruption or a schema mismatch.
///
/// # Errors
///
/// Returns `LoadAllIdsError` on database errors or if stored aggregate
/// IDs fail to parse.
pub async fn load_all_ids<Entity: EventSourced>(
    pool: &SqlitePool,
) -> Result<Vec<Entity::Id>, LoadAllIdsError>
where
    <Entity::Id as FromStr>::Err: Debug,
{
    let rows: Vec<(String,)> = sqlx::query_as(
        "SELECT aggregate_id FROM ( \
             SELECT aggregate_id FROM events WHERE aggregate_type = ?1 \
             UNION \
             SELECT aggregate_id FROM snapshots WHERE aggregate_type = ?1 \
         ) \
         ORDER BY aggregate_id ASC",
    )
    .bind(Entity::AGGREGATE_TYPE)
    .fetch_all(pool)
    .await?;

    let (ids, invalid) = rows.into_iter().fold(
        (Vec::new(), Vec::new()),
        |(mut ids, mut invalid), (id_str,)| {
            match id_str.parse::<Entity::Id>() {
                Ok(id) => ids.push(id),
                Err(parse_error) => {
                    warn!(
                        target: "cqrs",
                        aggregate_id = id_str,
                        aggregate_type = Entity::AGGREGATE_TYPE,
                        ?parse_error,
                        "Failed to parse aggregate ID"
                    );
                    invalid.push(id_str);
                }
            }
            (ids, invalid)
        },
    );

    if invalid.is_empty() {
        Ok(ids)
    } else {
        Err(LoadAllIdsError::InvalidIds {
            aggregate_type: Entity::AGGREGATE_TYPE,
            ids: invalid,
        })
    }
}

/// Load aggregate IDs with pagination, newest first (by highest rowid).
///
/// Returns up to `limit` IDs starting from `offset`, ordered by most
/// recently created aggregate first (based on the maximum rowid of each
/// aggregate's events or snapshot).
///
/// # Errors
///
/// Returns `LoadAllIdsError` on database errors or unparseable IDs.
pub async fn load_ids_paginated<Entity: EventSourced>(
    pool: &SqlitePool,
    limit: usize,
    offset: usize,
) -> Result<Vec<Entity::Id>, LoadAllIdsError>
where
    <Entity::Id as FromStr>::Err: Debug,
{
    let rows: Vec<(String,)> = sqlx::query_as(
        "SELECT aggregate_id FROM ( \
             SELECT aggregate_id, MAX(rowid) AS latest_rowid \
             FROM events \
             WHERE aggregate_type = ?1 \
             GROUP BY aggregate_id \
             UNION ALL \
             SELECT aggregate_id, MAX(rowid) AS latest_rowid \
             FROM snapshots \
             WHERE aggregate_type = ?1 \
             GROUP BY aggregate_id \
         ) \
         GROUP BY aggregate_id \
         ORDER BY MAX(latest_rowid) DESC \
         LIMIT ?2 OFFSET ?3",
    )
    .bind(Entity::AGGREGATE_TYPE)
    .bind(i64::try_from(limit)?)
    .bind(i64::try_from(offset)?)
    .fetch_all(pool)
    .await?;

    let (ids, invalid) = rows.into_iter().fold(
        (Vec::new(), Vec::new()),
        |(mut ids, mut invalid), (id_str,)| {
            match id_str.parse::<Entity::Id>() {
                Ok(id) => ids.push(id),
                Err(parse_error) => {
                    warn!(
                        target: "cqrs",
                        aggregate_id = id_str,
                        aggregate_type = Entity::AGGREGATE_TYPE,
                        ?parse_error,
                        "Failed to parse aggregate ID (paginated)"
                    );
                    invalid.push(id_str);
                }
            }
            (ids, invalid)
        },
    );

    if invalid.is_empty() {
        Ok(ids)
    } else {
        Err(LoadAllIdsError::InvalidIds {
            aggregate_type: Entity::AGGREGATE_TYPE,
            ids: invalid,
        })
    }
}

/// Count the total number of distinct aggregates of this type.
///
/// # Errors
///
/// Returns `LoadAllIdsError` on database failure or numeric conversion error.
pub async fn count_aggregates<Entity: EventSourced>(
    pool: &SqlitePool,
) -> Result<usize, LoadAllIdsError> {
    let row: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM ( \
             SELECT aggregate_id FROM events WHERE aggregate_type = ?1 \
             UNION \
             SELECT aggregate_id FROM snapshots WHERE aggregate_type = ?1 \
         )",
    )
    .bind(Entity::AGGREGATE_TYPE)
    .fetch_one(pool)
    .await?;

    Ok(usize::try_from(row.0)?)
}

/// Errors that can occur when loading all aggregate IDs.
#[derive(Debug, thiserror::Error)]
pub enum LoadAllIdsError {
    #[error("Database error: {0}")]
    Sql(#[from] sqlx::Error),
    #[error(
        "Found unparseable aggregate IDs for {aggregate_type}: \
         {ids:?}"
    )]
    InvalidIds {
        aggregate_type: &'static str,
        ids: Vec<String>,
    },
    #[error("Numeric conversion error: {0}")]
    NumericConversion(#[from] std::num::TryFromIntError),
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use cqrs_es::DomainEvent;
    use serde::{Deserialize, Serialize};
    use sqlx::SqlitePool;
    use std::time::Duration;
    use tokio::sync::{Notify, OnceCell, oneshot};
    use tokio::time::{Instant, sleep, timeout};

    use super::*;
    use crate::deps;

    /// Numeric-only ID that rejects non-numeric strings.
    #[derive(Debug, Clone, PartialEq, Eq)]
    struct NumericId(u64);

    impl Display for NumericId {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(formatter, "{}", self.0)
        }
    }

    impl FromStr for NumericId {
        type Err = std::num::ParseIntError;

        fn from_str(value: &str) -> Result<Self, Self::Err> {
            value.parse::<u64>().map(NumericId)
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct Widget {
        name: String,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    enum WidgetEvent {
        Created { name: String },
        Renamed { name: String },
    }

    impl DomainEvent for WidgetEvent {
        fn event_type(&self) -> String {
            match self {
                Self::Created { .. } => "WidgetEvent::Created".to_string(),
                Self::Renamed { .. } => "WidgetEvent::Renamed".to_string(),
            }
        }

        fn event_version(&self) -> String {
            "1.0".to_string()
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
    #[error("widget error")]
    struct WidgetError;

    enum WidgetCommand {
        Create { name: String },
        Rename { name: String },
    }

    #[async_trait]
    impl EventSourced for Widget {
        type Id = NumericId;
        type Event = WidgetEvent;
        type Command = WidgetCommand;
        type Error = WidgetError;
        type Services = ();
        type Materialized = Nil;

        const AGGREGATE_TYPE: &'static str = "Widget";
        const PROJECTION: Nil = Nil;
        const SCHEMA_VERSION: u64 = 1;
        const COMPACTION_POLICY: CompactionPolicy = CompactionPolicy::CompactAfterSnapshot;
        const SNAPSHOT_SIZE: usize = 1;

        fn originate(event: &WidgetEvent) -> Option<Self> {
            match event {
                WidgetEvent::Created { name } => Some(Self { name: name.clone() }),
                WidgetEvent::Renamed { .. } => None,
            }
        }

        fn evolve(_entity: &Self, event: &WidgetEvent) -> Result<Option<Self>, WidgetError> {
            match event {
                WidgetEvent::Created { .. } => Ok(None),
                WidgetEvent::Renamed { name } => Ok(Some(Self { name: name.clone() })),
            }
        }

        async fn initialize(
            command: WidgetCommand,
            _services: &(),
        ) -> Result<Vec<WidgetEvent>, WidgetError> {
            match command {
                WidgetCommand::Create { name } => Ok(vec![WidgetEvent::Created { name }]),
                WidgetCommand::Rename { name } => Ok(vec![WidgetEvent::Renamed { name }]),
            }
        }

        async fn transition(
            &self,
            command: WidgetCommand,
            _services: &(),
        ) -> Result<Vec<WidgetEvent>, WidgetError> {
            match command {
                WidgetCommand::Create { .. } => Ok(vec![]),
                WidgetCommand::Rename { name } => Ok(vec![WidgetEvent::Renamed { name }]),
            }
        }
    }

    async fn test_pool() -> SqlitePool {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!("../../migrations").run(&pool).await.unwrap();
        pool
    }

    async fn insert_event(pool: &SqlitePool, aggregate_type: &str, aggregate_id: &str) {
        sqlx::query(
            "INSERT INTO events (aggregate_type, aggregate_id, sequence, \
             event_type, event_version, payload, metadata) \
             VALUES (?1, ?2, 1, 'WidgetEvent::Created', '1.0', ?3, '{}')",
        )
        .bind(aggregate_type)
        .bind(aggregate_id)
        .bind(r#"{"Created":{"name":"test-widget"}}"#)
        .execute(pool)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn load_entity_replays_events_into_entity() {
        let pool = test_pool().await;
        let store = testing::test_store::<Widget>(pool.clone(), ());

        store
            .send(
                &NumericId(42),
                WidgetCommand::Create {
                    name: "test-widget".to_string(),
                },
            )
            .await
            .unwrap();

        let entity = load_entity::<Widget>(&pool, &NumericId(42)).await.unwrap();

        let widget = entity.expect("entity should exist after event replay");
        assert_eq!(widget.name, "test-widget");
    }

    #[tokio::test]
    async fn load_entity_uses_snapshot_after_events_are_compacted() {
        let pool = test_pool().await;
        let store = testing::test_store::<Widget>(pool.clone(), ());

        store
            .send(
                &NumericId(42),
                WidgetCommand::Create {
                    name: "first".to_string(),
                },
            )
            .await
            .unwrap();
        store
            .send(
                &NumericId(42),
                WidgetCommand::Rename {
                    name: "second".to_string(),
                },
            )
            .await
            .unwrap();

        let deleted = compact_events::<Widget>(&pool).await.unwrap();

        assert_eq!(deleted, 2);
        let event_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM events \
             WHERE aggregate_type = 'Widget' AND aggregate_id = '42'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(event_count, 0);

        let entity = load_entity::<Widget>(&pool, &NumericId(42)).await.unwrap();

        let widget = entity.expect("entity should load from snapshot after compaction");
        assert_eq!(widget.name, "second");
    }

    #[tokio::test]
    async fn snapshot_version_advances_across_loads() {
        let pool = test_pool().await;
        let store = testing::test_store::<Widget>(pool.clone(), ());

        store
            .send(
                &NumericId(42),
                WidgetCommand::Create {
                    name: "first".to_string(),
                },
            )
            .await
            .unwrap();
        store
            .send(
                &NumericId(42),
                WidgetCommand::Rename {
                    name: "second".to_string(),
                },
            )
            .await
            .unwrap();

        let snapshot_version: i64 = sqlx::query_scalar(
            "SELECT snapshot_version FROM snapshots \
             WHERE aggregate_type = 'Widget' AND aggregate_id = '42'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(snapshot_version, 2);
    }

    #[tokio::test]
    async fn snapshot_rebuild_applies_events_exactly_once() {
        #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
        struct Tally {
            count: u64,
        }

        #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
        enum TallyEvent {
            Started,
            Incremented,
        }

        impl DomainEvent for TallyEvent {
            fn event_type(&self) -> String {
                format!("{self:?}")
            }

            fn event_version(&self) -> String {
                "1.0".to_string()
            }
        }

        enum TallyCommand {
            Start,
            Increment,
        }

        #[async_trait]
        impl EventSourced for Tally {
            type Id = NumericId;
            type Event = TallyEvent;
            type Command = TallyCommand;
            type Error = WidgetError;
            type Services = ();
            type Materialized = Nil;

            const AGGREGATE_TYPE: &'static str = "Tally";
            const PROJECTION: Nil = Nil;
            const SCHEMA_VERSION: u64 = 1;
            const SNAPSHOT_SIZE: usize = 1;

            fn originate(event: &TallyEvent) -> Option<Self> {
                match event {
                    TallyEvent::Started => Some(Self { count: 0 }),
                    TallyEvent::Incremented => None,
                }
            }

            fn evolve(entity: &Self, event: &TallyEvent) -> Result<Option<Self>, WidgetError> {
                match event {
                    TallyEvent::Started => Ok(None),
                    TallyEvent::Incremented => Ok(Some(Self {
                        count: entity.count + 1,
                    })),
                }
            }

            async fn initialize(
                command: TallyCommand,
                _services: &(),
            ) -> Result<Vec<TallyEvent>, WidgetError> {
                match command {
                    TallyCommand::Start => Ok(vec![TallyEvent::Started]),
                    TallyCommand::Increment => Ok(vec![TallyEvent::Incremented]),
                }
            }

            async fn transition(
                &self,
                command: TallyCommand,
                _services: &(),
            ) -> Result<Vec<TallyEvent>, WidgetError> {
                match command {
                    TallyCommand::Start => Ok(vec![]),
                    TallyCommand::Increment => Ok(vec![TallyEvent::Incremented]),
                }
            }
        }

        let pool = test_pool().await;
        let store = testing::test_store::<Tally>(pool.clone(), ());

        // SNAPSHOT_SIZE = 1 forces commit's snapshot rebuild
        // (`update_snapshot_with_events`) after every command, exercising the
        // re-apply path that requires `Lifecycle::handle` to leave `self` at
        // its pre-command state. A counting entity makes double-application
        // visible as a wrong count rather than a coincidentally-equal value.
        store
            .send(&NumericId(1), TallyCommand::Start)
            .await
            .unwrap();
        store
            .send(&NumericId(1), TallyCommand::Increment)
            .await
            .unwrap();
        store
            .send(&NumericId(1), TallyCommand::Increment)
            .await
            .unwrap();

        let entity = load_entity::<Tally>(&pool, &NumericId(1)).await.unwrap();
        let tally = entity.expect("tally should exist after three commands");
        assert_eq!(tally.count, 2);

        let payload: String = sqlx::query_scalar(
            "SELECT payload FROM snapshots \
             WHERE aggregate_type = 'Tally' AND aggregate_id = '1'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        let snapshot: serde_json::Value = serde_json::from_str(&payload).unwrap();
        assert_eq!(snapshot, serde_json::json!({"Live": {"count": 2}}));
    }

    #[tokio::test]
    async fn retain_policy_does_not_compact_events() {
        #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
        struct RetainedWidget {
            name: String,
        }

        #[async_trait]
        impl EventSourced for RetainedWidget {
            type Id = NumericId;
            type Event = WidgetEvent;
            type Command = WidgetCommand;
            type Error = WidgetError;
            type Services = ();
            type Materialized = Nil;

            const AGGREGATE_TYPE: &'static str = "RetainedWidget";
            const PROJECTION: Nil = Nil;
            const SCHEMA_VERSION: u64 = 1;

            fn originate(event: &WidgetEvent) -> Option<Self> {
                match event {
                    WidgetEvent::Created { name } => Some(Self { name: name.clone() }),
                    WidgetEvent::Renamed { .. } => None,
                }
            }

            fn evolve(_entity: &Self, event: &WidgetEvent) -> Result<Option<Self>, WidgetError> {
                match event {
                    WidgetEvent::Created { .. } => Ok(None),
                    WidgetEvent::Renamed { name } => Ok(Some(Self { name: name.clone() })),
                }
            }

            async fn initialize(
                command: WidgetCommand,
                _services: &(),
            ) -> Result<Vec<WidgetEvent>, WidgetError> {
                match command {
                    WidgetCommand::Create { name } => Ok(vec![WidgetEvent::Created { name }]),
                    WidgetCommand::Rename { name } => Ok(vec![WidgetEvent::Renamed { name }]),
                }
            }

            async fn transition(
                &self,
                command: WidgetCommand,
                _services: &(),
            ) -> Result<Vec<WidgetEvent>, WidgetError> {
                match command {
                    WidgetCommand::Create { .. } => Ok(vec![]),
                    WidgetCommand::Rename { name } => Ok(vec![WidgetEvent::Renamed { name }]),
                }
            }
        }

        let pool = test_pool().await;
        let store = testing::test_store::<RetainedWidget>(pool.clone(), ());
        store
            .send(
                &NumericId(7),
                WidgetCommand::Create {
                    name: "kept".to_string(),
                },
            )
            .await
            .unwrap();

        let deleted = compact_events::<RetainedWidget>(&pool).await.unwrap();

        assert_eq!(deleted, 0);
        let event_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM events \
             WHERE aggregate_type = 'RetainedWidget' AND aggregate_id = '7'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(event_count, 1);
    }

    #[tokio::test]
    async fn load_entity_returns_none_when_no_events() {
        let pool = test_pool().await;

        let entity = load_entity::<Widget>(&pool, &NumericId(999)).await.unwrap();

        assert!(entity.is_none(), "expected None for nonexistent aggregate");
    }

    #[tokio::test]
    async fn load_all_ids_returns_parsed_ids() {
        let pool = test_pool().await;
        insert_event(&pool, "Widget", "10").await;
        insert_event(&pool, "Widget", "20").await;

        let ids = load_all_ids::<Widget>(&pool).await.unwrap();

        assert_eq!(ids, vec![NumericId(10), NumericId(20)]);
    }

    #[tokio::test]
    async fn load_all_ids_includes_snapshot_only_aggregates() {
        let pool = test_pool().await;
        let store = testing::test_store::<Widget>(pool.clone(), ());
        store
            .send(
                &NumericId(30),
                WidgetCommand::Create {
                    name: "snapshot-only".to_string(),
                },
            )
            .await
            .unwrap();
        compact_events::<Widget>(&pool).await.unwrap();

        let ids = load_all_ids::<Widget>(&pool).await.unwrap();

        assert_eq!(ids, vec![NumericId(30)]);
    }

    #[tokio::test]
    async fn load_all_ids_returns_empty_when_no_events() {
        let pool = test_pool().await;

        let ids = load_all_ids::<Widget>(&pool).await.unwrap();

        assert!(ids.is_empty());
    }

    #[tokio::test]
    async fn load_all_ids_errors_on_unparseable_id() {
        let pool = test_pool().await;
        insert_event(&pool, "Widget", "42").await;
        insert_event(&pool, "Widget", "not-a-number").await;

        let error = load_all_ids::<Widget>(&pool)
            .await
            .expect_err("should fail when an ID cannot parse");

        match error {
            LoadAllIdsError::InvalidIds {
                aggregate_type,
                ids,
            } => {
                assert_eq!(aggregate_type, "Widget");
                assert_eq!(ids, vec!["not-a-number"]);
            }
            LoadAllIdsError::Sql(sql_error) => {
                panic!("expected InvalidIds, got Sql: {sql_error}")
            }
            LoadAllIdsError::NumericConversion(conv_error) => {
                panic!("expected InvalidIds, got NumericConversion: {conv_error}")
            }
        }
    }

    #[tokio::test]
    async fn load_all_ids_ignores_other_aggregate_types() {
        let pool = test_pool().await;
        insert_event(&pool, "Widget", "1").await;
        insert_event(&pool, "OtherAggregate", "should-be-excluded").await;

        let ids = load_all_ids::<Widget>(&pool).await.unwrap();

        assert_eq!(ids, vec![NumericId(1)]);
    }

    #[tokio::test]
    async fn count_aggregates_includes_snapshot_only_aggregates() {
        let pool = test_pool().await;
        let store = testing::test_store::<Widget>(pool.clone(), ());
        store
            .send(
                &NumericId(30),
                WidgetCommand::Create {
                    name: "snapshot-only".to_string(),
                },
            )
            .await
            .unwrap();
        compact_events::<Widget>(&pool).await.unwrap();

        let count = count_aggregates::<Widget>(&pool).await.unwrap();

        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn per_aggregate_locks_serializes_same_key() {
        let locks = Arc::new(PerAggregateLocks::default());
        let order = Arc::new(SyncMutex::new(Vec::new()));
        let (acquired_tx, acquired_rx) = oneshot::channel();
        let release = Arc::new(Notify::new());

        let holder_locks = Arc::clone(&locks);
        let holder_order = Arc::clone(&order);
        let holder_release = Arc::clone(&release);
        let holder = tokio::spawn(async move {
            let guard = holder_locks.acquire("agg-1").await;
            holder_order
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push("A-acquired");
            acquired_tx
                .send(())
                .expect("test should still be waiting on the receiver");

            holder_release.notified().await;
            holder_order
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push("A-released");
            drop(guard);
        });

        // Barrier: don't spawn the contender until the holder has confirmed
        // it holds the guard, so the contender's acquire() is guaranteed to
        // contend with an already-held lock rather than racing to acquire it
        // first.
        acquired_rx
            .await
            .expect("holder task should signal before completing");

        let contender_locks = Arc::clone(&locks);
        let contender_order = Arc::clone(&order);
        let contender = tokio::spawn(async move {
            let _guard = contender_locks.acquire("agg-1").await;
            contender_order
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push("B-acquired");
        });

        release.notify_one();

        holder.await.unwrap();
        contender.await.unwrap();

        assert_eq!(
            *order.lock().unwrap_or_else(PoisonError::into_inner),
            vec!["A-acquired", "A-released", "B-acquired"],
        );
    }

    #[tokio::test]
    async fn per_aggregate_locks_does_not_block_different_keys() {
        let locks = Arc::new(PerAggregateLocks::default());
        let gate = Arc::new(Notify::new());
        let (acquired_tx, acquired_rx) = oneshot::channel();

        let task_locks = Arc::clone(&locks);
        let task_gate = Arc::clone(&gate);
        let _held_forever = tokio::spawn(async move {
            let _guard = task_locks.acquire("agg-1").await;
            acquired_tx
                .send(())
                .expect("test should still be waiting on the receiver");
            task_gate.notified().await;
        });

        // Barrier: don't attempt "agg-2" until "agg-1" is confirmed held, so
        // this proves "agg-2" is unaffected by an *actually held* lock rather
        // than inferring it from a single scheduling opportunity.
        acquired_rx
            .await
            .expect("holder task should signal before completing");

        timeout(Duration::from_millis(200), locks.acquire("agg-2"))
            .await
            .expect("acquiring a different aggregate ID must not block on an unrelated held lock");
    }

    /// Order-sensitive fixture for `Store::send` serialization tests: any
    /// `Append` command is valid whether the aggregate is uninitialized or
    /// live, and the resulting state is the exact sequence of applied
    /// labels -- so out-of-order reactor dispatch is directly observable
    /// as a wrong label order, not a coincidentally-equal value.
    #[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
    struct Ledger {
        order: Vec<String>,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct LedgerEvent {
        label: String,
    }

    impl DomainEvent for LedgerEvent {
        fn event_type(&self) -> String {
            "LedgerEvent::Appended".to_string()
        }

        fn event_version(&self) -> String {
            "1.0".to_string()
        }
    }

    enum LedgerCommand {
        Append { label: String },
    }

    #[async_trait]
    impl EventSourced for Ledger {
        type Id = NumericId;
        type Event = LedgerEvent;
        type Command = LedgerCommand;
        type Error = Never;
        type Services = ();
        type Materialized = Nil;

        const AGGREGATE_TYPE: &'static str = "Ledger";
        const PROJECTION: Nil = Nil;
        const SCHEMA_VERSION: u64 = 1;

        fn originate(event: &LedgerEvent) -> Option<Self> {
            Some(Self {
                order: vec![event.label.clone()],
            })
        }

        fn evolve(entity: &Self, event: &LedgerEvent) -> Result<Option<Self>, Never> {
            let mut order = entity.order.clone();
            order.push(event.label.clone());
            Ok(Some(Self { order }))
        }

        async fn initialize(
            command: LedgerCommand,
            _services: &(),
        ) -> Result<Vec<LedgerEvent>, Never> {
            let LedgerCommand::Append { label } = command;
            Ok(vec![LedgerEvent { label }])
        }

        async fn transition(
            &self,
            command: LedgerCommand,
            _services: &(),
        ) -> Result<Vec<LedgerEvent>, Never> {
            let LedgerCommand::Append { label } = command;
            Ok(vec![LedgerEvent { label }])
        }
    }

    /// Reactor that stalls its dispatch of the event labeled `"first"` on a
    /// test-controlled latch, simulating the retry window a slow/retrying
    /// reactor opens up (see `RetryOnBusy`). Signals `committed` right
    /// before stalling, so a test can prove the corresponding command has
    /// already committed before it fires a second, concurrent command, and
    /// signals `contender_dispatched` whenever any *other* event reaches
    /// dispatch, so a test can prove that cannot happen while `"first"` is
    /// still stalled.
    struct StallFirstReactor {
        order_log: Arc<AsyncMutex<Vec<String>>>,
        committed: SyncMutex<Option<oneshot::Sender<()>>>,
        release: Notify,
        contender_dispatched: Notify,
    }

    deps!(StallFirstReactor, [Ledger]);

    #[async_trait]
    impl Reactor for StallFirstReactor {
        type Error = Never;

        async fn react(
            &self,
            event: <Self::Dependencies as EntityList>::Event,
        ) -> Result<(), Self::Error> {
            let (_id, event) = event.into_inner();

            if event.label == "first" {
                let sender = self
                    .committed
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .take();
                if let Some(sender) = sender {
                    let _ = sender.send(());
                }
                self.release.notified().await;
            } else {
                self.contender_dispatched.notify_one();
            }

            self.order_log.lock().await.push(event.label);
            Ok(())
        }
    }

    /// Wall-clock headroom given to a contending command to do its worst
    /// before a test asserts it has not.
    ///
    /// Deliberately real time, not a count of `yield_now()`s: under a broken
    /// `Store::send` the contender reaches the state these tests forbid only
    /// by completing actual SQLite work, which runs on the pool's blocking
    /// threads. Cooperative yields on the test's own task do not schedule
    /// that work at all, so no number of them bounds it -- a yield-drain can
    /// return long before a genuine regression has had the chance to show
    /// itself, and the test then passes for the wrong reason. Against an
    /// in-memory pool that work is sub-millisecond, so this leaves ~2 orders
    /// of magnitude of headroom.
    const CONTENDER_GRACE: Duration = Duration::from_millis(250);

    /// Regression test: proves `Store::send` delivers
    /// events to reactors in commit order even when the first command's
    /// dispatch stalls (simulating a `RetryOnBusy` retry window). Without
    /// per-aggregate serialization, "second" -- whose load only happens
    /// once "first" has already committed, per the barrier below -- can
    /// finish its own (non-stalled) dispatch before "first"'s stalled
    /// dispatch completes, so the reactor observes "second" before
    /// "first" even though "first" committed strictly earlier.
    #[tokio::test]
    async fn store_send_serializes_same_aggregate_dispatch_in_commit_order() {
        let pool = test_pool().await;

        let order_log: Arc<AsyncMutex<Vec<String>>> = Arc::new(AsyncMutex::new(Vec::new()));
        let (committed_tx, committed_rx) = oneshot::channel();
        let reactor = Arc::new(StallFirstReactor {
            order_log: Arc::clone(&order_log),
            committed: SyncMutex::new(Some(committed_tx)),
            release: Notify::new(),
            contender_dispatched: Notify::new(),
        });

        let store = StoreBuilder::<Ledger>::new(pool.clone())
            .with(Arc::clone(&reactor))
            .build(())
            .await
            .unwrap();

        let id = NumericId(1);

        let store_first = Arc::clone(&store);
        let id_first = id.clone();
        let first = tokio::spawn(async move {
            store_first
                .send(
                    &id_first,
                    LedgerCommand::Append {
                        label: "first".to_string(),
                    },
                )
                .await
                .unwrap();
        });

        // Barrier: "second" must not even be spawned until "first" has
        // committed and entered its stall, so "second" is guaranteed to load
        // live state (going through `transition`, not `initialize`) rather
        // than racing an uninitialized aggregate.
        committed_rx
            .await
            .expect("first's reactor should signal before stalling");

        let store_second = Arc::clone(&store);
        let id_second = id.clone();
        let second = tokio::spawn(async move {
            store_second
                .send(
                    &id_second,
                    LedgerCommand::Append {
                        label: "second".to_string(),
                    },
                )
                .await
                .unwrap();
        });

        // The invariant, asserted directly: while "first"'s dispatch is
        // stalled, "second" must not be able to reach dispatch at all. Under
        // a correct `Store::send` it is blocked on the per-aggregate lock
        // "first" still holds, so this signal can never fire and the wait is
        // guaranteed to time out. Narrow the lock to cover only dispatch and
        // "second" -- whose own dispatch does not stall -- runs straight
        // through, firing this and naming the violation on the spot, rather
        // than leaving the final commit-order assertion to be decided by
        // which task the executor happened to poll first.
        match timeout(CONTENDER_GRACE, reactor.contender_dispatched.notified()).await {
            Ok(()) => panic!(
                "\"second\" reached reactor dispatch while \"first\"'s dispatch was still \
                 stalled: Store::send is not serializing the whole execute per aggregate, \
                 so reactors can observe events out of commit order"
            ),
            Err(_elapsed) => {}
        }

        release_and_join(&reactor.release, first, second).await;

        let payloads: Vec<String> = sqlx::query_scalar(
            "SELECT payload FROM events \
             WHERE aggregate_type = 'Ledger' AND aggregate_id = '1' \
             ORDER BY sequence",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        let commit_order: Vec<String> = payloads
            .iter()
            .map(|payload| serde_json::from_str::<LedgerEvent>(payload).unwrap().label)
            .collect();

        assert_eq!(
            *order_log.lock().await,
            commit_order,
            "reactor dispatch order must match commit order for a single \
             aggregate, even when the first event's dispatch stalls",
        );
    }

    /// The projected twin of `Ledger`: same append-only label sequence, but
    /// `Materialized = Table`, so `StoreBuilder::build` wires a real
    /// `Projection` for it. Lets the ordering guarantee be asserted against an
    /// actual materialized view -- the read model whose corruption motivates
    /// per-aggregate serialization -- rather than only against a bespoke
    /// reactor's log.
    #[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
    struct ProjectedLedger {
        order: Vec<String>,
    }

    #[async_trait]
    impl EventSourced for ProjectedLedger {
        type Id = NumericId;
        type Event = LedgerEvent;
        type Command = LedgerCommand;
        type Error = Never;
        type Services = ();
        type Materialized = Table;

        const AGGREGATE_TYPE: &'static str = "ProjectedLedger";
        const PROJECTION: Table = Table("projected_ledger_view");
        const SCHEMA_VERSION: u64 = 1;

        fn originate(event: &LedgerEvent) -> Option<Self> {
            Some(Self {
                order: vec![event.label.clone()],
            })
        }

        fn evolve(entity: &Self, event: &LedgerEvent) -> Result<Option<Self>, Never> {
            let mut order = entity.order.clone();
            order.push(event.label.clone());
            Ok(Some(Self { order }))
        }

        async fn initialize(
            command: LedgerCommand,
            _services: &(),
        ) -> Result<Vec<LedgerEvent>, Never> {
            let LedgerCommand::Append { label } = command;
            Ok(vec![LedgerEvent { label }])
        }

        async fn transition(
            &self,
            command: LedgerCommand,
            _services: &(),
        ) -> Result<Vec<LedgerEvent>, Never> {
            let LedgerCommand::Append { label } = command;
            Ok(vec![LedgerEvent { label }])
        }
    }

    /// `StallFirstReactor` for `ProjectedLedger`. Registered via `.with()`, so
    /// `StoreBuilder` dispatches it *before* the projection it auto-wires
    /// (the projection's `ReactorBridge` is pushed last). Stalling here
    /// therefore holds the first command's *view update* open, which is
    /// exactly the window a retrying `Projection::react` opens in production.
    struct StallFirstProjectedReactor {
        committed: SyncMutex<Option<oneshot::Sender<()>>>,
        release: Notify,
        contender_dispatched: Notify,
    }

    deps!(StallFirstProjectedReactor, [ProjectedLedger]);

    #[async_trait]
    impl Reactor for StallFirstProjectedReactor {
        type Error = Never;

        async fn react(
            &self,
            event: <Self::Dependencies as EntityList>::Event,
        ) -> Result<(), Self::Error> {
            let (_id, event) = event.into_inner();

            if event.label == "first" {
                let sender = self
                    .committed
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .take();
                if let Some(sender) = sender {
                    let _ = sender.send(());
                }
                self.release.notified().await;
            } else {
                self.contender_dispatched.notify_one();
            }

            Ok(())
        }
    }

    async fn projected_test_pool() -> SqlitePool {
        let pool = test_pool().await;
        sqlx::query(
            "CREATE TABLE projected_ledger_view ( \
                 view_id TEXT NOT NULL PRIMARY KEY, \
                 version BIGINT NOT NULL, \
                 payload TEXT NOT NULL \
             )",
        )
        .execute(&pool)
        .await
        .unwrap();
        pool
    }

    /// The scenario this whole change exists for, asserted end to end against
    /// a real wired `Projection` rather than a bespoke reactor: two concurrent
    /// same-aggregate commands, where the first's view update is stalled (the
    /// window `Projection::react`'s retry loop opens in production).
    ///
    /// Without per-aggregate serialization, "second" commits and applies its
    /// view update against a view that has not yet seen "first", and "first"'s
    /// delayed update then lands on top -- so the materialized read model ends
    /// up ordered `["second", "first"]` while the event log says
    /// `["first", "second"]`. The view must match commit order.
    #[tokio::test]
    async fn store_send_keeps_projection_in_commit_order_when_view_update_stalls() {
        let pool = projected_test_pool().await;

        let (committed_tx, committed_rx) = oneshot::channel();
        let reactor = Arc::new(StallFirstProjectedReactor {
            committed: SyncMutex::new(Some(committed_tx)),
            release: Notify::new(),
            contender_dispatched: Notify::new(),
        });

        let (store, projection) = StoreBuilder::<ProjectedLedger>::new(pool.clone())
            .with(Arc::clone(&reactor))
            .build(())
            .await
            .unwrap();

        let id = NumericId(1);

        let store_first = Arc::clone(&store);
        let id_first = id.clone();
        let first = tokio::spawn(async move {
            store_first
                .send(
                    &id_first,
                    LedgerCommand::Append {
                        label: "first".to_string(),
                    },
                )
                .await
                .unwrap();
        });

        // Barrier: "first" has committed its event and is now stalled with its
        // view update still pending.
        committed_rx
            .await
            .expect("first's reactor should signal before stalling");

        let store_second = Arc::clone(&store);
        let id_second = id.clone();
        let second = tokio::spawn(async move {
            store_second
                .send(
                    &id_second,
                    LedgerCommand::Append {
                        label: "second".to_string(),
                    },
                )
                .await
                .unwrap();
        });

        // "second" must not reach dispatch -- and therefore must not touch the
        // view -- while "first"'s view update is still outstanding.
        match timeout(CONTENDER_GRACE, reactor.contender_dispatched.notified()).await {
            Ok(()) => panic!(
                "\"second\" reached dispatch while \"first\"'s view update was still stalled: \
                 the projection can be written out of commit order"
            ),
            Err(_elapsed) => {}
        }

        release_and_join(&reactor.release, first, second).await;

        let payloads: Vec<String> = sqlx::query_scalar(
            "SELECT payload FROM events \
             WHERE aggregate_type = 'ProjectedLedger' AND aggregate_id = '1' \
             ORDER BY sequence",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        let commit_order: Vec<String> = payloads
            .iter()
            .map(|payload| serde_json::from_str::<LedgerEvent>(payload).unwrap().label)
            .collect();

        assert_eq!(
            commit_order,
            vec!["first".to_string(), "second".to_string()],
            "the event log itself must record the commit order the barrier established",
        );

        let view = projection
            .load(&id)
            .await
            .unwrap()
            .expect("the projection must have materialized the aggregate");

        assert_eq!(
            view.order, commit_order,
            "the materialized view must apply events in commit order, even when the first \
             event's view update stalls",
        );
    }

    /// Polls the `Ledger` event count across the whole `window`, asserting it
    /// never leaves 1 -- i.e. the contending command never commits while the
    /// first command's `execute` is still in flight.
    ///
    /// A single check after `sleep(window)` would only sample the endpoint;
    /// this watches the entire interval, so a commit that lands mid-window
    /// fails the test at the moment it happens rather than needing to still be
    /// observable at the end.
    async fn assert_events_stay_at_one(pool: &SqlitePool, window: Duration) {
        const POLL_INTERVAL: Duration = Duration::from_millis(5);

        let deadline = Instant::now() + window;
        while Instant::now() < deadline {
            let committed: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM events \
                 WHERE aggregate_type = 'Ledger' AND aggregate_id = '1'",
            )
            .fetch_one(pool)
            .await
            .unwrap();

            assert_eq!(
                committed, 1,
                "second command must not even commit while first's execute \
                 (dispatch included) is still in flight",
            );

            sleep(POLL_INTERVAL).await;
        }
    }

    /// Releases a `StallFirstReactor`'s latch and awaits both spawned
    /// `Store::send` tasks. Shared by tests that only differ in what they
    /// assert afterward.
    async fn release_and_join(
        release: &Notify,
        first: tokio::task::JoinHandle<()>,
        second: tokio::task::JoinHandle<()>,
    ) {
        release.notify_one();
        first.await.unwrap();
        second.await.unwrap();
    }

    #[tokio::test]
    async fn send_serializes_same_aggregate() {
        let pool = test_pool().await;

        let order_log: Arc<AsyncMutex<Vec<String>>> = Arc::new(AsyncMutex::new(Vec::new()));
        let (committed_tx, committed_rx) = oneshot::channel();
        let reactor = Arc::new(StallFirstReactor {
            order_log: Arc::clone(&order_log),
            committed: SyncMutex::new(Some(committed_tx)),
            release: Notify::new(),
            contender_dispatched: Notify::new(),
        });

        let store = StoreBuilder::<Ledger>::new(pool.clone())
            .with(Arc::clone(&reactor))
            .build(())
            .await
            .unwrap();

        let id = NumericId(1);

        let store_first = Arc::clone(&store);
        let id_first = id.clone();
        let first = tokio::spawn(async move {
            store_first
                .send(
                    &id_first,
                    LedgerCommand::Append {
                        label: "first".to_string(),
                    },
                )
                .await
                .unwrap();
        });

        committed_rx
            .await
            .expect("first's reactor should signal before stalling");

        let store_second = Arc::clone(&store);
        let id_second = id.clone();
        let second = tokio::spawn(async move {
            store_second
                .send(
                    &id_second,
                    LedgerCommand::Append {
                        label: "second".to_string(),
                    },
                )
                .await
                .unwrap();
        });

        // If `Store::send` only serialized dispatch (not the whole
        // `execute`), "second" could load, handle and commit right here,
        // since committing does not require the reactor to have run -- it
        // would then block on the dispatch lock, never reaching the reactor,
        // so `contender_dispatched` cannot detect this one. The events table
        // is the observable, and reaching it means real SQLite work: give
        // "second" genuine wall-clock room to do it, and actively watch the
        // whole window rather than sampling once at the end of a sleep, so a
        // commit that lands and is later overtaken cannot slip past. With the
        // whole-`execute` lock "second" cannot even begin loading.
        assert_events_stay_at_one(&pool, CONTENDER_GRACE).await;

        release_and_join(&reactor.release, first, second).await;

        let committed_after_release: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM events WHERE aggregate_type = 'Ledger' AND aggregate_id = '1'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(committed_after_release, 2);
    }

    #[tokio::test]
    async fn send_does_not_serialize_different_aggregates() {
        let pool = test_pool().await;

        let order_log: Arc<AsyncMutex<Vec<String>>> = Arc::new(AsyncMutex::new(Vec::new()));
        let (committed_tx, committed_rx) = oneshot::channel();
        let reactor = Arc::new(StallFirstReactor {
            order_log: Arc::clone(&order_log),
            committed: SyncMutex::new(Some(committed_tx)),
            release: Notify::new(),
            contender_dispatched: Notify::new(),
        });

        let store = StoreBuilder::<Ledger>::new(pool.clone())
            .with(Arc::clone(&reactor))
            .build(())
            .await
            .unwrap();

        let store_first = Arc::clone(&store);
        let first = tokio::spawn(async move {
            store_first
                .send(
                    &NumericId(1),
                    LedgerCommand::Append {
                        label: "first".to_string(),
                    },
                )
                .await
                .unwrap();
        });

        committed_rx
            .await
            .expect("first's reactor should signal before stalling");

        // Aggregate "2" is a different aggregate ID from the stalled
        // aggregate "1"; its `send` must complete promptly even while "1"'s
        // command is still stalled in dispatch.
        timeout(
            Duration::from_millis(500),
            store.send(
                &NumericId(2),
                LedgerCommand::Append {
                    label: "other".to_string(),
                },
            ),
        )
        .await
        .expect("a different aggregate ID must not block on an unrelated held lock")
        .unwrap();

        reactor.release.notify_one();
        first.await.unwrap();
    }

    /// Reports the result of `SelfCommandingReactor`'s inner, self-directed
    /// `Store::send()` back to the test.
    type SelfSendResultSender = oneshot::Sender<Result<(), SendError<Ledger>>>;

    /// Reactor that, upon reacting to a `Ledger` event, immediately calls
    /// `Store::send()` back onto the *same* aggregate ID it is reacting to --
    /// the exact self-cycle the task-local reentrancy guard exists to catch.
    /// The inner `send()`'s result is reported over a oneshot rather than
    /// through `react()`'s own return type, since `Ledger::Error` is `Never`
    /// (uninhabited) and cannot carry it.
    struct SelfCommandingReactor {
        store: OnceCell<Arc<Store<Ledger>>>,
        inner_result: SyncMutex<Option<SelfSendResultSender>>,
    }

    deps!(SelfCommandingReactor, [Ledger]);

    #[async_trait]
    impl Reactor for SelfCommandingReactor {
        type Error = Never;

        async fn react(
            &self,
            event: <Self::Dependencies as EntityList>::Event,
        ) -> Result<(), Self::Error> {
            let (id, _event) = event.into_inner();
            let store = self
                .store
                .get()
                .expect("store must be set before the reactor can be dispatched");

            let result = store
                .send(
                    &id,
                    LedgerCommand::Append {
                        label: "reentrant".to_string(),
                    },
                )
                .await;

            let sender = self
                .inner_result
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take();
            if let Some(sender) = sender {
                let _ = sender.send(result);
            }

            Ok(())
        }
    }

    /// Regression test for the reentrancy hazard `Store::send`'s
    /// per-aggregate lock introduces (see ADR-0004 and the `Store` type-level
    /// doc comment): a reactor commanding the same aggregate it is reacting
    /// to would deadlock on the lock its own outer command already holds.
    /// Proves the guard turns that into a fast, typed error instead --
    /// wrapped in a short `timeout` so a regression that reintroduces the
    /// deadlock fails the test instead of hanging the suite.
    #[tokio::test]
    async fn reactor_self_commanding_same_aggregate_fails_fast_instead_of_deadlocking() {
        let pool = test_pool().await;

        let (result_tx, result_rx) = oneshot::channel();
        let reactor = Arc::new(SelfCommandingReactor {
            store: OnceCell::new(),
            inner_result: SyncMutex::new(Some(result_tx)),
        });

        let store = StoreBuilder::<Ledger>::new(pool.clone())
            .with(Arc::clone(&reactor))
            .build(())
            .await
            .unwrap();
        let Ok(()) = reactor.store.set(Arc::clone(&store)) else {
            panic!("store should only be set once, before any dispatch");
        };

        let outer_result = timeout(
            Duration::from_secs(2),
            store.send(
                &NumericId(1),
                LedgerCommand::Append {
                    label: "outer".to_string(),
                },
            ),
        )
        .await
        .expect("a self-commanding reactor must fail fast, not hang the outer send");
        outer_result.expect("the outer command itself must still succeed and commit");

        let inner_result = timeout(Duration::from_secs(2), result_rx)
            .await
            .expect("reactor should report its inner send's result well within the timeout")
            .expect("inner_result sender must not be dropped without sending");

        let error =
            inner_result.expect_err("self-commanding the same aggregate must fail, not succeed");
        assert!(
            matches!(
                error,
                AggregateError::UserError(LifecycleError::ReentrantCommand)
            ),
            "expected AggregateError::UserError(ReentrantCommand), got: {error:?}",
        );
    }

    /// Reactor that, upon reacting to a `Ledger` event, calls `Store::send()`
    /// onto a *different* aggregate ID than the one it is reacting to -- the
    /// normal cross-aggregate orchestration pattern (see `RebalancingTrigger`
    /// in docs/cqrs.md) the reentrancy guard must leave alone even while the
    /// task-local held-set is non-empty (populated by the outer command this
    /// reactor is dispatched from). Reports the inner send's result over a
    /// oneshot for the same reason `SelfCommandingReactor` does.
    struct DifferentAggregateReactor {
        store: OnceCell<Arc<Store<Ledger>>>,
        target: NumericId,
        inner_result: SyncMutex<Option<SelfSendResultSender>>,
    }

    deps!(DifferentAggregateReactor, [Ledger]);

    #[async_trait]
    impl Reactor for DifferentAggregateReactor {
        type Error = Never;

        async fn react(
            &self,
            event: <Self::Dependencies as EntityList>::Event,
        ) -> Result<(), Self::Error> {
            let (id, _event) = event.into_inner();

            // This reactor is registered for every `Ledger` event, including
            // the one its own orchestrated command produces on `target`.
            // Without this guard, reacting to `target`'s own event would
            // re-enter this branch and self-command `target` while already
            // reacting to it -- a genuine reentrancy case, not the
            // different-aggregate case this test exercises.
            if id == self.target {
                return Ok(());
            }

            let store = self
                .store
                .get()
                .expect("store must be set before the reactor can be dispatched");

            let result = store
                .send(
                    &self.target,
                    LedgerCommand::Append {
                        label: "orchestrated".to_string(),
                    },
                )
                .await;

            let sender = self
                .inner_result
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take();
            if let Some(sender) = sender {
                let _ = sender.send(result);
            }

            Ok(())
        }
    }

    /// Proves the reentrancy guard's `Admission::Proceed` branch (populated
    /// held-set, different key -> proceed) is not broadened into rejecting
    /// every command issued from within a reactor.
    /// `send_does_not_serialize_different_aggregates` exercises a
    /// different-aggregate `send()` from the *test's own task*, where
    /// `HELD_AGGREGATE_LOCKS` is an empty/fresh scope; this test instead
    /// exercises it from *inside* a reactor's inline dispatch, where the
    /// held-set already contains the outer aggregate's key -- the only place
    /// `Proceed` under a populated ancestor scope is reachable, and the exact
    /// shape of the `RebalancingTrigger` cross-aggregate orchestration pattern
    /// documented on `Store`.
    #[tokio::test]
    async fn reactor_commanding_different_aggregate_from_inline_dispatch_succeeds() {
        let pool = test_pool().await;

        let (result_tx, result_rx) = oneshot::channel();
        let reactor = Arc::new(DifferentAggregateReactor {
            store: OnceCell::new(),
            target: NumericId(2),
            inner_result: SyncMutex::new(Some(result_tx)),
        });

        let store = StoreBuilder::<Ledger>::new(pool.clone())
            .with(Arc::clone(&reactor))
            .build(())
            .await
            .unwrap();
        let Ok(()) = reactor.store.set(Arc::clone(&store)) else {
            panic!("store should only be set once, before any dispatch");
        };

        let outer_result = timeout(
            Duration::from_secs(2),
            store.send(
                &NumericId(1),
                LedgerCommand::Append {
                    label: "outer".to_string(),
                },
            ),
        )
        .await
        .expect("orchestrating a different aggregate must not hang");
        outer_result.expect("the outer command itself must succeed and commit");

        let inner_result = timeout(Duration::from_secs(2), result_rx)
            .await
            .expect("reactor should report its inner send's result well within the timeout")
            .expect("inner_result sender must not be dropped without sending");
        inner_result.expect(
            "commanding a different aggregate from inside a reactor must succeed, \
             not be rejected as reentrant",
        );

        let target_entity = load_entity::<Ledger>(&pool, &NumericId(2))
            .await
            .unwrap()
            .expect("the orchestrated aggregate's event must be committed");
        assert_eq!(target_entity.order, vec!["orchestrated".to_string()]);
    }

    /// Reports the results of `SiblingSendingReactor`'s two joined, sibling
    /// `Store::send()` calls back to the test.
    type SiblingSendResultsSender =
        oneshot::Sender<(Result<(), SendError<Ledger>>, Result<(), SendError<Ledger>>)>;

    /// Reactor that, upon reacting to a `Ledger` event, issues two *sibling*
    /// `Store::send()` calls to the same *different* aggregate concurrently
    /// via `tokio::join!` -- the exact shape the reentrancy guard must not
    /// spuriously reject: both calls inherit the same ancestor task-local
    /// snapshot (the outer aggregate's key), so neither has committed a key
    /// the other can see yet, and they must queue on `PerAggregateLocks`
    /// instead of one being rejected as reentrant.
    struct SiblingSendingReactor {
        store: OnceCell<Arc<Store<Ledger>>>,
        target: NumericId,
        results: SyncMutex<Option<SiblingSendResultsSender>>,
    }

    deps!(SiblingSendingReactor, [Ledger]);

    #[async_trait]
    impl Reactor for SiblingSendingReactor {
        type Error = Never;

        async fn react(
            &self,
            event: <Self::Dependencies as EntityList>::Event,
        ) -> Result<(), Self::Error> {
            let (id, _event) = event.into_inner();

            // Guard against re-entering on the target aggregate's own
            // resulting events, mirroring `DifferentAggregateReactor` --
            // this test is about sibling concurrency, not self-commanding.
            if id == self.target {
                return Ok(());
            }

            let store = self
                .store
                .get()
                .expect("store must be set before the reactor can be dispatched");

            let (result_one, result_two) = tokio::join!(
                store.send(
                    &self.target,
                    LedgerCommand::Append {
                        label: "sibling-a".to_string(),
                    },
                ),
                store.send(
                    &self.target,
                    LedgerCommand::Append {
                        label: "sibling-b".to_string(),
                    },
                ),
            );

            let sender = self
                .results
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take();
            if let Some(sender) = sender {
                let _ = sender.send((result_one, result_two));
            }

            Ok(())
        }
    }

    /// Regression test for the finding that sibling `send()` calls to the
    /// same aggregate, joined within a single reactor's inline dispatch, were
    /// spuriously rejected as reentrant instead of queuing: `send_under_lock`
    /// used to insert its key into the *shared* inherited task-local set
    /// before acquiring the per-aggregate mutex, so the second sibling
    /// `tokio::join!` polled would observe the first's not-yet-acquired key
    /// and fail fast even though there is no ancestor relationship (and thus
    /// no deadlock) between the two -- only ordinary contention.
    #[tokio::test]
    async fn reactor_joining_sibling_sends_to_same_aggregate_queues() {
        let pool = test_pool().await;

        let (results_tx, results_rx) = oneshot::channel();
        let reactor = Arc::new(SiblingSendingReactor {
            store: OnceCell::new(),
            target: NumericId(2),
            results: SyncMutex::new(Some(results_tx)),
        });

        let store = StoreBuilder::<Ledger>::new(pool.clone())
            .with(Arc::clone(&reactor))
            .build(())
            .await
            .unwrap();
        let Ok(()) = reactor.store.set(Arc::clone(&store)) else {
            panic!("store should only be set once, before any dispatch");
        };

        let outer_result = timeout(
            Duration::from_secs(2),
            store.send(
                &NumericId(1),
                LedgerCommand::Append {
                    label: "outer".to_string(),
                },
            ),
        )
        .await
        .expect("joining sibling sends to the same aggregate must not hang");
        outer_result.expect("the outer command itself must still succeed and commit");

        let (result_one, result_two) = timeout(Duration::from_secs(2), results_rx)
            .await
            .expect("reactor should report both sibling results well within the timeout")
            .expect("results sender must not be dropped without sending");

        result_one
            .expect("a sibling send must queue behind its sibling, not be rejected as reentrant");
        result_two
            .expect("a sibling send must queue behind its sibling, not be rejected as reentrant");

        let target_entity = load_entity::<Ledger>(&pool, &NumericId(2))
            .await
            .unwrap()
            .expect("both sibling commands' events must be committed");
        assert_eq!(
            target_entity.order.len(),
            2,
            "both sibling commands must have committed exactly once each, got: {:?}",
            target_entity.order,
        );
        assert!(
            target_entity.order.contains(&"sibling-a".to_string())
                && target_entity.order.contains(&"sibling-b".to_string()),
            "both sibling labels must appear in the committed order, got: {:?}",
            target_entity.order,
        );
    }

    /// Reactor that forms a two-aggregate, same-task cycle back to itself:
    /// reacting to aggregate 1's event, it commands aggregate 2 inline; then,
    /// reacting (in the same task, since dispatch is always inline-awaited)
    /// to aggregate 2's event, it commands back onto aggregate 1 -- which is
    /// still held by the outermost `send()` on the call stack. Proves the
    /// guard's *transitive* detection: the innermost self-command fails fast
    /// even though it isn't a *direct* self-command, just a link in a chain
    /// that closes back onto an ancestor key.
    struct CyclingReactor {
        store: OnceCell<Arc<Store<Ledger>>>,
        cycle_result: SyncMutex<Option<SelfSendResultSender>>,
    }

    deps!(CyclingReactor, [Ledger]);

    #[async_trait]
    impl Reactor for CyclingReactor {
        type Error = Never;

        async fn react(
            &self,
            event: <Self::Dependencies as EntityList>::Event,
        ) -> Result<(), Self::Error> {
            let (id, event) = event.into_inner();
            let store = self
                .store
                .get()
                .expect("store must be set before the reactor can be dispatched");

            match (id, event.label.as_str()) {
                (NumericId(1), "outer") => {
                    store
                        .send(
                            &NumericId(2),
                            LedgerCommand::Append {
                                label: "hop".to_string(),
                            },
                        )
                        .await
                        .expect("hopping to an unheld aggregate must succeed");
                }
                (NumericId(2), _) => {
                    let result = store
                        .send(
                            &NumericId(1),
                            LedgerCommand::Append {
                                label: "cycle-back".to_string(),
                            },
                        )
                        .await;
                    let sender = self
                        .cycle_result
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .take();
                    if let Some(sender) = sender {
                        let _ = sender.send(result);
                    }
                }
                _ => {}
            }

            Ok(())
        }
    }

    /// Regression test for the *transitive* branch of the reentrancy rule
    /// (see the `Store` type-level doc comment and ADR-0004): a chain of
    /// inline-awaited reactor dispatches that closes back onto an ancestor
    /// aggregate ID must fail fast, even though the closing `send()` is not
    /// itself a direct self-command.
    #[tokio::test]
    async fn reactor_transitive_cycle_through_second_aggregate_fails_fast() {
        let pool = test_pool().await;

        let (result_tx, result_rx) = oneshot::channel();
        let reactor = Arc::new(CyclingReactor {
            store: OnceCell::new(),
            cycle_result: SyncMutex::new(Some(result_tx)),
        });

        let store = StoreBuilder::<Ledger>::new(pool.clone())
            .with(Arc::clone(&reactor))
            .build(())
            .await
            .unwrap();
        let Ok(()) = reactor.store.set(Arc::clone(&store)) else {
            panic!("store should only be set once, before any dispatch");
        };

        let outer_result = timeout(
            Duration::from_secs(2),
            store.send(
                &NumericId(1),
                LedgerCommand::Append {
                    label: "outer".to_string(),
                },
            ),
        )
        .await
        .expect("a transitively self-commanding chain must fail fast, not hang");
        outer_result.expect("the outer command itself must still succeed and commit");

        let cycle_result = timeout(Duration::from_secs(2), result_rx)
            .await
            .expect("the cycle-closing reactor should report its result well within the timeout")
            .expect("cycle_result sender must not be dropped without sending");

        let error = cycle_result
            .expect_err("closing the cycle back onto aggregate 1 must fail, not succeed");
        assert!(
            matches!(
                error,
                AggregateError::UserError(LifecycleError::ReentrantCommand)
            ),
            "expected AggregateError::UserError(ReentrantCommand), got: {error:?}",
        );
    }

    /// Reactor that issues two *sibling* `Store::send()` calls, joined within
    /// one `react()`, both targeting the aggregate it is *currently reacting
    /// to*. The mirror of `SiblingSendingReactor`, which deliberately aims its
    /// siblings at a different aggregate: here the target key is already in
    /// the snapshot both siblings inherited, so each must be rejected as
    /// reentrant rather than queued. Queuing them instead would deadlock --
    /// they would be waiting on a lock their own ancestor `send()` holds.
    struct SiblingSendingToSourceReactor {
        store: OnceCell<Arc<Store<Ledger>>>,
        results: SyncMutex<Option<SiblingSendResultsSender>>,
    }

    deps!(SiblingSendingToSourceReactor, [Ledger]);

    #[async_trait]
    impl Reactor for SiblingSendingToSourceReactor {
        type Error = Never;

        async fn react(
            &self,
            event: <Self::Dependencies as EntityList>::Event,
        ) -> Result<(), Self::Error> {
            let (id, _event) = event.into_inner();

            let store = self
                .store
                .get()
                .expect("store must be set before the reactor can be dispatched");

            // Both siblings target `id` -- the aggregate being reacted to --
            // so no new events are produced and this reactor cannot re-enter.
            let (result_one, result_two) = tokio::join!(
                store.send(
                    &id,
                    LedgerCommand::Append {
                        label: "sibling-to-source-a".to_string(),
                    },
                ),
                store.send(
                    &id,
                    LedgerCommand::Append {
                        label: "sibling-to-source-b".to_string(),
                    },
                ),
            );

            let sender = self
                .results
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take();
            if let Some(sender) = sender {
                let _ = sender.send((result_one, result_two));
            }

            Ok(())
        }
    }

    /// Asserts one sibling `send()` was rejected by the reentrancy guard
    /// rather than queued on the per-aggregate lock.
    fn assert_rejected_as_reentrant(result: Result<(), SendError<Ledger>>) {
        let error = result.expect_err(
            "a sibling aimed at the aggregate under reaction must be rejected as reentrant, \
             not queued",
        );
        assert!(
            matches!(
                error,
                AggregateError::UserError(LifecycleError::ReentrantCommand)
            ),
            "expected AggregateError::UserError(ReentrantCommand), got: {error:?}",
        );
    }

    /// The other half of the sibling contract that
    /// `reactor_joining_sibling_sends_to_same_aggregate_queues` pins. Siblings
    /// aimed at a *different* aggregate queue; siblings aimed at the aggregate
    /// currently under reaction must each fail fast with `ReentrantCommand`,
    /// because that key *is* in the snapshot they inherited. Without this the
    /// sibling fix could silently regress into queuing them, reintroducing the
    /// deadlock the guard exists to prevent.
    #[tokio::test]
    async fn reactor_joining_sibling_sends_to_reacted_aggregate_fails_fast() {
        let pool = test_pool().await;

        let (results_tx, results_rx) = oneshot::channel();
        let reactor = Arc::new(SiblingSendingToSourceReactor {
            store: OnceCell::new(),
            results: SyncMutex::new(Some(results_tx)),
        });

        let store = StoreBuilder::<Ledger>::new(pool.clone())
            .with(Arc::clone(&reactor))
            .build(())
            .await
            .unwrap();
        let Ok(()) = reactor.store.set(Arc::clone(&store)) else {
            panic!("store should only be set once, before any dispatch");
        };

        let outer_result = timeout(
            Duration::from_secs(2),
            store.send(
                &NumericId(1),
                LedgerCommand::Append {
                    label: "outer".to_string(),
                },
            ),
        )
        .await
        .expect("siblings aimed at the reacted aggregate must fail fast, not deadlock");
        outer_result.expect("the outer command itself must still succeed and commit");

        let (result_one, result_two) = timeout(Duration::from_secs(2), results_rx)
            .await
            .expect("reactor should report both sibling results well within the timeout")
            .expect("results sender must not be dropped without sending");

        assert_rejected_as_reentrant(result_one);
        assert_rejected_as_reentrant(result_two);

        // Rejected means rejected: neither sibling may have committed an event.
        let entity = load_entity::<Ledger>(&pool, &NumericId(1))
            .await
            .unwrap()
            .expect("the outer command's event must be committed");
        assert_eq!(
            entity.order,
            vec!["outer".to_string()],
            "no rejected sibling command may have committed an event",
        );
    }

    /// Reactor that, from a *single* `react()` body, hops to a different
    /// aggregate and awaits it, and only then sends back to the aggregate it
    /// is reacting to. Distinct from `CyclingReactor`, which closes its cycle
    /// from the *nested* dispatch (while reacting to aggregate 2): here the
    /// closing send is issued by the original `react()` itself, after the
    /// intervening hop has fully completed and its nested scope has been
    /// popped. Pins that the inherited held-set survives an intervening
    /// cross-aggregate hop rather than being clobbered by it.
    struct TwoStepReactor {
        store: OnceCell<Arc<Store<Ledger>>>,
        target: NumericId,
        source_result: SyncMutex<Option<SelfSendResultSender>>,
    }

    deps!(TwoStepReactor, [Ledger]);

    #[async_trait]
    impl Reactor for TwoStepReactor {
        type Error = Never;

        async fn react(
            &self,
            event: <Self::Dependencies as EntityList>::Event,
        ) -> Result<(), Self::Error> {
            let (id, _event) = event.into_inner();

            // Don't re-enter on the hop's own resulting event -- this test is
            // about the follow-up send from the *original* react() body.
            if id == self.target {
                return Ok(());
            }

            let store = self
                .store
                .get()
                .expect("store must be set before the reactor can be dispatched");

            store
                .send(
                    &self.target,
                    LedgerCommand::Append {
                        label: "hop".to_string(),
                    },
                )
                .await
                .expect("hopping to an unheld aggregate must succeed");

            // The hop above has completed and its scope popped. `id` is still
            // held by the outer `send()` that dispatched this reactor, so this
            // must still be rejected.
            let result = store
                .send(
                    &id,
                    LedgerCommand::Append {
                        label: "back-to-source".to_string(),
                    },
                )
                .await;

            let sender = self
                .source_result
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take();
            if let Some(sender) = sender {
                let _ = sender.send(result);
            }

            Ok(())
        }
    }

    /// A send back to the source aggregate must be rejected for as long as the
    /// outer `execute()` holds it -- not just when it is the reactor's first
    /// action. An intervening, fully-awaited hop to another aggregate must not
    /// launder the held-set and let the follow-up through.
    #[tokio::test]
    async fn reactor_sending_back_to_source_after_cross_aggregate_hop_fails_fast() {
        let pool = test_pool().await;

        let (result_tx, result_rx) = oneshot::channel();
        let reactor = Arc::new(TwoStepReactor {
            store: OnceCell::new(),
            target: NumericId(2),
            source_result: SyncMutex::new(Some(result_tx)),
        });

        let store = StoreBuilder::<Ledger>::new(pool.clone())
            .with(Arc::clone(&reactor))
            .build(())
            .await
            .unwrap();
        let Ok(()) = reactor.store.set(Arc::clone(&store)) else {
            panic!("store should only be set once, before any dispatch");
        };

        let outer_result = timeout(
            Duration::from_secs(2),
            store.send(
                &NumericId(1),
                LedgerCommand::Append {
                    label: "outer".to_string(),
                },
            ),
        )
        .await
        .expect("a send back to the source after a hop must fail fast, not hang");
        outer_result.expect("the outer command itself must still succeed and commit");

        let source_result = timeout(Duration::from_secs(2), result_rx)
            .await
            .expect("the reactor should report its follow-up result well within the timeout")
            .expect("source_result sender must not be dropped without sending");

        let error = source_result
            .expect_err("sending back to the still-held source aggregate must fail, not succeed");
        assert!(
            matches!(
                error,
                AggregateError::UserError(LifecycleError::ReentrantCommand)
            ),
            "expected AggregateError::UserError(ReentrantCommand), got: {error:?}",
        );

        // The intervening hop must still have committed -- the guard rejects
        // only the send that closes back onto a held key.
        let hop_entity = load_entity::<Ledger>(&pool, &NumericId(2))
            .await
            .unwrap()
            .expect("the intervening hop's event must be committed");
        assert_eq!(hop_entity.order, vec!["hop".to_string()]);

        let source_entity = load_entity::<Ledger>(&pool, &NumericId(1))
            .await
            .unwrap()
            .expect("the outer command's event must be committed");
        assert_eq!(
            source_entity.order,
            vec!["outer".to_string()],
            "the rejected follow-up must not have committed an event",
        );
    }

    /// Reactor for `Ledger` that, while reacting to aggregate `"1"`, commands
    /// a *different* `EventSourced` entity type (`Widget`) using the exact
    /// same ID string ("1"). Proves the `HELD_AGGREGATE_LOCKS` key -- keyed
    /// by `AggregateLockKey`'s `TypeId` and `String` fields, not a formatted
    /// string -- does not treat two different `(Entity, id)` pairs that
    /// share an ID representation as the same held key: `Store<Widget>::send`
    /// must not be spuriously rejected as reentrant just because
    /// `Store<Ledger>` currently holds the ID string "1".
    /// Reports the result of `CrossEntityTypeReactor`'s inner `Store<Widget>`
    /// send back to the test.
    type WidgetSendResultSender = oneshot::Sender<Result<(), SendError<Widget>>>;

    struct CrossEntityTypeReactor {
        widget_store: OnceCell<Arc<Store<Widget>>>,
        inner_result: SyncMutex<Option<WidgetSendResultSender>>,
    }

    deps!(CrossEntityTypeReactor, [Ledger]);

    #[async_trait]
    impl Reactor for CrossEntityTypeReactor {
        type Error = Never;

        async fn react(
            &self,
            event: <Self::Dependencies as EntityList>::Event,
        ) -> Result<(), Self::Error> {
            let (id, _event) = event.into_inner();
            let widget_store = self
                .widget_store
                .get()
                .expect("widget store must be set before the reactor can be dispatched");

            let result = widget_store
                .send(
                    &id,
                    WidgetCommand::Create {
                        name: "cross-entity".to_string(),
                    },
                )
                .await;

            let sender = self
                .inner_result
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take();
            if let Some(sender) = sender {
                let _ = sender.send(result);
            }

            Ok(())
        }
    }

    /// Proves `HELD_AGGREGATE_LOCKS`'s `AggregateLockKey` keying prevents a
    /// false-positive reentrancy rejection across different entity types
    /// that happen to share an ID string: `Ledger` and `Widget` both use
    /// `NumericId` as their `Id`, so aggregate "1" of each renders to the
    /// same string, but they must not collide on the held-set key.
    #[tokio::test]
    async fn reactor_commanding_different_entity_type_with_same_id_string_succeeds() {
        let pool = test_pool().await;

        let (result_tx, result_rx) = oneshot::channel();
        let reactor = Arc::new(CrossEntityTypeReactor {
            widget_store: OnceCell::new(),
            inner_result: SyncMutex::new(Some(result_tx)),
        });

        let ledger_store = StoreBuilder::<Ledger>::new(pool.clone())
            .with(Arc::clone(&reactor))
            .build(())
            .await
            .unwrap();
        let widget_store = Arc::new(testing::test_store::<Widget>(pool.clone(), ()));
        let Ok(()) = reactor.widget_store.set(widget_store) else {
            panic!("widget store should only be set once, before any dispatch");
        };

        let outer_result = timeout(
            Duration::from_secs(2),
            ledger_store.send(
                &NumericId(1),
                LedgerCommand::Append {
                    label: "outer".to_string(),
                },
            ),
        )
        .await
        .expect("commanding a different entity type with a colliding ID string must not hang");
        outer_result.expect("the outer Ledger command itself must succeed and commit");

        let inner_result = timeout(Duration::from_secs(2), result_rx)
            .await
            .expect("reactor should report its inner send's result well within the timeout")
            .expect("inner_result sender must not be dropped without sending");
        inner_result.expect(
            "a different entity type sharing an ID string with the held aggregate must not \
             be rejected as reentrant",
        );

        let widget = load_entity::<Widget>(&pool, &NumericId(1))
            .await
            .unwrap()
            .expect("the Widget aggregate's event must be committed");
        assert_eq!(widget.name, "cross-entity");
    }

    /// Regression test for the RAII release claims on
    /// `HELD_AGGREGATE_LOCKS`'s nested `.scope()`/`PerAggregateLocks::acquire`
    /// (see their doc comments): dropping an in-flight `Store::send` future --
    /// here, via `JoinHandle::abort()` on a task stalled *inside* a reactor
    /// while holding the per-aggregate lock -- must release both the mutex
    /// guard and the nested task-local scope holding its key, so a subsequent
    /// same-aggregate `send()` proceeds instead of hanging on an orphaned
    /// lock.
    #[tokio::test]
    async fn send_releases_lock_on_task_cancellation() {
        let pool = test_pool().await;

        let (committed_tx, committed_rx) = oneshot::channel();
        let reactor = Arc::new(StallFirstReactor {
            order_log: Arc::new(AsyncMutex::new(Vec::new())),
            committed: SyncMutex::new(Some(committed_tx)),
            release: Notify::new(),
            contender_dispatched: Notify::new(),
        });

        let store = StoreBuilder::<Ledger>::new(pool.clone())
            .with(Arc::clone(&reactor))
            .build(())
            .await
            .unwrap();

        let id = NumericId(1);

        let store_stalled = Arc::clone(&store);
        let id_stalled = id.clone();
        let stalled = tokio::spawn(async move {
            let _ = store_stalled
                .send(
                    &id_stalled,
                    LedgerCommand::Append {
                        label: "first".to_string(),
                    },
                )
                .await;
        });

        committed_rx
            .await
            .expect("first's reactor should signal before stalling");

        // Cancel the task while it holds the per-aggregate lock: its reactor
        // is stalled awaiting `reactor.release`, which this test never
        // notifies, so the abort must interrupt it there.
        stalled.abort();
        let join_error = stalled
            .await
            .expect_err("an aborted task must yield a JoinError");
        assert!(
            join_error.is_cancelled(),
            "the task should have been cancelled by abort(), not panicked: {join_error:?}",
        );

        let second_result = timeout(
            Duration::from_secs(2),
            store.send(
                &id,
                LedgerCommand::Append {
                    label: "second".to_string(),
                },
            ),
        )
        .await
        .expect("a same-aggregate send after cancellation must not deadlock on an orphaned lock");
        second_result.expect("send after lock release via cancellation must succeed");
    }
}
