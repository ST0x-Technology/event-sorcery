//! Lifecycle adapter for event-sourced entities.
//!
//! Wraps domain entities in a state machine that tracks whether
//! they are uninitialized, live, or failed. Provides a blanket
//! `Aggregate` impl that delegates to [`EventSourced`] methods,
//! eliminating per-entity boilerplate.
//!
//! See the [crate root](crate) for the full design rationale.

use async_trait::async_trait;
use cqrs_es::event_sink::EventSink;
use cqrs_es::{Aggregate, EventEnvelope, Query, View};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::str::FromStr;
use std::sync::Arc;
use tracing::{error, warn};

use crate::EventSourced;
use crate::dependency::HasEntity;
use crate::reactor::Reactor;

/// Adapter that bridges [`EventSourced`] to cqrs-es `Aggregate`.
///
/// Wraps a domain entity and tracks whether it has been
/// initialized, is live, or has entered an error state. The
/// blanket `Aggregate` impl delegates to `EventSourced` methods
/// and translates between the two interfaces.
///
/// Application code should not construct or match on `Lifecycle`
/// directly in most cases. Interact through
/// [`Store::send`](crate::Store::send) for commands and through
/// views for queries.
///
/// # State machine
///
/// ```text
/// Uninitialized --originate(entity)--> Live(entity)
/// Uninitialized --originate(None)----> Failed { EventCantOriginate }
///
/// Live(entity) --evolve(Ok(Some))----> Live(new_entity)
/// Live(entity) --evolve(Ok(None))----> Failed { UnexpectedEvent }
/// Live(entity) --evolve(Err(e))------> Failed { Apply(e) }
///
/// Failed { .. } ---- any event ------> Failed { AlreadyFailed }
/// ```
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
// Override serde's inferred bounds. Without this, serde derives
// `Entity: Serialize + Deserialize` bounds, but Entity's serde
// impls are already guaranteed by the EventSourced supertrait.
// The empty bound avoids redundant constraints that confuse the
// compiler when Entity has complex associated types.
#[serde(bound = "")]
pub(crate) enum Lifecycle<Entity: EventSourced> {
    #[default]
    Uninitialized,
    Live(Entity),
    Failed {
        error: LifecycleError<Entity>,
        last_valid_entity: Option<Box<Entity>>,
    },
}

impl<Entity: EventSourced> Lifecycle<Entity> {
    pub(crate) fn into_result(self) -> Result<Option<Entity>, LifecycleError<Entity>> {
        match self {
            Self::Live(entity) => Ok(Some(entity)),
            Self::Uninitialized => Ok(None),
            Self::Failed { error, .. } => Err(error),
        }
    }
}

/// Errors from lifecycle state management.
///
/// These are infrastructure-level errors produced by
/// [`Lifecycle`]'s blanket `Aggregate` impl, not by domain
/// code directly. Domain errors are wrapped in the [`Apply`]
/// variant.
///
/// The error carries typed state and event information rather
/// than opaque debug strings, enabling meaningful error
/// handling and debugging.
///
/// [`Apply`]: LifecycleError::Apply
#[derive(Debug, Clone, Serialize, Deserialize, thiserror::Error)]
// Same override as Lifecycle above -- serde would infer
// `Entity::Error: Serialize + Deserialize` bounds that are
// already guaranteed by EventSourced's DomainError supertrait.
#[serde(bound = "")]
pub enum LifecycleError<Entity: EventSourced> {
    #[error("event '{event:?}' cannot originate entity")]
    EventCantOriginate { event: Entity::Event },
    #[error("event '{event:?}' not applicable to entity '{entity:?}'")]
    UnexpectedEvent {
        entity: Box<Entity>,
        event: Entity::Event,
    },
    #[error("event '{event:?}' applied to already-failed lifecycle")]
    AlreadyFailed {
        failure: Box<Self>,
        event: Entity::Event,
    },
    #[error(transparent)]
    Apply(Entity::Error),
    /// A command in this task is already commanding the same aggregate (directly,
    /// or transitively through a chain of inline-awaited reactor dispatches) when
    /// another `Store::send()` for that same aggregate is attempted. Reactors are
    /// dispatched inline while `Store::send`'s per-aggregate lock is held (see
    /// ADR-0004), so a same-aggregate self-command would block forever on a lock
    /// this task already holds. Returned instead of deadlocking.
    ///
    /// The offending aggregate ID is logged at the `Store::send` return site
    /// rather than carried here, to avoid widening `EventSourced::Id`'s bounds
    /// just for this one error variant.
    #[error(
        "command to this aggregate is already in flight in this task \
         (reentrant same-aggregate command would deadlock on the held per-aggregate lock)"
    )]
    ReentrantCommand,
}

/// Uninhabited error type for entities with infallible
/// operations.
///
/// Similar to `std::convert::Infallible` but derives
/// `Serialize`/`Deserialize` for cqrs-es compatibility.
/// Use as `type Error = Never` on entities where neither
/// command handling nor event application can fail.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[error("never")]
pub enum Never {}

/// Bridges [`EventSourced`] to cqrs-es `Aggregate`.
///
/// This blanket impl eliminates per-entity boilerplate. All
/// command routing (uninitialized -> `initialize`, live ->
/// `transition`) and event application (uninitialized ->
/// `originate`, live -> `evolve`) is handled here.
///
/// The `apply` method uses `std::mem::take` to move out of
/// `&mut self`, avoiding unnecessary clones when transitioning
/// between lifecycle states.
impl<Entity> Aggregate for Lifecycle<Entity>
where
    Entity: EventSourced,
    Entity::Event: Clone + Debug + Serialize + DeserializeOwned + Send + Sync,
{
    type Command = Entity::Command;
    type Event = Entity::Event;
    type Error = LifecycleError<Entity>;
    type Services = Entity::Services;

    const TYPE: &'static str = Entity::AGGREGATE_TYPE;

    async fn handle(
        &mut self,
        command: Self::Command,
        services: &Self::Services,
        sink: &EventSink<Self>,
    ) -> Result<(), Self::Error> {
        let events = match &*self {
            Self::Uninitialized => Entity::initialize(command, services).await,
            Self::Live(entity) => entity.transition(command, services).await,
            Self::Failed { error, .. } => {
                warn!(
                    target: "cqrs",
                    %error,
                    aggregate_type = Entity::AGGREGATE_TYPE,
                    "Rejecting command because lifecycle is already failed"
                );
                return Err(error.clone());
            }
        }
        .inspect_err(|domain_error| {
            error!(
                target: "cqrs",
                %domain_error,
                aggregate_type = Entity::AGGREGATE_TYPE,
                "Command handler returned domain error"
            );
        })
        .map_err(LifecycleError::Apply)?;

        if events.is_empty() {
            return Ok(());
        }

        // cqrs-es 0.5 rebuilds the snapshot inside `commit` by re-applying these
        // events to the aggregate it received from `handle`
        // (`PersistedEventStore::update_snapshot_with_events`). `self` must
        // therefore remain at its pre-command state when `handle` returns,
        // otherwise the genesis/transition events get applied twice and the
        // lifecycle collapses into `Failed`. Our `EventSourced` handlers already
        // produce the full event list up front, so we feed the sink through a
        // throwaway copy; `apply` evolves the persisted aggregate later during
        // load (replay) and commit (snapshot).
        // The scratch copy doubles as a validity check: if the entity emitted
        // an event its own `originate`/`evolve` rejects, `scratch` lands in
        // `Failed`. Surfacing that as the command's error keeps the poisoned
        // events out of the store (the framework discards sink contents when
        // `handle` errors) instead of committing them and failing the
        // aggregate on its next load. The check runs after every write so the
        // caller gets the root-cause error, not an `AlreadyFailed` wrapper
        // from feeding later events through an already-failed scratch.
        let mut scratch = self.clone();
        for event in events {
            sink.write(event, &mut scratch).await;

            if let Self::Failed { error, .. } = &scratch {
                error!(
                    target: "cqrs",
                    %error,
                    aggregate_type = Entity::AGGREGATE_TYPE,
                    "Command produced an event the entity cannot apply"
                );
                return Err(error.clone());
            }
        }

        Ok(())
    }

    fn apply(&mut self, event: Self::Event) {
        *self = match std::mem::take(self) {
            Self::Uninitialized => Entity::originate(&event).map_or_else(
                || {
                    let err = LifecycleError::EventCantOriginate { event };
                    error!(target: "cqrs", "lifecycle failed during originate: {err}");
                    Self::Failed {
                        error: err,
                        last_valid_entity: None,
                    }
                },
                Self::Live,
            ),

            Self::Live(entity) => match Entity::evolve(&entity, &event) {
                Ok(Some(new_entity)) => Self::Live(new_entity),
                Ok(None) => {
                    let err = LifecycleError::UnexpectedEvent {
                        entity: Box::new(entity.clone()),
                        event,
                    };
                    error!(target: "cqrs", "lifecycle failed during evolve: {err}");
                    Self::Failed {
                        error: err,
                        last_valid_entity: Some(Box::new(entity)),
                    }
                }
                Err(domain_err) => {
                    let err = LifecycleError::Apply(domain_err);
                    error!(target: "cqrs", "lifecycle failed during evolve: {err}");
                    Self::Failed {
                        error: err,
                        last_valid_entity: Some(Box::new(entity)),
                    }
                }
            },

            Self::Failed {
                error,
                last_valid_entity,
            } => {
                let err = LifecycleError::AlreadyFailed {
                    failure: Box::new(error),
                    event,
                };
                error!(target: "cqrs", "lifecycle already failed, ignoring event: {err}");
                Self::Failed {
                    error: err,
                    last_valid_entity,
                }
            }
        };
    }
}

/// Allows any `Lifecycle<Entity>` to serve as its own
/// materialized view by replaying events through `apply`.
impl<Entity> View<Self> for Lifecycle<Entity>
where
    Self: Aggregate,
    Entity: EventSourced,
{
    fn update(&mut self, event: &EventEnvelope<Self>) {
        self.apply(event.payload.clone());
    }
}

/// Enables sharing a single query processor across multiple
/// CQRS frameworks via `Arc`.
#[async_trait]
impl<QueryImpl, Entity> Query<Lifecycle<Entity>> for Arc<QueryImpl>
where
    QueryImpl: Query<Lifecycle<Entity>> + Send + Sync,
    Entity: EventSourced,
    Lifecycle<Entity>: Aggregate,
{
    async fn dispatch(&self, aggregate_id: &str, events: &[EventEnvelope<Lifecycle<Entity>>]) {
        QueryImpl::dispatch(self, aggregate_id, events).await;
    }
}

/// Bridges a [`Reactor`](crate::Reactor) to
/// `cqrs_es::Query<Lifecycle<Entity>>`.
///
/// Parses the stringly-typed aggregate ID into `Entity::Id`,
/// injects the entity's (Id, Event) pair into the reactor's
/// computed event type via [`HasEntity`], and dispatches it.
pub(crate) struct ReactorBridge<R> {
    pub(crate) reactor: Arc<R>,
}

#[async_trait]
impl<R, Entity> Query<Lifecycle<Entity>> for ReactorBridge<R>
where
    R: Reactor,
    R::Dependencies: HasEntity<Entity>,
    Entity: EventSourced,
    Entity::Id: Clone,
    Entity::Event: Clone,
    <Entity::Id as FromStr>::Err: Debug,
    Lifecycle<Entity>: Aggregate<Event = Entity::Event>,
{
    async fn dispatch(&self, aggregate_id: &str, events: &[EventEnvelope<Lifecycle<Entity>>]) {
        let Ok(typed_id) = aggregate_id.parse::<Entity::Id>() else {
            warn!(
                target: "cqrs",
                aggregate_id = aggregate_id,
                aggregate_type = Entity::AGGREGATE_TYPE,
                "Failed to parse aggregate ID in reactor bridge"
            );
            return;
        };

        for envelope in events {
            let injected = <R::Dependencies as HasEntity<Entity>>::inject(
                typed_id.clone(),
                envelope.payload.clone(),
            );

            // No retry here by design -- wrap the reactor in `RetryOnBusy`
            // (gated by `IdempotentReactor`) to retry on transient SQLite
            // busy errors. See `docs/cqrs.md`'s "Reactors" section.
            if let Err(error) = self.reactor.react(injected).await {
                error!(
                    target: "cqrs",
                    ?error,
                    aggregate_id = aggregate_id,
                    aggregate_type = Entity::AGGREGATE_TYPE,
                    "Reactor failed to handle event"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use cqrs_es::event_sink::EventSink;
    use cqrs_es::{Aggregate, DomainEvent, EventEnvelope, View};
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

    use super::*;
    use crate::dependency::{Cons, Dependent, EntityList};
    use crate::reactor::{IdempotentReactor, RETRY_MAX_ATTEMPTS, RetryOnBusy};
    use crate::testing::sqlx_error_with_code;
    use crate::{EventSourced, Nil};

    /// Test entity: a simple counter with controllable error behavior.
    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct Counter {
        value: u32,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    enum CounterEvent {
        Created {
            initial: u32,
        },
        Incremented,
        /// evolve returns Ok(None) for this, triggering Mismatch.
        Invalid,
        /// evolve returns Err for this, triggering Apply.
        Broken,
    }

    impl DomainEvent for CounterEvent {
        fn event_type(&self) -> String {
            format!("{self:?}")
        }
        fn event_version(&self) -> String {
            "1.0".to_string()
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
    #[error("domain error")]
    struct CounterError;

    enum CounterCommand {
        Create {
            initial: u32,
        },
        Increment,
        Fail,
        /// Emits a multi-event batch whose first event is rejected by
        /// `originate`/`evolve`, exercising the scratch short-circuit.
        BrokenBatch,
    }

    #[async_trait]
    impl EventSourced for Counter {
        type Id = String;
        type Event = CounterEvent;
        type Command = CounterCommand;
        type Error = CounterError;
        type Services = ();
        type Materialized = Nil;

        const AGGREGATE_TYPE: &'static str = "Counter";
        const PROJECTION: Nil = Nil;
        const SCHEMA_VERSION: u64 = 1;

        fn originate(event: &CounterEvent) -> Option<Self> {
            match event {
                CounterEvent::Created { initial } => Some(Self { value: *initial }),
                _ => None,
            }
        }

        fn evolve(entity: &Self, event: &CounterEvent) -> Result<Option<Self>, CounterError> {
            use CounterEvent::*;
            match event {
                Broken => Err(CounterError),
                Created { .. } | Invalid => Ok(None),
                Incremented => Ok(Some(Self {
                    value: entity.value + 1,
                })),
            }
        }

        async fn initialize(
            command: CounterCommand,
            _services: &(),
        ) -> Result<Vec<CounterEvent>, CounterError> {
            use CounterCommand::*;
            match command {
                Create { initial } => Ok(vec![CounterEvent::Created { initial }]),
                Increment => Ok(vec![CounterEvent::Incremented]),
                Fail => Err(CounterError),
                BrokenBatch => Ok(vec![CounterEvent::Incremented, CounterEvent::Incremented]),
            }
        }

        async fn transition(
            &self,
            command: CounterCommand,
            _services: &(),
        ) -> Result<Vec<CounterEvent>, CounterError> {
            match command {
                CounterCommand::Create { .. } => Ok(vec![]),
                CounterCommand::Increment => Ok(vec![CounterEvent::Incremented]),
                CounterCommand::Fail => Err(CounterError),
                CounterCommand::BrokenBatch => {
                    Ok(vec![CounterEvent::Invalid, CounterEvent::Incremented])
                }
            }
        }
    }

    #[test]
    fn originate_success_transitions_to_live() {
        let mut lifecycle = Lifecycle::<Counter>::default();

        lifecycle.apply(CounterEvent::Created { initial: 10 });

        assert!(matches!(lifecycle, Lifecycle::Live(Counter { value: 10 })));
    }

    #[test]
    fn originate_none_transitions_to_failed_mismatch() {
        let mut lifecycle = Lifecycle::<Counter>::default();

        lifecycle.apply(CounterEvent::Incremented);

        assert!(matches!(
            lifecycle,
            Lifecycle::Failed {
                error: LifecycleError::EventCantOriginate { .. },
                last_valid_entity: None,
            }
        ));
    }

    #[test]
    fn evolve_success_stays_live_with_new_state() {
        let mut lifecycle = Lifecycle::Live(Counter { value: 5 });

        lifecycle.apply(CounterEvent::Incremented);

        assert!(matches!(lifecycle, Lifecycle::Live(Counter { value: 6 })));
    }

    #[test]
    fn evolve_mismatch_transitions_to_failed_with_last_valid_entity() {
        let mut lifecycle = Lifecycle::Live(Counter { value: 5 });

        lifecycle.apply(CounterEvent::Invalid);

        match lifecycle {
            Lifecycle::Failed {
                error: LifecycleError::UnexpectedEvent { .. },
                last_valid_entity: Some(last),
            } => assert_eq!(*last, Counter { value: 5 }),
            other => panic!("expected Failed with UnexpectedEvent, got {other:?}"),
        }
    }

    #[test]
    fn evolve_domain_error_transitions_to_failed_apply() {
        let mut lifecycle = Lifecycle::Live(Counter { value: 5 });

        lifecycle.apply(CounterEvent::Broken);

        match lifecycle {
            Lifecycle::Failed {
                error: LifecycleError::Apply(CounterError),
                last_valid_entity: Some(last),
            } => assert_eq!(*last, Counter { value: 5 }),
            other => panic!("expected Failed with Apply, got {other:?}"),
        }
    }

    #[test]
    fn failed_state_is_sticky() {
        let prior_error = LifecycleError::EventCantOriginate {
            event: CounterEvent::Incremented,
        };

        let mut lifecycle = Lifecycle::<Counter>::Failed {
            error: prior_error,
            last_valid_entity: None,
        };

        lifecycle.apply(CounterEvent::Created { initial: 99 });

        assert!(matches!(
            lifecycle,
            Lifecycle::Failed {
                error: LifecycleError::AlreadyFailed { .. },
                ..
            }
        ));
    }

    #[tokio::test]
    async fn handle_uninitialized_delegates_to_initialize() {
        let mut lifecycle = Lifecycle::<Counter>::default();
        let sink = EventSink::default();

        lifecycle
            .handle(CounterCommand::Create { initial: 42 }, &(), &sink)
            .await
            .unwrap();

        assert_eq!(
            sink.collect().await,
            vec![CounterEvent::Created { initial: 42 }]
        );
        // cqrs-es 0.5 re-applies the sink's events to `self` during commit's
        // snapshot rebuild, so `handle` must leave `self` at its pre-command
        // state or every event gets applied twice.
        assert!(matches!(lifecycle, Lifecycle::Uninitialized));
    }

    #[tokio::test]
    async fn handle_live_delegates_to_transition() {
        let mut lifecycle = Lifecycle::Live(Counter { value: 0 });
        let sink = EventSink::default();

        lifecycle
            .handle(CounterCommand::Increment, &(), &sink)
            .await
            .unwrap();

        assert_eq!(sink.collect().await, vec![CounterEvent::Incremented]);
        // Same pre-command-state invariant as the uninitialized case: the
        // entity value must not advance until commit replays the events.
        assert!(matches!(lifecycle, Lifecycle::Live(Counter { value: 0 })));
    }

    #[tokio::test]
    async fn handle_rejects_events_the_entity_cannot_apply() {
        let mut lifecycle = Lifecycle::<Counter>::default();
        let sink = EventSink::default();

        // `initialize` maps Increment to an Incremented event, which
        // `originate` rejects -- the scratch replay must surface that as a
        // command error instead of committing a poisoned event stream.
        let error = lifecycle
            .handle(CounterCommand::Increment, &(), &sink)
            .await
            .unwrap_err();

        assert!(matches!(error, LifecycleError::EventCantOriginate { .. }));
        assert!(matches!(lifecycle, Lifecycle::Uninitialized));
    }

    #[tokio::test]
    async fn handle_multi_event_rejection_returns_root_cause_error() {
        let mut lifecycle = Lifecycle::Live(Counter { value: 3 });
        let sink = EventSink::default();

        // BrokenBatch emits [Invalid, Incremented]; evolve rejects Invalid.
        // The scratch short-circuit must return the root UnexpectedEvent
        // error, not an AlreadyFailed wrapper from the trailing event.
        let error = lifecycle
            .handle(CounterCommand::BrokenBatch, &(), &sink)
            .await
            .unwrap_err();

        assert!(matches!(error, LifecycleError::UnexpectedEvent { .. }));
        assert!(matches!(lifecycle, Lifecycle::Live(Counter { value: 3 })));
    }

    #[tokio::test]
    async fn handle_maps_domain_error_to_lifecycle_apply() {
        let mut lifecycle = Lifecycle::Live(Counter { value: 0 });
        let sink = EventSink::default();

        let error = lifecycle
            .handle(CounterCommand::Fail, &(), &sink)
            .await
            .unwrap_err();

        assert!(matches!(error, LifecycleError::Apply(CounterError)));
        // Error paths must also leave `self` at its pre-command state.
        assert!(matches!(lifecycle, Lifecycle::Live(Counter { value: 0 })));
    }

    #[tokio::test]
    async fn handle_failed_returns_stored_error() {
        let stored_error = LifecycleError::EventCantOriginate {
            event: CounterEvent::Incremented,
        };
        let mut lifecycle = Lifecycle::<Counter>::Failed {
            error: stored_error.clone(),
            last_valid_entity: None,
        };
        let sink = EventSink::default();

        let error = lifecycle
            .handle(CounterCommand::Increment, &(), &sink)
            .await
            .unwrap_err();

        assert!(matches!(error, LifecycleError::EventCantOriginate { .. }));
    }

    #[test]
    fn view_update_applies_event_to_lifecycle() {
        let mut lifecycle = Lifecycle::<Counter>::default();
        let envelope = EventEnvelope {
            aggregate_id: "test".to_string(),
            sequence: 1,
            payload: CounterEvent::Created { initial: 7 },
            metadata: HashMap::new(),
        };

        lifecycle.update(&envelope);

        assert!(matches!(lifecycle, Lifecycle::Live(Counter { value: 7 })));
    }

    #[derive(Debug, thiserror::Error)]
    #[error("always fails")]
    struct AlwaysFailError;

    /// Test reactor that always fails with a non-busy error, proving
    /// `dispatch`'s baseline swallow behavior (no retry, no panic).
    struct AlwaysFailReactor {
        calls: AtomicU32,
    }

    impl Dependent for AlwaysFailReactor {
        type Dependencies = Cons<Counter, Nil>;
    }

    #[async_trait]
    impl Reactor for AlwaysFailReactor {
        type Error = AlwaysFailError;

        async fn react(
            &self,
            _event: <Self::Dependencies as EntityList>::Event,
        ) -> Result<(), Self::Error> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Err(AlwaysFailError)
        }
    }

    #[derive(Debug, thiserror::Error)]
    enum FlakyDispatchError {
        #[error("busy: {0}")]
        Busy(#[source] sqlx::Error),
    }

    /// Test reactor that fails with a busy-classified error a configurable
    /// number of times before succeeding. Also implements `IdempotentReactor`
    /// so it can be wrapped in `RetryOnBusy`.
    ///
    /// `busy_code` is the SQLite extended result code the busy failures carry,
    /// so the dispatch recovery path can be driven with both plain
    /// `SQLITE_BUSY` (`"5"`) and `SQLITE_BUSY_SNAPSHOT` (`"517"`).
    struct FlakyIdempotentReactor {
        remaining_busy_failures: AtomicU32,
        calls: AtomicU32,
        applied: AtomicBool,
        busy_code: &'static str,
    }

    impl Dependent for FlakyIdempotentReactor {
        type Dependencies = Cons<Counter, Nil>;
    }

    #[async_trait]
    impl Reactor for FlakyIdempotentReactor {
        type Error = FlakyDispatchError;

        async fn react(
            &self,
            event: <Self::Dependencies as EntityList>::Event,
        ) -> Result<(), Self::Error> {
            self.calls.fetch_add(1, Ordering::SeqCst);

            let remaining = self.remaining_busy_failures.load(Ordering::SeqCst);
            if remaining > 0 {
                self.remaining_busy_failures
                    .store(remaining - 1, Ordering::SeqCst);
                return Err(FlakyDispatchError::Busy(sqlx_error_with_code(
                    self.busy_code,
                )));
            }

            let _ = event.into_inner();
            self.applied.store(true, Ordering::SeqCst);
            Ok(())
        }
    }

    impl IdempotentReactor for FlakyIdempotentReactor {}

    #[tokio::test]
    async fn dispatch_swallows_reactor_error_without_retry() {
        let reactor = Arc::new(AlwaysFailReactor {
            calls: AtomicU32::new(0),
        });
        let bridge = ReactorBridge {
            reactor: Arc::clone(&reactor),
        };
        let envelope: EventEnvelope<Lifecycle<Counter>> = EventEnvelope {
            aggregate_id: "counter-1".to_string(),
            sequence: 1,
            payload: CounterEvent::Created { initial: 1 },
            metadata: HashMap::new(),
        };

        bridge.dispatch("counter-1", &[envelope]).await;

        assert_eq!(reactor.calls.load(Ordering::SeqCst), 1);
    }

    /// Drives the full dispatch -> `RetryOnBusy` -> reactor path to a verified
    /// successful write with busy failures carrying `busy_code`.
    async fn dispatch_retries_busy_and_succeeds(busy_code: &'static str) {
        let reactor = Arc::new(RetryOnBusy {
            inner: FlakyIdempotentReactor {
                remaining_busy_failures: AtomicU32::new(2),
                calls: AtomicU32::new(0),
                applied: AtomicBool::new(false),
                busy_code,
            },
        });
        let bridge = ReactorBridge {
            reactor: Arc::clone(&reactor),
        };
        let envelope: EventEnvelope<Lifecycle<Counter>> = EventEnvelope {
            aggregate_id: "counter-1".to_string(),
            sequence: 1,
            payload: CounterEvent::Created { initial: 1 },
            metadata: HashMap::new(),
        };

        bridge.dispatch("counter-1", &[envelope]).await;

        assert!(
            reactor.inner.applied.load(Ordering::SeqCst),
            "the write must actually land after retrying code {busy_code}"
        );
        assert_eq!(reactor.inner.calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn dispatch_retries_busy_error_via_retry_on_busy_and_succeeds() {
        dispatch_retries_busy_and_succeeds("5").await;
    }

    /// `busy_timeout` cannot absorb `SQLITE_BUSY_SNAPSHOT`; rolling back and
    /// re-attempting is the only fix. Covers that scenario through the real
    /// dispatch path, not just `is_retryable_sqlite_busy`'s unit tests.
    #[tokio::test]
    async fn dispatch_retries_busy_snapshot_error_via_retry_on_busy_and_succeeds() {
        dispatch_retries_busy_and_succeeds("517").await;
    }

    #[tokio::test(start_paused = true)]
    async fn dispatch_swallows_retry_on_busy_exhaustion_after_full_schedule() {
        // Never reaches zero within RETRY_MAX_ATTEMPTS + 1 calls -- exercises
        // the real production schedule end to end (~4.3s), proving dispatch
        // still swallows the error once RetryOnBusy exhausts its budget.
        let reactor = Arc::new(RetryOnBusy {
            inner: FlakyIdempotentReactor {
                remaining_busy_failures: AtomicU32::new(u32::MAX),
                calls: AtomicU32::new(0),
                applied: AtomicBool::new(false),
                busy_code: "5",
            },
        });
        let bridge = ReactorBridge {
            reactor: Arc::clone(&reactor),
        };
        let envelope: EventEnvelope<Lifecycle<Counter>> = EventEnvelope {
            aggregate_id: "counter-1".to_string(),
            sequence: 1,
            payload: CounterEvent::Created { initial: 1 },
            metadata: HashMap::new(),
        };

        bridge.dispatch("counter-1", &[envelope]).await;

        assert!(!reactor.inner.applied.load(Ordering::SeqCst));
        assert_eq!(
            reactor.inner.calls.load(Ordering::SeqCst),
            RETRY_MAX_ATTEMPTS + 1
        );
    }
}
