//! Schema version registry for detecting stale snapshots.
//!
//! Tracks the last-known [`SCHEMA_VERSION`] for each aggregate type
//! in the event store. Startup reconciliation is a two-step contract:
//! [`Reconciler::reconcile`] compares the stored version against the
//! current code version and clears snapshots when they diverge, then
//! the caller runs any further version-change recovery it owns (for
//! projected entities, a view rebuild) and finally calls
//! [`Reconciler::record_version`] to mark the version handled.
//! Recording is deferred to the end so a crash mid-recovery re-runs the
//! whole idempotent sequence on restart instead of stranding
//! half-reconciled state behind an already-advanced version. The
//! `StoreBuilder` wires this sequence; callers driving [`Reconciler`]
//! directly must follow the same order.
//!
//! This is itself an event-sourced aggregate whose state is rebuilt
//! from the full event log on every startup -- no views, no
//! snapshots. This avoids a circular dependency: views depend on
//! the schema registry for reprojection, so the registry must be
//! self-sufficient.
//!
//! [`SCHEMA_VERSION`]: crate::EventSourced::SCHEMA_VERSION

use async_trait::async_trait;
use cqrs_es::AggregateError;
use cqrs_es::persist::PersistenceError;
use serde::{Deserialize, Serialize};
use sqlite_es::SqliteCqrs;
use sqlx::SqlitePool;
use std::collections::BTreeMap;
use tracing::{debug, info};

use crate::CompactionPolicy;
use crate::lifecycle::{Lifecycle, LifecycleError, Never};
use crate::{DomainEvent, EventSourced, Nil};

/// Singleton aggregate ID for the schema registry.
const REGISTRY_ID: &str = "schema";

/// Tracks schema versions for all aggregates.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchemaRegistry {
    versions: BTreeMap<String, u64>,
}

impl SchemaRegistry {
    fn version_of(&self, name: &str) -> Option<u64> {
        self.versions.get(name).copied()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum SchemaRegistryEvent {
    VersionUpdated { name: String, version: u64 },
}

impl DomainEvent for SchemaRegistryEvent {
    fn event_type(&self) -> String {
        "SchemaRegistryEvent::VersionUpdated".to_string()
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SchemaRegistryCommand {
    Register { name: String, version: u64 },
}

#[async_trait]
impl EventSourced for SchemaRegistry {
    type Id = String;
    type Event = SchemaRegistryEvent;
    type Command = SchemaRegistryCommand;
    type Error = Never;
    type Services = ();
    type Materialized = Nil;

    const AGGREGATE_TYPE: &'static str = "SchemaRegistry";
    const PROJECTION: Nil = Nil;
    const SCHEMA_VERSION: u64 = 1;

    fn originate(event: &Self::Event) -> Option<Self> {
        let SchemaRegistryEvent::VersionUpdated { name, version } = event;
        let mut versions = BTreeMap::new();
        versions.insert(name.clone(), *version);
        Some(Self { versions })
    }

    fn evolve(entity: &Self, event: &Self::Event) -> Result<Option<Self>, Self::Error> {
        let SchemaRegistryEvent::VersionUpdated { name, version } = event;
        let mut new_state = entity.clone();
        new_state.versions.insert(name.clone(), *version);
        Ok(Some(new_state))
    }

    async fn initialize(
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        let SchemaRegistryCommand::Register { name, version } = command;
        Ok(vec![SchemaRegistryEvent::VersionUpdated { name, version }])
    }

    async fn transition(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        let SchemaRegistryCommand::Register { name, version } = command;
        if self.version_of(&name) == Some(version) {
            Ok(vec![])
        } else {
            Ok(vec![SchemaRegistryEvent::VersionUpdated { name, version }])
        }
    }
}

/// Outcome of [`Reconciler::reconcile`]: whether the stored schema version
/// diverged from the code's [`SCHEMA_VERSION`] for an entity.
///
/// Callers branch on this to choose recovery: a `Changed` projected entity
/// rebuilds its views, an `Unchanged` one only catches up on missed events.
///
/// [`SCHEMA_VERSION`]: crate::EventSourced::SCHEMA_VERSION
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use = "reconciliation must be completed by calling `Reconciler::record_version` after \
              recovery; dropping the outcome usually means that call was forgotten, leaving the \
              schema version unadvanced and the recovery re-running on every startup"]
pub enum SchemaReconciliation {
    /// The stored version did not match the current code version; any existing
    /// snapshots were cleared.
    Changed,
    /// The stored version matched the code version; nothing was cleared.
    Unchanged,
}

/// Handles schema version reconciliation at startup.
///
/// Reads state by replaying all SchemaRegistry events from the event
/// store (no views, no snapshots). Writes go through the CQRS
/// framework to maintain event sourcing invariants.
pub struct Reconciler {
    cqrs: SqliteCqrs<Lifecycle<SchemaRegistry>>,
    pool: SqlitePool,
}

impl Reconciler {
    pub fn new(pool: SqlitePool) -> Self {
        #[allow(clippy::disallowed_methods)]
        let cqrs = sqlite_es::sqlite_cqrs(pool.clone(), vec![], ());
        Self { cqrs, pool }
    }

    /// Rebuilds SchemaRegistry state from the full event log.
    async fn load_registry(&self) -> Result<Option<SchemaRegistry>, ReconcileError> {
        let payloads: Vec<String> = sqlx::query_scalar(
            "SELECT payload FROM events \
             WHERE aggregate_type = 'SchemaRegistry' \
             AND aggregate_id = 'schema' \
             ORDER BY sequence",
        )
        .fetch_all(&self.pool)
        .await?;

        let mut state: Option<SchemaRegistry> = None;

        for payload in payloads {
            let event: SchemaRegistryEvent = serde_json::from_str(&payload)?;

            state = match state {
                None => SchemaRegistry::originate(&event),
                Some(current) => {
                    let evolved = SchemaRegistry::evolve(&current, &event)?;
                    Some(evolved.unwrap_or(current))
                }
            };
        }

        Ok(state)
    }

    /// Clears version-mismatched snapshots for `Entity` and reports whether the
    /// schema version [`Changed`](SchemaReconciliation::Changed) (snapshots
    /// cleared) or was [`Unchanged`](SchemaReconciliation::Unchanged).
    ///
    /// Does NOT record the new version. The caller must call
    /// [`Reconciler::record_version`] only after every version-change recovery
    /// it owns (the snapshot clear here, plus any view rebuild the caller
    /// performs) has durably completed -- recording the version is the "done"
    /// marker, and a crash before it leaves the version unadvanced so the whole
    /// recovery re-runs on the next startup. The snapshot clear is idempotent,
    /// so re-running is safe.
    #[allow(clippy::cognitive_complexity)]
    pub async fn reconcile<Entity: EventSourced>(
        &self,
    ) -> Result<SchemaReconciliation, ReconcileError> {
        let name = Entity::AGGREGATE_TYPE;
        let current_version = Entity::SCHEMA_VERSION;

        let stored_version = self
            .load_registry()
            .await?
            .and_then(|registry| registry.version_of(name));

        debug!(target: "cqrs", aggregate = %name, ?stored_version, "Loaded stored schema version");

        let needs_clear = stored_version != Some(current_version);

        if needs_clear {
            // Compactable aggregates may have deleted their pre-snapshot
            // events. Clearing their snapshots would permanently lose
            // that state with no way to rebuild. Refuse and require an
            // explicit migration instead.
            //
            // Skip the guard on first registration (stored_version = None):
            // there are no snapshots to clear and no compacted events yet.
            if stored_version.is_some()
                && matches!(
                    Entity::COMPACTION_POLICY,
                    CompactionPolicy::CompactAfterSnapshot
                )
            {
                return Err(ReconcileError::CompactedSnapshotClear {
                    aggregate: name.to_string(),
                    old_version: stored_version,
                    new_version: current_version,
                });
            }

            sqlx::query("DELETE FROM snapshots WHERE aggregate_type = ?")
                .bind(name)
                .execute(&self.pool)
                .await?;

            info!(
                target: "cqrs",
                aggregate = name,
                old_version = ?stored_version,
                new_version = current_version,
                "Cleared stale snapshots for schema version change"
            );

            Ok(SchemaReconciliation::Changed)
        } else {
            debug!(target: "cqrs", aggregate = %name, version = current_version, "Schema version unchanged");

            Ok(SchemaReconciliation::Unchanged)
        }
    }

    /// Records that `Entity`'s current [`SCHEMA_VERSION`] has been fully
    /// reconciled. Call only after all version-change recovery (snapshot clear
    /// and, for projected entities, view rebuild) has durably completed, so a
    /// recorded version always implies its recovery finished.
    ///
    /// [`SCHEMA_VERSION`]: crate::EventSourced::SCHEMA_VERSION
    pub async fn record_version<Entity: EventSourced>(&self) -> Result<(), ReconcileError> {
        self.cqrs
            .execute(
                REGISTRY_ID,
                SchemaRegistryCommand::Register {
                    name: Entity::AGGREGATE_TYPE.to_string(),
                    version: Entity::SCHEMA_VERSION,
                },
            )
            .await?;

        Ok(())
    }
}

/// Errors from schema reconciliation during startup.
#[derive(Debug, thiserror::Error)]
pub enum ReconcileError {
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Aggregate(#[from] AggregateError<LifecycleError<SchemaRegistry>>),
    #[error(transparent)]
    Persistence(#[from] PersistenceError),

    /// A compactable aggregate's schema version changed but its
    /// snapshots cannot be safely deleted because compacted events
    /// are already gone. Requires an explicit migration.
    #[error(
        "Cannot clear snapshots for compactable aggregate '{aggregate}' \
         (version {old_version:?} -> {new_version}). \
         Compacted events have been deleted; clearing snapshots would \
         permanently lose state. Write a migration that handles the \
         schema change without deleting snapshots."
    )]
    CompactedSnapshotClear {
        aggregate: String,
        old_version: Option<u64>,
        new_version: u64,
    },
}

impl From<Never> for ReconcileError {
    fn from(never: Never) -> Self {
        match never {}
    }
}

#[cfg(test)]
mod tests {
    use cqrs_es::Aggregate;
    use cqrs_es::event_sink::EventSink;

    use super::*;

    #[tokio::test]
    async fn register_new_aggregate_emits_event() {
        let mut aggregate = Lifecycle::<SchemaRegistry>::default();
        let sink = EventSink::default();

        aggregate
            .handle(
                SchemaRegistryCommand::Register {
                    name: "Position".to_string(),
                    version: 1,
                },
                &(),
                &sink,
            )
            .await
            .unwrap();

        let events = sink.collect().await;

        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0],
            SchemaRegistryEvent::VersionUpdated {
                name: "Position".to_string(),
                version: 1,
            }
        );
    }

    #[tokio::test]
    async fn register_same_version_is_noop() {
        let mut aggregate = Lifecycle::<SchemaRegistry>::default();

        aggregate.apply(SchemaRegistryEvent::VersionUpdated {
            name: "Position".to_string(),
            version: 1,
        });

        let sink = EventSink::default();

        aggregate
            .handle(
                SchemaRegistryCommand::Register {
                    name: "Position".to_string(),
                    version: 1,
                },
                &(),
                &sink,
            )
            .await
            .unwrap();

        let events = sink.collect().await;

        assert!(events.is_empty());
    }

    #[tokio::test]
    async fn register_new_version_emits_event() {
        let mut aggregate = Lifecycle::<SchemaRegistry>::default();

        aggregate.apply(SchemaRegistryEvent::VersionUpdated {
            name: "Position".to_string(),
            version: 1,
        });

        let sink = EventSink::default();

        aggregate
            .handle(
                SchemaRegistryCommand::Register {
                    name: "Position".to_string(),
                    version: 2,
                },
                &(),
                &sink,
            )
            .await
            .unwrap();

        let events = sink.collect().await;

        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0],
            SchemaRegistryEvent::VersionUpdated {
                name: "Position".to_string(),
                version: 2,
            }
        );
    }

    #[test]
    fn tracks_multiple_aggregates() {
        let mut aggregate = Lifecycle::<SchemaRegistry>::default();

        aggregate.apply(SchemaRegistryEvent::VersionUpdated {
            name: "Position".to_string(),
            version: 1,
        });
        aggregate.apply(SchemaRegistryEvent::VersionUpdated {
            name: "OffchainOrder".to_string(),
            version: 3,
        });

        let Lifecycle::Live(registry) = &aggregate else {
            panic!("Expected Live state");
        };

        assert_eq!(registry.version_of("Position"), Some(1));
        assert_eq!(registry.version_of("OffchainOrder"), Some(3));
        assert_eq!(registry.version_of("Unknown"), None);
    }

    #[tokio::test]
    async fn reconciler_detects_version_change() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!("../../migrations").run(&pool).await.unwrap();

        let reconciler = Reconciler::new(pool);

        // First run: no stored version -> changed.
        let outcome = reconciler.reconcile::<SchemaRegistry>().await.unwrap();
        assert_eq!(outcome, SchemaReconciliation::Changed);

        // Until the version is recorded, every reconcile still reports a change
        // -- a crash before record_version re-runs the whole recovery.
        let outcome = reconciler.reconcile::<SchemaRegistry>().await.unwrap();
        assert_eq!(outcome, SchemaReconciliation::Changed);

        reconciler.record_version::<SchemaRegistry>().await.unwrap();

        // After recording, the version matches -> unchanged.
        let outcome = reconciler.reconcile::<SchemaRegistry>().await.unwrap();
        assert_eq!(outcome, SchemaReconciliation::Unchanged);
    }

    #[tokio::test]
    async fn load_registry_replays_from_events() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!("../../migrations").run(&pool).await.unwrap();

        let reconciler = Reconciler::new(pool);

        // Initially empty
        let registry = reconciler.load_registry().await.unwrap();
        assert!(registry.is_none());

        // Reconcile then record the version into the registry.
        let _ = reconciler.reconcile::<SchemaRegistry>().await.unwrap();
        reconciler.record_version::<SchemaRegistry>().await.unwrap();

        // Should have SchemaRegistry at version 1
        let registry = reconciler.load_registry().await.unwrap().unwrap();
        assert_eq!(registry.version_of("SchemaRegistry"), Some(1));
    }

    /// Minimal compactable entity for testing the reconciliation guard.
    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct CompactableWidget;

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
    enum CompactableEvent {
        Created,
    }

    impl cqrs_es::DomainEvent for CompactableEvent {
        fn event_type(&self) -> String {
            "Created".to_string()
        }
        fn event_version(&self) -> String {
            "1.0".to_string()
        }
    }

    #[async_trait]
    impl EventSourced for CompactableWidget {
        type Id = String;
        type Event = CompactableEvent;
        type Command = ();
        type Error = Never;
        type Services = ();
        type Materialized = Nil;

        const AGGREGATE_TYPE: &'static str = "CompactableWidget";
        const PROJECTION: Nil = Nil;
        const SCHEMA_VERSION: u64 = 2;
        const COMPACTION_POLICY: CompactionPolicy = CompactionPolicy::CompactAfterSnapshot;

        fn originate(_event: &Self::Event) -> Option<Self> {
            Some(Self)
        }

        fn evolve(_entity: &Self, _event: &Self::Event) -> Result<Option<Self>, Never> {
            Ok(Some(Self))
        }

        async fn initialize(
            _command: Self::Command,
            _services: &Self::Services,
        ) -> Result<Vec<Self::Event>, Never> {
            Ok(vec![CompactableEvent::Created])
        }

        async fn transition(
            &self,
            _command: Self::Command,
            _services: &Self::Services,
        ) -> Result<Vec<Self::Event>, Never> {
            Ok(vec![])
        }
    }

    #[tokio::test]
    async fn reconcile_allows_first_registration_for_compactable_aggregate() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!("../../migrations").run(&pool).await.unwrap();

        let reconciler = Reconciler::new(pool);

        // First reconcile: stored=None, current=2. No prior snapshots
        // exist, so this should succeed even for compactable entities.
        let outcome = reconciler.reconcile::<CompactableWidget>().await.unwrap();

        assert_eq!(outcome, SchemaReconciliation::Changed);
    }

    #[tokio::test]
    async fn reconcile_errors_on_version_mismatch_for_compactable_aggregate() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!("../../migrations").run(&pool).await.unwrap();

        let reconciler = Reconciler::new(pool.clone());

        // First registration succeeds (stored=None) and records version 2.
        let _ = reconciler.reconcile::<CompactableWidget>().await.unwrap();
        reconciler
            .record_version::<CompactableWidget>()
            .await
            .unwrap();

        // Manually update the stored version to simulate a version
        // bump (we can't change the const at runtime).
        sqlx::query(
            "UPDATE events SET payload = \
             '{\"VersionUpdated\":{\"name\":\"CompactableWidget\",\"version\":999}}' \
             WHERE aggregate_type = 'SchemaRegistry' \
             AND payload LIKE '%CompactableWidget%'",
        )
        .execute(&pool)
        .await
        .unwrap();

        // Second reconcile: stored=999, current=2. The entity is
        // compactable, so reconcile should refuse.
        let result = reconciler.reconcile::<CompactableWidget>().await;

        assert!(
            matches!(result, Err(ReconcileError::CompactedSnapshotClear { .. })),
            "Expected CompactedSnapshotClear error, got {result:?}"
        );
    }
}
