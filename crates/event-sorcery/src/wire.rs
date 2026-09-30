//! CQRS framework construction via [`StoreBuilder`].
//!
//! All CQRS framework construction must go through
//! [`StoreBuilder`], which reconciles schema versions and
//! registers reactors. Direct CQRS framework construction is
//! blocked via clippy's `disallowed-methods`; `StoreBuilder`
//! contains the narrow escape hatch.
//!
//! # Registering reactors
//!
//! Use [`.with()`](StoreBuilder::with) to register a reactor
//! with a builder. For single-entity reactors, wrap in
//! `Arc::new()`. For multi-entity reactors, clone the same
//! `Arc` into each builder.
//!
//! # Auto-wired projections
//!
//! `build()` dispatches on `Entity::Materialized` via a type
//! parameter that defaults to `Entity::Materialized`:
//!
//! - `Table` entities: auto-creates and wires a [`Projection`],
//!   returning `(Arc<Store>, Arc<Projection>)`.
//! - `Nil` entities: returns `Arc<Store>`.
//!
//! This eliminates the footgun of forgetting to wire a
//! projection -- if the entity declares a table, the projection
//! is always present.
//!
//! Exhaustive entity handling is enforced by the reactor's
//! [`.on()`](crate::OneOf::on) /
//! [`.exhaustive()`](crate::Fold::exhaustive) chain at compile
//! time, not by the wiring infrastructure.

use std::fmt::Debug;
use std::str::FromStr;
use std::sync::Arc;

use cqrs_es::persist::PersistedEventStore;
use cqrs_es::persist::PersistenceError;
use cqrs_es::{AggregateError, CqrsFramework, EventStore, Query};
use sqlx::SqlitePool;
use tracing::{info, warn};

use crate::Nil;
use crate::dependency::HasEntity;
use crate::lifecycle::{Lifecycle, LifecycleError, ReactorBridge};
use crate::projection::{Projection, ProjectionError, Table};
use crate::reactor::Reactor;
use crate::schema_registry::{ReconcileError, Reconciler, SchemaReconciliation};
use crate::sqlite_event_repository::SqliteEventRepository;
use crate::{CompactionPolicy, EventSourced, SqliteCqrs, Store};

/// Builder for a single CQRS framework.
///
/// Parameterized on an [`EventSourced`] entity type. The
/// `Materialized` type parameter defaults to
/// `Entity::Materialized` and determines the `build()` return
/// type: `Table` returns `(Arc<Store>, Arc<Projection>)`, `Nil`
/// returns `Arc<Store>`.
///
/// Register reactors via [`.with()`](Self::with), then call
/// [`.build()`](Self::build) to construct the framework.
pub struct StoreBuilder<Entity: EventSourced, Materialized = <Entity as EventSourced>::Materialized>
{
    pool: SqlitePool,
    queries: Vec<Box<dyn Query<Lifecycle<Entity>>>>,
    _materialized: std::marker::PhantomData<Materialized>,
}

impl<Entity: EventSourced> StoreBuilder<Entity> {
    /// Creates a new builder for the given entity type.
    pub fn new(pool: SqlitePool) -> Self {
        Self {
            pool,
            queries: vec![],
            _materialized: std::marker::PhantomData,
        }
    }

    /// Registers a reactor with this CQRS framework.
    ///
    /// The reactor must declare `Entity` in its dependency list
    /// (via [`deps!`](crate::deps)). For multi-entity reactors,
    /// clone the same `Arc` into each relevant builder.
    #[must_use]
    pub fn with<R>(mut self, reactor: Arc<R>) -> Self
    where
        R: Reactor + 'static,
        R::Dependencies: HasEntity<Entity>,
        Entity::Id: Clone,
        Entity::Event: Clone,
        <Entity::Id as FromStr>::Err: Debug,
    {
        self.queries.push(Box::new(ReactorBridge { reactor }));
        self
    }
}

fn sqlite_snapshot_cqrs<Entity: EventSourced>(
    pool: SqlitePool,
    queries: Vec<Box<dyn Query<Lifecycle<Entity>>>>,
    services: Entity::Services,
) -> SqliteCqrs<Entity> {
    let repo = SqliteEventRepository::new(pool, Entity::COMPACTION_POLICY);
    let store = PersistedEventStore::<SqliteEventRepository, Lifecycle<Entity>>::new_snapshot_store(
        repo,
        Entity::SNAPSHOT_SIZE,
    );
    #[allow(clippy::disallowed_methods)]
    CqrsFramework::new(store, queries, services)
}

/// Writes a snapshot for every retained `Entity` aggregate whose stream
/// reaches `SNAPSHOT_SIZE` events and that has no snapshot.
///
/// A schema version change clears every snapshot of the type, and commits
/// only write a new one when a command crosses a `SNAPSHOT_SIZE` boundary. An
/// aggregate that receives no commands would replay its full stream on every
/// load. Running on every build, not only after a version change, also heals
/// snapshots cleared by an earlier release and a rebuild interrupted by a
/// crash.
///
/// Runs before the store accepts commands, so no commit races it. Aggregates
/// are rebuilt one at a time so startup does not contend with itself on
/// SQLite. Compactable entities are skipped: the events behind their snapshot
/// may be gone, so replaying what remains would build wrong state.
async fn rebuild_missing_snapshots<Entity: EventSourced>(
    pool: &SqlitePool,
) -> Result<(), ReconcileError> {
    if matches!(
        Entity::COMPACTION_POLICY,
        CompactionPolicy::CompactAfterSnapshot
    ) {
        return Ok(());
    }

    let repo = SqliteEventRepository::new(pool.clone(), Entity::COMPACTION_POLICY);
    let aggregate_ids = repo
        .aggregates_missing_snapshot::<Lifecycle<Entity>>(Entity::SNAPSHOT_SIZE)
        .await?;
    if aggregate_ids.is_empty() {
        return Ok(());
    }

    let store = PersistedEventStore::<SqliteEventRepository, Lifecycle<Entity>>::new_snapshot_store(
        SqliteEventRepository::new(pool.clone(), Entity::COMPACTION_POLICY),
        Entity::SNAPSHOT_SIZE,
    );

    let mut rebuilt = 0_usize;
    for aggregate_id in &aggregate_ids {
        let context = store
            .load_aggregate(aggregate_id)
            .await
            .map_err(|error| snapshot_rebuild_error::<Entity>(aggregate_id, error))?;

        // A snapshot would freeze the failure: later loads would start from it,
        // so a code fix to `evolve` could no longer heal the aggregate by
        // replaying its events.
        if matches!(&context.aggregate, Lifecycle::Failed { .. }) {
            warn!(
                target: "cqrs",
                aggregate = Entity::AGGREGATE_TYPE,
                aggregate_id,
                "Skipping snapshot rebuild for a failed lifecycle"
            );
            continue;
        }

        let snapshot_version = context.current_snapshot.map_or(1, |version| version + 1);

        repo.insert_snapshot_if_absent::<Lifecycle<Entity>>(
            aggregate_id,
            context.current_sequence,
            snapshot_version,
            serde_json::to_value(&context.aggregate)?,
        )
        .await?;
        rebuilt += 1;
    }

    info!(
        target: "cqrs",
        aggregate = Entity::AGGREGATE_TYPE,
        rebuilt,
        "Rebuilt missing snapshots"
    );

    Ok(())
}

/// A failed snapshot rebuild, naming the stream that blocked startup.
#[derive(Debug, thiserror::Error)]
#[error("failed to rebuild the {aggregate_type} snapshot for {aggregate_id}: {source}")]
struct SnapshotRebuildError {
    aggregate_type: &'static str,
    aggregate_id: String,
    source: Box<dyn std::error::Error + Send + Sync>,
}

/// Keeps the connection and deserialization classes of a replay failure, so
/// callers can still tell a transient read failure from a malformed event.
fn snapshot_rebuild_error<Entity: EventSourced>(
    aggregate_id: &str,
    error: AggregateError<LifecycleError<Entity>>,
) -> PersistenceError {
    let context = |source| {
        Box::new(SnapshotRebuildError {
            aggregate_type: Entity::AGGREGATE_TYPE,
            aggregate_id: aggregate_id.to_string(),
            source,
        })
    };

    match error {
        AggregateError::DatabaseConnectionError(source) => {
            PersistenceError::ConnectionError(context(source))
        }
        AggregateError::DeserializationError(source) => {
            PersistenceError::DeserializationError(context(source))
        }
        other => PersistenceError::UnknownError(context(Box::new(other))),
    }
}

/// Projected entities: auto-creates and wires a [`Projection`],
/// returning `(Arc<Store>, Arc<Projection>)`.
impl<Entity: EventSourced<Materialized = Table> + 'static> StoreBuilder<Entity, Table>
where
    Entity::Id: Clone,
    Entity::Event: Clone,
    <Entity::Id as FromStr>::Err: Debug,
{
    pub async fn build(
        mut self,
        services: Entity::Services,
    ) -> Result<(Arc<Store<Entity>>, Arc<Projection<Entity>>), ReconcileError> {
        // Projected entities must retain all events so that
        // `catch_up`/`rebuild_all` can replay the full history.
        // Compacted aggregates lose events after snapshot, making
        // projection rebuilds silently incomplete.
        const {
            assert!(
                matches!(Entity::COMPACTION_POLICY, CompactionPolicy::Retain),
                "CompactAfterSnapshot entities must not have table projections -- \
                 rebuild_all only reads the events table and would miss \
                 compacted snapshot-only aggregates"
            );
        }

        let reconciler = Reconciler::new(self.pool.clone());
        let reconciliation = reconciler.reconcile::<Entity>().await?;

        let projection = Arc::new(Projection::sqlite(self.pool.clone()));

        // A schema version change can leave stored view payloads in an
        // incompatible format. `catch_up` only revisits views that are behind
        // on event sequence, so a view that is current but incompatible (the
        // normal state after a view-schema change with no new events) would
        // never be healed. On a detected schema change, rebuild every view
        // from the event log; otherwise just replay any events the view missed
        // due to a crash between event persistence and view update. Projected
        // entities are guaranteed `CompactionPolicy::Retain` (asserted above),
        // so the full history is always available to rebuild from. Either path
        // runs before registering the projection as a reactor so no concurrent
        // writes can interfere.
        let recovery = match reconciliation {
            SchemaReconciliation::Changed => projection.rebuild_all().await,
            SchemaReconciliation::Unchanged => projection.catch_up().await,
        };
        recovery.map_err(|error| match error {
            ProjectionError::Sqlx(sqlx_error) => ReconcileError::from(sqlx_error),
            ProjectionError::Persistence(persistence_error) => {
                ReconcileError::from(persistence_error)
            }
            other => ReconcileError::Persistence(PersistenceError::UnknownError(Box::new(other))),
        })?;

        rebuild_missing_snapshots::<Entity>(&self.pool).await?;

        // Mark the schema version reconciled only after the snapshot clear and
        // view recovery above have durably completed. A crash before this point
        // leaves the version unadvanced, so the next startup re-runs the whole
        // reconcile -- including rebuild_all -- instead of recording a version
        // whose view rebuild never finished.
        reconciler.record_version::<Entity>().await?;

        self.queries.push(Box::new(ReactorBridge {
            reactor: projection.clone(),
        }));

        let cqrs = sqlite_snapshot_cqrs(self.pool.clone(), self.queries, services);
        Ok((Arc::new(Store::new(cqrs, self.pool)), projection))
    }
}

/// Non-projected entities: returns just `Store`.
impl<Entity: EventSourced<Materialized = Nil>> StoreBuilder<Entity, Nil> {
    pub async fn build(
        self,
        services: Entity::Services,
    ) -> Result<Arc<Store<Entity>>, ReconcileError> {
        // A non-projected entity has no views to rebuild, so the reconciliation
        // outcome does not change recovery; reconcile still clears stale
        // snapshots, the rebuild replaces them, and record_version marks the
        // version handled.
        let reconciler = Reconciler::new(self.pool.clone());
        let _ = reconciler.reconcile::<Entity>().await?;
        rebuild_missing_snapshots::<Entity>(&self.pool).await?;
        reconciler.record_version::<Entity>().await?;

        let cqrs = sqlite_snapshot_cqrs(self.pool.clone(), self.queries, services);
        Ok(Arc::new(Store::new(cqrs, self.pool)))
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use cqrs_es::DomainEvent;
    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::dependency::EntityList;
    use crate::deps;
    use crate::lifecycle::{Lifecycle, Never};

    #[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
    struct AggregateA;

    #[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
    struct AggregateB;

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct EventA;

    impl DomainEvent for EventA {
        fn event_type(&self) -> String {
            "EventA".to_string()
        }

        fn event_version(&self) -> String {
            "1.0".to_string()
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct EventB;

    impl DomainEvent for EventB {
        fn event_type(&self) -> String {
            "EventB".to_string()
        }

        fn event_version(&self) -> String {
            "1.0".to_string()
        }
    }

    #[async_trait]
    impl EventSourced for AggregateA {
        type Id = String;
        type Event = EventA;
        type Command = ();
        type Error = Never;
        type Services = ();
        type Materialized = Nil;

        const AGGREGATE_TYPE: &'static str = "AggregateA";
        const PROJECTION: Nil = Nil;
        const SCHEMA_VERSION: u64 = 1;

        fn originate(_event: &EventA) -> Option<Self> {
            Some(Self)
        }

        fn evolve(_entity: &Self, _event: &EventA) -> Result<Option<Self>, Never> {
            Ok(Some(Self))
        }

        async fn initialize(_command: (), _services: &()) -> Result<Vec<EventA>, Never> {
            Ok(vec![])
        }

        async fn transition(&self, _command: (), _services: &()) -> Result<Vec<EventA>, Never> {
            Ok(vec![])
        }
    }

    #[async_trait]
    impl EventSourced for AggregateB {
        type Id = String;
        type Event = EventB;
        type Command = ();
        type Error = Never;
        type Services = ();
        type Materialized = Nil;

        const AGGREGATE_TYPE: &'static str = "AggregateB";
        const PROJECTION: Nil = Nil;
        const SCHEMA_VERSION: u64 = 1;

        fn originate(_event: &EventB) -> Option<Self> {
            Some(Self)
        }

        fn evolve(_entity: &Self, _event: &EventB) -> Result<Option<Self>, Never> {
            Ok(Some(Self))
        }

        async fn initialize(_command: (), _services: &()) -> Result<Vec<EventB>, Never> {
            Ok(vec![])
        }

        async fn transition(&self, _command: (), _services: &()) -> Result<Vec<EventB>, Never> {
            Ok(vec![])
        }
    }

    struct MultiEntityReactor;

    deps!(MultiEntityReactor, [AggregateA, AggregateB]);

    #[async_trait]
    impl Reactor for MultiEntityReactor {
        type Error = Never;

        async fn react(
            &self,
            event: <Self::Dependencies as EntityList>::Event,
        ) -> Result<(), Self::Error> {
            event
                .on(|_id, _event| async {})
                .on(|_id, _event| async {})
                .exhaustive()
                .await;
            Ok(())
        }
    }

    struct SingleEntityReactor;

    deps!(SingleEntityReactor, [AggregateA]);

    #[async_trait]
    impl Reactor for SingleEntityReactor {
        type Error = Never;

        async fn react(
            &self,
            event: <Self::Dependencies as EntityList>::Event,
        ) -> Result<(), Self::Error> {
            let (_id, _event) = event.into_inner();
            Ok(())
        }
    }

    #[tokio::test]
    async fn single_entity_wiring() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!("../../migrations").run(&pool).await.unwrap();

        let _store = StoreBuilder::<AggregateA>::new(pool.clone())
            .with(Arc::new(SingleEntityReactor))
            .build(())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn multi_entity_wiring() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!("../../migrations").run(&pool).await.unwrap();

        let multi = Arc::new(MultiEntityReactor);
        let single = Arc::new(SingleEntityReactor);

        let _store_a = StoreBuilder::<AggregateA>::new(pool.clone())
            .with(multi.clone())
            .with(single)
            .build(())
            .await
            .unwrap();

        let _store_b = StoreBuilder::<AggregateB>::new(pool.clone())
            .with(multi)
            .build(())
            .await
            .unwrap();
    }

    #[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
    struct Tally {
        count: u64,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    enum TallyEvent {
        Bumped,
    }

    impl DomainEvent for TallyEvent {
        fn event_type(&self) -> String {
            "TallyEvent::Bumped".to_string()
        }

        fn event_version(&self) -> String {
            "1.0".to_string()
        }
    }

    #[async_trait]
    impl EventSourced for Tally {
        type Id = String;
        type Event = TallyEvent;
        type Command = ();
        type Error = Never;
        type Services = ();
        type Materialized = Table;

        const AGGREGATE_TYPE: &'static str = "Tally";
        const PROJECTION: Table = Table("tally_view");
        const SCHEMA_VERSION: u64 = 1;

        fn originate(event: &TallyEvent) -> Option<Self> {
            match event {
                TallyEvent::Bumped => Some(Self { count: 1 }),
            }
        }

        fn evolve(entity: &Self, event: &TallyEvent) -> Result<Option<Self>, Never> {
            match event {
                TallyEvent::Bumped => Ok(Some(Self {
                    count: entity.count + 1,
                })),
            }
        }

        async fn initialize(_command: (), _services: &()) -> Result<Vec<TallyEvent>, Never> {
            Ok(vec![])
        }

        async fn transition(&self, _command: (), _services: &()) -> Result<Vec<TallyEvent>, Never> {
            Ok(vec![])
        }
    }

    /// A schema-version change (here, the first build, when no stored version
    /// exists) must rebuild views from the event log, healing a view that is
    /// current on event sequence but holds an incompatible payload -- the case
    /// `catch_up` alone never revisits because the view is not behind.
    #[tokio::test]
    async fn schema_change_rebuilds_incompatible_current_view() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!("../../migrations").run(&pool).await.unwrap();

        sqlx::query(
            "CREATE TABLE tally_view ( \
                 view_id TEXT NOT NULL PRIMARY KEY, \
                 version BIGINT NOT NULL, \
                 payload TEXT NOT NULL \
             )",
        )
        .execute(&pool)
        .await
        .unwrap();

        let payload = serde_json::to_string(&TallyEvent::Bumped).unwrap();
        for sequence in 1..=3 {
            sqlx::query(
                "INSERT INTO events (aggregate_type, aggregate_id, sequence, event_type, \
                 event_version, payload, metadata) \
                 VALUES ('Tally', 'tally-1', ?1, 'TallyEvent::Bumped', '1.0', ?2, '{}')",
            )
            .bind(sequence)
            .bind(&payload)
            .execute(&pool)
            .await
            .unwrap();
        }

        // A view that is CURRENT (version == max_seq == 3) but holds an
        // incompatible payload. `catch_up` would never revisit it.
        sqlx::query("INSERT INTO tally_view (view_id, version, payload) VALUES ('tally-1', 3, ?1)")
            .bind(r#"{"Completed": {"count": 0}}"#)
            .execute(&pool)
            .await
            .unwrap();

        let (_store, _projection) = StoreBuilder::<Tally>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        let healed: String =
            sqlx::query_scalar("SELECT payload FROM tally_view WHERE view_id = 'tally-1'")
                .fetch_one(&pool)
                .await
                .unwrap();
        let lifecycle: Lifecycle<Tally> = serde_json::from_str(&healed).unwrap();
        assert!(matches!(lifecycle, Lifecycle::Live(Tally { count: 3 })));

        let version: i64 =
            sqlx::query_scalar("SELECT version FROM tally_view WHERE view_id = 'tally-1'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(version, 3);
    }

    /// A real version transition (a prior version is already recorded, then the
    /// code's `SCHEMA_VERSION` differs) also rebuilds incompatible current
    /// views. This distinguishes the genuine-mismatch trigger from the
    /// first-registration trigger above: a regression that gated `rebuild_all`
    /// on `stored_version.is_some()` would pass the test above but fail here.
    #[tokio::test]
    async fn schema_version_bump_rebuilds_incompatible_current_view() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!("../../migrations").run(&pool).await.unwrap();

        sqlx::query(
            "CREATE TABLE tally_view ( \
                 view_id TEXT NOT NULL PRIMARY KEY, \
                 version BIGINT NOT NULL, \
                 payload TEXT NOT NULL \
             )",
        )
        .execute(&pool)
        .await
        .unwrap();

        // Seed a PRIOR recorded schema version (0) for Tally, so build() sees a
        // genuine 0 -> 1 mismatch rather than a first registration.
        sqlx::query(
            "INSERT INTO events (aggregate_type, aggregate_id, sequence, event_type, \
             event_version, payload, metadata) \
             VALUES ('SchemaRegistry', 'schema', 1, 'SchemaRegistryEvent::VersionUpdated', '1.0', \
             '{\"VersionUpdated\":{\"name\":\"Tally\",\"version\":0}}', '{}')",
        )
        .execute(&pool)
        .await
        .unwrap();

        let payload = serde_json::to_string(&TallyEvent::Bumped).unwrap();
        for sequence in 1..=3 {
            sqlx::query(
                "INSERT INTO events (aggregate_type, aggregate_id, sequence, event_type, \
                 event_version, payload, metadata) \
                 VALUES ('Tally', 'tally-1', ?1, 'TallyEvent::Bumped', '1.0', ?2, '{}')",
            )
            .bind(sequence)
            .bind(&payload)
            .execute(&pool)
            .await
            .unwrap();
        }

        sqlx::query("INSERT INTO tally_view (view_id, version, payload) VALUES ('tally-1', 3, ?1)")
            .bind(r#"{"Completed": {"count": 0}}"#)
            .execute(&pool)
            .await
            .unwrap();

        let (_store, _projection) = StoreBuilder::<Tally>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        let healed: String =
            sqlx::query_scalar("SELECT payload FROM tally_view WHERE view_id = 'tally-1'")
                .fetch_one(&pool)
                .await
                .unwrap();
        let lifecycle: Lifecycle<Tally> = serde_json::from_str(&healed).unwrap();
        assert!(matches!(lifecycle, Lifecycle::Live(Tally { count: 3 })));

        // The version bookmark must advance to max_seq; otherwise the next
        // catch_up would re-replay every event and double-apply increments.
        let version: i64 =
            sqlx::query_scalar("SELECT version FROM tally_view WHERE view_id = 'tally-1'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(version, 3);
    }

    #[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
    struct CompactableCounter;

    #[async_trait]
    impl EventSourced for CompactableCounter {
        type Id = String;
        type Event = EventA;
        type Command = ();
        type Error = Never;
        type Services = ();
        type Materialized = Nil;

        const AGGREGATE_TYPE: &'static str = "CompactableCounter";
        const PROJECTION: Nil = Nil;
        const SCHEMA_VERSION: u64 = 1;
        const COMPACTION_POLICY: CompactionPolicy = CompactionPolicy::CompactAfterSnapshot;

        fn originate(_event: &EventA) -> Option<Self> {
            Some(Self)
        }

        fn evolve(_entity: &Self, _event: &EventA) -> Result<Option<Self>, Never> {
            Ok(Some(Self))
        }

        async fn initialize(_command: (), _services: &()) -> Result<Vec<EventA>, Never> {
            Ok(vec![])
        }

        async fn transition(&self, _command: (), _services: &()) -> Result<Vec<EventA>, Never> {
            Ok(vec![])
        }
    }

    async fn migrated_pool_with_tally_view() -> SqlitePool {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!("../../migrations").run(&pool).await.unwrap();
        sqlx::query(
            "CREATE TABLE tally_view ( \
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

    async fn insert_events(
        pool: &SqlitePool,
        aggregate_type: &str,
        aggregate_id: &str,
        event_type: &str,
        payload: &str,
        sequences: std::ops::RangeInclusive<i64>,
    ) {
        for sequence in sequences {
            sqlx::query(
                "INSERT INTO events (aggregate_type, aggregate_id, sequence, event_type, \
                 event_version, payload, metadata) \
                 VALUES (?1, ?2, ?3, ?4, '1.0', ?5, '{}')",
            )
            .bind(aggregate_type)
            .bind(aggregate_id)
            .bind(sequence)
            .bind(event_type)
            .bind(payload)
            .execute(pool)
            .await
            .unwrap();
        }
    }

    async fn insert_tally_events(
        pool: &SqlitePool,
        aggregate_id: &str,
        sequences: std::ops::RangeInclusive<i64>,
    ) {
        let payload = serde_json::to_string(&TallyEvent::Bumped).unwrap();
        insert_events(
            pool,
            "Tally",
            aggregate_id,
            "TallyEvent::Bumped",
            &payload,
            sequences,
        )
        .await;
    }

    async fn insert_snapshot(
        pool: &SqlitePool,
        aggregate_type: &str,
        aggregate_id: &str,
        last_sequence: i64,
        payload: &str,
    ) {
        sqlx::query(
            "INSERT INTO snapshots \
             (aggregate_type, aggregate_id, last_sequence, snapshot_version, payload, timestamp) \
             VALUES (?1, ?2, ?3, 1, ?4, '2026-09-30T00:00:00.000Z')",
        )
        .bind(aggregate_type)
        .bind(aggregate_id)
        .bind(last_sequence)
        .bind(payload)
        .execute(pool)
        .await
        .unwrap();
    }

    /// `(last_sequence, snapshot_version, payload)` of the stored snapshot.
    async fn stored_snapshot(
        pool: &SqlitePool,
        aggregate_type: &str,
        aggregate_id: &str,
    ) -> Option<(i64, i64, String)> {
        sqlx::query_as(
            "SELECT last_sequence, snapshot_version, payload FROM snapshots \
             WHERE aggregate_type = ?1 AND aggregate_id = ?2",
        )
        .bind(aggregate_type)
        .bind(aggregate_id)
        .fetch_optional(pool)
        .await
        .unwrap()
    }

    /// RAI-2766: a schema version bump clears every snapshot. Without a
    /// rebuild, an aggregate that receives no commands replays its full
    /// stream on every load. `build()` must leave it with a snapshot at its
    /// latest sequence.
    #[tokio::test]
    async fn schema_version_bump_rebuilds_cleared_snapshot_at_latest_sequence() {
        let pool = migrated_pool_with_tally_view().await;

        insert_events(
            &pool,
            "SchemaRegistry",
            "schema",
            "SchemaRegistryEvent::VersionUpdated",
            r#"{"VersionUpdated":{"name":"Tally","version":0}}"#,
            1..=1,
        )
        .await;
        insert_tally_events(&pool, "tally-1", 1..=25).await;
        insert_snapshot(&pool, "Tally", "tally-1", 20, r#"{"stale":"shape"}"#).await;

        let (store, _projection) = StoreBuilder::<Tally>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        let (last_sequence, snapshot_version, payload) =
            stored_snapshot(&pool, "Tally", "tally-1").await.unwrap();
        assert_eq!(last_sequence, 25);
        assert_eq!(snapshot_version, 1);
        let lifecycle: Lifecycle<Tally> = serde_json::from_str(&payload).unwrap();
        assert!(matches!(lifecycle, Lifecycle::Live(Tally { count: 25 })));

        let loaded = store.load(&"tally-1".to_string()).await.unwrap();
        assert_eq!(loaded, Some(Tally { count: 25 }));
    }

    /// Fails on its second event, so a replay ends in `Lifecycle::Failed`.
    #[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
    struct Brittle;

    #[async_trait]
    impl EventSourced for Brittle {
        type Id = String;
        type Event = EventA;
        type Command = ();
        type Error = Never;
        type Services = ();
        type Materialized = Nil;

        const AGGREGATE_TYPE: &'static str = "Brittle";
        const PROJECTION: Nil = Nil;
        const SCHEMA_VERSION: u64 = 1;

        fn originate(_event: &EventA) -> Option<Self> {
            Some(Self)
        }

        fn evolve(_entity: &Self, _event: &EventA) -> Result<Option<Self>, Never> {
            Ok(None)
        }

        async fn initialize(_command: (), _services: &()) -> Result<Vec<EventA>, Never> {
            Ok(vec![])
        }

        async fn transition(&self, _command: (), _services: &()) -> Result<Vec<EventA>, Never> {
            Ok(vec![])
        }
    }

    /// A missing snapshot is rebuilt even when the schema version is already
    /// recorded. This covers a deployment whose snapshots were cleared by an
    /// earlier release that did not rebuild them. Every missing snapshot is
    /// rebuilt in one build, a snapshot of another aggregate type with the
    /// same ID does not count, and an existing snapshot is left as it is, even
    /// when events were committed after it.
    #[tokio::test]
    async fn unchanged_schema_version_rebuilds_only_missing_snapshots() {
        let pool = migrated_pool_with_tally_view().await;
        let (_store, _projection) = StoreBuilder::<Tally>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        insert_tally_events(&pool, "tally-1", 1..=12).await;
        insert_snapshot(&pool, "AggregateA", "tally-1", 12, "null").await;
        insert_tally_events(&pool, "tally-2", 1..=20).await;
        insert_snapshot(&pool, "Tally", "tally-2", 10, r#"{"Live":{"count":10}}"#).await;
        insert_tally_events(&pool, "tally-3", 1..=15).await;

        let (_store, _projection) = StoreBuilder::<Tally>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        let (last_sequence, snapshot_version, payload) =
            stored_snapshot(&pool, "Tally", "tally-1").await.unwrap();
        assert_eq!(last_sequence, 12);
        assert_eq!(snapshot_version, 1);
        let lifecycle: Lifecycle<Tally> = serde_json::from_str(&payload).unwrap();
        assert!(matches!(lifecycle, Lifecycle::Live(Tally { count: 12 })));

        let (last_sequence, _, payload) = stored_snapshot(&pool, "Tally", "tally-3").await.unwrap();
        assert_eq!(last_sequence, 15);
        let lifecycle: Lifecycle<Tally> = serde_json::from_str(&payload).unwrap();
        assert!(matches!(lifecycle, Lifecycle::Live(Tally { count: 15 })));

        let (last_sequence, _, payload) = stored_snapshot(&pool, "Tally", "tally-2").await.unwrap();
        assert_eq!(last_sequence, 10);
        assert_eq!(payload, r#"{"Live":{"count":10}}"#);
    }

    /// A stream that replays to a failed lifecycle gets no rebuilt snapshot:
    /// it would freeze the failure, so a code fix to `evolve` could no longer
    /// heal the aggregate by replaying its events. The build still succeeds.
    #[tokio::test]
    async fn failed_lifecycle_gets_no_rebuilt_snapshot() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!("../../migrations").run(&pool).await.unwrap();
        let payload = serde_json::to_string(&EventA).unwrap();
        insert_events(&pool, "Brittle", "b-1", "EventA", &payload, 1..=10).await;

        let _store = StoreBuilder::<Brittle>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        assert_eq!(stored_snapshot(&pool, "Brittle", "b-1").await, None);
    }

    /// A rebuild that fails must not record the schema version: the recorded
    /// version is the marker that startup recovery finished. The error names
    /// the stream that failed and keeps the deserialization class.
    #[tokio::test]
    async fn failed_rebuild_does_not_record_schema_version() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!("../../migrations").run(&pool).await.unwrap();
        let payload = serde_json::to_string(&EventA).unwrap();
        insert_events(&pool, "AggregateA", "a-1", "EventA", &payload, 1..=9).await;
        insert_events(
            &pool,
            "AggregateA",
            "a-1",
            "EventA",
            r#"{"not":"an EventA"}"#,
            10..=10,
        )
        .await;

        let Err(error) = StoreBuilder::<AggregateA>::new(pool.clone())
            .build(())
            .await
        else {
            panic!("build must fail when a snapshot rebuild fails");
        };

        assert!(matches!(
            error,
            ReconcileError::Persistence(PersistenceError::DeserializationError(_))
        ));
        assert!(error.to_string().contains("AggregateA snapshot for a-1"));

        let recorded: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM events \
             WHERE aggregate_type = 'SchemaRegistry' AND payload LIKE '%\"AggregateA\"%'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(recorded, 0);
    }

    /// A stream shorter than `SNAPSHOT_SIZE` would not have a snapshot through
    /// normal commits either, and replaying it is cheap.
    #[tokio::test]
    async fn stream_shorter_than_snapshot_size_gets_no_snapshot() {
        let pool = migrated_pool_with_tally_view().await;
        insert_tally_events(&pool, "tally-1", 1..=9).await;

        let (_store, _projection) = StoreBuilder::<Tally>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        assert_eq!(stored_snapshot(&pool, "Tally", "tally-1").await, None);
    }

    #[tokio::test]
    async fn non_projected_entity_build_rebuilds_missing_snapshot() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!("../../migrations").run(&pool).await.unwrap();
        let payload = serde_json::to_string(&EventA).unwrap();
        insert_events(&pool, "AggregateA", "a-1", "EventA", &payload, 1..=10).await;

        let _store = StoreBuilder::<AggregateA>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        let (last_sequence, snapshot_version, _) =
            stored_snapshot(&pool, "AggregateA", "a-1").await.unwrap();
        assert_eq!(last_sequence, 10);
        assert_eq!(snapshot_version, 1);
    }

    /// A compactable aggregate may have lost the events behind a snapshot,
    /// so replaying its remaining events would build wrong state. Snapshot
    /// rebuild leaves compactable entities alone.
    #[tokio::test]
    async fn compactable_entity_build_does_not_rebuild_snapshots() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!("../../migrations").run(&pool).await.unwrap();
        let payload = serde_json::to_string(&EventA).unwrap();
        insert_events(
            &pool,
            "CompactableCounter",
            "c-1",
            "EventA",
            &payload,
            1..=10,
        )
        .await;

        let _store = StoreBuilder::<CompactableCounter>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        assert_eq!(
            stored_snapshot(&pool, "CompactableCounter", "c-1").await,
            None
        );
    }
}
