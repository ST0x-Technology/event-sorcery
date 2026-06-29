use cqrs_es::Aggregate;
use cqrs_es::persist::{
    PersistedEventRepository, PersistenceError, ReplayStream, SerializedEvent, SerializedSnapshot,
};
use serde_json::Value;
use sqlx::sqlite::SqliteRow;
use sqlx::{AssertSqlSafe, Pool, Row, Sqlite, SqliteConnection};

use crate::sql_query::SqlQueryFactory;

/// Errors that can occur during SQLite event repository operations
#[derive(Debug, thiserror::Error)]
pub enum SqliteAggregateError {
    /// Optimistic locking conflict - the aggregate was modified concurrently
    #[error("Optimistic lock error: aggregate has been modified concurrently")]
    OptimisticLock,

    /// Database connection or query error
    #[error("Database connection error: {0}")]
    Connection(#[from] sqlx::Error),

    /// Event or snapshot deserialization error
    #[error("Event deserialization error: {0}")]
    Deserialization(#[from] serde_json::Error),

    /// Integer conversion error (e.g., sequence number overflow)
    #[error("Integer conversion error: {0}")]
    TryFromInt(#[from] std::num::TryFromIntError),

    /// A snapshot update was requested for a commit that contains no events,
    /// so there is no event sequence to record for the snapshot
    #[error("snapshot update without accompanying events")]
    EmptySnapshotUpdate,
}

impl From<SqliteAggregateError> for PersistenceError {
    fn from(err: SqliteAggregateError) -> Self {
        match err {
            SqliteAggregateError::OptimisticLock => Self::OptimisticLockError,
            SqliteAggregateError::Connection(e) => Self::ConnectionError(Box::new(e)),
            SqliteAggregateError::Deserialization(e) => Self::DeserializationError(Box::new(e)),
            SqliteAggregateError::TryFromInt(e) => Self::UnknownError(Box::new(e)),
            err @ SqliteAggregateError::EmptySnapshotUpdate => Self::UnknownError(Box::new(err)),
        }
    }
}

/// SQLite implementation of the `PersistedEventRepository` trait
///
/// Provides event sourcing persistence backed by SQLite, including:
/// - Event storage with optimistic locking
/// - Snapshot support for performance optimization
/// - Event streaming capabilities
pub struct SqliteEventRepository {
    pool: Pool<Sqlite>,
    query_factory: SqlQueryFactory,
    stream_channel_size: usize,
}

impl PersistedEventRepository for SqliteEventRepository {
    async fn get_events<A: Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> Result<Vec<SerializedEvent>, PersistenceError> {
        Ok(self.load_events::<A>(aggregate_id).await?)
    }

    async fn get_last_events<A: Aggregate>(
        &self,
        aggregate_id: &str,
        last_sequence: usize,
    ) -> Result<Vec<SerializedEvent>, PersistenceError> {
        Ok(self
            .load_events_since::<A>(aggregate_id, last_sequence)
            .await?)
    }

    async fn get_snapshot<A: Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> Result<Option<SerializedSnapshot>, PersistenceError> {
        // This layer is generic over `cqrs_es::Aggregate` and never deletes
        // events, so for the stores it manages the full history is always
        // present and replaying it is safe. It returns the snapshot as stored;
        // on a deserialize failure `cqrs_es::EventStore::load_aggregate` ignores
        // the stale snapshot and rebuilds from events (its built-in corruption
        // recovery). The compaction-aware override -- needed only where events
        // CAN be deleted, so that an unrecoverable snapshot surfaces an error
        // instead of a silent rebuild from incomplete history (ADR-0003) -- lives
        // in the event-sorcery layer that knows `EventSourced::COMPACTION_POLICY`.
        Ok(self.load_snapshot::<A>(aggregate_id).await?)
    }

    async fn persist<A: Aggregate>(
        &self,
        events: &[SerializedEvent],
        snapshot_update: Option<(String, Value, usize)>,
    ) -> Result<(), PersistenceError> {
        // One transaction for both writes: a rejected snapshot write must
        // roll back the events, otherwise the caller sees a conflict for a
        // commit whose events durably persisted and a retry double-applies
        // the command.
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(SqliteAggregateError::from)?;

        self.insert_events_within(&mut tx, events).await?;

        // The third tuple element is cqrs-es's snapshot VERSION counter
        // (`PersistedEventStore::commit` passes `next_snapshot`), not an
        // event sequence. The snapshot's `last_sequence` must come from the
        // events being committed -- conflating the two re-applies already-
        // folded events on every reload after the first snapshot.
        if let Some((aggregate_id, aggregate, snapshot_version)) = snapshot_update {
            let last_sequence = events
                .last()
                .map(|event| event.sequence)
                .ok_or(SqliteAggregateError::EmptySnapshotUpdate)?;

            self.update_snapshot_within::<A>(
                &mut tx,
                &aggregate_id,
                last_sequence,
                snapshot_version,
                aggregate,
            )
            .await?;
        }

        tx.commit().await.map_err(SqliteAggregateError::from)?;

        Ok(())
    }

    async fn stream_events<A: Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> Result<ReplayStream, PersistenceError> {
        Ok(self.stream_events_impl::<A>(Some(aggregate_id)))
    }

    async fn stream_all_events<A: Aggregate>(&self) -> Result<ReplayStream, PersistenceError> {
        Ok(self.stream_events_impl::<A>(None))
    }
}

impl SqliteEventRepository {
    /// Creates a new `SqliteEventRepository` with default table names
    ///
    /// Uses "events" and "snapshots" as the table names with a default stream channel size of 1000.
    #[must_use]
    pub fn new(pool: Pool<Sqlite>) -> Self {
        Self {
            pool,
            query_factory: SqlQueryFactory::new("events".to_string(), "snapshots".to_string()),
            stream_channel_size: 1000,
        }
    }

    /// Creates a new `SqliteEventRepository` with custom table names
    ///
    /// Allows specifying custom names for the events and snapshots tables.
    #[must_use]
    pub const fn with_tables(
        pool: Pool<Sqlite>,
        events_table: String,
        snapshots_table: String,
    ) -> Self {
        Self {
            pool,
            query_factory: SqlQueryFactory::new(events_table, snapshots_table),
            stream_channel_size: 1000,
        }
    }

    /// Sets the channel size for event streaming
    ///
    /// The stream channel size determines how many events can be buffered during streaming operations.
    #[must_use]
    pub const fn with_stream_channel_size(mut self, stream_channel_size: usize) -> Self {
        self.stream_channel_size = stream_channel_size;
        self
    }

    async fn load_events<A: Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> Result<Vec<SerializedEvent>, SqliteAggregateError> {
        let query = self.query_factory.select_events();
        let rows = sqlx::query(AssertSqlSafe(query))
            .bind(A::TYPE)
            .bind(aggregate_id)
            .fetch_all(&self.pool)
            .await?;

        rows.iter().map(row_to_serialized_event).collect()
    }

    async fn load_events_since<A: Aggregate>(
        &self,
        aggregate_id: &str,
        last_sequence: usize,
    ) -> Result<Vec<SerializedEvent>, SqliteAggregateError> {
        let query = self.query_factory.get_last_events();
        let last_sequence_i64 = i64::try_from(last_sequence)?;

        let rows = sqlx::query(AssertSqlSafe(query))
            .bind(A::TYPE)
            .bind(aggregate_id)
            .bind(last_sequence_i64)
            .fetch_all(&self.pool)
            .await?;

        rows.iter().map(row_to_serialized_event).collect()
    }

    async fn load_snapshot<A: Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> Result<Option<SerializedSnapshot>, SqliteAggregateError> {
        let query = self.query_factory.select_snapshot();
        let row = sqlx::query(AssertSqlSafe(query))
            .bind(A::TYPE)
            .bind(aggregate_id)
            .fetch_optional(&self.pool)
            .await?;

        match row {
            None => Ok(None),
            Some(row) => Ok(Some(row_to_serialized_snapshot(&row)?)),
        }
    }

    #[cfg(test)]
    async fn insert_events(&self, events: &[SerializedEvent]) -> Result<(), SqliteAggregateError> {
        let mut tx = self.pool.begin().await?;
        self.insert_events_within(&mut tx, events).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn insert_events_within(
        &self,
        tx: &mut sqlx::Transaction<'_, Sqlite>,
        events: &[SerializedEvent],
    ) -> Result<(), SqliteAggregateError> {
        insert_serialized_events_batch(tx, self.query_factory.events_table(), events).await
    }

    #[cfg(test)]
    async fn update_snapshot<A: Aggregate>(
        &self,
        aggregate_id: &str,
        last_sequence: usize,
        snapshot_version: usize,
        aggregate: Value,
    ) -> Result<(), SqliteAggregateError> {
        let mut tx = self.pool.begin().await?;
        self.update_snapshot_within::<A>(
            &mut tx,
            aggregate_id,
            last_sequence,
            snapshot_version,
            aggregate,
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    /// Writes a snapshot guarded on `last_sequence` monotonicity: a writer
    /// whose snapshot covers an older event sequence loses with an
    /// optimistic-lock error instead of clobbering a newer snapshot.
    ///
    /// Guarding on the event sequence rather than `snapshot_version` keeps
    /// cqrs-es's snapshot-corruption recovery working: after a snapshot
    /// payload fails to deserialize, `load_aggregate` rebuilds from all
    /// events and the next commit re-writes the snapshot at version 1 -- but
    /// always at a strictly later event sequence, so the rebuild replaces the
    /// stale row while genuinely stale writers still cannot.
    async fn update_snapshot_within<A: Aggregate>(
        &self,
        tx: &mut sqlx::Transaction<'_, Sqlite>,
        aggregate_id: &str,
        last_sequence: usize,
        snapshot_version: usize,
        aggregate: Value,
    ) -> Result<(), SqliteAggregateError> {
        let query = self.query_factory.update_snapshot();
        let last_sequence_i64 = i64::try_from(last_sequence)?;
        let snapshot_version_i64 = i64::try_from(snapshot_version)?;

        let timestamp = chrono::Utc::now().to_rfc3339();

        let result = sqlx::query(AssertSqlSafe(query))
            .bind(A::TYPE)
            .bind(aggregate_id)
            .bind(last_sequence_i64)
            .bind(snapshot_version_i64)
            .bind(&aggregate)
            .bind(&timestamp)
            .execute(&mut **tx)
            .await?;

        if result.rows_affected() == 0 {
            return Err(SqliteAggregateError::OptimisticLock);
        }

        Ok(())
    }

    fn stream_events_impl<A: Aggregate>(&self, aggregate_id: Option<&str>) -> ReplayStream {
        let (mut feed, stream) = ReplayStream::new(self.stream_channel_size);

        let pool = self.pool.clone();
        let query = match aggregate_id {
            Some(_) => self.query_factory.select_events(),
            None => self.query_factory.all_events(),
        };
        let aggregate_type = A::TYPE;
        let aggregate_id = aggregate_id.map(String::from);

        tokio::spawn(async move {
            let rows = match &aggregate_id {
                Some(id) => {
                    sqlx::query(AssertSqlSafe(query))
                        .bind(aggregate_type)
                        .bind(id)
                        .fetch_all(&pool)
                        .await
                }
                None => {
                    sqlx::query(AssertSqlSafe(query))
                        .bind(aggregate_type)
                        .fetch_all(&pool)
                        .await
                }
            };

            let rows = match rows {
                Ok(rows) => rows,
                Err(e) => {
                    let _ = feed
                        .push(Err(PersistenceError::ConnectionError(Box::new(e))))
                        .await;
                    return;
                }
            };

            for row in &rows {
                let event = match row_to_serialized_event(row) {
                    Ok(event) => event,
                    Err(e) => {
                        let _ = feed
                            .push(Err(PersistenceError::DeserializationError(Box::new(e))))
                            .await;
                        return;
                    }
                };

                if feed.push(Ok(event)).await.is_err() {
                    return;
                }
            }
        });

        stream
    }
}

fn row_to_serialized_event(row: &SqliteRow) -> Result<SerializedEvent, SqliteAggregateError> {
    let sequence_i64: i64 = row.try_get("sequence")?;
    let sequence = usize::try_from(sequence_i64)?;

    let payload_str: String = row.try_get("payload")?;
    let metadata_str: String = row.try_get("metadata")?;

    Ok(SerializedEvent {
        aggregate_type: row.try_get("aggregate_type")?,
        aggregate_id: row.try_get("aggregate_id")?,
        sequence,
        event_type: row.try_get("event_type")?,
        event_version: row.try_get("event_version")?,
        payload: serde_json::from_str(&payload_str)?,
        metadata: serde_json::from_str(&metadata_str)?,
    })
}

fn row_to_serialized_snapshot(row: &SqliteRow) -> Result<SerializedSnapshot, SqliteAggregateError> {
    let last_sequence_i64: i64 = row.try_get("last_sequence")?;
    let current_sequence = usize::try_from(last_sequence_i64)?;

    let snapshot_version_i64: i64 = row.try_get("snapshot_version")?;
    let current_snapshot = usize::try_from(snapshot_version_i64)?;

    let payload_str: String = row.try_get("payload")?;

    Ok(SerializedSnapshot {
        aggregate_id: row.try_get("aggregate_id")?,
        aggregate: serde_json::from_str(&payload_str)?,
        current_sequence,
        current_snapshot,
    })
}

/// Inserts serialized events in batched multi-row `INSERT` statements.
///
/// SQLite caps bind parameters at 999; with seven columns per row, each batch
/// carries at most 142 events in one round trip.
pub async fn insert_serialized_events_batch(
    connection: &mut SqliteConnection,
    events_table: &str,
    events: &[SerializedEvent],
) -> Result<(), SqliteAggregateError> {
    if events.is_empty() {
        return Ok(());
    }

    const BINDS_PER_ROW: usize = 7;
    const SQLITE_MAX_VARIABLE_NUMBER: usize = 999;
    const MAX_ROWS_PER_BATCH: usize = SQLITE_MAX_VARIABLE_NUMBER / BINDS_PER_ROW;

    let mut sequences = Vec::with_capacity(events.len().min(MAX_ROWS_PER_BATCH));
    for chunk in events.chunks(MAX_ROWS_PER_BATCH) {
        sequences.clear();
        for event in chunk {
            sequences.push(i64::try_from(event.sequence)?);
        }

        let mut query_builder = sqlx::QueryBuilder::new(format!(
            "INSERT INTO {events_table} \
             (aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata) "
        ));

        query_builder.push_values(
            chunk.iter().zip(&sequences),
            |mut row, (event, sequence)| {
                row.push_bind(&event.aggregate_type)
                    .push_bind(&event.aggregate_id)
                    .push_bind(*sequence)
                    .push_bind(&event.event_type)
                    .push_bind(&event.event_version)
                    .push_bind(&event.payload)
                    .push_bind(&event.metadata);
            },
        );

        let result = query_builder.build().execute(&mut *connection).await;

        if let Err(error) = result {
            if is_optimistic_lock_error(&error) {
                return Err(SqliteAggregateError::OptimisticLock);
            }
            return Err(SqliteAggregateError::Connection(error));
        }
    }

    Ok(())
}

fn is_optimistic_lock_error(err: &sqlx::Error) -> bool {
    match err {
        sqlx::Error::Database(db_err) => {
            db_err.is_unique_violation() || db_err.message().contains("UNIQUE constraint failed")
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use cqrs_es::event_sink::EventSink;
    use cqrs_es::persist::PersistedEventStore;
    use cqrs_es::{AggregateContext, CqrsFramework, DomainEvent, EventStore};
    use serde::{Deserialize, Serialize};
    use std::fmt::{self, Display};

    use super::*;
    use crate::testing::create_test_pool;

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
    struct TestAggregate {
        events: Vec<String>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    enum TestEvent {
        Created,
        Updated { value: String },
    }

    impl DomainEvent for TestEvent {
        fn event_type(&self) -> String {
            match self {
                Self::Created => "Created".to_string(),
                Self::Updated { .. } => "Updated".to_string(),
            }
        }

        fn event_version(&self) -> String {
            "1.0".to_string()
        }
    }

    #[derive(Debug)]
    struct TestError;

    impl Display for TestError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "test error")
        }
    }

    impl std::error::Error for TestError {}

    impl Aggregate for TestAggregate {
        const TYPE: &'static str = "TestAggregate";
        type Command = ();
        type Event = TestEvent;
        type Error = TestError;
        type Services = ();

        async fn handle(
            &mut self,
            _command: Self::Command,
            _services: &Self::Services,
            _sink: &EventSink<Self>,
        ) -> Result<(), Self::Error> {
            Ok(())
        }

        fn apply(&mut self, event: Self::Event) {
            self.events.push(event.event_type());
        }
    }

    /// Appends each event's value to a list, making double- or zero-
    /// application of any event visible in the final state.
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
    struct AppendingAggregate {
        applied: Vec<String>,
    }

    impl Aggregate for AppendingAggregate {
        const TYPE: &'static str = "AppendingAggregate";
        type Command = String;
        type Event = TestEvent;
        type Error = TestError;
        type Services = ();

        async fn handle(
            &mut self,
            command: Self::Command,
            _services: &Self::Services,
            sink: &EventSink<Self>,
        ) -> Result<(), Self::Error> {
            // Write through a scratch copy so `self` stays at its pre-command
            // state: `PersistedEventStore::commit` re-applies the sink's
            // events to the aggregate it received from `handle` when
            // rebuilding the snapshot, so mutating `self` here would fold
            // every event into the snapshot payload twice.
            let mut scratch = self.clone();
            sink.write(TestEvent::Updated { value: command }, &mut scratch)
                .await;
            Ok(())
        }

        fn apply(&mut self, event: Self::Event) {
            match event {
                TestEvent::Created => self.applied.push("created".to_string()),
                TestEvent::Updated { value } => self.applied.push(value),
            }
        }
    }

    #[tokio::test]
    async fn snapshot_round_trip_applies_events_exactly_once() {
        let pool = create_test_pool().await.unwrap();
        let repo = SqliteEventRepository::new(pool.clone());
        // Threshold 2 makes the event sequence diverge from the snapshot
        // version counter (snapshot lands at sequence 2, version 1), so a
        // regression to the old sequence/version conflation cannot pass by
        // numeric coincidence the way it would with threshold 1.
        let store = PersistedEventStore::<_, AppendingAggregate>::new_snapshot_store(repo, 2);
        let cqrs = CqrsFramework::new(store, vec![], ());

        cqrs.execute("agg-1", "a".to_string()).await.unwrap();
        cqrs.execute("agg-1", "b".to_string()).await.unwrap();
        cqrs.execute("agg-1", "c".to_string()).await.unwrap();

        // The snapshot row must record the sequence of the last folded event,
        // not the snapshot version counter.
        let repo = SqliteEventRepository::new(pool.clone());
        let snapshot = repo
            .load_snapshot::<AppendingAggregate>("agg-1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.current_sequence, 2);
        assert_eq!(snapshot.current_snapshot, 1);

        // Rehydrate through a fresh store: snapshot + trailing events must
        // apply each committed event exactly once.
        let repo = SqliteEventRepository::new(pool);
        let store = PersistedEventStore::<_, AppendingAggregate>::new_snapshot_store(repo, 2);
        let mut context = store.load_aggregate("agg-1").await.unwrap();

        assert_eq!(
            *context.aggregate(),
            AppendingAggregate {
                applied: vec!["a".to_string(), "b".to_string(), "c".to_string()],
            }
        );
    }

    #[tokio::test]
    async fn test_persist_and_load_events() {
        let pool = create_test_pool().await.unwrap();
        let repo = SqliteEventRepository::new(pool);

        let event = SerializedEvent {
            aggregate_type: TestAggregate::TYPE.to_string(),
            aggregate_id: "test-123".to_string(),
            sequence: 1,
            event_type: "TestEvent".to_string(),
            event_version: "1.0".to_string(),
            payload: serde_json::json!({"test": "data"}),
            metadata: serde_json::json!({}),
        };

        repo.insert_events(std::slice::from_ref(&event))
            .await
            .unwrap();

        let loaded = repo.load_events::<TestAggregate>("test-123").await.unwrap();

        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].aggregate_id, "test-123");
        assert_eq!(loaded[0].sequence, 1);
        assert_eq!(loaded[0].event_type, "TestEvent");
    }

    #[tokio::test]
    async fn test_optimistic_locking() {
        let pool = create_test_pool().await.unwrap();
        let repo = SqliteEventRepository::new(pool);

        let event = SerializedEvent {
            aggregate_type: TestAggregate::TYPE.to_string(),
            aggregate_id: "test-456".to_string(),
            sequence: 1,
            event_type: "TestEvent".to_string(),
            event_version: "1.0".to_string(),
            payload: serde_json::json!({}),
            metadata: serde_json::json!({}),
        };

        repo.insert_events(std::slice::from_ref(&event))
            .await
            .unwrap();

        let result = repo.insert_events(&[event]).await;

        assert!(matches!(result, Err(SqliteAggregateError::OptimisticLock)));
    }

    #[tokio::test]
    async fn test_snapshot_operations() {
        let pool = create_test_pool().await.unwrap();
        let repo = SqliteEventRepository::new(pool);

        repo.update_snapshot::<TestAggregate>(
            "test-789",
            5,
            1,
            serde_json::json!({"state": "first"}),
        )
        .await
        .unwrap();

        let aggregate = serde_json::json!({"state": "data"});
        repo.update_snapshot::<TestAggregate>("test-789", 9, 2, aggregate.clone())
            .await
            .unwrap();

        let loaded = repo
            .load_snapshot::<TestAggregate>("test-789")
            .await
            .unwrap()
            .unwrap();

        assert_eq!(loaded.aggregate_id, "test-789");
        assert_eq!(loaded.current_sequence, 9);
        assert_eq!(loaded.current_snapshot, 2);
        assert_eq!(loaded.aggregate, aggregate);
    }

    #[tokio::test]
    async fn incompatible_snapshot_is_returned_unchanged_and_load_rebuilds() {
        let pool = create_test_pool().await.unwrap();
        let repo = SqliteEventRepository::new(pool.clone());

        // sqlite-es never deletes events and cannot see a compaction policy, so
        // it does not discard an incompatible snapshot (ADR-0003): get_snapshot
        // returns the stored row unchanged, and cqrs-es then ignores the stale
        // snapshot and rebuilds from the always-complete event history.
        let events: Vec<SerializedEvent> = (1..=3)
            .map(|sequence| SerializedEvent {
                aggregate_type: TestAggregate::TYPE.to_string(),
                aggregate_id: "agg-incompatible".to_string(),
                sequence,
                event_type: "Created".to_string(),
                event_version: "1.0".to_string(),
                payload: serde_json::json!("Created"),
                metadata: serde_json::json!({}),
            })
            .collect();
        repo.insert_events(&events).await.unwrap();
        repo.update_snapshot::<TestAggregate>(
            "agg-incompatible",
            3,
            1,
            serde_json::json!({"events": "not-a-list"}),
        )
        .await
        .unwrap();

        // get_snapshot returns the incompatible row unchanged, not discarded.
        let snapshot = repo
            .get_snapshot::<TestAggregate>("agg-incompatible")
            .await
            .unwrap()
            .expect("snapshot returned unchanged, not discarded");
        assert_eq!(snapshot.current_sequence, 3);
        assert_eq!(
            snapshot.aggregate,
            serde_json::json!({"events": "not-a-list"})
        );

        // Loading through the framework rebuilds correct state from events.
        let store = PersistedEventStore::<_, TestAggregate>::new_snapshot_store(
            SqliteEventRepository::new(pool.clone()),
            100,
        );
        let mut context = store.load_aggregate("agg-incompatible").await.unwrap();
        assert_eq!(
            *context.aggregate(),
            TestAggregate {
                events: vec![
                    "Created".to_string(),
                    "Created".to_string(),
                    "Created".to_string(),
                ],
            }
        );
    }

    #[tokio::test]
    async fn stale_snapshot_writer_fails_optimistic_lock() {
        let pool = create_test_pool().await.unwrap();
        let repo = SqliteEventRepository::new(pool);

        repo.update_snapshot::<TestAggregate>("test-cas", 2, 1, serde_json::json!({"v": 1}))
            .await
            .unwrap();
        repo.update_snapshot::<TestAggregate>("test-cas", 4, 2, serde_json::json!({"v": 2}))
            .await
            .unwrap();

        // A delayed writer whose snapshot covers an older event sequence
        // must not clobber the newer row already written.
        let stale_update = repo
            .update_snapshot::<TestAggregate>("test-cas", 2, 2, serde_json::json!({"v": "stale"}))
            .await;
        assert!(matches!(
            stale_update,
            Err(SqliteAggregateError::OptimisticLock)
        ));

        // A delayed first-snapshot writer loses the same way.
        let duplicate_insert = repo
            .update_snapshot::<TestAggregate>("test-cas", 2, 1, serde_json::json!({"v": "dup"}))
            .await;
        assert!(matches!(
            duplicate_insert,
            Err(SqliteAggregateError::OptimisticLock)
        ));

        let loaded = repo
            .load_snapshot::<TestAggregate>("test-cas")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded.current_sequence, 4);
        assert_eq!(loaded.current_snapshot, 2);
        assert_eq!(loaded.aggregate, serde_json::json!({"v": 2}));
    }

    #[tokio::test]
    async fn snapshot_rebuild_after_corruption_replaces_stale_row() {
        let pool = create_test_pool().await.unwrap();
        let repo = SqliteEventRepository::new(pool);

        repo.update_snapshot::<TestAggregate>("test-heal", 4, 2, serde_json::json!({"v": 2}))
            .await
            .unwrap();

        // cqrs-es's snapshot-corruption recovery rebuilds from all events and
        // re-writes the snapshot at version 1 -- always at a strictly later
        // event sequence. The rebuild must replace the stale row.
        repo.update_snapshot::<TestAggregate>(
            "test-heal",
            6,
            1,
            serde_json::json!({"v": "rebuilt"}),
        )
        .await
        .unwrap();

        let loaded = repo
            .load_snapshot::<TestAggregate>("test-heal")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded.current_sequence, 6);
        assert_eq!(loaded.current_snapshot, 1);
        assert_eq!(loaded.aggregate, serde_json::json!({"v": "rebuilt"}));
    }

    #[tokio::test]
    async fn failed_snapshot_guard_rolls_back_events() {
        let pool = create_test_pool().await.unwrap();
        let repo = SqliteEventRepository::new(pool);

        repo.update_snapshot::<TestAggregate>("test-atomic", 10, 1, serde_json::json!({"v": 1}))
            .await
            .unwrap();

        // A commit whose snapshot write loses the monotonicity guard must
        // not leave its events behind, otherwise the caller retries a
        // command that already half-persisted.
        let event = SerializedEvent {
            aggregate_type: TestAggregate::TYPE.to_string(),
            aggregate_id: "test-atomic".to_string(),
            sequence: 1,
            event_type: "TestEvent".to_string(),
            event_version: "1.0".to_string(),
            payload: serde_json::json!({}),
            metadata: serde_json::json!({}),
        };

        let error = repo
            .persist::<TestAggregate>(
                std::slice::from_ref(&event),
                Some(("test-atomic".to_string(), serde_json::json!({}), 2)),
            )
            .await
            .unwrap_err();
        assert!(matches!(error, PersistenceError::OptimisticLockError));

        let events = repo
            .load_events::<TestAggregate>("test-atomic")
            .await
            .unwrap();
        assert_eq!(events.len(), 0);
    }

    #[tokio::test]
    async fn persist_rejects_snapshot_update_without_events() {
        let pool = create_test_pool().await.unwrap();
        let repo = SqliteEventRepository::new(pool);

        let error = repo
            .persist::<TestAggregate>(
                &[],
                Some(("test-empty".to_string(), serde_json::json!({}), 1)),
            )
            .await
            .unwrap_err();

        let PersistenceError::UnknownError(inner) = error else {
            panic!("expected UnknownError, got {error:?}");
        };
        assert!(matches!(
            inner.downcast_ref::<SqliteAggregateError>(),
            Some(SqliteAggregateError::EmptySnapshotUpdate)
        ));

        let snapshot = repo
            .load_snapshot::<TestAggregate>("test-empty")
            .await
            .unwrap();
        assert_eq!(snapshot, None);
    }

    #[tokio::test]
    async fn test_load_events_since() {
        let pool = create_test_pool().await.unwrap();
        let repo = SqliteEventRepository::new(pool);

        let events = vec![
            SerializedEvent {
                aggregate_type: TestAggregate::TYPE.to_string(),
                aggregate_id: "test-abc".to_string(),
                sequence: 1,
                event_type: "Event1".to_string(),
                event_version: "1.0".to_string(),
                payload: serde_json::json!({}),
                metadata: serde_json::json!({}),
            },
            SerializedEvent {
                aggregate_type: TestAggregate::TYPE.to_string(),
                aggregate_id: "test-abc".to_string(),
                sequence: 2,
                event_type: "Event2".to_string(),
                event_version: "1.0".to_string(),
                payload: serde_json::json!({}),
                metadata: serde_json::json!({}),
            },
            SerializedEvent {
                aggregate_type: TestAggregate::TYPE.to_string(),
                aggregate_id: "test-abc".to_string(),
                sequence: 3,
                event_type: "Event3".to_string(),
                event_version: "1.0".to_string(),
                payload: serde_json::json!({}),
                metadata: serde_json::json!({}),
            },
        ];

        repo.insert_events(&events).await.unwrap();

        let loaded = repo
            .load_events_since::<TestAggregate>("test-abc", 1)
            .await
            .unwrap();

        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].sequence, 2);
        assert_eq!(loaded[1].sequence, 3);
    }
}
