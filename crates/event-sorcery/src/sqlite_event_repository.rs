use cqrs_es::Aggregate;
use cqrs_es::persist::{
    PersistedEventRepository, PersistenceError, ReplayStream, SerializedEvent, SerializedSnapshot,
};
use serde_json::Value;
use sqlx::sqlite::SqliteRow;
use sqlx::{Row, SqlitePool};
use tracing::warn;

use crate::CompactionPolicy;

#[derive(Debug, thiserror::Error)]
enum SqliteEventRepositoryError {
    #[error("optimistic lock error: aggregate has been modified concurrently")]
    OptimisticLock,
    #[error("snapshot update was requested without persisted events")]
    EmptySnapshotUpdate,
    #[error(transparent)]
    Sql(#[from] sqlx::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Integer(#[from] std::num::TryFromIntError),
    #[error(transparent)]
    JobFlush(#[from] crate::job::JobStoreError),
}

impl From<SqliteEventRepositoryError> for PersistenceError {
    fn from(error: SqliteEventRepositoryError) -> Self {
        use SqliteEventRepositoryError::*;
        match error {
            OptimisticLock => Self::OptimisticLockError,
            EmptySnapshotUpdate => Self::UnknownError(Box::new(error)),
            Sql(source) => Self::ConnectionError(Box::new(source)),
            Json(source) => Self::DeserializationError(Box::new(source)),
            Integer(source) => Self::UnknownError(Box::new(source)),
            JobFlush(source) => Self::UnknownError(Box::new(source)),
        }
    }
}

/// SQLite implementation of the cqrs-es [`PersistedEventRepository`].
///
/// Public so it can be the [`crate::EventBackend::EventRepo`] of
/// [`crate::SqliteBackend`]; consumers obtain it via the backend, not directly.
pub struct SqliteEventRepository {
    pool: SqlitePool,
    compaction_policy: CompactionPolicy,
    stream_channel_size: usize,
}

impl SqliteEventRepository {
    pub(crate) fn new(pool: SqlitePool, compaction_policy: CompactionPolicy) -> Self {
        Self {
            pool,
            compaction_policy,
            stream_channel_size: 1000,
        }
    }

    async fn load_events<A: Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> Result<Vec<SerializedEvent>, SqliteEventRepositoryError> {
        let rows = sqlx::query(
            "SELECT aggregate_type, aggregate_id, sequence, event_type, \
             event_version, payload, metadata \
             FROM events \
             WHERE aggregate_type = ?1 AND aggregate_id = ?2 \
             ORDER BY sequence",
        )
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
    ) -> Result<Vec<SerializedEvent>, SqliteEventRepositoryError> {
        let last_sequence = i64::try_from(last_sequence)?;
        let rows = sqlx::query(
            "SELECT aggregate_type, aggregate_id, sequence, event_type, \
             event_version, payload, metadata \
             FROM events \
             WHERE aggregate_type = ?1 AND aggregate_id = ?2 AND sequence > ?3 \
             ORDER BY sequence",
        )
        .bind(A::TYPE)
        .bind(aggregate_id)
        .bind(last_sequence)
        .fetch_all(&self.pool)
        .await?;

        rows.iter().map(row_to_serialized_event).collect()
    }

    async fn load_snapshot<A: Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> Result<Option<SerializedSnapshot>, SqliteEventRepositoryError> {
        let row = sqlx::query(
            "SELECT aggregate_type, aggregate_id, last_sequence, snapshot_version, payload, timestamp \
             FROM snapshots \
             WHERE aggregate_type = ?1 AND aggregate_id = ?2",
        )
        .bind(A::TYPE)
        .bind(aggregate_id)
        .fetch_optional(&self.pool)
        .await?;

        row.as_ref().map(row_to_serialized_snapshot).transpose()
    }

    async fn persist_events<A: Aggregate>(
        &self,
        events: &[SerializedEvent],
        snapshot_update: Option<(String, Value, usize)>,
    ) -> Result<(), SqliteEventRepositoryError> {
        let mut tx = self.pool.begin().await?;

        sqlite_es::insert_serialized_events_batch(&mut tx, "events", events)
            .await
            .map_err(map_sqlite_aggregate_error)?;

        if let Some((aggregate_id, aggregate, snapshot_version)) = snapshot_update {
            let last_sequence = events
                .last()
                .map(|event| event.sequence)
                .ok_or(SqliteEventRepositoryError::EmptySnapshotUpdate)?;
            let last_sequence = i64::try_from(last_sequence)?;
            let snapshot_version = i64::try_from(snapshot_version)?;

            sqlx::query(
                "INSERT OR REPLACE INTO snapshots \
                 (aggregate_type, aggregate_id, last_sequence, snapshot_version, payload, timestamp) \
                 VALUES (?1, ?2, ?3, ?4, ?5, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))",
            )
            .bind(A::TYPE)
            .bind(aggregate_id)
            .bind(last_sequence)
            .bind(snapshot_version)
            .bind(aggregate)
            .execute(&mut *tx)
            .await?;
        }

        // Drain any jobs the command buffered: append each as an Enqueued event
        // AND seed its job_queue projection row (version 1), both in this
        // transaction, so a job becomes durable and pollable iff the triggering
        // events commit.
        for request in crate::job::take_pending()? {
            let event = crate::job::enqueued_event(&request)?;
            sqlite_es::insert_serialized_events_batch(
                &mut tx,
                "events",
                std::slice::from_ref(&event),
            )
            .await
            .map_err(map_sqlite_aggregate_error)?;

            let payload = crate::job::pending_seed_payload(&request)?;
            sqlx::query(
                "INSERT INTO job_queue (view_id, version, payload, lease_until) \
                 VALUES (?1, 1, ?2, NULL)",
            )
            .bind(&request.job_id)
            .bind(&payload)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    /// Stream events from the `events` table for replay.
    ///
    /// **Compaction caveat:** This only queries the `events` table.
    /// Fully-compacted aggregates (all events deleted, only a
    /// snapshot remains) will not appear in the stream.
    /// [`load_aggregate`](cqrs_es::persist::PersistedEventStore::load_aggregate)
    /// handles this correctly via snapshot loading, but
    /// stream-based replay will miss snapshot-only entities.
    fn stream_events_impl<A: Aggregate>(&self, aggregate_id: Option<&str>) -> ReplayStream {
        let (mut feed, stream) = ReplayStream::new(self.stream_channel_size);
        let pool = self.pool.clone();
        let aggregate_type = A::TYPE;
        let aggregate_id = aggregate_id.map(String::from);

        tokio::spawn(async move {
            let rows = match &aggregate_id {
                Some(aggregate_id) => {
                    sqlx::query(
                        "SELECT aggregate_type, aggregate_id, sequence, event_type, \
                         event_version, payload, metadata \
                         FROM events \
                         WHERE aggregate_type = ?1 AND aggregate_id = ?2 \
                         ORDER BY sequence",
                    )
                    .bind(aggregate_type)
                    .bind(aggregate_id)
                    .fetch_all(&pool)
                    .await
                }
                None => {
                    sqlx::query(
                        "SELECT aggregate_type, aggregate_id, sequence, event_type, \
                         event_version, payload, metadata \
                         FROM events \
                         WHERE aggregate_type = ?1 \
                         ORDER BY sequence",
                    )
                    .bind(aggregate_type)
                    .fetch_all(&pool)
                    .await
                }
            };

            let rows = match rows {
                Ok(rows) => rows,
                Err(error) => {
                    let _ = feed
                        .push(Err(PersistenceError::ConnectionError(Box::new(error))))
                        .await;
                    return;
                }
            };

            for row in &rows {
                let event = match row_to_serialized_event(row) {
                    Ok(event) => event,
                    Err(error) => {
                        let _ = feed.push(Err(PersistenceError::from(error))).await;
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
        let snapshot = self.load_snapshot::<A>(aggregate_id).await?;
        let Some(snapshot) = snapshot else {
            return Ok(None);
        };

        // Whether an incompatible snapshot must be guarded depends on the
        // entity's compaction policy (ADR-0003).
        match self.compaction_policy {
            // A `Retain` aggregate keeps its full event history, so cqrs-es
            // safely rebuilds from events on a deserialize failure. The snapshot
            // is returned as stored -- no shape check is needed here, and doing
            // one would only duplicate the deserialize cqrs-es already performs.
            CompactionPolicy::Retain => Ok(Some(snapshot)),
            // A `CompactAfterSnapshot` aggregate may have lost the events behind
            // the snapshot, making the snapshot the only durable record of
            // state. An incompatible payload must surface an error rather than
            // letting cqrs-es silently rebuild from an incomplete history.
            CompactionPolicy::CompactAfterSnapshot => match A::deserialize(&snapshot.aggregate) {
                Ok(_) => Ok(Some(snapshot)),
                Err(source) => {
                    warn!(
                        target: "cqrs",
                        aggregate_type = A::TYPE,
                        aggregate_id,
                        %source,
                        "Incompatible snapshot for a compactable aggregate cannot be \
                         safely rebuilt from events; surfacing error"
                    );
                    Err(SqliteEventRepositoryError::Json(source).into())
                }
            },
        }
    }

    async fn persist<A: Aggregate>(
        &self,
        events: &[SerializedEvent],
        snapshot_update: Option<(String, Value, usize)>,
    ) -> Result<(), PersistenceError> {
        Ok(self.persist_events::<A>(events, snapshot_update).await?)
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

fn row_to_serialized_event(row: &SqliteRow) -> Result<SerializedEvent, SqliteEventRepositoryError> {
    let sequence: i64 = row.try_get("sequence")?;
    let payload: String = row.try_get("payload")?;
    let metadata: String = row.try_get("metadata")?;

    Ok(SerializedEvent {
        aggregate_type: row.try_get("aggregate_type")?,
        aggregate_id: row.try_get("aggregate_id")?,
        sequence: usize::try_from(sequence)?,
        event_type: row.try_get("event_type")?,
        event_version: row.try_get("event_version")?,
        payload: serde_json::from_str(&payload)?,
        metadata: serde_json::from_str(&metadata)?,
    })
}

fn row_to_serialized_snapshot(
    row: &SqliteRow,
) -> Result<SerializedSnapshot, SqliteEventRepositoryError> {
    let current_sequence: i64 = row.try_get("last_sequence")?;
    let current_snapshot: i64 = row.try_get("snapshot_version")?;
    let payload: String = row.try_get("payload")?;

    Ok(SerializedSnapshot {
        aggregate_id: row.try_get("aggregate_id")?,
        aggregate: serde_json::from_str(&payload)?,
        current_sequence: usize::try_from(current_sequence)?,
        current_snapshot: usize::try_from(current_snapshot)?,
    })
}

fn map_sqlite_aggregate_error(
    error: sqlite_es::SqliteAggregateError,
) -> SqliteEventRepositoryError {
    match error {
        sqlite_es::SqliteAggregateError::OptimisticLock => {
            SqliteEventRepositoryError::OptimisticLock
        }
        sqlite_es::SqliteAggregateError::Connection(source) => {
            SqliteEventRepositoryError::Sql(source)
        }
        sqlite_es::SqliteAggregateError::Deserialization(source) => {
            SqliteEventRepositoryError::Json(source)
        }
        sqlite_es::SqliteAggregateError::TryFromInt(source) => {
            SqliteEventRepositoryError::Integer(source)
        }
        sqlite_es::SqliteAggregateError::EmptySnapshotUpdate => {
            SqliteEventRepositoryError::EmptySnapshotUpdate
        }
    }
}

#[cfg(test)]
mod tests {
    use cqrs_es::event_sink::EventSink;
    use cqrs_es::persist::PersistedEventStore;
    use cqrs_es::{AggregateContext, AggregateError, DomainEvent, EventStore};
    use serde::{Deserialize, Serialize};
    use std::fmt::{self, Display};

    use super::*;

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
    struct TestAggregate {
        events: Vec<String>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    enum TestEvent {
        Created,
    }

    impl DomainEvent for TestEvent {
        fn event_type(&self) -> String {
            "Created".to_string()
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

    async fn test_pool() -> SqlitePool {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!("../../migrations").run(&pool).await.unwrap();
        pool
    }

    fn covering_events(aggregate_id: &str, through: usize) -> Vec<SerializedEvent> {
        (1..=through)
            .map(|sequence| SerializedEvent {
                aggregate_type: TestAggregate::TYPE.to_string(),
                aggregate_id: aggregate_id.to_string(),
                sequence,
                event_type: "Created".to_string(),
                event_version: "1.0".to_string(),
                payload: serde_json::json!("Created"),
                metadata: serde_json::json!({}),
            })
            .collect()
    }

    /// For a `Retain` entity, get_snapshot returns the incompatible snapshot
    /// unchanged (no shape check, no discard); cqrs-es then rebuilds from the
    /// always-complete event history. See the end-to-end rebuild test below.
    #[tokio::test]
    async fn incompatible_snapshot_returned_unchanged_for_retain_entity() {
        let pool = test_pool().await;
        let repo = SqliteEventRepository::new(pool.clone(), CompactionPolicy::Retain);

        repo.persist::<TestAggregate>(
            &covering_events("agg-replayable", 3),
            Some((
                "agg-replayable".to_string(),
                serde_json::json!({"events": "not-a-list"}),
                1,
            )),
        )
        .await
        .unwrap();

        let snapshot = repo
            .get_snapshot::<TestAggregate>("agg-replayable")
            .await
            .unwrap()
            .expect("Retain returns the snapshot unchanged");
        assert_eq!(snapshot.current_sequence, 3);
        assert_eq!(
            snapshot.aggregate,
            serde_json::json!({"events": "not-a-list"})
        );
    }

    /// For a `CompactAfterSnapshot` entity, the events behind the snapshot may
    /// have been compacted away, so an incompatible snapshot is preserved and an
    /// error surfaced rather than silently rebuilding from an incomplete history.
    /// The policy alone decides this -- no inspection of the event rows.
    #[tokio::test]
    async fn incompatible_snapshot_preserved_for_compactable_entity() {
        let pool = test_pool().await;
        let repo = SqliteEventRepository::new(pool.clone(), CompactionPolicy::CompactAfterSnapshot);

        repo.persist::<TestAggregate>(
            &covering_events("agg-compacted", 3),
            Some((
                "agg-compacted".to_string(),
                serde_json::json!({"events": "not-a-list"}),
                1,
            )),
        )
        .await
        .unwrap();

        let result = repo.get_snapshot::<TestAggregate>("agg-compacted").await;
        assert!(matches!(
            result,
            Err(PersistenceError::DeserializationError(_))
        ));

        let remaining: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM snapshots WHERE aggregate_id = 'agg-compacted'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(remaining, 1);
    }

    /// End-to-end: loading a `Retain` aggregate whose snapshot is incompatible
    /// discards it and reconstructs correct state from the full event history.
    #[tokio::test]
    async fn load_aggregate_rebuilds_after_discard_for_retain_entity() {
        let pool = test_pool().await;
        let repo = SqliteEventRepository::new(pool.clone(), CompactionPolicy::Retain);

        repo.persist::<TestAggregate>(
            &covering_events("agg-load", 3),
            Some((
                "agg-load".to_string(),
                serde_json::json!({"events": "not-a-list"}),
                1,
            )),
        )
        .await
        .unwrap();

        // load_aggregate must discard the incompatible snapshot and replay the
        // events, reconstructing the correct state rather than Default.
        let store = PersistedEventStore::<_, TestAggregate>::new_snapshot_store(
            SqliteEventRepository::new(pool.clone(), CompactionPolicy::Retain),
            100,
        );
        let mut context = store.load_aggregate("agg-load").await.unwrap();
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

    /// End-to-end: loading a `CompactAfterSnapshot` aggregate whose snapshot is
    /// incompatible surfaces a `DeserializationError` through cqrs-es rather than
    /// silently rebuilding from a possibly-incomplete history. This pins the
    /// guard's reliance on cqrs-es propagating the `Err` returned by
    /// `get_snapshot` (instead of falling through to its rebuild-from-events
    /// recovery, which fires only on `Ok(Some(..))`/`Ok(None)`).
    #[tokio::test]
    async fn load_aggregate_errors_for_compactable_entity_with_incompatible_snapshot() {
        let pool = test_pool().await;
        let repo = SqliteEventRepository::new(pool.clone(), CompactionPolicy::CompactAfterSnapshot);

        repo.persist::<TestAggregate>(
            &covering_events("agg-compact-load", 3),
            Some((
                "agg-compact-load".to_string(),
                serde_json::json!({"events": "not-a-list"}),
                1,
            )),
        )
        .await
        .unwrap();

        let store = PersistedEventStore::<_, TestAggregate>::new_snapshot_store(
            SqliteEventRepository::new(pool.clone(), CompactionPolicy::CompactAfterSnapshot),
            100,
        );
        let result = store.load_aggregate("agg-compact-load").await;
        assert!(matches!(
            result,
            Err(AggregateError::DeserializationError(_))
        ));
    }
}
