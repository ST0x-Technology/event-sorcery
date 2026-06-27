//! SQLite implementation of [`EventBackend`] -- the default durable-job backend.
//!
//! Two job-shaped primitives: the [`claim`](EventBackend::claim) transaction
//! (`BEGIN IMMEDIATE`, re-read the row, enact the crate's decision via the events
//! UNIQUE compare-and-swap, write the projection row) and the projection-only
//! [`renew`](EventBackend::renew). Everything else is cqrs-es. See
//! [ADR-0006](../../adrs/0006-cqrs-native-durable-jobs.md).

use cqrs_es::persist::SerializedEvent;
use sqlx::{Row, SqliteConnection, SqlitePool};

use sqlite_es::{SqliteAggregateError, insert_serialized_events_batch};

use crate::CompactionPolicy;
use crate::job_store::{ClaimDecision, ClaimOutcome, ClaimRead, EventBackend, LeaseRenewal};
use crate::sqlite_event_repository::SqliteEventRepository;

/// The default [`EventBackend`]: durable jobs over SQLite.
///
/// The pool MUST be configured with `journal_mode=WAL`, `busy_timeout>=5000ms`,
/// and `synchronous=FULL` so the compare-and-swap claim holds (a zero
/// busy-timeout makes the CAS loser see `SQLITE_BUSY` before the uniqueness
/// check, defeating the conflict contract).
#[derive(Clone)]
pub struct SqliteBackend {
    pool: SqlitePool,
}

impl SqliteBackend {
    /// Builds a SQLite job backend over `pool`.
    #[must_use]
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// The underlying pool, for the SQLite-bound view reconciliation paths.
    pub(crate) fn pool(&self) -> &SqlitePool {
        &self.pool
    }
}

/// Backend-native error for the SQLite job store.
#[derive(Debug, thiserror::Error)]
pub enum SqliteJobError {
    /// A direct SQL statement failed.
    #[error("job store SQL error")]
    Sql(#[from] sqlx::Error),
    /// A compare-and-swap event append failed (other than a conflict, which is
    /// reported as [`ClaimOutcome::Contended`]).
    #[error("job event append failed")]
    Append(#[from] SqliteAggregateError),
    /// A sequence value exceeded the storable range.
    #[error("job sequence out of range")]
    Sequence(#[from] std::num::TryFromIntError),
}

impl EventBackend for SqliteBackend {
    type EventRepo = SqliteEventRepository;
    type Error = SqliteJobError;

    fn event_repo(&self, compaction_policy: CompactionPolicy) -> SqliteEventRepository {
        SqliteEventRepository::new(self.pool.clone(), compaction_policy)
    }

    async fn migrate(&self) -> Result<(), SqliteJobError> {
        sqlx::migrate!("../../migrations")
            .run(&self.pool)
            .await
            .map_err(|error| SqliteJobError::Sql(error.into()))?;
        Ok(())
    }

    async fn claim<Decide, Won>(
        &self,
        job_id: &str,
        decide: Decide,
    ) -> Result<ClaimOutcome<Won>, SqliteJobError>
    where
        Decide: FnOnce(Option<ClaimRead>) -> ClaimDecision<Won> + Send,
        Won: Send,
    {
        let mut connection = self.pool.acquire().await?;

        // BEGIN IMMEDIATE takes the write lock up front, so the row we re-read is
        // the row we write -- and the events UNIQUE is the sole claim arbiter.
        if let Err(error) = sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut *connection)
            .await
        {
            return Err(SqliteJobError::Sql(error));
        }

        // `?`-free between BEGIN and the closer: every path runs COMMIT/ROLLBACK.
        let outcome = claim_in_txn(&mut connection, job_id, decide).await;
        let commit = matches!(outcome, Ok(ClaimOutcome::Won(_) | ClaimOutcome::Abandoned));
        let closer = if commit { "COMMIT" } else { "ROLLBACK" };
        match sqlx::query(closer).execute(&mut *connection).await {
            // A failed COMMIT means the claim did not durably happen.
            Err(error) if commit => Err(SqliteJobError::Sql(error)),
            Err(error) => {
                tracing::warn!(target: "cqrs", ?error, job_id, "job claim rollback failed");
                outcome
            }
            Ok(_) => outcome,
        }
    }

    async fn renew(
        &self,
        job_id: &str,
        claim_seq: i64,
        new_lease_until_ms: i64,
    ) -> Result<LeaseRenewal, SqliteJobError> {
        let done = sqlx::query(
            "UPDATE job_queue SET lease_until = ?1 \
             WHERE view_id = ?2 AND version = ?3 AND status = 'claimed'",
        )
        .bind(new_lease_until_ms)
        .bind(job_id)
        .bind(claim_seq)
        .execute(&self.pool)
        .await?;

        if done.rows_affected() == 0 {
            Ok(LeaseRenewal::Lost)
        } else {
            Ok(LeaseRenewal::Held)
        }
    }
}

async fn claim_in_txn<Decide, Won>(
    connection: &mut SqliteConnection,
    job_id: &str,
    decide: Decide,
) -> Result<ClaimOutcome<Won>, SqliteJobError>
where
    Decide: FnOnce(Option<ClaimRead>) -> ClaimDecision<Won>,
{
    let read = read_claim_row(connection, job_id).await?;
    match decide(read) {
        ClaimDecision::Skip => Ok(ClaimOutcome::Skip),
        ClaimDecision::Claim {
            event,
            payload,
            lease_until_ms,
            won,
        } => match append(connection, &event).await? {
            Some(version) => {
                write_row(connection, job_id, version, &payload, Some(lease_until_ms)).await?;
                Ok(ClaimOutcome::Won(won))
            }
            None => Ok(ClaimOutcome::Contended),
        },
        ClaimDecision::Abandon { event, payload } => match append(connection, &event).await? {
            Some(version) => {
                // A dead-lettered job is terminal: clear the lease.
                write_row(connection, job_id, version, &payload, None).await?;
                Ok(ClaimOutcome::Abandoned)
            }
            None => Ok(ClaimOutcome::Contended),
        },
    }
}

/// Compare-and-swap-appends one event. `Ok(Some(version))` committed at that
/// sequence; `Ok(None)` lost the CAS (the events UNIQUE rejected the sequence).
async fn append(
    connection: &mut SqliteConnection,
    event: &SerializedEvent,
) -> Result<Option<i64>, SqliteJobError> {
    match insert_serialized_events_batch(connection, "events", std::slice::from_ref(event)).await {
        Ok(()) => Ok(Some(i64::try_from(event.sequence)?)),
        Err(SqliteAggregateError::OptimisticLock) => Ok(None),
        Err(other) => Err(SqliteJobError::Append(other)),
    }
}

async fn read_claim_row(
    connection: &mut SqliteConnection,
    job_id: &str,
) -> Result<Option<ClaimRead>, SqliteJobError> {
    let row = sqlx::query("SELECT version, payload, lease_until FROM job_queue WHERE view_id = ?1")
        .bind(job_id)
        .fetch_optional(connection)
        .await?;

    let Some(row) = row else { return Ok(None) };
    Ok(Some(ClaimRead {
        version: row.try_get("version")?,
        payload: row.try_get("payload")?,
        lease_until_ms: row.try_get("lease_until")?,
    }))
}

/// Writes the projection row at `version`; the generated `kind`/`status`/`run_at`
/// columns derive from `payload`. `lease_until` is the projection-only column.
async fn write_row(
    connection: &mut SqliteConnection,
    job_id: &str,
    version: i64,
    payload: &str,
    lease_until_ms: Option<i64>,
) -> Result<(), SqliteJobError> {
    sqlx::query(
        "INSERT INTO job_queue (view_id, version, payload, lease_until) VALUES (?1, ?2, ?3, ?4) \
         ON CONFLICT(view_id) DO UPDATE SET \
           version = excluded.version, payload = excluded.payload, lease_until = excluded.lease_until",
    )
    .bind(job_id)
    .bind(version)
    .bind(payload)
    .bind(lease_until_ms)
    .execute(connection)
    .await?;
    Ok(())
}
