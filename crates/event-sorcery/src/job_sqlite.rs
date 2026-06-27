//! SQLite implementation of [`JobStore`] -- the default durable-job backend.
//!
//! Every method is the event-sourced job backend's original SQLite SQL, with the
//! backend-neutral types of [`crate::job_store`] mapped at the boundary. See
//! [ADR-0005](../../adrs/0005-backend-agnostic-event-store.md).

use std::ops::{Deref, DerefMut};

use cqrs_es::persist::SerializedEvent;
use sqlx::pool::PoolConnection;
use sqlx::{Row, Sqlite, SqliteConnection, SqlitePool};

use sqlite_es::{SqliteAggregateError, insert_serialized_events_batch};

use crate::job_store::{
    Candidate, CasOutcome, JobRow, JobStore, LeaseRenewal, QueueRow, QueueStatus, Severity,
};

/// The default [`JobStore`]: durable jobs over SQLite.
///
/// Holds only a [`SqlitePool`], which MUST be configured with `journal_mode=WAL`,
/// `busy_timeout>=5000ms`, and `synchronous=FULL` so the compare-and-swap claim
/// holds (a zero busy-timeout makes the CAS loser see `SQLITE_BUSY` before the
/// uniqueness check, defeating the conflict contract).
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
}

/// Owned claim-transaction guard: a pooled connection carrying a manual
/// `BEGIN IMMEDIATE`. Not a `sqlx::Transaction`, because `Pool::begin` issues a
/// deferred `BEGIN`, not the write-locking `IMMEDIATE` the claim CAS needs.
pub struct SqliteTx(PoolConnection<Sqlite>);

impl Deref for SqliteTx {
    type Target = SqliteConnection;

    fn deref(&self) -> &SqliteConnection {
        &self.0
    }
}

impl DerefMut for SqliteTx {
    fn deref_mut(&mut self) -> &mut SqliteConnection {
        &mut self.0
    }
}

/// Backend-native error for the SQLite job store. Distinct from a CAS conflict,
/// which rides [`CasOutcome::Conflict`].
#[derive(Debug, thiserror::Error)]
pub enum SqliteJobError {
    /// A direct SQL statement failed.
    #[error("job store SQL error")]
    Sql(#[from] sqlx::Error),
    /// A compare-and-swap event append failed (other than a conflict).
    #[error("job event append failed")]
    Append(#[from] SqliteAggregateError),
    /// A sequence/attempt value exceeded the storable range.
    #[error("job sequence out of range")]
    Sequence(#[from] std::num::TryFromIntError),
    /// A stored job payload could not be parsed as JSON.
    #[error("job payload is not valid JSON")]
    Codec(#[from] serde_json::Error),
}

impl JobStore for SqliteBackend {
    type Connection = SqliteConnection;
    type Tx = SqliteTx;
    type Conn = PoolConnection<Sqlite>;
    type Error = SqliteJobError;

    async fn begin_claim(&self) -> Result<SqliteTx, SqliteJobError> {
        let mut connection = self.pool.acquire().await?;
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut *connection)
            .await?;
        Ok(SqliteTx(connection))
    }

    async fn commit(&self, mut tx: SqliteTx) -> Result<(), SqliteJobError> {
        sqlx::query("COMMIT").execute(&mut *tx).await?;
        Ok(())
    }

    async fn rollback(&self, mut tx: SqliteTx) {
        if let Err(error) = sqlx::query("ROLLBACK").execute(&mut *tx).await {
            tracing::warn!(target: "cqrs", ?error, "job claim rollback failed");
        }
    }

    async fn acquire(&self) -> Result<PoolConnection<Sqlite>, SqliteJobError> {
        Ok(self.pool.acquire().await?)
    }

    async fn append_event(
        &self,
        connection: &mut SqliteConnection,
        event: &SerializedEvent,
    ) -> Result<CasOutcome, SqliteJobError> {
        match insert_serialized_events_batch(connection, "events", std::slice::from_ref(event))
            .await
        {
            Ok(()) => Ok(CasOutcome::Committed),
            Err(SqliteAggregateError::OptimisticLock) => Ok(CasOutcome::Conflict),
            Err(other) => Err(SqliteJobError::Append(other)),
        }
    }

    async fn read_head(
        &self,
        connection: &mut SqliteConnection,
        job_id: &str,
    ) -> Result<Option<QueueRow>, SqliteJobError> {
        let row = sqlx::query(
            "SELECT kind, status, run_at, lease_until, attempt, sequence \
             FROM job_queue WHERE job_id = ?1",
        )
        .bind(job_id)
        .fetch_optional(&mut *connection)
        .await?;

        let Some(row) = row else { return Ok(None) };

        let status: String = row.try_get("status")?;
        let status = match status.as_str() {
            "pending" => QueueStatus::Pending,
            "claimed" => QueueStatus::Claimed,
            other => {
                tracing::warn!(target: "cqrs", job_id, status = other, "job_queue row has an unexpected status; treating as not runnable");
                return Ok(None);
            }
        };

        let attempt: i64 = row.try_get("attempt")?;
        Ok(Some(QueueRow {
            kind: row.try_get("kind")?,
            status,
            run_at_ms: row.try_get("run_at")?,
            lease_until_ms: row.try_get("lease_until")?,
            attempt: u32::try_from(attempt)?,
            sequence: row.try_get("sequence")?,
        }))
    }

    async fn upsert_row(
        &self,
        connection: &mut SqliteConnection,
        job_id: &str,
        row: &JobRow,
    ) -> Result<(), SqliteJobError> {
        let status = match row.status {
            QueueStatus::Pending => "pending",
            QueueStatus::Claimed => "claimed",
        };
        sqlx::query(
            "INSERT INTO job_queue \
               (job_id, kind, status, run_at, lease_until, attempt, sequence) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7) \
             ON CONFLICT(job_id) DO UPDATE SET \
               status = excluded.status, run_at = excluded.run_at, \
               lease_until = excluded.lease_until, attempt = excluded.attempt, \
               sequence = excluded.sequence",
        )
        .bind(job_id)
        .bind(&row.kind)
        .bind(status)
        .bind(row.run_at_ms)
        .bind(row.lease_until_ms)
        .bind(i64::from(row.attempt))
        .bind(row.sequence)
        .execute(&mut *connection)
        .await?;
        Ok(())
    }

    async fn delete_row(
        &self,
        connection: &mut SqliteConnection,
        job_id: &str,
    ) -> Result<(), SqliteJobError> {
        sqlx::query("DELETE FROM job_queue WHERE job_id = ?1")
            .bind(job_id)
            .execute(&mut *connection)
            .await?;
        Ok(())
    }

    async fn fetch_candidates(
        &self,
        kind: &str,
        now_ms: i64,
        scan_limit: i64,
    ) -> Result<Vec<Candidate>, SqliteJobError> {
        let rows = sqlx::query(
            "SELECT job_id, sequence FROM job_queue \
             WHERE kind = ?1 \
               AND ( (status = 'pending' AND run_at <= ?2) \
                  OR (status = 'claimed' AND lease_until < ?2) ) \
             ORDER BY run_at ASC LIMIT ?3",
        )
        .bind(kind)
        .bind(now_ms)
        .bind(scan_limit)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok(Candidate {
                    job_id: row.try_get("job_id")?,
                    sequence: row.try_get("sequence")?,
                })
            })
            .collect()
    }

    async fn renew_lease(
        &self,
        job_id: &str,
        claim_seq: i64,
        new_lease_until_ms: i64,
    ) -> Result<LeaseRenewal, SqliteJobError> {
        let done = sqlx::query(
            "UPDATE job_queue SET lease_until = ?1 \
             WHERE job_id = ?2 AND sequence = ?3 AND status = 'claimed'",
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

    async fn load_enqueued_event(&self, job_id: &str) -> Result<serde_json::Value, SqliteJobError> {
        let payload: String = sqlx::query_scalar(
            "SELECT payload FROM events \
             WHERE aggregate_type = 'job' AND aggregate_id = ?1 AND sequence = 1",
        )
        .bind(job_id)
        .fetch_one(&self.pool)
        .await?;
        Ok(serde_json::from_str(&payload)?)
    }

    fn classify(error: &SqliteJobError) -> Severity {
        match error {
            SqliteJobError::Sql(_)
            | SqliteJobError::Append(SqliteAggregateError::Connection(_)) => Severity::Transient,
            SqliteJobError::Append(_) | SqliteJobError::Sequence(_) | SqliteJobError::Codec(_) => {
                Severity::Fatal
            }
        }
    }
}
