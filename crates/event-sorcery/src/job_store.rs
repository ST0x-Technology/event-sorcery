//! Backend-agnostic durable-job storage.
//!
//! [`JobStore`] names the operations cqrs-es does not provide -- the `job_queue`
//! projection, a compare-and-swap event append, a write-locked claim
//! transaction, lease renewal, and the runnable poll. Each cqrs-es backend
//! implements it once; [`crate::SqliteBackend`] is the default. This mirrors the
//! [`crate::ViewBackend`] idiom (a backend trait whose concrete capabilities are
//! associated types) but needs no GAT: the job aggregate is mono-typed and a
//! pooled connection/transaction is owned and `'static`. See
//! [ADR-0005](../../adrs/0005-backend-agnostic-event-store.md).

use std::future::Future;
use std::ops::DerefMut;

use cqrs_es::persist::SerializedEvent;

/// Outcome of a compare-and-swap event append.
///
/// `Conflict` is the backend-neutral image of a uniqueness violation on
/// `events (aggregate_type, aggregate_id, sequence)` -- the arbiter of every
/// claim/ack race. It is an EXPECTED control-flow result on the `Ok` channel,
/// not an error: a stream `Err` would stop the worker, so contention must ride
/// `Ok` and force every call site to handle it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CasOutcome {
    /// The event was appended at the requested sequence.
    Committed,
    /// Another writer already holds that sequence.
    Conflict,
}

/// Backend-neutral severity of a store error, so the worker branches on a
/// classification instead of decoding driver codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    /// Busy/locked/dropped-connection/pool-timeout/serialization-failure.
    /// Claim: skip the candidate; ack: retry within lease; poll: idle.
    Transient,
    /// Will not succeed on retry. Skip and log; never treated as success.
    Fatal,
}

/// Result of the projection-only lease-renewal `UPDATE`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseRenewal {
    /// The update matched the claimed row: the lease is still held.
    Held,
    /// Zero rows matched: the ack already advanced/deleted the row, or a
    /// re-claimer stole it. Stop renewing.
    Lost,
}

/// Live status of a `job_queue` row. Terminal jobs are deleted, so the worker
/// never sees done/dead here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueStatus {
    /// Runnable at `run_at`.
    Pending,
    /// Held under a lease until `lease_until`.
    Claimed,
}

/// One runnable-job nomination from the poll: a stale snapshot, re-validated
/// inside the claim transaction.
pub struct Candidate {
    /// The job's id (its event-stream aggregate id).
    pub job_id: String,
    /// The job's last applied sequence, the expected-version for the claim CAS.
    pub sequence: i64,
}

/// The `job_queue` head re-read inside the claim transaction (the guard against
/// a stale poll snapshot). Times are Unix epoch milliseconds.
pub struct QueueRow {
    /// The job kind ([`crate::Job::KIND`]).
    pub kind: String,
    /// Whether the row is pending or claimed.
    pub status: QueueStatus,
    /// When a pending job becomes runnable.
    pub run_at_ms: i64,
    /// Lease expiry; `Some` iff `status == Claimed`.
    pub lease_until_ms: Option<i64>,
    /// Recorded-failure count.
    pub attempt: u32,
    /// Last applied event sequence.
    pub sequence: i64,
}

/// The active-job row to upsert (a pending or claimed projection row). The crate
/// folds the job state and builds this; the backend writes it in its dialect.
pub struct JobRow {
    /// The job kind.
    pub kind: String,
    /// Whether the row is pending or claimed.
    pub status: QueueStatus,
    /// When a pending job becomes runnable.
    pub run_at_ms: i64,
    /// Lease expiry; `Some` iff `status == Claimed`.
    pub lease_until_ms: Option<i64>,
    /// Recorded-failure count.
    pub attempt: u32,
    /// Last applied event sequence.
    pub sequence: i64,
}

/// Per-backend storage for the durable-job queue.
///
/// Every method returns `impl Future + Send` (RPITIT, **not** `async fn`): the
/// worker boxes the poll stream (`BoxStream<'static>`) and `tokio::spawn`s the
/// lease renewal, both of which require `Send` futures. This mirrors
/// [`cqrs_es::persist::PersistedEventRepository`] and is mandatory.
pub trait JobStore: Clone + Send + Sync + 'static {
    /// What SQL executes against (SQLite: `SqliteConnection`). No lifetime: that
    /// is the move that avoids a lifetime GAT.
    type Connection: Send;

    /// Owned, write-locked transaction guard, consumed by [`commit`](Self::commit)
    /// or [`rollback`](Self::rollback) on every path -- never dropped open (a
    /// manual `BEGIN IMMEDIATE` is not undone by drop). The generic claim path
    /// stays `?`-free between begin and the closer to honour this.
    type Tx: DerefMut<Target = Self::Connection> + Send;

    /// Owned, autocommit connection guard for the ack append + projection
    /// (returned to the pool on drop). Deref to reach the connection.
    type Conn: DerefMut<Target = Self::Connection> + Send;

    /// Backend-native error for everything that is not a CAS conflict.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Begin a write-locked transaction for a claim (SQLite: `BEGIN IMMEDIATE`).
    fn begin_claim(&self) -> impl Future<Output = Result<Self::Tx, Self::Error>> + Send;

    /// Commit a claim transaction.
    fn commit(&self, tx: Self::Tx) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Roll back a claim transaction (best-effort; errors are logged by the impl).
    fn rollback(&self, tx: Self::Tx) -> impl Future<Output = ()> + Send;

    /// Acquire a plain (autocommit) connection for the ack append + projection.
    fn acquire(&self) -> impl Future<Output = Result<Self::Conn, Self::Error>> + Send;

    /// Compare-and-swap append one serialized job event at `event.sequence`.
    fn append_event(
        &self,
        connection: &mut Self::Connection,
        event: &SerializedEvent,
    ) -> impl Future<Output = Result<CasOutcome, Self::Error>> + Send;

    /// Re-read the `job_queue` head inside the claim transaction. The read MUST
    /// serialize concurrent claimers of the same `job_id` (SQLite: the
    /// `BEGIN IMMEDIATE` write lock; Postgres: `SELECT ... FOR UPDATE`).
    fn read_head(
        &self,
        connection: &mut Self::Connection,
        job_id: &str,
    ) -> impl Future<Output = Result<Option<QueueRow>, Self::Error>> + Send;

    /// Upsert the active-job row (claim/retry/enqueue).
    fn upsert_row(
        &self,
        connection: &mut Self::Connection,
        job_id: &str,
        row: &JobRow,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Delete the row on a terminal (done/dead) outcome.
    fn delete_row(
        &self,
        connection: &mut Self::Connection,
        job_id: &str,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Poll runnable jobs (snapshot, no locks): pending-and-due or
    /// claimed-with-expired-lease of `kind`, oldest `run_at` first.
    fn fetch_candidates(
        &self,
        kind: &str,
        now_ms: i64,
        scan_limit: i64,
    ) -> impl Future<Output = Result<Vec<Candidate>, Self::Error>> + Send;

    /// Projection-only lease renewal keyed on the fixed claim sequence.
    fn renew_lease(
        &self,
        job_id: &str,
        claim_seq: i64,
        new_lease_until_ms: i64,
    ) -> impl Future<Output = Result<LeaseRenewal, Self::Error>> + Send;

    /// Read the raw `Enqueued` event (sequence 1) for `job_id` as a JSON value,
    /// so the TEXT-vs-jsonb storage difference stays inside the impl.
    fn load_enqueued_event(
        &self,
        job_id: &str,
    ) -> impl Future<Output = Result<serde_json::Value, Self::Error>> + Send;

    /// Map a backend error to a neutral [`Severity`].
    fn classify(error: &Self::Error) -> Severity;
}

/// apalis-facing worker error: a backend store error, or a job that cannot be
/// decoded into its job type.
#[derive(Debug, thiserror::Error)]
pub enum BackendError<E> {
    /// The job store backend failed.
    #[error("job store backend error")]
    Backend(#[source] E),
    /// A claimed job's stored event could not be decoded into the job type.
    #[error("failed to decode a claimed job")]
    Decode(#[source] serde_json::Error),
    /// A job event stream does not begin with an `Enqueued` event.
    #[error("job event stream does not begin with Enqueued")]
    OrphanEvent,
}
