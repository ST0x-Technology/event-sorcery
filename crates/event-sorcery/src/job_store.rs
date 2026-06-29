//! The backend abstraction for durable jobs: [`EventBackend`].
//!
//! A consumer supplies a single `EventBackend` -- a cqrs-es event repository plus
//! two engine-shaped primitives ([`claim`](EventBackend::claim) and
//! [`renew`](EventBackend::renew)) -- and gets the whole durable-jobs capability,
//! built by event-sorcery's own cqrs/es machinery (the job is an
//! [`crate::EventSourced`] aggregate, the ack is a command, the `job_queue` is a
//! projection). The backend never names a job type: the claim *decision* (which
//! event to append, the new projection payload, the strongly-typed claim result)
//! is a crate-side closure, and the result rides through the backend as an opaque
//! `Won` payload. The backend owns only the *transaction*. See
//! [ADR-0006](../../adrs/0006-cqrs-native-durable-jobs.md).

use std::future::Future;

use cqrs_es::persist::{PersistedEventRepository, SerializedEvent};

use crate::CompactionPolicy;

/// A complete event-sorcery backend.
///
/// Beyond the cqrs-es event repository, it exposes exactly two job-shaped
/// primitives that cqrs-es cannot: a write-locked compare-and-swap
/// [`claim`](Self::claim) (whose runnable check must read the projection-only
/// lease column, not a folded event lease), and a projection-only
/// [`renew`](Self::renew). Everything else -- the claim/ack CAS, the fence, the
/// retry, the dead-letter, the poll -- is cqrs-es `execute` + the generic
/// projection. Used only as a generic bound, never `dyn`.
pub trait EventBackend: Clone + Send + Sync + 'static {
    /// The cqrs-es event repository. Its `persist` flushes buffered `Enqueued`
    /// events AND seeds their `job_queue` rows in the same transaction that
    /// commits the triggering events (the atomic-enqueue guarantee).
    type EventRepo: PersistedEventRepository + Send + Sync + 'static;

    /// Backend-native error.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Builds the flush-aware event repository for `compaction_policy`.
    fn event_repo(&self, compaction_policy: CompactionPolicy) -> Self::EventRepo;

    /// Applies the canonical events + snapshots + `job_queue` schema.
    fn migrate(&self) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Claims one job in a single write-locked transaction: re-read the row, hand
    /// the RAW row to `decide`, and enact the decision (compare-and-swap-append
    /// the event via the events UNIQUE; write the row). `decide` is crate-side
    /// and owns all `JobState`/`JobEvent` knowledge; its strongly-typed success
    /// payload `Won` rides back through the backend untouched -- so the backend
    /// stays generic and names no job type.
    fn claim<Decide, Won>(
        &self,
        job_id: &str,
        decide: Decide,
    ) -> impl Future<Output = Result<ClaimOutcome<Won>, Self::Error>> + Send
    where
        Decide: FnOnce(Option<ClaimRead>) -> ClaimDecision<Won> + Send,
        Won: Send;

    /// Projection-only lease renewal: bump `lease_until` on the claimed row,
    /// keyed on its `version` (== the claim sequence), without touching
    /// `version`. Zero rows matched (the ack already advanced/changed the row, or
    /// a re-claimer took it) -> [`LeaseRenewal::Lost`].
    fn renew(
        &self,
        job_id: &str,
        claim_seq: i64,
        new_lease_until_ms: i64,
    ) -> impl Future<Output = Result<LeaseRenewal, Self::Error>> + Send;
}

/// The `job_queue` row as the claim transaction re-reads it, raw. The crate
/// deserializes `payload` into `Lifecycle<JobState>` itself.
pub struct ClaimRead {
    /// The last applied event sequence (the view version).
    pub version: i64,
    /// The serialized `Lifecycle<JobState>` projection payload.
    pub payload: String,
    /// The projection-only lease column -- the value the runnable check reads.
    pub lease_until_ms: Option<i64>,
}

/// What the crate's `plan_claim` decided after re-reading the row
/// in-transaction. `Won` is the crate's strongly-typed success payload, opaque
/// to the backend.
pub enum ClaimDecision<Won> {
    /// Win the claim: append `event` (a `Claimed`) at `event.sequence` (a
    /// uniqueness violation -> [`ClaimOutcome::Contended`]); then write the row
    /// `(version = event.sequence, payload, lease_until_ms)` ->
    /// [`ClaimOutcome::Won`].
    Claim {
        /// The `Claimed` event to compare-and-swap-append.
        event: SerializedEvent,
        /// The new `Lifecycle<JobState>` projection payload.
        payload: String,
        /// The lease to write on the projection row.
        lease_until_ms: i64,
        /// The strongly-typed result handed back on success.
        won: Won,
    },
    /// Dead-letter as abandoned (claim budget exhausted): append `event` (a
    /// `Dead`) then write the terminal row -> [`ClaimOutcome::Abandoned`].
    Abandon {
        /// The `Dead` event to append.
        event: SerializedEvent,
        /// The new `Lifecycle<JobState>` (Dead) projection payload.
        payload: String,
    },
    /// Gone, not runnable, or undecodable: roll back -> [`ClaimOutcome::Skip`].
    Skip,
}

/// Outcome of [`EventBackend::claim`], carrying the crate's strongly-typed `Won`.
pub enum ClaimOutcome<Won> {
    /// Claimed; run it.
    Won(Won),
    /// Dead-lettered as abandoned; the slot is freed.
    Abandoned,
    /// A concurrent worker won the compare-and-swap.
    Contended,
    /// No longer runnable (terminated, re-claimed with a live lease, or gone).
    Skip,
}

/// Outcome of [`EventBackend::renew`].
pub enum LeaseRenewal {
    /// The update matched the claimed row: the lease is still held.
    Held,
    /// Zero rows matched: the ack advanced the row, or a re-claimer took it.
    Lost,
}
