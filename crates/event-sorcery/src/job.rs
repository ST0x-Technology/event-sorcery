//! Durable, retryable jobs for command side effects.
//!
//! Command handlers stay pure `(state, command) -> Vec<Event>` and
//! enqueue side effects as [`Job`]s. The framework flushes pending
//! jobs inside the same SQLite transaction that commits the
//! triggering events, so a job is enqueued iff its events commit --
//! closing the crash-safety window between a side effect and the
//! event meant to record it.
//!
//! Each job is its own event stream in the event store (a `JobEnqueued`
//! event and the lifecycle events that follow); a supervised worker claims
//! and runs it. [`perform`](Job::perform) receives the consumer-owned
//! [`Input`](Job::Input) dependency bundle.

use chrono::{DateTime, Utc};
use cqrs_es::persist::SerializedEvent;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sqlx::SqliteConnection;
use std::cell::RefCell;
use std::future::Future;
use std::time::Duration;
use tracing::warn;
use ulid::Ulid;

use sqlite_es::{SqliteAggregateError, insert_serialized_events_batch};

/// A durable, retryable unit of side-effecting work.
///
/// Each implementation is one self-contained side effect; an entity
/// declares the set of jobs its commands dispatch. The job is appended
/// as an event and executed by a supervised worker, which calls
/// [`perform`](Job::perform) with the consumer-owned
/// [`Input`](Job::Input) dependency bundle.
pub trait Job: Serialize + DeserializeOwned + Send + 'static {
    /// Dependency bundle injected into [`perform`](Job::perform).
    ///
    /// The consumer's worker wiring constructs and owns this; the
    /// framework only forwards a shared reference.
    type Input: Send + Sync + 'static;

    /// Value produced on successful completion.
    type Output: Send + 'static;

    /// Error returned when [`perform`](Job::perform) fails.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Worker name prefix; the registered worker name is
    /// `format!("{WORKER_NAME}-{index}")`.
    const WORKER_NAME: &'static str;

    /// Stable identifier for this job kind, used by the
    /// failure-injection registry and structured logs. Distinct
    /// from [`WORKER_NAME`](Job::WORKER_NAME) because multiple
    /// workers can share a kind.
    const KIND: &'static str;

    /// Logged when retries are exhausted.
    const TERMINAL_FAILURE_MSG: &'static str = "Job failed after retries";

    /// Human-readable label for structured logging.
    fn label(&self) -> Label;

    /// Execute this job against the injected input.
    fn perform(
        &self,
        input: &Self::Input,
    ) -> impl Future<Output = Result<Self::Output, Self::Error>> + Send;
}

/// Human-readable identifier for a job instance, used in logs and
/// failure-injection targeting.
#[derive(Debug, Clone)]
pub struct Label(String);

impl Label {
    /// Wraps a string-like value as a label.
    pub fn new(label: impl Into<String>) -> Self {
        Self(label.into())
    }
}

impl std::fmt::Display for Label {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Stable identifier for a job kind -- the [`Job::KIND`] -- recorded on the
/// `Enqueued` event and used to route a job to the worker that runs it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct JobKind(String);

impl JobKind {
    pub(crate) fn new(kind: impl Into<String>) -> Self {
        Self(kind.into())
    }

    pub(crate) fn as_str(&self) -> &str {
        let Self(kind) = self;
        kind
    }
}

/// Identifier of the worker holding a claim, recorded on `Claimed` for lease
/// ownership and diagnostics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct WorkerId(String);

impl WorkerId {
    /// A process-run-unique worker id: the worker name plus a fresh ULID, so a
    /// restarted process never reuses an old id (sound ownership and audit).
    pub(crate) fn new(name: &str) -> Self {
        Self(format!("{name}:{}", Ulid::new()))
    }
}

/// Why a job was dead-lettered.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum DeadReason {
    /// Every retry attempt failed.
    RetriesExhausted,
    /// The stored payload no longer deserializes into the job type and the
    /// rolling-deploy grace window has elapsed (genuine poison, not version skew).
    Undecodable,
    /// The job was claimed too many times without ever recording an outcome
    /// (a crash or hang loop), exceeding the claim budget.
    Abandoned,
}

/// A transition in a durable job's lifecycle, appended to the job's own event
/// stream.
///
/// A failed attempt is recorded by the `RetryScheduled` or `Dead` event it
/// produces (each carries the failing `error`), so there is exactly one event
/// per outcome and the state machine has no in-between "failed" limbo.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) enum JobEvent {
    /// Job created and runnable at `run_at` (now, for an immediate enqueue).
    Enqueued {
        kind: JobKind,
        payload: serde_json::Value,
        run_at: DateTime<Utc>,
    },
    /// A worker took the job under a lease valid until `lease_until`.
    Claimed {
        worker: WorkerId,
        lease_until: DateTime<Utc>,
    },
    /// The job ran to success (terminal).
    Succeeded,
    /// An attempt failed with `error`; the job runs again as `attempt` at
    /// `run_at`.
    RetryScheduled {
        run_at: DateTime<Utc>,
        attempt: u32,
        error: String,
    },
    /// An attempt failed with `error` and retries are exhausted (terminal).
    Dead { reason: DeadReason, error: String },
}

/// The folded state of a durable job.
///
/// Each status carries exactly the data it needs, so invalid combinations (a
/// claimed job with no lease, a completed job that still has a payload) are
/// unrepresentable.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum JobState {
    /// Runnable at `run_at`; `attempt` counts attempts that have already failed.
    Pending {
        kind: JobKind,
        payload: serde_json::Value,
        run_at: DateTime<Utc>,
        attempt: u32,
    },
    /// Held by `worker` until `lease_until`; `run_at` is retained so a
    /// re-queue on lease expiry keeps the job's schedule.
    Claimed {
        kind: JobKind,
        payload: serde_json::Value,
        worker: WorkerId,
        run_at: DateTime<Utc>,
        lease_until: DateTime<Utc>,
        attempt: u32,
    },
    /// Completed successfully (terminal).
    Done,
    /// Dead-lettered (terminal).
    Dead { reason: DeadReason },
}

impl JobState {
    /// Builds the initial state from the first event; only `Enqueued` originates
    /// a valid job stream.
    pub(crate) fn originate(event: &JobEvent) -> Option<Self> {
        match event {
            JobEvent::Enqueued {
                kind,
                payload,
                run_at,
            } => Some(Self::Pending {
                kind: kind.clone(),
                payload: payload.clone(),
                run_at: *run_at,
                attempt: 0,
            }),
            _ => None,
        }
    }

    /// Folds one event onto the current state. A `Claimed` on an
    /// already-claimed job is a lease-expiry re-claim and adopts the new lease
    /// without counting an attempt (a crash is not a failed attempt).
    /// Inapplicable transitions leave the state unchanged.
    pub(crate) fn apply(self, event: &JobEvent) -> Self {
        match (self, event) {
            (
                Self::Pending {
                    kind,
                    payload,
                    run_at,
                    attempt,
                }
                | Self::Claimed {
                    kind,
                    payload,
                    run_at,
                    attempt,
                    ..
                },
                JobEvent::Claimed {
                    worker,
                    lease_until,
                },
            ) => Self::Claimed {
                kind,
                payload,
                worker: worker.clone(),
                run_at,
                lease_until: *lease_until,
                attempt,
            },
            (Self::Claimed { .. }, JobEvent::Succeeded) => Self::Done,
            (
                Self::Claimed { kind, payload, .. },
                JobEvent::RetryScheduled {
                    run_at, attempt, ..
                },
            ) => Self::Pending {
                kind,
                payload,
                run_at: *run_at,
                attempt: *attempt,
            },
            (Self::Claimed { .. }, JobEvent::Dead { reason, .. }) => Self::Dead {
                reason: reason.clone(),
            },
            (state, _) => state,
        }
    }
}

const JOB_AGGREGATE_TYPE: &str = "job";
const JOB_EVENT_VERSION: &str = "1.0";

/// Error from the event-sourced job store's append + projection writes.
#[derive(Debug, thiserror::Error)]
pub enum JobStoreError {
    /// A job event could not be JSON-encoded.
    #[error("failed to encode job event")]
    Encode(#[from] serde_json::Error),
    /// A job event could not be appended to the event store.
    #[error("failed to append job event")]
    Append(#[from] SqliteAggregateError),
    /// A `job_queue` projection row could not be written.
    #[error("failed to update the job_queue projection")]
    Project(#[source] sqlx::Error),
    /// The job's sequence number exceeded the storable range.
    #[error("job sequence number out of range")]
    Sequence(#[from] std::num::TryFromIntError),
    /// The requested delay exceeds the representable range.
    #[error("job delay exceeds the representable range")]
    DelayOverflow,
    /// A job stream whose first event is not `Enqueued` was projected.
    #[error("job event stream does not begin with Enqueued")]
    OrphanEvent,
}

impl JobEvent {
    fn event_type(&self) -> &'static str {
        match self {
            Self::Enqueued { .. } => "JobEnqueued",
            Self::Claimed { .. } => "JobClaimed",
            Self::Succeeded => "JobSucceeded",
            Self::RetryScheduled { .. } => "JobRetryScheduled",
            Self::Dead { .. } => "JobDead",
        }
    }

    pub(crate) fn serialized(
        &self,
        job_id: &str,
        sequence: usize,
    ) -> Result<SerializedEvent, serde_json::Error> {
        Ok(SerializedEvent {
            aggregate_id: job_id.to_string(),
            sequence,
            aggregate_type: JOB_AGGREGATE_TYPE.to_string(),
            event_type: self.event_type().to_string(),
            event_version: JOB_EVENT_VERSION.to_string(),
            payload: serde_json::to_value(self)?,
            metadata: serde_json::Value::Object(serde_json::Map::new()),
        })
    }
}

/// Appends a job event to its stream within `connection`. The events table's
/// `(aggregate_type, aggregate_id, sequence)` uniqueness makes this a
/// compare-and-swap on `sequence`: a duplicate returns
/// [`SqliteAggregateError::OptimisticLock`].
pub(crate) async fn append_job_event(
    connection: &mut SqliteConnection,
    job_id: &str,
    sequence: usize,
    event: &JobEvent,
) -> Result<(), JobStoreError> {
    let serialized = event.serialized(job_id, sequence)?;
    insert_serialized_events_batch(
        &mut *connection,
        "events",
        std::slice::from_ref(&serialized),
    )
    .await?;
    Ok(())
}

/// Writes the active-job row for `state`, or removes it once the job reaches a
/// terminal state. `job_queue` holds only runnable/claimed jobs; the event
/// store keeps the full history.
pub(crate) async fn apply_to_job_queue(
    connection: &mut SqliteConnection,
    job_id: &str,
    state: &JobState,
    sequence: usize,
) -> Result<(), JobStoreError> {
    let sequence = i64::try_from(sequence)?;

    match state {
        JobState::Pending {
            kind,
            run_at,
            attempt,
            ..
        } => {
            sqlx::query(
                "INSERT INTO job_queue \
                   (job_id, kind, status, run_at, lease_until, attempt, sequence) \
                 VALUES (?1, ?2, 'pending', ?3, NULL, ?4, ?5) \
                 ON CONFLICT(job_id) DO UPDATE SET \
                   status = 'pending', run_at = excluded.run_at, lease_until = NULL, \
                   attempt = excluded.attempt, sequence = excluded.sequence",
            )
            .bind(job_id)
            .bind(kind.as_str())
            .bind(run_at.timestamp_millis())
            .bind(i64::from(*attempt))
            .bind(sequence)
            .execute(&mut *connection)
            .await
            .map_err(JobStoreError::Project)?;
        }
        JobState::Claimed {
            kind,
            run_at,
            lease_until,
            attempt,
            ..
        } => {
            sqlx::query(
                "INSERT INTO job_queue \
                   (job_id, kind, status, run_at, lease_until, attempt, sequence) \
                 VALUES (?1, ?2, 'claimed', ?3, ?4, ?5, ?6) \
                 ON CONFLICT(job_id) DO UPDATE SET \
                   status = 'claimed', run_at = excluded.run_at, \
                   lease_until = excluded.lease_until, attempt = excluded.attempt, \
                   sequence = excluded.sequence",
            )
            .bind(job_id)
            .bind(kind.as_str())
            .bind(run_at.timestamp_millis())
            .bind(lease_until.timestamp_millis())
            .bind(i64::from(*attempt))
            .bind(sequence)
            .execute(&mut *connection)
            .await
            .map_err(JobStoreError::Project)?;
        }
        JobState::Done | JobState::Dead { .. } => {
            sqlx::query("DELETE FROM job_queue WHERE job_id = ?1")
                .bind(job_id)
                .execute(&mut *connection)
                .await
                .map_err(JobStoreError::Project)?;
        }
    }

    Ok(())
}

/// Folds `event` onto `prior` and writes the resulting `job_queue` row.
async fn project_event(
    connection: &mut SqliteConnection,
    job_id: &str,
    sequence: usize,
    event: &JobEvent,
    prior: Option<JobState>,
) -> Result<(), JobStoreError> {
    let state = prior.map_or_else(
        || JobState::originate(event),
        |prior| Some(prior.apply(event)),
    );
    let Some(state) = state else {
        warn!(target: "cqrs", job_id, "job event stream does not begin with Enqueued");
        return Err(JobStoreError::OrphanEvent);
    };
    apply_to_job_queue(connection, job_id, &state, sequence).await
}

/// Enqueues a job: appends its `Enqueued` event and projects the new row, both
/// within `connection` (the triggering command's commit transaction), so the
/// job is enqueued iff that transaction commits. Returns the new job id.
pub(crate) async fn enqueue_job(
    connection: &mut SqliteConnection,
    kind: JobKind,
    payload: serde_json::Value,
    run_at: DateTime<Utc>,
) -> Result<String, JobStoreError> {
    let job_id = Ulid::new().to_string();
    let event = JobEvent::Enqueued {
        kind,
        payload,
        run_at,
    };
    append_job_event(connection, &job_id, 1, &event).await?;
    project_event(connection, &job_id, 1, &event, None).await?;
    Ok(job_id)
}

/// A job buffered by a handler, awaiting flush at the next commit.
struct PendingPush {
    job: Box<dyn ErasedJob>,
    delay: Option<Duration>,
}

/// Type erasure over a [`Job`] so a [`JobQueue`] can buffer heterogeneous job
/// types; the queue only needs the routing kind and the encoded payload.
trait ErasedJob: Send {
    fn kind(&self) -> JobKind;
    fn encode(&self) -> Result<serde_json::Value, serde_json::Error>;
}

impl<J: Job> ErasedJob for J {
    fn kind(&self) -> JobKind {
        JobKind::new(J::KIND)
    }

    fn encode(&self) -> Result<serde_json::Value, serde_json::Error> {
        serde_json::to_value(self)
    }
}

/// Handler-facing handle for enqueuing [`Job`]s.
///
/// [`push`](Self::push) / [`push_with_delay`](Self::push_with_delay) are
/// synchronous and merely buffer onto the handle; the framework drains the
/// buffer inside the event-commit transaction, so enqueue is atomic with the
/// triggering events and the handler stays I/O-free.
#[derive(Default)]
pub struct JobQueue {
    buffered: Vec<PendingPush>,
}

impl JobQueue {
    /// Buffers a job to run as soon as the triggering events commit.
    pub fn push<J: Job>(&mut self, job: J) {
        self.buffered.push(PendingPush {
            job: Box::new(job),
            delay: None,
        });
    }

    /// Buffers a job to run no sooner than `delay` after the events commit.
    pub fn push_with_delay<J: Job>(&mut self, job: J, delay: Duration) {
        self.buffered.push(PendingPush {
            job: Box::new(job),
            delay: Some(delay),
        });
    }
}

tokio::task_local! {
    /// Per-command buffer of jobs awaiting flush, drained by the event
    /// repository inside its commit transaction.
    static PENDING_JOBS: RefCell<Vec<PendingPush>>;
}

/// Drains the per-command pending-job buffer into the event store within
/// `connection`, appending each as an `Enqueued` event in the caller's commit
/// transaction. A no-op when no buffer scope is active or it is empty.
pub(crate) async fn flush_pending_jobs(
    connection: &mut SqliteConnection,
) -> Result<(), JobStoreError> {
    let pending = PENDING_JOBS
        .try_with(|buffer| buffer.borrow_mut().drain(..).collect::<Vec<_>>())
        .unwrap_or_default();

    for push in pending {
        let run_at = match push.delay {
            None => Utc::now(),
            Some(delay) => {
                Utc::now()
                    + chrono::Duration::from_std(delay).map_err(|_| JobStoreError::DelayOverflow)?
            }
        };
        enqueue_job(connection, push.job.kind(), push.job.encode()?, run_at).await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};
    use std::convert::Infallible;

    use super::*;

    #[derive(Debug, Serialize, Deserialize)]
    enum SendEmail {
        Welcome { address: String },
        Reminder { address: String },
    }

    impl Job for SendEmail {
        type Input = ();
        type Output = ();
        type Error = Infallible;

        const WORKER_NAME: &'static str = "send-email";
        const KIND: &'static str = "send-email";

        fn label(&self) -> Label {
            match self {
                Self::Welcome { address } => Label::new(format!("welcome:{address}")),
                Self::Reminder { address } => Label::new(format!("reminder:{address}")),
            }
        }

        async fn perform(&self, _input: &()) -> Result<(), Infallible> {
            Ok(())
        }
    }

    #[test]
    fn label_reflects_variant_and_renders_via_display() {
        let welcome = SendEmail::Welcome {
            address: "a@example.com".to_string(),
        };
        let reminder = SendEmail::Reminder {
            address: "b@example.com".to_string(),
        };

        assert_eq!(welcome.label().to_string(), "welcome:a@example.com");
        assert_eq!(reminder.label().to_string(), "reminder:b@example.com");
    }

    fn at(secs: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(secs, 0).expect("valid timestamp")
    }

    #[tokio::test]
    async fn enqueue_appends_an_enqueued_event_and_a_pending_row() {
        let pool = sqlx::SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!("../../migrations").run(&pool).await.unwrap();
        let mut connection = pool.acquire().await.unwrap();

        let job_id = enqueue_job(
            &mut connection,
            JobKind::new("send-email"),
            serde_json::json!({ "to": "a@example.com" }),
            at(0),
        )
        .await
        .unwrap();

        let (kind, status, attempt): (String, String, i64) =
            sqlx::query_as("SELECT kind, status, attempt FROM job_queue WHERE job_id = ?1")
                .bind(&job_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(kind, "send-email");
        assert_eq!(status, "pending");
        assert_eq!(attempt, 0);

        let (aggregate_type, sequence, event_type): (String, i64, String) = sqlx::query_as(
            "SELECT aggregate_type, sequence, event_type FROM events WHERE aggregate_id = ?1",
        )
        .bind(&job_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(aggregate_type, "job");
        assert_eq!(sequence, 1);
        assert_eq!(event_type, "JobEnqueued");
    }
}
