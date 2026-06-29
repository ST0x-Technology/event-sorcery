//! Durable, retryable jobs for command side effects.
//!
//! Command handlers stay pure `(state, command) -> Vec<Event>` and enqueue side
//! effects as [`Job`]s. The framework flushes pending jobs inside the same
//! transaction that commits the triggering events, so a job is enqueued iff its
//! events commit -- closing the crash window between a side effect and the event
//! meant to record it.
//!
//! A job is its own event stream (`aggregate_type = "job"`): [`JobState`] is an
//! [`EventSourced`] aggregate, so the claim/ack lifecycle rides cqrs-es. The
//! claim itself is a backend transaction ([`crate::EventBackend::claim`]) because
//! the live lease is projection-only (see [ADR-0006]); the ack (succeed / retry /
//! dead) is a [`JobCommand`] fenced on the [`ClaimId`] of the claim that ran it.
//!
//! [ADR-0006]: ../../adrs/0006-cqrs-native-durable-jobs.md

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cqrs_es::DomainEvent;
use cqrs_es::persist::SerializedEvent;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::future::Future;
use std::marker::PhantomData;
use std::time::Duration;
use tracing::{error, warn};
use ulid::Ulid;

use crate::dependency::{Cons, Nil};
use crate::job_store::{ClaimDecision, ClaimRead};
use crate::lifecycle::Lifecycle;
use crate::{CompactionPolicy, EventSourced, Table};

const JOB_AGGREGATE_TYPE: &str = "job";
const JOB_EVENT_VERSION: &str = "1.0";

/// A durable, retryable unit of side-effecting work.
///
/// Each implementation is one self-contained side effect; an entity declares the
/// set of jobs its commands dispatch. The job is appended as an event and
/// executed by a supervised worker, which calls [`perform`](Job::perform) with
/// the consumer-owned [`Input`](Job::Input) dependency bundle.
pub trait Job: Serialize + DeserializeOwned + Send + 'static {
    /// Dependency bundle injected into [`perform`](Job::perform).
    type Input: Send + Sync + 'static;

    /// Value produced on successful completion.
    type Output: Send + 'static;

    /// Error returned when [`perform`](Job::perform) fails.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Worker name prefix; the registered worker name is
    /// `format!("{WORKER_NAME}-{index}")`.
    const WORKER_NAME: &'static str;

    /// Stable identifier for this job kind, recorded on the `Enqueued` event and
    /// used to route a job to the worker that runs it.
    const KIND: &'static str;

    /// Logged when retries are exhausted.
    const TERMINAL_FAILURE_MSG: &'static str = "Job failed after retries";

    /// Human-readable label for structured logging.
    fn label(&self) -> Label;

    /// Execute this job against the injected input.
    ///
    /// Return [`JobOutcome::Done`] when finished, or [`JobOutcome::Defer`] to
    /// re-run later without counting an attempt (a poll-while-pending or
    /// external-wait snooze). An `Err` is a failure that counts an attempt and is
    /// retried/dead-lettered per the worker config.
    fn perform(
        &self,
        input: &Self::Input,
    ) -> impl Future<Output = Result<JobOutcome<Self::Output>, Self::Error>> + Send;
}

/// What a [`Job::perform`] reports back to the worker (ADR-0007).
#[derive(Debug)]
pub enum JobOutcome<Output> {
    /// The job finished; the worker records success and acks it (terminal).
    Done(Output),
    /// The job is not done but did not fail -- re-run it after this delay WITHOUT
    /// counting an attempt or recording a failure (distinct from a retry). The
    /// claim budget resets on a defer, so a poller may defer indefinitely; the
    /// job keeps its id, attempt, and payload across the snooze.
    Defer(Duration),
}

/// Human-readable identifier for a job instance, used in logs.
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

/// Stable identifier for a job kind -- the [`Job::KIND`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct JobKind(String);

impl JobKind {
    pub(crate) fn new(kind: impl Into<String>) -> Self {
        Self(kind.into())
    }
}

/// Identifier of the worker holding a claim, recorded on `Claimed` for diagnostics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct WorkerId(String);

impl WorkerId {
    /// A process-run-unique worker id: the name plus a fresh ULID.
    pub(crate) fn new(name: &str) -> Self {
        Self(format!("{name}:{}", Ulid::new()))
    }
}

/// Identity of a single claim attempt -- a fresh ULID minted per claim (never per
/// worker), so an ack fences against the exact claim that produced it. A
/// re-claim mints a new `ClaimId`, so the prior runner's ack is rejected.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ClaimId(String);

impl ClaimId {
    pub(crate) fn new() -> Self {
        Self(Ulid::new().to_string())
    }
}

/// Why a job was dead-lettered.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum DeadReason {
    /// Every retry attempt failed.
    RetriesExhausted,
    /// The stored payload no longer deserializes into the job type.
    Undecodable,
    /// The job was claimed too many times without recording an outcome
    /// (a crash/hang loop), exceeding the claim budget.
    Abandoned,
}

/// A transition in a durable job's lifecycle, appended to the job's event stream.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) enum JobEvent {
    /// Job created and runnable at `run_at`.
    Enqueued {
        kind: JobKind,
        payload: serde_json::Value,
        #[serde(with = "chrono::serde::ts_milliseconds")]
        run_at: DateTime<Utc>,
    },
    /// A worker took the job under claim `claim_id`, leased until `lease_until`.
    /// `lease_until` is retained for audit; the live lease is the projection
    /// column, so the fold drops it from [`JobState`].
    Claimed {
        worker: WorkerId,
        claim_id: ClaimId,
        #[serde(with = "chrono::serde::ts_milliseconds")]
        lease_until: DateTime<Utc>,
    },
    /// The job ran to success (terminal).
    Succeeded,
    /// An attempt failed with `error`; the job runs again as `attempt` at `run_at`.
    RetryScheduled {
        #[serde(with = "chrono::serde::ts_milliseconds")]
        run_at: DateTime<Utc>,
        attempt: u32,
        error: String,
    },
    /// A successful defer: the job runs again at `run_at` without counting an
    /// attempt or recording a failure (ADR-0007). The claim budget resets.
    Rescheduled {
        #[serde(with = "chrono::serde::ts_milliseconds")]
        run_at: DateTime<Utc>,
    },
    /// An attempt failed terminally (terminal).
    Dead { reason: DeadReason, error: String },
}

impl JobEvent {
    fn type_name(&self) -> &'static str {
        match self {
            Self::Enqueued { .. } => "JobEnqueued",
            Self::Claimed { .. } => "JobClaimed",
            Self::Succeeded => "JobSucceeded",
            Self::RetryScheduled { .. } => "JobRetryScheduled",
            Self::Rescheduled { .. } => "JobRescheduled",
            Self::Dead { .. } => "JobDead",
        }
    }

    /// Serializes this event for the given job stream at `sequence`.
    pub(crate) fn serialized(
        &self,
        job_id: &str,
        sequence: usize,
    ) -> Result<SerializedEvent, serde_json::Error> {
        Ok(SerializedEvent {
            aggregate_id: job_id.to_string(),
            sequence,
            aggregate_type: JOB_AGGREGATE_TYPE.to_string(),
            event_type: self.type_name().to_string(),
            event_version: JOB_EVENT_VERSION.to_string(),
            payload: serde_json::to_value(self)?,
            metadata: serde_json::Value::Object(serde_json::Map::new()),
        })
    }
}

impl DomainEvent for JobEvent {
    fn event_type(&self) -> String {
        self.type_name().to_string()
    }

    fn event_version(&self) -> String {
        JOB_EVENT_VERSION.to_string()
    }
}

/// The folded state of a durable job -- the [`EventSourced`] entity.
///
/// `Claimed` carries no `lease_until`: the live lease lives only in the
/// `job_queue` projection column (D1), so it cannot be folded from the event
/// stream. `claims` counts claim attempts (the D5 budget), folded because
/// `transition` sees only `&self`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) enum JobState {
    /// Runnable at `run_at`; `attempt` counts attempts that have already failed.
    Pending {
        kind: JobKind,
        payload: serde_json::Value,
        #[serde(with = "chrono::serde::ts_milliseconds")]
        run_at: DateTime<Utc>,
        attempt: u32,
        claims: u32,
    },
    /// Held under claim `claim_id` by `worker`.
    Claimed {
        kind: JobKind,
        payload: serde_json::Value,
        worker: WorkerId,
        claim_id: ClaimId,
        #[serde(with = "chrono::serde::ts_milliseconds")]
        run_at: DateTime<Utc>,
        attempt: u32,
        claims: u32,
    },
    /// Completed successfully (terminal).
    Done,
    /// Dead-lettered (terminal).
    Dead { reason: DeadReason },
}

/// A command against a claimed job: the ack of the claim `claim_id` ran. The
/// claim itself is not a command -- it is [`crate::EventBackend::claim`].
#[derive(Debug, Clone)]
pub(crate) enum JobCommand {
    /// The job ran to success.
    Succeed { claim_id: ClaimId },
    /// The attempt failed; reschedule as `attempt` at `run_at`.
    RetrySchedule {
        claim_id: ClaimId,
        run_at: DateTime<Utc>,
        attempt: u32,
        error: String,
    },
    /// The attempt deferred (success): re-run at `run_at` without counting an
    /// attempt or recording a failure.
    Reschedule {
        claim_id: ClaimId,
        run_at: DateTime<Utc>,
    },
    /// The attempt failed terminally; dead-letter it.
    Kill {
        claim_id: ClaimId,
        reason: DeadReason,
        error: String,
    },
}

impl JobCommand {
    fn claim_id(&self) -> &ClaimId {
        match self {
            Self::Succeed { claim_id }
            | Self::RetrySchedule { claim_id, .. }
            | Self::Reschedule { claim_id, .. }
            | Self::Kill { claim_id, .. } => claim_id,
        }
    }
}

/// Domain error from a [`JobCommand`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub(crate) enum JobError {
    /// The ack's claim does not own the job -- a re-claimer took it. The ack is
    /// rejected before any event is written.
    #[error("job ack fenced: the claim no longer owns the job")]
    Fenced,
}

#[async_trait]
impl EventSourced for JobState {
    type Id = String;
    type Event = JobEvent;
    type Command = JobCommand;
    type Error = JobError;
    type Jobs = Nil;
    type Materialized = Table;

    const AGGREGATE_TYPE: &'static str = JOB_AGGREGATE_TYPE;
    const PROJECTION: Table = Table("job_queue");
    const SCHEMA_VERSION: u64 = 1;
    const COMPACTION_POLICY: CompactionPolicy = CompactionPolicy::Retain;

    fn originate(event: &JobEvent) -> Option<Self> {
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
                claims: 0,
            }),
            _ => None,
        }
    }

    fn evolve(entity: &Self, event: &JobEvent) -> Result<Option<Self>, JobError> {
        let next = match (entity, event) {
            // Claim or lease-expiry re-claim: adopt the new claim, count it, keep
            // the attempt (a crash is not a failed attempt). The event's
            // lease_until is dropped -- the live lease is projection-only.
            (
                Self::Pending {
                    kind,
                    payload,
                    run_at,
                    attempt,
                    claims,
                }
                | Self::Claimed {
                    kind,
                    payload,
                    run_at,
                    attempt,
                    claims,
                    ..
                },
                JobEvent::Claimed {
                    worker, claim_id, ..
                },
            ) => Self::Claimed {
                kind: kind.clone(),
                payload: payload.clone(),
                worker: worker.clone(),
                claim_id: claim_id.clone(),
                run_at: *run_at,
                attempt: *attempt,
                claims: claims + 1,
            },
            (Self::Claimed { .. }, JobEvent::Succeeded) => Self::Done,
            (
                Self::Claimed {
                    kind,
                    payload,
                    claims,
                    ..
                },
                JobEvent::RetryScheduled {
                    run_at, attempt, ..
                },
            ) => Self::Pending {
                kind: kind.clone(),
                payload: payload.clone(),
                run_at: *run_at,
                attempt: *attempt,
                claims: *claims,
            },
            // A defer is a productive checkpoint, not a failure: keep the attempt,
            // re-arm run_at, and reset the claim budget so a poller may defer
            // indefinitely (the budget only catches claims with no outcome).
            (
                Self::Claimed {
                    kind,
                    payload,
                    attempt,
                    ..
                },
                JobEvent::Rescheduled { run_at },
            ) => Self::Pending {
                kind: kind.clone(),
                payload: payload.clone(),
                run_at: *run_at,
                attempt: *attempt,
                claims: 0,
            },
            (Self::Pending { .. } | Self::Claimed { .. }, JobEvent::Dead { reason, .. }) => {
                Self::Dead {
                    reason: reason.clone(),
                }
            }
            // Any other event for this state is a corrupt stream; surface it
            // loudly rather than silently no-op.
            _ => return Ok(None),
        };
        Ok(Some(next))
    }

    async fn initialize(
        _command: JobCommand,
        _jobs: &JobQueue<Nil>,
    ) -> Result<Vec<JobEvent>, JobError> {
        // Jobs are born from the enqueue flush (a raw `Enqueued` append), never a
        // command. An ack of a vanished job is a harmless no-op.
        Ok(vec![])
    }

    async fn transition(
        &self,
        command: JobCommand,
        _jobs: &JobQueue<Nil>,
    ) -> Result<Vec<JobEvent>, JobError> {
        let Self::Claimed { claim_id: held, .. } = self else {
            return Err(JobError::Fenced);
        };
        if held != command.claim_id() {
            return Err(JobError::Fenced);
        }
        let event = match command {
            JobCommand::Succeed { .. } => JobEvent::Succeeded,
            JobCommand::RetrySchedule {
                run_at,
                attempt,
                error,
                ..
            } => JobEvent::RetryScheduled {
                run_at,
                attempt,
                error,
            },
            JobCommand::Reschedule { run_at, .. } => JobEvent::Rescheduled { run_at },
            JobCommand::Kill { reason, error, .. } => JobEvent::Dead { reason, error },
        };
        Ok(vec![event])
    }
}

/// The strongly-typed success payload of a claim, handed back to the worker (and
/// carried opaquely through [`crate::EventBackend::claim`] as its `Won`).
pub(crate) struct WonClaim {
    /// Sequence of the appended `Claimed` event; the renew/ack key.
    pub(crate) claim_seq: i64,
    /// This claim attempt's id; the ack fence key.
    pub(crate) claim_id: ClaimId,
    /// Recorded failures so far.
    pub(crate) attempt: u32,
    /// The job arguments, folded from state.
    pub(crate) args: serde_json::Value,
}

/// A fully-resolved enqueue (id + run_at computed), drained from the per-command
/// buffer and flushed by the event repository in the commit transaction.
pub(crate) struct EnqueueRequest {
    /// The new job's id (a fresh ULID).
    pub(crate) job_id: String,
    /// The job kind.
    pub(crate) kind: JobKind,
    /// The job arguments.
    pub(crate) payload: serde_json::Value,
    /// When the job becomes runnable (Unix epoch milliseconds).
    pub(crate) run_at_ms: i64,
}

/// Pure es-side errors building enqueue/claim payloads (no backend specifics).
#[derive(Debug, thiserror::Error)]
pub enum JobStoreError {
    /// A job event/state could not be JSON-encoded.
    #[error("failed to encode a job payload")]
    Encode(#[from] serde_json::Error),
    /// A requested delay exceeds the representable range.
    #[error("job delay exceeds the representable range")]
    DelayOverflow,
}

/// Decides what a claim transaction should do, given the row it re-read in-txn.
///
/// The runnable check reads `read.lease_until_ms` -- the renewed projection
/// column, NOT a folded event lease -- which is the entire reason the claim is a
/// backend transaction rather than an `execute(Claim)` command.
pub(crate) fn plan_claim(
    job_id: &str,
    read: Option<ClaimRead>,
    worker: &WorkerId,
    now_ms: i64,
    lease_ms: i64,
    max_claims: u32,
) -> ClaimDecision<WonClaim> {
    let Some(read) = read else {
        return ClaimDecision::Skip;
    };
    let lifecycle: Lifecycle<JobState> = match serde_json::from_str(&read.payload) {
        Ok(lifecycle) => lifecycle,
        Err(error) => {
            error!(target: "cqrs", job_id, ?error, "job_queue payload undecodable; needs rebuild");
            return ClaimDecision::Skip;
        }
    };
    let Lifecycle::Live(state) = lifecycle else {
        return ClaimDecision::Skip;
    };

    // Runnable keys on the STATE, not on whether the lease column is set: the
    // reactor never clears lease_until, so a pending row can carry a stale lease
    // from a prior claim. A Pending consults run_at; a Claimed consults the lease
    // column (NULL means a rebuilt-but-unclaimed row, which is reclaimable).
    let runnable = match &state {
        JobState::Pending { run_at, .. } => run_at.timestamp_millis() <= now_ms,
        JobState::Claimed { .. } => read.lease_until_ms.is_none_or(|lease| lease < now_ms),
        JobState::Done | JobState::Dead { .. } => false,
    };
    if !runnable {
        return ClaimDecision::Skip;
    }

    let (kind, payload, run_at, attempt, claims) = match state {
        JobState::Pending {
            kind,
            payload,
            run_at,
            attempt,
            claims,
        }
        | JobState::Claimed {
            kind,
            payload,
            run_at,
            attempt,
            claims,
            ..
        } => (kind, payload, run_at, attempt, claims),
        JobState::Done | JobState::Dead { .. } => return ClaimDecision::Skip,
    };

    let claim_seq = read.version + 1;
    let Ok(claim_seq_usize) = usize::try_from(claim_seq) else {
        error!(target: "cqrs", job_id, claim_seq, "job claim sequence out of range");
        return ClaimDecision::Skip;
    };

    if claims >= max_claims {
        return plan_abandon(job_id, claim_seq_usize);
    }

    let claim_id = ClaimId::new();
    let lease_until_ms = now_ms + lease_ms;
    let claimed = JobEvent::Claimed {
        worker: worker.clone(),
        claim_id: claim_id.clone(),
        lease_until: from_millis(lease_until_ms),
    };
    let new_state = JobState::Claimed {
        kind,
        payload: payload.clone(),
        worker: worker.clone(),
        claim_id: claim_id.clone(),
        run_at,
        attempt,
        claims: claims + 1,
    };
    let (Ok(event), Ok(payload_str)) = (
        claimed.serialized(job_id, claim_seq_usize),
        serde_json::to_string(&Lifecycle::Live(new_state)),
    ) else {
        error!(target: "cqrs", job_id, "failed to encode Claimed");
        return ClaimDecision::Skip;
    };

    ClaimDecision::Claim {
        event,
        payload: payload_str,
        lease_until_ms,
        won: WonClaim {
            claim_seq,
            claim_id,
            attempt,
            args: payload,
        },
    }
}

fn plan_abandon(job_id: &str, claim_seq_usize: usize) -> ClaimDecision<WonClaim> {
    let dead = JobEvent::Dead {
        reason: DeadReason::Abandoned,
        error: "claim budget exhausted".to_string(),
    };
    let (Ok(event), Ok(payload_str)) = (
        dead.serialized(job_id, claim_seq_usize),
        serde_json::to_string(&Lifecycle::<JobState>::Live(JobState::Dead {
            reason: DeadReason::Abandoned,
        })),
    ) else {
        error!(target: "cqrs", job_id, "failed to encode Dead{{Abandoned}}");
        return ClaimDecision::Skip;
    };
    ClaimDecision::Abandon {
        event,
        payload: payload_str,
    }
}

fn from_millis(millis: i64) -> DateTime<Utc> {
    DateTime::from_timestamp_millis(millis).unwrap_or_else(Utc::now)
}

/// Builds the `Enqueued` event (sequence 1) for an [`EnqueueRequest`].
pub(crate) fn enqueued_event(request: &EnqueueRequest) -> Result<SerializedEvent, JobStoreError> {
    Ok(JobEvent::Enqueued {
        kind: request.kind.clone(),
        payload: request.payload.clone(),
        run_at: from_millis(request.run_at_ms),
    }
    .serialized(&request.job_id, 1)?)
}

/// Builds the seed `job_queue` projection payload (version 1) for an enqueue:
/// `Lifecycle::Live(Pending)`.
pub(crate) fn pending_seed_payload(request: &EnqueueRequest) -> Result<String, JobStoreError> {
    Ok(serde_json::to_string(&Lifecycle::Live(
        JobState::Pending {
            kind: request.kind.clone(),
            payload: request.payload.clone(),
            run_at: from_millis(request.run_at_ms),
            attempt: 0,
            claims: 0,
        },
    ))?)
}

/// A job buffered by a handler, awaiting flush at the next commit.
struct PendingPush {
    job: Box<dyn ErasedJob>,
    delay: Option<Duration>,
}

/// Type erasure over a [`Job`] so a [`JobQueue`] can buffer heterogeneous job
/// types; the queue needs only the routing kind and the encoded payload.
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

/// Type-level list of the [`Job`] types an entity may dispatch.
///
/// Built from [`Cons`]/[`Nil`] -- write it with the [`jobs!`](crate::jobs) macro
/// (`jobs![SendEmail, ChargeCard]`). A [`JobQueue`] over this list compile-checks
/// that every pushed job is a declared member.
pub trait JobList {}

impl JobList for Nil {}

impl<Head: Job, Tail: JobList> JobList for Cons<Head, Tail> {}

/// Compile-time proof that job `J` is a member of a [`JobList`].
///
/// `Index` is an inferred [`Here`]/[`There`] witness that disambiguates the two
/// recursive impls so they don't overlap; call sites never name it.
pub trait Contains<J: Job, Index> {}

/// Membership witness: `J` is the head of the list.
pub struct Here;

/// Membership witness: `J` is `Index` positions into the tail.
pub struct There<Index>(PhantomData<Index>);

impl<J: Job, Tail> Contains<J, Here> for Cons<J, Tail> {}

impl<J: Job, Head, Tail, Index> Contains<J, There<Index>> for Cons<Head, Tail> where
    Tail: Contains<J, Index>
{
}

/// Build a type-level [`JobList`] from job types.
///
/// `jobs![SendEmail, ChargeCard]` expands to `Cons<SendEmail, Cons<ChargeCard,
/// Nil>>`; empty `jobs![]` is `Nil`. Use it for
/// [`EventSourced::Jobs`](crate::EventSourced::Jobs).
#[macro_export]
macro_rules! jobs {
    () => { $crate::Nil };
    ($head:ty $(, $tail:ty)* $(,)?) => {
        $crate::Cons<$head, $crate::jobs![$($tail),*]>
    };
}

/// Handler-facing handle for enqueuing an entity's [`Job`]s.
///
/// Parameterized by the entity's [`JobList`] so [`push`](Self::push)
/// compile-checks that the job is declared in
/// [`EventSourced::Jobs`](crate::EventSourced::Jobs).
///
/// [`push`](Self::push) / [`push_with_delay`](Self::push_with_delay) are
/// synchronous and buffer onto the per-command scope that [`Store::send`] opens
/// around command execution; the event repository drains that scope inside the
/// commit transaction ([`take_pending`]), so enqueue is atomic with the
/// triggering events. A push outside a command scope is a programming error (the
/// handle escaped its handler) and is dropped with a warning.
///
/// [`Store::send`]: crate::Store::send
pub struct JobQueue<Jobs: JobList = Nil>(PhantomData<fn() -> Jobs>);

impl<Jobs: JobList> Default for JobQueue<Jobs> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<Jobs: JobList> JobQueue<Jobs> {
    /// Enqueues a job (which must be declared in `Jobs`) to run as soon as the
    /// triggering events commit.
    pub fn push<J, Index>(&self, job: J)
    where
        J: Job,
        Jobs: Contains<J, Index>,
    {
        buffer(PendingPush {
            job: Box::new(job),
            delay: None,
        });
    }

    /// Enqueues a job (which must be declared in `Jobs`) to run no sooner than
    /// `delay` after the events commit.
    pub fn push_with_delay<J, Index>(&self, job: J, delay: Duration)
    where
        J: Job,
        Jobs: Contains<J, Index>,
    {
        buffer(PendingPush {
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

/// Runs `command_execution` with a fresh pending-job scope active, so a handler's
/// [`JobQueue::push`] calls land where the repository's flush ([`take_pending`])
/// will drain them -- in the same transaction that commits the events.
pub(crate) async fn with_pending_scope<Output>(
    command_execution: impl Future<Output = Output>,
) -> Output {
    PENDING_JOBS
        .scope(RefCell::new(Vec::new()), command_execution)
        .await
}

/// Pushes onto the active pending-job scope, warning if none is active (a push
/// from outside a command handler, which cannot be flushed atomically).
fn buffer(push: PendingPush) {
    if PENDING_JOBS
        .try_with(|pending| pending.borrow_mut().push(push))
        .is_err()
    {
        warn!(
            target: "cqrs",
            "JobQueue::push called outside a command scope; the job was dropped"
        );
    }
}

/// Drains the per-command pending-job buffer into [`EnqueueRequest`]s (each with
/// a fresh ULID id and a resolved `run_at`). A no-op when no buffer scope is
/// active or it is empty.
pub(crate) fn take_pending() -> Result<Vec<EnqueueRequest>, JobStoreError> {
    let pending = PENDING_JOBS
        .try_with(|buffer| buffer.borrow_mut().drain(..).collect::<Vec<_>>())
        .unwrap_or_default();

    pending
        .into_iter()
        .map(|push| {
            Ok(EnqueueRequest {
                job_id: Ulid::new().to_string(),
                kind: push.job.kind(),
                payload: push.job.encode()?,
                run_at_ms: resolve_run_at_ms(push.delay)?,
            })
        })
        .collect()
}

/// Builds a standalone [`EnqueueRequest`] (fresh ULID, resolved `run_at`) for one
/// `job` -- the reactor / poller / job-chain enqueue path, without the
/// command-scope buffer [`take_pending`] drains.
pub(crate) fn enqueue_request<J: Job>(
    job: &J,
    delay: Option<Duration>,
) -> Result<EnqueueRequest, JobStoreError> {
    Ok(EnqueueRequest {
        job_id: Ulid::new().to_string(),
        kind: JobKind::new(J::KIND),
        payload: serde_json::to_value(job)?,
        run_at_ms: resolve_run_at_ms(delay)?,
    })
}

/// Resolves a job's `run_at` (epoch millis) from an optional delay-from-now.
fn resolve_run_at_ms(delay: Option<Duration>) -> Result<i64, JobStoreError> {
    let run_at = match delay {
        None => Utc::now(),
        Some(delay) => {
            Utc::now()
                + chrono::Duration::from_std(delay).map_err(|_| JobStoreError::DelayOverflow)?
        }
    };
    Ok(run_at.timestamp_millis())
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use serde::{Deserialize, Serialize};

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

        async fn perform(&self, _input: &()) -> Result<JobOutcome<()>, Infallible> {
            Ok(JobOutcome::Done(()))
        }
    }

    fn at(secs: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(secs, 0).expect("valid timestamp")
    }

    fn claimed(claim_id: &str, claims: u32) -> JobState {
        JobState::Claimed {
            kind: JobKind::new("send-email"),
            payload: serde_json::json!({}),
            worker: WorkerId::new("w"),
            claim_id: ClaimId(claim_id.to_string()),
            run_at: at(0),
            attempt: 0,
            claims,
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

    #[tokio::test]
    async fn push_within_a_scope_is_taken_by_the_flush() {
        let taken = with_pending_scope(async {
            let queue = JobQueue::<crate::jobs![SendEmail]>::default();
            queue.push(SendEmail::Welcome {
                address: "a@example.com".to_string(),
            });
            queue.push_with_delay(
                SendEmail::Reminder {
                    address: "b@example.com".to_string(),
                },
                Duration::from_secs(60),
            );
            take_pending().unwrap()
        })
        .await;

        assert_eq!(taken.len(), 2);
        assert_eq!(taken[0].kind, JobKind::new("send-email"));
        assert_eq!(
            taken[0].payload,
            serde_json::json!({ "Welcome": { "address": "a@example.com" } })
        );
    }

    #[tokio::test]
    async fn push_outside_a_scope_is_dropped() {
        let queue = JobQueue::<crate::jobs![SendEmail]>::default();
        queue.push(SendEmail::Welcome {
            address: "a@example.com".to_string(),
        });

        // The earlier push had no scope and was dropped; a fresh scope is empty.
        let taken = with_pending_scope(async { take_pending().unwrap() }).await;
        assert!(taken.is_empty());
    }

    #[tokio::test]
    async fn transition_fences_a_foreign_claim_and_succeeds_for_the_owner() {
        let state = claimed("claim-a", 1);

        let fenced = state
            .transition(
                JobCommand::Succeed {
                    claim_id: ClaimId("claim-b".to_string()),
                },
                &JobQueue::<Nil>::default(),
            )
            .await;
        assert_eq!(fenced, Err(JobError::Fenced));

        let owned = state
            .transition(
                JobCommand::Succeed {
                    claim_id: ClaimId("claim-a".to_string()),
                },
                &JobQueue::<Nil>::default(),
            )
            .await
            .unwrap();
        assert_eq!(owned, vec![JobEvent::Succeeded]);
    }

    #[test]
    fn evolve_claim_increments_claims_and_drops_the_lease() {
        let pending = JobState::Pending {
            kind: JobKind::new("send-email"),
            payload: serde_json::json!({}),
            run_at: at(0),
            attempt: 0,
            claims: 0,
        };
        let event = JobEvent::Claimed {
            worker: WorkerId::new("w"),
            claim_id: ClaimId("c".to_string()),
            lease_until: at(100),
        };

        let next = JobState::evolve(&pending, &event).unwrap().unwrap();
        let JobState::Claimed {
            claims, claim_id, ..
        } = next
        else {
            panic!("expected Claimed");
        };
        // The fold counts the claim and adopts its id; the event's lease_until is
        // intentionally not folded -- the live lease is the projection column.
        assert_eq!(claims, 1);
        assert_eq!(claim_id, ClaimId("c".to_string()));
    }

    #[test]
    fn evolve_retry_preserves_claims_and_returns_to_pending() {
        let state = claimed("c", 3);
        let event = JobEvent::RetryScheduled {
            run_at: at(50),
            attempt: 1,
            error: "boom".to_string(),
        };

        let next = JobState::evolve(&state, &event).unwrap().unwrap();
        let JobState::Pending {
            claims, attempt, ..
        } = next
        else {
            panic!("expected Pending");
        };
        assert_eq!(claims, 3);
        assert_eq!(attempt, 1);
    }
}
