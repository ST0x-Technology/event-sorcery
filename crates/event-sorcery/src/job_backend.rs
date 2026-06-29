//! Worker-side durable-job execution: a generic [`apalis_core::Backend`].
//!
//! [`JobRuntime`] turns one [`EventBackend`] into the whole durable-jobs
//! capability -- the job [`Store`] (cqrs/es) and the `job_queue` [`Projection`],
//! auto-wired by [`StoreBuilder`]. [`EventStoreBackend`] polls that projection
//! for runnable jobs, claims each via [`EventBackend::claim`], runs it through an
//! apalis worker, and durably records the outcome as a [`JobCommand`] fenced on
//! the claim's [`ClaimId`]. See [ADR-0006](../../adrs/0006-cqrs-native-durable-jobs.md).
//!
//! Deployment is single-process, multiple in-process workers, one database.

use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use apalis_core::backend::Backend as ApalisBackend;
use apalis_core::error::BoxDynError;
use apalis_core::task::Task;
use apalis_core::task::data::Data;
use apalis_core::task::task_id::TaskId;
use apalis_core::worker::context::WorkerContext;
use chrono::{DateTime, TimeDelta, Utc};
use cqrs_es::AggregateError;
use futures_util::future::BoxFuture;
use futures_util::stream::{self, BoxStream};
use futures_util::{FutureExt, StreamExt};
use sqlx::SqlitePool;
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;
use tower::limit::ConcurrencyLimitLayer;
use tower_layer::{Layer, Stack};
use tower_service::Service;
use tracing::{error, warn};

use sqlite_es::{Cmp, Order, Predicate, Term, Value};

use crate::job::{
    ClaimId, DeadReason, Job, JobCommand, JobError, JobState, JobStoreError, WonClaim, WorkerId,
    enqueue_request, enqueued_event, pending_seed_payload, plan_claim,
};
use crate::job_sqlite::SqliteBackend;
use crate::job_store::{ClaimOutcome, EventBackend, LeaseRenewal};
use crate::projection::Projection;
use crate::{LifecycleError, ReconcileError, Store, StoreBuilder};

/// The durable-jobs runtime over one [`EventBackend`].
///
/// Holds the job [`Store`] (cqrs/es) and the `job_queue` [`Projection`], built
/// once per process. Cheap to clone (the store and projection are `Arc`); one
/// runtime is shared by every job type's worker.
pub struct JobRuntime<Backend: EventBackend = SqliteBackend> {
    backend: Backend,
    jobs: Arc<Store<JobState, Backend>>,
    queue: Arc<Projection<JobState>>,
}

impl<Backend: EventBackend> Clone for JobRuntime<Backend> {
    fn clone(&self) -> Self {
        Self {
            backend: self.backend.clone(),
            jobs: self.jobs.clone(),
            queue: self.queue.clone(),
        }
    }
}

impl<Backend: EventBackend> JobRuntime<Backend> {
    /// Standalone durable enqueue (ADR-0007): append `job`'s `Enqueued` event and
    /// seed its pending `job_queue` row in the backend's own transaction. This is
    /// the reactor / poller / job-chain / startup enqueue path -- there is no
    /// command commit for the job to ride. Returns the new job's id.
    ///
    /// Contrast the handler-side [`JobQueue::push`](crate::JobQueue::push), which
    /// buffers into the command's commit so the job is atomic with the triggering
    /// events. Use that for command-born jobs; use this for everything else. A
    /// standalone enqueue is its own transaction -- NOT atomic with whatever event
    /// or poll prompted it (that has already committed).
    pub async fn enqueue<J: Job>(&self, job: J) -> Result<String, JobEnqueueError<Backend::Error>> {
        self.enqueue_resolved(job, None).await
    }

    /// Like [`enqueue`](Self::enqueue) but the job becomes runnable only after
    /// `delay` from now -- for self-rescheduling pollers and deferred work.
    pub async fn enqueue_with_delay<J: Job>(
        &self,
        job: J,
        delay: std::time::Duration,
    ) -> Result<String, JobEnqueueError<Backend::Error>> {
        self.enqueue_resolved(job, Some(delay)).await
    }

    // Takes `job` by value (not `&J`): an owned `Job` is `Send` and so the future
    // is `Send`, whereas a `&J` held across the await would demand `J: Sync`.
    async fn enqueue_resolved<J: Job>(
        &self,
        job: J,
        delay: Option<std::time::Duration>,
    ) -> Result<String, JobEnqueueError<Backend::Error>> {
        let request = enqueue_request(&job, delay)?;
        let event = enqueued_event(&request)?;
        let payload = pending_seed_payload(&request)?;
        self.backend
            .enqueue(event, payload)
            .await
            .map_err(JobEnqueueError::Backend)?;
        Ok(request.job_id)
    }
}

/// Why a standalone [`JobRuntime::enqueue`] failed.
#[derive(Debug, thiserror::Error)]
pub enum JobEnqueueError<BackendError> {
    /// The `Enqueued` event or seed payload could not be built (es-side encoding).
    #[error("failed to build the job enqueue payload")]
    Build(#[from] JobStoreError),
    /// The backend rejected the append + seed transaction (e.g. a reused job id).
    #[error("the job backend rejected the enqueue")]
    Backend(#[source] BackendError),
}

impl JobRuntime<SqliteBackend> {
    /// Wires the SQLite job runtime over `pool`: the job `Store` + `job_queue`
    /// projection. The canonical schema (events + snapshots + `job_queue`) must
    /// already be applied -- like [`StoreBuilder`], the consumer owns migrations,
    /// so this composes with the consumer's own view migrations instead of
    /// re-running event-sorcery's and conflicting. `pool` MUST satisfy the pool
    /// contract (WAL, `busy_timeout>=5000ms`, `synchronous=FULL`); see
    /// [`SqliteBackend`].
    pub async fn build(pool: SqlitePool) -> Result<Self, ReconcileError> {
        let backend = SqliteBackend::new(pool);
        let (jobs, queue) = StoreBuilder::<JobState>::with_backend(backend.clone())
            .build()
            .await?;
        Ok(Self {
            backend,
            jobs,
            queue,
        })
    }
}

/// The event-sourced job backend for a single job type `J` over an
/// [`EventBackend`].
pub struct EventStoreBackend<J: Job, Backend: EventBackend = SqliteBackend> {
    runtime: JobRuntime<Backend>,
    worker_id: WorkerId,
    config: JobWorkerConfig,
    clock: Clock,
    _job: PhantomData<fn() -> J>,
}

impl<J: Job, Backend: EventBackend> EventStoreBackend<J, Backend> {
    /// Builds a worker for job type `J` over `runtime`.
    #[must_use]
    pub fn new(
        runtime: JobRuntime<Backend>,
        worker_name: &str,
        config: JobWorkerConfig,
        clock: Clock,
    ) -> Self {
        Self {
            runtime,
            worker_id: WorkerId::new(worker_name),
            config,
            clock,
            _job: PhantomData,
        }
    }

    async fn claim_one(
        &self,
        job_id: &str,
        now_ms: i64,
    ) -> Result<ClaimOutcome<WonClaim>, Backend::Error> {
        let worker = &self.worker_id;
        let lease_ms = lease_millis(self.config.lease_duration);
        let max_claims = self.config.max_claims;
        self.runtime
            .backend
            .claim(job_id, |read| {
                plan_claim(job_id, read, worker, now_ms, lease_ms, max_claims)
            })
            .await
    }
}

/// Decodes a won claim into an apalis task, or `None` if the stored args no
/// longer deserialize into `J` (a poison payload the worker skips).
fn build_task<J: Job>(job_id: String, won: WonClaim) -> Option<Task<J, JobContext, String>> {
    let job: J = match serde_json::from_value(won.args) {
        Ok(job) => job,
        Err(error) => {
            error!(target: "cqrs", ?error, job_id, "claimed job failed to decode; skipping");
            return None;
        }
    };
    let mut task = Task::new_with_ctx(
        job,
        JobContext {
            job_id: job_id.clone(),
            claim_seq: won.claim_seq,
            claim_id: won.claim_id,
            attempt: won.attempt,
        },
    );
    task.parts.task_id = Some(TaskId::new(job_id));
    Some(task)
}

/// The apalis task handler: run a decoded job against its injected input.
///
/// The worker's middleware (claim, lease renewal, fenced ack) wraps this; the
/// handler itself only performs the side effect. Used by
/// [`build_supervised_worker!`](crate::build_supervised_worker).
pub async fn run_job<J: Job>(job: J, input: Data<Arc<J::Input>>) -> Result<J::Output, J::Error> {
    job.perform(&input).await
}

impl<J: Job, Backend: EventBackend> ApalisBackend for EventStoreBackend<J, Backend> {
    type Args = J;
    type IdType = String;
    type Context = JobContext;
    type Error = BoxDynError;
    type Stream = BoxStream<'static, Result<Option<Task<J, JobContext, String>>, BoxDynError>>;
    type Beat = BoxStream<'static, Result<(), BoxDynError>>;
    type Layer = Stack<AckLayer<J, Backend>, ConcurrencyLimitLayer>;

    fn heartbeat(&self, _worker: &WorkerContext) -> Self::Beat {
        let interval = self.config.poll_interval;
        stream::unfold((), move |()| async move {
            sleep(interval).await;
            Some((Ok(()), ()))
        })
        .boxed()
    }

    fn middleware(&self) -> Self::Layer {
        Stack::new(
            AckLayer {
                jobs: self.runtime.jobs.clone(),
                backend: self.runtime.backend.clone(),
                config: self.config.clone(),
                clock: self.clock.clone(),
                _job: PhantomData,
            },
            ConcurrencyLimitLayer::new(self.config.max_concurrency),
        )
    }

    fn poll(self, _worker: &WorkerContext) -> Self::Stream {
        stream::unfold(self, |backend| async move {
            let now_ms = backend.clock.now().timestamp_millis();
            let predicate = poll_predicate(J::KIND, now_ms);
            let order = poll_order();
            let candidates = match backend
                .runtime
                .queue
                .find(&predicate, Some(&order), backend.config.scan_limit)
                .await
            {
                Ok(ids) => ids,
                Err(error) => {
                    warn!(target: "cqrs", ?error, kind = J::KIND, "job poll failed; idling");
                    sleep(backend.config.poll_interval).await;
                    return Some((Ok(None), backend));
                }
            };

            for job_id in candidates {
                match backend.claim_one(&job_id, now_ms).await {
                    Ok(ClaimOutcome::Won(won)) => {
                        if let Some(task) = build_task::<J>(job_id, won) {
                            return Some((Ok(Some(task)), backend));
                        }
                    }
                    // An abandoned dead-letter freed the slot; contended/skip move on.
                    Ok(ClaimOutcome::Abandoned | ClaimOutcome::Contended | ClaimOutcome::Skip) => {}
                    Err(error) => {
                        warn!(target: "cqrs", ?error, "job claim error; idling");
                        break;
                    }
                }
            }

            sleep(backend.config.poll_interval).await;
            Some((Ok(None), backend))
        })
        .boxed()
    }
}

/// Tuning for a job worker. Defaults target a single-host bot with sub-second
/// pickup; see the spec for the rationale behind each bound.
#[derive(Clone, Debug)]
pub struct JobWorkerConfig {
    /// Idle poll cadence.
    pub poll_interval: Duration,
    /// Candidates examined per poll tick.
    pub scan_limit: i64,
    /// Max jobs executing at once (mandatory backpressure that gates claiming).
    pub max_concurrency: usize,
    /// How long a claim is valid before another worker may steal it.
    pub lease_duration: Duration,
    /// How often a live worker renews its lease (well below `lease_duration`).
    pub renew_interval: Duration,
    /// Hard cap on a single `perform` invocation.
    pub execution_timeout: Duration,
    /// Recorded-failure budget before a job is dead-lettered.
    pub max_attempts: u32,
    /// Lifetime-claim ceiling (much larger than `max_attempts`) bounding a
    /// crash/hang loop that never records an outcome.
    pub max_claims: u32,
    /// Exponential backoff for retries.
    pub backoff: Backoff,
}

impl Default for JobWorkerConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_millis(250),
            scan_limit: 16,
            max_concurrency: 8,
            lease_duration: Duration::from_secs(30),
            renew_interval: Duration::from_secs(10),
            execution_timeout: Duration::from_secs(300),
            max_attempts: 5,
            max_claims: 50,
            backoff: Backoff::default(),
        }
    }
}

/// Exponential backoff with a cap, indexed on the failed-attempt count.
#[derive(Clone, Debug)]
pub struct Backoff {
    /// Delay for the first retry.
    pub base: Duration,
    /// Per-attempt multiplier.
    pub factor: f64,
    /// Maximum delay.
    pub cap: Duration,
}

impl Default for Backoff {
    fn default() -> Self {
        Self {
            base: Duration::from_secs(1),
            factor: 2.0,
            cap: Duration::from_secs(300),
        }
    }
}

impl Backoff {
    fn delay(&self, attempt: u32) -> Duration {
        let scaled = self.base.as_secs_f64() * self.factor.powf(f64::from(attempt.min(32)));
        Duration::from_secs_f64(scaled.min(self.cap.as_secs_f64()))
    }
}

/// Injectable wall clock, so tests drive lease/retry timing deterministically.
#[derive(Clone)]
pub struct Clock(Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>);

impl Clock {
    /// The real system clock.
    #[must_use]
    pub fn system() -> Self {
        Self(Arc::new(Utc::now))
    }

    /// A fixed-function clock for tests.
    #[must_use]
    pub fn from_fn(now: impl Fn() -> DateTime<Utc> + Send + Sync + 'static) -> Self {
        Self(Arc::new(now))
    }

    fn now(&self) -> DateTime<Utc> {
        let Self(now) = self;
        now()
    }
}

impl std::fmt::Debug for Clock {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Clock")
    }
}

/// Per-task context captured at claim time; the ack reads it from `parts.ctx`.
#[derive(Clone, Default)]
pub struct JobContext {
    job_id: String,
    /// Sequence of the `Claimed` event this worker appended; the renew keys on it.
    claim_seq: i64,
    /// Identity of the claim that produced this run; the ack fences on it.
    claim_id: ClaimId,
    /// Recorded failures so far (0 on first run), from the fold.
    attempt: u32,
}

fn poll_predicate(kind: &str, now_ms: i64) -> Predicate {
    let kind_eq = || Term {
        column: "kind".to_string(),
        cmp: Cmp::Eq,
        value: Some(Value::Text(kind.to_string())),
    };
    let status_eq = |status: &str| Term {
        column: "status".to_string(),
        cmp: Cmp::Eq,
        value: Some(Value::Text(status.to_string())),
    };
    Predicate {
        any_of: vec![
            // Pending and due.
            vec![
                kind_eq(),
                status_eq("pending"),
                Term {
                    column: "run_at".to_string(),
                    cmp: Cmp::Le,
                    value: Some(Value::Int(now_ms)),
                },
            ],
            // Claimed with an expired lease.
            vec![
                kind_eq(),
                status_eq("claimed"),
                Term {
                    column: "lease_until".to_string(),
                    cmp: Cmp::Lt,
                    value: Some(Value::Int(now_ms)),
                },
            ],
            // Claimed but the lease column is NULL (a rebuilt-but-unclaimed row).
            vec![
                kind_eq(),
                status_eq("claimed"),
                Term {
                    column: "lease_until".to_string(),
                    cmp: Cmp::IsNull,
                    value: None,
                },
            ],
        ],
    }
}

fn poll_order() -> Order {
    Order {
        column: "run_at".to_string(),
        ascending: true,
    }
}

/// Layer that wraps the worker's execution service to durably record each job's
/// outcome and hold its lease for the duration of the run.
pub struct AckLayer<J: Job, Backend: EventBackend> {
    jobs: Arc<Store<JobState, Backend>>,
    backend: Backend,
    config: JobWorkerConfig,
    clock: Clock,
    _job: PhantomData<fn() -> J>,
}

impl<S, J: Job, Backend: EventBackend> Layer<S> for AckLayer<J, Backend> {
    type Service = AckService<S, J, Backend>;

    fn layer(&self, inner: S) -> Self::Service {
        AckService {
            inner,
            jobs: self.jobs.clone(),
            backend: self.backend.clone(),
            config: self.config.clone(),
            clock: self.clock.clone(),
            _job: PhantomData,
        }
    }
}

/// See [`AckLayer`].
pub struct AckService<S, J: Job, Backend: EventBackend> {
    inner: S,
    jobs: Arc<Store<JobState, Backend>>,
    backend: Backend,
    config: JobWorkerConfig,
    clock: Clock,
    _job: PhantomData<fn() -> J>,
}

impl<S, J, Backend> Service<Task<J, JobContext, String>> for AckService<S, J, Backend>
where
    J: Job,
    Backend: EventBackend,
    S: Service<Task<J, JobContext, String>, Response = J::Output>,
    S::Error: Into<BoxDynError> + Send + Sync + 'static,
    S::Future: Send + 'static,
{
    type Response = J::Output;
    type Error = BoxDynError;
    type Future = BoxFuture<'static, Result<J::Output, BoxDynError>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, task: Task<J, JobContext, String>) -> Self::Future {
        let ctx = task.parts.ctx.clone();
        let jobs = self.jobs.clone();
        let backend = self.backend.clone();
        let config = self.config.clone();
        let clock = self.clock.clone();
        let run = self.inner.call(task);

        async move {
            let lost = Arc::new(AtomicBool::new(false));
            let cancel = CancellationToken::new();
            let renew = tokio::spawn(renew_loop(
                backend,
                config.clone(),
                clock.clone(),
                ctx.clone(),
                cancel.clone(),
                lost.clone(),
            ));

            // Normalize to a single boxed-error result up front (the inner stack's
            // error type is apalis's BoxDynError, not a concrete Error), so the
            // ack and the returned future share one type.
            let result: Result<J::Output, BoxDynError> =
                match timeout(config.execution_timeout, run).await {
                    Ok(Ok(output)) => Ok(output),
                    Ok(Err(error)) => Err(error.into()),
                    Err(_elapsed) => Err(format!(
                        "job execution exceeded {:?}",
                        config.execution_timeout
                    )
                    .into()),
                };
            let (succeeded, error_text) = match &result {
                Ok(_) => (true, String::new()),
                Err(error) => (false, error_chain(&**error)),
            };

            if lost.load(Ordering::Relaxed) {
                warn!(target: "cqrs", job_id = %ctx.job_id, "lease lost during execution; re-claimer owns the outcome");
            } else {
                ack(&jobs, &config, &clock, &ctx, succeeded, error_text).await;
            }

            cancel.cancel();
            let _ = renew.await;

            result
        }
        .boxed()
    }
}

fn error_chain(error: &(dyn std::error::Error + 'static)) -> String {
    let mut message = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        message.push_str(": ");
        message.push_str(&cause.to_string());
        source = cause.source();
    }
    message
}

/// Renews the lease as a projection-only `UPDATE` keyed on the fixed
/// `claim_seq`, never advancing the event stream. Once the ack advances the row,
/// the renewal matches zero rows and stops -- so a late tick can never resurrect
/// a completed job.
async fn renew_loop<Backend: EventBackend>(
    backend: Backend,
    config: JobWorkerConfig,
    clock: Clock,
    ctx: JobContext,
    cancel: CancellationToken,
    lost: Arc<AtomicBool>,
) {
    loop {
        tokio::select! {
            () = cancel.cancelled() => break,
            () = sleep(config.renew_interval) => {
                let new_lease = clock.now() + lease_delta(config.lease_duration);
                match backend.renew(&ctx.job_id, ctx.claim_seq, new_lease.timestamp_millis()).await {
                    Ok(LeaseRenewal::Lost) => {
                        error!(target: "cqrs", job_id = %ctx.job_id, claim_seq = ctx.claim_seq, "lease lost; stopping renewal");
                        lost.store(true, Ordering::Relaxed);
                        break;
                    }
                    Ok(LeaseRenewal::Held) => {}
                    Err(error) => {
                        warn!(target: "cqrs", ?error, "lease renewal transient error");
                    }
                }
            }
        }
    }
}

/// Acks the outcome by sending a fenced [`JobCommand`], retrying transient errors
/// while the lease is held. The ack is fenced two ways: the command's `claim_id`
/// (a re-claimer changed the live claim, so `transition` returns
/// [`JobError::Fenced`]) and, as a backstop, the events UNIQUE (a concurrent ack
/// took the next sequence -> [`AggregateError::AggregateConflict`]). Either fence
/// on a *successful* job is a double-execution alarm.
async fn ack<Backend: EventBackend>(
    jobs: &Store<JobState, Backend>,
    config: &JobWorkerConfig,
    clock: &Clock,
    ctx: &JobContext,
    succeeded: bool,
    error_text: String,
) {
    let command = ack_command(config, clock, ctx, succeeded, error_text);

    loop {
        match jobs.send(&ctx.job_id, command.clone()).await {
            Ok(()) => return,
            Err(
                AggregateError::UserError(LifecycleError::Apply(JobError::Fenced))
                | AggregateError::AggregateConflict,
            ) => {
                if succeeded {
                    error!(target: "cqrs", job_id = %ctx.job_id, "DOUBLE-EXECUTION RISK: job succeeded but a re-claimer owns the outcome");
                } else {
                    warn!(target: "cqrs", job_id = %ctx.job_id, "ack fenced; a re-claimer owns the outcome");
                }
                return;
            }
            Err(AggregateError::UserError(domain_error)) => {
                error!(target: "cqrs", job_id = %ctx.job_id, ?domain_error, "ack hit a non-fence lifecycle error; the aggregate is failed");
                return;
            }
            Err(AggregateError::DatabaseConnectionError(error)) => {
                warn!(target: "cqrs", ?error, job_id = %ctx.job_id, "ack transient error; retrying within lease");
                sleep(Duration::from_millis(50)).await;
            }
            Err(AggregateError::DeserializationError(error)) => {
                error!(target: "cqrs", ?error, job_id = %ctx.job_id, "ack deserialization error; giving up");
                return;
            }
            Err(AggregateError::UnexpectedError(error)) => {
                error!(target: "cqrs", ?error, job_id = %ctx.job_id, "ack unexpected error; giving up");
                return;
            }
        }
    }
}

fn ack_command(
    config: &JobWorkerConfig,
    clock: &Clock,
    ctx: &JobContext,
    succeeded: bool,
    error_text: String,
) -> JobCommand {
    if succeeded {
        return JobCommand::Succeed {
            claim_id: ctx.claim_id.clone(),
        };
    }
    let failed = ctx.attempt + 1;
    if failed >= config.max_attempts {
        return JobCommand::Kill {
            claim_id: ctx.claim_id.clone(),
            reason: DeadReason::RetriesExhausted,
            error: error_text,
        };
    }
    let run_at = clock.now() + lease_delta(config.backoff.delay(ctx.attempt));
    JobCommand::RetrySchedule {
        claim_id: ctx.claim_id.clone(),
        run_at,
        attempt: failed,
        error: error_text,
    }
}

fn lease_delta(duration: Duration) -> TimeDelta {
    TimeDelta::from_std(duration).unwrap_or_else(|_| TimeDelta::seconds(30))
}

fn lease_millis(duration: Duration) -> i64 {
    i64::try_from(duration.as_millis()).unwrap_or(30_000)
}

/// Wire a supervised apalis [`Monitor`](crate::__worker::Monitor) with one worker
/// per job type over a shared [`JobRuntime`].
///
/// Each entry is `JobType => input_expr`, where `input_expr` builds that job's
/// [`Job::Input`] dependency bundle. The macro builds an [`EventStoreBackend`]
/// per job (named `"{WORKER_NAME}-{index}"`, supervised/restartable) and returns
/// the `Monitor`; call `.run().await` to start processing.
///
/// ```ignore
/// let runtime = JobRuntime::build(pool).await?;
/// let monitor = build_supervised_worker!(runtime, JobWorkerConfig::default(), Clock::system(), {
///     SendEmail => email_client.clone(),
///     ChargeCard => billing_client.clone(),
/// });
/// monitor.run().await?;
/// ```
#[macro_export]
macro_rules! build_supervised_worker {
    ($runtime:expr, $config:expr, $clock:expr, { $($job:ty => $input:expr),+ $(,)? }) => {{
        let runtime = $runtime;
        let config = $config;
        let clock = $clock;
        let monitor = $crate::__worker::Monitor::new();
        $(
            let monitor = {
                let runtime = ::std::clone::Clone::clone(&runtime);
                let config = ::std::clone::Clone::clone(&config);
                let clock = ::std::clone::Clone::clone(&clock);
                let input = ::std::sync::Arc::new($input);
                monitor.register(move |index| {
                    let name = ::std::format!(
                        "{}-{}",
                        <$job as $crate::Job>::WORKER_NAME,
                        index
                    );
                    let backend = $crate::EventStoreBackend::<$job>::new(
                        ::std::clone::Clone::clone(&runtime),
                        &name,
                        ::std::clone::Clone::clone(&config),
                        ::std::clone::Clone::clone(&clock),
                    );
                    $crate::__worker::WorkerBuilder::new(name)
                        .backend(backend)
                        .data(::std::clone::Clone::clone(&input))
                        .build($crate::run_job::<$job>)
                })
            };
        )+
        monitor
    }};
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};
    use sqlx::SqlitePool;
    use sqlx::sqlite::SqlitePoolOptions;
    use ulid::Ulid;

    use sqlite_es::insert_serialized_events_batch;

    use crate::job::{EnqueueRequest, JobKind, Label, enqueued_event, pending_seed_payload};

    use super::*;

    #[derive(Serialize, Deserialize)]
    struct TestJob {
        n: u32,
    }

    impl Job for TestJob {
        type Input = ();
        type Output = ();
        type Error = std::convert::Infallible;

        const WORKER_NAME: &'static str = "test-worker";
        const KIND: &'static str = "test-job";

        fn label(&self) -> Label {
            Label::new(format!("test:{}", self.n))
        }

        async fn perform(&self, _input: &()) -> Result<(), std::convert::Infallible> {
            Ok(())
        }
    }

    async fn one_db_pool() -> SqlitePool {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("connect");
        sqlx::migrate!("../../migrations")
            .run(&pool)
            .await
            .expect("migrate");
        pool
    }

    fn from_millis(millis: i64) -> DateTime<Utc> {
        DateTime::from_timestamp_millis(millis).expect("valid timestamp")
    }

    /// Seeds a job exactly as the enqueue flush does: the `Enqueued` event at
    /// sequence 1 plus the seed `job_queue` row (version 1, pending, no lease).
    async fn enqueue(pool: &SqlitePool, run_at_ms: i64) -> String {
        let request = EnqueueRequest {
            job_id: Ulid::new().to_string(),
            kind: JobKind::new(TestJob::KIND),
            payload: serde_json::to_value(TestJob { n: 1 }).unwrap(),
            run_at_ms,
        };
        let event = enqueued_event(&request).unwrap();
        let payload = pending_seed_payload(&request).unwrap();
        let mut connection = pool.acquire().await.unwrap();
        insert_serialized_events_batch(&mut connection, "events", std::slice::from_ref(&event))
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO job_queue (view_id, version, payload, lease_until) VALUES (?1, 1, ?2, NULL)",
        )
        .bind(&request.job_id)
        .bind(&payload)
        .execute(&mut *connection)
        .await
        .unwrap();
        request.job_id
    }

    async fn claim(
        backend: &SqliteBackend,
        job_id: &str,
        worker: &str,
        now_ms: i64,
        max_claims: u32,
    ) -> ClaimOutcome<WonClaim> {
        let worker = WorkerId::new(worker);
        backend
            .claim(job_id, |read| {
                plan_claim(job_id, read, &worker, now_ms, 30_000, max_claims)
            })
            .await
            .unwrap()
    }

    async fn event_types(pool: &SqlitePool, job_id: &str) -> Vec<String> {
        sqlx::query_scalar::<_, String>(
            "SELECT event_type FROM events WHERE aggregate_type = 'job' AND aggregate_id = ?1 \
             ORDER BY sequence",
        )
        .bind(job_id)
        .fetch_all(pool)
        .await
        .unwrap()
    }

    async fn queue_status(pool: &SqlitePool, job_id: &str) -> Option<String> {
        sqlx::query_scalar::<_, Option<String>>("SELECT status FROM job_queue WHERE view_id = ?1")
            .bind(job_id)
            .fetch_optional(pool)
            .await
            .unwrap()
            .flatten()
    }

    #[tokio::test]
    async fn claim_appends_claimed_and_marks_the_row() {
        let pool = one_db_pool().await;
        let backend = SqliteBackend::new(pool.clone());
        let job_id = enqueue(&pool, 1000).await;

        let outcome = claim(&backend, &job_id, "w1", 1000, 50).await;

        assert!(matches!(
            outcome,
            ClaimOutcome::Won(WonClaim { claim_seq: 2, .. })
        ));
        assert_eq!(
            event_types(&pool, &job_id).await,
            ["JobEnqueued", "JobClaimed"]
        );
        assert_eq!(
            queue_status(&pool, &job_id).await.as_deref(),
            Some("claimed")
        );
    }

    #[tokio::test]
    async fn standalone_enqueue_seeds_a_claimable_pending_job() {
        let pool = one_db_pool().await;
        let runtime = JobRuntime::build(pool.clone()).await.unwrap();

        let job_id = runtime.enqueue(TestJob { n: 7 }).await.unwrap();

        assert_eq!(event_types(&pool, &job_id).await, ["JobEnqueued"]);
        assert_eq!(
            queue_status(&pool, &job_id).await.as_deref(),
            Some("pending")
        );

        // A standalone-enqueued job is claimable like any handler-pushed one.
        let backend = SqliteBackend::new(pool.clone());
        let now_ms = Utc::now().timestamp_millis() + 1_000;
        let outcome = claim(&backend, &job_id, "w1", now_ms, 50).await;
        assert!(matches!(outcome, ClaimOutcome::Won(_)));
        assert_eq!(
            event_types(&pool, &job_id).await,
            ["JobEnqueued", "JobClaimed"]
        );
    }

    #[tokio::test]
    async fn standalone_enqueue_with_delay_is_not_runnable_until_run_at() {
        let pool = one_db_pool().await;
        let runtime = JobRuntime::build(pool.clone()).await.unwrap();

        let job_id = runtime
            .enqueue_with_delay(TestJob { n: 1 }, std::time::Duration::from_secs(3600))
            .await
            .unwrap();

        assert_eq!(
            queue_status(&pool, &job_id).await.as_deref(),
            Some("pending")
        );

        // run_at is an hour out, so a poll at "now" finds nothing runnable.
        let projection = Projection::<JobState>::sqlite(pool.clone());
        let now_ms = Utc::now().timestamp_millis();
        let found = projection
            .find(
                &poll_predicate(TestJob::KIND, now_ms),
                Some(&poll_order()),
                16,
            )
            .await
            .unwrap();
        assert!(
            found.is_empty(),
            "a delayed standalone enqueue must not be runnable before its run_at"
        );
    }

    #[tokio::test]
    async fn enqueue_rejects_a_reused_job_id() {
        let pool = one_db_pool().await;
        let backend = SqliteBackend::new(pool.clone());

        let request = EnqueueRequest {
            job_id: Ulid::new().to_string(),
            kind: JobKind::new(TestJob::KIND),
            payload: serde_json::to_value(TestJob { n: 1 }).unwrap(),
            run_at_ms: 1000,
        };
        let payload = pending_seed_payload(&request).unwrap();

        backend
            .enqueue(enqueued_event(&request).unwrap(), payload.clone())
            .await
            .unwrap();
        let error = backend
            .enqueue(enqueued_event(&request).unwrap(), payload)
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            crate::job_sqlite::SqliteJobError::DuplicateEnqueue { job_id } if job_id == request.job_id
        ));
    }

    #[tokio::test]
    async fn second_sequential_claim_is_not_runnable() {
        let pool = one_db_pool().await;
        let backend = SqliteBackend::new(pool.clone());
        let job_id = enqueue(&pool, 1000).await;

        let first = claim(&backend, &job_id, "w1", 1000, 50).await;
        let second = claim(&backend, &job_id, "w2", 1000, 50).await;

        assert!(matches!(first, ClaimOutcome::Won(_)));
        assert!(matches!(second, ClaimOutcome::Skip));
        let claimed = event_types(&pool, &job_id)
            .await
            .iter()
            .filter(|event| *event == "JobClaimed")
            .count();
        assert_eq!(claimed, 1);
    }

    #[tokio::test]
    async fn claimed_job_is_not_runnable_before_its_lease_expires() {
        let pool = one_db_pool().await;
        let backend = SqliteBackend::new(pool.clone());
        let job_id = enqueue(&pool, 1000).await;
        claim(&backend, &job_id, "w1", 1000, 50).await;

        let projection = Projection::<JobState>::sqlite(pool.clone());
        let found = projection
            .find(
                &poll_predicate(TestJob::KIND, 1000 + 5_000),
                Some(&poll_order()),
                16,
            )
            .await
            .unwrap();
        assert!(found.is_empty());
    }

    #[tokio::test]
    async fn expired_lease_is_reclaimable_without_counting_an_attempt() {
        let pool = one_db_pool().await;
        let backend = SqliteBackend::new(pool.clone());
        let job_id = enqueue(&pool, 1000).await;
        claim(&backend, &job_id, "w1", 1000, 50).await;

        let later = 1000 + 31_000;
        let outcome = claim(&backend, &job_id, "w2", later, 50).await;

        assert!(matches!(
            outcome,
            ClaimOutcome::Won(WonClaim {
                claim_seq: 3,
                attempt: 0,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn ack_retry_then_kill_via_commands() {
        let pool = one_db_pool().await;
        let runtime = JobRuntime::build(pool.clone()).await.unwrap();
        let job_id = enqueue(&pool, 1000).await;

        let ClaimOutcome::Won(won) = claim(&runtime.backend, &job_id, "w1", 1000, 50).await else {
            panic!("expected claim");
        };
        runtime
            .jobs
            .send(
                &job_id,
                JobCommand::RetrySchedule {
                    claim_id: won.claim_id,
                    run_at: from_millis(2000),
                    attempt: 1,
                    error: "boom".to_string(),
                },
            )
            .await
            .unwrap();
        assert_eq!(
            event_types(&pool, &job_id).await,
            ["JobEnqueued", "JobClaimed", "JobRetryScheduled"]
        );
        assert_eq!(
            queue_status(&pool, &job_id).await.as_deref(),
            Some("pending")
        );

        // Re-claim the now-pending job and kill it.
        let ClaimOutcome::Won(won) = claim(&runtime.backend, &job_id, "w1", 2000, 50).await else {
            panic!("expected re-claim");
        };
        runtime
            .jobs
            .send(
                &job_id,
                JobCommand::Kill {
                    claim_id: won.claim_id,
                    reason: DeadReason::RetriesExhausted,
                    error: "boom".to_string(),
                },
            )
            .await
            .unwrap();
        assert_eq!(
            event_types(&pool, &job_id).await,
            [
                "JobEnqueued",
                "JobClaimed",
                "JobRetryScheduled",
                "JobClaimed",
                "JobDead"
            ]
        );
        // Terminal rows are retained (status 'dead'), not deleted.
        assert_eq!(queue_status(&pool, &job_id).await.as_deref(), Some("dead"));
    }

    #[tokio::test]
    async fn ack_is_fenced_when_a_reclaimer_holds_the_job() {
        let pool = one_db_pool().await;
        let runtime = JobRuntime::build(pool.clone()).await.unwrap();
        let job_id = enqueue(&pool, 1000).await;

        // w1 claims.
        let ClaimOutcome::Won(stale) = claim(&runtime.backend, &job_id, "w1", 1000, 50).await
        else {
            panic!("expected claim");
        };
        // The lease expires and w2 re-claims, minting a new claim_id.
        let ClaimOutcome::Won(_) = claim(&runtime.backend, &job_id, "w2", 1000 + 31_000, 50).await
        else {
            panic!("expected re-claim");
        };

        // w1's ack carries its stale claim_id -> fenced, no event written.
        let fenced = runtime
            .jobs
            .send(
                &job_id,
                JobCommand::Succeed {
                    claim_id: stale.claim_id,
                },
            )
            .await;
        assert!(matches!(
            fenced,
            Err(AggregateError::UserError(LifecycleError::Apply(
                JobError::Fenced
            )))
        ));
        assert_eq!(
            event_types(&pool, &job_id).await,
            ["JobEnqueued", "JobClaimed", "JobClaimed"]
        );
    }

    #[tokio::test]
    async fn claim_budget_dead_letters_a_crash_loop() {
        let pool = one_db_pool().await;
        let backend = SqliteBackend::new(pool.clone());
        let job_id = enqueue(&pool, 1000).await;

        // max_claims = 2: claim twice (advancing past each lease), the third
        // claim exhausts the budget and dead-letters as abandoned.
        let mut now = 1000;
        for _ in 0..2 {
            let outcome = claim(&backend, &job_id, "w1", now, 2).await;
            assert!(matches!(outcome, ClaimOutcome::Won(_)));
            now += 31_000;
        }

        let outcome = claim(&backend, &job_id, "w1", now, 2).await;
        assert!(matches!(outcome, ClaimOutcome::Abandoned));
        assert_eq!(queue_status(&pool, &job_id).await.as_deref(), Some("dead"));
        assert!(
            event_types(&pool, &job_id)
                .await
                .contains(&"JobDead".to_string())
        );
    }

    #[tokio::test]
    async fn build_supervised_worker_wires_a_monitor() {
        let pool = one_db_pool().await;
        let runtime = JobRuntime::build(pool).await.unwrap();
        // Build (but do not run) a supervised monitor: this type-checks the whole
        // worker wiring -- EventStoreBackend + its claim/ack middleware +
        // WorkerBuilder + run_job + Monitor::register's bounds all line up.
        let _monitor = crate::build_supervised_worker!(
            runtime,
            JobWorkerConfig::default(),
            Clock::system(),
            { TestJob => () }
        );
    }
}
