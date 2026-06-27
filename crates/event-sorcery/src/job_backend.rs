//! Worker-side durable-job execution: a generic [`apalis_core::Backend`].
//!
//! [`EventStoreBackend`] polls a [`JobStore`] for runnable jobs, claims each by a
//! compare-and-swap event append, runs it through an apalis worker, and durably
//! records the outcome -- over any backend that implements [`JobStore`]
//! ([`SqliteBackend`] by default). See
//! [ADR-0005](../../adrs/0005-backend-agnostic-event-store.md) and
//! `docs/event-sourced-job-backend-spec.md` for the concurrency rationale.
//!
//! Deployment is single-process, multiple in-process workers, one database.

use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use apalis_core::backend::Backend;
use apalis_core::error::BoxDynError;
use apalis_core::task::Task;
use apalis_core::task::task_id::TaskId;
use apalis_core::worker::context::WorkerContext;
use chrono::{DateTime, TimeDelta, Utc};
use futures_util::future::BoxFuture;
use futures_util::stream::{self, BoxStream};
use futures_util::{FutureExt, StreamExt};
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;
use tower::limit::ConcurrencyLimitLayer;
use tower_layer::{Layer, Stack};
use tower_service::Service;
use tracing::{error, warn};

use crate::job::{DeadReason, Job, JobEvent, WorkerId};
use crate::job_sqlite::SqliteBackend;
use crate::job_store::{
    BackendError, Candidate, CasOutcome, JobRow, JobStore, LeaseRenewal, QueueRow, QueueStatus,
    Severity,
};

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
    pub max_claims: i64,
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
    kind: String,
    /// Sequence of the `Claimed` event this worker appended; the ack CAS targets
    /// `claim_seq + 1` and lease renewal keys on `claim_seq`.
    claim_seq: i64,
    /// Recorded failures so far (0 on first run), from the fold.
    attempt: u32,
}

/// The event-sourced job backend for a single job type over store `Q`.
pub struct EventStoreBackend<J: Job, Q: JobStore = SqliteBackend> {
    store: Q,
    worker_id: WorkerId,
    config: JobWorkerConfig,
    clock: Clock,
    _job: PhantomData<fn() -> J>,
}

impl<J: Job> EventStoreBackend<J, SqliteBackend> {
    /// Builds a SQLite-backed worker. `pool` MUST satisfy the pool contract
    /// (WAL, `busy_timeout>=5000ms`, `synchronous=FULL`); see [`SqliteBackend`].
    #[must_use]
    pub fn new(
        pool: sqlx::SqlitePool,
        worker_name: &str,
        config: JobWorkerConfig,
        clock: Clock,
    ) -> Self {
        Self::with_store(SqliteBackend::new(pool), worker_name, config, clock)
    }
}

impl<J: Job, Q: JobStore> EventStoreBackend<J, Q> {
    /// Builds a worker over an arbitrary [`JobStore`].
    #[must_use]
    pub fn with_store(store: Q, worker_name: &str, config: JobWorkerConfig, clock: Clock) -> Self {
        Self {
            store,
            worker_id: WorkerId::new(worker_name),
            config,
            clock,
            _job: PhantomData,
        }
    }

    async fn fetch_candidates(&self, now_ms: i64) -> Result<Vec<Candidate>, Q::Error> {
        self.store
            .fetch_candidates(J::KIND, now_ms, self.config.scan_limit)
            .await
    }

    /// Attempts to claim one candidate in a single write-locked transaction:
    /// re-read the head, re-validate runnable, enforce the claim budget, then
    /// compare-and-swap-append `Claimed` at the expected next sequence.
    async fn try_claim(&self, candidate: &Candidate, now_ms: i64) -> ClaimOutcome<Q::Error> {
        let mut tx = match self.store.begin_claim().await {
            Ok(tx) => tx,
            Err(error) => return ClaimOutcome::Transient(error),
        };

        let outcome = self.claim_in_txn(&mut tx, candidate, now_ms).await;

        match &outcome {
            ClaimOutcome::Won { .. } => {
                if let Err(error) = self.store.commit(tx).await {
                    return ClaimOutcome::Transient(error);
                }
            }
            _ => self.store.rollback(tx).await,
        }

        outcome
    }

    async fn claim_in_txn(
        &self,
        connection: &mut Q::Connection,
        candidate: &Candidate,
        now_ms: i64,
    ) -> ClaimOutcome<Q::Error> {
        let row = match self.store.read_head(connection, &candidate.job_id).await {
            Ok(Some(row)) => row,
            Ok(None) => return ClaimOutcome::Skip,
            Err(error) => return classify_claim::<Q>(&candidate.job_id, error),
        };

        if row.sequence != candidate.sequence || !runnable(&row, now_ms) {
            return ClaimOutcome::Skip;
        }

        let next_seq = row.sequence + 1;
        let Some(next_seq_usize) = sequence_to_usize(&candidate.job_id, next_seq) else {
            return ClaimOutcome::Skip;
        };

        // Stream length is 1 Enqueued + #Claimed + #RetryScheduled and
        // attempt == #RetryScheduled, so claims so far is sequence - 1 - attempt.
        let claims_so_far = row.sequence - 1 - i64::from(row.attempt);
        if claims_so_far >= self.config.max_claims {
            return self
                .dead_letter_abandoned(connection, candidate, &row, next_seq, next_seq_usize)
                .await;
        }

        let lease_until = self.clock.now() + lease_delta(self.config.lease_duration);
        let claimed = JobEvent::Claimed {
            worker: self.worker_id.clone(),
            lease_until,
        };
        let Ok(serialized) = claimed.serialized(&candidate.job_id, next_seq_usize) else {
            error!(target: "cqrs", job_id = %candidate.job_id, "failed to encode Claimed event");
            return ClaimOutcome::Skip;
        };
        match self.store.append_event(connection, &serialized).await {
            Ok(CasOutcome::Committed) => {}
            Ok(CasOutcome::Conflict) => return ClaimOutcome::Contended,
            Err(error) => return classify_claim::<Q>(&candidate.job_id, error),
        }

        let job_row = JobRow {
            kind: row.kind.clone(),
            status: QueueStatus::Claimed,
            run_at_ms: row.run_at_ms,
            lease_until_ms: Some(lease_until.timestamp_millis()),
            attempt: row.attempt,
            sequence: next_seq,
        };
        if let Err(error) = self
            .store
            .upsert_row(connection, &candidate.job_id, &job_row)
            .await
        {
            return classify_claim::<Q>(&candidate.job_id, error);
        }

        ClaimOutcome::Won {
            job_id: candidate.job_id.clone(),
            kind: row.kind,
            claim_seq: next_seq,
            attempt: row.attempt,
            abandoned: false,
        }
    }

    async fn dead_letter_abandoned(
        &self,
        connection: &mut Q::Connection,
        candidate: &Candidate,
        row: &QueueRow,
        next_seq: i64,
        next_seq_usize: usize,
    ) -> ClaimOutcome<Q::Error> {
        let event = JobEvent::Dead {
            reason: DeadReason::Abandoned,
            error: "claim budget exhausted".to_string(),
        };
        let Ok(serialized) = event.serialized(&candidate.job_id, next_seq_usize) else {
            error!(target: "cqrs", job_id = %candidate.job_id, "failed to encode Dead event");
            return ClaimOutcome::Skip;
        };
        match self.store.append_event(connection, &serialized).await {
            Ok(CasOutcome::Committed) => {}
            Ok(CasOutcome::Conflict) => return ClaimOutcome::Contended,
            Err(error) => return classify_claim::<Q>(&candidate.job_id, error),
        }
        if let Err(error) = self.store.delete_row(connection, &candidate.job_id).await {
            return classify_claim::<Q>(&candidate.job_id, error);
        }
        ClaimOutcome::Won {
            job_id: candidate.job_id.clone(),
            kind: row.kind.clone(),
            claim_seq: next_seq,
            attempt: row.attempt,
            abandoned: true,
        }
    }

    async fn build_task(
        &self,
        job_id: String,
        kind: String,
        claim_seq: i64,
        attempt: u32,
    ) -> Option<Task<J, JobContext, String>> {
        let job = match self.decode_job(&job_id).await {
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
                kind,
                claim_seq,
                attempt,
            },
        );
        task.parts.task_id = Some(TaskId::new(job_id));
        Some(task)
    }

    async fn decode_job(&self, job_id: &str) -> Result<J, BackendError<Q::Error>> {
        let value = self
            .store
            .load_enqueued_event(job_id)
            .await
            .map_err(BackendError::Backend)?;
        let event: JobEvent = serde_json::from_value(value).map_err(BackendError::Decode)?;
        let JobEvent::Enqueued { payload, .. } = event else {
            return Err(BackendError::OrphanEvent);
        };
        serde_json::from_value(payload).map_err(BackendError::Decode)
    }
}

fn runnable(row: &QueueRow, now_ms: i64) -> bool {
    match row.status {
        QueueStatus::Pending => row.run_at_ms <= now_ms,
        QueueStatus::Claimed => row.lease_until_ms.is_some_and(|lease| lease < now_ms),
    }
}

fn lease_delta(duration: Duration) -> TimeDelta {
    TimeDelta::from_std(duration).unwrap_or_else(|_| TimeDelta::seconds(30))
}

fn sequence_to_usize(job_id: &str, sequence: i64) -> Option<usize> {
    usize::try_from(sequence).map_or_else(
        |_| {
            error!(target: "cqrs", job_id, sequence, "job sequence out of range");
            None
        },
        Some,
    )
}

fn classify_claim<Q: JobStore>(job_id: &str, error: Q::Error) -> ClaimOutcome<Q::Error> {
    match Q::classify(&error) {
        Severity::Transient => ClaimOutcome::Transient(error),
        Severity::Fatal => {
            error!(target: "cqrs", job_id, ?error, "fatal job store error during claim; skipping");
            ClaimOutcome::Skip
        }
    }
}

enum ClaimOutcome<E> {
    /// Claimed (or dead-lettered as abandoned, which also frees the slot).
    Won {
        job_id: String,
        kind: String,
        claim_seq: i64,
        attempt: u32,
        abandoned: bool,
    },
    /// A concurrent worker won the compare-and-swap.
    Contended,
    /// No longer runnable, or a fatal error skips the candidate.
    Skip,
    /// A transient backend error; retry on the next tick.
    Transient(E),
}

impl<J: Job, Q: JobStore> Backend for EventStoreBackend<J, Q> {
    type Args = J;
    type IdType = String;
    type Context = JobContext;
    type Error = BackendError<Q::Error>;
    type Stream =
        BoxStream<'static, Result<Option<Task<J, JobContext, String>>, BackendError<Q::Error>>>;
    type Beat = BoxStream<'static, Result<(), BackendError<Q::Error>>>;
    type Layer = Stack<AckLayer<J, Q>, ConcurrencyLimitLayer>;

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
                store: self.store.clone(),
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
            let candidates = match backend.fetch_candidates(now_ms).await {
                Ok(candidates) => candidates,
                Err(error) => {
                    warn!(target: "cqrs", ?error, kind = J::KIND, "job poll failed; idling");
                    sleep(backend.config.poll_interval).await;
                    return Some((Ok(None), backend));
                }
            };

            for candidate in &candidates {
                match backend.try_claim(candidate, now_ms).await {
                    ClaimOutcome::Won {
                        job_id,
                        kind,
                        claim_seq,
                        attempt,
                        abandoned: false,
                    } => {
                        if let Some(task) =
                            backend.build_task(job_id, kind, claim_seq, attempt).await
                        {
                            return Some((Ok(Some(task)), backend));
                        }
                    }
                    ClaimOutcome::Transient(error) => {
                        warn!(target: "cqrs", ?error, "job claim transient error; idling");
                        break;
                    }
                    // An abandoned dead-letter freed the slot; contended/skip move on.
                    ClaimOutcome::Won {
                        abandoned: true, ..
                    }
                    | ClaimOutcome::Contended
                    | ClaimOutcome::Skip => {}
                }
            }

            sleep(backend.config.poll_interval).await;
            Some((Ok(None), backend))
        })
        .boxed()
    }
}

/// Layer that wraps the worker's execution service to durably record each job's
/// outcome and hold its lease for the duration of the run.
pub struct AckLayer<J: Job, Q: JobStore> {
    store: Q,
    config: JobWorkerConfig,
    clock: Clock,
    _job: PhantomData<fn() -> J>,
}

impl<S, J: Job, Q: JobStore> Layer<S> for AckLayer<J, Q> {
    type Service = AckService<S, J, Q>;

    fn layer(&self, inner: S) -> Self::Service {
        AckService {
            inner,
            store: self.store.clone(),
            config: self.config.clone(),
            clock: self.clock.clone(),
            _job: PhantomData,
        }
    }
}

/// See [`AckLayer`].
pub struct AckService<S, J: Job, Q: JobStore> {
    inner: S,
    store: Q,
    config: JobWorkerConfig,
    clock: Clock,
    _job: PhantomData<fn() -> J>,
}

impl<S, J, Q> Service<Task<J, JobContext, String>> for AckService<S, J, Q>
where
    J: Job,
    Q: JobStore,
    S: Service<Task<J, JobContext, String>, Response = J::Output>,
    S::Error: std::error::Error + Send + Sync + 'static,
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
        let store = self.store.clone();
        let config = self.config.clone();
        let clock = self.clock.clone();
        let run = self.inner.call(task);

        async move {
            let lost = Arc::new(AtomicBool::new(false));
            let cancel = CancellationToken::new();
            let renew = tokio::spawn(renew_loop(
                store.clone(),
                config.clone(),
                clock.clone(),
                ctx.clone(),
                cancel.clone(),
                lost.clone(),
            ));

            let result = timeout(config.execution_timeout, run).await;
            let (succeeded, error_text) = match &result {
                Ok(Ok(_)) => (true, String::new()),
                Ok(Err(error)) => (false, error_chain(error)),
                Err(_) => (
                    false,
                    format!("job execution exceeded {:?}", config.execution_timeout),
                ),
            };

            if lost.load(Ordering::Relaxed) {
                warn!(target: "cqrs", job_id = %ctx.job_id, "lease lost during execution; re-claimer owns the outcome");
            } else {
                persist_outcome(&store, &config, &clock, &ctx, succeeded, error_text).await;
            }

            cancel.cancel();
            let _ = renew.await;

            match result {
                Ok(Ok(output)) => Ok(output),
                Ok(Err(error)) => Err(error.into()),
                Err(_elapsed) => {
                    Err(format!("job execution exceeded {:?}", config.execution_timeout).into())
                }
            }
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
/// `claim_seq`, never advancing the event stream. Once the ack deletes/advances
/// the row, the renewal matches zero rows and stops -- so a late tick can never
/// resurrect a completed job.
async fn renew_loop<Q: JobStore>(
    store: Q,
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
                match store.renew_lease(&ctx.job_id, ctx.claim_seq, new_lease.timestamp_millis()).await {
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

/// Compare-and-swap-appends the outcome event at `claim_seq + 1` and updates the
/// projection, retrying transient errors while the lease is still held. A lost
/// CAS means a re-claimer owns the outcome: never clobber it; on a successful
/// job that is a double-execution alarm.
async fn persist_outcome<Q: JobStore>(
    store: &Q,
    config: &JobWorkerConfig,
    clock: &Clock,
    ctx: &JobContext,
    succeeded: bool,
    error_text: String,
) {
    let next_seq = ctx.claim_seq + 1;
    let Some(next_seq_usize) = sequence_to_usize(&ctx.job_id, next_seq) else {
        return;
    };

    let (event, row) = outcome_event(config, clock, ctx, next_seq, succeeded, error_text);
    let Ok(serialized) = event.serialized(&ctx.job_id, next_seq_usize) else {
        error!(target: "cqrs", job_id = %ctx.job_id, "failed to encode outcome event");
        return;
    };

    loop {
        let mut connection = match store.acquire().await {
            Ok(connection) => connection,
            Err(error) => {
                error!(target: "cqrs", ?error, job_id = %ctx.job_id, "ack could not acquire a connection");
                return;
            }
        };
        match store.append_event(&mut connection, &serialized).await {
            Ok(CasOutcome::Committed) => {
                let projected = match &row {
                    Some(row) => store.upsert_row(&mut connection, &ctx.job_id, row).await,
                    None => store.delete_row(&mut connection, &ctx.job_id).await,
                };
                if let Err(error) = projected {
                    error!(target: "cqrs", ?error, job_id = %ctx.job_id, "ack projection failed; retrying");
                    sleep(Duration::from_millis(50)).await;
                    continue;
                }
                return;
            }
            Ok(CasOutcome::Conflict) => {
                if succeeded {
                    error!(target: "cqrs", job_id = %ctx.job_id, "DOUBLE-EXECUTION RISK: job succeeded but its lease was stolen; re-claimer owns the outcome");
                } else {
                    error!(target: "cqrs", job_id = %ctx.job_id, "ack lost the compare-and-swap; re-claimer owns the outcome");
                }
                return;
            }
            Err(error) => match Q::classify(&error) {
                Severity::Transient => {
                    warn!(target: "cqrs", ?error, job_id = %ctx.job_id, "ack transient error; retrying within lease");
                    sleep(Duration::from_millis(50)).await;
                }
                Severity::Fatal => {
                    error!(target: "cqrs", ?error, job_id = %ctx.job_id, "ack fatal error; giving up");
                    return;
                }
            },
        }
    }
}

fn outcome_event(
    config: &JobWorkerConfig,
    clock: &Clock,
    ctx: &JobContext,
    next_seq: i64,
    succeeded: bool,
    error_text: String,
) -> (JobEvent, Option<JobRow>) {
    if succeeded {
        return (JobEvent::Succeeded, None);
    }
    let failed = ctx.attempt + 1;
    if failed >= config.max_attempts {
        return (
            JobEvent::Dead {
                reason: DeadReason::RetriesExhausted,
                error: error_text,
            },
            None,
        );
    }
    let run_at = clock.now() + lease_delta(config.backoff.delay(ctx.attempt));
    (
        JobEvent::RetryScheduled {
            run_at,
            attempt: failed,
            error: error_text,
        },
        Some(JobRow {
            kind: ctx.kind.clone(),
            status: QueueStatus::Pending,
            run_at_ms: run_at.timestamp_millis(),
            lease_until_ms: None,
            attempt: failed,
            sequence: next_seq,
        }),
    )
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicI64;

    use serde::{Deserialize, Serialize};
    use sqlx::SqlitePool;
    use sqlx::sqlite::SqlitePoolOptions;

    use crate::job::{JobKind, Label, enqueue_job};

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

    fn movable_clock(initial_ms: i64) -> (Clock, Arc<AtomicI64>) {
        let now = Arc::new(AtomicI64::new(initial_ms));
        let handle = now.clone();
        let clock = Clock::from_fn(move || from_millis(handle.load(Ordering::Relaxed)));
        (clock, now)
    }

    fn from_millis(millis: i64) -> DateTime<Utc> {
        DateTime::from_timestamp_millis(millis).expect("valid timestamp")
    }

    fn worker(
        store: &SqliteBackend,
        name: &str,
        clock: Clock,
    ) -> EventStoreBackend<TestJob, SqliteBackend> {
        EventStoreBackend::with_store(store.clone(), name, JobWorkerConfig::default(), clock)
    }

    async fn enqueue(pool: &SqlitePool, run_at_ms: i64) -> String {
        let mut connection = pool.acquire().await.unwrap();
        enqueue_job(
            &mut connection,
            JobKind::new(TestJob::KIND),
            serde_json::to_value(TestJob { n: 1 }).unwrap(),
            from_millis(run_at_ms),
        )
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
        sqlx::query_scalar::<_, String>("SELECT status FROM job_queue WHERE job_id = ?1")
            .bind(job_id)
            .fetch_optional(pool)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn claim_appends_claimed_and_marks_the_row() {
        let pool = one_db_pool().await;
        let store = SqliteBackend::new(pool.clone());
        let (clock, _now) = movable_clock(1000);
        let job_id = enqueue(&pool, 1000).await;
        let worker = worker(&store, "w1", clock);

        let outcome = worker
            .try_claim(
                &Candidate {
                    job_id: job_id.clone(),
                    sequence: 1,
                },
                1000,
            )
            .await;

        assert!(matches!(outcome, ClaimOutcome::Won { claim_seq: 2, .. }));
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
    async fn second_claim_at_the_same_sequence_is_contended() {
        let pool = one_db_pool().await;
        let store = SqliteBackend::new(pool.clone());
        let (clock, _now) = movable_clock(1000);
        let job_id = enqueue(&pool, 1000).await;
        let worker = worker(&store, "w1", clock);
        let candidate = Candidate {
            job_id: job_id.clone(),
            sequence: 1,
        };

        let first = worker.try_claim(&candidate, 1000).await;
        let second = worker.try_claim(&candidate, 1000).await;

        assert!(matches!(first, ClaimOutcome::Won { .. }));
        assert!(matches!(
            second,
            ClaimOutcome::Skip | ClaimOutcome::Contended
        ));
        let claimed = event_types(&pool, &job_id)
            .await
            .iter()
            .filter(|event| *event == "JobClaimed")
            .count();
        assert_eq!(claimed, 1);
    }

    #[tokio::test]
    async fn job_is_not_runnable_before_its_lease_expires() {
        let pool = one_db_pool().await;
        let store = SqliteBackend::new(pool.clone());
        let (clock, _now) = movable_clock(1000);
        let job_id = enqueue(&pool, 1000).await;
        let worker = worker(&store, "w1", clock);
        worker
            .try_claim(
                &Candidate {
                    job_id,
                    sequence: 1,
                },
                1000,
            )
            .await;

        let candidates = worker.fetch_candidates(1000 + 5_000).await.unwrap();
        assert!(candidates.is_empty());
    }

    #[tokio::test]
    async fn expired_lease_is_reclaimable_without_counting_an_attempt() {
        let pool = one_db_pool().await;
        let store = SqliteBackend::new(pool.clone());
        let (clock, now) = movable_clock(1000);
        let job_id = enqueue(&pool, 1000).await;
        let worker = worker(&store, "w1", clock);
        worker
            .try_claim(
                &Candidate {
                    job_id: job_id.clone(),
                    sequence: 1,
                },
                1000,
            )
            .await;

        let later = 1000 + 31_000;
        now.store(later, Ordering::Relaxed);
        let candidates = worker.fetch_candidates(later).await.unwrap();
        assert_eq!(candidates.len(), 1);

        let outcome = worker.try_claim(&candidates[0], later).await;
        assert!(matches!(
            outcome,
            ClaimOutcome::Won {
                claim_seq: 3,
                attempt: 0,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn failure_under_budget_schedules_a_retry_then_dead_at_the_cap() {
        let pool = one_db_pool().await;
        let store = SqliteBackend::new(pool.clone());
        let (clock, _now) = movable_clock(1000);
        let config = JobWorkerConfig {
            max_attempts: 2,
            ..JobWorkerConfig::default()
        };
        let job_id = enqueue(&pool, 1000).await;
        let ctx = JobContext {
            job_id: job_id.clone(),
            kind: TestJob::KIND.to_string(),
            claim_seq: 1,
            attempt: 0,
        };

        persist_outcome(&store, &config, &clock, &ctx, false, "boom".to_string()).await;
        assert_eq!(
            event_types(&pool, &job_id).await,
            ["JobEnqueued", "JobRetryScheduled"]
        );
        assert_eq!(
            queue_status(&pool, &job_id).await.as_deref(),
            Some("pending")
        );

        let ctx2 = JobContext {
            claim_seq: 2,
            attempt: 1,
            ..ctx.clone()
        };
        persist_outcome(&store, &config, &clock, &ctx2, false, "boom".to_string()).await;
        assert_eq!(
            event_types(&pool, &job_id).await,
            ["JobEnqueued", "JobRetryScheduled", "JobDead"]
        );
        assert_eq!(queue_status(&pool, &job_id).await, None);
    }

    #[tokio::test]
    async fn ack_is_fenced_when_a_reclaimer_took_the_next_sequence() {
        let pool = one_db_pool().await;
        let store = SqliteBackend::new(pool.clone());
        let (clock, _now) = movable_clock(1000);
        let job_id = enqueue(&pool, 1000).await;
        let worker = worker(&store, "w1", clock.clone());
        worker
            .try_claim(
                &Candidate {
                    job_id: job_id.clone(),
                    sequence: 1,
                },
                1000,
            )
            .await;
        {
            let mut connection = pool.acquire().await.unwrap();
            crate::job::append_job_event(
                &mut connection,
                &job_id,
                3,
                &JobEvent::Claimed {
                    worker: WorkerId::new("w2"),
                    lease_until: from_millis(99_999),
                },
            )
            .await
            .unwrap();
        }

        let ctx = JobContext {
            job_id: job_id.clone(),
            kind: TestJob::KIND.to_string(),
            claim_seq: 2,
            attempt: 0,
        };
        persist_outcome(
            &store,
            &JobWorkerConfig::default(),
            &clock,
            &ctx,
            true,
            String::new(),
        )
        .await;

        assert_eq!(
            event_types(&pool, &job_id).await,
            ["JobEnqueued", "JobClaimed", "JobClaimed"]
        );
    }

    #[tokio::test]
    async fn claim_budget_dead_letters_a_crash_loop() {
        let pool = one_db_pool().await;
        let store = SqliteBackend::new(pool.clone());
        let (clock, now) = movable_clock(1000);
        let config = JobWorkerConfig {
            max_claims: 2,
            ..JobWorkerConfig::default()
        };
        let job_id = enqueue(&pool, 1000).await;
        let worker =
            EventStoreBackend::<TestJob, SqliteBackend>::with_store(store, "w1", config, clock);

        let mut sequence = 1;
        let mut clock_ms = 1000;
        for _ in 0..2 {
            let outcome = worker
                .try_claim(
                    &Candidate {
                        job_id: job_id.clone(),
                        sequence,
                    },
                    clock_ms,
                )
                .await;
            let ClaimOutcome::Won { claim_seq, .. } = outcome else {
                panic!("expected claim");
            };
            sequence = claim_seq;
            clock_ms += 31_000;
            now.store(clock_ms, Ordering::Relaxed);
        }

        let outcome = worker
            .try_claim(
                &Candidate {
                    job_id: job_id.clone(),
                    sequence,
                },
                clock_ms,
            )
            .await;
        assert!(matches!(
            outcome,
            ClaimOutcome::Won {
                abandoned: true,
                ..
            }
        ));
        assert_eq!(queue_status(&pool, &job_id).await, None);
        assert!(
            event_types(&pool, &job_id)
                .await
                .contains(&"JobDead".to_string())
        );
    }
}
