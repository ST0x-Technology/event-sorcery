//! Durable, retryable jobs for command side effects.
//!
//! Command handlers stay pure `(state, command) -> Vec<Event>` and
//! enqueue side effects as [`Job`]s. The framework flushes pending
//! jobs inside the same SQLite transaction that commits the
//! triggering events, so a job is enqueued iff its events commit --
//! closing the crash-safety window between a side effect and the
//! event meant to record it.
//!
//! Jobs are stored in apalis's `Jobs` table and executed by a
//! supervised apalis worker. [`perform`](Job::perform) receives the
//! consumer-owned [`Input`](Job::Input) dependency bundle.
//!
//! The queue is written through the event store's own connection
//! (sqlx 0.9), while the worker side reads it through apalis's
//! storage (sqlx 0.8). Both address the same `Jobs` table in the
//! same SQLite database.

use apalis::layers::retry::backoff::Backoff;
use apalis::prelude::{Attempt, Data};
use apalis_codec::json::JsonCodec;
use apalis_core::backend::poll_strategy::{BackoffConfig, IntervalStrategy, StrategyBuilder};
use apalis_core::worker::context::WorkerContext;
use apalis_core::worker::event::Event;
use apalis_sqlite::fetcher::SqliteFetcher;
use apalis_sqlite::{CompactType, Config, SqlitePool, SqliteStorage};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error};

/// A durable, retryable unit of side-effecting work.
///
/// Each implementation is one self-contained side effect; an entity
/// declares the set of jobs its commands dispatch. The job is
/// serialized into apalis's queue and executed by a supervised
/// worker, which calls [`perform`](Job::perform) with the
/// consumer-owned [`Input`](Job::Input) dependency bundle.
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

    /// Stable identifier for this job kind, used as the queue name
    /// in apalis's `Jobs` table, by the failure-injection registry,
    /// and in structured logs. Must be unique across job types --
    /// two kinds sharing a name would consume each other's queue.
    /// Distinct from [`WORKER_NAME`](Job::WORKER_NAME) because
    /// multiple workers can share a kind.
    ///
    /// Persisted in the database, so it must stay stable across
    /// refactors and compiler upgrades (which is why
    /// `std::any::type_name` is not used here).
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

/// Worker-side handle to apalis's SQLite storage for a single job
/// type.
///
/// Built once at startup from a pool addressing the same database as
/// the event store. Consumed by the worker wiring via
/// [`into_storage`](Self::into_storage).
pub struct JobBackend<J: Job> {
    storage: Storage<J>,
}

impl<J: Job> JobBackend<J> {
    /// Builds a backend over apalis's `Jobs` table in `pool`.
    ///
    /// `pool` is an apalis (sqlx 0.8) pool; the same database is
    /// written by the event store's own connection at enqueue time.
    #[must_use]
    pub fn new(pool: &SqlitePool) -> Self {
        Self {
            storage: SqliteStorage::new_with_config(pool, &build_poll_config::<J>()),
        }
    }

    /// Consumes the backend, yielding the apalis storage for worker
    /// registration.
    #[must_use]
    pub fn into_storage(self) -> Storage<J> {
        self.storage
    }

    /// The pool backing this storage, for queue maintenance.
    #[must_use]
    pub fn pool(&self) -> &SqlitePool {
        self.storage.pool()
    }
}

/// Error returned by the worker handler when a job fails or is
/// deliberately failed by the injector.
#[derive(Debug, thiserror::Error)]
pub enum JobError {
    /// The job's own [`perform`](Job::perform) returned an error.
    #[error("{label}: {source}")]
    Failed {
        /// Label of the failed job instance.
        label: Label,
        /// The underlying domain error.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// A failure injected by [`FailureInjector`] for fault testing.
    #[cfg(any(test, feature = "test-support"))]
    #[error("injected terminal job failure")]
    Injected,
}

/// Human-readable identifier for a job instance, used in logs and
/// failure-injection targeting.
#[derive(Debug, Clone, PartialEq, Eq)]
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

/// Circuit-breaker recovery timeout. Set effectively infinite: a
/// tripped breaker stays open until the supervisor restarts, since a
/// terminal job failure indicates a problem a human must inspect.
pub const FAIL_STOP_RECOVERY_TIMEOUT: Duration = Duration::from_secs(60 * 60 * 24 * 365);

/// Retry backoff applied to job execution: 1s base, doubling, capped
/// at 30s.
pub const RETRY_BACKOFF: ExponentialBackoff =
    ExponentialBackoff::new(Duration::from_secs(1), Duration::from_secs(30));

/// Exponential backoff for apalis retries.
#[derive(Clone, Debug)]
pub struct ExponentialBackoff {
    base: Duration,
    max: Duration,
    iteration: u32,
}

impl ExponentialBackoff {
    /// Creates a backoff that starts at `base` and doubles up to
    /// `max`.
    #[must_use]
    pub const fn new(base: Duration, max: Duration) -> Self {
        Self {
            base,
            max,
            iteration: 0,
        }
    }
}

impl Backoff for ExponentialBackoff {
    type Future = tokio::time::Sleep;

    fn next_backoff(&mut self) -> Self::Future {
        let factor = 2u32.saturating_pow(self.iteration);
        let delay = self.base.saturating_mul(factor).min(self.max);
        self.iteration = self.iteration.saturating_add(1);
        tokio::time::sleep(delay)
    }
}

/// apalis SQLite storage specialized for a single job type with JSON
/// payload encoding.
pub type Storage<J> = SqliteStorage<J, JsonCodec<CompactType>, SqliteFetcher>;

/// The apalis worker handler. Deserializes the job, logs, and runs
/// [`perform`](Job::perform) against the injected input.
#[cfg(not(any(test, feature = "test-support")))]
pub async fn work<J>(
    job: J,
    input: Data<Arc<J::Input>>,
    attempt: Attempt,
) -> Result<J::Output, JobError>
where
    J: Job + Sync,
{
    let label = job.label();
    log_processing(&label, attempt.current());
    job.perform(&input)
        .await
        .map_err(|source| JobError::Failed {
            label,
            source: Box::new(source),
        })
}

/// Test-support [`work`] variant that routes execution through a
/// [`FailureInjector`] so end-to-end tests can force a terminal
/// failure for a targeted job.
#[cfg(any(test, feature = "test-support"))]
pub async fn work<J>(
    job: J,
    input: Data<Arc<J::Input>>,
    injector: Data<FailureInjector>,
    attempt: Attempt,
) -> Result<J::Output, JobError>
where
    J: Job + Sync,
{
    injector.perform::<J>(&job, &input, attempt.current()).await
}

/// Worker event handler that stops the worker and notifies the
/// supervisor on a terminal (retries-exhausted) failure.
pub fn on_terminal_failure(
    failure_notify: Arc<tokio::sync::Notify>,
    error_msg: &'static str,
) -> impl Fn(&WorkerContext, &Event) + Send + Sync + 'static {
    move |context, event| {
        if let Event::Error(error) = event {
            error!(%error, worker = %context.name(), "{error_msg}");
            // notify_one stores a permit when no task is awaiting, so
            // the signal is not lost if the supervisor has not reached
            // its `notified().await` yet (notify_waiters would drop it).
            failure_notify.notify_one();
            let _ = context.stop();
        }
    }
}

/// Builds a supervised apalis worker for a [`Job`] type, applying the
/// library's standard concurrency, retry policy, circuit breaker, and
/// terminal-failure handling.
///
/// Pass `::<JobType>`, a worker index, the job's [`JobBackend`], an
/// `Arc<JobType::Input>`, a
/// [`CircuitBreakerConfig`](crate::CircuitBreakerConfig), and an
/// `Arc<tokio::sync::Notify>` signalled on terminal failure. Under
/// `test-support`, also pass a [`FailureInjector`]. Register the
/// returned worker with a [`Monitor`](crate::Monitor).
#[macro_export]
macro_rules! build_supervised_worker {
    (
        ::<$job:ty>,
        $index:expr,
        $backend:expr,
        $input:expr,
        $fail_stop:expr,
        $failure_notify:expr
        $(, $failure_injector:expr)? $(,)?
    ) => {{
        use $crate::__apalis::{
            CircuitBreaker, EventListenerExt, RetryPolicy, WorkerBuilder, WorkerBuilderExt,
        };

        let builder = WorkerBuilder::new(::std::format!(
            "{}-{}",
            <$job as $crate::Job>::WORKER_NAME,
            $index,
        ))
        .backend($backend.into_storage())
        .data($input);

        $(
            let builder = builder.data($failure_injector);
        )?

        builder
            .concurrency(1)
            .retry(RetryPolicy::retries(3).with_backoff($crate::RETRY_BACKOFF.clone()))
            .break_circuit_with($fail_stop)
            .on_event($crate::on_terminal_failure(
                $failure_notify,
                <$job as $crate::Job>::TERMINAL_FAILURE_MSG,
            ))
            .build($crate::work::<$job>)
    }};
}

fn build_poll_config<J: Job>() -> Config {
    let strategy = StrategyBuilder::new()
        .apply(
            IntervalStrategy::new(Duration::from_millis(100))
                .with_backoff(BackoffConfig::new(Duration::from_secs(1))),
        )
        .build();

    Config::new(J::KIND).with_poll_interval(strategy)
}

fn log_processing(label: &Label, attempt: usize) {
    debug!(target: "job", %label, attempt, "processing job");
}

/// Fault-injection registry for end-to-end terminal-failure tests.
///
/// [`arm`](Self::arm) marks a [`Job::KIND`] so the next job of that
/// kind fails terminally; the failure then sticks to that specific
/// job instance (by [`Label`]) so retries of it keep failing while
/// other instances of the same kind run normally.
#[cfg(any(test, feature = "test-support"))]
#[derive(Clone, Debug, Default)]
pub struct FailureInjector {
    states: Arc<std::sync::Mutex<std::collections::HashMap<&'static str, InjectionState>>>,
}

#[cfg(any(test, feature = "test-support"))]
#[derive(Debug, Default)]
enum InjectionState {
    #[default]
    Idle,
    Armed,
    Targeted(Label),
}

#[cfg(any(test, feature = "test-support"))]
impl FailureInjector {
    /// Creates an injector with no kinds armed.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Arms injection for the next job of `kind`.
    pub fn arm(&self, kind: &'static str) {
        self.lock().insert(kind, InjectionState::Armed);
    }

    fn should_inject(&self, kind: &'static str, label: &Label) -> bool {
        let mut guard = self.lock();
        let state = guard.entry(kind).or_default();
        let inject = match state {
            InjectionState::Idle => false,
            InjectionState::Armed => {
                *state = InjectionState::Targeted(label.clone());
                true
            }
            InjectionState::Targeted(target) => target == label,
        };
        drop(guard);
        inject
    }

    fn lock(
        &self,
    ) -> std::sync::MutexGuard<'_, std::collections::HashMap<&'static str, InjectionState>> {
        self.states
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    async fn perform<J: Job + Sync>(
        &self,
        job: &J,
        input: &J::Input,
        attempt: usize,
    ) -> Result<J::Output, JobError> {
        let label = job.label();
        if self.should_inject(J::KIND, &label) {
            return Err(JobError::Injected);
        }
        log_processing(&label, attempt);
        job.perform(input).await.map_err(|source| JobError::Failed {
            label,
            source: Box::new(source),
        })
    }
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

    #[tokio::test(start_paused = true)]
    async fn backoff_doubles_from_base_and_caps_at_max() {
        let mut backoff = ExponentialBackoff::new(Duration::from_secs(1), Duration::from_secs(30));

        for expected_seconds in [1, 2, 4, 8, 16, 30, 30] {
            let sleep = backoff.next_backoff();
            assert_eq!(
                sleep.deadline() - tokio::time::Instant::now(),
                Duration::from_secs(expected_seconds),
                "backoff delay must double from base and cap at max"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn backoff_saturates_instead_of_overflowing() {
        let mut backoff = ExponentialBackoff::new(Duration::from_secs(1), Duration::MAX);
        backoff.iteration = u32::MAX;

        // The doubling factor saturates at u32::MAX instead of
        // overflowing, so the uncapped delay is base * u32::MAX.
        let sleep = backoff.next_backoff();
        assert_eq!(
            sleep.deadline() - tokio::time::Instant::now(),
            Duration::from_secs(u64::from(u32::MAX)),
        );

        // A second call must not panic on the iteration counter and
        // keeps yielding the saturated delay.
        let next = backoff.next_backoff();
        assert_eq!(
            next.deadline() - tokio::time::Instant::now(),
            Duration::from_secs(u64::from(u32::MAX)),
        );
    }

    #[tokio::test]
    async fn injector_targets_first_armed_instance_and_spares_others() {
        let injector = FailureInjector::new();
        injector.arm(SendEmail::KIND);

        let welcome = SendEmail::Welcome {
            address: "a@example.com".to_string(),
        };
        let reminder = SendEmail::Reminder {
            address: "b@example.com".to_string(),
        };

        let first = injector.perform(&welcome, &(), 1).await;
        assert!(matches!(first, Err(JobError::Injected)));

        let retry = injector.perform(&welcome, &(), 2).await;
        assert!(
            matches!(retry, Err(JobError::Injected)),
            "the injected failure must stick to the targeted instance across retries"
        );

        injector.perform(&reminder, &(), 1).await.unwrap();
    }

    #[tokio::test]
    async fn build_supervised_worker_constructs_a_worker() {
        let pool = apalis_sqlite::SqlitePool::connect(":memory:")
            .await
            .unwrap();
        let backend = JobBackend::<SendEmail>::new(&pool);

        let input = Arc::new(());
        let fail_stop = crate::CircuitBreakerConfig::default()
            .with_recovery_timeout(FAIL_STOP_RECOVERY_TIMEOUT);
        let failure_notify = Arc::new(tokio::sync::Notify::new());
        let injector = FailureInjector::new();

        let _worker = crate::build_supervised_worker!(
            ::<SendEmail>,
            0,
            backend,
            input,
            fail_stop,
            failure_notify,
            injector,
        );
    }
}
