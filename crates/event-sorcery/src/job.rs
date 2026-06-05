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
#[derive(Debug, Clone)]
pub struct Label(String);

impl Label {
    /// Wraps a string-like value as a label.
    pub fn new(label: impl Into<String>) -> Self {
        Self(label.into())
    }

    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn as_str(&self) -> &str {
        &self.0
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
#[cfg(not(feature = "test-support"))]
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

/// Worker event handler that stops the worker and notifies the
/// supervisor on a terminal (retries-exhausted) failure.
pub fn on_terminal_failure(
    failure_notify: Arc<tokio::sync::Notify>,
    error_msg: &'static str,
) -> impl Fn(&WorkerContext, &Event) + Send + Sync + 'static {
    move |context, event| {
        if let Event::Error(error) = event {
            error!(%error, worker = %context.name(), "{error_msg}");
            failure_notify.notify_waiters();
            let _ = context.stop();
        }
    }
}

fn build_poll_config<J: 'static>() -> Config {
    let strategy = StrategyBuilder::new()
        .apply(
            IntervalStrategy::new(Duration::from_millis(100))
                .with_backoff(BackoffConfig::new(Duration::from_secs(1))),
        )
        .build();

    Config::new(std::any::type_name::<J>()).with_poll_interval(strategy)
}

fn log_processing(label: &Label, attempt: usize) {
    debug!(target: "job", %label, attempt, "processing job");
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
}
