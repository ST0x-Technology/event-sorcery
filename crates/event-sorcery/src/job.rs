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

use serde::Serialize;
use serde::de::DeserializeOwned;
use std::future::Future;

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
