//! `SupportTicket` aggregate: an `Open -> Pending -> Closed` state machine with a
//! materialized view that exposes `status` as a generated column for filtered
//! queries. Closing a ticket enqueues a durable [`NotifyClosed`] job -- the
//! command stays a pure `(state, command) -> events` function and the side
//! effect (notifying the customer) runs in a supervised worker.
//!
//! Event timestamps are caller-provided (carried on the command) rather than
//! read from an injected clock: handlers take only a typed [`JobQueue`], not
//! services, so the only non-determinism is what the command brings in.

use std::fmt::{self, Display};
use std::str::FromStr;

use async_trait::async_trait;
use cqrs_es::DomainEvent;
use serde::{Deserialize, Serialize};

use event_sorcery::{Column, EventSourced, Job, JobOutcome, JobQueue, Label, Table};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TicketId(pub u64);

impl Display for TicketId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "T-{:04}", self.0)
    }
}

impl FromStr for TicketId {
    type Err = ParseTicketIdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let suffix = value
            .strip_prefix("T-")
            .ok_or(ParseTicketIdError::MissingPrefix)?;
        let raw = suffix.parse::<u64>()?;
        Ok(Self(raw))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ParseTicketIdError {
    #[error("ticket id must start with 'T-'")]
    MissingPrefix,
    #[error("ticket id suffix is not numeric: {0}")]
    NotNumeric(#[from] std::num::ParseIntError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "TEXT")]
pub enum Status {
    Open,
    Pending,
    Closed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SupportTicket {
    pub subject: String,
    pub status: Status,
    pub last_updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SupportTicketEvent {
    Opened {
        subject: String,
        at: chrono::DateTime<chrono::Utc>,
    },
    AwaitingCustomer {
        at: chrono::DateTime<chrono::Utc>,
    },
    Closed {
        at: chrono::DateTime<chrono::Utc>,
    },
}

impl DomainEvent for SupportTicketEvent {
    fn event_type(&self) -> String {
        match self {
            Self::Opened { .. } => "SupportTicketEvent::Opened".to_string(),
            Self::AwaitingCustomer { .. } => "SupportTicketEvent::AwaitingCustomer".to_string(),
            Self::Closed { .. } => "SupportTicketEvent::Closed".to_string(),
        }
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
    }
}

/// Commands carry the timestamp the caller wants stamped on the resulting event.
#[derive(Debug, Clone)]
pub enum SupportTicketCommand {
    Open {
        subject: String,
        at: chrono::DateTime<chrono::Utc>,
    },
    AwaitCustomer {
        at: chrono::DateTime<chrono::Utc>,
    },
    Close {
        at: chrono::DateTime<chrono::Utc>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum SupportTicketError {
    #[error("ticket already exists")]
    AlreadyOpen,
    #[error("ticket has not been opened")]
    NotOpen,
    #[error("ticket is already closed")]
    AlreadyClosed,
}

/// Durable side effect dispatched when a ticket closes: notify the customer.
///
/// The handler enqueues this on the typed [`JobQueue`]; a supervised worker runs
/// it later, exactly once per close, atomically with the `Closed` event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotifyClosed {
    pub subject: String,
}

/// Dependency bundle the worker injects into [`NotifyClosed::perform`]. A real
/// app would hold an email client; here it just records sent notifications.
#[derive(Clone, Default)]
pub struct Notifier;

impl Job for NotifyClosed {
    type Input = Notifier;
    type Output = ();
    type Error = std::convert::Infallible;

    const WORKER_NAME: &'static str = "notify-closed";
    const KIND: &'static str = "notify-closed";

    fn label(&self) -> Label {
        Label::new(format!("notify-closed:{}", self.subject))
    }

    async fn perform(&self, _input: &Notifier) -> Result<JobOutcome<()>, Self::Error> {
        println!(
            "  [worker] notified customer that '{}' was closed",
            self.subject
        );
        Ok(JobOutcome::Done(()))
    }
}

#[async_trait]
impl EventSourced for SupportTicket {
    type Id = TicketId;
    type Event = SupportTicketEvent;
    type Command = SupportTicketCommand;
    type Error = SupportTicketError;
    type Jobs = event_sorcery::jobs![NotifyClosed];
    type Materialized = Table;

    const AGGREGATE_TYPE: &'static str = "SupportTicket";
    const PROJECTION: Table = Table("support_ticket_view");
    const SCHEMA_VERSION: u64 = 1;

    fn originate(event: &SupportTicketEvent) -> Option<Self> {
        match event {
            SupportTicketEvent::Opened { subject, at } => Some(Self {
                subject: subject.clone(),
                status: Status::Open,
                last_updated_at: *at,
            }),
            SupportTicketEvent::AwaitingCustomer { .. } | SupportTicketEvent::Closed { .. } => None,
        }
    }

    fn evolve(
        entity: &Self,
        event: &SupportTicketEvent,
    ) -> Result<Option<Self>, SupportTicketError> {
        match event {
            SupportTicketEvent::Opened { .. } => Ok(None),
            SupportTicketEvent::AwaitingCustomer { at } => Ok(Some(Self {
                status: Status::Pending,
                last_updated_at: *at,
                ..entity.clone()
            })),
            SupportTicketEvent::Closed { at } => Ok(Some(Self {
                status: Status::Closed,
                last_updated_at: *at,
                ..entity.clone()
            })),
        }
    }

    async fn initialize(
        command: SupportTicketCommand,
        _jobs: &JobQueue<Self::Jobs>,
    ) -> Result<Vec<SupportTicketEvent>, SupportTicketError> {
        match command {
            SupportTicketCommand::Open { subject, at } => {
                Ok(vec![SupportTicketEvent::Opened { subject, at }])
            }
            SupportTicketCommand::AwaitCustomer { .. } | SupportTicketCommand::Close { .. } => {
                Err(SupportTicketError::NotOpen)
            }
        }
    }

    async fn transition(
        &self,
        command: SupportTicketCommand,
        jobs: &JobQueue<Self::Jobs>,
    ) -> Result<Vec<SupportTicketEvent>, SupportTicketError> {
        match command {
            SupportTicketCommand::Open { .. } => Err(SupportTicketError::AlreadyOpen),
            SupportTicketCommand::AwaitCustomer { at } => match self.status {
                Status::Closed => Err(SupportTicketError::AlreadyClosed),
                Status::Open | Status::Pending => {
                    Ok(vec![SupportTicketEvent::AwaitingCustomer { at }])
                }
            },
            SupportTicketCommand::Close { at } => match self.status {
                Status::Closed => Err(SupportTicketError::AlreadyClosed),
                Status::Open | Status::Pending => {
                    // Enqueue the notification side effect; it flushes atomically
                    // with the Closed event and runs in a supervised worker.
                    jobs.push(NotifyClosed {
                        subject: self.subject.clone(),
                    });
                    Ok(vec![SupportTicketEvent::Closed { at }])
                }
            },
        }
    }
}

pub const STATUS: Column = Column("status");

#[cfg(test)]
mod tests {
    use sqlx::SqlitePool;

    use event_sorcery::{LifecycleError, StoreBuilder, TestHarness, TestStore, replay};

    use super::*;

    fn fixed_instant() -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc)
    }

    #[test]
    fn replay_reconstructs_state_from_events() {
        let ticket = replay::<SupportTicket>(vec![
            SupportTicketEvent::Opened {
                subject: "login broken".to_string(),
                at: fixed_instant(),
            },
            SupportTicketEvent::AwaitingCustomer {
                at: fixed_instant(),
            },
        ])
        .unwrap()
        .unwrap();

        assert_eq!(ticket.status, Status::Pending);
        assert_eq!(ticket.subject, "login broken");
    }

    #[test]
    fn replay_rejects_history_without_genesis_event() {
        let error = replay::<SupportTicket>(vec![SupportTicketEvent::Closed {
            at: fixed_instant(),
        }])
        .unwrap_err();

        assert!(matches!(error, LifecycleError::EventCantOriginate { .. }));
    }

    #[tokio::test]
    async fn open_then_close_emits_closed_event() {
        TestHarness::<SupportTicket>::new()
            .given(vec![SupportTicketEvent::Opened {
                subject: "login broken".to_string(),
                at: fixed_instant(),
            }])
            .when(SupportTicketCommand::Close {
                at: fixed_instant(),
            })
            .await
            .then_expect_events(&[SupportTicketEvent::Closed {
                at: fixed_instant(),
            }]);
    }

    #[tokio::test]
    async fn closing_twice_returns_already_closed() {
        let error = TestHarness::<SupportTicket>::new()
            .given(vec![
                SupportTicketEvent::Opened {
                    subject: "login broken".to_string(),
                    at: fixed_instant(),
                },
                SupportTicketEvent::Closed {
                    at: fixed_instant(),
                },
            ])
            .when(SupportTicketCommand::Close {
                at: fixed_instant(),
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(SupportTicketError::AlreadyClosed)
        ));
    }

    #[tokio::test]
    async fn test_store_round_trip_against_in_memory_store() {
        let store = TestStore::<SupportTicket>::new();
        let id = TicketId(1);

        store
            .send(
                &id,
                SupportTicketCommand::Open {
                    subject: "x".to_string(),
                    at: fixed_instant(),
                },
            )
            .await
            .unwrap();
        store
            .send(
                &id,
                SupportTicketCommand::AwaitCustomer {
                    at: fixed_instant(),
                },
            )
            .await
            .unwrap();

        let ticket = store.load(&id).await.unwrap().unwrap();
        assert_eq!(ticket.status, Status::Pending);
    }

    #[tokio::test]
    async fn projection_filter_returns_only_matching_status() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();

        let (store, projection) = StoreBuilder::<SupportTicket>::new(pool.clone())
            .build()
            .await
            .unwrap();

        let alpha = TicketId(1);
        let bravo = TicketId(2);
        let charlie = TicketId(3);

        for id in [alpha, bravo, charlie] {
            store
                .send(
                    &id,
                    SupportTicketCommand::Open {
                        subject: "x".to_string(),
                        at: fixed_instant(),
                    },
                )
                .await
                .unwrap();
        }
        store
            .send(
                &bravo,
                SupportTicketCommand::Close {
                    at: fixed_instant(),
                },
            )
            .await
            .unwrap();

        let open = projection.filter(STATUS, &Status::Open).await.unwrap();
        let mut open_ids: Vec<TicketId> = open.iter().map(|(id, _)| *id).collect();
        open_ids.sort();
        assert_eq!(open_ids, vec![alpha, charlie]);

        let closed = projection.filter(STATUS, &Status::Closed).await.unwrap();
        assert_eq!(closed.len(), 1);
        assert_eq!(closed[0].0, bravo);
    }

    #[tokio::test]
    async fn closing_enqueues_a_notify_job() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();

        let (store, _projection) = StoreBuilder::<SupportTicket>::new(pool.clone())
            .build()
            .await
            .unwrap();

        let id = TicketId(1);
        store
            .send(
                &id,
                SupportTicketCommand::Open {
                    subject: "billing".to_string(),
                    at: fixed_instant(),
                },
            )
            .await
            .unwrap();
        store
            .send(
                &id,
                SupportTicketCommand::Close {
                    at: fixed_instant(),
                },
            )
            .await
            .unwrap();

        // The Close handler enqueued a NotifyClosed job; it is a pending row in
        // the job_queue, flushed in the same transaction as the Closed event.
        let pending: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM job_queue WHERE kind = 'notify-closed'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(pending, 1);
    }
}
