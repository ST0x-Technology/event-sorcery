//! `SupportTicket` aggregate: an `Open -> Pending -> Closed` state machine
//! whose commands carry their own timestamp and a materialized view that
//! exposes `status` as a generated column for filtered queries.

use std::fmt::{self, Display};
use std::str::FromStr;

use cqrs_es::DomainEvent;
use serde::{Deserialize, Serialize};

use event_sorcery::{Column, EventSourced, JobQueue, Nil, Table};

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

impl EventSourced for SupportTicket {
    type Id = TicketId;
    type Event = SupportTicketEvent;
    type Command = SupportTicketCommand;
    type Error = SupportTicketError;
    type Jobs = Nil;
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

    fn initialize(
        command: SupportTicketCommand,
        _jobs: &mut JobQueue<Self::Jobs>,
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

    fn transition(
        &self,
        command: SupportTicketCommand,
        _jobs: &mut JobQueue<Self::Jobs>,
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
                Status::Open | Status::Pending => Ok(vec![SupportTicketEvent::Closed { at }]),
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
        TestHarness::<SupportTicket>::with()
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
        let error = TestHarness::<SupportTicket>::with()
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
    async fn rebuild_all_replays_views_from_events_idempotently() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();

        let (store, projection) = StoreBuilder::<SupportTicket>::new(pool.clone())
            .build()
            .await
            .unwrap();

        let alpha = TicketId(1);
        store
            .send(
                &alpha,
                SupportTicketCommand::Open {
                    subject: "x".to_string(),
                    at: fixed_instant(),
                },
            )
            .await
            .unwrap();
        store
            .send(
                &alpha,
                SupportTicketCommand::AwaitCustomer {
                    at: fixed_instant(),
                },
            )
            .await
            .unwrap();

        projection.rebuild_all().await.unwrap();
        projection.rebuild_all().await.unwrap();

        let ticket = projection.load(&alpha).await.unwrap().unwrap();
        assert_eq!(ticket.status, Status::Pending);
    }
}
