//! Materialized view (`Projection`) end-to-end with a generated column for
//! filtered queries.
//!
//! Domain: a `SupportTicket` aggregate that goes through `Open -> Pending ->
//! Closed`. Its view table extracts the current `status` from the JSON
//! payload via a SQLite generated column, letting `Projection::filter`
//! return only tickets in a chosen state.
//!
//! `main()` walks through:
//! - declaring `type Materialized = Table` and `PROJECTION =
//!   Table("support_ticket_view")`;
//! - injecting a domain service (`Arc<dyn Clock>`) for deterministic event
//!   timestamps;
//! - creating the view table inline before `StoreBuilder::build()`;
//! - building the store via `StoreBuilder` and reading through the
//!   auto-wired `Projection`;
//! - using `Projection::load`, `load_all`, and `filter` (typed value);
//! - rebuilding views via `rebuild` and `rebuild_all` as recovery tools.
//!
//! Run with: `cargo run -p event-sorcery --example projection`
//!
//! See `README.md` next to this file for design notes.

use std::error::Error;
use std::sync::Arc;

use async_trait::async_trait;
use cqrs_es::DomainEvent;
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;

use event_sorcery::{Column, EventSourced, StoreBuilder, Table};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "TEXT")]
enum Status {
    Open,
    Pending,
    Closed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct SupportTicket {
    subject: String,
    status: Status,
    last_updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum SupportTicketEvent {
    Opened { subject: String, at: String },
    AwaitingCustomer { at: String },
    Closed { at: String },
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
enum SupportTicketCommand {
    Open { subject: String },
    AwaitCustomer,
    Close,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
enum SupportTicketError {
    #[error("ticket already exists")]
    AlreadyOpen,
    #[error("ticket has not been opened")]
    NotOpen,
    #[error("ticket is already closed")]
    AlreadyClosed,
}

/// Domain service for time-of-event resolution. A real consumer would back
/// this with `chrono::Utc::now()`; the example uses a deterministic stub so
/// output is reproducible. Mirrors the `Arc<dyn Service>` pattern used by
/// `OffchainOrder` in `st0x.liquidity`.
trait Clock: Send + Sync {
    fn now(&self) -> String;
}

struct StepClock {
    counter: std::sync::atomic::AtomicU64,
}

impl StepClock {
    fn new() -> Self {
        Self {
            counter: std::sync::atomic::AtomicU64::new(0),
        }
    }
}

impl Clock for StepClock {
    fn now(&self) -> String {
        let tick = self
            .counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        format!("t{tick}")
    }
}

#[async_trait]
impl EventSourced for SupportTicket {
    type Id = String;
    type Event = SupportTicketEvent;
    type Command = SupportTicketCommand;
    type Error = SupportTicketError;
    type Services = Arc<dyn Clock>;
    type Materialized = Table;

    const AGGREGATE_TYPE: &'static str = "SupportTicket";
    const PROJECTION: Table = Table("support_ticket_view");
    const SCHEMA_VERSION: u64 = 1;

    fn originate(event: &SupportTicketEvent) -> Option<Self> {
        match event {
            SupportTicketEvent::Opened { subject, at } => Some(Self {
                subject: subject.clone(),
                status: Status::Open,
                last_updated_at: at.clone(),
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
                last_updated_at: at.clone(),
                ..entity.clone()
            })),
            SupportTicketEvent::Closed { at } => Ok(Some(Self {
                status: Status::Closed,
                last_updated_at: at.clone(),
                ..entity.clone()
            })),
        }
    }

    async fn initialize(
        command: SupportTicketCommand,
        services: &Arc<dyn Clock>,
    ) -> Result<Vec<SupportTicketEvent>, SupportTicketError> {
        match command {
            SupportTicketCommand::Open { subject } => Ok(vec![SupportTicketEvent::Opened {
                subject,
                at: services.now(),
            }]),
            SupportTicketCommand::AwaitCustomer | SupportTicketCommand::Close => {
                Err(SupportTicketError::NotOpen)
            }
        }
    }

    async fn transition(
        &self,
        command: SupportTicketCommand,
        services: &Arc<dyn Clock>,
    ) -> Result<Vec<SupportTicketEvent>, SupportTicketError> {
        match command {
            SupportTicketCommand::Open { .. } => Err(SupportTicketError::AlreadyOpen),
            SupportTicketCommand::AwaitCustomer => match self.status {
                Status::Closed => Err(SupportTicketError::AlreadyClosed),
                Status::Open | Status::Pending => Ok(vec![SupportTicketEvent::AwaitingCustomer {
                    at: services.now(),
                }]),
            },
            SupportTicketCommand::Close => match self.status {
                Status::Closed => Err(SupportTicketError::AlreadyClosed),
                Status::Open | Status::Pending => {
                    Ok(vec![SupportTicketEvent::Closed { at: services.now() }])
                }
            },
        }
    }
}

const STATUS: Column = Column("status");

/// Create the view table with a generated column extracting `status` from
/// the JSON payload. Workspace migrations only ship the events / snapshots /
/// schema_registry tables; view tables are the consumer's responsibility.
async fn create_view_table(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS support_ticket_view ( \
             view_id TEXT PRIMARY KEY, \
             version BIGINT NOT NULL, \
             payload JSON NOT NULL, \
             status TEXT GENERATED ALWAYS AS \
                 (json_extract(payload, '$.Live.status')) STORED \
         )",
    )
    .execute(pool)
    .await?;
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_support_ticket_view_status \
         ON support_ticket_view(status) WHERE status IS NOT NULL",
    )
    .execute(pool)
    .await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = SqlitePool::connect(":memory:").await?;
    sqlx::migrate!("../../migrations").run(&pool).await?;
    create_view_table(&pool).await?;

    let clock: Arc<dyn Clock> = Arc::new(StepClock::new());
    let (store, projection) = StoreBuilder::<SupportTicket>::new(pool.clone())
        .build(clock)
        .await?;

    // Three tickets in different states.
    for (id, subject) in [
        ("ticket-1", "login broken"),
        ("ticket-2", "feature request"),
        ("ticket-3", "billing question"),
    ] {
        store
            .send(
                &id.to_string(),
                SupportTicketCommand::Open {
                    subject: subject.to_string(),
                },
            )
            .await?;
    }

    store
        .send(&"ticket-2".to_string(), SupportTicketCommand::AwaitCustomer)
        .await?;
    store
        .send(&"ticket-3".to_string(), SupportTicketCommand::Close)
        .await?;

    // Single load via the view table.
    let ticket_one = projection
        .load(&"ticket-1".to_string())
        .await?
        .ok_or("ticket-1 missing")?;
    println!(
        "projection.load(ticket-1)    = {{ subject: {:?}, status: {:?} }}",
        ticket_one.subject, ticket_one.status
    );

    // Bulk read: every live aggregate, regardless of status.
    let mut all = projection.load_all().await?;
    all.sort_by(|left, right| left.0.cmp(&right.0));
    println!("projection.load_all          = {} rows", all.len());

    // Filter on the generated column with a typed value -- column existence
    // and non-null coverage are validated automatically.
    let open_tickets = projection.filter(STATUS, &Status::Open).await?;
    let pending_tickets = projection.filter(STATUS, &Status::Pending).await?;
    let closed_tickets = projection.filter(STATUS, &Status::Closed).await?;
    println!(
        "filter(STATUS, Open)         = {} (ids: {:?})",
        open_tickets.len(),
        open_tickets.iter().map(|(id, _)| id).collect::<Vec<_>>()
    );
    println!(
        "filter(STATUS, Pending)      = {} (ids: {:?})",
        pending_tickets.len(),
        pending_tickets.iter().map(|(id, _)| id).collect::<Vec<_>>()
    );
    println!(
        "filter(STATUS, Closed)       = {} (ids: {:?})",
        closed_tickets.len(),
        closed_tickets.iter().map(|(id, _)| id).collect::<Vec<_>>()
    );

    // Recovery tools: rebuild a single view, then rebuild every view, from
    // the event stream. Useful when a view becomes corrupted (e.g., lost
    // updates due to optimistic-lock conflicts).
    projection.rebuild(&"ticket-1".to_string()).await?;
    projection.rebuild_all().await?;
    let after_rebuild = projection.load_all().await?;
    println!(
        "load_all after rebuild_all   = {} rows (idempotent)",
        after_rebuild.len()
    );

    Ok(())
}

#[cfg(all(test, feature = "test-support"))]
mod tests {
    use event_sorcery::TestHarness;

    use super::*;

    fn clock() -> Arc<dyn Clock> {
        Arc::new(StepClock::new())
    }

    #[tokio::test]
    async fn open_then_close_emits_closed_event() {
        TestHarness::<SupportTicket>::with(clock())
            .given(vec![SupportTicketEvent::Opened {
                subject: "login broken".to_string(),
                at: "t0".to_string(),
            }])
            .when(SupportTicketCommand::Close)
            .await
            .then_expect_events(&[SupportTicketEvent::Closed {
                at: "t0".to_string(),
            }]);
    }

    #[tokio::test]
    async fn closing_twice_returns_already_closed() {
        let error = TestHarness::<SupportTicket>::with(clock())
            .given(vec![
                SupportTicketEvent::Opened {
                    subject: "login broken".to_string(),
                    at: "t0".to_string(),
                },
                SupportTicketEvent::Closed {
                    at: "t1".to_string(),
                },
            ])
            .when(SupportTicketCommand::Close)
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            event_sorcery::LifecycleError::Apply(SupportTicketError::AlreadyClosed)
        ));
    }

    #[tokio::test]
    async fn projection_filter_returns_only_matching_status() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!("../../migrations").run(&pool).await.unwrap();
        create_view_table(&pool).await.unwrap();

        let (store, projection) = StoreBuilder::<SupportTicket>::new(pool.clone())
            .build(clock())
            .await
            .unwrap();

        for id in ["a", "b", "c"] {
            store
                .send(
                    &id.to_string(),
                    SupportTicketCommand::Open {
                        subject: "x".to_string(),
                    },
                )
                .await
                .unwrap();
        }
        store
            .send(&"b".to_string(), SupportTicketCommand::Close)
            .await
            .unwrap();

        let open = projection.filter(STATUS, &Status::Open).await.unwrap();
        let mut open_ids: Vec<&String> = open.iter().map(|(id, _)| id).collect();
        open_ids.sort();
        assert_eq!(open_ids, vec![&"a".to_string(), &"c".to_string()]);

        let closed = projection.filter(STATUS, &Status::Closed).await.unwrap();
        assert_eq!(closed.len(), 1);
        assert_eq!(closed[0].0, "b");
    }

    #[tokio::test]
    async fn rebuild_all_replays_views_from_events_idempotently() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!("../../migrations").run(&pool).await.unwrap();
        create_view_table(&pool).await.unwrap();

        let (store, projection) = StoreBuilder::<SupportTicket>::new(pool.clone())
            .build(clock())
            .await
            .unwrap();

        store
            .send(
                &"a".to_string(),
                SupportTicketCommand::Open {
                    subject: "x".to_string(),
                },
            )
            .await
            .unwrap();
        store
            .send(&"a".to_string(), SupportTicketCommand::AwaitCustomer)
            .await
            .unwrap();

        projection.rebuild_all().await.unwrap();
        projection.rebuild_all().await.unwrap();

        let ticket = projection.load(&"a".to_string()).await.unwrap().unwrap();
        assert_eq!(ticket.status, Status::Pending);
    }
}
