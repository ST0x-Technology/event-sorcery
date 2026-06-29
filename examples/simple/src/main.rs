//! Single-entity `event-sorcery` example: a support-ticket aggregate with a
//! materialized view (filtered queries via a SQLite generated column) and a
//! durable job -- closing a ticket enqueues a `NotifyClosed` job that a
//! supervised worker runs.
//!
//! Run with: `cargo run --manifest-path examples/simple/Cargo.toml`
//!
//! See `README.md` next to this file for design notes; see
//! `support_ticket.rs` for the entity definition, the job, view SQL, and tests.

use std::error::Error;
use std::time::Duration;

use sqlx::SqlitePool;

use event_sorcery::{Clock, JobRuntime, JobWorkerConfig, StoreBuilder, build_supervised_worker};

mod support_ticket;

use support_ticket::{
    Notifier, NotifyClosed, STATUS, Status, SupportTicket, SupportTicketCommand, TicketId,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = SqlitePool::connect(":memory:").await?;
    sqlx::migrate!("./migrations").run(&pool).await?;

    let (store, projection) = StoreBuilder::<SupportTicket>::new(pool.clone())
        .build()
        .await?;

    // Callers stamp the timestamp onto the command (handlers take no services).
    let now = chrono::Utc::now();
    let login = TicketId(1);
    let feature = TicketId(2);
    let billing = TicketId(3);

    for (id, subject) in [
        (login, "login broken"),
        (feature, "feature request"),
        (billing, "billing question"),
    ] {
        store
            .send(
                &id,
                SupportTicketCommand::Open {
                    subject: subject.to_string(),
                    at: now,
                },
            )
            .await?;
    }

    store
        .send(&feature, SupportTicketCommand::AwaitCustomer { at: now })
        .await?;
    // Closing enqueues a NotifyClosed job atomically with the Closed event.
    store
        .send(&billing, SupportTicketCommand::Close { at: now })
        .await?;

    let one = projection.load(&login).await?.ok_or("login missing")?;
    println!(
        "projection.load({login})       = {{ subject: {:?}, status: {:?} }}",
        one.subject, one.status
    );

    let open = projection.filter(STATUS, &Status::Open).await?;
    let pending = projection.filter(STATUS, &Status::Pending).await?;
    let closed = projection.filter(STATUS, &Status::Closed).await?;
    println!(
        "filter(STATUS, Open)         = {} (ids: {:?})",
        open.len(),
        open.iter().map(|(id, _)| id).collect::<Vec<_>>()
    );
    println!(
        "filter(STATUS, Pending)      = {} (ids: {:?})",
        pending.len(),
        pending.iter().map(|(id, _)| id).collect::<Vec<_>>()
    );
    println!(
        "filter(STATUS, Closed)       = {} (ids: {:?})",
        closed.len(),
        closed.iter().map(|(id, _)| id).collect::<Vec<_>>()
    );

    // Durable jobs: closing `billing` enqueued a NotifyClosed job. Wire a
    // supervised worker over the same database and run it briefly to drain it.
    let runtime = JobRuntime::build(pool.clone()).await?;

    // Standalone enqueue (ADR-0007): a job can also be enqueued directly on the
    // runtime, outside any command -- the path reactors, pollers, and startup
    // recovery use, since they have no command commit to ride.
    runtime
        .enqueue(NotifyClosed {
            subject: "startup sweep".to_string(),
        })
        .await?;

    let monitor = build_supervised_worker!(
        runtime,
        JobWorkerConfig::default(),
        Clock::system(),
        { NotifyClosed => Notifier }
    );
    println!("running the job worker briefly to drain the queue...");
    let _ = tokio::time::timeout(Duration::from_millis(750), monitor.run()).await;

    Ok(())
}
