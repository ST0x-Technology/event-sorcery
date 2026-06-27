//! Single-entity `event-sorcery` example: a support-ticket aggregate whose
//! commands carry their own timestamp and a materialized view that supports
//! filtered queries via a SQLite generated column.
//!
//! Run with: `cargo run --manifest-path examples/simple/Cargo.toml`
//!
//! See `README.md` next to this file for design notes; see
//! `support_ticket.rs` for the entity definition, view SQL, and tests.

use std::error::Error;

use sqlx::SqlitePool;

use event_sorcery::StoreBuilder;

mod support_ticket;

use support_ticket::{STATUS, Status, SupportTicket, SupportTicketCommand, TicketId};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = SqlitePool::connect(":memory:").await?;
    sqlx::migrate!("./migrations").run(&pool).await?;

    let (store, projection) = StoreBuilder::<SupportTicket>::new(pool.clone())
        .build()
        .await?;

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
                    at: chrono::Utc::now(),
                },
            )
            .await?;
    }

    store
        .send(
            &feature,
            SupportTicketCommand::AwaitCustomer {
                at: chrono::Utc::now(),
            },
        )
        .await?;
    store
        .send(
            &billing,
            SupportTicketCommand::Close {
                at: chrono::Utc::now(),
            },
        )
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

    projection.rebuild_all().await?;
    let after_rebuild = projection.load_all().await?;
    println!(
        "load_all after rebuild_all   = {} rows (idempotent)",
        after_rebuild.len()
    );

    Ok(())
}
