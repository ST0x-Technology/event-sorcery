//! Multi-entity `event-sorcery` example: an order/inventory domain with
//! one reactor watching both event streams (alerts) and one reactor
//! watching `Order` only (audit log). Placing an order also enqueues a
//! durable `SendOrderConfirmation` job that a supervised worker runs.
//!
//! Run with: `cargo run --manifest-path examples/complex/Cargo.toml`
//!
//! See `README.md` next to this file for design notes; see each module
//! for the entity/reactor definitions and tests.

use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use sqlx::SqlitePool;

use event_sorcery::{
    Clock, JobRuntime, JobWorkerConfig, StoreBuilder, build_supervised_worker, compact_events,
    count_aggregates, load_entity,
};

mod audit_log;
mod inventory;
mod order;
mod stock_alert;

use audit_log::AuditLog;
use inventory::{Inventory, InventoryCommand, Sku};
use order::{Confirmer, Order, OrderCommand, OrderId, SendOrderConfirmation};
use stock_alert::{LogNotifier, Notifier, StockAlert};

const LOW_WATER: u32 = 2;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = SqlitePool::connect(":memory:").await?;
    sqlx::migrate!("./migrations").run(&pool).await?;

    // One reactor instance, shared across both stores. cqrs-es dispatches
    // every event to every registered reactor for that aggregate type;
    // cloning the Arc does not duplicate StockAlert's internal state.
    let notifier: Arc<dyn Notifier> = Arc::new(LogNotifier);
    let stock_alert = Arc::new(StockAlert::new(LOW_WATER, notifier));
    let audit = Arc::new(AuditLog::new());

    // Order is Materialized = Nil -- no projection tuple in the return type.
    let orders = StoreBuilder::<Order>::new(pool.clone())
        .with(stock_alert.clone())
        .with(audit.clone())
        .build()
        .await?;

    // Inventory is Materialized = Table -- the auto-wired projection is
    // returned alongside the store, and our custom reactor runs next to it.
    let (inventory, inventory_projection) = StoreBuilder::<Inventory>::new(pool.clone())
        .with(stock_alert.clone())
        .build()
        .await?;

    let widgets = Sku("widgets".to_string());
    let order_one = OrderId(1);
    let order_two = OrderId(2);

    inventory
        .send(
            &widgets,
            InventoryCommand::Initialize {
                item: widgets.clone(),
                on_hand: 1,
            },
        )
        .await?;
    inventory
        .send(&widgets, InventoryCommand::Restock { added: 5 })
        .await?;

    orders
        .send(
            &order_one,
            OrderCommand::Place {
                item: widgets.clone(),
                quantity: 3,
            },
        )
        .await?;
    orders.send(&order_one, OrderCommand::Fill).await?;
    inventory
        .send(&widgets, InventoryCommand::Consume { taken: 3 })
        .await?;

    orders
        .send(
            &order_two,
            OrderCommand::Place {
                item: widgets.clone(),
                quantity: 1,
            },
        )
        .await?;
    orders.send(&order_two, OrderCommand::Cancel).await?;

    println!(
        "stock_alert.fills            = {}",
        stock_alert.fills.load(Ordering::SeqCst)
    );
    println!(
        "stock_alert.low_stock_alerts = {} (initial on_hand=1 was at/under the threshold)",
        stock_alert.low_stock_alerts.load(Ordering::SeqCst)
    );

    let log = audit.entries().await;
    println!("audit.entries                = {} lines", log.len());
    for line in &log {
        println!("    {line}");
    }

    let inventory_state = inventory_projection
        .load(&widgets)
        .await?
        .ok_or("widgets missing")?;
    println!(
        "inventory_projection         = {{ item: {}, on_hand: {} }}",
        inventory_state.item, inventory_state.on_hand
    );

    // Standalone helpers -- useful for CLI / migration code that does not
    // hold a long-lived Store. `load_entity` replays from events; for
    // Materialized = Table entities a Projection::load would be cheaper.
    let order_via_helper = load_entity::<Order>(&pool, &order_one)
        .await?
        .ok_or("order missing")?;
    println!(
        "load_entity(order_one)       = {{ item: {}, quantity: {}, status: {:?} }}",
        order_via_helper.item, order_via_helper.quantity, order_via_helper.status
    );

    let total_orders = count_aggregates::<Order>(&pool).await?;
    println!("count_aggregates::<Order>    = {total_orders}");

    // Order opts into CompactionPolicy::CompactAfterSnapshot so events
    // covered by a snapshot can be reclaimed. SNAPSHOT_SIZE = 1 means every
    // command wrote a snapshot, so most order events are eligible.
    // Inventory keeps the default Retain because its projection rebuild path
    // reads from the events table.
    let deleted = compact_events::<Order>(&pool).await?;
    println!("compact_events(Order)        = {deleted} events reclaimed");

    // Durable jobs: placing each order enqueued a SendOrderConfirmation job
    // atomically with its Placed event. Wire a supervised worker over the same
    // database and run it briefly to drain the queue.
    let runtime = JobRuntime::build(pool.clone()).await?;
    let monitor = build_supervised_worker!(
        runtime,
        JobWorkerConfig::default(),
        Clock::system(),
        { SendOrderConfirmation => Confirmer }
    );
    println!("running the job worker briefly to drain the queue...");
    let _ = tokio::time::timeout(std::time::Duration::from_millis(750), monitor.run()).await;

    Ok(())
}
