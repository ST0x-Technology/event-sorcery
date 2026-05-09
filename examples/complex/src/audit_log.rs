//! Single-entity reactor: appends a line to an in-memory log for every
//! `Order` event. Demonstrates the `OneOf::into_inner` shorthand available
//! when a reactor depends on exactly one entity.

use async_trait::async_trait;
use cqrs_es::DomainEvent;
use tokio::sync::Mutex;

use event_sorcery::{EntityList, Never, Reactor, deps};

use crate::order::Order;

pub struct AuditLog {
    entries: Mutex<Vec<String>>,
}

impl AuditLog {
    pub fn new() -> Self {
        Self {
            entries: Mutex::new(Vec::new()),
        }
    }

    pub async fn entries(&self) -> Vec<String> {
        self.entries.lock().await.clone()
    }
}

deps!(AuditLog, [Order]);

#[async_trait]
impl Reactor for AuditLog {
    type Error = Never;

    async fn react(
        &self,
        event: <Self::Dependencies as EntityList>::Event,
    ) -> Result<(), Self::Error> {
        let (id, event) = event.into_inner();
        let line = format!("{id}: {}", event.event_type());
        self.entries.lock().await.push(line);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use sqlx::SqlitePool;

    use event_sorcery::{SpyReactor, StoreBuilder};

    use super::*;
    use crate::inventory::Sku;
    use crate::order::{OrderCommand, OrderId};

    fn widgets() -> Sku {
        Sku("widgets".to_string())
    }

    #[tokio::test]
    async fn appends_one_entry_per_dispatched_event() {
        let log = Arc::new(AuditLog::new());

        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();
        let store = StoreBuilder::<Order>::new(pool)
            .with(log.clone())
            .build(())
            .await
            .unwrap();

        let order = OrderId(7);
        store
            .send(
                &order,
                OrderCommand::Place {
                    item: widgets(),
                    quantity: 1,
                },
            )
            .await
            .unwrap();
        store.send(&order, OrderCommand::Fill).await.unwrap();

        let entries = log.entries().await;
        assert_eq!(
            entries,
            vec![
                "O-0007: OrderEvent::Placed".to_string(),
                "O-0007: OrderEvent::Filled".to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn spy_reactor_captures_dispatched_order_events() {
        let spy = SpyReactor::<Order>::new();

        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();
        let store = StoreBuilder::<Order>::new(pool)
            .with(Arc::new(spy.clone()))
            .build(())
            .await
            .unwrap();

        let order = OrderId(1);
        store
            .send(
                &order,
                OrderCommand::Place {
                    item: widgets(),
                    quantity: 3,
                },
            )
            .await
            .unwrap();
        store.send(&order, OrderCommand::Fill).await.unwrap();

        let captured = spy.events().await;
        let kinds: Vec<_> = captured
            .iter()
            .map(|(_, event)| event.event_type())
            .collect();
        assert_eq!(kinds, vec!["OrderEvent::Placed", "OrderEvent::Filled"]);
    }
}
