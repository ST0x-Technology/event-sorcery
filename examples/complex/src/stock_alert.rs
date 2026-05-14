//! Multi-entity reactor: watches both `Order` and `Inventory` event
//! streams, raising a `Notifier` alert when an order fills or stock falls
//! to/under a low-water threshold.
//!
//! The `deps!` macro declares the entity-list dependencies and generates
//! the `Dependent` and `HasEntity` impls used by `ReactorHarness::receive`.
//! `.on(...).on(...).exhaustive()` enforces compile-time exhaustiveness
//! over the entity list -- a missing handler is a build error, not a
//! runtime fallthrough.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
#[cfg(test)]
use tokio::sync::Mutex;

use event_sorcery::{EntityList, Never, Reactor, deps};

use crate::inventory::{Inventory, InventoryEvent, Sku};
use crate::order::{Order, OrderEvent, OrderId};

/// External notification gateway. Production wires `LogNotifier` (or a real
/// transport); tests wire `RecordingNotifier` to assert on what would have
/// been sent. Two named impls beat one configurable mock with booleans --
/// the type at the call site documents which behavior is in play.
#[async_trait]
pub trait Notifier: Send + Sync {
    async fn alert(&self, message: String);
}

pub struct LogNotifier;

#[async_trait]
impl Notifier for LogNotifier {
    async fn alert(&self, message: String) {
        println!("[ALERT] {message}");
    }
}

#[cfg(test)]
pub struct RecordingNotifier {
    messages: Mutex<Vec<String>>,
}

#[cfg(test)]
impl RecordingNotifier {
    pub fn new() -> Self {
        Self {
            messages: Mutex::new(Vec::new()),
        }
    }

    pub async fn messages(&self) -> Vec<String> {
        self.messages.lock().await.clone()
    }
}

#[cfg(test)]
#[async_trait]
impl Notifier for RecordingNotifier {
    async fn alert(&self, message: String) {
        self.messages.lock().await.push(message);
    }
}

pub struct StockAlert {
    low_water: u32,
    notifier: Arc<dyn Notifier>,
    pub fills: AtomicUsize,
    pub low_stock_alerts: AtomicUsize,
}

impl StockAlert {
    pub fn new(low_water: u32, notifier: Arc<dyn Notifier>) -> Self {
        Self {
            low_water,
            notifier,
            fills: AtomicUsize::new(0),
            low_stock_alerts: AtomicUsize::new(0),
        }
    }
}

deps!(StockAlert, [Order, Inventory]);

#[async_trait]
impl Reactor for StockAlert {
    type Error = Never;

    async fn react(
        &self,
        event: <Self::Dependencies as EntityList>::Event,
    ) -> Result<(), Self::Error> {
        event
            .on(|id: OrderId, event: OrderEvent| async move {
                if matches!(event, OrderEvent::Filled) {
                    self.fills.fetch_add(1, Ordering::SeqCst);
                    self.notifier.alert(format!("order {id} filled")).await;
                }
            })
            .on(|sku: Sku, event: InventoryEvent| async move {
                let new_on_hand = match event {
                    InventoryEvent::Initialized { on_hand, .. } => on_hand,
                    InventoryEvent::Restocked { .. } | InventoryEvent::Consumed { .. } => {
                        return;
                    }
                };
                if new_on_hand <= self.low_water {
                    self.low_stock_alerts.fetch_add(1, Ordering::SeqCst);
                    self.notifier
                        .alert(format!("inventory {sku} low: {new_on_hand} on hand"))
                        .await;
                }
            })
            .exhaustive()
            .await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use event_sorcery::ReactorHarness;

    use super::*;

    fn widgets() -> Sku {
        Sku("widgets".to_string())
    }

    #[tokio::test]
    async fn reactor_harness_dispatches_to_each_entity_arm() {
        let recorder = Arc::new(RecordingNotifier::new());
        let alert = StockAlert::new(2, recorder.clone());
        let harness = ReactorHarness::new(alert);

        harness
            .receive::<Order>(OrderId(1), OrderEvent::Filled)
            .await
            .unwrap();
        harness
            .receive::<Inventory>(
                widgets(),
                InventoryEvent::Initialized {
                    item: widgets(),
                    on_hand: 1,
                },
            )
            .await
            .unwrap();

        let alert = harness.inner();
        assert_eq!(alert.fills.load(Ordering::SeqCst), 1);
        assert_eq!(alert.low_stock_alerts.load(Ordering::SeqCst), 1);

        let messages = recorder.messages().await;
        assert_eq!(
            messages,
            vec![
                "order O-0001 filled".to_string(),
                "inventory widgets low: 1 on hand".to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn restock_and_consume_events_do_not_trigger_alerts() {
        let recorder = Arc::new(RecordingNotifier::new());
        let alert = StockAlert::new(2, recorder.clone());
        let harness = ReactorHarness::new(alert);

        harness
            .receive::<Inventory>(widgets(), InventoryEvent::Restocked { added: 10 })
            .await
            .unwrap();
        harness
            .receive::<Inventory>(widgets(), InventoryEvent::Consumed { taken: 5 })
            .await
            .unwrap();

        assert_eq!(
            harness.inner().low_stock_alerts.load(Ordering::SeqCst),
            0,
            "low-stock alerts should only fire on Initialized at/below threshold"
        );
        assert!(recorder.messages().await.is_empty());
    }
}
