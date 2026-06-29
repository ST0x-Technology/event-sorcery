//! `Inventory` aggregate: stock-on-hand per SKU with a materialized view.
//! Keeps the default `CompactionPolicy::Retain` -- the projection's
//! `rebuild_all` reads from the events table, so compacted entities would
//! be invisible to the rebuild path. See `Order` for the compactable case.

use std::fmt::{self, Display};
use std::str::FromStr;

use async_trait::async_trait;
use cqrs_es::DomainEvent;
use serde::{Deserialize, Serialize};

use event_sorcery::{EventSourced, JobQueue, Nil, Table};

/// Stock-keeping unit. Used both as `Inventory::Id` and as the foreign-key
/// payload on `Order::item`, so the two entities share an identifier currency.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Sku(pub String);

impl Display for Sku {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for Sku {
    type Err = std::convert::Infallible;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Ok(Self(value.to_string()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Inventory {
    pub item: Sku,
    pub on_hand: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum InventoryEvent {
    Initialized { item: Sku, on_hand: u32 },
    Restocked { added: u32 },
    Consumed { taken: u32 },
}

impl DomainEvent for InventoryEvent {
    fn event_type(&self) -> String {
        match self {
            Self::Initialized { .. } => "InventoryEvent::Initialized".to_string(),
            Self::Restocked { .. } => "InventoryEvent::Restocked".to_string(),
            Self::Consumed { .. } => "InventoryEvent::Consumed".to_string(),
        }
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
    }
}

#[derive(Debug, Clone)]
pub enum InventoryCommand {
    Initialize { item: Sku, on_hand: u32 },
    Restock { added: u32 },
    Consume { taken: u32 },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum InventoryError {
    #[error("inventory already initialized")]
    AlreadyInitialized,
    #[error("inventory has not been initialized")]
    NotInitialized,
    #[error("on-hand overflow")]
    Overflow,
    #[error("only {on_hand} units on hand, cannot consume {taken}")]
    Underflow { on_hand: u32, taken: u32 },
}

#[async_trait]
impl EventSourced for Inventory {
    type Id = Sku;
    type Event = InventoryEvent;
    type Command = InventoryCommand;
    type Error = InventoryError;
    type Jobs = Nil;
    type Materialized = Table;

    const AGGREGATE_TYPE: &'static str = "Inventory";
    const PROJECTION: Table = Table("inventory_view");
    const SCHEMA_VERSION: u64 = 1;

    fn originate(event: &InventoryEvent) -> Option<Self> {
        match event {
            InventoryEvent::Initialized { item, on_hand } => Some(Self {
                item: item.clone(),
                on_hand: *on_hand,
            }),
            InventoryEvent::Restocked { .. } | InventoryEvent::Consumed { .. } => None,
        }
    }

    fn evolve(entity: &Self, event: &InventoryEvent) -> Result<Option<Self>, InventoryError> {
        match event {
            InventoryEvent::Initialized { .. } => Ok(None),
            InventoryEvent::Restocked { added } => entity
                .on_hand
                .checked_add(*added)
                .map(|on_hand| {
                    Some(Self {
                        on_hand,
                        ..entity.clone()
                    })
                })
                .ok_or(InventoryError::Overflow),
            InventoryEvent::Consumed { taken } => entity
                .on_hand
                .checked_sub(*taken)
                .map(|on_hand| {
                    Some(Self {
                        on_hand,
                        ..entity.clone()
                    })
                })
                .ok_or(InventoryError::Underflow {
                    on_hand: entity.on_hand,
                    taken: *taken,
                }),
        }
    }

    async fn initialize(
        command: InventoryCommand,
        _jobs: &JobQueue<Self::Jobs>,
    ) -> Result<Vec<InventoryEvent>, InventoryError> {
        match command {
            InventoryCommand::Initialize { item, on_hand } => {
                Ok(vec![InventoryEvent::Initialized { item, on_hand }])
            }
            InventoryCommand::Restock { .. } | InventoryCommand::Consume { .. } => {
                Err(InventoryError::NotInitialized)
            }
        }
    }

    async fn transition(
        &self,
        command: InventoryCommand,
        _jobs: &JobQueue<Self::Jobs>,
    ) -> Result<Vec<InventoryEvent>, InventoryError> {
        match command {
            InventoryCommand::Initialize { .. } => Err(InventoryError::AlreadyInitialized),
            InventoryCommand::Restock { added } => {
                self.on_hand
                    .checked_add(added)
                    .ok_or(InventoryError::Overflow)?;
                Ok(vec![InventoryEvent::Restocked { added }])
            }
            InventoryCommand::Consume { taken } => {
                if taken > self.on_hand {
                    return Err(InventoryError::Underflow {
                        on_hand: self.on_hand,
                        taken,
                    });
                }
                Ok(vec![InventoryEvent::Consumed { taken }])
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use event_sorcery::TestHarness;

    use super::*;

    fn widgets() -> Sku {
        Sku("widgets".to_string())
    }

    #[tokio::test]
    async fn consume_more_than_on_hand_returns_underflow() {
        let error = TestHarness::<Inventory>::new()
            .given(vec![InventoryEvent::Initialized {
                item: widgets(),
                on_hand: 2,
            }])
            .when(InventoryCommand::Consume { taken: 5 })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            event_sorcery::LifecycleError::Apply(InventoryError::Underflow {
                on_hand: 2,
                taken: 5
            })
        ));
    }

    #[tokio::test]
    async fn restock_then_consume_settles_on_expected_balance() {
        TestHarness::<Inventory>::new()
            .given(vec![
                InventoryEvent::Initialized {
                    item: widgets(),
                    on_hand: 1,
                },
                InventoryEvent::Restocked { added: 4 },
            ])
            .when(InventoryCommand::Consume { taken: 3 })
            .await
            .then_expect_events(&[InventoryEvent::Consumed { taken: 3 }]);
    }
}
