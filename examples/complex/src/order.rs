//! `Order` aggregate: a placed/filled/cancelled state machine, no
//! materialized view. Reads go through `Store::load` (replay from events).
//!
//! Orders hit a short-lived terminal state (filled or cancelled), so the
//! aggregate opts into `CompactionPolicy::CompactAfterSnapshot`. Once a
//! snapshot covers an event, `compact_events` can reclaim it; the snapshot
//! becomes the durable record of pre-compaction state, hence
//! `SNAPSHOT_SIZE = 1`. Compactable entities must be `Materialized = Nil`
//! -- the projection rebuild path reads from the events table and would
//! miss compacted aggregates.

use std::fmt::{self, Display};
use std::str::FromStr;

use cqrs_es::DomainEvent;
use serde::{Deserialize, Serialize};

use event_sorcery::{CompactionPolicy, EventSourced, JobQueue, Nil};

use crate::inventory::Sku;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OrderId(pub u64);

impl Display for OrderId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "O-{:04}", self.0)
    }
}

impl FromStr for OrderId {
    type Err = ParseOrderIdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let suffix = value
            .strip_prefix("O-")
            .ok_or(ParseOrderIdError::MissingPrefix)?;
        let raw = suffix.parse::<u64>()?;
        Ok(Self(raw))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ParseOrderIdError {
    #[error("order id must start with 'O-'")]
    MissingPrefix,
    #[error("order id suffix is not numeric: {0}")]
    NotNumeric(#[from] std::num::ParseIntError),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Order {
    pub item: Sku,
    pub quantity: u32,
    pub status: OrderStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderStatus {
    Placed,
    Filled,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderEvent {
    Placed { item: Sku, quantity: u32 },
    Filled,
    Cancelled,
}

impl DomainEvent for OrderEvent {
    fn event_type(&self) -> String {
        match self {
            Self::Placed { .. } => "OrderEvent::Placed".to_string(),
            Self::Filled => "OrderEvent::Filled".to_string(),
            Self::Cancelled => "OrderEvent::Cancelled".to_string(),
        }
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
    }
}

#[derive(Debug, Clone)]
pub enum OrderCommand {
    Place { item: Sku, quantity: u32 },
    Fill,
    Cancel,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum OrderError {
    #[error("order already placed")]
    AlreadyPlaced,
    #[error("order has not been placed")]
    NotPlaced,
    #[error("order is no longer open")]
    NotOpen,
}

impl EventSourced for Order {
    type Id = OrderId;
    type Event = OrderEvent;
    type Command = OrderCommand;
    type Error = OrderError;
    type Jobs = Nil;
    type Materialized = Nil;

    const AGGREGATE_TYPE: &'static str = "Order";
    const PROJECTION: Nil = Nil;
    const SCHEMA_VERSION: u64 = 1;
    const COMPACTION_POLICY: CompactionPolicy = CompactionPolicy::CompactAfterSnapshot;
    const SNAPSHOT_SIZE: usize = 1;

    fn originate(event: &OrderEvent) -> Option<Self> {
        match event {
            OrderEvent::Placed { item, quantity } => Some(Self {
                item: item.clone(),
                quantity: *quantity,
                status: OrderStatus::Placed,
            }),
            OrderEvent::Filled | OrderEvent::Cancelled => None,
        }
    }

    fn evolve(entity: &Self, event: &OrderEvent) -> Result<Option<Self>, OrderError> {
        match event {
            OrderEvent::Placed { .. } => Ok(None),
            OrderEvent::Filled => Ok(Some(Self {
                status: OrderStatus::Filled,
                ..entity.clone()
            })),
            OrderEvent::Cancelled => Ok(Some(Self {
                status: OrderStatus::Cancelled,
                ..entity.clone()
            })),
        }
    }

    fn initialize(
        command: OrderCommand,
        _jobs: &mut JobQueue<Self::Jobs>,
    ) -> Result<Vec<OrderEvent>, OrderError> {
        match command {
            OrderCommand::Place { item, quantity } => {
                Ok(vec![OrderEvent::Placed { item, quantity }])
            }
            OrderCommand::Fill | OrderCommand::Cancel => Err(OrderError::NotPlaced),
        }
    }

    fn transition(
        &self,
        command: OrderCommand,
        _jobs: &mut JobQueue<Self::Jobs>,
    ) -> Result<Vec<OrderEvent>, OrderError> {
        match command {
            OrderCommand::Place { .. } => Err(OrderError::AlreadyPlaced),
            OrderCommand::Fill => match self.status {
                OrderStatus::Placed => Ok(vec![OrderEvent::Filled]),
                OrderStatus::Filled | OrderStatus::Cancelled => Err(OrderError::NotOpen),
            },
            OrderCommand::Cancel => match self.status {
                OrderStatus::Placed => Ok(vec![OrderEvent::Cancelled]),
                OrderStatus::Filled | OrderStatus::Cancelled => Err(OrderError::NotOpen),
            },
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
    async fn fill_after_place_emits_filled_event() {
        TestHarness::<Order>::with()
            .given(vec![OrderEvent::Placed {
                item: widgets(),
                quantity: 3,
            }])
            .when(OrderCommand::Fill)
            .await
            .then_expect_events(&[OrderEvent::Filled]);
    }

    #[tokio::test]
    async fn cancel_filled_order_returns_not_open() {
        let error = TestHarness::<Order>::with()
            .given(vec![
                OrderEvent::Placed {
                    item: widgets(),
                    quantity: 1,
                },
                OrderEvent::Filled,
            ])
            .when(OrderCommand::Cancel)
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            event_sorcery::LifecycleError::Apply(OrderError::NotOpen)
        ));
    }
}
