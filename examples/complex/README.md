# complex

Multi-entity event-sourced example: an `Order` placed/filled/cancelled
aggregate, an `Inventory` aggregate with a materialized view, and two reactors
-- one watching both streams (alerts), one watching `Order` only (audit log).

## Run

```bash
cargo run --manifest-path examples/complex/Cargo.toml
cargo nextest run --manifest-path examples/complex/Cargo.toml
```

## What it covers

| Capability                                              | Where                                   |
| ------------------------------------------------------- | --------------------------------------- |
| Two entities, `Materialized = Nil` and `= Table`        | `order.rs`, `inventory.rs`              |
| `CompactionPolicy::CompactAfterSnapshot` + snapshotting | `order.rs`                              |
| Shared identifier across entities (`Sku`)               | `inventory.rs` owns it, `order.rs` uses |
| `deps!(Reactor, [A, B])` (multi-entity)                 | `stock_alert.rs`                        |
| `deps!(Reactor, [A])` (single-entity)                   | `audit_log.rs`                          |
| `.on(...).on(...).exhaustive()` over the entity list    | `StockAlert::react`                     |
| `OneOf::into_inner` for single-entity events            | `AuditLog::react`                       |
| Sharing one `Arc<R>` across multiple `StoreBuilder`s    | `main.rs` (`stock_alert.clone()`)       |
| Custom reactor wired alongside an auto-projection       | `StockAlert` on the `Inventory` builder |
| `StoreBuilder::with` on `Materialized = Nil`            | `AuditLog` on the `Order` builder       |
| Injected `Notifier` service                             | `stock_alert.rs`                        |
| Standalone helpers (`load_entity`, `count_aggregates`)  | `main.rs`                               |
| `compact_events` on a compactable aggregate             | `main.rs` (Order)                       |
| `SpyReactor`, `ReactorHarness`                          | `audit_log.rs`, `stock_alert.rs` tests  |

## Why these choices

**`Order` opts into compaction.** Orders hit a short-lived terminal state
(filled or cancelled), so `CompactionPolicy::CompactAfterSnapshot` with
`SNAPSHOT_SIZE = 1` lets `compact_events` reclaim every event covered by a
snapshot. Compactable entities must be `Materialized = Nil` -- the projection's
`rebuild_all` reads from the events table and would miss compacted aggregates.
Financial-audit aggregates that need every event preserved must keep the default
`CompactionPolicy::Retain`.

**Shared reactor via `Arc`.** A multi-entity reactor depends on multiple stores,
so the same `Arc<StockAlert>` is wired into each entity's `StoreBuilder` via
`.with()`. Cloning the `Arc` does not duplicate state -- both stores increment
the same atomics.

**`.on(...).on(...).exhaustive()`.** Each `.on()` consumes one entity from the
type-level entity list. `.exhaustive()` requires the remaining tail to be
`Never`, so missing a handler is a compile error rather than a runtime
fallthrough. Single-entity reactors get the dual `event.into_inner()` shorthand
(see `AuditLog`).

**Custom reactor + auto-projection.** `Materialized = Table` auto-wires a
`Projection<Inventory>` reactor; you can still register arbitrary additional
reactors via `.with()`. Here `StockAlert` runs alongside the `Inventory`
projection. This is how production systems compose: side-effects (alerts, logs,
downstream publishing) sit next to the read-side view without the wiring code
knowing about either.

**Two named `Notifier` impls (`LogNotifier`, `RecordingNotifier`)** instead of
one configurable mock with booleans -- distinct types make it obvious at the
call site which behavior is in play.

**Migrations live inside the example crate.** Both the canonical event-sorcery
schema (events + snapshots tables) and the inventory view table are committed
under `migrations/` and applied with `sqlx::migrate!("./migrations")`. This
mirrors the real consumer layout: a downstream project copies the event-sorcery
schema into its own migration directory next to its own view tables, rather than
depending on a path inside the library's source tree.
