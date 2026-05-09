# projection

Materialized view for an event-sourced entity, with filtered queries backed by a
SQLite generated column. A `SupportTicket` aggregate moves through
`Open -> Pending -> Closed`, and the `support_ticket_view` table extracts
`status` from the JSON payload so callers can filter by it without scanning
every row.

## Run

```bash
cargo run -p event-sorcery --example projection
cargo nextest run -p event-sorcery --example projection --features test-support
```

The first command runs `main()` against an in-memory SQLite event store plus a
freshly-created view table. The second runs the example's tests covering
`TestHarness`, `Projection::filter`, and `rebuild_all`.

## What it covers

| Capability                                         | Where in `main.rs`               |
| -------------------------------------------------- | -------------------------------- |
| `type Materialized = Table` + `PROJECTION = Table` | trait constants                  |
| Domain service via `type Services = Arc<dyn …>`    | `Clock` trait + `StepClock` stub |
| Inline view-table SQL with a generated column      | `create_view_table`              |
| `StoreBuilder::build(clock)` returning a tuple     | start of `main`                  |
| `Projection::load(&id)` (single)                   | `ticket_one`                     |
| `Projection::load_all()`                           | bulk read block                  |
| `Projection::filter(COLUMN, &typed_value)`         | three filtered reads             |
| `Column` constant pattern                          | `const STATUS: Column = …`       |
| `Projection::rebuild` and `rebuild_all`            | recovery block                   |

The `#[cfg(all(test, feature = "test-support"))] mod tests` block adds
`TestHarness` BDD-style command verification and an end-to-end test that sends
real commands and asserts on `Projection::filter` results.

## Why these choices

**`type Materialized = Table`.** Reads go through the projection (a denormalized
SQLite table), not by replaying events for every read. Use this when you have a
read pattern that benefits from a query-shaped table, especially with filtering
— see `Projection::filter` below.

**Auto-wiring.** `StoreBuilder::build()` returns `(Arc<Store>, Arc<Projection>)`
for `Materialized = Table` entities. Forgetting to wire a projection becomes a
compile error rather than silent staleness.

**Inline view-table SQL.** The workspace migrations only ship the events /
snapshots / schema_registry tables; consumers own their view tables. The example
creates `support_ticket_view` inline before `StoreBuilder::build()` so the
projection has somewhere to write. In a real project, this would live in a
migration alongside the events schema.

**Generated columns and the `$.Live.…` JSON path.** Inside the database, each
entity is wrapped in a `Lifecycle::Live(...)` enum. For struct-typed entities
like `SupportTicket`, fields land at JSON path `$.Live.<field>` and are stable
enough to use in a generated column. Enum-typed entities have variant-dependent
paths and are unsuitable for generated columns — prefer `load_all` +
filter-in-Rust there.

**`Services = Arc<dyn Clock>`.** Demonstrates the typical pattern for injecting
an external dependency into command handlers. The example uses a deterministic
stub for reproducible output; production code wires `chrono::Utc::now()` through
the same trait. `()` is fine for entities that don't need any service.

**`rebuild` / `rebuild_all` as recovery tools.** `Projection` retries on
optimistic-lock conflicts, but if a view is corrupted (e.g., a previous deploy
lost an update), `rebuild_all` deletes every row and replays the event stream
from scratch. It's idempotent — safe to call at startup when you suspect a view
is stale.
