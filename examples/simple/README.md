# simple

Single-entity event-sourced example: a `SupportTicket` aggregate moving through
`Open -> Pending -> Closed`, whose commands carry their own timestamp and a
materialized view backed by a SQLite generated column for filtered queries.

## Run

```bash
cargo run --manifest-path examples/simple/Cargo.toml
cargo nextest run --manifest-path examples/simple/Cargo.toml
```

## What it covers

| Capability                                            | Where                                        |
| ----------------------------------------------------- | -------------------------------------------- |
| `impl EventSourced` (all four methods)                | `support_ticket.rs`                          |
| Typed `Id` with `Display` + `FromStr`                 | `TicketId`                                   |
| Domain error with `thiserror`                         | `SupportTicketError`                         |
| `type Materialized = Table` + `PROJECTION = Table`    | trait consts                                 |
| Sync, pure handlers; reads carried on the `Command`   | `SupportTicketCommand { at }`                |
| `type Jobs = Nil` (entity dispatches no jobs)         | trait assoc type                             |
| View-table SQL with a generated column                | `migrations/*_support_ticket_view.sql`       |
| `StoreBuilder::build()` returning a tuple             | `main.rs`                                    |
| `Projection::load`, `load_all`, `filter`, `rebuild_*` | `main.rs`                                    |
| `Column` constant pattern                             | `const STATUS: Column = …`                   |
| `replay`, `TestHarness`, `TestStore`                  | `#[cfg(test)] mod tests` in `support_ticket` |

## Why these choices

**`type Materialized = Table`.** Reads go through the projection (a denormalized
SQLite table), not by replaying events on every read. Use this when you have a
read pattern that benefits from a query-shaped table — filtering by status is
the canonical case.

**Generated column on `$.Live.status`.** The library wraps each entity in a
`Lifecycle::Live(...)` enum before serializing, so struct fields land at
`$.Live.<field>` in the JSON payload. That path is stable enough to feed a
SQLite generated column, which lets `Projection::filter` push the predicate into
SQL. Enum-typed entities have variant-dependent paths and are unsuitable for
generated columns; prefer `load_all` + filter-in-Rust there.

**Migrations live inside the example crate.** Both the canonical event-sorcery
schema (events + snapshots tables) and the view-table SQL are committed under
`migrations/` and applied with `sqlx::migrate!("./migrations")`. This mirrors
the real consumer layout: a downstream project copies the event-sorcery schema
into its own migration directory next to its own view tables, rather than
depending on a path inside the library's source tree.

**Commands carry their own timestamp.** Handlers are sync and pure — no external
dependencies, no I/O. Anything a handler would otherwise read at dispatch (here,
the current time) is supplied by the caller on the `Command`:
`Open { subject,
at }`, `Close { at }`. Production passes `chrono::Utc::now()`;
tests pass a fixed instant for reproducible event payloads. This keeps the write
path deterministic and testable without injecting a clock.

**`type Jobs = Nil`.** This entity dispatches no side-effect jobs, so its job
list is empty. An entity that needs durable side effects declares
`type Jobs =
jobs![...]` and enqueues through the handler's `JobQueue` — see
`docs/cqrs.md`.
