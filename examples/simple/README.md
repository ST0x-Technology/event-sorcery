# simple

Single-entity event-sourced example: a `SupportTicket` aggregate moving through
`Open -> Pending -> Closed`, with a materialized view backed by a SQLite
generated column for filtered queries, and a durable job -- closing a ticket
enqueues a `NotifyClosed` job that a supervised worker runs.

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
| Typed durable jobs: `type Jobs = jobs![…]`            | `NotifyClosed`, the `Close` handler          |
| `impl Job` + a supervised worker via the macro        | `NotifyClosed`, `build_supervised_worker!`   |
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

**Migrations live inside the example crate.** The canonical event-sorcery schema
(events + snapshots + the `job_queue` view table) and the view-table SQL are
committed under `migrations/` and applied with `sqlx::migrate!("./migrations")`.
This mirrors the real consumer layout: a downstream project copies the
event-sorcery schema into its own migration directory next to its own view
tables, rather than depending on a path inside the library's source tree. The
consumer owns migrations — `StoreBuilder::build()` and `JobRuntime::build()`
wire over an already-migrated pool and do not migrate themselves.

**Typed jobs instead of injected services.** Command handlers are pure
`(state, command) -> events` and take only a `JobQueue<Self::Jobs>` — no service
injection. Inputs that used to come from a service (here, the event timestamp)
are carried on the command instead, and side effects (notifying the customer)
become durable jobs. `type Jobs = jobs![NotifyClosed]` declares the job types
the entity may enqueue; `jobs.push(...)` is compile-checked to only accept a
declared job. The job is flushed in the same transaction that commits its
events, so it runs iff the close commits.

**`build_supervised_worker!`.** Wires a supervised apalis worker per job type
over one `JobRuntime`, so the consumer supplies only the `Job::Input` bundle
(`Notifier`) and the macro builds the claim/run/ack pipeline. `main` runs the
monitor briefly to drain the queue the `Close` command filled.

**Standalone enqueue.** Besides the handler path, a job can be enqueued directly
on the runtime with `runtime.enqueue(job)` (ADR-0007) — the path reactors,
pollers, and startup recovery use, since they have no command commit to ride.
`main` enqueues one `NotifyClosed` this way, and the worker drains it alongside
the command-born one. (Unlike the handler push, a standalone enqueue is its own
transaction, not atomic with whatever event or poll prompted it.)

## Migrating from an existing job system

This example enqueues jobs both from a command handler (the atomic-with-events
path) and standalone on the runtime. If you are moving an existing manual or
apalis-backed job system onto event-sorcery durable jobs — where jobs are
enqueued from reactors, polling loops, or other jobs, not command handlers — see
[`docs/migrating-to-durable-jobs.md`](../../docs/migrating-to-durable-jobs.md):
what event-sorcery covers, the enqueue-side prerequisites still landing, and the
safe per-kind cutover.
