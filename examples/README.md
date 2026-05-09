# Examples

Runnable, end-to-end examples of the `event-sorcery` crate. Each example is a
single `main.rs` plus a `README.md` explaining what it covers and why. Patterns
are validated against real production usage in `st0x.liquidity` and
`st0x.issuance`.

| Example                         | Concept                                                                                                                                     |
| ------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| [`basic_entity`](basic_entity/) | An `EventSourced` entity without a materialized view, plus the standalone helpers (`load_entity`, `send_command`, `compact_events`, …).     |
| [`projection`](projection/)     | An entity with `Materialized = Table`, filtered queries through a SQLite generated column, and view recovery via `rebuild` / `rebuild_all`. |
| [`reactor`](reactor/)           | Multi-entity `Reactor` shared across two stores, plus a single-entity reactor wired alongside an auto-projection.                           |

## Run

```bash
cargo run -p event-sorcery --example basic_entity
cargo run -p event-sorcery --example projection
cargo run -p event-sorcery --example reactor
```

Each example also has a test module covering the library's testing helpers
(`replay`, `TestHarness`, `TestStore`, `SpyReactor`, `ReactorHarness`). Tests
are gated behind the `test-support` feature so the example binaries themselves
build without it:

```bash
cargo nextest run -p event-sorcery --examples --features test-support
```

Examples target SQLite by default — that's the backend currently bundled with
this repo via `crates/sqlite-es`. The `event-sorcery` crate is backend-agnostic
in principle; SQLite is the supported backend today.
