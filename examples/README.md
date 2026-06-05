# Examples

Runnable, end-to-end examples of the `event-sorcery` crate. Two examples cover
the full surface: a simple single-entity flow, and a more realistic multi-entity
domain with cross-entity reactors. Patterns are validated against production
usage in `st0x.liquidity` and `st0x.issuance`.

| Example               | Domain                                                                                                                    |
| --------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| [`simple`](simple/)   | One entity (`SupportTicket`) with a materialized view and filtered queries.                                               |
| [`complex`](complex/) | Two entities (`Order`, `Inventory`) with two reactors (`StockAlert`, `AuditLog`), an injected `Notifier`, and compaction. |

## Run

Each example is a standalone Cargo project, _excluded_ from the workspace so it
mirrors what an external consumer's project layout looks like:

```bash
cargo run --manifest-path examples/simple/Cargo.toml
cargo run --manifest-path examples/complex/Cargo.toml
```

Each example carries `#[cfg(test)] mod tests` blocks next to the modules they
exercise, covering `replay`, `TestHarness`, `TestStore`, `SpyReactor`, and
`ReactorHarness`. Run all checks (fmt, clippy, nextest, the demo run) through
the helper script:

```bash
nu scripts/check-examples.nu
```

CI runs the same script.

Examples target SQLite, the backend currently bundled with this repo via
`crates/sqlite-es`. The `event-sorcery` crate is backend-agnostic in principle;
SQLite is the supported backend today.
