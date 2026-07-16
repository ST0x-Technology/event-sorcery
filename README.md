# Event Sorcery

Event-sourcing primitives in Rust. A thin, opinionated layer on top of
[`cqrs-es`](https://crates.io/crates/cqrs-es) plus a SQLite-backed event store.

## Crates

- **[`crates/sqlite-es`](crates/sqlite-es)** — SQLite implementation of
  `cqrs-es`'s event repository, view repository, and `CqrsFramework` glue.
  Standalone — usable wherever you'd plug in a `cqrs-es` backend.
- **[`crates/event-sorcery`](crates/event-sorcery)** — higher-level ergonomics
  on top of `sqlite-es`: the `EventSourced` trait, `Lifecycle` adapter, typed
  `Store`, projections, schema registry, reactor.

`event-sorcery` is the recommended entry point. Use `sqlite-es` directly only if
you need lower-level control.

`Store::send()` serializes commands per aggregate ID, so reactors and
projections always observe events for a given aggregate in commit order. This
comes with a reentrancy rule (a reactor commanding the same aggregate it is
reacting to fails fast instead of deadlocking) and a cross-aggregate cycle
caveat — see [`docs/cqrs.md`](docs/cqrs.md) and
[ADR-0004](adrs/0004-per-aggregate-command-serialization.md) for details.

## Status

Extracted from internal services
([st0x.issuance](https://github.com/ST0x-Technology/st0x.issuance) and
[st0x.liquidity](https://github.com/ST0x-Technology/st0x.liquidity)) and made
standalone so they can share the implementation. External users are welcome but
the API surface is still in flux.

## Examples

Runnable end-to-end examples live under [`examples/`](examples/) — one directory
per concept (`basic_entity`, `projection`, `reactor`), each with its own README
explaining what it covers. Run any of them with
`cargo run -p event-sorcery --example <name>`.

## Documentation

- [`docs/domain.md`](docs/domain.md) — domain terminology and naming
  conventions.
- [`docs/cqrs.md`](docs/cqrs.md) — event-sourcing patterns, the `EventSourced`
  trait, projections, services, schema registry.
- [`docs/sqlx.md`](docs/sqlx.md) — `SQLX_OFFLINE`, `query!` vs runtime queries,
  regenerating the query cache.
- [`docs/ttdd.md`](docs/ttdd.md) — type-driven TDD methodology used in this
  project.

## Development

```bash
nix develop          # rust toolchain + sqlx-cli + cargo-nextest
cargo check --workspace
cargo nextest run --workspace
```

See [AGENTS.md](AGENTS.md) for contribution conventions.

## License

MIT — see [LICENSE](LICENSE).
