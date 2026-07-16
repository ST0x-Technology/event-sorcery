# SQLx

## Offline mode (`SQLX_OFFLINE=true`)

The nix dev shell sets `SQLX_OFFLINE=true` (in `flake.nix`), which tells sqlx
compile-time macros (`query!`, `query_scalar!`, etc.) to use cached metadata
from the `.sqlx/` directory instead of connecting to the database. This means:

- Regular `cargo check`/`cargo nextest run` typically don't need a running
  database -- they read from `.sqlx/` cache files checked into version control.
  Exception: test code using `query!` macros (see below).
- If you add or change a `query!` macro invocation, you must regenerate the
  cache before the change will compile under `SQLX_OFFLINE=true`.

## Regenerating the query cache

```bash
cargo sqlx prepare --workspace -- --all-targets
```

Then check the updated `.sqlx/` files into version control.

### Pitfall: `#[cfg(test)]` queries don't work with offline mode

`cargo sqlx prepare` does NOT collect queries from `#[cfg(test)]` code, even
with `-- --all-targets`. This is a known limitation -- the `--all-targets` flag
is supposed to compile test targets during preparation, but in practice the
test-only queries are silently skipped.

When you then run `cargo nextest run` (which enables `cfg(test)`), the compiler
sees the query macro, finds no cached metadata, and fails with:

```text
`SQLX_OFFLINE=true` but there is no cached data for this query
```

**The fix: use runtime query functions in test code.** Instead of the
compile-time macro `sqlx::query_scalar!("...")`, use the runtime function
`sqlx::query_scalar("...")`. The non-macro version doesn't need offline cache
entries. Since test code runs against a real in-memory database anyway,
compile-time query verification adds no value.

```rust
// BROKEN in offline mode -- macro needs cache entry that prepare won't generate
#[cfg(test)]
let count = sqlx::query_scalar!("SELECT COUNT(*) FROM my_table")
    .fetch_one(pool).await?;

// WORKS -- runtime query, no cache needed
#[cfg(test)]
let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM my_table")
    .fetch_one(pool).await?;
```

Note the type annotation on the `let` binding -- the runtime function doesn't
infer return types like the macro does.

## `SQLITE_BUSY` vs `SQLITE_BUSY_SNAPSHOT`

`sqlx-sqlite` surfaces both as the _extended_ result code via
`DatabaseError::code()`:

- `SQLITE_BUSY` = `"5"` -- ordinary lock wait. `busy_timeout` usually covers
  this by retrying internally within the configured window.
- `SQLITE_BUSY_SNAPSHOT` = `"517"` -- WAL write-write conflict detection.
  `busy_timeout` does **not** retry this at all; the transaction must be rolled
  back and re-begun on a fresh snapshot by the caller.

`crates/event-sorcery/src/reactor.rs`'s `is_retryable_sqlite_busy` classifies
these -- and the rest of the `SQLITE_BUSY` extended-code family, i.e. any
extended code whose primary code is `5`, which also covers
`SQLITE_BUSY_RECOVERY` (`"261"`) and `SQLITE_BUSY_TIMEOUT` (`"773"`) -- as
retryable. It walks the error's `source()` chain, downcasting each node to both
`sqlx::Error` (matching its `Database` variant) and `Box<dyn DatabaseError>`,
since a `#[error(transparent)]` sqlx wrapper delegates `source()` straight past
the `sqlx::Error` node onto the boxed database error one hop in.

### Pitfall: the source-chain walk has a real, only partially-closable blind spot

`cqrs-es`'s own `PersistenceError::ConnectionError(Box<dyn Error>)` and
`AggregateError::DatabaseConnectionError(Box<dyn Error>)` are declared with
plain `#[error("{0}")]` and **no** `#[source]`/`#[from]` on the boxed field.
`thiserror` only wires `source()` for fields marked `#[source]`/`#[from]`/named
`source`, so a naive `.source()`-only walk stalls at the first
`PersistenceError`/`AggregateError` it hits and never reaches the `sqlx::Error`
sealed inside.

Two sub-cases, and they are not symmetric:

- `cqrs_es::persist::PersistenceError` is a **concrete, non-generic** public
  enum. Its boxed field is reachable without knowing any `Entity` type --
  `error.downcast_ref::<PersistenceError>()` works from fully generic code.
  `is_retryable_sqlite_busy` special-cases it: match `ConnectionError`/
  `UnknownError` and manually continue the walk into the boxed inner error as
  the next "source". This closes the gap for any reactor error chain that passes
  through `PersistenceError` directly (e.g. via `ProjectionError<Entity>` or
  `sqlite_event_repository.rs`'s conversions).
- `cqrs_es::AggregateError<T>` (what `Store::send`'s error type resolves to) is
  **generic over `T`**. Downcasting to a concrete
  `AggregateError<LifecycleError<Entity>>` requires naming `Entity`, which is
  unknowable at a generic classification site. This half of the gap is **not
  closable** without a new bound on `Reactor` or a downstream-supplied hook --
  both out of scope. A busy error originating inside a nested `Store::send()`
  call (cross-aggregate orchestration) stays unreachable and
  `is_retryable_sqlite_busy` fails closed ("not retryable") for it. This is the
  correct default: don't retry what can't be positively identified as a safe
  SQLite conflict.

### Pitfall: `#[error(transparent)]` breaks the walk differently than expected

`#[error(transparent)]` is the idiomatic thiserror pattern for "just forward a
foreign error", and it changes what `source()` returns in a way that could
defeat the walk above: thiserror generates `source()` to delegate to the
_wrapped field's own_ `source()`, not to return the field itself. So a variant
like `#[error(transparent)] Sqlx(#[from] sqlx::Error)` never surfaces the
wrapped `sqlx::Error` node to the walk directly -- it's skipped straight
through, and the walk instead lands on whatever `sqlx::Error::source()` itself
returns.

For the case that matters here
(`sqlx::Error::Database(#[source] Box<dyn
DatabaseError>)`), that is one further
hop in: `Box<dyn DatabaseError>` itself, _not_ the concrete
`sqlx::sqlite::SqliteError` sealed inside it. The invariant, pinned by a test
that provokes a real `SQLITE_BUSY`
(`is_retryable_sqlite_busy_true_for_real_busy_through_transparent_wrapper`): the
node reachable after the transparent skip downcasts as
`Box<dyn
sqlx::error::DatabaseError>` (sqlx's own
`impl StdError for Box<dyn
DatabaseError> {}` is what makes this typecheck) but
**not** as `SqliteError` -- the concrete sqlite type is not what the
trait-object coercion vtables on. `is_retryable_sqlite_busy` therefore downcasts
each node to both shapes: `Box<dyn
DatabaseError>` for the transparent case, and
`sqlx::Error` for the non-transparent one.

Two independent rules follow, and both hold:

- **This crate's own error type.** `ProjectionError`'s `Sqlx` and `Persistence`
  variants use an explicit `#[error("...: {0}")]` message rather than
  `transparent`. That makes `source()` return the wrapped field itself, so the
  walk reaches the `sqlx::Error` node directly rather than being delegated past
  it.
- **Downstream reactor error types.** Either shape works, because the classifier
  handles both: `#[error(transparent)] Sqlx(#[from] sqlx::Error)` is reached via
  the `Box<dyn DatabaseError>` downcast, and an explicit
  `#[error("...: {0}")] Sqlx(#[source] sqlx::Error)` via the `sqlx::Error` one.
  Downstream types are under no obligation to avoid `transparent`.

A `transparent` variant wrapping something other than `sqlx::Error` directly (or
a type that doesn't ultimately expose a `sqlx::Error`/`Box<dyn DatabaseError>`
node in its `source()` chain) remains an unclosable blind spot from this
function's perspective, same as the `AggregateError<T>` case above.
