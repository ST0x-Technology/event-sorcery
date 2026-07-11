//! Event reactor trait for multi-entity event handling.
//!
//! [`Reactor`] defines a multi-entity event handler whose event
//! type is computed from its dependency list. Reactors handle
//! events via the [`.on()`](crate::OneOf::on) /
//! [`.exhaustive()`](crate::Fold::exhaustive) chain, which
//! guarantees at compile time that every entity is handled.
//!
//! Dependency lists are declared via [`deps!`] in the
//! [`dependency`](crate::dependency) module.

use async_trait::async_trait;
use cqrs_es::persist::PersistenceError;
use sqlx::error::DatabaseError;
use std::future::Future;
use std::sync::Arc;
use tokio::time::{Duration, sleep};
use tracing::{debug, warn};

use crate::dependency::{Dependent, EntityList};

/// Event reactor with exhaustive compile-time checked handling.
///
/// The event type is computed from [`Dependent::Dependencies`]
/// -- no manual enum definition or `From` impls needed. Use the
/// [`.on()`](crate::OneOf::on) /
/// [`.exhaustive()`](crate::Fold::exhaustive) chain in the
/// `react` implementation to handle each entity.
///
/// Each `.on()` handler returns a future, which is boxed
/// internally for type erasure. Call `.exhaustive().await` to
/// run the matched handler.
///
/// ```ignore
/// deps!(RebalancingTrigger, [Position, TokenizedEquityMint]);
///
/// #[async_trait]
/// impl Reactor for RebalancingTrigger {
///     type Error = TriggerError;
///
///     async fn react(
///         &self,
///         event: <Self::Dependencies as EntityList>::Event,
///     ) -> Result<(), Self::Error> {
///         event
///             .on(|symbol, event| async move {
///                 self.on_position(symbol, event).await
///             })
///             .on(|id, event| async move {
///                 self.on_mint(id, event).await
///             })
///             .exhaustive()
///             .await;
///         Ok(())
///     }
/// }
/// ```
#[async_trait]
pub trait Reactor: Dependent {
    /// Error type for reactor failures.
    type Error: std::error::Error + Send + Sync;

    /// Handle a single event from any supported entity.
    async fn react(
        &self,
        event: <Self::Dependencies as EntityList>::Event,
    ) -> Result<(), Self::Error>;
}

/// Enables sharing a reactor via `Arc`.
#[async_trait]
impl<R: Reactor> Reactor for Arc<R> {
    type Error = R::Error;

    async fn react(
        &self,
        event: <Self::Dependencies as EntityList>::Event,
    ) -> Result<(), Self::Error> {
        R::react(self, event).await
    }
}

/// Marks a [`Reactor`] whose `react()` implementation is safe to retry in
/// full after a transient SQLite busy error.
///
/// # Safety contract
///
/// Implement this only for reactors whose `react()` performs exclusively
/// SQLite writes, with no side effect preceding the write that would double-
/// fire on retry: no HTTP/RPC calls, no `Store::send()` to another aggregate,
/// no message-queue publish. `Projection` does not need this -- it has its
/// own internal retry already, covering both optimistic-lock conflicts and
/// transient SQLite busy errors (see `Projection::react`). Reactors that
/// orchestrate across aggregates (e.g. the `RebalancingTrigger` example in
/// `docs/cqrs.md`) must NOT implement this unless the downstream command
/// handler is independently confirmed idempotent under re-invocation.
///
/// A `react()` that issues two or more separate, non-transactional SQLite
/// statements is also unsafe to mark: if the first statement commits and a
/// later one hits `SQLITE_BUSY`, the retry replays the *whole* `react()`,
/// re-running the already-committed statement. A conforming `react()` must
/// therefore be atomic as a whole -- a single statement, a single
/// transaction, or written so replaying it is safe (upserts, not bare
/// inserts).
///
/// This is a marker trait: implementing it is a declaration, not a
/// capability check the compiler can verify -- the burden of proof is on the
/// implementor, the same as an `unsafe impl Send`. Unlike `Send`, though,
/// this trait is not `unsafe`: implementors write a plain
/// `impl IdempotentReactor for MyReactor {}` (see `docs/cqrs.md`), so treat
/// the analogy as a discipline reminder, not a compiler guarantee.
///
/// # Latency tradeoff of wrapping this in `RetryOnBusy`
///
/// `CqrsFramework::execute_with_metadata` awaits every registered reactor's
/// `dispatch()` synchronously before returning to the command caller, once per
/// event. Wrapping an `IdempotentReactor` in [`RetryOnBusy`] means a
/// busy/busy-snapshot conflict -- including one caused by an unrelated writer
/// on the same database file -- can block that caller *per reacted event*, and
/// a command that emits multiple events dispatches the reactor once per event,
/// so the worst-case block is that per-event cost multiplied by the event
/// count. `Store::send` also serializes commands per aggregate ID (see
/// ADR-0004), so this block is not confined to the original caller: any other
/// command queued behind it on the *same* aggregate also waits it out before it
/// can even begin.
///
/// The per-event cost is **the ~4.3s sleep budget plus up to one
/// `busy_timeout` per attempt**, not ~4.3s flat. The sleep budget is only the
/// sum of the backoff delays between attempts; a plain `SQLITE_BUSY` surfaces
/// to the application only after the connection's `busy_timeout` has been
/// waited out first, and sqlx defaults that to 5s. Under persistent contention
/// each of the 11 attempts can therefore block for that timeout before even
/// returning the error to retry, putting the worst case on the order of a
/// minute per reacted event. Only errors that surface immediately
/// (`SQLITE_BUSY_SNAPSHOT`, which `busy_timeout` cannot absorb, or a connection
/// configured with `busy_timeout` = 0) cost just the ~4.3s. Size caller
/// timeouts against the full bound, not the sleep budget. See `docs/cqrs.md`'s
/// "Retrying on transient SQLite busy errors" section for the full writeup.
pub trait IdempotentReactor: Reactor {}

/// Wraps an [`IdempotentReactor`] to retry on transient SQLite busy errors.
///
/// Retries `react()` with exponential backoff when it fails with
/// `SQLITE_BUSY` / `SQLITE_BUSY_SNAPSHOT`. See [`IdempotentReactor`] for the
/// safety contract that gates this, including the caller-latency tradeoff.
pub struct RetryOnBusy<R> {
    pub inner: R,
}

impl<R: Dependent> Dependent for RetryOnBusy<R> {
    type Dependencies = R::Dependencies;
}

#[async_trait]
impl<R> Reactor for RetryOnBusy<R>
where
    R: IdempotentReactor,
    R::Error: 'static,
    <R::Dependencies as EntityList>::Event: Clone,
{
    type Error = R::Error;

    async fn react(
        &self,
        event: <Self::Dependencies as EntityList>::Event,
    ) -> Result<(), Self::Error> {
        retry_with_backoff(
            RETRY_MAX_ATTEMPTS,
            RETRY_BASE_DELAY_MS,
            RETRY_MAX_DELAY_MS,
            move || {
                let event = event.clone();
                async move { self.inner.react(event).await }
            },
            |error: &Self::Error| is_retryable_sqlite_busy(error),
        )
        .await
        .inspect_err(|error| {
            // `debug`, not `warn`: `ReactorBridge::dispatch` already logs every
            // reactor failure at `error` and is the single source-of-truth line
            // for log-based failure metrics. A second high-severity line here
            // would double the apparent failure count.
            debug!(
                target: "cqrs",
                ?error,
                "RetryOnBusy giving up: reactor error was not a retryable SQLite busy error, \
                 or the busy-retry budget was exhausted"
            );
        })
    }
}

/// Default retry schedule: 10ms base delay, doubling, capped at 1s, 10 retries.
///
/// That is a ~4.3s total retry budget. Shared between `Projection::react`'s own
/// retry loop and [`RetryOnBusy`]'s call to [`retry_with_backoff`], so the two
/// stay in sync without sharing the loop itself.
pub const RETRY_MAX_ATTEMPTS: u32 = 10;
pub const RETRY_BASE_DELAY_MS: u64 = 10;
pub const RETRY_MAX_DELAY_MS: u64 = 1000;

/// Computes the exponential backoff delay for a given retry attempt.
///
/// Saturates toward `max_delay_ms` instead of overflowing: a caller-supplied
/// `attempt`/`base_delay_ms` large enough to overflow the exponential term
/// just hits the cap sooner rather than panicking (debug) or wrapping
/// (release). Shared by [`retry_with_backoff`] and `Projection::react`'s own
/// retry loop so the two schedules can't silently diverge on this property.
pub(crate) fn backoff_delay_ms(attempt: u32, base_delay_ms: u64, max_delay_ms: u64) -> u64 {
    2u64.checked_pow(attempt)
        .and_then(|multiplier| base_delay_ms.checked_mul(multiplier))
        .map_or(max_delay_ms, |computed| computed.min(max_delay_ms))
}

/// Retries `make_attempt` with exponential backoff while `should_retry`
/// returns `true` for the error, up to `max_attempts` retries (so
/// `max_attempts + 1` total calls in the worst case).
///
/// The schedule is an explicit parameter rather than baked-in constants so
/// callers exercising the exhaustion/backoff mechanics in tests can use a
/// tiny synthetic schedule instead of paying the real multi-second
/// production budget. Production callers pass [`RETRY_MAX_ATTEMPTS`],
/// [`RETRY_BASE_DELAY_MS`], and [`RETRY_MAX_DELAY_MS`].
pub async fn retry_with_backoff<Output, Error, MakeAttempt, Attempt>(
    max_attempts: u32,
    base_delay_ms: u64,
    max_delay_ms: u64,
    mut make_attempt: MakeAttempt,
    should_retry: impl Fn(&Error) -> bool,
) -> Result<Output, Error>
where
    MakeAttempt: FnMut() -> Attempt,
    Attempt: Future<Output = Result<Output, Error>>,
{
    let mut attempt = 0u32;

    loop {
        match make_attempt().await {
            Ok(output) => return Ok(output),
            Err(error) if attempt < max_attempts && should_retry(&error) => {
                let delay_ms = backoff_delay_ms(attempt, base_delay_ms, max_delay_ms);
                warn!(
                    target: "cqrs",
                    attempt = attempt + 1, max_attempts, delay_ms,
                    "Retrying after transient error"
                );
                sleep(Duration::from_millis(delay_ms)).await;
                attempt += 1;
            }
            Err(error) => return Err(error),
        }
    }
}

/// Classifies whether an error chain contains a transient SQLite busy error.
///
/// The `SQLITE_BUSY` extended-code family (`5` plain, `261` recovery, `517`
/// snapshot, `773` timeout) is safe to retry from an event-log perspective.
///
/// Walks the `source()` chain, downcasting each node to `sqlx::Error`. A
/// downstream reactor error using the idiomatic
/// `#[error(transparent)] Sqlx(#[from] sqlx::Error)` shape makes thiserror
/// delegate `source()` past the `sqlx::Error` node to the `Box<dyn
/// DatabaseError>` that `sqlx::Error::Database` exposes one hop further in, so
/// each node is also downcast to that boxed database error.
/// `cqrs_es::persist::PersistenceError`'s boxed inner error isn't wired as
/// `#[source]` by cqrs-es, so the walk special-cases it and continues
/// manually into the box. `cqrs_es::AggregateError<T>` has the same shape but
/// is generic over the aggregate's `Entity`, so it can't be downcast from
/// fully generic code -- a busy error sealed behind it is unreachable here
/// and this function fails closed (`false`) for it. See `docs/sqlx.md` for the
/// full writeup of these shapes.
pub fn is_retryable_sqlite_busy(error: &(dyn std::error::Error + 'static)) -> bool {
    let mut current: Option<&(dyn std::error::Error + 'static)> = Some(error);

    while let Some(this_error) = current {
        if let Some(sqlx::Error::Database(database_error)) =
            this_error.downcast_ref::<sqlx::Error>()
            && is_busy_extended_code(database_error.code().as_deref())
        {
            return true;
        }

        // A `#[error(transparent)]` sqlx wrapper delegates `source()` past the
        // `sqlx::Error` node straight to this boxed database error, so classify
        // it here too rather than miss the whole idiomatic downstream shape.
        if let Some(database_error) = this_error.downcast_ref::<Box<dyn DatabaseError>>()
            && is_busy_extended_code(database_error.code().as_deref())
        {
            return true;
        }

        let persistence_boxed_source =
            this_error
                .downcast_ref::<PersistenceError>()
                .and_then(|persistence_error| match persistence_error {
                    PersistenceError::ConnectionError(inner)
                    | PersistenceError::UnknownError(inner) => {
                        Some(inner.as_ref() as &(dyn std::error::Error + 'static))
                    }
                    PersistenceError::DeserializationError(_)
                    | PersistenceError::OptimisticLockError => None,
                });

        current = persistence_boxed_source.or_else(|| this_error.source());
    }

    false
}

/// Whether a `DatabaseError::code()` value is in SQLite's `SQLITE_BUSY`
/// extended-code family.
///
/// sqlx reports the extended result code as a decimal string; the primary code
/// is the low byte (`extended & 0xFF`). `5` is `SQLITE_BUSY`, and every extended
/// code built on it (recovery `261`, snapshot `517`, timeout `773`) is the same
/// underlying lock conflict, so match the whole family rather than enumerating
/// each extended code by hand.
fn is_busy_extended_code(code: Option<&str>) -> bool {
    matches!(
        code.map(str::parse::<i32>),
        Some(Ok(extended_code)) if extended_code & 0xFF == 5
    )
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use cqrs_es::DomainEvent;
    use serde::{Deserialize, Serialize};
    use sqlx::Connection;
    use sqlx::sqlite::{SqliteConnectOptions, SqliteConnection};
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

    use super::*;
    use crate::EventSourced;
    use crate::dependency::{Cons, Nil, OneOf};
    use crate::lifecycle::Never;
    use crate::projection::ProjectionError;
    use crate::testing::sqlx_error_with_code;

    #[test]
    fn is_retryable_sqlite_busy_true_for_busy_code() {
        let error = sqlx_error_with_code("5");
        assert!(is_retryable_sqlite_busy(&error));
    }

    #[test]
    fn is_retryable_sqlite_busy_true_for_busy_snapshot_code() {
        let error = sqlx_error_with_code("517");
        assert!(is_retryable_sqlite_busy(&error));
    }

    #[test]
    fn is_retryable_sqlite_busy_true_for_busy_recovery_code() {
        let error = sqlx_error_with_code("261");
        assert!(is_retryable_sqlite_busy(&error));
    }

    #[test]
    fn is_retryable_sqlite_busy_true_for_busy_timeout_code() {
        let error = sqlx_error_with_code("773");
        assert!(is_retryable_sqlite_busy(&error));
    }

    #[test]
    fn is_retryable_sqlite_busy_false_for_other_code() {
        let error = sqlx_error_with_code("19");
        assert!(!is_retryable_sqlite_busy(&error));
    }

    /// Reproduces the idiomatic downstream-reactor error shape from
    /// `docs/sqlx.md`'s transparent-wrapper pitfall:
    /// `#[error(transparent)] Sqlx(#[from] sqlx::Error)`. Thiserror's
    /// transparent delegation skips the `sqlx::Error` node itself, landing
    /// `source()` on the `Box<dyn DatabaseError>` that `sqlx::Error::Database`
    /// exposes one hop in -- the node `is_retryable_sqlite_busy` must also
    /// classify.
    #[derive(Debug, thiserror::Error)]
    enum DownstreamTransparentError {
        #[error(transparent)]
        Sqlx(#[from] sqlx::Error),
    }

    /// Counter for unique real-SQLite-file paths within a single test process.
    ///
    /// nextest runs each test in its own process, so the counter alone always
    /// starts at 0 and gives no uniqueness *across* tests -- two tests
    /// following this pattern would delete and write the same file
    /// concurrently. The process id supplies the cross-test uniqueness; the
    /// counter only disambiguates multiple files taken by the same test.
    static REAL_BUSY_TEST_DB_COUNTER: AtomicU32 = AtomicU32::new(0);

    /// Provokes a genuine `SQLITE_BUSY` (extended code `"5"`) from real sqlx
    /// against a real SQLite file, then wraps it in the transparent shape
    /// above and asserts `is_retryable_sqlite_busy` still classifies it.
    ///
    /// `holder_connection` takes the RESERVED write lock via
    /// `BEGIN IMMEDIATE`; `contender_connection`, with `busy_timeout`
    /// disabled so it fails instead of waiting, then tries to write and is
    /// rejected. This also pins sqlx's `.code()` contract -- the extended
    /// result code as a decimal string -- against a real response, not just
    /// the hand-built `TestDatabaseError` used by the other tests here.
    #[tokio::test]
    async fn is_retryable_sqlite_busy_true_for_real_busy_through_transparent_wrapper() {
        let db_path = std::env::temp_dir().join(format!(
            "event_sorcery_reactor_real_busy_test_{}_{}.sqlite3",
            std::process::id(),
            REAL_BUSY_TEST_DB_COUNTER.fetch_add(1, Ordering::SeqCst)
        ));
        let _ = std::fs::remove_file(&db_path);

        let connect_options = SqliteConnectOptions::new()
            .filename(&db_path)
            .create_if_missing(true)
            .busy_timeout(Duration::from_secs(0));

        let mut holder_connection = SqliteConnection::connect_with(&connect_options)
            .await
            .unwrap();
        let mut contender_connection = SqliteConnection::connect_with(&connect_options)
            .await
            .unwrap();

        sqlx::query("CREATE TABLE busy_probe (value INTEGER NOT NULL)")
            .execute(&mut holder_connection)
            .await
            .unwrap();

        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut holder_connection)
            .await
            .unwrap();
        sqlx::query("INSERT INTO busy_probe (value) VALUES (1)")
            .execute(&mut holder_connection)
            .await
            .unwrap();

        let real_busy_error = sqlx::query("INSERT INTO busy_probe (value) VALUES (2)")
            .execute(&mut contender_connection)
            .await
            .unwrap_err();

        let downstream_error = DownstreamTransparentError::from(real_busy_error);

        assert!(is_retryable_sqlite_busy(&downstream_error));

        let _ = std::fs::remove_file(&db_path);
    }

    #[derive(Debug, thiserror::Error)]
    enum InnerError {
        #[error("sqlx failure: {0}")]
        Sqlx(#[from] sqlx::Error),
    }

    #[derive(Debug, thiserror::Error)]
    enum OuterError {
        #[error("wrapped: {0}")]
        Wrapped(#[source] InnerError),
    }

    #[test]
    fn is_retryable_sqlite_busy_true_two_levels_deep_via_source_chain() {
        let error = OuterError::Wrapped(InnerError::Sqlx(sqlx_error_with_code("5")));
        assert!(is_retryable_sqlite_busy(&error));
    }

    #[derive(Debug, thiserror::Error)]
    enum ReactorLikeError {
        #[error("persistence: {0}")]
        Persistence(#[from] cqrs_es::persist::PersistenceError),
    }

    #[test]
    fn is_retryable_sqlite_busy_true_behind_persistence_error_connection_error() {
        let boxed_sqlx_error: Box<dyn std::error::Error + Send + Sync + 'static> =
            Box::new(sqlx_error_with_code("517"));
        let error = ReactorLikeError::Persistence(
            cqrs_es::persist::PersistenceError::ConnectionError(boxed_sqlx_error),
        );

        assert!(is_retryable_sqlite_busy(&error));
    }

    /// Regression test for the `#[error(transparent)]` blind spot: thiserror
    /// makes a transparent variant's `source()` delegate to the wrapped
    /// field's *own* `source()` rather than returning the field itself, so a
    /// naive walk skips straight past it. `ProjectionError::Sqlx` and
    /// `ProjectionError::Persistence` used to be declared `transparent` --
    /// this exercises the real (non-generic-instantiation-specific) type
    /// after switching those variants to an explicit `#[error("...: {0}")]`
    /// message, which makes thiserror return the wrapped field itself from
    /// `source()` so the existing walk can reach it.
    #[test]
    fn is_retryable_sqlite_busy_true_for_projection_error_sqlx_variant() {
        let error = ProjectionError::<TestEntity>::Sqlx(sqlx_error_with_code("5"));
        assert!(is_retryable_sqlite_busy(&error));
    }

    #[test]
    fn is_retryable_sqlite_busy_true_for_projection_error_persistence_variant() {
        let boxed_sqlx_error: Box<dyn std::error::Error + Send + Sync + 'static> =
            Box::new(sqlx_error_with_code("517"));
        let error = ProjectionError::<TestEntity>::Persistence(
            cqrs_es::persist::PersistenceError::ConnectionError(boxed_sqlx_error),
        );
        assert!(is_retryable_sqlite_busy(&error));
    }

    /// Mirrors the shape of `cqrs_es::AggregateError::DatabaseConnectionError`
    /// -- a boxed `dyn Error` field with no `#[source]`/`#[from]`, so the
    /// walk cannot see through it. `AggregateError<T>` itself can't be named
    /// here (it's generic over `Entity`, unknowable at a generic
    /// classification site), but this reproduces the exact blind spot: a
    /// busy error sealed behind an opaque boxed field is un-classifiable and
    /// must fail closed.
    #[derive(Debug, thiserror::Error)]
    #[error("aggregate-shaped: {0}")]
    struct AggregateShapedError(Box<dyn std::error::Error + Send + Sync + 'static>);

    #[test]
    fn is_retryable_sqlite_busy_fails_closed_behind_unsourced_aggregate_error() {
        let error = AggregateShapedError(Box::new(sqlx_error_with_code("5")));
        assert!(!is_retryable_sqlite_busy(&error));
    }

    #[derive(Debug, PartialEq, Eq, thiserror::Error)]
    enum SyntheticError {
        #[error("retryable")]
        Retryable,
        #[error("permanent")]
        Permanent,
    }

    fn is_synthetic_retryable(error: &SyntheticError) -> bool {
        matches!(error, SyntheticError::Retryable)
    }

    /// Tiny synthetic schedule (sub-millisecond) so exhaustion tests don't
    /// pay the real ~4.3s production budget -- see the DRY decision in the
    /// module doc comment on why the schedule is a parameter, not baked-in
    /// consts, on this helper.
    const TEST_MAX_ATTEMPTS: u32 = 3;
    const TEST_BASE_DELAY_MS: u64 = 1;
    const TEST_MAX_DELAY_MS: u64 = 5;

    #[tokio::test]
    async fn retry_with_backoff_succeeds_on_first_attempt() {
        let call_count = AtomicU32::new(0);

        let result = retry_with_backoff(
            TEST_MAX_ATTEMPTS,
            TEST_BASE_DELAY_MS,
            TEST_MAX_DELAY_MS,
            || {
                call_count.fetch_add(1, Ordering::SeqCst);
                async { Ok::<_, SyntheticError>(42) }
            },
            is_synthetic_retryable,
        )
        .await;

        assert_eq!(result.unwrap(), 42);
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn retry_with_backoff_retries_then_succeeds() {
        let call_count = AtomicU32::new(0);

        let result = retry_with_backoff(
            TEST_MAX_ATTEMPTS,
            TEST_BASE_DELAY_MS,
            TEST_MAX_DELAY_MS,
            || {
                let attempt = call_count.fetch_add(1, Ordering::SeqCst);
                async move {
                    if attempt < 2 {
                        Err(SyntheticError::Retryable)
                    } else {
                        Ok(42)
                    }
                }
            },
            is_synthetic_retryable,
        )
        .await;

        assert_eq!(result.unwrap(), 42);
        assert_eq!(call_count.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn retry_with_backoff_stops_immediately_on_non_retryable_error() {
        let call_count = AtomicU32::new(0);

        let result: Result<u32, SyntheticError> = retry_with_backoff(
            TEST_MAX_ATTEMPTS,
            TEST_BASE_DELAY_MS,
            TEST_MAX_DELAY_MS,
            || {
                call_count.fetch_add(1, Ordering::SeqCst);
                async { Err(SyntheticError::Permanent) }
            },
            is_synthetic_retryable,
        )
        .await;

        assert_eq!(result.unwrap_err(), SyntheticError::Permanent);
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn retry_with_backoff_exhausts_budget_and_returns_err() {
        let call_count = AtomicU32::new(0);

        let result: Result<u32, SyntheticError> = retry_with_backoff(
            TEST_MAX_ATTEMPTS,
            TEST_BASE_DELAY_MS,
            TEST_MAX_DELAY_MS,
            || {
                call_count.fetch_add(1, Ordering::SeqCst);
                async { Err(SyntheticError::Retryable) }
            },
            is_synthetic_retryable,
        )
        .await;

        assert_eq!(result.unwrap_err(), SyntheticError::Retryable);
        assert_eq!(call_count.load(Ordering::SeqCst), TEST_MAX_ATTEMPTS + 1);
    }

    /// `2u64.pow(attempt)` overflows once `attempt >= 64`. A caller-supplied
    /// schedule with enough attempts to reach that must saturate toward
    /// `max_delay_ms` instead of panicking (debug builds) or silently
    /// wrapping (release builds) -- this exercises exactly that range with a
    /// tiny delay so the test stays fast.
    #[tokio::test]
    async fn retry_with_backoff_caps_delay_without_overflow_at_high_attempt_counts() {
        const HIGH_MAX_ATTEMPTS: u32 = 70;
        let call_count = AtomicU32::new(0);

        let result: Result<u32, SyntheticError> = retry_with_backoff(
            HIGH_MAX_ATTEMPTS,
            1,
            1,
            || {
                call_count.fetch_add(1, Ordering::SeqCst);
                async { Err(SyntheticError::Retryable) }
            },
            is_synthetic_retryable,
        )
        .await;

        assert_eq!(result.unwrap_err(), SyntheticError::Retryable);
        assert_eq!(call_count.load(Ordering::SeqCst), HIGH_MAX_ATTEMPTS + 1);
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct TestEntity {
        name: String,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct TestEvent;

    impl DomainEvent for TestEvent {
        fn event_type(&self) -> String {
            "TestEvent".to_string()
        }
        fn event_version(&self) -> String {
            "1.0".to_string()
        }
    }

    #[async_trait]
    impl EventSourced for TestEntity {
        type Id = String;
        type Event = TestEvent;
        type Command = ();
        type Error = Never;
        type Services = ();
        type Materialized = Nil;

        const AGGREGATE_TYPE: &'static str = "TestEntity";
        const PROJECTION: Nil = Nil;
        const SCHEMA_VERSION: u64 = 1;

        fn originate(_event: &TestEvent) -> Option<Self> {
            Some(Self {
                name: "test".to_string(),
            })
        }

        fn evolve(entity: &Self, _event: &TestEvent) -> Result<Option<Self>, Never> {
            Ok(Some(entity.clone()))
        }

        async fn initialize(_command: (), _services: &()) -> Result<Vec<TestEvent>, Never> {
            Ok(vec![])
        }

        async fn transition(&self, _command: (), _services: &()) -> Result<Vec<TestEvent>, Never> {
            Ok(vec![])
        }
    }

    #[derive(Debug, thiserror::Error)]
    enum FlakyReactorError {
        #[error("busy: {0}")]
        Busy(#[source] sqlx::Error),
        #[error("permanent")]
        Permanent,
    }

    /// Test reactor whose `react()` outcome is fully controlled: it can be
    /// configured to fail with a busy-classified error a fixed number of
    /// times before succeeding, or to always fail with a non-busy error.
    /// Mirrors `ConflictingRepo`'s pattern in `projection.rs`.
    ///
    /// `busy_code` is the SQLite extended result code the busy failures carry,
    /// so the retry loop can be driven with both plain `SQLITE_BUSY` (`"5"`)
    /// and `SQLITE_BUSY_SNAPSHOT` (`"517"`).
    struct FlakyReactor {
        remaining_busy_failures: AtomicU32,
        permanent_failure: bool,
        calls: AtomicU32,
        applied: AtomicBool,
        busy_code: &'static str,
    }

    impl Dependent for FlakyReactor {
        type Dependencies = Cons<TestEntity, Nil>;
    }

    #[async_trait]
    impl Reactor for FlakyReactor {
        type Error = FlakyReactorError;

        async fn react(
            &self,
            event: <Self::Dependencies as EntityList>::Event,
        ) -> Result<(), Self::Error> {
            self.calls.fetch_add(1, Ordering::SeqCst);

            if self.permanent_failure {
                return Err(FlakyReactorError::Permanent);
            }

            let remaining = self.remaining_busy_failures.load(Ordering::SeqCst);
            if remaining > 0 {
                self.remaining_busy_failures
                    .store(remaining - 1, Ordering::SeqCst);
                return Err(FlakyReactorError::Busy(sqlx_error_with_code(
                    self.busy_code,
                )));
            }

            let _ = event.into_inner();
            self.applied.store(true, Ordering::SeqCst);
            Ok(())
        }
    }

    impl IdempotentReactor for FlakyReactor {}

    /// Drives `RetryOnBusy`'s retry loop to a verified successful `react()`
    /// with busy failures carrying `busy_code`.
    async fn retry_on_busy_retries_then_succeeds(busy_code: &'static str) {
        let wrapped = RetryOnBusy {
            inner: FlakyReactor {
                remaining_busy_failures: AtomicU32::new(2),
                permanent_failure: false,
                calls: AtomicU32::new(0),
                applied: AtomicBool::new(false),
                busy_code,
            },
        };

        let event: OneOf<(String, TestEvent), Never> = OneOf::Here(("id-1".to_string(), TestEvent));

        wrapped.react(event).await.unwrap();

        assert!(
            wrapped.inner.applied.load(Ordering::SeqCst),
            "the write must actually land after retrying code {busy_code}"
        );
        assert_eq!(wrapped.inner.calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn retry_on_busy_retries_busy_classified_error_then_succeeds() {
        retry_on_busy_retries_then_succeeds("5").await;
    }

    /// `busy_timeout` cannot absorb `SQLITE_BUSY_SNAPSHOT` -- rolling back and
    /// re-attempting is the only fix, which is exactly what `RetryOnBusy` does.
    /// Drives the same loop with `"517"` so that scenario is covered end to end.
    #[tokio::test]
    async fn retry_on_busy_retries_busy_snapshot_classified_error_then_succeeds() {
        retry_on_busy_retries_then_succeeds("517").await;
    }

    #[tokio::test]
    async fn retry_on_busy_does_not_retry_non_busy_error() {
        let wrapped = RetryOnBusy {
            inner: FlakyReactor {
                remaining_busy_failures: AtomicU32::new(0),
                permanent_failure: true,
                calls: AtomicU32::new(0),
                applied: AtomicBool::new(false),
                busy_code: "5",
            },
        };

        let event: OneOf<(String, TestEvent), Never> = OneOf::Here(("id-1".to_string(), TestEvent));

        let error = wrapped.react(event).await.unwrap_err();

        assert!(matches!(error, FlakyReactorError::Permanent));
        assert!(!wrapped.inner.applied.load(Ordering::SeqCst));
        assert_eq!(wrapped.inner.calls.load(Ordering::SeqCst), 1);
    }
}
