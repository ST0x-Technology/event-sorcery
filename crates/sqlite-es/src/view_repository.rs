use cqrs_es::persist::{PersistenceError, ViewContext, ViewRepository};
use cqrs_es::{Aggregate, View};
use sqlx::{AssertSqlSafe, Pool, Row, Sqlite};
use std::marker::PhantomData;

use crate::sql_query::SqlQueryFactory;

/// Errors that can occur during SQLite view repository operations
#[derive(Debug, thiserror::Error)]
pub enum SqliteViewError {
    /// Optimistic locking conflict - the view was modified concurrently
    #[error("Optimistic lock error: view has been modified concurrently")]
    OptimisticLock,

    /// Database connection or query error
    #[error("Database connection error: {0}")]
    Connection(#[from] sqlx::Error),

    /// View serialization error
    #[error("View serialization error: {0}")]
    Serialization(#[source] serde_json::Error),

    /// View deserialization error
    #[error("View deserialization error: {0}")]
    Deserialization(#[source] serde_json::Error),

    /// Integer conversion error (e.g., version number overflow)
    #[error("Integer conversion error: {0}")]
    TryFromInt(#[from] std::num::TryFromIntError),
}

impl From<SqliteViewError> for PersistenceError {
    fn from(err: SqliteViewError) -> Self {
        match err {
            SqliteViewError::OptimisticLock => Self::OptimisticLockError,
            SqliteViewError::Connection(e) => Self::ConnectionError(Box::new(e)),
            SqliteViewError::Serialization(e) => Self::UnknownError(Box::new(e)),
            SqliteViewError::Deserialization(e) => Self::DeserializationError(Box::new(e)),
            SqliteViewError::TryFromInt(e) => Self::UnknownError(Box::new(e)),
        }
    }
}

/// SQLite implementation of the `ViewRepository` trait
///
/// Provides view persistence backed by SQLite for use with the `GenericQuery`
/// processor in the cqrs-es framework.
///
/// # Type Parameters
///
/// * `V` - The view type that implements `View<A>`
/// * `A` - The aggregate type that the view projects from
///
/// # Example
///
/// ```ignore
/// let pool = Pool::<Sqlite>::connect("sqlite::memory:").await?;
/// let view_repo = SqliteViewRepository::<MintView, Mint>::new(
///     pool,
///     "mint_view".to_string()
/// );
/// ```
pub struct SqliteViewRepository<V, A>
where
    V: View<A>,
    A: Aggregate,
{
    pool: Pool<Sqlite>,
    view_table: String,
    _phantom: PhantomData<(V, A)>,
}

impl<V, A> SqliteViewRepository<V, A>
where
    V: View<A>,
    A: Aggregate,
{
    /// Creates a new `SqliteViewRepository` for a specific view table
    ///
    /// # Arguments
    ///
    /// * `pool` - SQLite connection pool
    /// * `view_table` - Name of the table storing this view (e.g., "mint_view")
    #[must_use]
    pub const fn new(pool: Pool<Sqlite>, view_table: String) -> Self {
        Self {
            pool,
            view_table,
            _phantom: PhantomData,
        }
    }
}

impl<V, A> ViewRepository<V, A> for SqliteViewRepository<V, A>
where
    V: View<A>,
    A: Aggregate,
{
    async fn load(&self, view_id: &str) -> Result<Option<V>, PersistenceError> {
        let query = SqlQueryFactory::select_view(&self.view_table);

        let row = sqlx::query(AssertSqlSafe(query))
            .bind(view_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(SqliteViewError::Connection)?;

        let Some(row) = row else {
            return Ok(None);
        };

        let payload_str: String = row
            .try_get("payload")
            .map_err(SqliteViewError::Connection)?;

        let view: V =
            serde_json::from_str(&payload_str).map_err(SqliteViewError::Deserialization)?;

        Ok(Some(view))
    }

    async fn load_with_context(
        &self,
        view_id: &str,
    ) -> Result<Option<(V, ViewContext)>, PersistenceError> {
        let query = SqlQueryFactory::select_view(&self.view_table);

        let row = sqlx::query(AssertSqlSafe(query))
            .bind(view_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(SqliteViewError::Connection)?;

        let Some(row) = row else {
            return Ok(None);
        };

        let view_id_str: String = row
            .try_get("view_id")
            .map_err(SqliteViewError::Connection)?;

        let version_i64: i64 = row
            .try_get("version")
            .map_err(SqliteViewError::Connection)?;

        let payload_str: String = row
            .try_get("payload")
            .map_err(SqliteViewError::Connection)?;

        let view: V =
            serde_json::from_str(&payload_str).map_err(SqliteViewError::Deserialization)?;

        let context = ViewContext::new(view_id_str, version_i64);

        Ok(Some((view, context)))
    }

    /// Optimistic locking strategy: version 0 means initial insert, subsequent
    /// updates check the current version matches the expected version. If the
    /// UPDATE affects 0 rows, it means the view was modified concurrently and
    /// an OptimisticLockError is returned.
    async fn update_view(&self, view: V, context: ViewContext) -> Result<(), PersistenceError> {
        let payload = serde_json::to_string(&view).map_err(SqliteViewError::Serialization)?;

        let new_version = context.version + 1;

        if context.version == 0 {
            let insert_query = SqlQueryFactory::insert_view(&self.view_table);

            sqlx::query(AssertSqlSafe(insert_query))
                .bind(&context.view_instance_id)
                .bind(new_version)
                .bind(&payload)
                .execute(&self.pool)
                .await
                .map_err(SqliteViewError::Connection)?;
        } else {
            let update_query = SqlQueryFactory::update_view(&self.view_table);

            let result = sqlx::query(AssertSqlSafe(update_query))
                .bind(new_version)
                .bind(&payload)
                .bind(&context.view_instance_id)
                .bind(context.version)
                .execute(&self.pool)
                .await
                .map_err(SqliteViewError::Connection)?;

            if result.rows_affected() == 0 {
                return Err(SqliteViewError::OptimisticLock.into());
            }
        }

        Ok(())
    }
}

/// A predicate scan the load-by-id [`ViewRepository`] cannot express.
///
/// Returns the `view_id`s whose columns satisfy a predicate, ordered and
/// limited -- so a projection can be polled for "all rows matching X" (e.g. the
/// durable-job runnable poll) without the caller writing SQL.
pub trait IndexedView<V, A>: ViewRepository<V, A>
where
    V: View<A>,
    A: Aggregate,
{
    /// Returns the `view_id`s whose columns satisfy `predicate` (disjunctive
    /// normal form: an OR of AND-groups), ordered and capped at `limit`.
    fn find(
        &self,
        predicate: &Predicate,
        order: Option<&Order>,
        limit: i64,
    ) -> impl std::future::Future<Output = Result<Vec<String>, PersistenceError>> + Send;
}

/// A predicate in disjunctive normal form: any of these AND-groups matching is a
/// match. An empty `any_of` matches nothing.
pub struct Predicate {
    /// OR of conjunctions; each inner `Vec` is an AND of [`Term`]s.
    pub any_of: Vec<Vec<Term>>,
}

/// One column comparison.
pub struct Term {
    /// The column name. MUST be a trusted, code-controlled identifier (a view
    /// column), never user input -- it is interpolated, not bound.
    pub column: String,
    /// The comparison operator.
    pub cmp: Cmp,
    /// The bound value; `None` for [`Cmp::IsNull`]/[`Cmp::IsNotNull`].
    pub value: Option<Value>,
}

/// Comparison operators for a [`Term`].
pub enum Cmp {
    /// `=`
    Eq,
    /// `<`
    Lt,
    /// `<=`
    Le,
    /// `>`
    Gt,
    /// `>=`
    Ge,
    /// `IS NULL` (no bound value)
    IsNull,
    /// `IS NOT NULL` (no bound value)
    IsNotNull,
}

impl Cmp {
    fn sql(&self) -> &'static str {
        match self {
            Self::Eq => "=",
            Self::Lt => "<",
            Self::Le => "<=",
            Self::Gt => ">",
            Self::Ge => ">=",
            Self::IsNull => "IS NULL",
            Self::IsNotNull => "IS NOT NULL",
        }
    }
}

/// A bound scalar value.
pub enum Value {
    /// An integer column value.
    Int(i64),
    /// A text column value.
    Text(String),
}

/// Result ordering for [`IndexedView::find`].
pub struct Order {
    /// The column to order by (trusted identifier; interpolated).
    pub column: String,
    /// Ascending when true, descending otherwise.
    pub ascending: bool,
}

impl<V, A> IndexedView<V, A> for SqliteViewRepository<V, A>
where
    V: View<A>,
    A: Aggregate,
{
    async fn find(
        &self,
        predicate: &Predicate,
        order: Option<&Order>,
        limit: i64,
    ) -> Result<Vec<String>, PersistenceError> {
        if predicate.any_of.is_empty() {
            return Ok(vec![]);
        }

        let mut binds: Vec<&Value> = vec![];
        let groups: Vec<String> = predicate
            .any_of
            .iter()
            .map(|conjunction| {
                let terms: Vec<String> = conjunction
                    .iter()
                    .map(|term| {
                        term.value.as_ref().map_or_else(
                            || format!("{} {}", term.column, term.cmp.sql()),
                            |value| {
                                binds.push(value);
                                format!("{} {} ?", term.column, term.cmp.sql())
                            },
                        )
                    })
                    .collect();
                format!("({})", terms.join(" AND "))
            })
            .collect();

        let order_clause = order.map_or_else(String::new, |order| {
            let direction = if order.ascending { "ASC" } else { "DESC" };
            format!(" ORDER BY {} {direction}", order.column)
        });
        let sql = format!(
            "SELECT view_id FROM {} WHERE {}{order_clause} LIMIT ?",
            self.view_table,
            groups.join(" OR "),
        );

        let mut query = sqlx::query_scalar::<_, String>(AssertSqlSafe(sql));
        for value in binds {
            query = match value {
                Value::Int(int) => query.bind(int),
                Value::Text(text) => query.bind(text),
            };
        }
        query = query.bind(limit);

        query
            .fetch_all(&self.pool)
            .await
            .map_err(SqliteViewError::Connection)
            .map_err(PersistenceError::from)
    }
}

#[cfg(test)]
mod tests {
    use cqrs_es::event_sink::EventSink;
    use cqrs_es::{DomainEvent, EventEnvelope};
    use serde::{Deserialize, Serialize};
    use std::fmt::{self, Display};

    use super::*;
    use crate::testing::create_test_pool;

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
    struct TestAggregate;

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    enum TestEvent {
        Created,
        Updated { value: String },
    }

    impl DomainEvent for TestEvent {
        fn event_type(&self) -> String {
            match self {
                Self::Created => "Created".to_string(),
                Self::Updated { .. } => "Updated".to_string(),
            }
        }

        fn event_version(&self) -> String {
            "1.0".to_string()
        }
    }

    #[derive(Debug)]
    struct TestError;

    impl Display for TestError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "test error")
        }
    }

    impl std::error::Error for TestError {}

    impl Aggregate for TestAggregate {
        const TYPE: &'static str = "TestAggregate";
        type Command = ();
        type Event = TestEvent;
        type Error = TestError;
        type Services = ();

        async fn handle(
            &mut self,
            _command: Self::Command,
            _services: &Self::Services,
            _sink: &EventSink<Self>,
        ) -> Result<(), Self::Error> {
            Ok(())
        }

        fn apply(&mut self, _event: Self::Event) {}
    }

    #[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq)]
    struct TestView {
        count: i64,
        values: Vec<String>,
    }

    impl View<TestAggregate> for TestView {
        fn update(&mut self, event: &EventEnvelope<TestAggregate>) {
            match &event.payload {
                TestEvent::Created => self.count += 1,
                TestEvent::Updated { value } => self.values.push(value.clone()),
            }
        }
    }

    #[tokio::test]
    async fn test_load_nonexistent_view() {
        let pool = create_test_pool().await.unwrap();

        sqlx::query(
            "CREATE TABLE test_view (
                view_id TEXT PRIMARY KEY,
                version BIGINT NOT NULL,
                payload JSON NOT NULL
            )",
        )
        .execute(&pool)
        .await
        .unwrap();

        let repo =
            SqliteViewRepository::<TestView, TestAggregate>::new(pool, "test_view".to_string());

        let result = repo.load("nonexistent").await.unwrap();

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_load_view() {
        let pool = create_test_pool().await.unwrap();

        sqlx::query(
            "CREATE TABLE test_view (
                view_id TEXT PRIMARY KEY,
                version BIGINT NOT NULL,
                payload JSON NOT NULL
            )",
        )
        .execute(&pool)
        .await
        .unwrap();

        let test_view = TestView {
            count: 42,
            values: vec!["foo".to_string(), "bar".to_string()],
        };
        let payload = serde_json::to_string(&test_view).unwrap();

        sqlx::query("INSERT INTO test_view (view_id, version, payload) VALUES (?, ?, ?)")
            .bind("test-123")
            .bind(5_i64)
            .bind(&payload)
            .execute(&pool)
            .await
            .unwrap();

        let repo =
            SqliteViewRepository::<TestView, TestAggregate>::new(pool, "test_view".to_string());

        let result = repo.load("test-123").await.unwrap();

        assert!(result.is_some());
        let loaded_view = result.unwrap();
        assert_eq!(loaded_view.count, 42);
        assert_eq!(loaded_view.values, vec!["foo", "bar"]);
    }

    #[tokio::test]
    async fn find_returns_view_ids_matching_a_dnf_predicate_ordered_and_limited() {
        let pool = create_test_pool().await.unwrap();
        sqlx::query(
            "CREATE TABLE q (
                view_id TEXT PRIMARY KEY,
                version BIGINT NOT NULL,
                payload JSON NOT NULL,
                status TEXT    GENERATED ALWAYS AS (json_extract(payload, '$.status')) STORED,
                run_at INTEGER GENERATED ALWAYS AS (json_extract(payload, '$.run_at')) STORED
            )",
        )
        .execute(&pool)
        .await
        .unwrap();

        for (id, status, run_at) in [
            ("a", "pending", 10),
            ("b", "pending", 30),
            ("c", "claimed", 20),
            ("d", "done", 5),
        ] {
            let payload = serde_json::json!({ "status": status, "run_at": run_at }).to_string();
            sqlx::query("INSERT INTO q (view_id, version, payload) VALUES (?, 1, ?)")
                .bind(id)
                .bind(&payload)
                .execute(&pool)
                .await
                .unwrap();
        }

        let repo = SqliteViewRepository::<TestView, TestAggregate>::new(pool, "q".to_string());

        // (status='pending' AND run_at <= 20) OR (status='claimed'), oldest run_at first.
        let predicate = Predicate {
            any_of: vec![
                vec![
                    Term {
                        column: "status".to_string(),
                        cmp: Cmp::Eq,
                        value: Some(Value::Text("pending".to_string())),
                    },
                    Term {
                        column: "run_at".to_string(),
                        cmp: Cmp::Le,
                        value: Some(Value::Int(20)),
                    },
                ],
                vec![Term {
                    column: "status".to_string(),
                    cmp: Cmp::Eq,
                    value: Some(Value::Text("claimed".to_string())),
                }],
            ],
        };
        let order = Order {
            column: "run_at".to_string(),
            ascending: true,
        };

        let ids = repo.find(&predicate, Some(&order), 10).await.unwrap();
        // a (pending, 10) then c (claimed, 20); b excluded (run_at 30 > 20), d excluded (done).
        assert_eq!(ids, vec!["a".to_string(), "c".to_string()]);

        let limited = repo.find(&predicate, Some(&order), 1).await.unwrap();
        assert_eq!(limited, vec!["a".to_string()]);
    }

    #[tokio::test]
    async fn test_load_with_context() {
        let pool = create_test_pool().await.unwrap();

        sqlx::query(
            "CREATE TABLE test_view (
                view_id TEXT PRIMARY KEY,
                version BIGINT NOT NULL,
                payload JSON NOT NULL
            )",
        )
        .execute(&pool)
        .await
        .unwrap();

        let test_view = TestView {
            count: 100,
            values: vec!["alpha".to_string()],
        };
        let payload = serde_json::to_string(&test_view).unwrap();

        sqlx::query("INSERT INTO test_view (view_id, version, payload) VALUES (?, ?, ?)")
            .bind("test-456")
            .bind(7_i64)
            .bind(&payload)
            .execute(&pool)
            .await
            .unwrap();

        let repo =
            SqliteViewRepository::<TestView, TestAggregate>::new(pool, "test_view".to_string());

        let result = repo.load_with_context("test-456").await.unwrap();

        assert!(result.is_some());
        let (loaded_view, context) = result.unwrap();
        assert_eq!(loaded_view.count, 100);
        assert_eq!(loaded_view.values, vec!["alpha"]);
        assert_eq!(context.view_instance_id, "test-456");
        assert_eq!(context.version, 7);
    }

    #[test]
    fn test_optimistic_lock_error_conversion() {
        let err = SqliteViewError::OptimisticLock;
        let persistence_err: PersistenceError = err.into();

        assert!(matches!(
            persistence_err,
            PersistenceError::OptimisticLockError
        ));
    }

    #[test]
    fn test_serialization_error_conversion() {
        let json_err = serde_json::from_str::<i32>("invalid").unwrap_err();
        let err = SqliteViewError::Serialization(json_err);
        let persistence_err: PersistenceError = err.into();

        assert!(matches!(persistence_err, PersistenceError::UnknownError(_)));
    }

    #[test]
    fn test_deserialization_error_conversion() {
        let json_err = serde_json::from_str::<i32>("invalid").unwrap_err();
        let err = SqliteViewError::Deserialization(json_err);
        let persistence_err: PersistenceError = err.into();

        assert!(matches!(
            persistence_err,
            PersistenceError::DeserializationError(_)
        ));
    }

    #[test]
    fn test_try_from_int_error_conversion() {
        let int_err = i64::try_from(u64::MAX).unwrap_err();
        let err = SqliteViewError::TryFromInt(int_err);
        let persistence_err: PersistenceError = err.into();

        assert!(matches!(persistence_err, PersistenceError::UnknownError(_)));
    }

    #[test]
    fn test_error_display() {
        let err = SqliteViewError::OptimisticLock;
        assert_eq!(
            err.to_string(),
            "Optimistic lock error: view has been modified concurrently"
        );

        let json_err = serde_json::from_str::<i32>("invalid").unwrap_err();
        let err = SqliteViewError::Serialization(json_err);
        assert!(err.to_string().contains("View serialization error"));

        let json_err = serde_json::from_str::<i32>("invalid").unwrap_err();
        let err = SqliteViewError::Deserialization(json_err);
        assert!(err.to_string().contains("View deserialization error"));
    }

    #[tokio::test]
    async fn test_update_view_insert() {
        let pool = create_test_pool().await.unwrap();

        sqlx::query(
            "CREATE TABLE test_view (
                view_id TEXT PRIMARY KEY,
                version BIGINT NOT NULL,
                payload JSON NOT NULL
            )",
        )
        .execute(&pool)
        .await
        .unwrap();

        let repo = SqliteViewRepository::<TestView, TestAggregate>::new(
            pool.clone(),
            "test_view".to_string(),
        );

        let test_view = TestView {
            count: 10,
            values: vec!["test".to_string()],
        };

        let context = ViewContext::new("test-insert".to_string(), 0);

        repo.update_view(test_view.clone(), context).await.unwrap();

        let loaded = repo.load("test-insert").await.unwrap();
        assert!(loaded.is_some());
        let loaded_view = loaded.unwrap();
        assert_eq!(loaded_view.count, 10);
        assert_eq!(loaded_view.values, vec!["test"]);

        let row = sqlx::query("SELECT version FROM test_view WHERE view_id = ?")
            .bind("test-insert")
            .fetch_one(&pool)
            .await
            .unwrap();
        let version: i64 = row.try_get("version").unwrap();
        assert_eq!(version, 1);
    }

    #[tokio::test]
    async fn test_update_view_increment_version() {
        let pool = create_test_pool().await.unwrap();

        sqlx::query(
            "CREATE TABLE test_view (
                view_id TEXT PRIMARY KEY,
                version BIGINT NOT NULL,
                payload JSON NOT NULL
            )",
        )
        .execute(&pool)
        .await
        .unwrap();

        let repo = SqliteViewRepository::<TestView, TestAggregate>::new(
            pool.clone(),
            "test_view".to_string(),
        );

        let initial_view = TestView {
            count: 1,
            values: vec!["first".to_string()],
        };

        let context_v0 = ViewContext::new("test-increment".to_string(), 0);
        repo.update_view(initial_view, context_v0).await.unwrap();

        let updated_view = TestView {
            count: 2,
            values: vec!["first".to_string(), "second".to_string()],
        };

        let context_v1 = ViewContext::new("test-increment".to_string(), 1);
        repo.update_view(updated_view, context_v1).await.unwrap();

        let (loaded_view, loaded_context) = repo
            .load_with_context("test-increment")
            .await
            .unwrap()
            .unwrap();

        assert_eq!(loaded_view.count, 2);
        assert_eq!(loaded_view.values, vec!["first", "second"]);
        assert_eq!(loaded_context.version, 2);
    }

    #[tokio::test]
    async fn test_view_serialization_roundtrip() {
        let pool = create_test_pool().await.unwrap();

        sqlx::query(
            "CREATE TABLE test_view (
                view_id TEXT PRIMARY KEY,
                version BIGINT NOT NULL,
                payload JSON NOT NULL
            )",
        )
        .execute(&pool)
        .await
        .unwrap();

        let repo =
            SqliteViewRepository::<TestView, TestAggregate>::new(pool, "test_view".to_string());

        let original_view = TestView {
            count: 42,
            values: vec!["alpha".to_string(), "beta".to_string(), "gamma".to_string()],
        };

        let context = ViewContext::new("test-roundtrip".to_string(), 0);
        repo.update_view(original_view.clone(), context)
            .await
            .unwrap();

        let loaded_view = repo.load("test-roundtrip").await.unwrap().unwrap();

        assert_eq!(loaded_view, original_view);
    }

    #[tokio::test]
    async fn test_multiple_views() {
        let pool = create_test_pool().await.unwrap();

        sqlx::query(
            "CREATE TABLE test_view (
                view_id TEXT PRIMARY KEY,
                version BIGINT NOT NULL,
                payload JSON NOT NULL
            )",
        )
        .execute(&pool)
        .await
        .unwrap();

        let repo =
            SqliteViewRepository::<TestView, TestAggregate>::new(pool, "test_view".to_string());

        let view1 = TestView {
            count: 1,
            values: vec!["one".to_string()],
        };
        let view2 = TestView {
            count: 2,
            values: vec!["two".to_string()],
        };
        let view3 = TestView {
            count: 3,
            values: vec!["three".to_string()],
        };

        let context1 = ViewContext::new("view-1".to_string(), 0);
        let context2 = ViewContext::new("view-2".to_string(), 0);
        let context3 = ViewContext::new("view-3".to_string(), 0);

        repo.update_view(view1.clone(), context1).await.unwrap();
        repo.update_view(view2.clone(), context2).await.unwrap();
        repo.update_view(view3.clone(), context3).await.unwrap();

        let loaded1 = repo.load("view-1").await.unwrap().unwrap();
        let loaded2 = repo.load("view-2").await.unwrap().unwrap();
        let loaded3 = repo.load("view-3").await.unwrap().unwrap();

        assert_eq!(loaded1, view1);
        assert_eq!(loaded2, view2);
        assert_eq!(loaded3, view3);
    }

    #[tokio::test]
    async fn test_optimistic_locking() {
        let pool = create_test_pool().await.unwrap();

        sqlx::query(
            "CREATE TABLE test_view (
                view_id TEXT PRIMARY KEY,
                version BIGINT NOT NULL,
                payload JSON NOT NULL
            )",
        )
        .execute(&pool)
        .await
        .unwrap();

        let repo =
            SqliteViewRepository::<TestView, TestAggregate>::new(pool, "test_view".to_string());

        let initial_view = TestView {
            count: 1,
            values: vec!["initial".to_string()],
        };

        let context_v0 = ViewContext::new("test-lock".to_string(), 0);
        repo.update_view(initial_view, context_v0).await.unwrap();

        let updated_view = TestView {
            count: 2,
            values: vec!["updated".to_string()],
        };

        let context_v1 = ViewContext::new("test-lock".to_string(), 1);
        repo.update_view(updated_view, context_v1).await.unwrap();

        let stale_view = TestView {
            count: 99,
            values: vec!["stale".to_string()],
        };

        let stale_context_v1 = ViewContext::new("test-lock".to_string(), 1);
        let result = repo.update_view(stale_view, stale_context_v1).await;

        assert!(matches!(
            result.unwrap_err(),
            PersistenceError::OptimisticLockError
        ));

        let (loaded_view, loaded_context) =
            repo.load_with_context("test-lock").await.unwrap().unwrap();

        assert_eq!(loaded_view.count, 2);
        assert_eq!(loaded_view.values, vec!["updated"]);
        assert_eq!(loaded_context.version, 2);
    }
}
