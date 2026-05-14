//! Pluggable storage backend for materialized views.
//!
//! [`ViewBackend`] is a higher-kinded-type emulation: it supplies,
//! for each `(View, Aggregate)` pair, a concrete [`ViewRepository`]
//! implementation. The default is [`SqliteViewBackend`]; custom
//! backends plug in alternative storage (in-memory for tests,
//! Postgres, etc.).

use cqrs_es::Aggregate;
use cqrs_es::persist::ViewRepository;
use sqlite_es::SqliteViewRepository;

/// Pluggable storage backend for materialized views, supplying a
/// concrete [`ViewRepository`] for each `(View, Aggregate)` pair.
///
/// Implemented as a higher-kinded type emulation: the GAT
/// [`Repo`](Self::Repo) is a "type-level function"
/// `(View, Aggregate) -> SomeRepo` that [`crate::Projection`]
/// applies internally to obtain a repository for
/// `(Lifecycle<Entity>, Lifecycle<Entity>)`. This lets
/// `Projection<Entity, Backend>` express its repository requirement
/// without naming the `pub(crate)` `Lifecycle` type in any public
/// bound — the `Lifecycle` saturation happens inside the struct,
/// not in the impl `where` clauses.
///
/// Why this exists: Rust lacks native HKT, so we cannot write
/// `Projection<Entity, Repo<_, _>>` and have the compiler apply
/// `Repo` to `Lifecycle<Entity>`. The GAT-on-trait pattern below
/// is the standard workaround. The `Send + Sync + 'static` bound
/// on the GAT itself means downstream impls don't have to repeat
/// these bounds (and so don't reintroduce `Lifecycle` into their
/// `where` clauses).
pub trait ViewBackend: Send + Sync + 'static {
    /// View repository for views of `View` over `Aggregate`.
    type Repo<View, Agg>: ViewRepository<View, Agg> + Send + Sync + 'static
    where
        View: cqrs_es::View<Agg> + Clone + 'static,
        Agg: Aggregate + 'static;
}

/// Default [`ViewBackend`]: every `(View, Agg)` pair maps to a
/// [`SqliteViewRepository`].
#[derive(Debug, Clone, Copy)]
pub struct SqliteViewBackend;

impl ViewBackend for SqliteViewBackend {
    type Repo<View, Agg>
        = SqliteViewRepository<View, Agg>
    where
        View: cqrs_es::View<Agg> + Clone + 'static,
        Agg: Aggregate + 'static;
}
