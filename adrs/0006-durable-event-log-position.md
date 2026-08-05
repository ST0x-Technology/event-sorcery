# ADR-0006: Durable global position for the event log

## Status

Proposed.

## Context

`events_since` / `head_rowid` (PR #47) established `events.rowid` as the global
position of the shared event log: checkpointed read-model ingesters persist a
rowid watermark and resume from it across process restarts. That contract
silently assumes rowids are immutable and never reused. The current schema does
not guarantee either property, because `events` has a TEXT composite primary key
and therefore only an _implicit_ rowid:

1. **Reuse after compaction.** Without `AUTOINCREMENT`, SQLite assigns
   `max(rowid) + 1` to new rows. `compact_events` deletes events covered by a
   snapshot with no regard for position, so it can delete the row holding the
   global maximum (the newest event of the most recently compacted aggregate).
   The next insert then receives a rowid at or below existing checkpoints, and
   every ingester whose watermark is >= that rowid skips the new event
   permanently. Silent data loss in derived read models.

2. **Renumbering by VACUUM.** For rowid tables without an explicit
   `INTEGER PRIMARY KEY` column, full `VACUUM` may renumber rowids. This library
   exports a public `vacuum()` helper, so a consumer following our own API can
   invalidate every persisted checkpoint -- and stamped provenance -- in one
   call.

Fixing this properly requires the events table to carry an explicit,
never-reused position column. That collides with two standing constraints:
AGENTS.md forbids migrations to the `events` table after the initial one (the
schema is part of the library's public contract), and both crates are already
published at 0.2.0, so existing consumer databases were created from the current
migration.

## Options

### A. Amend the initial migration: explicit `id INTEGER PRIMARY KEY AUTOINCREMENT`

Change the canonical `events` DDL to:

- `id INTEGER PRIMARY KEY AUTOINCREMENT` -- an explicit rowid alias, so `VACUUM`
  preserves values and `AUTOINCREMENT` forbids reuse for the lifetime of the
  table (enforced via `sqlite_sequence`).
- Demote `(aggregate_type, aggregate_id, sequence)` to a `UNIQUE` constraint.
  sqlx maps both `SQLITE_CONSTRAINT_PRIMARYKEY` and `SQLITE_CONSTRAINT_UNIQUE`
  to `is_unique_violation()`, so sqlite-es optimistic-concurrency detection is
  unaffected.

`events_since` / `head_rowid` keep querying `rowid`; it is now an alias for
`id`. No API change.

Costs: existing 0.2.x databases must be rebuilt (SQLite cannot add a PRIMARY KEY
in place: create-new-table, copy preserving `rowid` into `id`, drop, and rename
-- a documented recipe for consumers). Inserts pay the small `sqlite_sequence`
bookkeeping cost. The no-events-migrations rule needs a carve-out acknowledging
this amendment as part of defining the contract the rule protects.

### B. Pin the newest row in `compact_events` and drop `vacuum()`

Add `AND rowid < (SELECT MAX(rowid) FROM events)` to the compaction delete, so
the global maximum rowid is never freed and `max(rowid) + 1` stays monotonic;
remove (or loudly re-document) `vacuum()` since full VACUUM still renumbers
implicit rowids. `incremental_vacuum` preserves rowids and remains.

Costs: correctness rests on an undocumented-adjacent SQLite allocation detail
rather than a declared schema guarantee; one stale event row is retained
indefinitely; the public API loses (or keeps a footgun in) `vacuum()`; the
watermark contract is still invisible in the schema for anyone operating on the
database directly.

### C. Document the limitation

State that `events_since` checkpoints are unsound for deployments using
compaction or `vacuum()`. Rejected outright: this library feeds financial
systems, and "the sanctioned ingestion API silently loses events under
documented maintenance operations" is not a contract worth shipping.

## Open question: how Option A reaches already-migrated databases

**Must be resolved before implementation.** Production consumers have already
applied `20251016210348_init.sql`, and sqlx records each applied migration's
checksum in `_sqlx_migrations`. The two delivery shapes for Option A differ only
operationally, but the difference is decisive for a live database:

1. **Amend the initial migration file in place.** Fresh databases get the new
   schema directly, but every existing database fails at startup with sqlx's
   version-mismatch error ("migration was previously applied but has been
   modified") until someone rebuilds the table by hand _and_ patches the
   recorded checksum. Manual surgery on production financial databases.
2. **Ship the change as a second migration** that rebuilds the table: create
   `events_new` with `id INTEGER PRIMARY KEY AUTOINCREMENT` and the `UNIQUE`
   constraint,
   `INSERT INTO events_new (id, ...) SELECT rowid, ...
   FROM events ORDER BY rowid`
   (preserving every existing position and seeding `sqlite_sequence`), drop the
   old table, rename. Existing deployments upgrade unattended on the next
   deploy; fresh installs end up identical. This is literally the act AGENTS.md
   forbids ("no migrations to the events table after the initial one"), but both
   shapes change the protected contract equally -- shape 2 is only the safer
   delivery vehicle, so it needs the same carve-out with none of the operational
   risk.

Since `events_since` ships in the same release, no rowid checkpoints exist in
production yet; the copy preserves positions either way.

## Decision

Option A. Pre-1.0 is exactly when the schema contract should be corrected: the
typed event stream is the feature that turns `rowid` into public API, and it
should not ship on a position that SQLite is free to reuse or rewrite. The
migration amendment lands together with the feature that depends on it, and the
rebuild recipe gives 0.2.x consumers a safe upgrade path.

## Consequences

- The canonical migration gains `id INTEGER PRIMARY KEY AUTOINCREMENT` and a
  `UNIQUE(aggregate_type, aggregate_id, sequence)` constraint; the `.sqlx` cache
  is regenerated.
- `Sequenced::rowid`, persisted watermarks, and provenance stamps become durable
  across compaction and all vacuum modes.
- CHANGELOG documents the breaking schema change and the rebuild recipe for
  databases created from the previous migration.
- AGENTS.md's events-migration prohibition gains a note that ADR-0006 amended
  the initial migration before the schema was frozen.
