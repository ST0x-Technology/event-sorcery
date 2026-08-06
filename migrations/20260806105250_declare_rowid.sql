-- Give the event log a durable global position.
--
-- events_since / head_rowid made events.rowid public API: checkpointed
-- ingesters persist it as a durable watermark. The implicit rowid SQLite
-- provides for a table without an INTEGER PRIMARY KEY column guarantees
-- neither immutability (full VACUUM may renumber it) nor uniqueness over
-- time (deleting the max row lets the next insert reuse its rowid, e.g.
-- after event compaction). Rebuild the table with an explicit
-- AUTOINCREMENT primary key: `id` aliases the rowid, VACUUM preserves it,
-- and sqlite_sequence forbids reuse for the lifetime of the table.
--
-- The copy carries each row's current rowid into `id`, so existing global
-- positions -- and anything stamped with them -- survive the rebuild. The
-- former composite PRIMARY KEY becomes a UNIQUE constraint; sqlx reports
-- both constraint flavors as unique violations, so optimistic-concurrency
-- detection is unaffected.

CREATE TABLE events_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    sequence BIGINT NOT NULL,
    event_type TEXT NOT NULL,
    event_version TEXT NOT NULL,
    payload JSON NOT NULL,
    metadata JSON NOT NULL,
    UNIQUE (aggregate_type, aggregate_id, sequence)
);

INSERT INTO events_new (
    id, aggregate_type, aggregate_id, sequence,
    event_type, event_version, payload, metadata
)
SELECT
    rowid, aggregate_type, aggregate_id, sequence,
    event_type, event_version, payload, metadata
FROM events
ORDER BY rowid;

DROP TABLE events;

ALTER TABLE events_new RENAME TO events;

CREATE INDEX idx_events_type
    ON events(aggregate_type);
CREATE INDEX idx_events_aggregate
    ON events(aggregate_id);
