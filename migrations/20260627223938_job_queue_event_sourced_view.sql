-- Reshape job_queue into a cqrs-es view table of the `job` aggregate (ADR-0006).
--
-- The row is the standard (view_id, version, payload) projection of
-- Lifecycle<JobState>, maintained by the generic projection reactor (acks) and
-- the claim primitive (Enqueued seed + Claimed). `lease_until` is the ONLY
-- projection-only column: the live lease is not folded from events (so a
-- renewal -- a bare UPDATE of this column -- is invisible to the event stream,
-- which is exactly why claiming re-reads it under BEGIN IMMEDIATE). The poll
-- keys (kind/status/run_at) are generated from the payload.
DROP INDEX IF EXISTS job_queue_runnable;
DROP INDEX IF EXISTS job_queue_reclaim;
DROP TABLE IF EXISTS job_queue;

CREATE TABLE job_queue (
    view_id     TEXT    NOT NULL PRIMARY KEY,   -- = job_id
    version     INTEGER NOT NULL,               -- == last applied event sequence
    payload     TEXT    NOT NULL,               -- Lifecycle<JobState> JSON
    -- Projection-only live lease (Unix epoch ms). Written by claim + renew only;
    -- the reactor never touches it. NULL on pending/terminal rows and on a
    -- rebuilt-but-unclaimed row.
    lease_until INTEGER,
    kind   TEXT    GENERATED ALWAYS AS (
        COALESCE(json_extract(payload, '$.Live.Pending.kind'),
                 json_extract(payload, '$.Live.Claimed.kind'))) STORED,
    status TEXT    GENERATED ALWAYS AS (
        CASE WHEN json_type(payload, '$.Live.Pending') IS NOT NULL THEN 'pending'
             WHEN json_type(payload, '$.Live.Claimed') IS NOT NULL THEN 'claimed'
             WHEN json_extract(payload, '$.Live') = 'Done'         THEN 'done'
             WHEN json_type(payload, '$.Live.Dead') IS NOT NULL    THEN 'dead'
             ELSE NULL END) STORED,
    run_at INTEGER GENERATED ALWAYS AS (
        COALESCE(json_extract(payload, '$.Live.Pending.run_at'),
                 json_extract(payload, '$.Live.Claimed.run_at'))) STORED
);

-- The pending-and-due poll branch.
CREATE INDEX job_queue_runnable ON job_queue (kind, status, run_at);
-- The claimed-with-expired-lease reclaim branch.
CREATE INDEX job_queue_reclaim ON job_queue (kind, status, lease_until);
