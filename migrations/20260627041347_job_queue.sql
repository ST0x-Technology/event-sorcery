-- Read model for the event-sourced job backend: one row per job, folded from
-- the job's event stream (aggregate_type = 'job'). Lets a worker poll for
-- runnable jobs with an indexed query instead of scanning the events table.
-- Fully rebuildable from the events table, like any projection.
CREATE TABLE job_queue (
    job_id      TEXT    NOT NULL PRIMARY KEY,
    kind        TEXT    NOT NULL,
    status      TEXT    NOT NULL CHECK (status IN ('pending', 'claimed', 'done', 'dead')),
    -- Unix epoch milliseconds. When a pending job becomes runnable.
    run_at      INTEGER NOT NULL,
    -- Set iff status = 'claimed'; the claim expires (and the job becomes
    -- re-claimable) once it passes. Unix epoch milliseconds.
    lease_until INTEGER,
    attempt     INTEGER NOT NULL DEFAULT 0,
    -- Last applied sequence on the job's event stream, used as the
    -- expected-version for the compare-and-swap claim append.
    sequence    INTEGER NOT NULL,
    -- A lease exists exactly when the job is claimed.
    CHECK ((status = 'claimed') = (lease_until IS NOT NULL))
);

-- The worker poll: runnable jobs of a given kind, oldest run_at first.
CREATE INDEX job_queue_runnable ON job_queue (kind, status, run_at);
