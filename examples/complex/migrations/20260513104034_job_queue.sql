-- event-sorcery's canonical durable-jobs view table (see the library's
-- migrations/). A consumer that uses jobs replays this alongside the events +
-- snapshots schema. It is a cqrs-es view table of the `job` aggregate: the
-- (view_id, version, payload) projection of Lifecycle<JobState> plus the
-- projection-only `lease_until` column; the poll keys are generated from payload.
CREATE TABLE job_queue (
    view_id     TEXT    NOT NULL PRIMARY KEY,
    version     INTEGER NOT NULL,
    payload     TEXT    NOT NULL,
    lease_until INTEGER,
    kind   TEXT    GENERATED ALWAYS AS (
        COALESCE(json_extract(payload, '$.Live.Pending.kind'),
                 json_extract(payload, '$.Live.Claimed.kind'))) STORED,
    status TEXT    GENERATED ALWAYS AS (
        CASE WHEN json_type(payload, '$.Live.Pending') IS NOT NULL THEN 'pending'
             WHEN json_type(payload, '$.Live.Claimed') IS NOT NULL THEN 'claimed'
             WHEN json_extract(payload, '$.Live') = 'Done'         THEN 'done'
             WHEN json_type(payload, '$.Live.Dead')    IS NOT NULL THEN 'dead'
             ELSE NULL END) STORED,
    run_at INTEGER GENERATED ALWAYS AS (
        COALESCE(json_extract(payload, '$.Live.Pending.run_at'),
                 json_extract(payload, '$.Live.Claimed.run_at'))) STORED
);

CREATE INDEX job_queue_runnable ON job_queue (kind, status, run_at);
CREATE INDEX job_queue_reclaim ON job_queue (kind, status, lease_until);
