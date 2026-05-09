-- Materialized view for the SupportTicket aggregate.
-- The `status` column is GENERATED from the JSON payload at path
-- `$.Live.status` (the library wraps each entity in a `Lifecycle::Live`
-- enum before serializing). Pushing the predicate into a real column with
-- an index lets `Projection::filter(STATUS, &Status::Open)` scan only
-- matching rows instead of every row.

CREATE TABLE IF NOT EXISTS support_ticket_view (
    view_id TEXT PRIMARY KEY,
    version BIGINT NOT NULL,
    payload JSON NOT NULL,
    status TEXT GENERATED ALWAYS AS
        (json_extract(payload, '$.Live.status')) STORED
);

CREATE INDEX IF NOT EXISTS idx_support_ticket_view_status
    ON support_ticket_view(status) WHERE status IS NOT NULL;
