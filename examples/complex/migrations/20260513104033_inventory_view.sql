-- Materialized view for the Inventory aggregate. No generated columns --
-- callers read by primary key (the SKU) via `Projection::load` or scan
-- everything via `load_all`.

CREATE TABLE IF NOT EXISTS inventory_view (
    view_id TEXT PRIMARY KEY,
    version BIGINT NOT NULL,
    payload JSON NOT NULL
);
