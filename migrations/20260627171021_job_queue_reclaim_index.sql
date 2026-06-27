-- Index for the worker's expired-lease reclaim poll: claimed jobs whose lease
-- has lapsed (a crashed/stalled holder), found by (kind, status, lease_until).
-- Complements job_queue_runnable, which serves the pending-and-due poll branch.
CREATE INDEX job_queue_reclaim ON job_queue (kind, status, lease_until);
