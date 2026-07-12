CREATE TABLE config_checked (
    key TEXT PRIMARY KEY CHECK (key IN ('max-retries', 'backoff-base')),
    value TEXT NOT NULL CHECK (
        value <> ''
        AND value NOT GLOB '*[^0-9]*'
        AND (
            (key = 'max-retries' AND CAST(value AS INTEGER) >= 0)
            OR (key = 'backoff-base' AND CAST(value AS INTEGER) >= 1)
        )
    ),
    updated_at INTEGER NOT NULL
);

INSERT INTO config_checked (key, value, updated_at)
SELECT key, value, updated_at FROM config;

DROP TABLE config;
ALTER TABLE config_checked RENAME TO config;

CREATE TRIGGER jobs_legal_state_transition
BEFORE UPDATE OF state ON jobs
WHEN NOT (
    OLD.state = NEW.state
    OR (OLD.state = 'pending' AND NEW.state = 'processing')
    OR (OLD.state = 'processing' AND NEW.state = 'completed')
    OR (OLD.state = 'processing' AND NEW.state = 'pending')
    OR (OLD.state = 'processing' AND NEW.state = 'failed')
    OR (OLD.state = 'processing' AND NEW.state = 'dead')
    OR (OLD.state = 'failed' AND NEW.state = 'processing')
    OR (OLD.state = 'dead' AND NEW.state = 'pending')
)
BEGIN
    SELECT RAISE(ABORT, 'illegal job state transition');
END;

CREATE TRIGGER jobs_completed_immutable
BEFORE UPDATE ON jobs
WHEN OLD.state = 'completed'
BEGIN
    SELECT RAISE(ABORT, 'completed jobs are immutable');
END;

CREATE TRIGGER jobs_dead_immutable
BEFORE UPDATE ON jobs
WHEN OLD.state = 'dead' AND NEW.state <> 'pending'
BEGIN
    SELECT RAISE(ABORT, 'dead jobs are immutable except retry');
END;

CREATE TRIGGER jobs_owner_required
BEFORE INSERT ON jobs
WHEN NEW.state = 'processing'
    AND (NEW.worker_id IS NULL OR NEW.lease_expires_at IS NULL)
BEGIN
    SELECT RAISE(ABORT, 'processing jobs require owner and lease');
END;

CREATE TRIGGER jobs_owner_required_update
BEFORE UPDATE ON jobs
WHEN NEW.state = 'processing'
    AND (NEW.worker_id IS NULL OR NEW.lease_expires_at IS NULL)
BEGIN
    SELECT RAISE(ABORT, 'processing jobs require owner and lease');
END;

CREATE TRIGGER jobs_owner_forbidden
BEFORE INSERT ON jobs
WHEN NEW.state <> 'processing'
    AND (NEW.worker_id IS NOT NULL OR NEW.lease_expires_at IS NOT NULL)
BEGIN
    SELECT RAISE(ABORT, 'only processing jobs may have owner or lease');
END;

CREATE TRIGGER jobs_owner_forbidden_update
BEFORE UPDATE ON jobs
WHEN NEW.state <> 'processing'
    AND (NEW.worker_id IS NOT NULL OR NEW.lease_expires_at IS NOT NULL)
BEGIN
    SELECT RAISE(ABORT, 'only processing jobs may have owner or lease');
END;
