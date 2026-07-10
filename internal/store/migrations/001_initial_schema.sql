CREATE TABLE jobs (
    id TEXT PRIMARY KEY,
    command TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('pending', 'processing', 'completed', 'failed', 'dead')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    max_retries INTEGER NOT NULL CHECK (max_retries >= 0),
    backoff_base INTEGER NOT NULL CHECK (backoff_base >= 1),
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    next_run_at INTEGER NULL,
    worker_id TEXT NULL,
    lease_expires_at INTEGER NULL,
    started_at INTEGER NULL,
    completed_at INTEGER NULL,
    exit_code INTEGER NULL,
    last_error TEXT NULL
);

CREATE INDEX idx_jobs_state ON jobs(state);

CREATE TABLE config (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE workers (
    id TEXT PRIMARY KEY,
    pid INTEGER NOT NULL,
    started_at INTEGER NOT NULL,
    heartbeat_at INTEGER NOT NULL
);
