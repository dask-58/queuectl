package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

var (
	ErrJobAlreadyExists = errors.New("job already exists")
	ErrJobNotFound      = errors.New("job not found")
)

type Job struct {
	ID             string
	Command        string
	State          string
	Attempts       int
	MaxRetries     int
	BackoffBase    int
	CreatedAt      int64
	UpdatedAt      int64
	NextRunAt      *int64
	WorkerID       *string
	LeaseExpiresAt *int64
	StartedAt      *int64
	CompletedAt    *int64
	ExitCode       *int
	LastError      *string
}

func (s *Store) Enqueue(ctx context.Context, id, command string) (*Job, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return nil, fmt.Errorf("job id is required")
	}
	if strings.TrimSpace(command) == "" {
		return nil, fmt.Errorf("job command is required")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	maxRetries, err := readIntConfig(ctx, tx, "max-retries")
	if err != nil {
		return nil, err
	}
	if maxRetries < 0 {
		return nil, fmt.Errorf("invalid config %q: must be >= 0, got %d", "max-retries", maxRetries)
	}

	backoffBase, err := readIntConfig(ctx, tx, "backoff-base")
	if err != nil {
		return nil, err
	}
	if backoffBase < 1 {
		return nil, fmt.Errorf("invalid config %q: must be >= 1, got %d", "backoff-base", backoffBase)
	}

	now := time.Now().UTC().UnixMilli()

	_, err = tx.ExecContext(ctx,
		`INSERT INTO jobs (id, command, state, attempts, max_retries, backoff_base, created_at, updated_at)
		 VALUES (?, ?, 'pending', 0, ?, ?, ?, ?)`,
		id, command, maxRetries, backoffBase, now, now,
	)
	if err != nil {
		if isJobIDConflict(err) {
			return nil, fmt.Errorf("job %q: %w", id, ErrJobAlreadyExists)
		}
		return nil, fmt.Errorf("insert job: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit transaction: %w", err)
	}

	return &Job{
		ID:          id,
		Command:     command,
		State:       "pending",
		Attempts:    0,
		MaxRetries:  maxRetries,
		BackoffBase: backoffBase,
		CreatedAt:   now,
		UpdatedAt:   now,
	}, nil
}

func (s *Store) Job(ctx context.Context, id string) (*Job, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT id, command, state, attempts, max_retries, backoff_base,
				created_at, updated_at, next_run_at, worker_id,
				lease_expires_at, started_at, completed_at, exit_code, last_error
		 FROM jobs WHERE id = ?`, id)

	var j Job
	err := row.Scan(
		&j.ID, &j.Command, &j.State, &j.Attempts, &j.MaxRetries, &j.BackoffBase,
		&j.CreatedAt, &j.UpdatedAt, &j.NextRunAt, &j.WorkerID,
		&j.LeaseExpiresAt, &j.StartedAt, &j.CompletedAt, &j.ExitCode, &j.LastError,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrJobNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("query job: %w", err)
	}

	return &j, nil
}

func readIntConfig(ctx context.Context, tx *sql.Tx, key string) (int, error) {
	var raw string
	err := tx.QueryRowContext(ctx, "SELECT value FROM config WHERE key = ?", key).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("missing required config %q", key)
	}
	if err != nil {
		return 0, fmt.Errorf("read config %q: %w", key, err)
	}

	val, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid config %q: not a valid integer: %w", key, err)
	}

	return val, nil
}

// isJobIDConflict detects a SQLite UNIQUE constraint violation on jobs.id,
// isolating driver-specific error inspection to one helper.
func isJobIDConflict(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "UNIQUE constraint failed: jobs.id")
}
