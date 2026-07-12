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

// Job states define the lifecycle phases of a queued job.
const (
	JobStatePending    = "pending"
	JobStateProcessing = "processing"
	JobStateCompleted  = "completed"
	JobStateFailed     = "failed"
	JobStateDead       = "dead"
)

// Sentinel errors for common job failures.
var (
	ErrJobAlreadyExists = errors.New("job already exists")
	ErrJobNotFound      = errors.New("job not found")
	ErrNoPendingJobs    = errors.New("no pending jobs")
)

const defaultLeaseDuration = 30 * time.Second

// IsValidJobState checks if the provided state is a recognized job state.
func IsValidJobState(state string) bool {
	switch state {
	case JobStatePending, JobStateProcessing, JobStateCompleted, JobStateFailed, JobStateDead:
		return true
	}
	return false
}

// Job represents a queued job.
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

// Enqueue creates a new pending job.
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
		State:       JobStatePending,
		Attempts:    0,
		MaxRetries:  maxRetries,
		BackoffBase: backoffBase,
		CreatedAt:   now,
		UpdatedAt:   now,
	}, nil
}

type scanner interface {
	Scan(dest ...any) error
}

func scanJob(s scanner) (*Job, error) {
	var j Job
	err := s.Scan(
		&j.ID, &j.Command, &j.State, &j.Attempts, &j.MaxRetries, &j.BackoffBase,
		&j.CreatedAt, &j.UpdatedAt, &j.NextRunAt, &j.WorkerID,
		&j.LeaseExpiresAt, &j.StartedAt, &j.CompletedAt, &j.ExitCode, &j.LastError,
	)
	if err != nil {
		return nil, err
	}
	return &j, nil
}

func queryJobByID(ctx context.Context, q interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}, id string) (*Job, error) {
	row := q.QueryRowContext(ctx,
		`SELECT id, command, state, attempts, max_retries, backoff_base,
				created_at, updated_at, next_run_at, worker_id,
				lease_expires_at, started_at, completed_at, exit_code, last_error
		 FROM jobs WHERE id = ?`, id)

	j, err := scanJob(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrJobNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("query job: %w", err)
	}

	return j, nil
}

// Job retrieves a job by its unique identifier.
func (s *Store) Job(ctx context.Context, id string) (*Job, error) {
	return queryJobByID(ctx, s.db, id)
}

// ListJobsByState retrieves all jobs matching the provided state, ordered by creation time.
func (s *Store) ListJobsByState(ctx context.Context, state string) ([]Job, error) {
	if !IsValidJobState(state) {
		return nil, fmt.Errorf("invalid job state: %q", state)
	}

	rows, err := s.db.QueryContext(ctx,
		`SELECT id, command, state, attempts, max_retries, backoff_base,
				created_at, updated_at, next_run_at, worker_id,
				lease_expires_at, started_at, completed_at, exit_code, last_error
		 FROM jobs WHERE state = ? ORDER BY created_at ASC, id ASC`, state)
	if err != nil {
		return nil, fmt.Errorf("query jobs by state: %w", err)
	}
	defer rows.Close()

	jobs := make([]Job, 0)
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, fmt.Errorf("scan job: %w", err)
		}
		jobs = append(jobs, *j)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate jobs: %w", err)
	}

	return jobs, nil
}

// ListRecentJobs retrieves the most recently created jobs up to a limit.
func (s *Store) ListRecentJobs(ctx context.Context, limit int) ([]Job, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, command, state, attempts, max_retries, backoff_base,
				created_at, updated_at, next_run_at, worker_id,
				lease_expires_at, started_at, completed_at, exit_code, last_error
		 FROM jobs ORDER BY created_at DESC, id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, fmt.Errorf("query recent jobs: %w", err)
	}
	defer rows.Close()

	jobs := make([]Job, 0)
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, fmt.Errorf("scan job: %w", err)
		}
		jobs = append(jobs, *j)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate jobs: %w", err)
	}

	return jobs, nil
}

// ClaimNextJob claims the oldest runnable job for a worker.
func (s *Store) ClaimNextJob(ctx context.Context, workerID string) (*Job, error) {
	workerID = strings.TrimSpace(workerID)
	if workerID == "" {
		return nil, fmt.Errorf("worker ID is required")
	}

	conn, err := s.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire connection: %w", err)
	}
	defer conn.Close()

	// acquire a write lock so no other worker claims this job concurrently
	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return nil, fmt.Errorf("begin immediate transaction: %w", err)
	}

	// if anything goes wrong, rollback the transaction to release the write lock
	var committed bool
	defer func() {
		if !committed {
			_ = rollbackTransaction(conn)
		}
	}()

	now := time.Now().UTC().UnixMilli()

	var id string
	err = conn.QueryRowContext(ctx,
		`SELECT id FROM jobs
		 WHERE state IN (?, ?) AND (next_run_at IS NULL OR next_run_at <= ?)
		 ORDER BY created_at ASC, id ASC LIMIT 1`,
		JobStatePending, JobStateFailed, now,
	).Scan(&id)
	// no rows matches the criteria, meaning there are no pending jobs to claim
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNoPendingJobs
	}
	if err != nil {
		return nil, fmt.Errorf("find next pending job: %w", err)
	}

	expiresAt := now + defaultLeaseDuration.Milliseconds()

	res, err := conn.ExecContext(ctx,
		`UPDATE jobs SET state = ?, worker_id = ?, started_at = ?, lease_expires_at = ?, next_run_at = NULL, updated_at = ? WHERE id = ?`,
		JobStateProcessing, workerID, now, expiresAt, now, id,
	)
	if err != nil {
		return nil, fmt.Errorf("update claimed job: %w", err)
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("check rows affected: %w", err)
	}
	if rowsAffected != 1 {
		return nil, fmt.Errorf("internal error: expected 1 row affected, got %d", rowsAffected)
	}

	j, err := queryJobByID(ctx, conn, id)
	if err != nil {
		return nil, fmt.Errorf("query updated job: %w", err)
	}

	// close the transaction to release the write lock so other workers can claim jobs
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		return nil, fmt.Errorf("commit transaction: %w", err)
	}
	committed = true

	return j, nil
}

// FinishJob records the result of a claimed job.
func (s *Store) FinishJob(ctx context.Context, jobID string, exitCode int, stderr string) (*Job, error) {
	return s.finishJob(ctx, "", jobID, exitCode, stderr)
}

// FinishJobOwned records the result only if the job is still owned by workerID.
func (s *Store) FinishJobOwned(ctx context.Context, workerID, jobID string, exitCode int, stderr string) (*Job, error) {
	workerID = strings.TrimSpace(workerID)
	if workerID == "" {
		return nil, fmt.Errorf("worker ID is required")
	}
	return s.finishJob(ctx, workerID, jobID, exitCode, stderr)
}

func (s *Store) finishJob(ctx context.Context, workerID, jobID string, exitCode int, stderr string) (*Job, error) {
	jobID = strings.TrimSpace(jobID)
	if jobID == "" {
		return nil, fmt.Errorf("job ID is required")
	}

	conn, err := s.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire connection: %w", err)
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return nil, fmt.Errorf("begin immediate transaction: %w", err)
	}

	var committed bool
	defer func() {
		if !committed {
			_ = rollbackTransaction(conn)
		}
	}()

	currentJob, err := queryJobByID(ctx, conn, jobID)
	if err != nil {
		if errors.Is(err, ErrJobNotFound) {
			return nil, ErrJobNotFound
		}
		return nil, fmt.Errorf("query current job: %w", err)
	}

	if currentJob.State != JobStateProcessing {
		return nil, fmt.Errorf("job %q is in state %q, expected %q", jobID, currentJob.State, JobStateProcessing)
	}
	if workerID != "" && (currentJob.WorkerID == nil || *currentJob.WorkerID != workerID) {
		return nil, fmt.Errorf("job %q is owned by another worker", jobID)
	}

	now := time.Now().UTC().UnixMilli()
	newAttempts := currentJob.Attempts + 1
	var newState string
	var nextRunAt *int64
	var completedAt *int64

	if exitCode == 0 {
		newState = JobStateCompleted
		completedAt = &now
	} else {
		if newAttempts <= currentJob.MaxRetries {
			newState = JobStateFailed
			delayMs := retryDelay(currentJob.BackoffBase, newAttempts)
			nextRun := now + delayMs.Milliseconds()
			nextRunAt = &nextRun
		} else {
			newState = JobStateDead
			completedAt = &now
		}
	}

	var lastError *string
	if exitCode != 0 {
		lastError = &stderr
	}

	res, err := conn.ExecContext(ctx,
		`UPDATE jobs SET state = ?, attempts = ?, updated_at = ?, completed_at = ?, next_run_at = ?, exit_code = ?, last_error = ?, worker_id = NULL, lease_expires_at = NULL WHERE id = ?`,
		newState, newAttempts, now, completedAt, nextRunAt, exitCode, lastError, jobID,
	)
	if err != nil {
		return nil, fmt.Errorf("update finished job: %w", err)
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("check rows affected: %w", err)
	}
	if rowsAffected != 1 {
		return nil, fmt.Errorf("internal error: expected 1 row affected, got %d", rowsAffected)
	}

	finalJob, err := queryJobByID(ctx, conn, jobID)
	if err != nil {
		if errors.Is(err, ErrJobNotFound) {
			return nil, ErrJobNotFound
		}
		return nil, fmt.Errorf("query updated job: %w", err)
	}

	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		return nil, fmt.Errorf("commit transaction: %w", err)
	}
	committed = true

	return finalJob, nil
}

// RetryDeadJob moves a dead job back to the pending state.
func (s *Store) RetryDeadJob(ctx context.Context, id string) (*Job, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return nil, fmt.Errorf("job id is required")
	}

	conn, err := s.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire connection: %w", err)
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return nil, fmt.Errorf("begin immediate transaction: %w", err)
	}

	var committed bool
	defer func() {
		if !committed {
			_ = rollbackTransaction(conn)
		}
	}()

	j, err := queryJobByID(ctx, conn, id)
	if err != nil {
		if errors.Is(err, ErrJobNotFound) {
			return nil, ErrJobNotFound
		}
		return nil, fmt.Errorf("query job: %w", err)
	}

	if j.State != JobStateDead {
		return nil, fmt.Errorf("job %q is in state %q, expected %q", id, j.State, JobStateDead)
	}

	now := time.Now().UTC().UnixMilli()

	res, err := conn.ExecContext(ctx, `
		UPDATE jobs
		SET state = ?, worker_id = NULL, lease_expires_at = NULL, next_run_at = NULL,
		    completed_at = NULL, exit_code = NULL, last_error = NULL, updated_at = ?
		WHERE id = ?`,
		JobStatePending, now, id,
	)
	if err != nil {
		return nil, fmt.Errorf("update job: %w", err)
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("check rows affected: %w", err)
	}
	if rows != 1 {
		return nil, fmt.Errorf("internal error: expected 1 row affected, got %d", rows)
	}

	updatedJob, err := queryJobByID(ctx, conn, id)
	if err != nil {
		if errors.Is(err, ErrJobNotFound) {
			return nil, ErrJobNotFound
		}
		return nil, fmt.Errorf("requery updated job: %w", err)
	}

	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		return nil, fmt.Errorf("commit transaction: %w", err)
	}
	committed = true

	return updatedJob, nil
}

// delay = base ^ attempt seconds
func retryDelay(base, attempt int) time.Duration {
	if attempt < 1 {
		return 0
	}
	delay := 1
	for i := 0; i < attempt; i++ {
		delay *= base
	}
	return time.Duration(delay) * time.Second
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

// RecoverExpiredJobs releases jobs whose worker leases have expired.
func (s *Store) RecoverExpiredJobs(ctx context.Context) (int, error) {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return 0, fmt.Errorf("acquire connection: %w", err)
	}
	defer conn.Close()

	now := time.Now().UTC().UnixMilli()

	query := `UPDATE jobs
		SET state = ?, worker_id = NULL, lease_expires_at = NULL, next_run_at = NULL, updated_at = ?
		WHERE state = ? AND lease_expires_at <= ?`

	res, err := conn.ExecContext(ctx, query, JobStatePending, now, JobStateProcessing, now)
	if err != nil {
		return 0, fmt.Errorf("recover expired jobs: %w", err)
	}

	affected, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("check rows affected: %w", err)
	}

	return int(affected), nil
}

// isJobIDConflict detects a SQLite UNIQUE constraint violation on jobs.id,
// isolating driver-specific error inspection to one helper.
func isJobIDConflict(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "UNIQUE constraint failed: jobs.id")
}
