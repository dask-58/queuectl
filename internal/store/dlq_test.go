package store

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRetryDeadJobSuccess(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().UnixMilli()

	// Insert a dead job with all fields populated
	_, err := s.db.Exec(`
		INSERT INTO jobs (
			id, command, state, attempts, max_retries, backoff_base, 
			created_at, updated_at, next_run_at, worker_id, 
			lease_expires_at, started_at, completed_at, exit_code, last_error
		) VALUES (
			?, 'echo dead', ?, 3, 3, 2, 
			100, 200, 300, 'worker-1', 
			400, 500, 600, 1, 'some error'
		)
	`, "job-dead", JobStateDead)
	require.NoError(t, err)

	job, err := s.RetryDeadJob(ctx, "job-dead")
	require.NoError(t, err)

	assert.Equal(t, "job-dead", job.ID)
	assert.Equal(t, JobStatePending, job.State)
	assert.Nil(t, job.WorkerID)
	assert.Nil(t, job.LeaseExpiresAt)
	assert.Nil(t, job.NextRunAt)
	assert.Nil(t, job.CompletedAt)
	assert.Nil(t, job.ExitCode)
	assert.Nil(t, job.LastError)
	assert.GreaterOrEqual(t, job.UpdatedAt, now)

	// Preserved fields
	assert.Equal(t, 3, job.Attempts)
	assert.Equal(t, 3, job.MaxRetries)
	assert.Equal(t, 2, job.BackoffBase)
	assert.Equal(t, "echo dead", job.Command)
	assert.Equal(t, int64(100), job.CreatedAt)
	var startedAt int64 = 500
	assert.Equal(t, &startedAt, job.StartedAt)
}

func TestRetryDeadJobMissing(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	_, err := s.RetryDeadJob(ctx, "non-existent")
	assert.ErrorIs(t, err, ErrJobNotFound)
}

func TestRetryDeadJobWrongState(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	states := []string{JobStatePending, JobStateProcessing, JobStateCompleted, JobStateFailed}
	for _, state := range states {
		t.Run(state, func(t *testing.T) {
			_, err := s.db.Exec(`
				INSERT INTO jobs (
					id, command, state, attempts, max_retries, backoff_base, created_at, updated_at
				) VALUES (?, 'echo', ?, 0, 3, 2, 100, 100)
			`, "job-"+state, state)
			require.NoError(t, err)

			_, err = s.RetryDeadJob(ctx, "job-"+state)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "expected \"dead\"")
		})
	}
}

func TestRetryDeadJobWhitespace(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	_, err := s.db.Exec(`
		INSERT INTO jobs (
			id, command, state, attempts, max_retries, backoff_base, 
			created_at, updated_at
		) VALUES (
			?, 'echo dead', ?, 3, 3, 2, 100, 200
		)
	`, "job-spaces", JobStateDead)
	require.NoError(t, err)

	job, err := s.RetryDeadJob(ctx, "  job-spaces  ")
	require.NoError(t, err)
	assert.Equal(t, "job-spaces", job.ID)
	assert.Equal(t, JobStatePending, job.State)

	_, err = s.RetryDeadJob(ctx, "   ")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "job id is required")
}
