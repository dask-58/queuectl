package store

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func insertDetailedJob(t *testing.T, s *Store, id, state string, leaseExpiresAt *int64) {
	t.Helper()
	var lease interface{} = leaseExpiresAt
	if leaseExpiresAt == nil {
		lease = nil
	}

	_, err := s.db.Exec(`
		INSERT INTO jobs (
			id, command, state, attempts, max_retries, backoff_base, 
			created_at, updated_at, next_run_at, worker_id, 
			lease_expires_at, started_at, completed_at, exit_code, last_error
		) VALUES (
			?, 'echo test', ?, 2, 5, 2, 
			100, 200, 300, 'worker-1', 
			?, 400, NULL, NULL, NULL
		)
	`, id, state, lease)
	require.NoError(t, err)
}

func TestRecoverExpiredJobs(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().UnixMilli()

	t.Run("expired processing job becomes pending", func(t *testing.T) {
		s := openTestStore(t)
		pastLease := now - 1000
		insertDetailedJob(t, s, "job-1", JobStateProcessing, &pastLease)

		count, err := s.RecoverExpiredJobs(ctx)
		require.NoError(t, err)
		assert.Equal(t, 1, count)

		job, err := s.Job(ctx, "job-1")
		require.NoError(t, err)
		assert.Equal(t, JobStatePending, job.State)
		assert.Nil(t, job.WorkerID)
		assert.Nil(t, job.LeaseExpiresAt)
		assert.Nil(t, job.NextRunAt)
		assert.GreaterOrEqual(t, job.UpdatedAt, now)

		// Verify preserved fields
		assert.Equal(t, 2, job.Attempts)
		assert.Equal(t, 5, job.MaxRetries)
		assert.Equal(t, 2, job.BackoffBase)
		assert.Equal(t, "echo test", job.Command)
		assert.Equal(t, int64(100), job.CreatedAt)
		var startedAt int64 = 400
		assert.Equal(t, &startedAt, job.StartedAt)
	})

	t.Run("future lease unchanged", func(t *testing.T) {
		s := openTestStore(t)
		futureLease := now + 10000
		insertDetailedJob(t, s, "job-1", JobStateProcessing, &futureLease)

		count, err := s.RecoverExpiredJobs(ctx)
		require.NoError(t, err)
		assert.Equal(t, 0, count)

		job, err := s.Job(ctx, "job-1")
		require.NoError(t, err)
		assert.Equal(t, JobStateProcessing, job.State)
		assert.NotNil(t, job.WorkerID)
	})

	t.Run("processing with NULL lease unchanged", func(t *testing.T) {
		s := openTestStore(t)
		insertDetailedJob(t, s, "job-1", JobStateProcessing, nil)

		count, err := s.RecoverExpiredJobs(ctx)
		require.NoError(t, err)
		assert.Equal(t, 0, count)

		job, err := s.Job(ctx, "job-1")
		require.NoError(t, err)
		assert.Equal(t, JobStateProcessing, job.State)
	})

	t.Run("other states unchanged", func(t *testing.T) {
		states := []string{JobStateCompleted, JobStateFailed, JobStateDead, JobStatePending}
		for _, state := range states {
			t.Run(state, func(t *testing.T) {
				s := openTestStore(t)
				pastLease := now - 1000
				insertDetailedJob(t, s, "job-1", state, &pastLease)

				count, err := s.RecoverExpiredJobs(ctx)
				require.NoError(t, err)
				assert.Equal(t, 0, count)

				job, err := s.Job(ctx, "job-1")
				require.NoError(t, err)
				assert.Equal(t, state, job.State)
			})
		}
	})

	t.Run("multiple expired jobs", func(t *testing.T) {
		s := openTestStore(t)
		pastLease := now - 1000
		futureLease := now + 10000

		insertDetailedJob(t, s, "job-1", JobStateProcessing, &pastLease)
		insertDetailedJob(t, s, "job-2", JobStateProcessing, &pastLease)
		insertDetailedJob(t, s, "job-3", JobStateProcessing, &futureLease)
		insertDetailedJob(t, s, "job-4", JobStateProcessing, nil)

		count, err := s.RecoverExpiredJobs(ctx)
		require.NoError(t, err)
		assert.Equal(t, 2, count)

		j1, _ := s.Job(ctx, "job-1")
		assert.Equal(t, JobStatePending, j1.State)

		j2, _ := s.Job(ctx, "job-2")
		assert.Equal(t, JobStatePending, j2.State)

		j3, _ := s.Job(ctx, "job-3")
		assert.Equal(t, JobStateProcessing, j3.State)

		j4, _ := s.Job(ctx, "job-4")
		assert.Equal(t, JobStateProcessing, j4.State)
	})
}
