package worker

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dask-58/queuectl/internal/store"
)

func testDBPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "queuectl.db")
}

func openTestStore(t *testing.T) (*store.Store, string) {
	t.Helper()
	path := testDBPath(t)
	s, err := store.Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	return s, path
}

func countWorkers(t *testing.T, path string) int {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	require.NoError(t, err)
	defer db.Close()

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM workers").Scan(&count)
	require.NoError(t, err)
	return count
}

// 1. Worker executes one successful job.
func TestWorkerRunSuccess(t *testing.T) {
	s, dbPath := openTestStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	job, err := s.Enqueue(ctx, "job-1", "echo hello")
	require.NoError(t, err)

	errCh := make(chan error, 1)
	go func() {
		errCh <- Run(ctx, s)
	}()

	require.Eventually(t, func() bool {
		j, err := s.Job(context.Background(), job.ID)
		return err == nil && j.State == store.JobStateCompleted
	}, 2*time.Second, 10*time.Millisecond)

	assert.Equal(t, 1, countWorkers(t, dbPath), "worker row should exist while Run is active")

	cancel()
	require.NoError(t, <-errCh)

	assert.Equal(t, 0, countWorkers(t, dbPath), "worker row should disappear after Run exits")

	j, err := s.Job(context.Background(), job.ID)
	require.NoError(t, err)
	assert.Equal(t, store.JobStateCompleted, j.State)
}

// 2. Worker retries failing jobs correctly.
func TestWorkerRunRetry(t *testing.T) {
	s, dbPath := openTestStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	job, err := s.Enqueue(ctx, "job-1", "false")
	require.NoError(t, err)

	errCh := make(chan error, 1)
	go func() {
		errCh <- Run(ctx, s)
	}()

	require.Eventually(t, func() bool {
		j, err := s.Job(context.Background(), job.ID)
		return err == nil && j.Attempts > 0 && j.State == store.JobStatePending
	}, 2*time.Second, 10*time.Millisecond)

	assert.Equal(t, 1, countWorkers(t, dbPath), "worker row should exist while Run is active")

	cancel()
	require.NoError(t, <-errCh)

	assert.Equal(t, 0, countWorkers(t, dbPath), "worker row should disappear after Run exits")

	j, err := s.Job(context.Background(), job.ID)
	require.NoError(t, err)
	assert.Greater(t, j.Attempts, 0)
	assert.Equal(t, store.JobStatePending, j.State)
}

// 3. Idle worker exits when context cancelled.
func TestWorkerRunIdleCancel(t *testing.T) {
	s, dbPath := openTestStore(t)
	ctx, cancel := context.WithCancel(context.Background())

	// Cancel immediately
	cancel()

	err := Run(ctx, s)
	require.NoError(t, err)

	assert.Equal(t, 0, countWorkers(t, dbPath), "worker row should disappear after Run exits")
}

// 4. Infrastructure error propagates.
func TestWorkerRunInfrastructureError(t *testing.T) {
	s, dbPath := openTestStore(t)
	ctx := context.Background()

	// Close store to force infrastructure error
	err := s.Close()
	require.NoError(t, err)

	err = Run(ctx, s)
	require.Error(t, err)

	assert.Equal(t, 0, countWorkers(t, dbPath), "worker row should disappear after Run exits on infrastructure error")
}

// 5. Worker recovers abandoned jobs on startup
func TestWorkerRunRecoversAbandonedJob(t *testing.T) {
	s, dbPath := openTestStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	job, err := s.Enqueue(ctx, "job-recovery", "echo recovery")
	require.NoError(t, err)

	// Simulate an abandoned job
	now := time.Now().UTC().UnixMilli()
	pastLease := now - 1000

	db, err := sql.Open("sqlite", dbPath)
	require.NoError(t, err)
	defer db.Close()

	_, err = db.ExecContext(ctx, "UPDATE jobs SET state = ?, worker_id = 'dead-worker', lease_expires_at = ? WHERE id = ?", store.JobStateProcessing, pastLease, job.ID)
	require.NoError(t, err)

	errCh := make(chan error, 1)
	go func() {
		errCh <- Run(ctx, s)
	}()

	require.Eventually(t, func() bool {
		j, err := s.Job(context.Background(), job.ID)
		return err == nil && j.State == store.JobStateCompleted
	}, 2*time.Second, 10*time.Millisecond)

	cancel()
	require.NoError(t, <-errCh)

	j, err := s.Job(context.Background(), job.ID)
	require.NoError(t, err)
	assert.Equal(t, store.JobStateCompleted, j.State)
}

// 6. Worker gracefully exits when stop is requested
func TestWorkerRunStopGracefully(t *testing.T) {
	s, dbPath := openTestStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Enqueue a long running job and a second job
	job1, err := s.Enqueue(ctx, "job-1", "sleep 1")
	require.NoError(t, err)

	job2, err := s.Enqueue(ctx, "job-2", "echo job-2")
	require.NoError(t, err)

	errCh := make(chan error, 1)
	go func() {
		errCh <- Run(ctx, s)
	}()

	// Wait for job-1 to start processing
	require.Eventually(t, func() bool {
		j, err := s.Job(context.Background(), job1.ID)
		return err == nil && j.State == store.JobStateProcessing
	}, 2*time.Second, 10*time.Millisecond)

	// Request stop while job-1 is executing
	affected, err := s.RequestWorkerStop(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, affected)

	// Wait for the worker to exit naturally (without context cancel)
	require.NoError(t, <-errCh)

	// The worker should unregister itself
	assert.Equal(t, 0, countWorkers(t, dbPath), "worker row should disappear after graceful exit")

	// Verify job-1 completed
	j1, err := s.Job(context.Background(), job1.ID)
	require.NoError(t, err)
	assert.Equal(t, store.JobStateCompleted, j1.State)

	// Verify job-2 remained pending (was not claimed)
	j2, err := s.Job(context.Background(), job2.ID)
	require.NoError(t, err)
	assert.Equal(t, store.JobStatePending, j2.State)
}
