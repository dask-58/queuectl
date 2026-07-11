package worker

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dask-58/queuectl/internal/store"
)

func testDBPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "queuectl.db")
}

func openTestStore(t *testing.T) *store.Store {
	t.Helper()
	s, err := store.Open(testDBPath(t))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	return s
}

// 1. Worker executes one successful job.
func TestWorkerRunSuccess(t *testing.T) {
	s := openTestStore(t)
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

	cancel()
	require.NoError(t, <-errCh)

	j, err := s.Job(context.Background(), job.ID)
	require.NoError(t, err)
	assert.Equal(t, store.JobStateCompleted, j.State)
}

// 2. Worker retries failing jobs correctly.
func TestWorkerRunRetry(t *testing.T) {
	s := openTestStore(t)
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

	cancel()
	require.NoError(t, <-errCh)

	j, err := s.Job(context.Background(), job.ID)
	require.NoError(t, err)
	assert.Greater(t, j.Attempts, 0)
	assert.Equal(t, store.JobStatePending, j.State)
}

// 3. Idle worker exits when context cancelled.
func TestWorkerRunIdleCancel(t *testing.T) {
	s := openTestStore(t)
	ctx, cancel := context.WithCancel(context.Background())

	// Cancel immediately
	cancel()

	err := Run(ctx, s)
	require.NoError(t, err)
}

// 4. Infrastructure error propagates.
func TestWorkerRunInfrastructureError(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	// Close store to force infrastructure error
	err := s.Close()
	require.NoError(t, err)

	err = Run(ctx, s)
	require.Error(t, err)
}
