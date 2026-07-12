package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStatusEmptyDatabase(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	st, err := s.Status(ctx)
	require.NoError(t, err)

	assert.Equal(t, 0, st.PendingJobs)
	assert.Equal(t, 0, st.ProcessingJobs)
	assert.Equal(t, 0, st.CompletedJobs)
	assert.Equal(t, 0, st.FailedJobs)
	assert.Equal(t, 0, st.DeadJobs)
	assert.Equal(t, 0, st.ActiveWorkers)
}

func TestStatusMixedStates(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().UnixMilli()

	insertDetailedJob(t, s, "job-pending", JobStatePending, nil)
	insertDetailedJob(t, s, "job-processing", JobStateProcessing, &now)
	insertDetailedJob(t, s, "job-completed", JobStateCompleted, nil)
	insertDetailedJob(t, s, "job-failed", JobStateFailed, nil)
	insertDetailedJob(t, s, "job-dead", JobStateDead, nil)

	st, err := s.Status(ctx)
	require.NoError(t, err)

	assert.Equal(t, 1, st.PendingJobs)
	assert.Equal(t, 1, st.ProcessingJobs)
	assert.Equal(t, 1, st.CompletedJobs)
	assert.Equal(t, 1, st.FailedJobs)
	assert.Equal(t, 1, st.DeadJobs)
	assert.Equal(t, 0, st.ActiveWorkers)
}

func TestStatusActiveWorkers(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	err := s.RegisterWorker(ctx, "worker-1", 111)
	require.NoError(t, err)
	err = s.RegisterWorker(ctx, "worker-2", 222)
	require.NoError(t, err)

	st, err := s.Status(ctx)
	require.NoError(t, err)

	assert.Equal(t, 2, st.ActiveWorkers)

	err = s.UnregisterWorker(ctx, "worker-1")
	require.NoError(t, err)

	st, err = s.Status(ctx)
	require.NoError(t, err)

	assert.Equal(t, 1, st.ActiveWorkers)
}

func TestStatusIgnoresStaleWorkers(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	err := s.RegisterWorker(ctx, "worker-1", 111)
	require.NoError(t, err)

	staleHeartbeat := time.Now().UTC().UnixMilli() - defaultLeaseDuration.Milliseconds() - 1
	_, err = s.db.Exec("UPDATE workers SET heartbeat_at = ? WHERE id = ?", staleHeartbeat, "worker-1")
	require.NoError(t, err)

	st, err := s.Status(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, st.ActiveWorkers)
}

func TestStatusSurvivesReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "queuectl.db")
	s, err := Open(path)
	require.NoError(t, err)

	ctx := context.Background()
	insertDetailedJob(t, s, "job-1", JobStateCompleted, nil)
	require.NoError(t, s.RegisterWorker(ctx, "worker-1", 123))

	st, err := s.Status(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, st.CompletedJobs)
	assert.Equal(t, 1, st.ActiveWorkers)

	require.NoError(t, s.Close())

	s2, err := Open(path)
	require.NoError(t, err)
	defer s2.Close()

	st2, err := s2.Status(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, st2.CompletedJobs)
	assert.Equal(t, 1, st2.ActiveWorkers)
}
