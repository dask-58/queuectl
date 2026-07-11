package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWorkerRegistration(t *testing.T) {
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "queuectl.db")
		s, err := Open(path)
		require.NoError(t, err)

		err = s.RegisterWorker(ctx, "worker-1", 1234)
		require.NoError(t, err)

		require.NoError(t, s.Close())

		// Verify persistence after reopen
		s2, err := Open(path)
		require.NoError(t, err)
		defer s2.Close()

		// Heartbeat should succeed if it exists
		err = s2.Heartbeat(ctx, "worker-1")
		require.NoError(t, err)
	})

	t.Run("duplicate registration", func(t *testing.T) {
		s := openTestStore(t)
		err := s.RegisterWorker(ctx, "worker-1", 1234)
		require.NoError(t, err)

		err = s.RegisterWorker(ctx, "worker-1", 5678)
		require.ErrorIs(t, err, ErrWorkerAlreadyExists)
	})

	t.Run("whitespace worker ID rejected", func(t *testing.T) {
		s := openTestStore(t)
		err := s.RegisterWorker(ctx, "   ", 1234)
		require.ErrorContains(t, err, "worker id is required")
	})

	t.Run("invalid pid rejected", func(t *testing.T) {
		s := openTestStore(t)
		err := s.RegisterWorker(ctx, "worker-1", 0)
		require.ErrorContains(t, err, "pid must be greater than zero")

		err = s.RegisterWorker(ctx, "worker-2", -1)
		require.ErrorContains(t, err, "pid must be greater than zero")
	})
}

func TestWorkerHeartbeat(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)

	err := s.RegisterWorker(ctx, "worker-1", 1234)
	require.NoError(t, err)

	// Sleep slightly to ensure timestamp would change if we read it back
	time.Sleep(5 * time.Millisecond)

	err = s.Heartbeat(ctx, "worker-1")
	require.NoError(t, err)

	err = s.Heartbeat(ctx, "worker-missing")
	require.ErrorIs(t, err, ErrWorkerNotFound)
}

func TestWorkerUnregister(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)

	err := s.RegisterWorker(ctx, "worker-1", 1234)
	require.NoError(t, err)

	err = s.UnregisterWorker(ctx, "worker-1")
	require.NoError(t, err)

	// Heartbeat should fail now
	err = s.Heartbeat(ctx, "worker-1")
	require.ErrorIs(t, err, ErrWorkerNotFound)

	// Unregister again should fail
	err = s.UnregisterWorker(ctx, "worker-1")
	require.ErrorIs(t, err, ErrWorkerNotFound)

	err = s.UnregisterWorker(ctx, "worker-missing")
	require.ErrorIs(t, err, ErrWorkerNotFound)
}
