package cli

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dask-58/queuectl/internal/store"
)

func testDBPathWorker(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "queuectl.db")
}

func TestWorkerStartCLI(t *testing.T) {
	dbPath := testDBPathWorker(t)

	env := map[string]string{
		"QUEUECTL_DB_PATH": dbPath,
	}

	getenv := func(key string) string {
		return env[key]
	}

	var stdout, stderr bytes.Buffer
	cmd := newRootCommand(&stdout, &stderr, getenv)
	cmd.SetArgs([]string{"worker", "start"})

	// Create cancelable context and cancel it immediately
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cmd.SetContext(ctx)

	err := cmd.Execute()
	require.NoError(t, err, "expected command to exit cleanly on cancel")
}

func TestWorkerStopCLI(t *testing.T) {
	dbPath := testDBPathWorker(t)

	env := map[string]string{
		"QUEUECTL_DB_PATH": dbPath,
	}

	getenv := func(key string) string {
		return env[key]
	}

	var stdout, stderr bytes.Buffer
	cmd := newRootCommand(&stdout, &stderr, getenv)
	cmd.SetArgs([]string{"worker", "stop"})

	err := cmd.Execute()
	require.NoError(t, err)

	require.Equal(t, "Stopped 0 worker(s)\n", stdout.String())
	require.Empty(t, stderr.String())

	// Start a worker to see 1
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, err := store.Open(dbPath)
	require.NoError(t, err)
	err = s.RegisterWorker(ctx, "worker-1", 1234)
	require.NoError(t, err)
	s.Close()

	stdout.Reset()
	cmd.SetArgs([]string{"worker", "stop"})
	err = cmd.Execute()
	require.NoError(t, err)

	require.Equal(t, "Stopped 1 worker(s)\n", stdout.String())
}
