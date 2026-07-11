package cli

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dask-58/queuectl/internal/store"
)

func TestStatusEmptyDatabase(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "queuectl.db")
	s, err := store.Open(dbPath)
	require.NoError(t, err)
	require.NoError(t, s.Close())

	var stdout, stderr bytes.Buffer
	env := map[string]string{"QUEUECTL_DB_PATH": dbPath}
	getenv := func(key string) string { return env[key] }
	cmd := newRootCommand(&stdout, &stderr, getenv)
	cmd.SetArgs([]string{"status"})

	err = cmd.Execute()
	require.NoError(t, err)

	out := stdout.String()
	assert.Contains(t, out, "Pending:     0")
	assert.Contains(t, out, "Processing:  0")
	assert.Contains(t, out, "Completed:   0")
	assert.Contains(t, out, "Failed:      0")
	assert.Contains(t, out, "Dead:        0")
	assert.Contains(t, out, "Workers:     0")
}

func TestStatusMixedDatabase(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "queuectl.db")
	s, err := store.Open(dbPath)
	require.NoError(t, err)

	ctx := context.Background()
	_, err = s.Enqueue(ctx, "job-1", "cmd")
	require.NoError(t, err)
	_, err = s.ClaimNextJob(ctx, "worker-1")
	require.NoError(t, err)
	require.NoError(t, s.RegisterWorker(ctx, "worker-1", 123))

	require.NoError(t, s.Close())

	var stdout, stderr bytes.Buffer
	env := map[string]string{"QUEUECTL_DB_PATH": dbPath}
	getenv := func(key string) string { return env[key] }
	cmd := newRootCommand(&stdout, &stderr, getenv)
	cmd.SetArgs([]string{"status"})

	err = cmd.Execute()
	require.NoError(t, err)

	out := stdout.String()
	assert.Contains(t, out, "Pending:     0")
	assert.Contains(t, out, "Processing:  1")
	assert.Contains(t, out, "Completed:   0")
	assert.Contains(t, out, "Failed:      0")
	assert.Contains(t, out, "Dead:        0")
	assert.Contains(t, out, "Workers:     1")
}

func TestStatusStoreOpenFailure(t *testing.T) {
	var stdout, stderr bytes.Buffer
	env := map[string]string{"QUEUECTL_DB_PATH": "/invalid/dir/queuectl.db"}
	getenv := func(key string) string { return env[key] }
	cmd := newRootCommand(&stdout, &stderr, getenv)
	cmd.SetArgs([]string{"status"})

	err := cmd.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "open store:")
	assert.Empty(t, stdout.String())
}

func TestStatusWriterFailure(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "queuectl.db")
	s, err := store.Open(dbPath)
	require.NoError(t, err)
	require.NoError(t, s.Close())

	var stderr bytes.Buffer
	env := map[string]string{"QUEUECTL_DB_PATH": dbPath}
	getenv := func(key string) string { return env[key] }
	cmd := newRootCommand(&errorWriter{}, &stderr, getenv)
	cmd.SetArgs([]string{"status"})

	err = cmd.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "flush output")
}
