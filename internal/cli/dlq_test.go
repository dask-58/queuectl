package cli

import (
	"bytes"
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"

	"github.com/dask-58/queuectl/internal/store"
)

func TestDLQListEmpty(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "queuectl.db")
	s, err := store.Open(dbPath)
	require.NoError(t, err)
	require.NoError(t, s.Close())

	var stdout, stderr bytes.Buffer
	env := map[string]string{"QUEUECTL_DB_PATH": dbPath}
	getenv := func(key string) string { return env[key] }
	cmd := newRootCommand(&stdout, &stderr, getenv)
	cmd.SetArgs([]string{"dlq", "list"})

	err = cmd.Execute()
	require.NoError(t, err)
	assert.Contains(t, stdout.String(), "ID  STATE  ATTEMPTS  COMMAND\n")
}

func TestDLQListMixed(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "queuectl.db")
	s, err := store.Open(dbPath)
	require.NoError(t, err)

	_, err = s.Enqueue(context.Background(), "job-1", "echo ok")
	require.NoError(t, err)

	db, err := sql.Open("sqlite", dbPath)
	require.NoError(t, err)
	_, err = db.Exec(`
		INSERT INTO jobs (id, command, state, attempts, max_retries, backoff_base, created_at, updated_at) 
		VALUES ('job-dead', 'echo dead', ?, 3, 3, 2, 100, 100)
	`, store.JobStateDead)
	require.NoError(t, err)
	db.Close()
	require.NoError(t, s.Close())

	var stdout, stderr bytes.Buffer
	env := map[string]string{"QUEUECTL_DB_PATH": dbPath}
	getenv := func(key string) string { return env[key] }
	cmd := newRootCommand(&stdout, &stderr, getenv)
	cmd.SetArgs([]string{"dlq", "list"})

	err = cmd.Execute()
	require.NoError(t, err)

	out := stdout.String()
	assert.Contains(t, out, "job-dead")
	assert.NotContains(t, out, "job-1")

	// JSON output
	stdout.Reset()
	cmd.SetArgs([]string{"dlq", "list", "--json"})
	err = cmd.Execute()
	require.NoError(t, err)

	outJSON := stdout.String()
	assert.Contains(t, outJSON, `"id":"job-dead"`)
	assert.NotContains(t, outJSON, `"id":"job-1"`)
}

func TestDLQRetrySuccess(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "queuectl.db")
	s, err := store.Open(dbPath)
	require.NoError(t, err)

	db, err := sql.Open("sqlite", dbPath)
	require.NoError(t, err)
	_, err = db.Exec(`
		INSERT INTO jobs (id, command, state, attempts, max_retries, backoff_base, created_at, updated_at) 
		VALUES ('job-dead', 'echo dead', ?, 3, 3, 2, 100, 100)
	`, store.JobStateDead)
	require.NoError(t, err)
	db.Close()
	require.NoError(t, s.Close())

	var stdout, stderr bytes.Buffer
	env := map[string]string{"QUEUECTL_DB_PATH": dbPath}
	getenv := func(key string) string { return env[key] }
	cmd := newRootCommand(&stdout, &stderr, getenv)
	cmd.SetArgs([]string{"dlq", "retry", "job-dead"})

	err = cmd.Execute()
	require.NoError(t, err)
	assert.Equal(t, "job-dead\n", stdout.String())
}

func TestDLQRetryMissingAndWrongState(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "queuectl.db")
	s, err := store.Open(dbPath)
	require.NoError(t, err)

	_, err = s.Enqueue(context.Background(), "job-pending", "echo pending")
	require.NoError(t, err)
	require.NoError(t, s.Close())

	env := map[string]string{"QUEUECTL_DB_PATH": dbPath}
	getenv := func(key string) string { return env[key] }

	t.Run("missing", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		cmd := newRootCommand(&stdout, &stderr, getenv)
		cmd.SetArgs([]string{"dlq", "retry", "missing"})
		err := cmd.Execute()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "job not found")
		assert.Empty(t, stdout.String())
	})

	t.Run("wrong state", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		cmd := newRootCommand(&stdout, &stderr, getenv)
		cmd.SetArgs([]string{"dlq", "retry", "job-pending"})
		err := cmd.Execute()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "expected \"dead\"")
		assert.Empty(t, stdout.String())
	})
}

func TestDLQStoreOpenFailure(t *testing.T) {
	var stdout, stderr bytes.Buffer
	env := map[string]string{"QUEUECTL_DB_PATH": "/invalid/dir/queuectl.db"}
	getenv := func(key string) string { return env[key] }

	cmd := newRootCommand(&stdout, &stderr, getenv)
	cmd.SetArgs([]string{"dlq", "list"})
	err := cmd.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "open store:")

	cmd = newRootCommand(&stdout, &stderr, getenv)
	cmd.SetArgs([]string{"dlq", "retry", "job-id"})
	err = cmd.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "open store:")
}

func TestDLQWriterFailure(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "queuectl.db")
	s, err := store.Open(dbPath)
	require.NoError(t, err)

	db, err := sql.Open("sqlite", dbPath)
	require.NoError(t, err)
	_, err = db.Exec(`
		INSERT INTO jobs (id, command, state, attempts, max_retries, backoff_base, created_at, updated_at) 
		VALUES ('job-dead', 'echo dead', ?, 3, 3, 2, 100, 100)
	`, store.JobStateDead)
	require.NoError(t, err)
	db.Close()
	require.NoError(t, s.Close())

	var stderr bytes.Buffer
	env := map[string]string{"QUEUECTL_DB_PATH": dbPath}
	getenv := func(key string) string { return env[key] }
	cmd := newRootCommand(&errorWriter{}, &stderr, getenv)
	cmd.SetArgs([]string{"dlq", "list"})

	err = cmd.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "flush list output")
}
