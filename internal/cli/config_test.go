package cli

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dask-58/queuectl/internal/store"
)

func TestConfigList(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "queuectl.db")
	s, err := store.Open(dbPath)
	require.NoError(t, err)
	require.NoError(t, s.Close())

	var stdout, stderr bytes.Buffer
	env := map[string]string{"QUEUECTL_DB_PATH": dbPath}
	getenv := func(key string) string { return env[key] }
	cmd := newRootCommand(&stdout, &stderr, getenv)
	cmd.SetArgs([]string{"config", "list"})

	err = cmd.Execute()
	require.NoError(t, err)

	out := stdout.String()
	assert.Contains(t, out, "KEY           VALUE\n")
	assert.Contains(t, out, "max-retries   3")
	assert.Contains(t, out, "backoff-base  2")
}

func TestConfigSetSuccess(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "queuectl.db")
	s, err := store.Open(dbPath)
	require.NoError(t, err)
	require.NoError(t, s.Close())

	var stdout, stderr bytes.Buffer
	env := map[string]string{"QUEUECTL_DB_PATH": dbPath}
	getenv := func(key string) string { return env[key] }
	cmd := newRootCommand(&stdout, &stderr, getenv)
	cmd.SetArgs([]string{"config", "set", "max-retries", "10"})

	err = cmd.Execute()
	require.NoError(t, err)
	assert.Empty(t, stdout.String())
}

func TestConfigSetUnknownKey(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "queuectl.db")
	s, err := store.Open(dbPath)
	require.NoError(t, err)
	require.NoError(t, s.Close())

	var stdout, stderr bytes.Buffer
	env := map[string]string{"QUEUECTL_DB_PATH": dbPath}
	getenv := func(key string) string { return env[key] }
	cmd := newRootCommand(&stdout, &stderr, getenv)
	cmd.SetArgs([]string{"config", "set", "unknown_key", "10"})

	err = cmd.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), `unknown config key "unknown_key"`)
	assert.Empty(t, stdout.String())
}

func TestConfigStoreOpenFailure(t *testing.T) {
	var stdout, stderr bytes.Buffer
	env := map[string]string{"QUEUECTL_DB_PATH": "/invalid/dir/queuectl.db"}
	getenv := func(key string) string { return env[key] }

	cmd := newRootCommand(&stdout, &stderr, getenv)
	cmd.SetArgs([]string{"config", "list"})
	err := cmd.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "open store:")

	cmd = newRootCommand(&stdout, &stderr, getenv)
	cmd.SetArgs([]string{"config", "set", "max-retries", "10"})
	err = cmd.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "open store:")
}

func TestConfigWriterFailure(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "queuectl.db")
	s, err := store.Open(dbPath)
	require.NoError(t, err)
	require.NoError(t, s.Close())

	var stderr bytes.Buffer
	env := map[string]string{"QUEUECTL_DB_PATH": dbPath}
	getenv := func(key string) string { return env[key] }
	cmd := newRootCommand(&errorWriter{}, &stderr, getenv)
	cmd.SetArgs([]string{"config", "list"})

	err = cmd.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "flush config output")
}
