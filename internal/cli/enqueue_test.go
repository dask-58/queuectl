package cli

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"

	"github.com/dask-58/queuectl/internal/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func executeWithEnv(getenv func(string) string, args ...string) (error, string, string) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	cmd := newRootCommand(&stdout, &stderr, getenv)
	cmd.SetArgs(args)

	err := cmd.Execute()
	return err, stdout.String(), stderr.String()
}

func testGetenv(t *testing.T) (func(string) string, string) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test.db")
	return func(key string) string {
		if key == "QUEUECTL_DB_PATH" {
			return dbPath
		}
		return ""
	}, dbPath
}

func TestEnqueueSuccess(t *testing.T) {
	getenv, _ := testGetenv(t)

	err, stdout, stderr := executeWithEnv(getenv, "enqueue", `{"id":"job1","command":"sleep 2"}`)
	require.NoError(t, err)
	assert.Equal(t, "job1\n", stdout)
	assert.Empty(t, stderr)
}

func TestEnqueueRejectsInvalidInput(t *testing.T) {
	getenv, _ := testGetenv(t)

	tests := []struct {
		name  string
		input string
	}{
		{"malformed JSON", `{"id":`},
		{"missing id", `{"command":"echo hi"}`},
		{"empty id", `{"id":"","command":"echo hi"}`},
		{"whitespace-only id", `{"id":"   ","command":"echo hi"}`},
		{"missing command", `{"id":"job1"}`},
		{"empty command", `{"id":"job1","command":""}`},
		{"whitespace-only command", `{"id":"job1","command":"   "}`},
		{"unknown field", `{"id":"job1","command":"echo hi","extra":"bad"}`},
		{"trailing JSON", `{"id":"a","command":"echo hi"} {"id":"b","command":"echo bye"}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err, stdout, _ := executeWithEnv(getenv, "enqueue", tt.input)
			assert.Error(t, err)
			assert.Empty(t, stdout)
		})
	}
}

func TestEnqueueDuplicateID(t *testing.T) {
	getenv, _ := testGetenv(t)

	err, _, _ := executeWithEnv(getenv, "enqueue", `{"id":"job1","command":"echo first"}`)
	require.NoError(t, err)

	err, stdout, _ := executeWithEnv(getenv, "enqueue", `{"id":"job1","command":"echo second"}`)
	assert.Error(t, err)
	assert.Empty(t, stdout)
	assert.ErrorContains(t, err, "job1")
}

func TestEnqueueQueuectlDBPath(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "custom.db")
	getenv := func(key string) string {
		if key == "QUEUECTL_DB_PATH" {
			return dbPath
		}
		return ""
	}

	err, stdout, _ := executeWithEnv(getenv, "enqueue", `{"id":"job1","command":"echo test"}`)
	require.NoError(t, err)
	assert.Equal(t, "job1\n", stdout)

	s, err := store.Open(dbPath)
	require.NoError(t, err)
	defer s.Close()

	job, err := s.Job(context.Background(), "job1")
	require.NoError(t, err)
	assert.Equal(t, "echo test", job.Command)
}

func TestDatabasePathDefault(t *testing.T) {
	path := databasePath(func(string) string { return "" })
	assert.Equal(t, "./queuectl.db", path)

	path = databasePath(func(key string) string {
		if key == "QUEUECTL_DB_PATH" {
			return "   "
		}
		return ""
	})
	assert.Equal(t, "./queuectl.db", path)
}

func TestDatabasePathFromEnv(t *testing.T) {
	path := databasePath(func(key string) string {
		if key == "QUEUECTL_DB_PATH" {
			return "/tmp/custom.db"
		}
		return ""
	})
	assert.Equal(t, "/tmp/custom.db", path)
}

func TestEnqueueIntegration(t *testing.T) {
	getenv, dbPath := testGetenv(t)

	err, stdout, _ := executeWithEnv(getenv, "enqueue", `{"id":"job1","command":"sleep 2"}`)
	require.NoError(t, err)
	assert.Equal(t, "job1\n", stdout)

	s, err := store.Open(dbPath)
	require.NoError(t, err)
	defer s.Close()

	job, err := s.Job(context.Background(), "job1")
	require.NoError(t, err)
	assert.Equal(t, "pending", job.State)
	assert.Equal(t, "sleep 2", job.Command)
	assert.Equal(t, 0, job.Attempts)
	assert.Equal(t, 3, job.MaxRetries)
	assert.Equal(t, 2, job.BackoffBase)
}
