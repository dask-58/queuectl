package cli

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/dask-58/queuectl/internal/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListRequiresStateFlag(t *testing.T) {
	getenv, _ := testGetenv(t)
	err, _, _ := executeWithEnv(getenv, "list")
	require.Error(t, err)
	assert.ErrorContains(t, err, "required flag(s) \"state\" not set")
}

func TestListRejectsInvalidState(t *testing.T) {
	getenv, _ := testGetenv(t)

	tests := []string{
		"unknown",
		"",
		" pending ",
		"PENDING",
	}

	for _, state := range tests {
		t.Run(state, func(t *testing.T) {
			err, stdout, _ := executeWithEnv(getenv, "list", "--state", state)
			require.Error(t, err)
			assert.ErrorContains(t, err, "invalid job state")
			assert.Empty(t, stdout)
		})
	}
}

func TestListJSONFormat(t *testing.T) {
	getenv, dbPath := testGetenv(t)

	s, err := store.Open(dbPath)
	require.NoError(t, err)
	_, err = s.Enqueue(context.Background(), "job1", "sleep 2")
	require.NoError(t, err)
	require.NoError(t, s.Close())

	err, stdout, stderr := executeWithEnv(getenv, "list", "--state", "pending", "--json")
	require.NoError(t, err)
	assert.Empty(t, stderr)

	var jobs []jobOutput
	require.NoError(t, json.Unmarshal([]byte(stdout), &jobs))

	require.Len(t, jobs, 1)
	job := jobs[0]
	assert.Equal(t, "job1", job.ID)
	assert.Equal(t, "sleep 2", job.Command)
	assert.Equal(t, "pending", job.State)
	assert.Equal(t, 0, job.Attempts)
	assert.Equal(t, 3, job.MaxRetries)
	assert.Equal(t, 2, job.BackoffBase)

	// Verify RFC3339 constraints
	_, err = time.Parse(time.RFC3339Nano, job.CreatedAt)
	assert.NoError(t, err, "created_at must be RFC3339")
	_, err = time.Parse(time.RFC3339Nano, job.UpdatedAt)
	assert.NoError(t, err, "updated_at must be RFC3339")

	// Ensure optional fields are missing
	assert.Nil(t, job.NextRunAt)
	assert.Nil(t, job.WorkerID)
	assert.Nil(t, job.LeaseExpiresAt)
	assert.Nil(t, job.StartedAt)
	assert.Nil(t, job.CompletedAt)
	assert.Nil(t, job.ExitCode)
	assert.Nil(t, job.LastError)
}

func TestListJSONEmptyResultIsArray(t *testing.T) {
	getenv, _ := testGetenv(t)

	err, stdout, stderr := executeWithEnv(getenv, "list", "--state", "pending", "--json")
	require.NoError(t, err)
	assert.Empty(t, stderr)

	// Must be precisely "[]" and not "null"
	assert.Equal(t, "[]", strings.TrimSpace(stdout))
}

func TestListJSONMultipleJobsDeterministicOrder(t *testing.T) {
	getenv, dbPath := testGetenv(t)

	s, err := store.Open(dbPath)
	require.NoError(t, err)
	ctx := context.Background()

	_, err = s.Enqueue(ctx, "jobA", "sleep 1")
	require.NoError(t, err)
	_, err = s.Enqueue(ctx, "jobB", "sleep 2")
	require.NoError(t, err)
	require.NoError(t, s.Close())

	err, stdout, _ := executeWithEnv(getenv, "list", "--state", "pending", "--json")
	require.NoError(t, err)

	var jobs []jobOutput
	require.NoError(t, json.Unmarshal([]byte(stdout), &jobs))

	require.Len(t, jobs, 2)
	assert.Equal(t, "jobA", jobs[0].ID)
	assert.Equal(t, "jobB", jobs[1].ID)
}

func TestListJSONOptionalLifecycleFields(t *testing.T) {
	getenv, dbPath := testGetenv(t)

	s, err := store.Open(dbPath)
	require.NoError(t, err)
	require.NoError(t, s.Close())

	// Open raw SQLite connection to insert a record with lifecycle fields
	// since we don't have worker APIs yet and s.db is unexported.
	db, err := sql.Open("sqlite", dbPath)
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`INSERT INTO jobs (
			id, command, state, attempts, max_retries, backoff_base, created_at, updated_at,
			next_run_at, worker_id, lease_expires_at, started_at, completed_at, exit_code, last_error
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"jobC", "cmd", store.JobStateProcessing, 2, 3, 2, 100, 200,
		nil, "worker1", 2000, 3000, nil, nil, nil,
	)
	require.NoError(t, err)

	err, stdout, _ := executeWithEnv(getenv, "list", "--state", "processing", "--json")
	require.NoError(t, err)

	var jobs []jobOutput
	require.NoError(t, json.Unmarshal([]byte(stdout), &jobs))
	require.Len(t, jobs, 1)

	job := jobs[0]
	assert.Nil(t, job.NextRunAt)

	assert.NotNil(t, job.WorkerID)
	assert.Equal(t, "worker1", *job.WorkerID)

	assert.NotNil(t, job.LeaseExpiresAt)
	_, err = time.Parse(time.RFC3339Nano, *job.LeaseExpiresAt)
	assert.NoError(t, err)

	assert.NotNil(t, job.StartedAt)
	_, err = time.Parse(time.RFC3339Nano, *job.StartedAt)
	assert.NoError(t, err)

	assert.Nil(t, job.CompletedAt)

	assert.Nil(t, job.ExitCode)

	assert.Nil(t, job.LastError)
}

func TestListHumanOutput(t *testing.T) {
	getenv, dbPath := testGetenv(t)

	s, err := store.Open(dbPath)
	require.NoError(t, err)
	_, err = s.Enqueue(context.Background(), "job1", "sleep 2")
	require.NoError(t, err)
	require.NoError(t, s.Close())

	err, stdout, stderr := executeWithEnv(getenv, "list", "--state", "pending")
	require.NoError(t, err)
	assert.Empty(t, stderr)

	assert.NotContains(t, stdout, "{") // Not JSON
	assert.Contains(t, stdout, "ID")
	assert.Contains(t, stdout, "STATE")
	assert.Contains(t, stdout, "ATTEMPTS")
	assert.Contains(t, stdout, "COMMAND")
	assert.Contains(t, stdout, "job1")
	assert.Contains(t, stdout, "sleep 2")
}

func TestListHumanOutputEmpty(t *testing.T) {
	getenv, _ := testGetenv(t)

	err, stdout, stderr := executeWithEnv(getenv, "list", "--state", "pending")
	require.NoError(t, err)
	assert.Empty(t, stderr)

	assert.Contains(t, stdout, "ID")
	assert.Contains(t, stdout, "STATE")

	// Should not print fake prose
	assert.NotContains(t, stdout, "No jobs found")
}

func TestListFailsWhenStoreOpenFails(t *testing.T) {
	// Provide invalid db path
	getenv := func(key string) string {
		if key == "QUEUECTL_DB_PATH" {
			return "/invalid/path/that/fails.db"
		}
		return ""
	}

	err, stdout, _ := executeWithEnv(getenv, "list", "--state", "pending", "--json")
	require.Error(t, err)
	assert.Empty(t, stdout)
	assert.ErrorContains(t, err, "open store")
}

type errorWriter struct{}

func (w *errorWriter) Write(p []byte) (n int, err error) {
	return 0, assert.AnError
}

func TestListHumanOutputWriteError(t *testing.T) {
	getenv, dbPath := testGetenv(t)

	s, err := store.Open(dbPath)
	require.NoError(t, err)
	_, err = s.Enqueue(context.Background(), "job1", "sleep 2")
	require.NoError(t, err)
	require.NoError(t, s.Close())

	cmd := newListCommand(getenv)
	cmd.SetArgs([]string{"--state", "pending"})
	cmd.SetOut(&errorWriter{})

	err = cmd.Execute()
	require.Error(t, err)
	assert.ErrorContains(t, err, assert.AnError.Error())
}
