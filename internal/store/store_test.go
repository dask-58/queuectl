package store

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpenCreatesNewSQLiteDatabase(t *testing.T) {
	path := testDBPath(t)

	store, err := Open(path)
	require.NoError(t, err)
	
	err = store.Close()
	require.NoError(t, err)

	_, err = os.Stat(path)
	assert.NoError(t, err, "expected database file to exist")
}

func TestOpenRejectsEmptyPath(t *testing.T) {
	store, err := Open("")
	assert.Error(t, err)
	assert.Nil(t, store)
	assert.ErrorContains(t, err, "database path")
}

func TestOpenRejectsWhitespaceOnlyPath(t *testing.T) {
	store, err := Open("   \t\n")
	assert.Error(t, err)
	assert.Nil(t, store)
	assert.ErrorContains(t, err, "database path")
}

func TestRequiredTablesExistAfterInitialization(t *testing.T) {
	store := openTestStore(t)

	for _, table := range []string{"schema_migrations", "jobs", "config", "workers"} {
		assert.True(t, tableExists(t, store, table), "expected table %q to exist", table)
	}
}

func TestIdxJobsStateExists(t *testing.T) {
	store := openTestStore(t)

	var name string
	err := store.db.QueryRow(
		"SELECT name FROM sqlite_master WHERE type = 'index' AND name = ?",
		"idx_jobs_state",
	).Scan(&name)
	require.NoError(t, err)
	assert.Equal(t, "idx_jobs_state", name)
}

func TestMigrationVersionOneRecordedExactlyOnce(t *testing.T) {
	store := openTestStore(t)

	count := countRows(t, store, "SELECT COUNT(*) FROM schema_migrations WHERE version = 1")
	assert.Equal(t, 1, count)
}

func TestCloseAndReopenDoesNotReapplyMigration(t *testing.T) {
	path := testDBPath(t)

	store, err := Open(path)
	require.NoError(t, err)
	require.NoError(t, store.Close())

	reopened, err := Open(path)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, reopened.Close())
	})

	count := countRows(t, reopened, "SELECT COUNT(*) FROM schema_migrations WHERE version = 1")
	assert.Equal(t, 1, count)
}

func TestReopenDoesNotDuplicateDefaultConfig(t *testing.T) {
	path := testDBPath(t)
	store, err := Open(path)
	require.NoError(t, err)
	require.NoError(t, store.Close())

	reopened, err := Open(path)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, reopened.Close())
	})

	count := countRows(t, reopened, "SELECT COUNT(*) FROM config")
	assert.Equal(t, 2, count)
}

func TestDirectlyWrittenDataSurvivesCloseAndReopen(t *testing.T) {
	path := testDBPath(t)
	store, err := Open(path)
	require.NoError(t, err)

	insertJob(t, store, "job-1", "pending", 0, 3, 2)
	require.NoError(t, store.Close())

	reopened, err := Open(path)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, reopened.Close())
	})

	var command string
	err = reopened.db.QueryRow("SELECT command FROM jobs WHERE id = ?", "job-1").Scan(&command)
	require.NoError(t, err)
	assert.Equal(t, "echo hello", command)
}

func TestDefaultConfigRowsExist(t *testing.T) {
	store := openTestStore(t)

	defaults := []struct {
		key   string
		value string
	}{
		{"max-retries", "3"},
		{"backoff-base", "2"},
	}

	for _, d := range defaults {
		var got string
		err := store.db.QueryRow("SELECT value FROM config WHERE key = ?", d.key).Scan(&got)
		require.NoError(t, err, "read config %q", d.key)
		assert.Equal(t, d.value, got, "config %q value mismatch", d.key)
	}
}

func TestJobConstraintsRejectInvalidValues(t *testing.T) {
	tests := []struct {
		name        string
		state       string
		attempts    int
		maxRetries  int
		backoffBase int
	}{
		{
			name:        "unknown state",
			state:       "queued",
			attempts:    0,
			maxRetries:  3,
			backoffBase: 2,
		},
		{
			name:        "negative attempts",
			state:       "pending",
			attempts:    -1,
			maxRetries:  3,
			backoffBase: 2,
		},
		{
			name:        "negative max retries",
			state:       "pending",
			attempts:    0,
			maxRetries:  -1,
			backoffBase: 2,
		},
		{
			name:        "backoff base less than one",
			state:       "pending",
			attempts:    0,
			maxRetries:  3,
			backoffBase: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := openTestStore(t)

			_, err := store.db.Exec(
				`INSERT INTO jobs (
					id, command, state, attempts, max_retries, backoff_base, created_at, updated_at
				) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
				tt.name,
				"echo hello",
				tt.state,
				tt.attempts,
				tt.maxRetries,
				tt.backoffBase,
				1,
				1,
			)
			assert.Error(t, err, "expected constraint error for %s", tt.name)
		})
	}
}

func TestValidNullableLifecycleFieldsAreAccepted(t *testing.T) {
	store := openTestStore(t)

	_, err := store.db.Exec(
		`INSERT INTO jobs (
			id, command, state, attempts, max_retries, backoff_base, created_at, updated_at,
			next_run_at, worker_id, lease_expires_at, started_at, completed_at, exit_code, last_error
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"job-with-nulls",
		"echo hello",
		"pending",
		0,
		3,
		2,
		1,
		1,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
	)
	assert.NoError(t, err)
}

func TestSQLitePragmas(t *testing.T) {
	store := openTestStore(t)

	var journalMode string
	err := store.db.QueryRow("PRAGMA journal_mode").Scan(&journalMode)
	require.NoError(t, err)
	assert.Equal(t, "wal", journalMode)

	var foreignKeys int
	err = store.db.QueryRow("PRAGMA foreign_keys").Scan(&foreignKeys)
	require.NoError(t, err)
	assert.Equal(t, 1, foreignKeys)

	var busyTimeout int
	err = store.db.QueryRow("PRAGMA busy_timeout").Scan(&busyTimeout)
	require.NoError(t, err)
	assert.Equal(t, 5000, busyTimeout)
}

func TestOpenRejectsNewerSchemaVersion(t *testing.T) {
	path := testDBPath(t)
	store, err := Open(path)
	require.NoError(t, err)
	
	_, err = store.db.Exec("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)", latestMigrationVersion+1, 1)
	require.NoError(t, err)
	require.NoError(t, store.Close())

	reopened, err := Open(path)
	assert.Error(t, err)
	assert.Nil(t, reopened)
	assert.ErrorContains(t, err, "newer than supported")
}

func TestValidateMigrationHistory(t *testing.T) {
	known := []migration{
		{version: 1},
		{version: 2},
		{version: 3},
	}
	latestSupported := 3

	tests := []struct {
		name      string
		applied   []int
		wantVer   int
		wantError string
	}{
		{
			name:    "valid: no versions applied",
			applied: nil,
			wantVer: 0,
		},
		{
			name:    "valid: version 1 only",
			applied: []int{1},
			wantVer: 1,
		},
		{
			name:    "valid: versions 1 and 2",
			applied: []int{1, 2},
			wantVer: 2,
		},
		{
			name:    "valid: all versions",
			applied: []int{1, 2, 3},
			wantVer: 3,
		},
		{
			name:      "invalid: version 0",
			applied:   []int{0},
			wantError: "invalid",
		},
		{
			name:      "invalid: negative version",
			applied:   []int{-1},
			wantError: "invalid",
		},
		{
			name:      "invalid: starts at 2",
			applied:   []int{2},
			wantError: "gap",
		},
		{
			name:      "invalid: gap 1 to 3",
			applied:   []int{1, 3},
			wantError: "gap",
		},
		{
			name:      "invalid: unknown newer version",
			applied:   []int{1, 2, 3, 4},
			wantError: "newer than supported",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := validateMigrationHistory(tt.applied, known, latestSupported)
			if tt.wantError != "" {
				assert.Error(t, err)
				assert.ErrorContains(t, err, tt.wantError)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.wantVer, got)
		})
	}
}

func TestConcurrentInitialization(t *testing.T) {
	path := testDBPath(t)
	const openers = 8

	start := make(chan struct{})
	var wg sync.WaitGroup
	stores := make([]*Store, openers)
	errs := make([]error, openers)

	wg.Add(openers)
	for i := range openers {
		go func(i int) {
			defer wg.Done()
			<-start

			stores[i], errs[i] = Open(path)
		}(i)
	}

	close(start)
	wg.Wait()

	for i, err := range errs {
		assert.NoError(t, err, "open store %d failed", i)
	}
	for i, store := range stores {
		require.NotNil(t, store, "store %d is nil", i)
		assert.NoError(t, store.Close(), "close store %d failed", i)
	}

	reopened, err := Open(path)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, reopened.Close())
	})

	count := countRows(t, reopened, "SELECT COUNT(*) FROM schema_migrations WHERE version = 1")
	assert.Equal(t, 1, count)

	for _, table := range []string{"schema_migrations", "jobs", "config", "workers"} {
		assert.True(t, tableExists(t, reopened, table), "expected table %q to exist", table)
	}
	count = countRows(t, reopened, "SELECT COUNT(*) FROM config")
	assert.Equal(t, 2, count)
}

func TestTwoStoreInstancesOpenSameDatabaseAndObserveData(t *testing.T) {
	path := testDBPath(t)

	first, err := Open(path)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, first.Close())
	})

	second, err := Open(path)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, second.Close())
	})

	_, err = first.db.Exec(
		"INSERT INTO workers (id, pid, started_at, heartbeat_at) VALUES (?, ?, ?, ?)",
		"worker-1",
		1234,
		1,
		1,
	)
	require.NoError(t, err)

	var pid int
	err = second.db.QueryRow("SELECT pid FROM workers WHERE id = ?", "worker-1").Scan(&pid)
	require.NoError(t, err)
	assert.Equal(t, 1234, pid)
}

// --- Enqueue tests ---

func TestEnqueueCreatesPendingJob(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	job, err := s.Enqueue(ctx, "job-1", "echo hello")
	require.NoError(t, err)

	assert.Equal(t, "job-1", job.ID)
	assert.Equal(t, "echo hello", job.Command)
	assert.Equal(t, "pending", job.State)
	assert.Equal(t, 0, job.Attempts)
	assert.Equal(t, 3, job.MaxRetries)
	assert.Equal(t, 2, job.BackoffBase)
	assert.NotZero(t, job.CreatedAt)
	assert.Equal(t, job.CreatedAt, job.UpdatedAt)

	persisted, err := s.Job(ctx, "job-1")
	require.NoError(t, err)
	assert.Nil(t, persisted.NextRunAt)
	assert.Nil(t, persisted.WorkerID)
	assert.Nil(t, persisted.LeaseExpiresAt)
	assert.Nil(t, persisted.StartedAt)
	assert.Nil(t, persisted.CompletedAt)
	assert.Nil(t, persisted.ExitCode)
	assert.Nil(t, persisted.LastError)
}

func TestEnqueueSurvivesCloseAndReopen(t *testing.T) {
	path := testDBPath(t)
	ctx := context.Background()

	s, err := Open(path)
	require.NoError(t, err)

	_, err = s.Enqueue(ctx, "job-1", "echo hello")
	require.NoError(t, err)
	require.NoError(t, s.Close())

	reopened, err := Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })

	job, err := reopened.Job(ctx, "job-1")
	require.NoError(t, err)
	assert.Equal(t, "echo hello", job.Command)
	assert.Equal(t, "pending", job.State)
}

func TestEnqueueDuplicateIDReturnsError(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	_, err := s.Enqueue(ctx, "job-1", "echo first")
	require.NoError(t, err)

	_, err = s.Enqueue(ctx, "job-1", "echo second")
	assert.ErrorIs(t, err, ErrJobAlreadyExists)
}

func TestEnqueueDuplicatePreservesOriginal(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	_, err := s.Enqueue(ctx, "job-1", "echo first")
	require.NoError(t, err)

	_, _ = s.Enqueue(ctx, "job-1", "echo second")

	job, err := s.Job(ctx, "job-1")
	require.NoError(t, err)
	assert.Equal(t, "echo first", job.Command)
}

func TestEnqueueSnapshotsPersistedConfig(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	_, err := s.db.Exec("UPDATE config SET value = '5' WHERE key = 'max-retries'")
	require.NoError(t, err)
	_, err = s.db.Exec("UPDATE config SET value = '3' WHERE key = 'backoff-base'")
	require.NoError(t, err)

	job, err := s.Enqueue(ctx, "job-1", "echo hello")
	require.NoError(t, err)
	assert.Equal(t, 5, job.MaxRetries)
	assert.Equal(t, 3, job.BackoffBase)
}

func TestEnqueueExistingJobConfigStable(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	jobA, err := s.Enqueue(ctx, "job-a", "echo a")
	require.NoError(t, err)

	_, err = s.db.Exec("UPDATE config SET value = '10' WHERE key = 'max-retries'")
	require.NoError(t, err)
	_, err = s.db.Exec("UPDATE config SET value = '5' WHERE key = 'backoff-base'")
	require.NoError(t, err)

	jobB, err := s.Enqueue(ctx, "job-b", "echo b")
	require.NoError(t, err)

	assert.Equal(t, 3, jobA.MaxRetries)
	assert.Equal(t, 2, jobA.BackoffBase)

	assert.Equal(t, 10, jobB.MaxRetries)
	assert.Equal(t, 5, jobB.BackoffBase)

	persistedA, err := s.Job(ctx, "job-a")
	require.NoError(t, err)
	assert.Equal(t, 3, persistedA.MaxRetries)
	assert.Equal(t, 2, persistedA.BackoffBase)
}

func TestEnqueueMissingConfigKey(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	_, err := s.db.Exec("DELETE FROM config WHERE key = ?", "max-retries")
	require.NoError(t, err)

	_, err = s.Enqueue(ctx, "job-1", "echo hello")
	assert.Error(t, err)
	assert.ErrorContains(t, err, "max-retries")
}

func TestEnqueueInvalidConfigValue(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	_, err := s.db.Exec("UPDATE config SET value = 'not-a-number' WHERE key = 'max-retries'")
	require.NoError(t, err)

	_, err = s.Enqueue(ctx, "job-1", "echo hello")
	assert.Error(t, err)
	assert.ErrorContains(t, err, "max-retries")
}

func TestEnqueueInvalidConfigRange(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		value string
	}{
		{"max-retries negative", "max-retries", "-1"},
		{"backoff-base zero", "backoff-base", "0"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := openTestStore(t)
			ctx := context.Background()

			_, err := s.db.Exec("UPDATE config SET value = ? WHERE key = ?", tt.value, tt.key)
			require.NoError(t, err)

			_, err = s.Enqueue(ctx, "job-1", "echo hello")
			assert.Error(t, err)
			assert.ErrorContains(t, err, tt.key)
		})
	}
}

func TestEnqueueRejectsEmptyID(t *testing.T) {
	s := openTestStore(t)
	_, err := s.Enqueue(context.Background(), "", "echo hello")
	assert.Error(t, err)
	assert.ErrorContains(t, err, "id")
}

func TestEnqueueRejectsWhitespaceID(t *testing.T) {
	s := openTestStore(t)
	_, err := s.Enqueue(context.Background(), "   ", "echo hello")
	assert.Error(t, err)
	assert.ErrorContains(t, err, "id")
}

func TestEnqueueRejectsEmptyCommand(t *testing.T) {
	s := openTestStore(t)
	_, err := s.Enqueue(context.Background(), "job-1", "")
	assert.Error(t, err)
	assert.ErrorContains(t, err, "command")
}

func TestEnqueueRejectsWhitespaceCommand(t *testing.T) {
	s := openTestStore(t)
	_, err := s.Enqueue(context.Background(), "job-1", "   ")
	assert.Error(t, err)
	assert.ErrorContains(t, err, "command")
}

func TestEnqueueTrimsID(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	job, err := s.Enqueue(ctx, "  job-1  ", "echo hello")
	require.NoError(t, err)
	assert.Equal(t, "job-1", job.ID)

	persisted, err := s.Job(ctx, "job-1")
	require.NoError(t, err)
	assert.Equal(t, "job-1", persisted.ID)
}

func TestEnqueuePreservesCommandWhitespace(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	job, err := s.Enqueue(ctx, "job-1", "  echo hello  ")
	require.NoError(t, err)
	assert.Equal(t, "  echo hello  ", job.Command)
}

// --- Test helpers ---

func testDBPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "queuectl.db")
}

func openTestStore(t *testing.T) *Store {
	t.Helper()

	store, err := Open(testDBPath(t))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})

	return store
}

func tableExists(t *testing.T, store *Store, table string) bool {
	t.Helper()

	var name string
	err := store.db.QueryRow(
		"SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
		table,
	).Scan(&name)
	if errors.Is(err, sql.ErrNoRows) {
		return false
	}
	require.NoError(t, err)

	return true
}

func countRows(t *testing.T, store *Store, query string) int {
	t.Helper()

	var count int
	err := store.db.QueryRow(query).Scan(&count)
	require.NoError(t, err)
	return count
}

func insertJob(t *testing.T, store *Store, id, state string, attempts, maxRetries, backoffBase int) {
	t.Helper()

	_, err := store.db.Exec(
		`INSERT INTO jobs (
			id, command, state, attempts, max_retries, backoff_base, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		id,
		"echo hello",
		state,
		attempts,
		maxRetries,
		backoffBase,
		1,
		1,
	)
	require.NoError(t, err)
}
