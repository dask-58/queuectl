package store

import (
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestOpenCreatesNewSQLiteDatabase(t *testing.T) {
	path := testDBPath(t)

	store, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected database file to exist: %v", err)
	}
}

func TestOpenRejectsEmptyPath(t *testing.T) {
	store, err := Open("")
	if err == nil {
		t.Fatal("expected error")
	}
	if store != nil {
		t.Fatal("expected nil store")
	}
	if !strings.Contains(err.Error(), "database path") {
		t.Fatalf("expected useful path error, got %v", err)
	}
}

func TestOpenRejectsWhitespaceOnlyPath(t *testing.T) {
	store, err := Open("   \t\n")
	if err == nil {
		t.Fatal("expected error")
	}
	if store != nil {
		t.Fatal("expected nil store")
	}
	if !strings.Contains(err.Error(), "database path") {
		t.Fatalf("expected useful path error, got %v", err)
	}
}

func TestRequiredTablesExistAfterInitialization(t *testing.T) {
	store := openTestStore(t)

	for _, table := range []string{"schema_migrations", "jobs", "config", "workers"} {
		if !tableExists(t, store, table) {
			t.Fatalf("expected table %q to exist", table)
		}
	}
}

func TestIdxJobsStateExists(t *testing.T) {
	store := openTestStore(t)

	var name string
	err := store.db.QueryRow(
		"SELECT name FROM sqlite_master WHERE type = 'index' AND name = ?",
		"idx_jobs_state",
	).Scan(&name)
	if errors.Is(err, sql.ErrNoRows) {
		t.Fatal("expected idx_jobs_state index to exist")
	}
	if err != nil {
		t.Fatalf("check index: %v", err)
	}
}

func TestMigrationVersionOneRecordedExactlyOnce(t *testing.T) {
	store := openTestStore(t)

	if count := countRows(t, store, "SELECT COUNT(*) FROM schema_migrations WHERE version = 1"); count != 1 {
		t.Fatalf("expected migration 1 recorded once, got %d", count)
	}
}

func TestCloseAndReopenDoesNotReapplyMigration(t *testing.T) {
	path := testDBPath(t)

	store, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Fatalf("close reopened store: %v", err)
		}
	})

	if count := countRows(t, reopened, "SELECT COUNT(*) FROM schema_migrations WHERE version = 1"); count != 1 {
		t.Fatalf("expected migration 1 recorded once, got %d", count)
	}
}

func TestReopenDoesNotDuplicateDefaultConfig(t *testing.T) {
	path := testDBPath(t)
	store, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Fatalf("close reopened store: %v", err)
		}
	})

	if count := countRows(t, reopened, "SELECT COUNT(*) FROM config"); count != 2 {
		t.Fatalf("expected 2 config rows, got %d", count)
	}
}

func TestDirectlyWrittenDataSurvivesCloseAndReopen(t *testing.T) {
	path := testDBPath(t)
	store, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	insertJob(t, store, "job-1", "pending", 0, 3, 2)
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Fatalf("close reopened store: %v", err)
		}
	})

	var command string
	if err := reopened.db.QueryRow("SELECT command FROM jobs WHERE id = ?", "job-1").Scan(&command); err != nil {
		t.Fatalf("read persisted job: %v", err)
	}
	if command != "echo hello" {
		t.Fatalf("expected persisted command, got %q", command)
	}
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
		if err := store.db.QueryRow("SELECT value FROM config WHERE key = ?", d.key).Scan(&got); err != nil {
			t.Fatalf("read config %q: %v", d.key, err)
		}
		if got != d.value {
			t.Fatalf("expected config %q = %q, got %q", d.key, d.value, got)
		}
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
			if err == nil {
				t.Fatal("expected constraint error")
			}
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
	if err != nil {
		t.Fatalf("insert job with nullable lifecycle fields: %v", err)
	}
}

func TestSQLitePragmas(t *testing.T) {
	store := openTestStore(t)

	var journalMode string
	if err := store.db.QueryRow("PRAGMA journal_mode").Scan(&journalMode); err != nil {
		t.Fatalf("read journal_mode: %v", err)
	}
	if !strings.EqualFold(journalMode, "wal") {
		t.Fatalf("expected journal_mode wal, got %q", journalMode)
	}

	var foreignKeys int
	if err := store.db.QueryRow("PRAGMA foreign_keys").Scan(&foreignKeys); err != nil {
		t.Fatalf("read foreign_keys: %v", err)
	}
	if foreignKeys != 1 {
		t.Fatalf("expected foreign_keys 1, got %d", foreignKeys)
	}

	var busyTimeout int
	if err := store.db.QueryRow("PRAGMA busy_timeout").Scan(&busyTimeout); err != nil {
		t.Fatalf("read busy_timeout: %v", err)
	}
	if busyTimeout != 5000 {
		t.Fatalf("expected busy_timeout 5000, got %d", busyTimeout)
	}
}

func TestOpenRejectsNewerSchemaVersion(t *testing.T) {
	path := testDBPath(t)
	store, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if _, err := store.db.Exec("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)", latestMigrationVersion+1, 1); err != nil {
		t.Fatalf("insert newer schema version: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	reopened, err := Open(path)
	if err == nil {
		_ = reopened.Close()
		t.Fatal("expected newer schema version error")
	}
	if !strings.Contains(err.Error(), "newer than supported") {
		t.Fatalf("expected newer schema version error, got %v", err)
	}
}

func TestValidateMigrationHistory(t *testing.T) {
	// Simulate a binary that knows migrations 1, 2, 3.
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
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tt.wantError)
				}
				if !strings.Contains(err.Error(), tt.wantError) {
					t.Fatalf("expected error containing %q, got %v", tt.wantError, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.wantVer {
				t.Fatalf("expected version %d, got %d", tt.wantVer, got)
			}
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
		if err != nil {
			t.Fatalf("open store %d: %v", i, err)
		}
	}
	for i, store := range stores {
		if store == nil {
			t.Fatalf("store %d is nil", i)
		}
		if err := store.Close(); err != nil {
			t.Fatalf("close store %d: %v", i, err)
		}
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Fatalf("close reopened store: %v", err)
		}
	})

	if count := countRows(t, reopened, "SELECT COUNT(*) FROM schema_migrations WHERE version = 1"); count != 1 {
		t.Fatalf("expected migration 1 recorded once, got %d", count)
	}
	for _, table := range []string{"schema_migrations", "jobs", "config", "workers"} {
		if !tableExists(t, reopened, table) {
			t.Fatalf("expected table %q to exist", table)
		}
	}
	if count := countRows(t, reopened, "SELECT COUNT(*) FROM config"); count != 2 {
		t.Fatalf("expected 2 config rows, got %d", count)
	}
}

func TestTwoStoreInstancesOpenSameDatabaseAndObserveData(t *testing.T) {
	path := testDBPath(t)

	first, err := Open(path)
	if err != nil {
		t.Fatalf("open first store: %v", err)
	}
	t.Cleanup(func() {
		if err := first.Close(); err != nil {
			t.Fatalf("close first store: %v", err)
		}
	})

	second, err := Open(path)
	if err != nil {
		t.Fatalf("open second store: %v", err)
	}
	t.Cleanup(func() {
		if err := second.Close(); err != nil {
			t.Fatalf("close second store: %v", err)
		}
	})

	if _, err := first.db.Exec(
		"INSERT INTO workers (id, pid, started_at, heartbeat_at) VALUES (?, ?, ?, ?)",
		"worker-1",
		1234,
		1,
		1,
	); err != nil {
		t.Fatalf("insert worker from first store: %v", err)
	}

	var pid int
	if err := second.db.QueryRow("SELECT pid FROM workers WHERE id = ?", "worker-1").Scan(&pid); err != nil {
		t.Fatalf("read worker from second store: %v", err)
	}
	if pid != 1234 {
		t.Fatalf("expected pid 1234, got %d", pid)
	}
}

// --- Test helpers ---

func testDBPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "queuectl.db")
}

func openTestStore(t *testing.T) *Store {
	t.Helper()

	store, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
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
	if err != nil {
		t.Fatalf("check table %q: %v", table, err)
	}

	return true
}

func countRows(t *testing.T, store *Store, query string) int {
	t.Helper()

	var count int
	if err := store.db.QueryRow(query).Scan(&count); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	return count
}

func insertJob(t *testing.T, store *Store, id, state string, attempts, maxRetries, backoffBase int) {
	t.Helper()

	if _, err := store.db.Exec(
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
	); err != nil {
		t.Fatalf("insert job: %v", err)
	}
}
