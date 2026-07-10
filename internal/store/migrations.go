package store

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"time"
)

// migrationFS contains all .sql migration files from the migrations directory.
//
//go:embed migrations/*.sql
var migrationFS embed.FS

const latestMigrationVersion = 1

type migration struct {
	version int
	file    string
	up      func(context.Context, *sql.Conn) error
}

func migrations() []migration {
	return []migration{
		{
			version: 1,
			file:    "migrations/001_initial_schema.sql",
			up:      seedDefaultConfig,
		},
	}
}

func (s *Store) applyMigrations(ctx context.Context) error {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire migration connection: %w", err)
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version INTEGER PRIMARY KEY,
			applied_at INTEGER NOT NULL
		)
	`); err != nil {
		return fmt.Errorf("create schema_migrations table: %w", err)
	}

	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return fmt.Errorf("begin migration transaction: %w", err)
	}

	if err := s.applyMigrationsLocked(ctx, conn); err != nil {
		if rollbackErr := rollbackMigration(conn); rollbackErr != nil {
			return fmt.Errorf("%w; rollback migration transaction: %v", err, rollbackErr)
		}
		return err
	}
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		if rollbackErr := rollbackMigration(conn); rollbackErr != nil {
			return fmt.Errorf("commit migration transaction: %w; rollback migration transaction: %v", err, rollbackErr)
		}
		return fmt.Errorf("commit migration transaction: %w", err)
	}

	return nil
}

func rollbackMigration(conn *sql.Conn) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := conn.ExecContext(ctx, "ROLLBACK")
	return err
}

func (s *Store) applyMigrationsLocked(ctx context.Context, conn *sql.Conn) error {
	appliedVersions, err := readAppliedMigrationVersions(ctx, conn)
	if err != nil {
		return err
	}
	currentVersion, err := validateMigrationHistory(appliedVersions, migrations(), latestMigrationVersion)
	if err != nil {
		return err
	}

	for _, m := range migrations() {
		if m.version <= currentVersion {
			continue
		}
		if err := applyMigration(ctx, conn, m); err != nil {
			return fmt.Errorf("apply migration %d: %w", m.version, err)
		}
	}

	return nil
}

func readAppliedMigrationVersions(ctx context.Context, conn *sql.Conn) ([]int, error) {
	rows, err := conn.QueryContext(ctx, "SELECT version FROM schema_migrations ORDER BY version")
	if err != nil {
		return nil, fmt.Errorf("read schema versions: %w", err)
	}
	defer rows.Close()

	var versions []int
	for rows.Next() {
		var version int
		if err := rows.Scan(&version); err != nil {
			return nil, fmt.Errorf("scan schema version: %w", err)
		}
		versions = append(versions, version)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read schema versions: %w", err)
	}

	return versions, nil
}

func validateMigrationHistory(applied []int, known []migration, latestSupported int) (int, error) {
	if len(applied) == 0 {
		return 0, nil
	}

	currentVersion := 0
	for i, version := range applied {
		if version <= 0 {
			return 0, fmt.Errorf("invalid schema migration version %d", version)
		}
		if version > latestSupported {
			return 0, fmt.Errorf("database schema version %d is newer than supported version %d", version, latestSupported)
		}
		if i >= len(known) {
			return 0, fmt.Errorf("unknown schema migration version %d", version)
		}
		if version != known[i].version {
			return 0, fmt.Errorf("migration history gap: expected version %d, got %d", known[i].version, version)
		}
		currentVersion = version
	}

	return currentVersion, nil
}

func applyMigration(ctx context.Context, conn *sql.Conn, m migration) error {
	ddl, err := migrationFS.ReadFile(m.file)
	if err != nil {
		return fmt.Errorf("read migration file: %w", err)
	}

	if _, err := conn.ExecContext(ctx, string(ddl)); err != nil {
		return fmt.Errorf("run schema changes: %w", err)
	}

	if m.up != nil {
		if err := m.up(ctx, conn); err != nil {
			return fmt.Errorf("run seed logic: %w", err)
		}
	}

	if _, err := conn.ExecContext(
		ctx,
		"INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)",
		m.version,
		time.Now().UTC().UnixMilli(),
	); err != nil {
		return fmt.Errorf("record schema version: %w", err)
	}

	return nil
}

func seedDefaultConfig(ctx context.Context, conn *sql.Conn) error {
	now := time.Now().UTC().UnixMilli()

	defaults := []struct {
		key   string
		value string
	}{
		{"max-retries", "3"},
		{"backoff-base", "2"},
	}

	for _, d := range defaults {
		if _, err := conn.ExecContext(
			ctx,
			"INSERT INTO config (key, value, updated_at) VALUES (?, ?, ?)",
			d.key,
			d.value,
			now,
		); err != nil {
			return err
		}
	}

	return nil
}
