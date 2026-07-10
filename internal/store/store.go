package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

const initTimeout = 10 * time.Second
const pragmaRetryDelay = 10 * time.Millisecond

type Store struct {
	db *sql.DB
}

func Open(path string) (*Store, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("database path is required")
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite database: %w", err)
	}

	store := &Store{db: db}
	if err := store.initialize(); err != nil {
		if closeErr := db.Close(); closeErr != nil {
			return nil, fmt.Errorf("%w; close sqlite database: %v", err, closeErr)
		}
		return nil, err
	}

	return store, nil
}

func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Store) initialize() error {
	s.db.SetMaxOpenConns(1)
	s.db.SetMaxIdleConns(1)

	ctx, cancel := context.WithTimeout(context.Background(), initTimeout)
	defer cancel()

	if err := s.configure(ctx); err != nil {
		return fmt.Errorf("configure sqlite database: %w", err)
	}
	if err := s.db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping sqlite database: %w", err)
	}
	if err := s.applyMigrations(ctx); err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}

	return nil
}

func (s *Store) configure(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, "PRAGMA busy_timeout = 5000"); err != nil {
		return fmt.Errorf("set busy_timeout: %w", err)
	}
	var busyTimeout int
	if err := s.db.QueryRowContext(ctx, "PRAGMA busy_timeout").Scan(&busyTimeout); err != nil {
		return fmt.Errorf("verify busy_timeout: %w", err)
	}
	if busyTimeout != 5000 {
		return fmt.Errorf("verify busy_timeout: expected 5000, got %d", busyTimeout)
	}

	if err := s.setJournalMode(ctx); err != nil {
		return err
	}

	if _, err := s.db.ExecContext(ctx, "PRAGMA foreign_keys = ON"); err != nil {
		return fmt.Errorf("enable foreign_keys: %w", err)
	}
	var foreignKeys int
	if err := s.db.QueryRowContext(ctx, "PRAGMA foreign_keys").Scan(&foreignKeys); err != nil {
		return fmt.Errorf("verify foreign_keys: %w", err)
	}
	if foreignKeys != 1 {
		return fmt.Errorf("verify foreign_keys: expected 1, got %d", foreignKeys)
	}

	return nil
}

func (s *Store) setJournalMode(ctx context.Context) error {
	var lastErr error
	for {
		var journalMode string
		err := s.db.QueryRowContext(ctx, "PRAGMA journal_mode = WAL").Scan(&journalMode)
		if err == nil {
			if !strings.EqualFold(journalMode, "wal") {
				return fmt.Errorf("set journal_mode: expected wal, got %q", journalMode)
			}
			return nil
		}
		if !isSQLiteBusy(err) {
			return fmt.Errorf("set journal_mode: %w", err)
		}
		lastErr = err

		timer := time.NewTimer(pragmaRetryDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("set journal_mode: %w", lastErr)
		case <-timer.C:
		}
	}
}

func isSQLiteBusy(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "SQLITE_BUSY") || strings.Contains(msg, "database is locked")
}
