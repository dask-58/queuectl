package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

// Sentinel errors for worker lifecycle failures.
var (
	ErrWorkerAlreadyExists = errors.New("worker already exists")
	ErrWorkerNotFound      = errors.New("worker not found")
)

// RegisterWorker inserts a new worker record.
func (s *Store) RegisterWorker(ctx context.Context, workerID string, pid int) error {
	workerID = strings.TrimSpace(workerID)
	if workerID == "" {
		return errors.New("worker id is required")
	}
	if pid <= 0 {
		return errors.New("pid must be greater than zero")
	}

	conn, err := s.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection: %w", err)
	}
	defer conn.Close()

	now := time.Now().UnixMilli()
	query := `INSERT INTO workers (id, pid, started_at, heartbeat_at) VALUES (?, ?, ?, ?)`

	if _, err := conn.ExecContext(ctx, query, workerID, pid, now, now); err != nil {
		if isWorkerIDConflict(err) {
			return ErrWorkerAlreadyExists
		}
		return fmt.Errorf("insert worker: %w", err)
	}

	return nil
}

func isWorkerIDConflict(err error) bool {
	var sqliteErr *sqlite.Error
	return errors.As(err, &sqliteErr) && sqliteErr.Code() == sqlite3.SQLITE_CONSTRAINT_PRIMARYKEY
}

// Heartbeat updates the heartbeat_at timestamp for a registered worker.
func (s *Store) Heartbeat(ctx context.Context, workerID string) error {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection: %w", err)
	}
	defer conn.Close()

	now := time.Now().UnixMilli()
	query := `UPDATE workers SET heartbeat_at = ? WHERE id = ?`

	res, err := conn.ExecContext(ctx, query, now, workerID)
	if err != nil {
		return fmt.Errorf("update worker heartbeat: %w", err)
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("get rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return ErrWorkerNotFound
	}

	return nil
}

// UnregisterWorker removes a worker record.
func (s *Store) UnregisterWorker(ctx context.Context, workerID string) error {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection: %w", err)
	}
	defer conn.Close()

	query := `DELETE FROM workers WHERE id = ?`

	res, err := conn.ExecContext(ctx, query, workerID)
	if err != nil {
		return fmt.Errorf("delete worker: %w", err)
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("get rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return ErrWorkerNotFound
	}

	return nil
}

// RequestWorkerStop marks all active workers to stop gracefully.
func (s *Store) RequestWorkerStop(ctx context.Context) (int, error) {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return 0, fmt.Errorf("acquire connection: %w", err)
	}
	defer conn.Close()

	query := `UPDATE workers SET stop_requested = 1 WHERE stop_requested = 0`
	res, err := conn.ExecContext(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("update stop_requested: %w", err)
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("get rows affected: %w", err)
	}

	return int(rows), nil
}

// ShouldStopWorker checks if a specific worker has been asked to stop.
func (s *Store) ShouldStopWorker(ctx context.Context, workerID string) (bool, error) {
	workerID = strings.TrimSpace(workerID)
	if workerID == "" {
		return false, fmt.Errorf("worker ID is required")
	}

	conn, err := s.db.Conn(ctx)
	if err != nil {
		return false, fmt.Errorf("acquire connection: %w", err)
	}
	defer conn.Close()

	var stopRequested int
	err = conn.QueryRowContext(ctx, `SELECT stop_requested FROM workers WHERE id = ?`, workerID).Scan(&stopRequested)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, ErrWorkerNotFound
		}
		// Try string match in case modernc wraps it or doesn't
		if strings.Contains(err.Error(), "no rows") {
			return false, ErrWorkerNotFound
		}
		return false, fmt.Errorf("query stop_requested: %w", err)
	}

	return stopRequested == 1, nil
}
