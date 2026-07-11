package store

import (
	"context"
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
