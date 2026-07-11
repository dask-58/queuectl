package store

import (
	"context"
	"fmt"
	"strings"
)

func (s *Store) GetConfig(ctx context.Context) (map[string]string, error) {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire connection: %w", err)
	}
	defer conn.Close()

	rows, err := conn.QueryContext(ctx, `SELECT key, value FROM config ORDER BY key ASC`)
	if err != nil {
		return nil, fmt.Errorf("query config: %w", err)
	}
	defer rows.Close()

	cfg := make(map[string]string)
	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			return nil, fmt.Errorf("scan config row: %w", err)
		}
		cfg[key] = value
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate config rows: %w", err)
	}

	return cfg, nil
}

func (s *Store) SetConfig(ctx context.Context, key, value string) error {
	key = strings.TrimSpace(key)
	if key == "" {
		return fmt.Errorf("config key is required")
	}

	conn, err := s.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection: %w", err)
	}
	defer conn.Close()

	res, err := conn.ExecContext(ctx, `UPDATE config SET value = ? WHERE key = ?`, value, key)
	if err != nil {
		return fmt.Errorf("update config: %w", err)
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("check rows affected: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("unknown config key %q", key)
	}

	return nil
}
