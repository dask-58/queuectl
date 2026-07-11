package store

import (
	"context"
	"fmt"
)

// Status summarizes queue state.
type Status struct {
	PendingJobs    int
	ProcessingJobs int
	CompletedJobs  int
	FailedJobs     int
	DeadJobs       int

	ActiveWorkers int
}

// Status retrieves the aggregate queue overview.
func (s *Store) Status(ctx context.Context) (*Status, error) {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire connection: %w", err)
	}
	defer conn.Close()

	var st Status

	queryJobs := `
		SELECT
			COALESCE(SUM(state = ?), 0),
			COALESCE(SUM(state = ?), 0),
			COALESCE(SUM(state = ?), 0),
			COALESCE(SUM(state = ?), 0),
			COALESCE(SUM(state = ?), 0)
		FROM jobs
	`

	if err := conn.QueryRowContext(ctx, queryJobs,
		JobStatePending,
		JobStateProcessing,
		JobStateCompleted,
		JobStateFailed,
		JobStateDead,
	).Scan(
		&st.PendingJobs,
		&st.ProcessingJobs,
		&st.CompletedJobs,
		&st.FailedJobs,
		&st.DeadJobs,
	); err != nil {
		return nil, fmt.Errorf("query job status: %w", err)
	}

	queryWorkers := `SELECT COUNT(*) FROM workers`
	if err := conn.QueryRowContext(ctx, queryWorkers).Scan(&st.ActiveWorkers); err != nil {
		return nil, fmt.Errorf("query worker status: %w", err)
	}

	return &st, nil
}
