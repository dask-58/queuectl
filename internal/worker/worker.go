package worker

import (
	"context"
	"errors"
	"time"

	"github.com/dask-58/queuectl/internal/executor"
	"github.com/dask-58/queuectl/internal/store"
)

const idlePollInterval = 250 * time.Millisecond

// Run executes jobs from the store until the context is cancelled.
// It uses a single foreground loop and handles basic backoff internally via timers.
func Run(ctx context.Context, s *store.Store) error {
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}

		job, err := s.ClaimNextJob(ctx, "local")
		if err != nil {
			if errors.Is(err, store.ErrNoPendingJobs) {
				timer := time.NewTimer(idlePollInterval)
				select {
				case <-ctx.Done():
					timer.Stop()
					return nil
				case <-timer.C:
					continue
				}
			}
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			return err
		}

		exitCode, stderr, err := executor.Execute(ctx, *job)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			return err
		}

		_, err = s.FinishJob(ctx, job.ID, exitCode, stderr)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			return err
		}
	}
}
