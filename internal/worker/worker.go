// Package worker provides the queuectl worker runtime.
package worker

import (
	"context"
	"errors"
	"os"
	"time"

	"github.com/google/uuid"

	"github.com/dask-58/queuectl/internal/executor"
	"github.com/dask-58/queuectl/internal/store"
)

const idlePollInterval = 250 * time.Millisecond
const heartbeatInterval = 5 * time.Second

// Run executes jobs from the store until the context is cancelled.
func Run(ctx context.Context, s *store.Store) (err error) {
	workerID := uuid.NewString()
	pid := os.Getpid()

	if _, recErr := s.RecoverExpiredJobs(ctx); recErr != nil {
		if errors.Is(recErr, context.Canceled) || errors.Is(recErr, context.DeadlineExceeded) {
			return nil
		}
		return recErr
	}

	if err := s.RegisterWorker(ctx, workerID, pid); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil
		}
		return err
	}

	heartbeatCtx, stopHeartbeat := context.WithCancel(context.Background())
	heartbeatErrs := make(chan error, 1)
	go heartbeatLoop(heartbeatCtx, s, workerID, heartbeatErrs)

	defer func() {
		stopHeartbeat()
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if unregErr := s.UnregisterWorker(cleanupCtx, workerID); unregErr != nil {
			if err == nil {
				err = unregErr
			}
		}
	}()

	for {
		select {
		case heartbeatErr := <-heartbeatErrs:
			return heartbeatErr
		default:
		}

		if err := ctx.Err(); err != nil {
			return nil
		}

		stop, err := s.ShouldStopWorker(ctx, workerID)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			return err
		}
		if stop {
			return nil
		}

		if _, err := s.RecoverExpiredJobs(ctx); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			return err
		}

		job, claimErr := s.ClaimNextJob(ctx, workerID)
		if claimErr != nil {
			if errors.Is(claimErr, store.ErrNoPendingJobs) {
				timer := time.NewTimer(idlePollInterval)
				select {
				case <-ctx.Done():
					timer.Stop()
					return nil
				case <-timer.C:
					continue
				}
			}
			if errors.Is(claimErr, context.Canceled) || errors.Is(claimErr, context.DeadlineExceeded) {
				return nil
			}
			return claimErr
		}

		exitCode, stderr, err := executor.Execute(context.Background(), *job)
		if err != nil {
			return err
		}

		finishCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_, err = s.FinishJobOwned(finishCtx, workerID, job.ID, exitCode, stderr)
		cancel()
		if err != nil {
			return err
		}
	}
}

func heartbeatLoop(ctx context.Context, s *store.Store, workerID string, errs chan<- error) {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			heartbeatCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			err := s.Heartbeat(heartbeatCtx, workerID)
			cancel()
			if err != nil {
				select {
				case errs <- err:
				default:
				}
				return
			}
		}
	}
}
