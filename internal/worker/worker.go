// Package worker provides the queuectl worker runtime.
package worker

import (
	"context"
	"errors"
	"log/slog"
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

	slog.Info("Worker started", "worker", workerID, "pid", pid)

	recovered, recErr := s.RecoverExpiredJobs(ctx)
	if recErr != nil {
		if errors.Is(recErr, context.Canceled) || errors.Is(recErr, context.DeadlineExceeded) {
			return nil
		}
		return recErr
	}
	if recovered > 0 {
		slog.Info("Recovered abandoned job(s)", "worker", workerID, "count", recovered)
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
		slog.Info("Worker stopped", "worker", workerID)
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
			slog.Info("Graceful shutdown requested", "worker", workerID)
			return nil
		}

		recovered, err = s.RecoverExpiredJobs(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			return err
		}
		if recovered > 0 {
			slog.Info("Recovered abandoned job(s)", "worker", workerID, "count", recovered)
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

		slog.Info("Job claimed", "worker", workerID, "job", job.ID)
		slog.Info("Job execution started", "worker", workerID, "job", job.ID, "command", job.Command)

		start := time.Now()
		exitCode, stderr, err := executor.Execute(context.Background(), *job)
		duration := time.Since(start)
		if err != nil {
			return err
		}

		finishCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		finishedJob, err := s.FinishJobOwned(finishCtx, workerID, job.ID, exitCode, stderr)
		cancel()
		if err != nil {
			return err
		}

		if finishedJob.State == store.JobStateCompleted {
			slog.Info("Job completed", "worker", workerID, "job", job.ID, "duration", duration)
		} else if finishedJob.State == store.JobStateFailed {
			var retryIn time.Duration
			if finishedJob.NextRunAt != nil {
				retryIn = time.Until(time.UnixMilli(*finishedJob.NextRunAt)).Round(time.Second)
			}
			slog.Warn("Job failed", "worker", workerID, "job", job.ID, "exit_code", exitCode, "attempt", finishedJob.Attempts, "retry_in", retryIn)
		} else if finishedJob.State == store.JobStateDead {
			slog.Error("Job moved to DLQ", "worker", workerID, "job", job.ID, "exit_code", exitCode, "attempt", finishedJob.Attempts)
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
