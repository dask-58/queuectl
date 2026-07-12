package cli

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/dask-58/queuectl/internal/store"
	"github.com/dask-58/queuectl/internal/worker"
)

func newWorkerCommand(getenv func(string) string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "worker",
		Short: "Manage workers",
	}

	cmd.AddCommand(newWorkerStartCommand(getenv), newWorkerStopCommand(getenv))
	return cmd
}

func newWorkerStartCommand(getenv func(string) string) *cobra.Command {
	var count int

	cmd := &cobra.Command{
		Use:   "start",
		Short: "Start workers",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if count < 1 {
				return fmt.Errorf("count must be greater than zero")
			}

			dbPath := databasePath(getenv)

			s, err := store.Open(dbPath)
			if err != nil {
				return fmt.Errorf("open store: %w", err)
			}
			defer s.Close()

			ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
			defer stop()

			return runWorkers(ctx, s, count)
		},
	}

	cmd.Flags().IntVar(&count, "count", 1, "number of workers to run in this process")

	return cmd
}

func runWorkers(ctx context.Context, s *store.Store, count int) error {
	if count == 1 {
		return worker.Run(ctx, s)
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	errs := make(chan error, count)
	for i := 0; i < count; i++ {
		go func() {
			errs <- worker.Run(runCtx, s)
		}()
	}

	var firstErr error
	for i := 0; i < count; i++ {
		if err := <-errs; err != nil && firstErr == nil {
			firstErr = err
			cancel()
		}
	}

	return firstErr
}

func newWorkerStopCommand(getenv func(string) string) *cobra.Command {
	return &cobra.Command{
		Use:   "stop",
		Short: "Stop workers",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			dbPath := databasePath(getenv)

			s, err := store.Open(dbPath)
			if err != nil {
				return fmt.Errorf("open store: %w", err)
			}
			defer s.Close()

			affected, err := s.RequestWorkerStop(cmd.Context())
			if err != nil {
				return fmt.Errorf("request worker stop: %w", err)
			}

			fmt.Fprintf(cmd.OutOrStdout(), "Stopped %d worker(s)\n", affected)
			return nil
		},
	}
}
