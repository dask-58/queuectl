package cli

import (
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

	cmd.AddCommand(newWorkerStartCommand(getenv), newWorkerStopCommand())
	return cmd
}

func newWorkerStartCommand(getenv func(string) string) *cobra.Command {

	cmd := &cobra.Command{
		Use:   "start",
		Short: "Start workers",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			dbPath := databasePath(getenv)

			s, err := store.Open(dbPath)
			if err != nil {
				return fmt.Errorf("open store: %w", err)
			}
			defer s.Close()

			ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
			defer stop()

			return worker.Run(ctx, s)
		},
	}

	return cmd
}

func newWorkerStopCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "stop",
		Short: "Stop workers",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return ErrNotImplemented
		},
	}
}
