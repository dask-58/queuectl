package cli

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/dask-58/queuectl/internal/dashboard"
	"github.com/dask-58/queuectl/internal/store"
)

func newDashboardCommand(getenv func(string) string) *cobra.Command {
	var addr string

	cmd := &cobra.Command{
		Use:   "dashboard",
		Short: "Start the web dashboard",
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

			fmt.Fprintf(cmd.OutOrStdout(), "Dashboard starting on http://%s\n", addr)
			return dashboard.Serve(ctx, addr, s)
		},
	}

	cmd.Flags().StringVar(&addr, "addr", "127.0.0.1:8080", "address to listen on")

	return cmd
}
