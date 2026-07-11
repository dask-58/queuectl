package cli

import (
	"fmt"
	"text/tabwriter"

	"github.com/spf13/cobra"

	"github.com/dask-58/queuectl/internal/store"
)

func newStatusCommand(getenv func(string) string) *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show queue status",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			dbPath := databasePath(getenv)

			s, err := store.Open(dbPath)
			if err != nil {
				return fmt.Errorf("open store: %w", err)
			}

			st, err := s.Status(cmd.Context())
			if err != nil {
				_ = s.Close()
				return fmt.Errorf("query status: %w", err)
			}

			if err := s.Close(); err != nil {
				return fmt.Errorf("close store: %w", err)
			}

			w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)

			lines := []struct {
				label string
				count int
			}{
				{"Pending:", st.PendingJobs},
				{"Processing:", st.ProcessingJobs},
				{"Completed:", st.CompletedJobs},
				{"Failed:", st.FailedJobs},
				{"Dead:", st.DeadJobs},
				{"Workers:", st.ActiveWorkers},
			}

			for _, l := range lines {
				if _, err := fmt.Fprintf(w, "%s\t%d\n", l.label, l.count); err != nil {
					return fmt.Errorf("write output: %w", err)
				}
			}

			if err := w.Flush(); err != nil {
				return fmt.Errorf("flush output: %w", err)
			}

			return nil
		},
	}
}
