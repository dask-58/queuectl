package cli

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/dask-58/queuectl/internal/store"
)

func newDLQCommand(getenv func(string) string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "dlq",
		Short: "Manage the dead-letter queue",
	}

	cmd.AddCommand(newDLQListCommand(getenv), newDLQRetryCommand(getenv))
	return cmd
}

func newDLQListCommand(getenv func(string) string) *cobra.Command {
	var jsonFlag bool

	cmd := &cobra.Command{
		Use:   "list",
		Short: "List dead-lettered jobs",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			dbPath := databasePath(getenv)
			s, err := store.Open(dbPath)
			if err != nil {
				return fmt.Errorf("open store: %w", err)
			}

			jobs, queryErr := s.ListJobsByState(cmd.Context(), store.JobStateDead)
			closeErr := s.Close()

			if queryErr != nil {
				return fmt.Errorf("list dead jobs: %w", queryErr)
			}
			if closeErr != nil {
				return fmt.Errorf("close store: %w", closeErr)
			}

			return renderJobList(cmd, jobs, jsonFlag)
		},
	}

	cmd.Flags().BoolVar(&jsonFlag, "json", false, "Output in machine-readable JSON format")
	return cmd
}

func newDLQRetryCommand(getenv func(string) string) *cobra.Command {
	return &cobra.Command{
		Use:   "retry <id>",
		Short: "Retry a dead-lettered job",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id := args[0]
			dbPath := databasePath(getenv)
			s, err := store.Open(dbPath)
			if err != nil {
				return fmt.Errorf("open store: %w", err)
			}

			job, retryErr := s.RetryDeadJob(cmd.Context(), id)
			closeErr := s.Close()

			if retryErr != nil {
				return fmt.Errorf("retry dead job: %w", retryErr)
			}
			if closeErr != nil {
				return fmt.Errorf("close store: %w", closeErr)
			}

			fmt.Fprintln(cmd.OutOrStdout(), job.ID)
			return nil
		},
	}
}
