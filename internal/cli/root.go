// Package cli provides the queuectl command-line interface.
package cli

import (
	"io"
	"os"

	"github.com/spf13/cobra"
)

// NewRootCommand creates the root CLI command.
func NewRootCommand(stdout, stderr io.Writer) *cobra.Command {
	return newRootCommand(stdout, stderr, os.Getenv)
}

func newRootCommand(stdout, stderr io.Writer, getenv func(string) string) *cobra.Command {
	cmd := &cobra.Command{
		Use:           "queuectl",
		Short:         "Manage background jobs",
		SilenceUsage:  true,
		SilenceErrors: true,
		CompletionOptions: cobra.CompletionOptions{
			DisableDefaultCmd: true,
		},
	}

	cmd.SetOut(stdout)
	cmd.SetErr(stderr)

	cmd.AddCommand(
		newEnqueueCommand(getenv),
		newWorkerCommand(getenv),
		newStatusCommand(getenv),
		newListCommand(getenv),
		newDLQCommand(getenv),
		newConfigCommand(getenv),
		newDashboardCommand(getenv),
	)

	return cmd
}
