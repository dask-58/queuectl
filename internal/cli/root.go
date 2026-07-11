package cli

import (
	"errors"
	"io"
	"os"

	"github.com/spf13/cobra"
)

var ErrNotImplemented = errors.New("command not implemented")

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
		newConfigCommand(),
	)

	return cmd
}

func newConfigCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config",
		Short: "Manage configuration",
	}

	cmd.AddCommand(newConfigSetCommand())
	return cmd
}

func newConfigSetCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "set <key> <value>",
		Short: "Set a configuration value",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return ErrNotImplemented
		},
	}
}
