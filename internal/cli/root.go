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
		newStatusCommand(),
		newListCommand(getenv),
		newDLQCommand(),
		newConfigCommand(),
	)

	return cmd
}

func newStatusCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show queue status",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return ErrNotImplemented
		},
	}
}

func newDLQCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "dlq",
		Short: "Manage the dead-letter queue",
	}

	cmd.AddCommand(newDLQListCommand(), newDLQRetryCommand())
	return cmd
}

func newDLQListCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List dead-lettered jobs",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return ErrNotImplemented
		},
	}
}

func newDLQRetryCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "retry <id>",
		Short: "Retry a dead-lettered job",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return ErrNotImplemented
		},
	}
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
