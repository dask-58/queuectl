package cli

import (
	"errors"
	"fmt"
	"io"

	"github.com/spf13/cobra"
)

var ErrNotImplemented = errors.New("command not implemented")

func NewRootCommand(stdout, stderr io.Writer) *cobra.Command {
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
		newEnqueueCommand(),
		newWorkerCommand(),
		newStatusCommand(),
		newListCommand(),
		newDLQCommand(),
		newConfigCommand(),
	)

	return cmd
}

func newEnqueueCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "enqueue <job-json>",
		Short: "Enqueue a job",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return ErrNotImplemented
		},
	}
}

func newWorkerCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "worker",
		Short: "Manage workers",
	}

	cmd.AddCommand(newWorkerStartCommand(), newWorkerStopCommand())
	return cmd
}

func newWorkerStartCommand() *cobra.Command {
	var count int

	cmd := &cobra.Command{
		Use:   "start",
		Short: "Start workers",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if count <= 0 {
				return fmt.Errorf("count must be greater than zero")
			}
			return ErrNotImplemented
		},
	}

	cmd.Flags().IntVar(&count, "count", 1, "number of workers to start")
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

func newListCommand() *cobra.Command {
	var state string
	var jsonOutput bool

	cmd := &cobra.Command{
		Use:   "list --state <state> [--json]",
		Short: "List jobs",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if state == "" {
				return fmt.Errorf("required flag \"state\" not set")
			}
			return ErrNotImplemented
		},
	}

	cmd.Flags().StringVar(&state, "state", "", "job state to list")
	cmd.Flags().BoolVar(&jsonOutput, "json", false, "emit JSON output")

	return cmd
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
