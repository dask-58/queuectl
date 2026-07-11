package cli

import (
	"fmt"
	"sort"
	"text/tabwriter"

	"github.com/spf13/cobra"

	"github.com/dask-58/queuectl/internal/store"
)

func newConfigCommand(getenv func(string) string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config",
		Short: "Manage configuration",
	}

	cmd.AddCommand(newConfigListCommand(getenv), newConfigSetCommand(getenv))
	return cmd
}

func newConfigListCommand(getenv func(string) string) *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List all runtime configurations",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			dbPath := databasePath(getenv)
			s, err := store.Open(dbPath)
			if err != nil {
				return fmt.Errorf("open store: %w", err)
			}

			cfg, queryErr := s.GetConfig(cmd.Context())
			closeErr := s.Close()

			if queryErr != nil {
				return fmt.Errorf("get config: %w", queryErr)
			}
			if closeErr != nil {
				return fmt.Errorf("close store: %w", closeErr)
			}

			keys := make([]string, 0, len(cfg))
			for k := range cfg {
				keys = append(keys, k)
			}
			sort.Strings(keys)

			w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)
			if _, err := fmt.Fprintln(w, "KEY\tVALUE"); err != nil {
				return fmt.Errorf("write config header: %w", err)
			}

			for _, k := range keys {
				if _, err := fmt.Fprintf(w, "%s\t%s\n", k, cfg[k]); err != nil {
					return fmt.Errorf("write config row: %w", err)
				}
			}

			if err := w.Flush(); err != nil {
				return fmt.Errorf("flush config output: %w", err)
			}

			return nil
		},
	}
}

func newConfigSetCommand(getenv func(string) string) *cobra.Command {
	return &cobra.Command{
		Use:   "set <key> <value>",
		Short: "Set a configuration value",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			key, value := args[0], args[1]
			dbPath := databasePath(getenv)
			s, err := store.Open(dbPath)
			if err != nil {
				return fmt.Errorf("open store: %w", err)
			}

			setErr := s.SetConfig(cmd.Context(), key, value)
			closeErr := s.Close()

			if setErr != nil {
				return fmt.Errorf("set config: %w", setErr)
			}
			if closeErr != nil {
				return fmt.Errorf("close store: %w", closeErr)
			}

			return nil
		},
	}
}
