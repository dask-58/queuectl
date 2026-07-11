package cli

import (
	"encoding/json"
	"fmt"
	"text/tabwriter"
	"time"

	"github.com/dask-58/queuectl/internal/store"
	"github.com/spf13/cobra"
)

type jobOutput struct {
	ID             string  `json:"id"`
	Command        string  `json:"command"`
	State          string  `json:"state"`
	Attempts       int     `json:"attempts"`
	MaxRetries     int     `json:"max_retries"`
	BackoffBase    int     `json:"backoff_base"`
	CreatedAt      string  `json:"created_at"`
	UpdatedAt      string  `json:"updated_at"`
	NextRunAt      *string `json:"next_run_at,omitempty"`
	WorkerID       *string `json:"worker_id,omitempty"`
	LeaseExpiresAt *string `json:"lease_expires_at,omitempty"`
	StartedAt      *string `json:"started_at,omitempty"`
	CompletedAt    *string `json:"completed_at,omitempty"`
	ExitCode       *int    `json:"exit_code,omitempty"`
	LastError      *string `json:"last_error,omitempty"`
}

func newListCommand(getenv func(string) string) *cobra.Command {
	var stateFlag string
	var jsonFlag bool

	cmd := &cobra.Command{
		Use:   "list --state <state>",
		Short: "List jobs",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if !store.IsValidJobState(stateFlag) {
				return fmt.Errorf("invalid job state: %q", stateFlag)
			}

			dbPath := databasePath(getenv)
			s, err := store.Open(dbPath)
			if err != nil {
				return fmt.Errorf("open store: %w", err)
			}

			jobs, queryErr := s.ListJobsByState(cmd.Context(), stateFlag)
			closeErr := s.Close()

			if queryErr != nil {
				return fmt.Errorf("list jobs: %w", queryErr)
			}
			if closeErr != nil {
				return fmt.Errorf("close store: %w", closeErr)
			}

			return renderJobList(cmd, jobs, jsonFlag)
		},
	}

	cmd.Flags().StringVar(&stateFlag, "state", "", "Job state to filter by")
	_ = cmd.MarkFlagRequired("state")
	cmd.Flags().BoolVar(&jsonFlag, "json", false, "Output in machine-readable JSON format")

	return cmd
}

func renderJobList(cmd *cobra.Command, jobs []store.Job, asJSON bool) error {
	if asJSON {
		outputs := make([]jobOutput, 0, len(jobs))
		for _, j := range jobs {
			outputs = append(outputs, toJobOutput(j))
		}
		return json.NewEncoder(cmd.OutOrStdout()).Encode(outputs)
	}

	w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(w, "ID\tSTATE\tATTEMPTS\tCOMMAND"); err != nil {
		return fmt.Errorf("write list header: %w", err)
	}
	for _, j := range jobs {
		if _, err := fmt.Fprintf(w, "%s\t%s\t%d\t%s\n", j.ID, j.State, j.Attempts, j.Command); err != nil {
			return fmt.Errorf("write job row: %w", err)
		}
	}
	if err := w.Flush(); err != nil {
		return fmt.Errorf("flush list output: %w", err)
	}

	return nil
}

func unixMilliToRFC3339(ms int64) string {
	return time.UnixMilli(ms).UTC().Format(time.RFC3339Nano)
}

func optionalUnixMilliToRFC3339(ms *int64) *string {
	if ms == nil {
		return nil
	}
	s := unixMilliToRFC3339(*ms)
	return &s
}

func toJobOutput(job store.Job) jobOutput {
	return jobOutput{
		ID:             job.ID,
		Command:        job.Command,
		State:          job.State,
		Attempts:       job.Attempts,
		MaxRetries:     job.MaxRetries,
		BackoffBase:    job.BackoffBase,
		CreatedAt:      unixMilliToRFC3339(job.CreatedAt),
		UpdatedAt:      unixMilliToRFC3339(job.UpdatedAt),
		NextRunAt:      optionalUnixMilliToRFC3339(job.NextRunAt),
		WorkerID:       job.WorkerID,
		LeaseExpiresAt: optionalUnixMilliToRFC3339(job.LeaseExpiresAt),
		StartedAt:      optionalUnixMilliToRFC3339(job.StartedAt),
		CompletedAt:    optionalUnixMilliToRFC3339(job.CompletedAt),
		ExitCode:       job.ExitCode,
		LastError:      job.LastError,
	}
}
