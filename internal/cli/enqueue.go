package cli

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/dask-58/queuectl/internal/store"
	"github.com/spf13/cobra"
)

type enqueueInput struct {
	ID      string `json:"id"`
	Command string `json:"command"`
}

func newEnqueueCommand(getenv func(string) string) *cobra.Command {
	return &cobra.Command{
		Use:   "enqueue <job-json>",
		Short: "Enqueue a job",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			input, err := decodeEnqueueInput(args[0])
			if err != nil {
				return fmt.Errorf("decode job JSON: %w", err)
			}

			dbPath := databasePath(getenv)
			s, err := store.Open(dbPath)
			if err != nil {
				return fmt.Errorf("open store: %w", err)
			}

			job, enqueueErr := s.Enqueue(cmd.Context(), input.ID, input.Command)
			closeErr := s.Close()

			if enqueueErr != nil {
				if errors.Is(enqueueErr, store.ErrJobAlreadyExists) {
					return fmt.Errorf("job %q already exists", input.ID)
				}
				return fmt.Errorf("enqueue job: %w", enqueueErr)
			}
			if closeErr != nil {
				return fmt.Errorf("close store: %w", closeErr)
			}

			fmt.Fprintln(cmd.OutOrStdout(), job.ID)
			return nil
		},
	}
}

func decodeEnqueueInput(raw string) (enqueueInput, error) {
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.DisallowUnknownFields()

	var input enqueueInput
	if err := dec.Decode(&input); err != nil {
		return enqueueInput{}, err
	}

	// Reject trailing content after the first JSON value.
	var extra json.RawMessage
	if err := dec.Decode(&extra); err != io.EOF {
		return enqueueInput{}, errors.New("unexpected content after JSON value")
	}

	input.ID = strings.TrimSpace(input.ID)
	if input.ID == "" {
		return enqueueInput{}, errors.New("job id is required")
	}
	if strings.TrimSpace(input.Command) == "" {
		return enqueueInput{}, errors.New("job command is required")
	}

	return input, nil
}

func databasePath(getenv func(string) string) string {
	if path := strings.TrimSpace(getenv("QUEUECTL_DB_PATH")); path != "" {
		return path
	}
	return "./queuectl.db"
}
