// Package executor provides command execution.
package executor

import (
	"bytes"
	"context"
	"errors"
	"os/exec"

	"github.com/dask-58/queuectl/internal/store"
)

var shellPath = "sh"

// Execute runs a job command and returns its exit status.
func Execute(ctx context.Context, job store.Job) (exitCode int, stderr string, err error) {
	cmd := exec.CommandContext(ctx, shellPath, "-c", job.Command)

	var errBuf bytes.Buffer
	cmd.Stderr = &errBuf

	runErr := cmd.Run()
	capturedStderr := errBuf.String()

	if runErr == nil {
		return 0, capturedStderr, nil
	}

	if code, ok := exitCodeFromErr(runErr); ok {
		return code, capturedStderr, nil
	}

	if err := ctx.Err(); err != nil {
		return 0, "", err
	}

	// Non-process errors: executable not found, fork failure, etc.
	return 0, "", runErr
}

func exitCodeFromErr(err error) (int, bool) {
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		if exitErr.ExitCode() == -1 {
			return 0, false
		}
		return exitErr.ExitCode(), true
	}
	return 0, false
}
