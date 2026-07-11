package executor

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dask-58/queuectl/internal/store"
)

func TestExecuteSuccess(t *testing.T) {
	ctx := context.Background()
	job := store.Job{Command: "echo hello"}

	code, stderr, err := Execute(ctx, job)

	require.NoError(t, err)
	assert.Equal(t, 0, code)
	assert.Equal(t, "", stderr)
}

func TestExecuteFailureExitCode(t *testing.T) {
	ctx := context.Background()
	job := store.Job{Command: "false"}

	code, stderr, err := Execute(ctx, job)

	require.NoError(t, err)
	assert.Equal(t, 1, code)
	assert.Equal(t, "", stderr)
}

func TestExecuteFailureStderr(t *testing.T) {
	ctx := context.Background()
	job := store.Job{Command: "echo err >&2; exit 7"}

	code, stderr, err := Execute(ctx, job)

	require.NoError(t, err)
	assert.Equal(t, 7, code)
	assert.Equal(t, "err\n", stderr)
}

func TestExecuteNonexistentCommand(t *testing.T) {
	oldShell := shellPath
	t.Cleanup(func() { shellPath = oldShell })
	shellPath = "nonexistent-shell-executable"

	ctx := context.Background()
	job := store.Job{Command: "echo hello"}

	_, _, err := Execute(ctx, job)
	require.Error(t, err)
}

func TestExecuteContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel before execution

	job := store.Job{Command: "echo hello"}

	_, _, err := Execute(ctx, job)
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
}

func TestExecuteContextTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	job := store.Job{Command: "sleep 5"}

	_, _, err := Execute(ctx, job)
	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestExecuteStdoutIgnored(t *testing.T) {
	ctx := context.Background()
	job := store.Job{Command: "echo hello"}

	code, stderr, err := Execute(ctx, job)

	require.NoError(t, err)
	assert.Equal(t, 0, code)
	assert.Equal(t, "", stderr) // Stdout should not leak into stderr or anywhere else
}
