package cli

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func testDBPathWorker(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "queuectl.db")
}

func TestWorkerStartCLI(t *testing.T) {
	dbPath := testDBPathWorker(t)

	env := map[string]string{
		"QUEUECTL_DB_PATH": dbPath,
	}

	getenv := func(key string) string {
		return env[key]
	}

	var stdout, stderr bytes.Buffer
	cmd := newRootCommand(&stdout, &stderr, getenv)
	cmd.SetArgs([]string{"worker", "start"})

	// Create cancelable context and cancel it immediately
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cmd.SetContext(ctx)

	err := cmd.Execute()
	require.NoError(t, err, "expected command to exit cleanly on cancel")
}
