package cli

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDashboardCommandExitsOnCancel(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "queuectl.db")
	env := map[string]string{
		"QUEUECTL_DB_PATH": dbPath,
	}
	getenv := func(key string) string { return env[key] }

	var stdout, stderr bytes.Buffer
	cmd := newRootCommand(&stdout, &stderr, getenv)

	// Use an unused port for testing to avoid conflicts
	cmd.SetArgs([]string{"dashboard", "--addr", "127.0.0.1:0"})

	ctx, cancel := context.WithCancel(context.Background())
	cmd.SetContext(ctx)

	// Cancel it shortly after starting
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	err := cmd.Execute()
	require.NoError(t, err)

	assert.Contains(t, stdout.String(), "Dashboard starting")
}
