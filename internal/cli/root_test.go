package cli

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func execute(args ...string) (error, string, string) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	cmd := NewRootCommand(&stdout, &stderr)
	cmd.SetArgs(args)

	err := cmd.Execute()
	return err, stdout.String(), stderr.String()
}

func TestRootHelpSucceeds(t *testing.T) {
	err, stdout, stderr := execute("--help")
	require.NoError(t, err)
	assert.NotEmpty(t, stdout)
	assert.Empty(t, stderr)
}

func TestRequiredTopLevelCommandsExist(t *testing.T) {
	_, stdout, _ := execute("--help")

	for _, name := range []string{"enqueue", "worker", "status", "list", "dlq", "config"} {
		assert.Contains(t, stdout, name)
	}
}

func TestListRequiresState(t *testing.T) {
	err, _, _ := execute("list")
	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrNotImplemented)
}

func TestUnknownCommandsFail(t *testing.T) {
	err, _, _ := execute("missing")
	assert.Error(t, err)
}

func TestCommandErrorsDoNotWriteToStdout(t *testing.T) {
	err, stdout, stderr := execute("dlq", "list")
	assert.ErrorIs(t, err, ErrNotImplemented)
	assert.Empty(t, stdout)
	assert.Empty(t, stderr)
}
