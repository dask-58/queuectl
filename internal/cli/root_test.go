package cli

import (
	"bytes"
	"errors"
	"strings"
	"testing"
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
	if err != nil {
		t.Fatalf("expected help to succeed, got %v", err)
	}
	if stdout == "" {
		t.Fatal("expected help on stdout")
	}
	if stderr != "" {
		t.Fatalf("expected empty stderr, got %q", stderr)
	}
}

func TestRequiredTopLevelCommandsExist(t *testing.T) {
	_, stdout, _ := execute("--help")

	for _, name := range []string{"enqueue", "worker", "status", "list", "dlq", "config"} {
		if !strings.Contains(stdout, name) {
			t.Fatalf("expected help to contain %q, got:\n%s", name, stdout)
		}
	}
}

func TestWorkerStartDefaultsCountToOne(t *testing.T) {
	err, stdout, _ := execute("worker", "start")
	if !errors.Is(err, ErrNotImplemented) {
		t.Fatalf("expected not implemented error, got %v", err)
	}
	if stdout != "" {
		t.Fatalf("expected empty stdout, got %q", stdout)
	}
}

func TestWorkerStartRejectsZeroCount(t *testing.T) {
	err, _, _ := execute("worker", "start", "--count", "0")
	if err == nil {
		t.Fatal("expected error")
	}
	if errors.Is(err, ErrNotImplemented) {
		t.Fatalf("expected validation error, got %v", err)
	}
}

func TestWorkerStartRejectsNegativeCount(t *testing.T) {
	err, _, _ := execute("worker", "start", "--count", "-1")
	if err == nil {
		t.Fatal("expected error")
	}
	if errors.Is(err, ErrNotImplemented) {
		t.Fatalf("expected validation error, got %v", err)
	}
}

func TestListRequiresState(t *testing.T) {
	err, _, _ := execute("list")
	if err == nil {
		t.Fatal("expected error")
	}
	if errors.Is(err, ErrNotImplemented) {
		t.Fatalf("expected validation error, got %v", err)
	}
}

func TestUnknownCommandsFail(t *testing.T) {
	err, _, _ := execute("missing")
	if err == nil {
		t.Fatal("expected unknown command error")
	}
}

func TestCommandErrorsDoNotWriteToStdout(t *testing.T) {
	err, stdout, stderr := execute("status")
	if !errors.Is(err, ErrNotImplemented) {
		t.Fatalf("expected not implemented error, got %v", err)
	}
	if stdout != "" {
		t.Fatalf("expected empty stdout, got %q", stdout)
	}
	if stderr != "" {
		t.Fatalf("expected empty stderr, got %q", stderr)
	}
}
