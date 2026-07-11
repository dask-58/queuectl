package main

import (
	"context"
	"fmt"
	"os/exec"
	"time"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := exec.CommandContext(ctx, "sh", "-c", "sleep 5").Run()
	if exitErr, ok := err.(*exec.ExitError); ok {
		fmt.Printf("ExitError: %d\n", exitErr.ExitCode())
	} else {
		fmt.Printf("Other error: %v\n", err)
	}
}
