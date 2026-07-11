// Package main is the entry point for the queuectl CLI.
package main

import (
	"fmt"
	"os"

	"github.com/dask-58/queuectl/internal/cli"
)

func main() {
	cmd := cli.NewRootCommand(os.Stdout, os.Stderr)
	if err := cmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
