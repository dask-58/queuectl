# QueueCTL

A Go CLI for managing a persistent SQLite-backed background job queue.

**Status:** Under development.

## Storage

QueueCTL uses an embedded SQLite database (via [modernc.org/sqlite](https://pkg.go.dev/modernc.org/sqlite)) for persistent storage of jobs, configuration, and worker state. The database is initialized automatically on first use with versioned schema migrations.
