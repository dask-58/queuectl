# QueueCTL

A Go CLI for managing a persistent SQLite-backed background job queue.

**Status:** Under development.

## Usage

### Enqueue a job

```sh
queuectl enqueue '{"id":"job1","command":"sleep 2"}'
```

Jobs persist in the SQLite database at `./queuectl.db` by default.
Set `QUEUECTL_DB_PATH` to override the database location.

## Storage

QueueCTL uses an embedded SQLite database (via [modernc.org/sqlite](https://pkg.go.dev/modernc.org/sqlite)) for persistent storage of jobs, configuration, and worker state. The database is initialized automatically on first use with versioned schema migrations.
