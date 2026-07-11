# QueueCTL

A Go CLI for managing a persistent SQLite-backed background job queue.

**Status:** Under development.

## Usage

### Enqueue a job

```sh
queuectl enqueue '{"id":"job1","command":"sleep 2"}'
```

### List jobs

You can list jobs by their state (e.g., pending, processing, completed, failed, dead):

```sh
queuectl list --state pending
```

For machine-readable output, append `--json`:

```sh
queuectl list --state pending --json
```

Jobs persist in the SQLite database at `./queuectl.db` by default.
Set `QUEUECTL_DB_PATH` to override the database location.

The underlying storage layer provides strict concurrency safety with atomic job claiming (`ClaimNextJob`), ensuring exactly-once processing capabilities for future workers.

## Lifecycle

Jobs transition through four storage states:
- `pending`: Awaiting execution (including exponential backoff retries).
- `processing`: Currently claimed by an execution layer.
- `completed`: Successfully finalized with `exit_code = 0`.
- `dead`: Failed beyond `max-retries`.

## Architecture

- A durable SQLite storage foundation supporting concurrent queue claiming.
- Safe retries with integer-based exponential backoff.
- Execution engine supporting shell pipelines via `sh -c`. (Note: Workers are still not implemented).

## Storage

QueueCTL uses an embedded SQLite database (via [modernc.org/sqlite](https://pkg.go.dev/modernc.org/sqlite)) for persistent storage of jobs, configuration, and worker state. The database is initialized automatically on first use with versioned schema migrations.
