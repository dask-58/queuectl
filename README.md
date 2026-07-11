# QueueCTL

A Go CLI for managing a persistent SQLite-backed background job queue.

**Status:** Alpha.

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

### Start a worker

```sh
queuectl worker start
```

### Queue status

```sh
queuectl status
```

### Dead-letter queue

```sh
queuectl dlq list
queuectl dlq retry <job-id>
```

### Configuration

```sh
queuectl config list
queuectl config set max-retries 10
```

Jobs persist in the SQLite database at `./queuectl.db` by default.
Set `QUEUECTL_DB_PATH` to override the database location.

The underlying storage layer provides strict concurrency safety with atomic job claiming (`ClaimNextJob`), ensuring exactly-once processing capabilities for future workers.

## Lifecycle

Jobs transition through five storage states:
- `pending`: Awaiting execution (including exponential backoff retries).
- `processing`: Currently claimed by a worker.
- `completed`: Successfully finalized with `exit_code = 0`.
- `failed`: Temporary failure awaiting retry.
- `dead`: Failed beyond `max-retries`.

## Architecture

- A durable SQLite storage foundation supporting concurrent queue claiming.
- Safe retries with integer-based exponential backoff.
- Execution engine supporting shell pipelines via `sh -c`.
- Single-process foreground workers with abandoned job recovery.
- Runtime configuration managed dynamically through the CLI.

## Storage

QueueCTL uses an embedded SQLite database (via [modernc.org/sqlite](https://pkg.go.dev/modernc.org/sqlite)) for persistent storage of jobs, configuration, and worker state. The database is initialized automatically on first use with versioned schema migrations.
