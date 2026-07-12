# QueueCTL

QueueCTL is a Go CLI for running shell-command jobs from a persistent SQLite queue.


> [!TIP]
> Read [DECISIONS.md](DECISIONS.md) for design decisions and reasoning.

## Demo Video Link

## How to Run

```sh
go build -o queuectl ./cmd/queuectl

./queuectl enqueue '{"id":"job-1","command":"echo hello"}'
./queuectl worker start
./queuectl status
./queuectl list --state completed
```

Use `QUEUECTL_DB_PATH` to choose a database:

```sh
QUEUECTL_DB_PATH=/tmp/queuectl.db ./queuectl enqueue '{"id":"job-1","command":"echo hello"}'
```

## CLI

```sh
queuectl enqueue '{"id":"job-1","command":"echo hello"}'
queuectl list --state pending
queuectl list --state failed --json
queuectl status
queuectl worker start
queuectl worker start --count 3
queuectl worker stop
queuectl dlq list
queuectl dlq list --json
queuectl dlq retry job-1
queuectl config list
queuectl config set max-retries 5
queuectl config set backoff-base 2
queuectl dashboard --addr 127.0.0.1:8080
```

`worker start` blocks in the foreground. `worker stop` can be run from another terminal; it marks active workers in SQLite and each worker exits after finishing its current job.

## Job States

- `pending`: waiting to be claimed.
- `processing`: claimed by a worker and currently running.
- `failed`: a failed attempt is waiting for retry backoff.
- `completed`: finished with exit code 0.
- `dead`: failed past its retry budget and is visible in the DLQ.

The normal lifecycle is:

```text
pending -> processing -> completed
                    |
                    -> failed -> processing
                    |
                    -> dead -> pending (dlq retry)
```

## Storage

SQLite is the source of truth for jobs, workers, and config. The schema is in `internal/store/migrations/*.sql` and is applied by `internal/store/migrations.go` when the database opens.

The database runs in WAL mode with a 5 second SQLite busy timeout. Job rows include retry settings copied from config at enqueue time, so changing config does not alter jobs already in the queue.

The important tables are:

- `jobs`: command, state, attempts, retry config, lease owner, timestamps, exit code, last stderr.
- `workers`: worker ID, PID, heartbeat, stop request flag.
- `config`: `max-retries` and `backoff-base`.
- `schema_migrations`: applied migration versions.

## Concurrency

`internal/store/job.go` uses `BEGIN IMMEDIATE` in `ClaimNextJob` before selecting and updating the next runnable job. That obtains SQLite's write lock for the transaction, including across separate OS processes using the same database file. A second worker cannot select and claim the same row until the first transaction commits or rolls back.

This gives atomic claiming. It does not make shell commands exactly-once after a crash. If a worker dies after a command has produced external side effects but before `FinishJobOwned` commits, the job can run again after lease recovery. The queue is designed for at-least-once execution.

## Worker Lifecycle

On startup a worker:

1. Recovers expired `processing` jobs.
2. Registers itself in `workers`.
3. Starts a heartbeat loop.
4. Polls for stop requests, expired leases, and runnable jobs.

While a job runs, heartbeat extends leases for jobs owned by that worker. On SIGINT, SIGTERM, or `worker stop`, the worker stops claiming new jobs but lets the current command finish and records its result.

`worker start --count N` starts N independent worker loops in one foreground process. Each loop has its own worker ID and uses the same SQLite store.

## Retry Model

Retry settings are:

- `max-retries`: number of retry attempts after the first failed execution.
- `backoff-base`: integer base for exponential backoff.

After a failed attempt, `attempts` is incremented and the next retry is scheduled after:

```text
backoff-base ^ attempts seconds
```

With the default base `2`, retry delays are 2s, 4s, 8s, and so on. A job moves to `dead` when a failure would exceed its `max-retries`.

`dlq retry <id>` moves a dead job back to `pending` without resetting `attempts`. That gives the operator one manual run. If it fails again, it returns to `dead`.

## Crash Recovery

When a worker claims a job, the job gets a 30 second lease. A live worker refreshes its leases through heartbeat. If a process is killed with SIGKILL, it cannot unregister or finish the job. Once the lease expires, any running or newly started worker calls `RecoverExpiredJobs`, moves the abandoned job back to `pending`, and it can be claimed again.

Recovery depends on a worker process running or being started. QueueCTL is a CLI tool, not a background daemon, so no recovery work happens while all workers are stopped.

## JSON Output

`list --json` and `dlq list --json` return a JSON array. Empty results are `[]`. Timestamps are RFC3339 strings. Optional fields are omitted when null.

## Testing

```sh
go fmt ./...
go test ./...
go test -race ./...
go build -o queuectl ./cmd/queuectl
```

The tests cover enqueue, duplicate IDs, config persistence and validation, migrations, list/status output, DLQ retry, retry backoff, atomic concurrent claims, worker stop, graceful cancellation, abandoned job recovery, and dashboard handlers.

## Crash Recovery Reproduction

Terminal 1:

```sh
QUEUECTL_DB_PATH=/tmp/queuectl.db ./queuectl worker start
```

Terminal 2:

```sh
QUEUECTL_DB_PATH=/tmp/queuectl.db ./queuectl enqueue '{"id":"crash-demo","command":"sleep 60"}'
pgrep -f "queuectl worker start"
kill -9 <worker-pid>
```

Terminal 3:

```sh
QUEUECTL_DB_PATH=/tmp/queuectl.db ./queuectl worker start
```

After the old lease expires, the new worker recovers and reruns the job. Use `./queuectl status` and `./queuectl list --state processing --json` to inspect state.

## Folder Structure

```text
cmd/queuectl/           CLI entrypoint
internal/cli/           Cobra commands and output formatting
internal/store/         SQLite store, migrations, transactions, recovery
internal/worker/        Worker loop, heartbeat, shutdown behavior
internal/executor/      Shell command execution
internal/dashboard/     Optional read-only dashboard
```

## Known Limitations

- Jobs are at-least-once after crashes, not exactly-once.
- There is no per-job timeout.
- Stdout is not captured. Stderr from failed commands is stored as `last_error`.
- Crash recovery is performed by workers; if no worker is running, recovery waits until one starts.
