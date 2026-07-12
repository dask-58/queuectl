# QueueCTL Design Decisions

### 1. Which exact line(s) prevent two workers from claiming the same job, and why is that operation atomic across separate OS processes?

The critical logic lives in `ClaimNextJob` in `internal/store/job.go`.

The important lines are:

```go
// acquire a write lock so no other worker claims this job concurrently
if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
	return nil, fmt.Errorf("begin immediate transaction: %w", err)
}

/*
 * job selection and db update logic here
 */

// close the transaction to release the write lock so other workers can claim jobs
if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
	return nil, fmt.Errorf("commit transaction: %w", err)
}
```

`BEGIN IMMEDIATE` is the key line. SQLite acquires the write lock before the job is even selected, so if two workers try to claim a job at the same time, one will succeed and the other will either wait (or receive a busy error after the configured timeout in `internal/store/store.go`). This guarantees that only one worker can claim a particular job at a time.

Once the job is selected, it is immediately updated to `processing` inside the same transaction. The transaction is then committed, which releases the write lock and allows another worker to claim the next available job.

This makes the claim operation atomic across separate OS processes because SQLite coordinates write transactions using database-file locks.

Without this transaction, two workers could both execute the `SELECT` before either executes the `UPDATE`, causing both to believe they claimed the same job.

I also considered using process-local mutexes. They were rejected because the assignment requires multiple OS processes. A Go mutex only protects goroutines inside one process and does nothing when two worker processes are running from different terminals.

---

### 2. What happens if a worker is SIGKILLed halfway through a job? What is the worst-case delay before recovery?

Timeline:

- The worker has already claimed the job through `ClaimNextJob`. The row is `processing`, has `worker_id` set, and has `lease_expires_at` set to the claim time plus 30 seconds.
- The worker is executing the shell command.
- The process is killed with `SIGKILL`. It cannot run defer cleanup, unregister itself, or call `FinishJobOwned`.
- The job remains in the `processing` state. The `attempts` count is **unchanged** because attempts are only incremented inside `finishJob`.
- A running worker, or a newly started worker, periodically calls `RecoverExpiredJobs`.
- Once the lease expires (30 seconds by default), `RecoverExpiredJobs` moves expired `processing` jobs back to `pending`, clears the owner and lease fields, and leaves the attempt count unchanged.
- The job can then be claimed again by `ClaimNextJob`.

Recovery normally happens shortly after the lease expires (30-second lease + workers polling every 250 ms). If all workers are stopped, no recovery happens until a worker starts. The job simply remains in `processing` until then.

This means QueueCTL provides **at-least-once execution**. If `SIGKILL` happens after the shell command has already changed an external system but before `FinishJobOwned` commits, QueueCTL cannot know that the side effect already happened. The job may run again after recovery. Exactly-once execution would require idempotent job handlers or an external transaction protocol.

#### Worst-case delay

##### Case 1: At least one worker is running

The live worker will recover the job after the lease expires. The worst-case delay is therefore approximately **30 seconds + 250 ms = 30.25 seconds**.

##### Case 2: No workers are running

Since recovery is worker-driven, nobody is checking for expired leases. The job remains in `processing` until another worker starts. The worst-case delay is therefore **unbounded**.

---

### 3. Does `dlq retry` reset attempts? Why is that the right call?

No. `dlq retry` does **not** reset the attempt count. The job moves from `dead` back to `pending`, but the `attempts` value is preserved.

I chose this because it gives the operator one manual retry without changing the job's original retry budget. If the job fails again, it immediately returns to the DLQ because its retry budget has already been exhausted. An operator can still manually run `dlq retry` again if they choose.

---

### 4. What designs were considered and rejected for `worker stop`?

`worker stop` calls `RequestWorkerStop` from `internal/cli/worker.go`. The store updates the worker row by setting `stop_requested = 1`, and each worker checks this flag between jobs.

This design lets `worker stop` work from another terminal without tracking process IDs. It also keeps the stop request in the same durable place as the rest of the worker lifecycle.

Rejected alternatives:

- **OS signals:** This would require discovering worker PIDs, assumes the CLI can signal those processes, and doesn't work well across different machines or containers.
- **HTTP/RPC control endpoints:** This would require running a server inside every worker, introducing networking, port management, and additional complexity that isn't needed for this project.

I also wanted graceful shutdown. By checking the flag between jobs, the worker finishes its current job before exiting, so no in-progress work is interrupted.

The tradeoff is that a long-running job won't stop immediately, but I felt finishing the current job is safer and more predictable than killing it halfway through.

---

### 5. If priorities were added tomorrow (high-priority jobs jump the queue), which parts of your design survive unchanged and which break?

Most of the worker logic remains unchanged. `internal/worker/worker.go` simply asks the store for the next job. It doesn't know how that job was selected.

That means retries, recovery, leases, heartbeats, ownership checks, and `worker stop` all continue to work without any changes.

The localized changes would be:

- Add a `priority INTEGER NOT NULL DEFAULT 0` column through a new migration.
- Extend enqueue input and `Store.Enqueue` to accept a priority.
- Update `ClaimNextJob` to order by `priority DESC, created_at ASC, id ASC` instead of only FIFO.
- Display priority in `list --json`, the human-readable CLI output, and the dashboard.
- Add tests to verify that higher-priority jobs are selected first while preserving FIFO ordering within the same priority.

I wouldn't redesign the worker system for this. Priority is a scheduling policy, so it belongs in the store. The worker's responsibility is only to execute the job it receives.