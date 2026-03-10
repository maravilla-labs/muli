# muli-queue

Priority job queue and scheduling for the Muli system.

## Overview

This crate provides the scheduling layer that orders jobs by priority, enforces concurrency limits, and dispatches work to executors.

## Key Components

### PriorityQueue

A `BinaryHeap`-backed priority queue that scores jobs using:

```
score = tier_weight * (10 + minutes_in_queue) / 10
```

This formula ensures higher-priority jobs run first while preventing starvation of lower-priority jobs over time. Supports push, pop, requeue, and remove operations.

### Scheduler

The main scheduling loop:

- Polls the queue every 5 seconds
- Checks global and per-tenant concurrency limits before dispatching
- Dispatches jobs to a registered callback
- Backs off when concurrency is exhausted
- Accepts a `CancellationToken` for graceful shutdown — stops cleanly without aborting in-flight work

### ConcurrencyLimiter

Enforces two levels of concurrency control:

- **Global** — A semaphore-based hard limit across all tenants
- **Per-tenant** — Atomic counters ensuring no single tenant monopolizes capacity

Returns RAII permit guards that automatically decrement counts when dropped.

### RetryPolicy

Exponential backoff retry logic for transient failures, configurable per job.

## Usage

```toml
[dependencies]
muli-queue = { path = "../muli-queue" }
```

Used by `muli-server` to manage the job scheduling pipeline.

See the [root README](../../README.md) for the full project overview.

## License

Apache-2.0 — see [LICENSE](../../LICENSE).
