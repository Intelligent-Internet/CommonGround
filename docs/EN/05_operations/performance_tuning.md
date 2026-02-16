# CommonGround Performance Tuning Guide (PMO / Agent Worker / NATS Pull / Postgres)

> Migration note: the public entry point has moved to `docs/EN/05_operations/performance_tuning.md`. This page is kept as a full parameter reference.

This document focuses on throughput and stability of the scheduling pipeline: PMO (including L1 and L0 control plane), Agent Worker (wakeup/inbox/turn execution), NATS JetStream Pull, and Postgres connection pools.

Goals:
- Increase throughput without breaking **per-agent L0 ordering**
- Provide backpressure when queueing lags, avoiding PMO/Worker OOM
- Use a small set of parameters to move the system into **usable**, **benchmarked**, and **production-ready** states

Related benchmark scripts:
- `scripts/benchmarks/bench_fork_join_batch.py`
- Parameter combinations and usage notes: see top usage section and parameter comments in `scripts/benchmarks/bench_fork_join_batch.py`

## 1. First Diagnose Bottlenecks by Symptoms (Do not tune blindly)

Common symptoms and likely causes:
- `queue_delay_ms` is high, but `step_ms` is stable
  - Most likely worker concurrency is insufficient (`worker.max_concurrency` / number of instances too small)
  - Or wakeup scan throughput is too low (`worker.wakeup_concurrency` too small, inbox backlog accumulates)
- PMO logs contain many `PMO.L0Guard: Watchdog resent wakeup ...`
  - Indicates many agent heads stay in `dispatched` for a long time
  - Common reasons are `worker backlog > dispatched_retry_seconds` or wakeup delivery loss
  - In benchmark scenarios, it is usually better to increase `pmo.dispatched_retry_seconds` to reduce watchdog interference
- Worker logs `timeout waiting for tool_result inbox`
  - Usually the worker cannot keep up, causing tool result consumers to lag, or PMO-side L0 enqueue path is blocked
  - First check worker concurrency and DB/NATS latency
- Postgres log `sorry, too many clients already`
  - Connection pool limit or process count is too high, hitting PG `max_connections`
  - Either reduce **pool size per process** or increase PG `max_connections`

## 2. Key Parameters at a Glance (by component)

Notes:
- Keys in `config.toml` map to environment variables with `CG__` and `__` path separators (all lowercase).
- Queue-like parameters set to `0` mean unbounded; this is generally not recommended in production.

### 2.1 NATS Pull (global defaults affecting all pull-subscribes)

| Key | Environment Variable | Default | Purpose | Tuning Tip |
|---|---|---:|---|---|
| `nats.pull.batch_size` | `CG__nats__pull__batch_size` | `1` | Number of messages fetched per `fetch()` | Use `32~128` for throughput-first, but pair with downstream backpressure and enough `ack_wait` |
| `nats.pull.max_inflight` | `CG__nats__pull__max_inflight` | `1` | Maximum concurrent callbacks on the same subscription (semaphore) | Non-order-sensitive subscriptions can be set to `16~256`; order-sensitive ones (for example, L0) usually remain at `1` |
| `nats.pull.fetch_timeout_seconds` | `CG__nats__pull__fetch_timeout_seconds` | `5.0` | `fetch()` timeout | Usually no change needed; larger values reduce idle-loop log noise, smaller values make shutdown faster |
| `nats.pull.warmup_timeout_seconds` | `CG__nats__pull__warmup_timeout_seconds` | `0.1` | Timeout for a small initial `fetch` during startup | Usually no change; raise moderately if worker/PMO startup throughput is too low |

Notes:
- `batch_size > max_inflight` is not an error, but it causes over-prefetch with serial processing and can increase `ack_wait` pressure.
- `subscribe_cmd` and `subscribe_cmd_with_ack` currently use a fixed `ack_wait=30` and do not support configuration.

### 2.2 PMO (L1 Handler and Watchdog Concurrency)

| Key | Environment Variable | Default | Purpose | Tuning Tip |
|---|---|---:|---|---|
| `pmo.max_concurrency` | `CG__pmo__max_concurrency` | `1` | PMO internal L1 handler concurrency | Fork/join scenarios often use `CPU` or `CPU*2`; too high can saturate DB/NATS |
| `pmo.queue_maxsize` | `CG__pmo__queue_maxsize` | `0` | PMO worker queue limit (tool cmd) | In production use bounded queues (e.g., `max_concurrency*10` or `100~1000`) to enforce backpressure |
| `pmo.batch_dispatch_concurrency` | `CG__pmo__batch_dispatch_concurrency` | `8` | Dispatch concurrency for BatchManager child tasks | Typical bound is `1~32`; higher values are ultimately capped by process DB pool and raise DB pressure |
| `pmo.watchdog_interval_seconds` | `CG__pmo__watchdog_interval_seconds` | `5.0` | PMO watchdog poll interval | Smaller values recover stale states faster; too small increases DB/NATS read-write load |
| `pmo.dispatched_retry_seconds` | `CG__pmo__dispatched_retry_seconds` | `10.0` | Triggers `dispatched` resend when state exceeds this time | In benchmarks set high (e.g., `60~300`) to avoid interference; production should align with normal slowest worker queueing time |
| `pmo.dispatched_timeout_seconds` | `CG__pmo__dispatched_timeout_seconds` | `30.0` | Reclaims `dispatched` items to idle when timed out | Benchmarks commonly set to `0` (disabled); production should be larger than worst-case queueing time to avoid false recovery |
| `pmo.active_reap_seconds` | `CG__pmo__active_reap_seconds` | `300.0` | Reclaims long-running `running` heads | Keep default in production; shorten with alarm correlation when abnormal long-task backlog appears |
| `pmo.pending_wakeup_seconds` | `CG__pmo__pending_wakeup_seconds` | `5.0` | Pending inbox (`queued/pending`) wakeup retransmit threshold | Lower to reduce visible latency when pending backlog grows |
| `pmo.pending_wakeup_skip_seconds` | `CG__pmo__pending_wakeup_skip_seconds` | `300.0` | Missing `channel` threshold for pending-inbox skip (`<=0` disables) | Used to avoid endless retries on dirty data; too small values may drop alerts incorrectly |

### 2.3 PMO L0 Ordering and Queue State (no separate L0Subscriber config)

The current implementation no longer exposes independent `pmo.l0_subscriber.*` settings. The L0 path on the PMO side still uses `cmd.sys.pmo`, `cmd.agent.*.wakeup`, and DB state-machine sequencing/retry.

Adjustable knobs:
- `nats.pull.batch_size` / `nats.pull.max_inflight`: both PMO and Worker subscriptions follow this global pull strategy
- `pmo.max_concurrency`: controls PMO handler pool concurrency and thus L0 command reflow speed
- `worker.max_concurrency`: determines each agent's ack and turn execution throughput
- `worker.wakeup_queue_maxsize` and `worker.inbox_fetch_limit`: influence wakeup scan pressure and backpressure

Notes:
- Ordering is enforced by state-machine and queue semantics in the current implementation; it is not implemented through an independent L0 shard config.

### 2.4 Agent Worker (wakeup scan and turn execution)

| Key | Environment Variable | Default | Purpose | Tuning Tip |
|---|---|---:|---|---|
| `worker.max_concurrency` | `CG__worker__max_concurrency` | `1` | Turn execution concurrency | Usually the first tuning lever in fork/join scenarios, commonly `16~256` depending on CPU/IO/LLM runtime |
| `worker.wakeup_concurrency` | `CG__worker__wakeup_concurrency` | `max_concurrency` | Wakeup processing concurrency (inbox scan + claim) | For high fan-out, set `>= max_concurrency`; otherwise inbox scanning becomes the bottleneck |
| `worker.queue_maxsize` | `CG__worker__queue_maxsize` | `max_concurrency*4` | Turn working queue limit (0 = unbounded) | In production use bounded values for backpressure and to avoid worker OOM |
| `worker.wakeup_queue_maxsize` | `CG__worker__wakeup_queue_maxsize` | `0` | Wakeup key queue limit (0 = unbounded) | In high concurrency, prefer bounded values (e.g., `1024`) to prevent extreme buildup |
| `worker.inbox_fetch_limit` | `CG__worker__inbox_fetch_limit` | `20` | Max number of inboxes claimed per wakeup | Increase to `50~200` for big backlogs; too high causes wakeup processing to run too long |
| `worker.inbox_processing_timeout_seconds` | `CG__worker__inbox_processing_timeout_seconds` | `300` | Requeue timeout for in-progress inbox processing | Keep production default. For benchmarks you can raise it to reduce interference. Note: this does not increase local inbox guard waiting budget; guard can still fail earlier by design |
| `worker.watchdog_interval_seconds` | `CG__worker__watchdog_interval_seconds` | `5.0` | Worker watchdog poll interval | Larger lowers inspection load; smaller reacts faster to missed scans |
| `worker.suspend_timeout_seconds` | `CG__worker__suspend_timeout_seconds` | `30.0` | Local timeout budget for tool/result suspension waits | Shares timeout window with upstream `tool wait` path; too large can elongate queue tails |

### 2.5 Postgres Connection Pooling (throughput and stability foundation)

Default PMO / Agent Worker startup path is `ServiceBase -> ServiceContext -> AsyncConnectionPool`,
`cardbox.psycopg_*` and `cardbox.postgres_*` are not two independent pools in parallel:
- `cardbox.psycopg_*` defines shared `AsyncConnectionPool` min/max created by each service
- Stores and CardBox adapter in the same process share this single pool
- `cardbox.postgres_*` remains a fallback config for CardBox standalone mode (for some standalone initialization paths)

| Key | Environment Variable | Default | Purpose | Tuning Tip |
|---|---|---:|---|---|
| `cardbox.psycopg_max_size` | `CG__cardbox__psycopg_max_size` | `10` | Max connections for shared per-process `AsyncConnectionPool` (stores + CardBox adapter) | Usually `4~16` per process; if many processes, reduce to avoid flooding PG |
| `cardbox.postgres_max_size` | `CG__cardbox__postgres_max_size` | `10` | Max connections for CardBox fallback pool (standalone mode) | Usually `4~16`; if shared pool path is used, this is usually not a primary tuning knob |

Capacity estimate (empirical):
- Under default service paths, approximate total connections are `(PMO process count + Worker process count + other service process count) * psycopg_max_size`, plus headroom for psql/migration/monitoring clients.
- If benchmarking on local Docker PG, prefer setting per-process `*_max_size` to `4~8`; avoid setting `64/256` as a starting point.

## 3. Recommended Tuning Process (repeatable and focused)

Change only one parameter group per iteration, and record results.

1. Run baseline smoke test first
- `--parallel-batches 1`, `--rounds 3`; verify chain correctness and no contention
2. Push down worker-side queuing
- Increase `worker.max_concurrency` and/or worker instance count first
- A clear drop in `queue_delay_ms` usually indicates the bottleneck is worker-side
3. Tune PMO watchdog-related parameters
- First adjust `pmo.dispatched_retry_seconds` and `pmo.dispatched_timeout_seconds` to observe resend/recovery rates
- Increase `pmo.batch_dispatch_concurrency` and/or `pmo.max_concurrency` to improve internal batch dispatch throughput
4. Then inspect DB behavior
- If `too many clients` appears, first lower pool sizes or raise PG max_connections
- If many slow queries/lock waits show up, then optimize SQL or add indexes
5. Finalize watchdog settings
- Benchmarks usually need reduced watchdog interference (raise resend thresholds, disable timeout reaping)
- Production should keep watchdog fast enough to recover, but not aggressively enough to preempt normal queued work

## 4. Scenario-Based Best Practices (ready-to-use presets)

The following values are starting points, not ceilings. Always retest before release based on actual workload and hardware capacity.

### 4.1 Local Debugging (stability-first and reproducible)

Suggestions:
- Low concurrency with bounded queues
- Increase watchdog timeout thresholds to avoid false positives

Example:
```bash
CG__pmo__max_concurrency=1 \
CG__pmo__queue_maxsize=100 \
CG__pmo__dispatched_retry_seconds=60 \
CG__pmo__dispatched_timeout_seconds=0 \
CG__cardbox__psycopg_max_size=4 \
CG__cardbox__postgres_max_size=4 \
uv run -m services.pmo.service
```

### 4.2 fork_join/batch benchmarking (mock worker, measure scheduling throughput)

Goal:
- Remove external tool/LLM noise and focus on whether `queue_delay_ms` drops when worker concurrency increases

Suggestion:
- Raise worker concurrency sufficiently (on par with or slightly below number of agents)
- Set `pmo.dispatched_timeout_seconds=0` to prevent false reclaim during benchmark

Example:
```bash
# PMO
CG__pmo__max_concurrency=4 \
CG__pmo__queue_maxsize=1000 \
CG__pmo__dispatched_retry_seconds=120 \
CG__pmo__dispatched_timeout_seconds=0 \
CG__cardbox__psycopg_max_size=8 \
CG__cardbox__postgres_max_size=8 \
uv run -m services.pmo.service
```

```bash
# Worker (mock mode per your bench script worker_target settings)
CG__worker__max_concurrency=128 \
CG__worker__wakeup_concurrency=128 \
CG__worker__queue_maxsize=0 \
CG__worker__wakeup_queue_maxsize=2048 \
CG__worker__inbox_fetch_limit=50 \
CG__cardbox__psycopg_max_size=8 \
CG__cardbox__postgres_max_size=8 \
uv run -m services.agent_worker.loop
```

### 4.3 Production Medium Load (steady-state first)

Suggestion:
- Keep all critical queues bounded (PMO queue, worker queue)
- Keep pools moderate and scale by "more instances + appropriate concurrency"

Starting points:
- `pmo.max_concurrency=CPU~CPU*2`
- `worker.max_concurrency=CPU*2~CPU*8` when turn execution is mostly IO
- `cardbox.psycopg_max_size=8~16`, `cardbox.postgres_max_size=8~16` depending on PG capacity and instance count

### 4.4 High Fan-Out (many concurrent wakeups/scheduling)

Typical bottleneck:
- Worker wakeup scan and inbox claim become the limit

Suggestion:
- Increase `worker.wakeup_concurrency` and worker instance count first
- Then increase `pmo.batch_dispatch_concurrency` or `pmo.max_concurrency`
- Raise `inbox_fetch_limit` moderately (e.g., `50~200`) to reduce wakeup frequency

### 4.5 k8s Multi-Instance Notes (avoid cross-instance PMO ordering interference)

Key points:
- Worker can scale horizontally (same queue group) to improve throughput.
- PMO should be horizontally partitioned by project/workflow dimensions (for example by tenant or project-cluster deployment) to avoid hotspot contention.
