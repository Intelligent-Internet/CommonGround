# CommonGround 性能配置与调优指南（PMO / Agent Worker / NATS Pull / Postgres）

> 迁移提示：对外主入口已切换到 `docs/CN/05_operations/performance_tuning.md`。本页保留为完整参数手册。

本文聚焦“调度链路”的吞吐与稳定性：PMO（含 L1 与 L0 控制面）、Agent Worker（wakeup/inbox/turn 执行）、NATS JetStream Pull、以及 Postgres 连接池。

目标:
- 在 **不破坏 L0 per-agent 顺序** 的前提下提升吞吐
- 出现 Lagging 时 **有反压**，避免 PMO/Worker OOM
- 通过少量参数就能把系统调到“可用、可压测、可上线”

相关压测脚本:
- `scripts/benchmarks/bench_fork_join_batch.py`
- 说明与参数组合: 见 `scripts/benchmarks/bench_fork_join_batch.py` 顶部 Usage 与参数注释

## 1. 先用症状定位瓶颈（不要盲调参数）

常见症状与可能原因:
- `queue_delay_ms` 高，但 `step_ms` 稳定
  - 典型是 **worker 并发不足**（`worker.max_concurrency`/实例数太少）
  - 或 wakeup 扫描吞吐不足（`worker.wakeup_concurrency` 太小，inbox backlog 堆积）
- PMO 日志大量出现 `PMO.L0Guard: Watchdog resent wakeup ...`
  - 说明大量 agent head 长时间停留在 `dispatched`
  - 常见是 **worker backlog > dispatched_retry_seconds** 或 wakeup 丢失
  - benchmark 场景通常应把 `pmo.dispatched_retry_seconds` 调大，避免 watchdog 干扰结果
- Worker 报 `timeout waiting for tool_result inbox`
  - 通常是 worker 本身处理不过来导致 tool 结果消费不及时，或 PMO 侧 L0 enqueue 路径被卡住
  - 优先检查 worker 并发与 DB/NATS 延迟
- Postgres 日志 `sorry, too many clients already`
  - 连接池上限或进程数过大，触发 PG `max_connections` 限制
  - 需要 “降每进程连接池上限” 或 “提升 PG max_connections”

## 2. 关键参数速查（按组件）

说明:
- `config.toml` 的 key 与环境变量互通，环境变量格式为 `CG__` + `__` 分隔的路径（全部小写）。
- 队列类参数如果配置为 `0` 表示“无界”，生产环境一般不建议无界。

### 2.1 NATS Pull（全局默认，影响所有 pull-subscribe）

| 配置项 | 环境变量 | 默认值 | 作用 | 调优建议 |
|---|---|---:|---|---|
| `nats.pull.batch_size` | `CG__nats__pull__batch_size` | `1` | 每次 `fetch()` 拉取的消息数 | 吞吐优先可设 `32~128`，但要配合下游反压与足够的 `ack_wait` |
| `nats.pull.max_inflight` | `CG__nats__pull__max_inflight` | `1` | 同一订阅同时处理回调的并发数（Semaphore） | 非顺序敏感订阅可设 `16~256`；顺序敏感（如 L0）通常保持 `1` |
| `nats.pull.fetch_timeout_seconds` | `CG__nats__pull__fetch_timeout_seconds` | `5.0` | `fetch()` 超时 | 通常不需要改，调大能减少空转日志，调小能更快响应 shutdown |
| `nats.pull.warmup_timeout_seconds` | `CG__nats__pull__warmup_timeout_seconds` | `0.1` | 启动期先行一次小窗口 `fetch` 的超时 | 通常不必改；若 worker/PMO 启动阶段消息吞吐过慢可适度抬高 |

备注:
- `batch_size > max_inflight` 不是错误，但会导致“预取很多、实际串行处理”，可能增加 `ack_wait` 压力。
- `subscribe_cmd` 与 `subscribe_cmd_with_ack` 目前使用固定 `ack_wait=30`，不支持配置化。

### 2.2 PMO（L1 handler 与 Watchdog 并发）

| 配置项 | 环境变量 | 默认值 | 作用 | 调优建议 |
|---|---|---:|---|---|
| `pmo.max_concurrency` | `CG__pmo__max_concurrency` | `1` | PMO 内部 handler（L1）并发 | 对 fork/join 场景通常设 `CPU` 或 `CPU*2`，过大易把 DB/NATS 打满 |
| `pmo.queue_maxsize` | `CG__pmo__queue_maxsize` | `0` | PMO 工作者队列上限（tool cmd） | 生产建议设为有限值（例如 `max_concurrency*10` 或 `100~1000`）形成反压 |
| `pmo.batch_dispatch_concurrency` | `CG__pmo__batch_dispatch_concurrency` | `8` | BatchManager 派发子任务并发 | 限制为 `1~32` 常见，且会被进程 DB 池上限收敛；太高会抬高 DB 压力 |
| `pmo.watchdog_interval_seconds` | `CG__pmo__watchdog_interval_seconds` | `5.0` | PMO watchdog 轮询间隔 | 小值可加快 stale 状态回收，过小会增加 DB/NATS 读写负载 |
| `pmo.dispatched_retry_seconds` | `CG__pmo__dispatched_retry_seconds` | `10.0` | `dispatched` 超过此时间触发 wakeup 重发 | benchmark 建议调大（如 `60~300`）避免干扰；线上按“正常 worker 最慢排队时间”设置 |
| `pmo.dispatched_timeout_seconds` | `CG__pmo__dispatched_timeout_seconds` | `30.0` | `dispatched` 超时回收（置回 idle） | benchmark 建议 `0`（禁用）；线上需要大于最坏排队时间，否则会误伤 |
| `pmo.active_reap_seconds` | `CG__pmo__active_reap_seconds` | `300.0` | 扫描长期 `running` 的 head 并回收 | 线上一般保持默认；出现异常长任务堆积可缩短并结合告警 |
| `pmo.pending_wakeup_seconds` | `CG__pmo__pending_wakeup_seconds` | `5.0` | `queued/pending` inbox 超时重发阈值 | 可适当下调，减少 pending 堆积后可见延迟 |
| `pmo.pending_wakeup_skip_seconds` | `CG__pmo__pending_wakeup_skip_seconds` | `300.0` | 缺失 `channel` 的 pending inbox 跳过阈值（<=0 禁用） | 用于避免脏数据无限重试，值太小可能误丢告警 |

### 2.3 PMO L0 顺序与排队现状（已无独立 L0Subscriber 配置）

当前实现不再暴露独立的 `pmo.l0_subscriber.*` 配置。L0 路径在 PMO 侧仍通过 `cmd.sys.pmo`、`cmd.agent.*.wakeup` 与 DB 状态机完成序列化与重试。

可调整参数如下:
- `nats.pull.batch_size` / `nats.pull.max_inflight`：PMO 和 Worker 订阅均遵循该全局 pull 策略。
- `pmo.max_concurrency`：决定 PMO 侧 handler 池并发，影响 L0 级命令回流速度。
- `worker.max_concurrency`：决定每个 Agent 的回执与 turn 执行吞吐。
- `worker.wakeup_queue_maxsize` 与 `worker.inbox_fetch_limit`：影响 wakeup 扫描压力与反压。


注意:
- 顺序约束依赖于当前实现的状态机与队列语义，不是由某个独立的 L0 shard 配置实现。

### 2.4 Agent Worker（wakeup 扫描与 turn 执行）

| 配置项 | 环境变量 | 默认值 | 作用 | 调优建议 |
|---|---|---:|---|---|
| `worker.max_concurrency` | `CG__worker__max_concurrency` | `1` | turn 执行并发 | fork/join 场景一般是第一调优项，常用 `16~256`（视 CPU/IO/LLM 真实耗时） |
| `worker.wakeup_concurrency` | `CG__worker__wakeup_concurrency` | `max_concurrency` | wakeup 处理并发（inbox 扫描 + claim） | 高 fan-out 建议 >= `max_concurrency`，否则 inbox 扫描成为瓶颈 |
| `worker.queue_maxsize` | `CG__worker__queue_maxsize` | `max_concurrency*4` | turn 工作队列上限（0=无界） | 生产建议有限值，形成反压，避免 worker OOM |
| `worker.wakeup_queue_maxsize` | `CG__worker__wakeup_queue_maxsize` | `0` | wakeup key 队列上限（0=无界） | 高并发下建议设有限值（例如 `1024`），避免极端情况下堆积 |
| `worker.inbox_fetch_limit` | `CG__worker__inbox_fetch_limit` | `20` | 每次 wakeup claim 的 inbox 条数上限 | backlog 大可提高到 `50~200`；太大会导致单次 wakeup 时间过长 |
| `worker.inbox_processing_timeout_seconds` | `CG__worker__inbox_processing_timeout_seconds` | `300` | 处理中的 inbox 超时重入队 | 生产建议保留；benchmark 可适当调大避免干扰。注意：该项不延长 inbox guard 本地等待预算，guard 可按设计更早失败。 |
| `worker.watchdog_interval_seconds` | `CG__worker__watchdog_interval_seconds` | `5.0` | Worker watchdog 轮询间隔 | 调大可减轻巡检负荷，调小可更快补齐漏扫 |
| `worker.suspend_timeout_seconds` | `CG__worker__suspend_timeout_seconds` | `30.0` | tool/result 挂起等待的本地超时预算 | 与上游 `tool wait` 超时路径共同决定超时窗口，避免设置过大导致队列拖尾 |

### 2.5 Postgres 连接池（吞吐与稳定性底座）

当前 PMO / Agent Worker 的默认启动路径为 `ServiceBase -> ServiceContext -> AsyncConnectionPool`，
`cardbox.psycopg_*` 与 `cardbox.postgres_*` 并不是两个并行使用的池：
- `cardbox.psycopg_*` 决定由服务创建的共享 `AsyncConnectionPool` 的 `min/max`。
- 同一进程内的 stores 与 CardBox adapter 共用这一个 pool。
- `cardbox.postgres_*` 保留为 CardBox 在未注入外部 pool 时的兜底配置（例如部分独立初始化场景）。

| 配置项 | 环境变量 | 默认值 | 作用 | 调优建议 |
|---|---|---:|---|---|
| `cardbox.psycopg_max_size` | `CG__cardbox__psycopg_max_size` | `10` | 每进程共享 `AsyncConnectionPool` 最大连接（stores + CardBox adapter） | 每进程常用 `4~16`；进程多时要压小，避免打爆 PG |
| `cardbox.postgres_max_size` | `CG__cardbox__postgres_max_size` | `10` | CardBox 兜底池最大连接（独立模式） | 通常 `4~16`；在共享池模式下可不作为主要调参项 |

容量估算（经验公式）:
- 默认服务路径下总连接数近似 `(PMO 进程数 + Worker 进程数 + 其他服务进程数) * psycopg_max_size`，再留出余量给 psql/迁移/监控。
- 如果你在本地 docker PG 上压测，优先把每进程 `*_max_size` 压到 `4~8`，不要一上来就设 `64/256`。

## 3. 推荐调优流程（可重复、少走弯路）

建议每轮只改一组参数，并记录结果。

1. 先跑基线冒烟
- `--parallel-batches 1`，`--rounds 3`，确认链路正确、无争用
2. 把 worker 侧排队打下去
- 优先加 `worker.max_concurrency`/worker 实例数
- `queue_delay_ms` 明显下降通常说明瓶颈在 worker
3. 结合 PMO 看门狗参数收敛
- 优先调低 `pmo.dispatched_retry_seconds` 与 `pmo.dispatched_timeout_seconds` 观察重发/回收速率。
- 提升 `pmo.batch_dispatch_concurrency` 和/或 `pmo.max_concurrency` 改善内部批处理吞吐。
4. 再看 DB
- 出现 `too many clients` 就先降 pool 或增 PG max_connections
- 出现大量慢查询/锁等待，再考虑优化 SQL 或加索引
5. 最后再收敛 watchdog 参数
- benchmark 需要减少 watchdog 干扰（适当调大重试阈值、禁用超时回收）
- 线上需要让 watchdog 及时兜底（但不能误伤正常排队）

## 4. 典型场景最佳实践（可直接套用）

下面只给“起始值”，不是上限。上线前务必结合实际 workload 与机器资源复测。

### 4.1 本地调试（稳定优先，便于复现问题）

建议:
- 低并发、队列有界
- watchdog 超时阈值调大，避免误判

示例:
```bash
CG__pmo__max_concurrency=1 \
CG__pmo__queue_maxsize=100 \
CG__pmo__dispatched_retry_seconds=60 \
CG__pmo__dispatched_timeout_seconds=0 \
CG__cardbox__psycopg_max_size=4 \
CG__cardbox__postgres_max_size=4 \
uv run -m services.pmo.service
```

### 4.2 fork_join/batch 压测（Mock worker，测调度吞吐）

目标:
- 排除外部工具/LLM 干扰，主要看 `queue_delay_ms` 是否被 worker 并发拉下来

建议:
- worker 并发拉到足够高（例如与 agents 同级或略小）
- `pmo.dispatched_timeout_seconds=0` 避免压测时误伤

示例:
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
# Worker（mock 模式按你的 bench 脚本参数/worker_target 配置）
CG__worker__max_concurrency=128 \
CG__worker__wakeup_concurrency=128 \
CG__worker__queue_maxsize=0 \
CG__worker__wakeup_queue_maxsize=2048 \
CG__worker__inbox_fetch_limit=50 \
CG__cardbox__psycopg_max_size=8 \
CG__cardbox__postgres_max_size=8 \
uv run -m services.agent_worker.loop
```

### 4.3 生产中等负载（稳态优先）

建议:
- 所有关键队列有界（PMO queue、worker queue）
- 连接池不要太大，靠“多实例 + 适度并发”扩展

起始建议:
- `pmo.max_concurrency=CPU~CPU*2`
- `worker.max_concurrency=CPU*2~CPU*8`（如果 turn 执行主要是 IO）
- `cardbox.psycopg_max_size=8~16`，`cardbox.postgres_max_size=8~16`（视 PG 容量与实例数）

### 4.4 高 fan-out（很多 agent 并发唤醒/调度）

典型瓶颈:
- worker wakeup 扫描与 inbox claim 成为瓶颈

建议:
- 先加 `worker.wakeup_concurrency` 与 worker 实例数
- 再加 `pmo.batch_dispatch_concurrency` 或 `pmo.max_concurrency`
- `inbox_fetch_limit` 适度提高（比如 `50~200`）减少 wakeup 次数

### 4.5 k8s 多实例注意事项（避免多实例干扰 PMO 顺序）

要点:
- Worker 可以水平扩展（同一个 queue group）来提升吞吐。
- PMO 建议按 project/工作流维度做水平切分（例如按租户或 project 集群部署）以避免热点争用。
