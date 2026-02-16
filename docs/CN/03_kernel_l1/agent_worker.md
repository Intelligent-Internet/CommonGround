# Agent Worker 实现规范

本文属于 L1 内核层，描述 Worker 如何实现并遵守 L0 协议。

## 1. 角色与职责
- 订阅 `cmd.agent.{worker_target}.wakeup` 作为执行门铃（由 `config.toml` 的 `[worker].worker_targets` 决定）。
- 从 `state.agent_inbox` 拉取任务与回执。
- 水合 `state/resource/cards`，执行 ReAct 循环。
- 写入 cards 与 `state.*`，发布事件与流式输出。
- 语义真源以 inbox 记录为准，wakeup header 不作为审计真源。

## 2. 并发模型与状态约束
- **单实例多协程**：单进程/单实例内启动固定数量协程，从内部队列并发处理任务。
- **严格无状态**：严禁在 `self` 上保存任何与 agent/turn/step 相关的可变业务状态（如 current agent、上下文、临时结果、缓存等）。
- **状态归属**：业务状态必须只存在于 WorkItem、函数局部变量或持久化存储中。
- **允许的 self 内容**：不可变配置、连接池/客户端、队列、日志器等基础设施对象。
- **锁的使用边界**：锁仅可用于无隔离需求的共享统计/限流/资源池管理；不得以“先共享业务态再加锁”替代隔离。

## 3. Worker Target
- `worker_target` 是唯一的路由字段，来自 Profile/Roster。
- Worker 实例通过 `config.toml` 的 `[worker].worker_targets` 声明要消费的目标（可多个）。
- 常见值：`worker_generic`（通用 pool）、`ui_worker`（UI 入口）、`sandbox`（受控执行环境）。

### 3.1 单实例 Target（`svc1_`）命名约定
- 目的：为“有状态地消费 inbox 的服务/编排器”提供可投递且不与多实例争抢的路由目标（避免多个进程实例对同一 `agent_id` 的 inbox 产生竞争消费）。
- 协议约束：`worker_target` 必须是 NATS subject 的**单段 token**；禁止包含 `.` / `*` / `>` / 空白。推荐字符集为 `[a-z0-9_-]` 且全小写。
- 命名格式：`svc1_<service>_<scope>`
- `service`：服务/二进制名，例如 `better_demo` / `ground_control`。
- `scope`：唯一作用域，推荐包含 `env` + `cluster` 或 `project_id`（避免跨环境/跨项目碰撞），例如 `dev_proj_cgnd_demo_01`。
- 操作约定（必须遵守）：任意 `svc1_*` target 在同一环境中只能被一个进程实例订阅并消费（对应的 `cmd.agent.{target}.wakeup` 只能有一个“真正处理者”）。
- 多副本/HA：不要让多个副本共享同一 `svc1_*`。改为每副本独立 `agent_id + worker_target`（上游按实例选择 caller），或在共享前引入 leader election/DB lock 等单主机制。

## 4. 读写边界
- **读**：`state.*`、`resource.*`、cards/boxes（通过 card-box-cg）。
- **写**：`state.agent_state_head/agent_steps`（仅自己的 AgentTurn）、cards/boxes；必要时通过 ExecutionService 写 Report 到 `state.agent_inbox`。

## 5. 核心流程（含状态与水合）
1) 收到 `cmd.agent.*.wakeup` → **先 claim inbox**（`FOR UPDATE SKIP LOCKED`），state 更新使用 CAS 门禁。
2) `state_agent` 由 L0 在 enqueue/claim 时初始化 `agent_turn_id/turn_epoch`；`agent_turn` 不由 Worker 自行生成。
3) Worker 进入处理时会将 `agent_turn` 置 `running`（状态变更与 CAS 保护），并按顺序水合：`state.agent_state_head` → `resource.*` → `profile_box_id/context_box_id/output_box_id`。
4) **Greedy Batch**：从 `claim_pending_inbox` 拉取同一 Worker 对应 agent 的可执行 inbox，按时间顺序顺序处理。
5) 若存在工具调用：写 `tool.call` 卡并发布 `cmd.tool.*` / `cmd.sys.pmo.internal.*`，记录步内 `tool_call_id`，并标记 `activity=executing_tool`（LLM step metadata）。
6) 每个工具等待在 state 中写入 `turn_waiting_tools`，并设置 `resume_deadline`；`suspend_timeout` 由 worker 相关配置与工具侧 timeout 约束控制。当前实现未将 `expecting_correlation_id` 作为匹配条件持久化使用（常见路径为 `None`），而是通过 `tool_waiting + resume_ledger` 做恢复判定。
7) `status=suspended` 时，wake/resume 不再依赖单一 correlation 过滤；watchdog 与 resume 路径会在可消费条件满足时重放 resume 记录并回写 inbox，随后继续运行。
8) 工具返回后、或无 tool call 的普通 LLM 回合，按 `next_step(is_continuation=True)` 逻辑在同一 `agent_turn_id/turn_epoch` 内继续下一个 step（同一 turn 的内部续航；`resume` 可能产生 epoch 重排）。
9) 终态：写 `task.deliverable`，发布 `evt.agent.*.task`，清空 `active_agent_turn_id`，`status` 回到 `idle`。

> 约束：Worker **不得自行生成** `agent_turn_id/turn_epoch`；非法/缺失的 inbox 先记录告警并中止相关副作用。

补充说明（批量处理中的上下文增量更新）：
- 水合只在 wakeup 时做一次，得到初始上下文快照。
- `worker` 的 ReAct 是 step 级续航：一条 turn 可在一次处理链路内触发多次模型调用，多条 Enqueue/report 可以在同一 `agent_turn` 内顺序推进。
- 处理每个 step 时会将新输入与产出卡增量追加到上下文，不再每次全量重建。
- 同步写入 `output_box_id` 的新卡通过后续 wakeup 水合可在崩溃/重启后恢复。

## 6. 水合顺序（固定）
1) 读取 `state.agent_state_head` 并做门禁校验。
2) 读取 `resource.project_agents` / `resource.tools` / `resource.profiles`。
3) 读取 `profile_box_id` / `context_box_id` / `output_box_id`。

补充说明（可见性边界）：
- Worker 只会读取上述 `box_id` 指向的 cards/boxes；不会自动扫描/读取项目内其他 box。
- 因此跨 Agent 的上下文共享必须显式发生：由上游/PMO 进行 context packing（或未来由工具完成），把需要共享的 cards 装配进新的 `context_box_id`。
- 参考：`03_kernel_l1/pmo_orchestration.md`
- **`output_box_id` 是滚动记忆**：每轮会把 `output_box_id` 的卡并入上下文，因此输出会被读回。

### 6.1 工具规格构建（tool_spec 接入）
- 入口文件：`services/agent_worker/tool_spec.py`
  - `build_tool_specs`：将 `resource.tools` 行转换为 LLM tool specs（OpenAI-style），实际实现位于 `infra/llm/tool_specs.py`。
  - `filter_allowed_tools`：按 profile 的 `allowed_tools` 白名单过滤，并记录不匹配告警。
- 处理细节：
  - `options.args.defaults`：当 LLM 未提供参数时注入默认值。
  - `options.args.fixed` / `options.envs`：强制注入并从 LLM 参数中隐藏。
- Worker 在构建工具列表后，会追加内建 `submit_result`（详见 `services/agent_worker/builtin_tools.py`）。

## 7. 事件发布
- `evt.agent.*.step`：阶段事件（started/planning/executing/completed）。
- `evt.agent.*.task`：终态事件，必须携带 `deliverable_card_id`。
- `evt.agent.*.chunk`：流式输出（Core NATS）。
- LLM 调用返回的 `usage`/`response_cost` 会写入 `state.agent_steps.metadata`（字段 `llm_usage` / `llm_response_cost`），便于 SQL 汇总。
- LLM 返回的工具调用 ID 列表会写入 `state.agent_steps.tool_call_ids`（step 级索引，替代旧 `state.agent_turns.tool_call_ids`）。

## 8. 幂等与异常处理
- 任何 `UPDATE state` 必须带 `turn_epoch + active_agent_turn_id`，0 行即停止副作用。
- tool_result/tool callback 的去重与收敛由 `turn_waiting_tools` / `resume_ledger` 共同约束，关键键包括 agent、turn、epoch 与 tool_call 维度，避免重复投递导致的重复执行。
- `status` 或 `turn` 不一致/卡片已过期时，resume 会重排为 drop/待重试并记录可观测信号，而不是盲目重复提交。
- `Worker watchdog` 会重扫 `resume_ledger` 与等待表：在 resume 超时和重放条件下注入 `tool.result` timeout 报告，并驱动 wakeup 进入同一转交流程。
- `error/parse` 失败会走失败兜底：例如 `task.result_fields` / tools schema 不合法或解析异常时，记录告警并继续可用降级路径，不等同于硬性协议拒绝（除非无法继续构建模型请求）。
- Worker 崩溃由 PMO 回收；`watchdog` 与 `resume` 负责超时和回放，不应跨 agent 修改他人 state。

## 9. 显式完成（内建 submit_result）
- Worker 内建流程工具 `submit_result` 不走 UTP。
- 语义：写 `tool.result` + `task.deliverable` 并结束 Turn。
- 约束：同一 turn 仅允许该工具调用。
- 若 `context_box_id` 内存在 `task.result_fields`，需做最小字段校验；若解析失败，会记录 warning 并继续输出缺失字段降级（当前实现优先可用性）。
- **`task.result_fields` 结构要求**：内容应可解析为 `FieldsSchemaContent`；不符合时记录告警并采用兼容降级，而非直接硬拒（除非阻断主链路的关键字段不可用）。
- **Profile 字段 `must_end_with`**（默认空）：当某轮 **无 tool_calls** 时：
  - `must_end_with` 为空/缺失：自动把本轮 assistant 内容写成 `task.deliverable` 并结束 Turn（空内容写占位文本）。
  - `must_end_with` 非空：写入 `sys.must_end_with_required` 提示，并 re-enqueue 继续（要求下轮调用其中任一工具）。

> 注：`must_end_with` 的强制校验目前仅在“无 tool_calls”且 Agent 试图自然结束时生效。若某轮中调用的工具（Tool Service）显式返回了 `after_execution=terminate`，则当前 Turn 会被直接终止，从而允许绕过 `must_end_with` 的约束。
