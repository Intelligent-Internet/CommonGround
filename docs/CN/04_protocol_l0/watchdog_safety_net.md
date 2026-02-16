# 超时与兜底机制（Watchdog Safety Net）

本文是 L0/实现参考文档，约束系统在“无人处理 / 消息丢失 / 超时”场景下的兜底语义与必达行为。  
实现可由 PMO/Worker 的 watchdog 完成，但终态语义与输出契约应与本文一致。

> 核心原则：**Inbox 为真源，NATS 仅门铃**。兜底机制不得绕过 Inbox 与 State 的门禁约束。

## 1. 目标与范围
- 防止 `dispatched` / `pending` 长时间悬挂无终态。
- 防止 `suspended` 永久等待工具结果。
- 防止 processing 死锁导致 inbox 任务不可见。
- 在超时/异常路径仍保证 best-effort 产出 `task.deliverable` 与 `evt.agent.*.task`。

## 2. 触发点与兜底动作
### 2.1 Worker：`agent_inbox` 处理中超时回收
- 条件：`state.agent_inbox.status='processing'` 且 `processed_at` 过期（`[worker].inbox_processing_timeout_seconds`）。
- 动作：状态改为 `pending`，清空 `processed_at`（`archived_at` 也清空），让队列重新抢占。
- 目标：防止 worker crash / 处理卡死导致消息长期“被占用”。

### 2.2 Worker：`suspended` + `resume_deadline` 超时
- 条件：`status='suspended'` 且 `resume_deadline < now`，并且对应等待项 `turn_waiting_tools.wait_status='waiting'`。
- 动作：
  - 对每个超时 `tool_call_id` 产出 timeout 报告（`message_type='timeout'`、`status=timeout`、`error.code=tool_timeout`）并写入 `state.agent_inbox`，随后发 `cmd.agent.<target>.wakeup`。
  - 已落盘的报告卡会按正常恢复流程走 `tool.result` 消费链路；watchdog 本身不直接下发终态事件。
  - turn 级别 `resume_deadline` 在首条注入后清空（仍留 `status='suspended'` 的其他等待项供续重试）。
- 特例：若缺少 `tool.call` 卡，会跳过该超时注入。
- `resume_deadline` 计算：
  - 默认来自 `[worker].suspend_timeout_seconds`；
  - 若工具定义含 `options.suspend_timeout_seconds` 或 `options.timeout_seconds`，该值与默认值取 `max`。

### 2.3 Worker：resume 兜底重放（补充）
- 每个 tick 会 `requeue_expired_resume_ledgers`：将 lease 过期的 `state.turn_resume_ledger` 条目从 `processing` 回退为 `pending`（可重试）。
- `reconcile` 会基于 resume ledger / 已收 `tool.result` 重建 `resume_data`，把恢复消息继续推入 worker 内部处理队列。

### 2.4 PMO：`dispatched` 长期未处理重试
- 条件：`status='dispatched'` 且 `updated_at < now - dispatched_retry_seconds`。
- 动作：重构造 turn envelope 并写入 execution queue（`enqueue`），然后发 wakeup；保持 `status='dispatched'` 不变，属于门铃补发。

### 2.5 PMO：`dispatched` 超时回收为终态
- 条件：`status='dispatched'` 且 `updated_at < now - dispatched_timeout_seconds`。
- 动作：
  - `finish_turn_idle(expect_status='dispatched', bump_epoch=True)`：清 `active_agent_turn_id`、`active_channel_id`，`turn_epoch += 1`。
  - `emit_agent_state`：`status='idle'`、`error='dispatch_timeout'`（便于链路观测）。
  - `record_force_termination(reason='dispatch_timeout')`。
  - 最佳努力创建 `task.deliverable` 兜底卡并在 `evt.agent.*.task` 返回 `deliverable_card_id`。
  - 最终事件 `evt.agent.*.task` `status=timeout`，`error='dispatch_timeout'`。
  - 触发 `dispatch_next` 以推进下一个 pending 入队。

### 2.6 PMO：`running` 超时回收为终态
- 条件：`status='running'` 且 `updated_at < now - active_reap_seconds`（当前实现仅按 `running` 触发）。
- 动作：
  - `finish_turn_idle(bump_epoch=True)`，清空正在运行状态；
  - `emit_agent_state`：`status='idle'`、`error='timeout_reaped_by_watchdog'`；
  - `record_force_termination(reason='timeout_reaped_by_watchdog')`；
  - 最佳努力创建兜底 `task.deliverable`；
  - 最终事件 `evt.agent.*.task` `status=failed`，`error='timeout_reaped_by_watchdog'`。
  - 触发 `dispatch_next` 以推进下一个 pending 入队。

### 2.7 PMO：`pending` inbox 门铃补齐
- 条件：`state.agent_inbox.status='pending'` 且 `created_at < now - pending_wakeup_seconds`。
- 动作：发 `cmd.agent.<target>.wakeup`，不改 inbox 状态；仅起门铃作用。
- 额外：若无法确定 `channel_id` 且等待时间达到 `pending_wakeup_skip_seconds`，会打上 `watchdog_error='missing_channel'`、`watchdog_at`，并将该 inbox 标记为 `skipped`。

## 3. 产出契约（必须满足）
- PMO 侧 watchdog 终态收敛（`dispatch_timeout`、`timeout_reaped_by_watchdog`）需 best-effort 追加 `task.deliverable`（若 `output_box_id` 不存在则可能无法落盘）。
- `emit_agent_task` 最终事件应尽量返回 `deliverable_card_id`，用于重放/可观测。
- 回收动作应 `bump turn_epoch`，让旧状态自然失效，避免 side-effect 回放。
- worker watchdog 侧不直接生产 `evt.agent.*.task`；它只注入恢复消息与 heartbeat，最终终态仍由正常交付路径发出。

## 4. 配置项（默认值来源）
- PMO（`services/pmo/service.py` / `services/pmo/l0_guard/guard.py`）：
  - `pmo.watchdog_interval_seconds`
  - `pmo.dispatched_retry_seconds`
  - `pmo.dispatched_timeout_seconds`
  - `pmo.pending_wakeup_seconds`
  - `pmo.pending_wakeup_skip_seconds`
  - `pmo.active_reap_seconds`
- Worker（`services/agent_worker/loop.py`）：
  - `worker.inbox_processing_timeout_seconds`
  - `worker.watchdog_interval_seconds`
  - `worker.suspend_timeout_seconds`
- 工具可覆盖：`options.suspend_timeout_seconds` / `options.timeout_seconds`

## 5. 设计约束与边界
- NATS 只保留门铃语义；所有语义和状态收敛必须通过 Inbox/State。
- 兜底机制不是“零丢失”保证，但应保证“终态可达”与“可观测回放”。
- 兜底机制不替代扩容策略，只补齐可观测失败路径。
