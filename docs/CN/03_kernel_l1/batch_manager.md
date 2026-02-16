# 批处理编排引擎（Batch Orchestration）

本文描述 L1 内核层当前 `fork_join` 下的 BatchManager 行为，与代码实现保持一致（`services/pmo/internal_handlers/fork_join.py` + `services/pmo/l1_orchestrators/batch_manager.py`）。

## 1. 目标与边界
- 管理 fork 后的子任务并发派发、子任务事件聚合、超时回收与父调用方回写。
- 不负责业务层最终整合与外部持久化回写；`fork_join` 仅返回统一的 `tool.result` 载荷给调用方。
- 批管理语义在 `pmo_batch` 表中的元数据与任务状态驱动；状态变更以幂等方式落库。

## 2. `fork_join` 输入契约（当前实现）
当前 seed/代码采用内部 Pydantic 模型校验（`_ForkJoinArgs`）：
- `tasks`（必填，数组，至少 1 项）
  - `target_strategy`：`new | reuse | clone`
  - `target_ref`：字符串
  - `instruction`：字符串
  - `context_box_id`：可选字符串（可为空，仍会通过 context 打包）
- `fail_fast`（可选，boolean，默认 `false`）
- `deadline_seconds`（可选，number，必须 `> 0`）

注意：`retry_batch_id`、`retry_task_indexes` 已不在当前实现里，不再支持“历史 batch 重试”参数。

示例：
```json
{
  "tasks": [
    {
      "target_strategy": "new",
      "target_ref": "Associate_Search",
      "instruction": "检索近三个月 RAG 评测论文"
    },
    {
      "target_strategy": "reuse",
      "target_ref": "agent_search_2",
      "instruction": "整理可复现实验参数"
    }
  ],
  "fail_fast": true,
  "deadline_seconds": 300
}
```

变更提醒：
- 旧字段如 `agent_profile/profile_name/module_id/task_id/agent_id/provision/conversation_mode/context_mode` 不再是 `fork_join` 输入契约的一部分。
- `fork_join` 校验会拒绝不认识字段（Pydantic `extra="forbid"`）。

## 3. 分叉（fork）构建与派发流程
1. `ForkJoinHandler.handle` 解析并校验参数，获取父递归深度。
2. 对每个任务执行 `preview_target`：
   - `new`：校验目标 profile，并在 `materialize_target` 中新建/启动派生实例（`provision=True`）。
   - `reuse`：要求复用已存在 agent；同一 batch 内不允许重复 `target_ref`。
   - `clone`：基于源码 agent 的输出构建并新建派生实例（`provision=True`）。
3. `pack_context_with_instruction` 为每个 task 生成独立 `context_box_id`（携带当前任务指令与继承上下文）。
4. 调用 `BatchManager.create_batch` 生成：
   - `state.pmo_batches` 行（`status=running`）
   - 若干 `state.pmo_batch_tasks` 行（`status=pending`）
   - `task_index` 保存在每个 task 的 `metadata.task_index`，确保结果映射稳定，不依赖 DB 行顺序。
5. `activate_batch` 先向父侧 `join(request)` 打 ACK，再触发 `dispatch_pending_tasks`。

## 4. `dispatch_pending_tasks` 与派发重试
`dispatch_pending_tasks` 仅调度满足条件的任务：
- `status='pending'`
- `retryable=TRUE`
- `next_retry_at <= NOW()`
- 父 batch `status='running'`

每条任务派发步骤：
- 生成稳定 `agent_turn_id` 并通过 `claim_task_dispatched` 原子占用任务。
- `AgentDispatcher.dispatch` 返回：
  - `rejected` 或 `error`（且 `error_code != internal_error`）视为确定性失败，直接 `mark_task_dispatch_failed`（`retryable=FALSE`）并尝试收尾 batch。
  - `busy` / `error`（可重试）执行指数退避（2,4,8,16,30）重排至 pending；重试超过 5 次改为失败（`dispatch_retry_exhausted`）。
  - 成功且有 `agent_turn_id + turn_epoch` 时写入 `current_agent_turn_id/current_turn_epoch`。
  - 成功但未返回 epoch 时保持 `dispatched`，等待后续 ACK 重建。

并发限制：
- 每批 `dispatch_pending_tasks` 使用 `watchdog_limit` 和 `dispatch_concurrency` 双重限流并发派发。

## 5. 分叉回收（reconcile / 回填）
`BatchManager.reconcile_dispatched_tasks` 每 tick 执行两段逻辑：
- 2 秒后：扫描 `status='dispatched'` 且 `current_turn_epoch IS NULL` 的任务，读取 `agent_inbox`（`correlation_id = agent_turn_id`）补齐 epoch。
- 60 秒后：仍缺 epoch 的任务仅记录告警与元数据（`dispatch_warning_*`），必要时 `dispatch_next` 唤醒；若任务所属批是 `fail_fast` 或 `deadline` 已到且 inbox 未可见，则按 `downstream_unavailable` 直接失败该 task，触发批次收尾。

重要：实现明确注释“不要自动回滚重试”以避免 late ACK 重放导致重复。

## 6. 子任务结果与批次失败聚合
`handle_child_task_event` 按 `agent_turn_id` 反查 `task`，并：
- 读取 `deliverable_card_id`，提取 `result_fields` 或 `content` 作为 `digest`（作为 `summary`）；
- 如 `tool_result_card_id` 存在且 `structured_output=tool_result`，解析 tool result payload；
- 写入 `task.status` 为事件 `status`（合法映射到 batch task terminal set）；
- 调用 `_maybe_finalize_batch`。

## 7. 批次终态判定与回写父侧
`_maybe_finalize_batch` 的收尾策略：
- `fail_fast=True` 且任一任务进入失败集（`failed/canceled/timeout`）时立即：
  - 将 batch 标为 `failed`
  - `_terminate_children`（stop/cancel 能打到的子任务）
  - `abort_remaining_tasks`（pending/dispatched）
  - 向父侧回写 `tool_result`
- 所有任务都进入终态后，根据 terminal 集合判定 batch 状态：
  - 全 success → `success`
  - 部分 success，且含其他非全成功状态 → `partial`
  - 含失败但无 success → `failed`
  - 含超时但无 success/failed → `timeout`
  - 仅 `partial` → `partial`
- 然后向父侧 `join(response)` + 门铃 `tool_result`。

## 8. 超时回收（timeout）
`reap_timed_out_batches` 扫描 `deadline_at < NOW()` 的运行批次：
1. `claim_batch_terminal(final_status=timeout)` 成功则执行：
   - `_terminate_children`：对有 `turn_epoch` 的活跃子任务发送 stop；对仅有 inbox 的任务按状态 `queued` 做取消，或从 inbox 提取 epoch 后发 stop。
   - `abort_remaining_tasks`：将剩余 `pending/dispatched` 标记为 `canceled`
   - 父侧回写 `timeout`

## 9. 数据模型（Postgres）
实际 schema（见 `scripts/setup/init_db.sql`）关键字段：
- `state.pmo_batches`
  - `status`：`running/success/failed/timeout/partial`
  - `task_count`、`deadline_at`、`metadata`
- `state.pmo_batch_tasks`
  - `status`：`pending/dispatched/success/failed/canceled/timeout/partial`
  - `attempt_count`、`retryable`、`next_retry_at`
  - `current_agent_turn_id/current_turn_epoch`、`output_box_id`、`error_detail`、`metadata`（含 `task_index`、`result_view` 等）

## 10. 父调用方返回格式
batch 完成后返回（通过 `tool.result` 回写）：
```json
{
  "status": "success|partial|failed|timeout",
  "results": [
    {
      "task_index": 0,
      "status": "success|failed|timeout|canceled|partial",
      "summary": "...",
      "output_box_id": "box_xxx",
      "error": "..."
    }
  ]
}
```

说明：
- `summary` 来源于可解析的 `deliverable/result_fields`，取 `summary` 或前若干 `result_fields` 文本摘要。
- `output_box_id`、`summary`、`error` 均为可选；失败任务会尽量带错误码（如 `downstream_unavailable`、`dispatch_*`、`missing_deliverable_card_id`）。

