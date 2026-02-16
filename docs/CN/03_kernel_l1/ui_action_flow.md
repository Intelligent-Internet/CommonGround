# UI Action 内部流程（L1）

本文描述 `cmd.sys.ui.action` 的内部处理流程，作为 UI 入口与 LLM 回复链路的实现参考。该流程是 **L1 实现细节**，对外协议以 L0 文档为准。

---

## 0) UI 启动：注册 UI Agent（前置）

UI 会话创建时通过管理 API 注册 UI agent（`worker_target=ui_worker`，不参与 LLM/delegation），并初始化 `state.agent_state_head` 为 `idle`。

示例要点：
- `agent_id = ui_{user_uuid}_{session_uuid}`
- `worker_target = ui_worker`
- `tags = ["ui"]`
- `metadata.ui_user_id / ui_session_id`

---

## 1) UI → UI Worker（入口）

UI 通过 NATS 发布：
`cg.{ver}.{project_id}.{channel_id}.cmd.sys.ui.action`

载荷建议包含 `action_id` / `agent_id` / `tool_name` / `args` / `metadata`。
兼容说明：
- `agent_turn_id` / `turn_epoch` 可选携带；未携带会在处理环节按兼容路径处理。

---

## 2) UI Worker（L0/L1 校验 + 入队）

UI Worker 的关键步骤（`_handle_ui_action`）：
- subject/header 校验通过 `parse_subject` / `normalize_headers`。
- roster 校验：`agent_id` 存在且 `worker_target=ui_worker`。
- busy gate：`state` 存在且 `status != idle` 时直接返回 `busy`（不写 Inbox、不排队）。
- 幂等键：使用 `action_id`，可用 `message_id` 覆盖；用于 wakeup 阶段 idempotency-store 对账。
- 通过 `L0Engine.enqueue(message_type="ui_action", correlation_id=action_id, wakeup=True)` 入队，`L0` 为 `ui_action` 写入 `agent_turn_id / turn_epoch` 到 inbox。
- 同步返回 `accepted / busy / rejected / error`。
- `tool.options.ui_allowed` 不在该入口阶段校验；会到 wakeup 阶段再校验。

UI Worker 的 wakeup 阶段（`_process_ui_action`）：
- 首次缺失 `agent_turn_id` 时，回退兼容为 `turn_<action_id>`。
- `tool_definition` 必须存在，且 `tool.options.ui_allowed == true`（缺省拒绝）。
- 若 `state.status != idle` 且 `active_agent_turn_id != agent_turn_id`，返回 `busy`；允许同一 `agent_turn_id` 的继续处理。
- `idempotency key` 抢占与回读结果：避免并发重复执行。
- 通过 `state_store.update(expect_turn_epoch, expect_agent_turn_id=...)` 做 CAS；失败返回 `stale_turn/rejected`。
- `dispatch` 为异步工具时（如 `delegate_async`）会把状态写为 `suspended`，并 `expecting_correlation_id=action_id`。
- `dispatch` 立即失败时回退写入 `idle`，并回写失败 ack。

---

## 3) PMO Internal Tool：`delegate_async`

在 UI worker wakeup 阶段，`UTPDispatcher` 会：
- 写 `tool.call` 卡片；
- 通过 `cmd.sys.pmo.internal.delegate_async` 触发 PMO internal handler。

`DelegateAsyncHandler.handle` 的关键步骤：
- 校验 `target_strategy` / `target_ref` / `instruction`。
- 解析目标（reuse/new/clone）与 profile/agent，检查可派发性。
- 用 `pack_context_with_instruction` 构建 `context_box`，将 `instruction` 写入 context handover 语义。
- 通过 `dispatch_to_agent_transactional` + `AgentDispatcher` 发起 `message_type="turn"` 的 L0 enqueue，完成 target agent 的 `agent_turn_id/turn_epoch` 绑定并 publish wakeup。
- 成功返回 `accepted + agent_turn_id + turn_epoch + output_box_id`；失败通过错误码返回 `rejected/busy/error`，由 UI worker 继续回传 `evt.sys.ui.action_ack`。

---

## 4) 目标 Agent Worker → LLM

目标 agent 通过其 wakeup 处理 `turn`：
- 加载 profile/context，走模型输出；
- 写 deliverable 卡（assistant 回复）；
- 发布 `evt.agent.{agent_id}.task`（包含 `deliverable_card_id` / `output_box_id`）。

---

## 5) PMO → UI Worker（Tool Result 回流）

PMO 不直接发布“UI 回执状态”给 UI action；而是：
- `_reply_resume` 先构建 `tool.result` 卡片（含 `result/error/status/after_execution`）；
- `publish_tool_result_report` 执行 `L0.report(message_type="tool_result")` 写入目标 UI agent 的 inbox；
- L0 发布 `cmd.agent.<ui_worker>.wakeup`，UI worker wakeup 收到 `message_type=tool_result` 进入收尾处理。

---

## 6) UI Worker → UI（回执）

UI Worker 对 `tool.result`：
- 加载 `tool.result` 卡并 append 到 `output_box`；
- 更新 step 状态为 completed/failed；
- `finish_turn_idle` 并发布 `evt.agent_state`（idle）；
- 持久化 idempotent 结果，发布 `evt.sys.ui.action_ack`。
- 典型 ack 状态：`done`（成功） / `rejected`（失败） / `error` / `busy`（重放/并发保护）。

---

## 7) UI 回读最终回复

UI 通过 HTTP API 用 `deliverable_card_id` / `output_box_id` 拉取卡片内容并展示。

---

## 8) 手工验证（UI → Chat Agent）

前置：`services.api` / `services.pmo.service` / `services.agent_worker.loop` / `services.ui_worker.loop` 已启动。

1) 创建项目并上传 profile：
```bash
curl -sS -X POST http://127.0.0.1:8099/projects \
  -H 'Content-Type: application/json' \
  -d '{"project_id":"proj_ui_chat_01","title":"UI Chat Demo","owner_id":"user_demo","bootstrap":true}'

curl -sS -X POST http://127.0.0.1:8099/projects/proj_ui_chat_01/profiles \
  -F file=@examples/profiles/ui.yaml

curl -sS -X POST http://127.0.0.1:8099/projects/proj_ui_chat_01/profiles \
  -F file=@examples/profiles/chat_assistant.yaml
```

2) 注册 UI agent + 目标 chat agent：
```bash
curl -sS -X POST http://127.0.0.1:8099/projects/proj_ui_chat_01/agents \
  -H 'content-type: application/json' \
  -d '{
    "agent_id":"ui_user_01",
    "profile_name":"UI_Actor_Profile",
    "worker_target":"ui_worker",
    "tags":["ui"],
    "display_name":"UI Session",
    "owner_agent_id":"user_demo",
    "metadata":{"is_ui_agent":true},
    "init_state":true,
    "channel_id":"public"
  }'

curl -sS -X POST http://127.0.0.1:8099/projects/proj_ui_chat_01/agents \
  -H 'content-type: application/json' \
  -d '{
    "agent_id":"chat_agent_ui",
    "profile_name":"Chat_Assistant",
    "worker_target":"worker_generic",
    "tags":["partner"],
    "display_name":"Chat Agent",
    "owner_agent_id":"user_demo",
    "init_state":true,
    "channel_id":"public"
  }'
```

3) 订阅 ack 与 task：
```bash
nats sub "cg.v1r3.proj_ui_chat_01.public.evt.sys.ui.action_ack"
nats sub "cg.v1r3.proj_ui_chat_01.public.evt.agent.chat_agent_ui.task"
```

4) 发送 UI action：
```bash
uv run python - <<'PY'
import uuid6
print(uuid6.uuid7().hex)
print(uuid6.uuid7().hex)
PY
```
假设输出：
- action_id = `AAA`

```bash
nats pub "cg.v1r3.proj_ui_chat_01.public.cmd.sys.ui.action" '{
  "action_id":"AAA",
  "agent_id":"ui_user_01",
  "tool_name":"delegate_async",
  "args":{
    "target_strategy":"reuse",
    "target_ref":"chat_agent_ui",
    "instruction":"hello chat via ui"
  },
  "metadata":{"source":"manual","client_ts":"2025-12-28T00:00:00Z"}
}'
```

5) 期望结果：
- `evt.sys.ui.action_ack`：入口阶段可见 `status=accepted`，工具执行结束后可见 `status=done`（或失败场景 `status=rejected`）。
- `evt.agent.chat_agent_ui.task`：`status=success` 且包含 `deliverable_card_id`。

