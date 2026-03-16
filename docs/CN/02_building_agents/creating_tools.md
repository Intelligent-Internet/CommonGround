# 工具开发指南

本文面向工具实现者，说明如何接入 UTP 工具流，以及如何基于 OpenAPI 快速实现工具执行器。

## 1. 基本约束
- 工具执行端不写 `state.*`。
- 工具参数必须通过 `tool_call_card_id` 指针读取 `tool.call` 卡，不要直接消费 payload 中的 `args/arguments`（当前服务端会按协议拒绝）。
- 控制/路由身份字段（`agent_id/agent_turn_id/turn_epoch/tool_call_id/step_id/...`）必须来自 `CG-*` headers / `CGContext`，不能放在 payload。
- 回调 payload 只承载业务/结果字段（例如：`status/after_execution/tool_result_card_id`）。
- 如需覆盖 **有效** `after_execution`，请在 `tool.result.content.result.__cg_control.after_execution` 写入 `suspend|terminate`（保留命名空间）；回调字段仍需按原样携带（命令与回写中的 `after_execution`）。
- 回调必须以入站 `traceparent` 为父创建子 span，并注入新的 `traceparent`（`tracestate` 可选）。
- `tool.result` 的 `metadata.trace_id/parent_step_id/step_id` 必须**从 `tool.call` 卡的 metadata 原样复制**（不使用 payload 补写）。
- **内容结构要求**：`tool.call` 内容必须是 `ToolCallContent`，`tool.result` 内容必须是 `ToolResultContent`；不再接受任意 dict/文本作为内容。
- `cmd.tool.*` 必须携带控制头 `CG-Tool-Call-Id`；`ToolCommandPayload` 只承载业务字段（常见为 `tool_call_card_id/tool_name/after_execution`）。
- `publish_tool_result_report` 现在会校验结果卡指针，以及 `author_id/function_name/source_ctx` 的一致性；仅“知道 correlation_id” 已不足以回写成功。

权威协议见：`04_protocol_l0/nats_protocol.md`。

## 2. 最小实现流程（任何语言）
1) 按 `resource.tools` 中该工具的 `target_subject`（已展开 `{project_id}/{channel_id}`）订阅 `cmd.tool.*` 主题。
2) 解析命令，校验必需的 `CG-*` 控制头（尤其 `CG-Tool-Call-Id`）和 payload 业务字段（`tool_call_card_id`，通常还包括 `tool_name/after_execution`）。
3) 读取 `tool.call` 卡（`tool_call_card_id`） → 从 `ToolCallContent.arguments` 取参数。
4) 执行业务逻辑。
5) 构造 `tool.result` 卡，并构造回写 payload（仅业务字段，通常为 `after_execution/status/tool_result_card_id`）。
6) 调用 `publish_tool_result_report` 写 `tool_result` 到 L0 Inbox；L0 会按 `agent_id` 目标 agent 的 `worker_target` 发起 `cmd.agent.{target}.wakeup`。

说明：回调唤醒目标不固定为 `worker_generic`，应以被调用 Agent 当前 roster 中的 `worker_target` 为准。

## 3. Python 示例（最小骨架）

```python
import json

from core.subject import parse_subject
from core.utp_protocol import ToolCallContent
from core.utils import safe_str
from infra.l0.tool_reports import publish_tool_result_report
from infra.messaging.ingress import build_nats_ingress_context
from infra.tool_executor import ToolResultBuilder, ToolResultContext


async def handle_tool_command(msg, *, cardbox, nats, execution_store, resource_store=None, state_store=None):
    raw = json.loads(msg.data)
    parts = parse_subject(msg.subject)
    if parts is None:
        raise RuntimeError("invalid subject")

    ingress_ctx, cmd_data = build_nats_ingress_context(
        dict(msg.headers or {}),
        raw,
        parts,
    )
    tool_call_card_id = str(cmd_data["tool_call_card_id"])

    cards = await cardbox.get_cards([tool_call_card_id], project_id=ingress_ctx.project_id)
    if not cards:
        raise RuntimeError(f"tool_call_card not found: {tool_call_card_id}")
    tool_call_card = cards[0]
    args = {}
    if isinstance(tool_call_card.content, ToolCallContent):
        args = dict(tool_call_card.content.arguments or {})

    tool_call_meta = getattr(tool_call_card, "metadata", {}) or {}

    try:
        result = do_something(args)
        status = "success"
    except Exception as exc:
        result = {
            "error_code": "internal_error",
            "error_message": str(exc),
            "error": {"code": "internal_error", "message": str(exc)},
        }
        status = "failed"

    ctx = ToolResultContext.from_cmd_data(
        ctx=ingress_ctx,
        cmd_data=cmd_data,
        tool_call_meta=tool_call_meta,
    )
    payload, result_card = ToolResultBuilder(
        ctx,
        author_id="tool.example",
        function_name=safe_str(cmd_data.get("tool_name")) or "unknown",
    ).build(
        status=status,
        result=result,
    )
    await cardbox.save_card(result_card)

    source_ctx = ingress_ctx.evolve(
        agent_id="tool.example",
        agent_turn_id="",
        step_id=None,
        tool_call_id=None,
    )
    await publish_tool_result_report(
        nats=nats,
        execution_store=execution_store,
        cardbox=cardbox,
        resource_store=resource_store,
        state_store=state_store,
        source_ctx=source_ctx,
        target_ctx=ingress_ctx,
        payload=payload,
    )
    return {
        "tool_call_id": ingress_ctx.require_tool_call_id,
        "agent_turn_id": ingress_ctx.require_agent_turn_id,
        "turn_epoch": ingress_ctx.turn_epoch,
        "status": status,
    }
```

## 3.1 错误结构建议（统一化）
- 推荐使用 `core/errors.py` 中的 `CGError` 体系，并通过 `build_error_result_from_exception` 生成：
  - `result.error_code` / `result.error_message`
  - `error.code` / `error.message` / `error.detail`
  - 这样 UI/Worker/PMO 能统一解析错误与状态。

## 3.2 工具定义与注册（resource.tools）
- 建议优先使用管理 API 上传定义：
  - `POST /projects/{project_id}/tools`（YAML）
  - 字段：`tool_name / target_subject / after_execution / parameters / options / description`
- `ToolService` 对外部工具有约束：
  - `after_execution` 仅可为 `suspend` 或 `terminate`
  - `target_subject` 必须是 `cmd.tool.*`（不能是 `cmd.sys.*`）
  - 工具名不能是 PMO 内部工具名（`delegate_async` / `launch_principal` / `ask_expert` / `fork_join` / `provision_agent`）
- 工具定义实际落库字段在 `resource.tools`：`project_id, tool_name, parameters, target_subject, after_execution, options`。
- Worker 侧会按 `target_subject` 找到定义并把 `after_execution + tool_call_card_id + 参数` 写入 `cmd.tool.*` 命令；Tool Service 回写时按 `tool.call` 卡元数据注入 trace/step lineage。

## 4. OpenAPI 工具（Jina Search/Reader）

当前 `services/tools/openapi_service.py` 已迁移到 Jina API：
- Search 文档：`https://s.jina.ai/docs`
- Reader 文档：`https://r.jina.ai/docs`

推荐在 `resource.tools.options` 使用 `jina` 命名空间：

```json
{
  "args": {
    "defaults": {"session_id": "{agent_turn_id}"},
    "fixed": {"project_id": "{project_id}"}
  },
  "envs": {"api_key": "SEARCH_API_KEY"},
  "jina": {
    "mode": "search",
    "base_url": "https://s.jina.ai",
    "path": "/search",
    "method": "GET",
    "auth_env": "JINA_API_KEY"
  }
}
```

建议：
- `mode=search` 使用 `s.jina.ai`，`mode=reader` 使用 `r.jina.ai`。
- 鉴权密钥仅从环境变量读取（默认 `JINA_API_KEY`），禁止落盘到 cards/日志。
- `openapi_service` 仅允许访问 `s.jina.ai` / `r.jina.ai` 两个域名。

### 参数 defaults / fixed / envs
- `options.args.defaults`：仅当 LLM 未提供该参数时注入；显式传 `null` 视为已提供。
- `options.args.fixed`：强制覆盖并且不暴露给 LLM。
- `options.envs`：参数名 → 环境变量名；只在卡片中记录变量名，实际值由执行端运行时注入。
- 工具执行端可通过服务配置提供 `envs` 覆盖注册值（服务端优先）。

模板变量（暂时仅支持 ID 字段）：`{project_id}` / `{channel_id}` / `{agent_id}` / `{agent_turn_id}` /
`{step_id}` / `{tool_call_id}` / `{parent_step_id}` / `{trace_id}` / `{turn_epoch}` / `{context_box_id}`。

如果工具接受 `session_id`，请把它视为调用者可见的 alias。运行时内部可能会按 owner（`project_id + agent_id`）重新作用域化底层执行身份，因此同名 alias 不是跨 agent 共享契约。
