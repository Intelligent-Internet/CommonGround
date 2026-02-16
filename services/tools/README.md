# Tool Services

本目录包含与 UTP 对齐的工具服务示例，便于本地联调与端到端回归。

- `mock_search.py`：监听 `cg.{protocol}.*.*.cmd.tool.search.*`，通过 `tool_call_card_id` 读取 `tool.call` 参数，写入 `tool.result` 卡并通过 **Inbox + Wakeup** 回传（`agent_inbox` + `cmd.agent.{worker_target}.wakeup`）。默认会尝试调用 LLM 生成 3 条“拟真搜索结果”（失败则回退到内置静态结果），用于验证 Worker 的工具派发、回调与后续 Agent 反馈。
- `openapi_service.py`：监听 `cg.{protocol}.*.*.cmd.tool.<target>.*`，按 `resource.tools.options.jina` 调用 Jina Search/Reader（`https://s.jina.ai/docs` / `https://r.jina.ai/docs`），并回传 `tool.result`。
- `skills_service.py`：监听 `cg.{protocol}.*.*.cmd.tool.skills.*`，提供 `skills.load`/`skills.activate`/`skills.run_cmd`/`skills.run_cmd_async`/`skills.run_service`/`skills.start_job`/`skills.job_status`/`skills.job_watch`/`skills.job_cancel`/`skills.task_status`/`skills.task_cancel`/`skills.task_watch` 并回传 `tool.result`。
- `word_count.py`：监听 `cg.{protocol}.*.*.cmd.tool.word_count.*`，提供确定性的 `map/reduce` 词频统计（用于 `fork_join` word-count demo）。
- `sandbox_reaper.py`：周期回收过期 E2B sandbox（基于 `resource.sandboxes`）。

运行示例（使用仓库根的 `config.toml`）：

```bash
uv run -m services.tools.mock_search
```

```bash
uv run services/tools/openapi_service.py --target-service jina
```

```bash
uv run -m services.tools.skills_service
```

```bash
uv run -m services.tools.word_count
```

```bash
uv run -m services.tools.sandbox_reaper
```

保持与 `resource.tools` 中的 `target_subject`/`after_execution` 定义一致，必要时调整数据库中的工具定义。
如需覆盖 **有效** `after_execution`，在 `tool.result.content.result.__cg_control.after_execution` 写入 `suspend|terminate`（保留命名空间）。

框架抽象说明：
- `ToolCallContext` 与 `ToolResultBuilder` 统一放在 `infra/tool_executor/`。
- `services/tools/tool_helpers.py` 提供 L2 便捷封装：`build_tool_result_package`、`extract_tool_call_args`、`extract_tool_call_metadata`。
- 上述便捷封装内部仍委托 `infra/tool_executor`，保证行为与内核实现一致。
- `ToolRunner` 在执行前会自动 hydration `tool.call`，业务工具直接使用 `ctx.args`。
- 业务层只返回业务结果或抛异常；`tool.result` 成功/失败封装由框架统一处理。

可选环境变量（调试）：
- `MOCK_SEARCH_USE_LLM=0`：禁用 LLM 生成，强制使用内置静态结果
- `MOCK_SEARCH_LLM_MODEL=gemini/gemini-2.5-flash`：指定模型名
- `MOCK_SEARCH_LLM_PROVIDER=gemini`：指定 LiteLLM provider（默认 gemini）
- `JINA_API_KEY`：`openapi_service.py` 访问 Jina Search/Reader 的鉴权密钥（默认读取该变量）
