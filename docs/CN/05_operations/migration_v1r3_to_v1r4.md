# v1r3 到 v1r4 迁移指南

适用范围：从 `v1r3` 升级到 `v1r4` 的 CommonGround 部署与集成方。

> 历史硬切说明：本文只适用于存量 `v1r3` 部署升级。如果你是从零开始使用最终版 `V1R4`，请优先阅读 `v1r4_release_notes.md` 和当前 quick start 文档。

## 1. 核心变化

1. 协议版本升级到 `v1r4`，NATS subject 前缀变为 `cg.v1r4.*`。
2. `CGContext` 成为控制面的唯一真源（ctx-only 方向）。
3. ingress 硬切策略：`cmd.*` 与 replay 对 payload 控制字段直接拒绝；`evt.*` 保留清洗。
4. UI action / PMO ingress 对 CG 头部校验更严格。
5. Gemini 参数从 `thinking_config` 迁移到 `reasoning_effort`。

## 2. 迁移前检查

1. 确认所有服务读到同一份协议配置（`config.toml` / `config.docker.toml`）。
2. 确认无遗留硬编码 subject（`cg.v1r3`）。
3. 确认外部调用方（脚本/API 网关）可发送完整 `CG-*` headers。

## 3. 执行步骤

### 步骤 A：切协议版本

把配置中的协议版本改为 `v1r4`，并重启服务。

### 步骤 B：更新工具 subject

1. 更新 `resource.tools.target_subject` 到 `cg.v1r4...`。
2. 重新上传 `examples/tools/*.yaml`（如有自定义工具也要同步）。

### 步骤 C：发布端改为 Context 出头

统一改为：

```python
headers = ctx.to_nats_headers()
await nats.publish_event(subject, payload, headers=headers)
```

不要再手工拼 `CG-Agent-Id` / `CG-Turn-Epoch` / `trace_id`。

### 步骤 D：消费端使用严格 ingress/replay 构建器

统一在 ingress 使用：

- `infra.messaging.ingress.build_nats_ingress_context(...)`
- `infra.stores.context_hydration.build_replay_context(raw_payload=..., db_row=...)`

规则：

- `cmd.*`：payload 控制字段直接拒绝（`ProtocolViolationError`）。
- `evt.*`：payload 控制字段清洗。
- replay：payload 控制字段直接拒绝。

### 步骤 E：迁移 ctx-only 接口

重点把旧签名替换为新签名：

1. `ensure_agent_ready(...)` -> `target_ctx=CGContext(...)`
2. `DispatchRequest(...)` -> `target_agent_id=...`（必要时补 `target_channel_id=...`）
3. `ExecutionStore.list_inbox_by_correlation(...)` -> `ctx=...`
4. `StateStore.fetch(...)` -> `ctx=...`

### 步骤 F：Gemini 配置迁移

1. 删除旧 `thinking_config`。
2. 使用：

```toml
reasoning_effort = "low"
```

并同步 profile/yaml 配置。

## 4. 建议的硬切方式

如果允许硬切（不保留双写兼容）：

1. 停服务
2. 升级代码与配置
3. 重置/初始化数据库
4. 重新 seed
5. 启服务并回归

示例：

```bash
scripts/setup/cg_stack.sh up
UV_PROJECT_ENVIRONMENT=.venv_local uv run -m scripts.setup.seed --ensure-schema --project proj_mvp_001
```

## 5. 回归验证清单

至少验证：

1. `GET /health` 返回 200
2. quickstarts 可运行（建议）：
   - `demo_simple_principal_stream`
   - `demo_ui_action`
   - `demo_fork_join_word_count`
   - `demo_principal_fullflow`
   - `demo_principal_fullflow_api`
3. 关键路径无 `missing required CG-Agent-Id`
4. 无 `got an unexpected keyword argument 'project_id'` 之类旧签名报错

当前最终 `V1R4` 相对 `main` 的发布摘要与验证基线见 `v1r4_release_notes.md`。

## 6. 常见故障

1. **UI action 超时**：通常是请求未携带完整 CG headers。
2. **PMO/tool ingress 协议拒绝**：检查是否仍从 payload 传身份字段。
3. **脚本 TypeError**：说明脚本还在调用旧接口签名。
