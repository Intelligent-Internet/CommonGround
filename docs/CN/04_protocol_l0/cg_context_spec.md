# CGContext 协议规范（v1r4）

本文定义 CommonGround 在 `v1r4` 中对 `CGContext` 的统一约束，用于确保控制流（Control Plane）与业务数据（Data Plane）彻底分离。

这就是 `v1` 系列最终定版的 control-plane contract，不再是迁移中的中间草案。

## 1. 核心定位

`CGContext` 是系统边界内（in-boundary）的唯一控制面事实来源（Single Source of Truth）。

- Data Plane：`payload` 只承载业务语义（参数、结果、内容）。
- Control Plane：`CGContext` 承载身份、路由、生命周期、血缘与追踪。
- 边界适配层（例如 API/God API）可以接收散装输入，但进入内核后必须转为 `ctx-only`。

## 2. 三个正交维度

### 2.1 Identity & Routing

- `project_id`, `channel_id`, `agent_id`
- 决定 NATS subject 路由和租户隔离。

### 2.2 State & Lifecycle

- `agent_turn_id`, `turn_epoch`, `step_id`, `tool_call_id`
- 与 DB CAS 约束绑定，保证幂等和乱序防护。

### 2.3 Lineage & Tracing

- `trace_id`, `recursion_depth`, `parent_agent_id`, `parent_agent_turn_id`, `parent_step_id`
- 用于跨 Agent 链路追踪与递归深度保护。

## 3. 标准桥接路径

### 3.1 Ingress

使用 `build_nats_ingress_context(...)` 构建上下文。

- 从 Subject + Headers 还原控制面字段。
- 对 payload 执行清洗（剥离控制字段，防伪造/越权注入）。

### 3.2 Egress

发布消息时统一使用：

```python
headers = ctx.to_nats_headers()
```

禁止业务层手工拼装 `CG-*` 头。

### 3.3 Replay

重放/唤醒场景使用 `build_replay_context(...)`，从 DB 行恢复 `CGContext`。

## 4. 不可变与演化

`CGContext` 为不可变对象（frozen dataclass），所有状态更新通过派生 API 完成：

- `with_turn(...)`
- `with_new_step(...)`
- `with_tool_call(...)`
- `with_bumped_epoch(...)`

禁止原地修改或“拆包再组包”式透传。

Spawn 的血缘/深度派生不再通过 L1/L2 的 `CGContext` helper 完成；
统一由 L0 的 `SpawnIntent` + `DepthPolicy` 强制约束。

## 5. Recursion Depth 语义（v1r4）

`v1r4` 明确了深度追踪规则：

1. 请求侧（request path）：
   - `enqueue` / `join_request` 以 `ctx.recursion_depth` 为准。
2. 响应侧（response path）：
   - `report` / `join_response` 按 `correlation_id` 继承并校验请求深度。
3. 超限保护：
   - 受 `MAX_RECURSION_DEPTH` 约束，超限应走协议拒绝/失败路径。

## 6. 例外与边界策略

以下仅允许存在于边界适配层，不可进入内核通路：

- 外部输入兼容参数（`project_id/agent_id/trace_id/...`）
- 旧客户端 fallback 逻辑

进入内核后统一收敛为 `ctx-only`。

## 7. 开发铁律

1. Never trust payload control fields.
2. 内核层函数优先签名：`(..., ctx: CGContext, ...)`。
3. 发布时只用 `ctx.to_nats_headers()`。
4. 恢复时只用标准 bridge，不手工拼控制字段。

## 8. 历史升级检查（`v1r3` -> `v1r4`）

1. 协议版本切到 `v1r4`。
2. 工具 subject 更新为 `cg.v1r4...`。
3. 清理 payload 中控制字段依赖。
4. 检查边界适配层是否仍泄漏散装参数到内核。
5. 验证 depth 语义：请求侧取 ctx，响应侧按 correlation 对齐。

## 9. 验收建议

最小验收集：

- `integration/test_report_depth.py`
- `integration/test_state_edges.py`
- `integration/test_ui_action_flow.py`
- `integration/test_submit_result_inbox_guard.py`

观测验收：

- 在 Jaeger/Tempo 中确认 delegate/fork_join/tool_result 三条链路可正确串联父子关系。
