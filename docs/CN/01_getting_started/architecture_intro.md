# 架构总览

本文描述系统的分层模型、组件边界与关键数据流，作为后续协议与实现文档的入口。

## 分层模型（认知视角）
- **L0 协议层（Physics）**：定义组件如何通信与不变量（Subject、Payload、幂等与门禁）。
- **L1 内核层（OS）**：系统内建实现机制（PMO/Worker/UI Worker/BatchManager）。
- **L2 生态层（User Space）**：可配置/可扩展能力（工具服务、项目配置、管理 API、技能/评测组件）。

对应文档入口：
- L0：`04_protocol_l0/`
- L1：`03_kernel_l1/`
- L2：`02_building_agents/`

## 组件与职责（按“离协议远近”）

### L0 协议与真源（最接近协议）
- **core/**：跨服务协议与常量（事件/Subject/DTO）。
- **infra/l0_engine.py**：L0 原语执行入口（`enqueue` / `report` / `join` / `wakeup`），负责 DB 交易式落库与 NATS 唤醒。
- **card-box-cg/**：Cards/Boxes 的事实真源与血缘存储。

### L0 实现基座（I/O 适配）
- **infra/**：NATS/PG/LLM/卡盒等 I/O 适配层，承载协议落地的最薄实现（`nats_client`、stores、`event_emitter` 等）。

### L1 内核实现（服务层）
- **services/agent_worker/**：执行 Turn、调用 LLM、推进 L0 状态机。
- **services/pmo/**：身份供给、执行编排（BatchManager）。
- **services/ui_worker/**：接收 `cmd.sys.ui.action`，派发 `ui_action` 到 L0 并通过 `cmd.agent.<worker_target>.wakeup` 拉起处理。

### L2 生态与扩展（最远离协议）
- **tool services（外部或内置）**：执行工具逻辑并回调 tool_result。
- **services/api/**：管理 API 与部分控制入口（例如 God API/启动/编排面）。
- **services/tools/**、**services/judge/**：内置工具与判定能力（tool service 生态的一部分）。

## 真源与读写边界
- **Cards/Boxes**：真源在 `card-box-cg`。
- **resource.***（配置资产）：真源在本仓 PG；由 PMO 与管理 API 写。
- **state.***（运行态/门禁）：真源在本仓 PG；由 PMO 与 Worker（含 UI Worker）写，PMO 负责调度与回收。
- **NATS**：用于控制面、任务控制命令与状态事件传播，不是事实真源；事实层状态由 PG 驱动。

## Boxes 的可见性（重要）
- **中心化存储 ≠ 默认可见**：Cards/Boxes 在物理存储上是项目内中心化的（CardBox/PG），不是“每个 Agent 各自一套存储”。
- **LLM 可见性由装配决定**：单个 Agent Turn 的 LLM 只会看到被装配进 `context_box_id` 的输入快照，以及本次 Turn 自己追加的 `output_box_id`（滚动输出）；不会自动读取项目内其他 box。
- **跨 Agent 共享需显式机制**：必须显式传递 `box_id` 并进行上下文装配/继承（当前主要由 PMO context packing 完成；未来也可能由工具完成）。
- 参考：`03_kernel_l1/pmo_orchestration.md`（PMO context packing / handover）

## 运行时核心数据流（Inbox + Wakeup）
1) **Enqueue/Report**：`L0Engine` 或其封装（如 `infra/l0/tool_reports.py`）将请求写入 `state.agent_inbox` 并记录 `execution_edges`。
2) **Wakeup**：L0 根据 `worker_target` 发布 `cmd.agent.{target}.wakeup`（`target` 为目标 agent 在 `resource.roster` 的 `worker_target`，例如 `worker_generic`）。
3) **Worker**：竞争锁 → 批量 claim inbox → 水合 `state/resource/cards` → 执行 LLM。
4) **Tool Call**：Worker 写 `tool.call` 卡，并按工具定义中的 `target_subject` 发布命令（通常是 `cmd.sys.pmo.internal.*` 或 `cmd.tool.*`）。
5) **Report**：工具回调写 `tool.result` 卡，通过 `infra/l0/tool_reports.py` 回流到 `L0Engine.report`，写入 `state.agent_inbox` 并再次触发 `cmd.agent.{target}.wakeup`。
6) **Finalize**：Worker 结束 Turn 时写/更新 `task.deliverable`，并发布 `evt.agent.*.task`。
> 约束：**一个 Turn 的成功/失败会汇聚为该 Turn 的一次 `evt.agent.*.task` 结果事件**；`BatchManager` 下发的是多个子 Turn（因此会产生多个 task 结果）。

## 图论视角（审计视图，不是编排）
- **Identity Graph**：结构上趋近无环（owner/derived_from 不应成环），用于描述身份关系变更。
- **Execution Graph**：高频执行链路审计视图，**可能存在环**（递归/回调链路），不作为调度输入。
- 结论：我们不引入用户可见的 DAG 编排；图仅用于可观测与审计。

## 系统稳定性与阻尼
参考 VSM 控制论模型，阻尼器负责消除子系统间的震荡并防止资源无限消耗（Entropy Damper）。
- **Global Recursion Depth Limiter**：
  - 目的：防止 Agent 之间形成无限递归调用链（Dead Loop）或过度消耗计算资源。
  - 机制：
    1.  通过 NATS Header `CG-Recursion-Depth` 传递调用深度（Entropy State），要求为非负整数字符串。
    2.  发布侧阻尼（Execution Gate）：L0 入站（如 `enqueue`、`report`）要求 `CG-Recursion-Depth` 非负整数；超阈值返回 `recursion_depth_exceeded`。
    3.  消费侧阻尼（Worker）：处理 Inbox 时校验深度；缺失/非法视为 `ProtocolViolationError` 并拒绝执行。
    4.  若 `depth >= threshold`，发布侧或消费侧都必须拒绝派单并终止调用链，避免资源无限消耗。
  - 失败语义：
    - 入库/入站阶段超限：`enqueue`/`report` 返回 `error_code=recursion_depth_exceeded`。
    - 运行期超限：Worker 会生成 `evt.agent.{agent_id}.task`，`status=failed` 且 `error_code=recursion_depth_exceeded`。
    - Header 缺失/非法：视为 `ProtocolViolationError`，在执行期通常转为 `error_code=protocol_violation` 的失败路径。
  - 透传与递增：
    - 当前 L0 原语默认 `enqueue_mode` 处理为 `"call"`；代码层并未保留 `transfer/notify` 模式。
    - 深度通常按调用关系“透传”；默认用户入口是 `0`。
    - 在需要显式派生子调用场景中，PMO 会手工加一，例如 `services/pmo/internal_handlers/context.py::bump_recursion_depth` 与 `services/pmo/l1_orchestrators/batch_manager.py::child_depth = parent_depth + 1`。
    - `Join/Report` 会通过 `correlation_id` 约束并继承原始深度；`tool_result`/`timeout` 路径保持一致。
  - 区分：该阈值用于“跨 Agent 调用链”稳定性，与单 Agent 内 `max_steps` 互补而非替代。

## 启动入口（核心运行）
- `uv run -m services.pmo.service`
- `uv run -m services.agent_worker.loop`
- `uv run -m services.ui_worker.loop`
- `uv run -m services.api`

## 跨仓库边界（CommonGround ↔ card-box-cg）
- **CommonGround**：控制流与门禁（L0/L1/部分L2）。
- **card-box-cg**：内容与血缘的事实真源，提供跨语言接入能力。

## 第三方接入建议
- **工具服务**：建议接入 NATS + CardStore API，遵循 UTP 回调与 `traceparent` 传播。
- **配置写入**：建议通过管理 API 写 `resource.*`，避免直接写库。

## 相关协议文档
- `04_protocol_l0/nats_protocol.md`
- `04_protocol_l0/state_machine.md`
- `04_protocol_l0/utp_tool_protocol.md`
