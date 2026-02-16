# Skills 支持范围（Scope）

本文档定义当前技能（Skills）能力的范围、运行模式与集成方式。实现细节可参考 `docs/CN/02_building_agents/skills_reference/skills_impl.md` 与 `docs/CN/02_building_agents/skills_reference/skills_background_job_spec.md`。

## 目标
- 提供可复用的“技能包”执行能力，以标准化工具调用与执行环境。
- 支持同步/异步/多步骤执行，避免长任务 watchdog 超时。
- 支持本地隔离运行与远程 sandbox 运行两种模式。

## 非目标
- 本文档不覆盖具体技能内容设计与模板（另行定义 SKILL.md 规范）。
- 不覆盖技能上传的产品化 UI 流程（目前为手动上传）。

## 运行模式

### 1) Local 模式（已验证）
- 运行时：`anthropic-experimental/sandbox-runtime`
- 沙箱隔离：Linux 使用 `bubblewrap`，macOS 使用 `sandbox-exec`
- 适用场景：本地开发/调试、离线环境、低延迟执行
- 当前状态：用于本地开发与调试链路，依赖本机 `srt` 执行环境。

### 2) Remote 模式（预览）
- 运行时：E2B SDK
- 适用场景：多租户隔离、弹性扩缩、分布式执行
- 当前状态：用于隔离/远程隔离运行；支持 E2B 沙箱复用与 TTL 回收。

> 说明：模式由 `skills.mode` 配置决定（见 `services/tools/skills_service.py`）。

## 组件与依赖
- 工具服务：`services/tools/skills_service.py`
- Agent Profile：`examples/profiles/skill_orchestrator_agent.yaml`
- 技能存储与 API：`services/api/main.py`
- 参考实现文档：`docs/CN/02_building_agents/skills_reference/skills_impl.md`

## 关键能力范围

### 1) 技能发现与加载
- `agent_worker` 仅注入可用技能列表（`name/description`），并且会先按 `profile.allowed_tools` 过滤，`allowed_tools` 不包含的 `skills.*` 不会注入到 LLM 工具列表。
- 必须显式调用 `skills.load` 才会把内容产出为 `tool.result`，并进入上下文。

### 1.1) 权限与作用域边界
- 技能工具注册按 `project_id` 维度存储在 `resource.tools`（`cg.{protocol}.{project_id}...`）。
- `skills` 工具链路基于 subject 作用域：`cg.{protocol_version}.{project_id}.{channel_id}.cmd.tool.skills.<tool_name>`，不存在跨项目越界调用（`skills_service` 在处理入口直接按 subject 解析 `project_id`）。
- `UI Worker` 调用能力受 `tool options.ui_allowed` 控制；`skills.*` 当前在 `project_bootstrap` 中未设置 `ui_allowed`，因此默认不对 UI action 开放（仅通过 Agent/LLM profile 的工具白名单 `allowed_tools` 可用）。
- 同步命令执行前置还需工具定义存在（`tool.call`、`tool_call_card_id` 校验、`tool_name` 路由）。

### 2) 技能执行
- 短任务：`skills.run_cmd`
- 长任务：`skills.run_cmd_async`
- 持久服务：`skills.run_service`（本地模式 `local` 下可用；可配置 `expose_ports`）
- 多步顺序：`skills.start_job`
- 状态查询：`skills.task_status` / `skills.job_status`
- 观测与取消：`skills.task_watch` / `skills.job_watch` / `skills.task_cancel` / `skills.job_cancel`
- 激活控制：`skills.activate`（本地直接返回本地路径；远程可触发沙箱内 zip 下发）

### 3) 输出与文件规则
- `workdir` 影响输出文件路径解析。
- 产物可通过工具结果回传到 CardBox / GCS（详见实现文档）。

### 4) 生命周期（Task/Job）
- Task（`run_cmd_async` / `run_service` 背景执行）生命周期：
  - `queued -> starting -> running -> succeeded|failed|canceled`
- Job（`start_job`）生命周期：
  - `queued -> running -> (succeeded|failed|canceled)`
- 任务与作业状态会持久化到 `state.skill_task_heads` / `state.skill_job_heads`；`run_cmd` 为同步命令不产生 `task_id/job_id`。
- `task_watch` / `job_watch` 为轮询/心跳型观测，不改变被测对象状态；重复 watch 会通过 `dedupe_key` 做幂等保护。
- `*_cancel` 将状态置为 `canceled`，并尽力取消当前运行中的底层 process/task（`_cancel_task`/任务级取消）。

## 集成与启动要求
- 启动 `skills_service`：
  - `uv run -m services.tools.skills_service`
- Agent 使用 `examples/profiles/skill_orchestrator_agent.yaml`
- 需确保 NATS / Postgres / CardBox / GCS 可用（按 `docs/CN/02_building_agents/skills_reference/skills_impl.md`）

## 技能上传（当前方式）
目前为**手动上传**：
- API 入口：`services/api/main.py`（`/projects/{project_id}/skills:upload`）
- 可参考脚本：`examples/flows/skills_e2b_bootstrap.py`

## 版本与兼容性
- 运行模式切换需更新 `config.toml` 的 `skills.mode`。

## 风险与限制（当前阶段）
- `run_service` 依赖 `skills.mode=local`；`mode!=local` 会被拒绝。
- Remote 模式需要持续验证稳定性与兼容性。
- 任务/作业状态与快照会写入持久层，但真实执行句柄在进程内存中，服务重启可能导致底层句柄丢失；`reaper` 会回收残留会话/forwarder。
- `run_service + expose_ports` 依赖 `socat` 与 SRT 配置（`allow_unix_sockets/allow_all_unix_sockets/allow_write`），环境不满足会降级/失败。
- 长任务应优先使用异步/Job，避免 watchdog 超时。

## TODO / 预期扩展
- Remote 模式稳定性验证与正式发布前验证
- 技能上传/管理的 UI 流程
- reaper 配置项开放（扫描上限/节流等）
