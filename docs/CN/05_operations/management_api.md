# 管理 API 文档（按实现同步）

本文档以当前 `services/api/main.py` 为准。

## 1. 启动与基础

- 启动：`uv run -m services.api`
- 默认监听：`8099`
- 健康检查：`GET /health`
- 观测报告：`GET /projects/{project_id}/observability/report`
  - 查询参数：`since`、`until`（ISO8601，可选）、`jaeger`（默认 `http://jaeger:16686`）、`max_otel_traces`（0-20000，默认 2000）

## 2. 鉴权与权限说明

- 当前接口层未接入全局鉴权中间件（无 API Key / JWT / RBAC 注入）。
- 权限相关约束主要在服务层：
  - 读取类接口：直接返回 404/403/400 等业务错误（错误码与状态码在 `core/errors.py` 定义）。
  - Tool 管理接口会校验：
    - `tool_name` 不能是系统保留名：`delegate_async`、`launch_principal`、`ask_expert`、`fork_join`、`provision_agent`。
    - `after_execution` 仅允许 `suspend` / `terminate`。
    - `target_subject` 必须包含 `.cmd.tool.` 且不得包含 `.cmd.sys.`。
  - Skill 激活接口要求请求体带 `version_hash`。
  - 受控功能：
    - `CG_ENABLE_GOD_API` 未设置或为 false 时，`/projects/{project_id}/god/*` 接口返回 Not Found。
- 生产环境建议在部署层（反向代理/网关）补充身份鉴权。

## 3. 管理接口清单

## 3.1 通用资源

| 方法 | 路径 | 请求 | 响应 | 说明 |
| --- | --- | --- | --- | --- |
| GET | `/projects` | `limit`=`100`, `offset`=`0` | `list[Project]` | 项目列表 |
| POST | `/projects` | `ProjectCreate`（`ProjectCreate`） | `Project` | 新建项目 |
| GET | `/projects/{project_id}` | - | `Project` | 查询项目 |
| PATCH | `/projects/{project_id}` | `ProjectUpdate` | `Project` | 更新项目 |
| DELETE | `/projects/{project_id}` | - | 204 | 删除项目 |
| POST | `/projects/{project_id}/bootstrap` | - | `{"status":"ok"}` | 项目 bootstrap，202 |
| GET | `/projects/{project_id}/profiles` | `limit`=`200`, `offset`=`0` | `list[Profile]` | 列表 |
| POST | `/projects/{project_id}/profiles` | `multipart/form-data`, `file`（Profile YAML） | `Profile` | 上传/覆盖 profile |
| GET | `/projects/{project_id}/profiles/{name}` | - | `Profile` | 查询单个 |
| DELETE | `/projects/{project_id}/profiles/{name}` | - | 204 | 删除 |
| POST | `/projects/{project_id}/tools` | `multipart/form-data`, `file`（Tool YAML） | `Tool` | 上传 external tool（业务约束见 2） |
| GET | `/projects/{project_id}/tools` | `limit`=`200`, `offset`=`0` | `list[Tool]` | 列表 |
| GET | `/projects/{project_id}/tools/{tool_name}` | - | `Tool` | 查询单个 |
| DELETE | `/projects/{project_id}/tools/{tool_name}` | - | 204 | 删除 |

## 3.2 Skill 与 Artifact

| 方法 | 路径 | 请求 | 响应 | 说明 |
| --- | --- | --- | --- | --- |
| POST | `/projects/{project_id}/skills:upload` | `multipart/form-data`, `file`（Skill zip） | `SkillUploadResponse` | 上传并激活版本 |
| GET | `/projects/{project_id}/skills` | `limit`=`200`, `offset`=`0` | `list[Skill]` | 列表 |
| GET | `/projects/{project_id}/skills/{skill_name}` | - | `Skill` | 查询单个 |
| PATCH | `/projects/{project_id}/skills/{skill_name}` | `SkillPatch` | `Skill` | 更新 `enabled` |
| POST | `/projects/{project_id}/skills/{skill_name}:activate` | `dict` with `version_hash` | `Skill` | 激活指定版本 |
| DELETE | `/projects/{project_id}/skills/{skill_name}` | - | 204 | 删除 |
| POST | `/projects/{project_id}/artifacts:upload` | `multipart/form-data`, `file` | `ArtifactUploadResponse` | 上传 artifact |
| GET | `/projects/{project_id}/artifacts/{artifact_id}` | `mode`=`json|redirect`, `expires_seconds`=`600`（60-86400） | `ArtifactSignedUrlResponse` 或 307 Redirect | `mode=redirect` 时返回重定向 |

## 3.3 Agent 与状态观测

| 方法 | 路径 | 请求 | 响应 | 说明 |
| --- | --- | --- | --- | --- |
| GET | `/projects/{project_id}/state/agents` | `channel_id`(可选), `limit`(默认 50) | `list[AgentStateHead]` | 状态头 |
| GET | `/projects/{project_id}/agents` | `limit`=`200`, `offset`=`0`, `worker_target`, `tag`（可选） | `list[AgentRoster]` | 资源层 roster 列表 |
| GET | `/projects/{project_id}/agents/{agent_id}` | - | `AgentRoster` | 查询 roster |
| POST | `/projects/{project_id}/agents` | `AgentRosterUpsert` | `AgentRoster` | 新增/更新（可 init state） |
| GET | `/projects/{project_id}/state/steps` | `channel_id`, `agent_id`, `agent_turn_id`, `step_id`, `parent_step_id`, `trace_id`, `status`, `started_after`, `started_before`, `cursor`, `order`(`asc|desc`), `limit`(80) | `list[AgentStepRow]` | 步骤查询 |
| GET | `/projects/{project_id}/state/execution/edges` | `channel_id`, `agent_id`, `primitive`, `edge_phase`, `trace_id`, `parent_step_id`, `correlation_id`, `limit`(200) | `list[ExecutionEdgeRow]` | 事件边 |
| GET | `/projects/{project_id}/state/identity/edges` | `channel_id`, `agent_id`, `owner`, `worker_target`, `tag`, `action`, `trace_id`, `parent_step_id`, `limit`(200) | `list[IdentityEdgeRow]` | 身份边 |

## 3.4 CardBox 访问

| 方法 | 路径 | 请求 | 响应 | 说明 |
| --- | --- | --- | --- | --- |
| GET | `/projects/{project_id}/boxes/{box_id}` | - | `CardBoxDTO` | 查询单个 box |
| POST | `/projects/{project_id}/boxes/batch` | `BatchBoxesRequest` | `BatchBoxesResponse` | 批量查询 box |
| GET | `/projects/{project_id}/boxes/{box_id}/cards` | `limit`（默认 200） | `list[CardDTO]` | 查询 box 内容 |
| GET | `/projects/{project_id}/cards/{card_id}` | - | `CardDTO` | 查询单张卡片 |
| POST | `/projects/{project_id}/cards/batch` | `BatchCardsRequest` | `BatchCardsResponse` | 批量查询卡片 |

## 3.5 God 运维控制（受开关控制）

| 方法 | 路径 | 请求 | 响应 | 说明 |
| --- | --- | --- | --- | --- |
| POST | `/projects/{project_id}/god/pmo/{tool_name}:call` | `PMOToolCallRequest` | `PMOToolCallResponse` | 调度 PMO internal tool |
| POST | `/projects/{project_id}/god/stop` | `GodStopRequest` | `GodStopResponse` | 停止 agent turn |

## 4. 与实现不一致点（已修订）

- 旧文档只覆盖 `projects / profiles / tools`，遗漏了 Skill、Artifact、Agent/State、CardBox、God、观测相关接口。
- 旧文档中仅示例性描述了工具约束，未明确 `cmd.sys` 及保留名校验。
- 旧文档未说明状态码细节（201/202/204）与请求结构（multipart/分页参数）。

## 5. 代码依据

- 路由与状态码：`services/api/main.py`
- 请求体与响应模型：
  - `services/api/models.py`
  - `services/api/profile_models.py`
  - `services/api/tool_models.py`
  - `services/api/skill_models.py`
  - `services/api/artifact_models.py`
  - `services/api/ground_control_models.py`
- Tool 名称与 subject 鉴权逻辑：`services/api/tool_service.py`
- God 开关逻辑：`services/api/main.py` 中 `_is_god_api_enabled` 与两个 `god/*` 接口
