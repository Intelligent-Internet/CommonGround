# Management API Reference (Implementation-Synced)

This document is synchronized with the current `services/api/main.py`.

## 1. Startup and Basics

- Start command: `uv run -m services.api`
- Default listen port: `8099`
- Health check: `GET /health`
- Observability report: `GET /projects/{project_id}/observability/report`
  - Query params: `since`, `until` (ISO8601, optional), `jaeger` (default `http://jaeger:16686`), `max_otel_traces` (0-20000, default 2000)

## 2. Authentication and Permission Notes

- The API layer currently does not use global authentication middleware (no API Key / JWT / RBAC injection).
- Permission-related constraints are mostly enforced at the service layer:
  - Read-type interfaces: return business errors such as 404/403/400 directly (error and status codes are defined in `core/errors.py`).
  - Tool management interface checks:
    - `tool_name` cannot be reserved system names: `delegate_async`, `launch_principal`, `ask_expert`, `fork_join`, `provision_agent`.
    - `after_execution` only allows `suspend` / `terminate`.
    - `target_subject` must contain `.cmd.tool.` and must not contain `.cmd.sys.`.
  - Skill activation interface requires request body field `version_hash`.
  - Controlled features:
    - When `CG_ENABLE_GOD_API` is unset or false, `/projects/{project_id}/god/*` returns Not Found.
- For production, it is recommended to add authentication at the deployment layer (reverse proxy / gateway).

## 3. Management Endpoints

## 3.1 General Resources

| Method | Path | Request | Response | Description |
| --- | --- | --- | --- | --- |
| GET | `/projects` | `limit`=`100`, `offset`=`0` | `list[Project]` | Project list |
| POST | `/projects` | `ProjectCreate` (`ProjectCreate`) | `Project` | Create project |
| GET | `/projects/{project_id}` | - | `Project` | Get project |
| PATCH | `/projects/{project_id}` | `ProjectUpdate` | `Project` | Update project |
| DELETE | `/projects/{project_id}` | - | 204 | Delete project |
| POST | `/projects/{project_id}/bootstrap` | - | `{"status":"ok"}` | Project bootstrap, 202 |
| GET | `/projects/{project_id}/profiles` | `limit`=`200`, `offset`=`0` | `list[Profile]` | List |
| POST | `/projects/{project_id}/profiles` | `multipart/form-data`, `file` (Profile YAML) | `Profile` | Upload/overwrite profile |
| GET | `/projects/{project_id}/profiles/{name}` | - | `Profile` | Get one |
| DELETE | `/projects/{project_id}/profiles/{name}` | - | 204 | Delete |
| POST | `/projects/{project_id}/tools` | `multipart/form-data`, `file` (Tool YAML) | `Tool` | Upload external tool (business constraints see section 2) |
| GET | `/projects/{project_id}/tools` | `limit`=`200`, `offset`=`0` | `list[Tool]` | List |
| GET | `/projects/{project_id}/tools/{tool_name}` | - | `Tool` | Get one |
| DELETE | `/projects/{project_id}/tools/{tool_name}` | - | 204 | Delete |

## 3.2 Skill and Artifact

| Method | Path | Request | Response | Description |
| --- | --- | --- | --- | --- |
| POST | `/projects/{project_id}/skills:upload` | `multipart/form-data`, `file` (Skill zip) | `SkillUploadResponse` | Upload and activate version |
| GET | `/projects/{project_id}/skills` | `limit`=`200`, `offset`=`0` | `list[Skill]` | List |
| GET | `/projects/{project_id}/skills/{skill_name}` | - | `Skill` | Get one |
| PATCH | `/projects/{project_id}/skills/{skill_name}` | `SkillPatch` | `Skill` | Update `enabled` |
| POST | `/projects/{project_id}/skills/{skill_name}:activate` | `dict` with `version_hash` | `Skill` | Activate specified version |
| DELETE | `/projects/{project_id}/skills/{skill_name}` | - | 204 | Delete |
| POST | `/projects/{project_id}/artifacts:upload` | `multipart/form-data`, `file` | `ArtifactUploadResponse` | Upload artifact |
| GET | `/projects/{project_id}/artifacts/{artifact_id}` | `mode`=`json|redirect`, `expires_seconds`=`600` (`60`-`86400`) | `ArtifactSignedUrlResponse` or 307 Redirect | returns redirect when `mode=redirect` |

## 3.3 Agents and State Observation

| Method | Path | Request | Response | Description |
| --- | --- | --- | --- | --- |
| GET | `/projects/{project_id}/state/agents` | `channel_id` (optional), `limit` (default `50`) | `list[AgentStateHead]` | State heads |
| GET | `/projects/{project_id}/agents` | `limit`=`200`, `offset`=`0`, `worker_target`, `tag` (optional) | `list[AgentRoster]` | Roster list from resource layer |
| GET | `/projects/{project_id}/agents/{agent_id}` | - | `AgentRoster` | Get roster |
| POST | `/projects/{project_id}/agents` | `AgentRosterUpsert` | `AgentRoster` | Create/update (optionally init state) |
| GET | `/projects/{project_id}/state/steps` | `channel_id`, `agent_id`, `agent_turn_id`, `step_id`, `parent_step_id`, `trace_id`, `status`, `started_after`, `started_before`, `cursor`, `order` (`asc|desc`), `limit` (80) | `list[AgentStepRow]` | Step query |
| GET | `/projects/{project_id}/state/execution/edges` | `channel_id`, `agent_id`, `primitive`, `edge_phase`, `trace_id`, `parent_step_id`, `correlation_id`, `limit` (200) | `list[ExecutionEdgeRow]` | Event edges |
| GET | `/projects/{project_id}/state/identity/edges` | `channel_id`, `agent_id`, `owner`, `worker_target`, `tag`, `action`, `trace_id`, `parent_step_id`, `limit` (200) | `list[IdentityEdgeRow]` | Identity edges |

## 3.4 CardBox Access

| Method | Path | Request | Response | Description |
| --- | --- | --- | --- | --- |
| GET | `/projects/{project_id}/boxes/{box_id}` | - | `CardBoxDTO` | Get one box |
| POST | `/projects/{project_id}/boxes/batch` | `BatchBoxesRequest` | `BatchBoxesResponse` | Batch query boxes |
| GET | `/projects/{project_id}/boxes/{box_id}/cards` | `limit` (default 200) | `list[CardDTO]` | Query box contents |
| GET | `/projects/{project_id}/cards/{card_id}` | - | `CardDTO` | Get one card |
| POST | `/projects/{project_id}/cards/batch` | `BatchCardsRequest` | `BatchCardsResponse` | Batch query cards |

## 3.5 God Operations (Feature-Gated)

| Method | Path | Request | Response | Description |
| --- | --- | --- | --- | --- |
| POST | `/projects/{project_id}/god/pmo/{tool_name}:call` | `PMOToolCallRequest` | `PMOToolCallResponse` | Dispatch PMO internal tool |
| POST | `/projects/{project_id}/god/stop` | `GodStopRequest` | `GodStopResponse` | Stop agent turn |

## 4. Differences vs Previous Docs (Revised)

- The old doc only covered `projects / profiles / tools`, and missed Skill, Artifact, Agent/State, CardBox, God, and observability endpoints.
- The old doc only gave examples of tool constraints and did not explicitly state `cmd.sys` and reserved-name validation.
- The old doc did not explain status code details (201/202/204) or request structure (multipart / pagination parameters).

## 5. Implementation References

- Routes and status codes: `services/api/main.py`
- Request/response models:
  - `services/api/models.py`
  - `services/api/profile_models.py`
  - `services/api/tool_models.py`
  - `services/api/skill_models.py`
  - `services/api/artifact_models.py`
  - `services/api/ground_control_models.py`
- Tool naming and subject authorization logic: `services/api/tool_service.py`
- God API toggle logic: `_is_god_api_enabled` and two `god/*` endpoints in `services/api/main.py`
