# CommonGround HTTP 参考

本文档描述 `CommonGround/service/routes.py`、`CommonGround/service/projection/router.py` 以及 Python HTTP clients 当前实现的 HTTP surface。它是最小 integration reference，不替代完整 OpenAPI。

## Base URL 与版本前缀

默认本地 service URL：

```text
http://127.0.0.1:8000
```

Project-scoped service routes 当前使用 implementation prefix：

```text
/v3r1/projects/{project_id}
```

请把 `/v3r1` 理解为当前 preview service route prefix。本文档与 service implementation 定义 v3r1 的 active HTTP contract。

Health routes 不带 project scope：

- `GET /healthz`
- `GET /readyz`

## 认证

Agent-authenticated request 使用三个 header：

```http
X-CG-Project-Id: cg-demo
X-CG-Agent-Id: worker
Authorization: Bearer <agent_credential_token>
```

`X-CG-Project-Id` 与 `X-CG-Agent-Id` 声明 caller identity。`Authorization` 用 Agent credential token 证明这个身份。Service 会拒绝缺失、错配、过期、inactive 或无效 credential。

Python clients 在同时传入 `agent=AgentRef(...)` 和 `auth_token=...` 时会设置这些 header；CLI 解析 authenticated profile 后也会设置这些 header。

## 响应与错误形态

HTTP 成功响应直接返回 resource payload。不同于 CLI，HTTP service 不会把成功结果包成 `{ "ok": true, "result": ... }`。

示例：

```json
{
  "project_id": "cg-demo",
  "turn_id": "turn-123"
}
```

部分 mutation route 返回：

```json
{
  "ok": true
}
```

部分 claim acquisition route 在没有可用 Turn 时可以返回 JSON `null`。

Service-level handled error 使用：

```json
{
  "error": "ConflictError",
  "message": "human-readable detail"
}
```

常见 status mapping：

- `401`：`UnauthorizedError`
- `403`：`ForbiddenError`
- `404`：`NotFoundError`
- `409`：`ConflictError` 或 `FencingError`
- `422`：`InvariantError` 或 request validation failure
- `400`：其他 `KernelError`

FastAPI validation error 可能使用 FastAPI 默认 validation response shape。

## 共享请求模型

Agent reference：

```json
{
  "project_id": "cg-demo",
  "agent_id": "worker"
}
```

Turn reference：

```json
{
  "project_id": "cg-demo",
  "turn_id": "turn-123"
}
```

Claim token：

```json
{
  "project_id": "cg-demo",
  "turn_id": "turn-123",
  "agent_id": "worker",
  "token": "<claim_token>",
  "expires_at": "2026-05-14T00:00:00Z"
}
```

可选 operation metadata：

```json
{
  "note": "operator note",
  "reason": "operator_stop",
  "annotations": {}
}
```

可选 trace context：

```json
{
  "trace_id": "trace-001",
  "parent_trace_id": null,
  "recursion_depth": 0
}
```

## 路由类别

### 健康检查

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/healthz` | 轻量 liveness check。 |
| `GET` | `/readyz` | 带 backend 与 claim-reaper 设置的 readiness check。 |

### Admin Service Admission

这些 routes 属于 product-layer Admin Service surface，不属于 Kernel `/v3r1` authority surface。

| Method | Path | Purpose |
| --- | --- | --- |
| `POST` | `/admin/v1/projects/{project_id}/agent-credential-tokens:request` | 通过 Admin Service policy 请求 profile bootstrap 和一次性 AgentCredential material。 |
| `POST` | `/admin/v1/projects/{project_id}/agent-join-invites` | 创建 scoped Agent join invite。需要 Admin Service bearer auth。 |
| `POST` | `/admin/v1/agent-joins:redeem` | 用 join code 兑换 profile metadata 和一次性 AgentCredential material。不需要 Admin Service bearer auth。 |
| `POST` | `/admin/v1/byoa/join:redeem` | `/admin/v1/agent-joins:redeem` 的兼容 alias。 |

Join invite create response 只返回一次 join code：

```json
{
  "invite": {
    "invite_id": "aginv_...",
    "project_id": "cg-demo",
    "agent_id": "worker-1",
    "profile_kind": "byoa.conversation_worker.v1",
    "runtime_kind": "manual.shell.v1",
    "expires_at": "2026-05-16T00:00:00+00:00",
    "single_use": true
  },
  "join_code": "cgjoin_..."
}
```

Join redeem response 包含 profile metadata 和一次性 `agent_credential_token`。token 不会出现在 profile object 内。

### Agent 注册与凭证

| Method | Path | Purpose |
| --- | --- | --- |
| `POST` | `/v3r1/projects/{project_id}/agents:register` | 通过 trusted registration service actor 注册 Agent。 |
| `POST` | `/v3r1/projects/{project_id}/agents/{agent_id}/credentials:issue` | 签发 Agent credential token。需要 credential-issue grant。 |
| `POST` | `/v3r1/projects/{project_id}/agents/{agent_id}/credentials/{credential_id}:revoke` | 撤销 Agent credential。 |
| `GET` | `/v3r1/projects/{project_id}/agents/{agent_id}/credentials` | 列出 caller 自己的 Agent credential，或 credential admin 可见的 credential。 |

Register Agent request：

```json
{
  "spec": {
    "agent_id": "worker",
    "role": "worker",
    "description": "Example worker",
    "enabled": true,
    "accepts_work": true,
    "capacity": 1,
    "capabilities": [],
    "grants": [],
    "public_metadata": {}
  },
  "provenance": {
    "kind": "local_setup",
    "external_ref": "seed",
    "payload_hash": null
  }
}
```

Credential issue response 同时包含 credential summary 与一次性 token：

```json
{
  "credential": {
    "credential_id": "cred-123",
    "project_id": "cg-demo",
    "agent_id": "worker",
    "status": "active"
  },
  "token": "<agent_credential_token>"
}
```

### Agent 与管理

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/v3r1/projects/{project_id}/agents/{agent_id}` | 获取 Agent truth snapshot。 |
| `POST` | `/v3r1/projects/{project_id}/agents/{agent_id}/work-memory-reports` | 以该 Agent 身份提交 work-memory report manifest。 |
| `POST` | `/v3r1/projects/{project_id}/management/agents/{agent_id}:drain` | 让 Agent 停止接收新的 Turn。 |
| `POST` | `/v3r1/projects/{project_id}/management/agents/{agent_id}:resume` | 恢复 Agent 接收新的 Turn。 |
| `POST` | `/v3r1/projects/{project_id}/management/agents/{agent_id}:heartbeat-presence` | 更新 Agent presence。 |
| `PUT` | `/v3r1/projects/{project_id}/management/agents/{agent_id}/public-metadata` | 替换 Agent public metadata。 |

Drain/resume body：

```json
{
  "agent": {"project_id": "cg-demo", "agent_id": "worker"},
  "requested_by": {"project_id": "cg-demo", "agent_id": "operator"},
  "meta": null,
  "trace": null
}
```

Work-memory report body：

```json
{
  "kind": "agent_work_memory_report_manifest.v1",
  "request_id": "report-001",
  "summary": "What changed",
  "records": [
    {
      "role": "observation",
      "payload": {"text": "Observed fact"},
      "source_refs": []
    }
  ],
  "final_payload": null,
  "declared_project_id": "cg-demo",
  "declared_agent_id": "reporter"
}
```

Work-memory manifest 不能包含 `meta`。Ledger operation metadata 是可信 service-side context，不是 prompt-facing manifest content。

### Turn 与 Dispatch

| Method | Path | Purpose |
| --- | --- | --- |
| `POST` | `/v3r1/projects/{project_id}/turns:dispatch` | Dispatch root 或 child Turn。 |
| `GET` | `/v3r1/projects/{project_id}/turns/{turn_id}` | 获取 Turn snapshot。 |
| `GET` | `/v3r1/projects/{project_id}/turns/{turn_id}/context` | 使用 `after_turn_seq` 与 `limit` 获取 semantic context。 |
| `GET` | `/v3r1/projects/{project_id}/turns/{turn_id}/feed` | 使用 `after_ledger_seq` 与 `limit` 获取 Turn ledger feed。 |
| `GET` | `/v3r1/projects/{project_id}/agents/{agent_id}/feed` | 使用 `after_ledger_seq` 与 `limit` 获取 Agent ledger feed。 |
| `POST` | `/v3r1/projects/{project_id}/management/turns/{turn_id}:stop` | 请求 stop 一个 Turn。 |
| `POST` | `/v3r1/projects/{project_id}/turns/{turn_id}:resume` | 恢复 suspended Turn。 |

Root dispatch body：

```json
{
  "requested_by": {"project_id": "cg-demo", "agent_id": "requester"},
  "target_agent": {"project_id": "cg-demo", "agent_id": "worker"},
  "input": {"prompt": "Summarize the repo status"},
  "turn_kind": "turn.conversation.v1",
  "dispatch_key": "req-001",
  "authority": {
    "mode": "root_request",
    "request_id": "req-001"
  },
  "meta": null,
  "trace": null
}
```

Child dispatch 使用：

```json
{
  "authority": {
    "mode": "child_derivation",
    "parent_claim": {
      "project_id": "cg-demo",
      "turn_id": "parent-turn",
      "agent_id": "worker",
      "token": "<claim_token>",
      "expires_at": "2026-05-14T00:00:00Z"
    }
  }
}
```

### Claim 防护的 Worker 操作

| Method | Path | Purpose |
| --- | --- | --- |
| `POST` | `/v3r1/projects/{project_id}/agents/{agent_id}/claims:claim` | 为 Agent claim 下一个可用 Turn。 |
| `POST` | `/v3r1/projects/{project_id}/claims:renew` | Renew active claim。 |
| `POST` | `/v3r1/projects/{project_id}/claims:reconcile-expired` | Reconcile expired claims。 |
| `POST` | `/v3r1/projects/{project_id}/turns/{turn_id}/semantic-records` | 在 active claim 下追加 semantic record。 |
| `POST` | `/v3r1/projects/{project_id}/turns/{turn_id}:suspend` | Suspend claimed Turn。 |
| `POST` | `/v3r1/projects/{project_id}/turns/{turn_id}:finish` | Finish claimed Turn。 |

Claim body：

```json
{
  "agent": {"project_id": "cg-demo", "agent_id": "worker"},
  "meta": null,
  "trace": null
}
```

Append semantic record body：

```json
{
  "claim": {
    "project_id": "cg-demo",
    "turn_id": "turn-123",
    "agent_id": "worker",
    "token": "<claim_token>",
    "expires_at": "2026-05-14T00:00:00Z"
  },
  "payload": {"status": "working"},
  "role": "progress",
  "meta": null,
  "trace": null
}
```

Finish body：

```json
{
  "claim": {
    "project_id": "cg-demo",
    "turn_id": "turn-123",
    "agent_id": "worker",
    "token": "<claim_token>",
    "expires_at": "2026-05-14T00:00:00Z"
  },
  "outcome": "succeeded",
  "final_payload": {"result": "done"},
  "final_record_role": "deliverable",
  "meta": null,
  "trace": null
}
```

### Projection 路由

Projection routes 是只读 project observation surface。

| Method | Path | Query | Purpose |
| --- | --- | --- | --- |
| `GET` | `/v3r1/projects/{project_id}/projection/agents` | `agent_id`, `enabled_only`, `accepts_work_only`, `role`, `capability`, `limit` | 列出 projected Agent directory entries。 |
| `GET` | `/v3r1/projects/{project_id}/projection/turn-offers` | `turn_kind`, `agent_id`, `enabled_only`, `accepts_work_only`, `limit` | 列出 canonical Turn offers。 |
| `GET` | `/v3r1/projects/{project_id}/projection/turns` | `target_agent_id`, `turn_kind`, `state`, `outcome`, `stop_requested_only`, `limit` | 列出 projected Turns。 |
| `GET` | `/v3r1/projects/{project_id}/projection/turns/{turn_id}/lineage` | `limit` | 获取 Turn lineage。 |
| `GET` | `/v3r1/projects/{project_id}/projection/feed` | `after_ledger_seq`, `limit` | 获取 project feed。 |

Projection page response 使用：

```json
{
  "project_id": "cg-demo",
  "items": [],
  "limit": 100,
  "diagnostics": []
}
```

## CLI 与 Integration API

`cg` 适合本地 setup、operator workflow、调试和脚本化 demo。CLI 额外提供 config resolution、profile bootstrap、token-file handling 和一次性 JSON envelope。

Runtime integration 应使用 HTTP 或 Python clients：

- `HttpAgentClient` 映射到 Agent、Turn、claim、dispatch、credential 与 work-memory routes。
- `ProjectionHttpClient` 只映射 projection routes。

HTTP API 仍然由 service mediation 保护。它会在调用 kernel operation 前执行 Agent credential、read policy、write guard、claim fencing、grant 以及 project/path consistency 检查。
