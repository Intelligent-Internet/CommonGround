# Migration Guide: v1r3 to v1r4

Scope: upgrading CommonGround deployments/integrations from `v1r3` to `v1r4`.

> Historical hard-cut note: this document is only for upgrading existing `v1r3` deployments. If you are starting fresh on the final `V1R4` release, start from `v1r4_release_notes.md` and the current quick start docs instead.

## 1. What Changed

1. Protocol version is now `v1r4` (`cg.v1r4.*` subjects).
2. `CGContext` is the single control-plane source of truth (ctx-only direction).
3. Ingress hard-cut guard: `cmd.*` / replay reject payload control fields; `evt.*` keeps sanitize-only behavior.
4. UI action / PMO ingress enforces stricter CG header validation.
5. Gemini config moved from `thinking_config` to `reasoning_effort`.

## 2. Pre-Flight Checks

1. All services read the same protocol config (`config.toml` / `config.docker.toml`).
2. No hardcoded `cg.v1r3` subjects remain.
3. External publishers can send complete `CG-*` headers.

## 3. Migration Steps

### Step A: Bump Protocol Version

Set protocol version to `v1r4`, then restart services.

### Step B: Update Tool Subjects

1. Update `resource.tools.target_subject` to `cg.v1r4...`.
2. Re-upload `examples/tools/*.yaml` (plus your custom tools).

### Step C: Publisher Uses Context Headers

Use:

```python
headers = ctx.to_nats_headers()
await nats.publish_event(subject, payload, headers=headers)
```

Do not handcraft `CG-Agent-Id` / `CG-Turn-Epoch` / `trace_id`.

### Step D: Use Strict Ingress/Replay Builders

Use:

- `infra.messaging.ingress.build_nats_ingress_context(...)`
- `infra.stores.context_hydration.build_replay_context(raw_payload=..., db_row=...)`

Rules:

- `cmd.*`: payload control fields are rejected (`ProtocolViolationError`).
- `evt.*`: payload control fields are sanitized.
- replay: payload control fields are rejected.

### Step E: Migrate to ctx-only Signatures

Replace common legacy calls:

1. `ensure_agent_ready(...)` -> `target_ctx=CGContext(...)`
2. `DispatchRequest(...)` -> `target_agent_id=...` (plus `target_channel_id=...` when needed)
3. `ExecutionStore.list_inbox_by_correlation(...)` -> `ctx=...`
4. `StateStore.fetch(...)` -> `ctx=...`

### Step F: Gemini Config

1. Remove deprecated `thinking_config`.
2. Use:

```toml
reasoning_effort = "low"
```

Also migrate related profiles/yaml.

## 4. Recommended Hard Cutover

If you allow hard cutover (no dual-write compatibility):

1. Stop services
2. Upgrade code + config
3. Reset/init DB
4. Seed baseline data
5. Restart and run regression checks

Example:

```bash
scripts/setup/cg_stack.sh up
UV_PROJECT_ENVIRONMENT=.venv_local uv run -m scripts.setup.seed --ensure-schema --project proj_mvp_001
```

## 5. Regression Checklist

Minimum checks:

1. `GET /health` returns 200
2. quickstarts run successfully:
   - `demo_simple_principal_stream`
   - `demo_ui_action`
   - `demo_fork_join_word_count`
   - `demo_principal_fullflow`
   - `demo_principal_fullflow_api`
3. No `missing required CG-Agent-Id`
4. No legacy signature errors like `unexpected keyword argument 'project_id'`

For the final `V1R4` release summary and current validation baseline against `main`, see `v1r4_release_notes.md`.

## 6. Common Failure Modes

1. **UI action timeout**: usually missing required CG headers.
2. **PMO/tool ingress rejection**: publisher still sends identity via payload.
3. **Script TypeError**: script still calls legacy function signatures.
