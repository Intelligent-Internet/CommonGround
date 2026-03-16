# CGContext Protocol Spec (v1r4)

This document defines the `CGContext` contract in `v1r4`, so control-plane data is physically separated from business payload data.

This is the final control-plane contract for the `v1` line, not an intermediate migration draft.

## 1. Core Positioning

Inside the system boundary, `CGContext` is the single source of truth for the control plane.

- Data Plane: `payload` carries business semantics only.
- Control Plane: `CGContext` carries identity, routing, lifecycle, lineage, and tracing.
- Boundary adapters (for example API/God API) may accept legacy scattered inputs, but must convert to `ctx-only` before entering kernel paths.

## 2. Three Orthogonal Dimensions

### 2.1 Identity & Routing

- `project_id`, `channel_id`, `agent_id`
- Defines tenant isolation and NATS routing.

### 2.2 State & Lifecycle

- `agent_turn_id`, `turn_epoch`, `step_id`, `tool_call_id`
- Bound to DB CAS constraints for idempotency and ordering safety.

### 2.3 Lineage & Tracing

- `trace_id`, `recursion_depth`, `parent_agent_id`, `parent_agent_turn_id`, `parent_step_id`
- Supports cross-agent lineage and recursion safety.

## 3. Standard Bridge Paths

### 3.1 Ingress

Use `build_nats_ingress_context(...)`.

- Reconstructs control fields from Subject + Headers.
- Sanitizes payload by removing control-plane keys.

### 3.2 Egress

Always publish with:

```python
headers = ctx.to_nats_headers()
```

Do not manually compose `CG-*` headers in business code.

### 3.3 Replay

Use `build_replay_context(...)` to restore `CGContext` from DB-backed inbox/replay rows.

## 4. Immutability and Evolution

`CGContext` is immutable (frozen dataclass). State transitions must use evolution methods:

- `with_turn(...)`
- `with_new_step(...)`
- `with_tool_call(...)`
- `with_bumped_epoch(...)`

Avoid unpack-and-repack context flows.

Spawn lineage/depth derivation is no longer done via `CGContext` helper methods in L1/L2;
it is enforced in L0 via `SpawnIntent` + `DepthPolicy`.

## 5. Recursion Depth Semantics (v1r4)

`v1r4` makes depth semantics explicit:

1. Request path:
   - `enqueue` / `join_request` use `ctx.recursion_depth` as source of truth.
2. Response path:
   - `report` / `join_response` inherit and validate depth by `correlation_id`.
3. Safety:
   - Enforced by `MAX_RECURSION_DEPTH`.

## 6. Exceptions and Boundary Policy

The following are allowed only in boundary adapters, not kernel internals:

- Legacy compatibility fields (`project_id/agent_id/trace_id/...`)
- Old-client fallback behavior

Kernel internals must remain `ctx-only`.

## 7. Hard Rules

1. Never trust payload control fields.
2. Prefer kernel signatures with `ctx: CGContext`.
3. Use `ctx.to_nats_headers()` for all publications.
4. Use official bridge functions for reconstruction; do not handcraft control fields.

## 8. Historical Upgrade Checklist (`v1r3` -> `v1r4`)

1. Set protocol version to `v1r4`.
2. Update tool subjects to `cg.v1r4...`.
3. Remove payload-based control-field assumptions.
4. Ensure boundary adapters do not leak scattered inputs into kernel code.
5. Validate depth semantics: request-from-ctx, response-by-correlation.

## 9. Validation Recommendations

Minimum integration set:

- `integration/test_report_depth.py`
- `integration/test_state_edges.py`
- `integration/test_ui_action_flow.py`
- `integration/test_submit_result_inbox_guard.py`

Observability validation:

- Verify parent/child continuity for delegate/fork_join/tool_result chains in Jaeger/Tempo.
