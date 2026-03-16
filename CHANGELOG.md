# Changelog

## Unreleased

### Breaking Changes

- God API `POST /projects/{project_id}/god/stop` removed `turn_epoch` from `GodStopRequest`.
  - New contract: `stop` targets the specified `agent_turn_id` turn without epoch-exact matching.
  - Clients must stop sending `turn_epoch` in stop requests.

## v1r4 (2026-02-25)

Migration Guide:
- CN: `docs/CN/05_operations/migration_v1r3_to_v1r4.md`
- EN: `docs/EN/05_operations/migration_v1r3_to_v1r4.md`

### Breaking Changes

- Protocol version bumped from `v1r3` to `v1r4`.
  - NATS subjects/streams now use `cg.v1r4.*`.
  - Update tool `target_subject` definitions and any hardcoded subjects accordingly.

- CGContext is now the single control-plane source of truth.
  - Identity/routing/trace fields must come from subject + headers + context, not payload fallback.
  - Internal flows are now ctx-first (`ctx-only`) across L0/L1/L2 hotspots.

- UI action / ingress strictness increased.
  - Missing required CG headers can now lead to protocol rejection.
  - Legacy payload-based identity fallback paths were removed.

### Protocol Contract Changes

- Payload sanitization is stricter:
  - The following control fields are no longer trusted from payload and are stripped in ingress processing:
    - `project_id`, `channel_id`, `agent_id`
    - `agent_turn_id`, `turn_epoch`, `step_id`
    - `trace_id`, `parent_agent_id`, `parent_agent_turn_id`, `parent_step_id`
    - `recursion_depth`

- Transport lineage now relies on `CGContext` + headers:
  - Use `ctx.to_nats_headers()` for publication.
  - Use `build_nats_ingress_context(...)` / `build_replay_context(...)` for reconstruction.

- Recursion depth semantics were clarified:
  - Request path (`enqueue` / `join_request`) now takes depth from `ctx.recursion_depth`.
  - Response path (`report` / `join_response`) inherits and validates depth by `correlation_id`.
  - This removes mixed source ambiguity across payload/header/scattered params.

- Boundary policy is explicit:
  - Compatibility/scattered inputs are allowed only at boundary adapters.
  - Kernel/internal flows are `ctx-only`.

### Documentation

- Added dedicated CGContext protocol specs:
  - `docs/CN/04_protocol_l0/cg_context_spec.md`
  - `docs/EN/04_protocol_l0/cg_context_spec.md`
- Updated docs examples from `v1r3` to `v1r4` in:
  - quick start pages
  - docker quickstart pages
  - external integration guides

### LLM/Gemini Compatibility Update

- Removed deprecated Gemini `extra_body.thinking_config` usage in live tests.
- Switched to `reasoning_effort` (now explicitly supported by `LLMConfig`).
- Default Gemini 3 profiles/examples were aligned to:
  - `reasoning_effort: "low"`

### Migration Checklist

1. Set protocol version to `v1r4` in config:
   - `config.toml` / `config.docker.toml`
2. Re-register tools with `cg.v1r4...` subjects:
   - DB `resource.tools.target_subject`
   - YAML under `examples/tools/*.yaml`
3. Ensure publishers include CG headers from context:
   - `headers = ctx.to_nats_headers()`
4. Remove payload identity assumptions in custom integrations.
5. For Gemini integrations, replace `thinking_config` with `reasoning_effort`.
