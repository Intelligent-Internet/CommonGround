# CommonGround V1R4 Release Notes

Date: 2026-03-12
Status: Final release of the `v1` line

## Summary

`V1R4` is the final stabilized release for `CommonGround v1`.

Compared with the pre-release `main` baseline, this branch is the complete `V1R4` release candidate: it includes the `v1r4` protocol cutover, the context/authority hard cut, the spawn/runtime cleanup, and the final stabilization fixes required to publish `v1`.

This release closes the remaining publish blockers from the post-migration period without over-investing in runtime pieces that are expected to change again in `v2`.

The release goal is:

- make the current `v1` runtime publishable and stable
- keep kernel contracts clean enough for `v2`
- avoid turning the current centralized worker/tool pipeline into a new long-term complexity center

## Compared with `main`

### 1. Protocol and context authority hard cut

- Protocol version is now `v1r4`, with `cg.v1r4.*` subjects as the release target.
- Control-plane identity is now treated as `CGContext`/header authority rather than payload authority.
- Migration docs, quickstarts, tools, and first-party examples have been aligned with the `v1r4` contract.

### 2. Runtime and topology cleanup

- Spawn-facing dispatch no longer accepts caller-owned child turn topology; L0 allocates the real child turn identity.
- `BatchManager` and PMO orchestration now track child work by the real `agent_turn_id` and avoid the old inbox/epoch archaeology path as the primary runtime truth.
- Child termination, downstream tracking, and wakeup flows continue to operate on the real L0 turn identity.

### 3. Final stabilization fixes for publish blockers

- `resume/tool_result` uses a minimal single-winner apply path instead of allowing obvious duplicate append/apply.
- Existing-agent `provision_agent/ensure` no longer allows rewriting `owner_agent_id`, `worker_target`, `tags`, or `profile_box_id`.
- Validation-failure context recovery no longer trusts payload lineage fields; it recovers only from ingress context and `tool.call` metadata.
- Watchdog/stop completion paths now emit `evt.agent.*` using the committed post-bump `(agent_turn_id, turn_epoch)` identity instead of stale pre-update context.
- Skills runtime resolves an owner-scoped internal `session_key`; public `session_id` remains a caller-visible alias only.
- `tool_result` ingress performs lightweight source/card validation, including card pointer, `tool_call_id`, and source/author/function consistency checks.

### 4. Release-facing documentation cleanup

- Tool callback examples now use the live `CGContext`/ingress-based helper shape.
- Skills docs now describe `session_id` as an alias rather than raw sandbox identity.
- PMO context docs now describe pre-authorized box inheritance rather than exists-only behavior.
- This release note replaces the earlier `v1r4` convergence playbook drafts and should be used as the release summary against `main`.

## Intentionally Deferred to V2

The following topics are important, but are not part of the `V1R4` release gate:

- full redesign of the centralized `tool_result / resume / UTP` model
- complete strict source-authenticity / producer-binding framework
- full capability-style ACL
- formal external runtime contract for edge runtimes and future agent-owned execution

These continue under the `v2` direction tracked from `#417`.

## Validation

The `V1R4` closure was verified with:

- targeted unit/regression suites for worker, PMO, skills, dispatcher, L0 report, and final stabilization regressions: the last blocker-focused patch passed with `47 passed`, on top of the earlier targeted coverage that closed the broader `V1R4` branch
- full integration coverage: `7 passed, 1 skipped`
- quickstart validation on an isolated local stack (`NATS 4223`, `API 8099`):
  - `demo_simple_principal_stream`
  - `demo_principal_fullflow`
  - `demo_principal_fullflow_api`
  - `demo_fork_join_word_count`
  - `demo_ui_action --mode chat`
  - `demo_ui_action --mode busy`
- compile verification on the touched Python modules

## Upgrade Notes

- `V1R4` should be treated as the stable final target for `v1`.
- Existing integrations should stop relying on payload control fields and outdated callback helper signatures.
- Any workflow that assumes `session_id` is globally reusable across agents should be updated to treat it as an alias only.
- If local port `4222` is already occupied during validation, point `[nats].servers` or `NATS_SERVERS` to an alternate local NATS port; the release validation itself was exercised on `4223`.
