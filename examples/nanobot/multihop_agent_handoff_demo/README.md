# Multi-Hop Agent Handoff Demo

This is an advanced NanoBot runtime fixture. It requires NanoBot gateway/companion behavior and is not the default BYOA setup path. Shallow BYOA work-memory reporting and generic BYOA conversation-worker admission live under `docs/en/guides/` and `examples/byoa/`, and do not require NanoBot.

This fixture exercises a multi-hop orchestration path:

`requester -> nanobot_a -> CommonGround -> nanobot_b -> CommonGround -> codex_c -> result`

This is a managed-continuation integration demo, not the default baseline for every CommonGround integration. The thin baseline remains `cg skill + cg CLI`; this fixture exercises the deeper path where a runtime companion owns claim lifecycle, project discovery, child dispatch, parent suspend/resume, and final absorption.

The intent is broader than a website PR demo:

- `nanobot_a` is the personal orchestrator
- `nanobot_b` is the current peer personal-agent fixture used to supply missing context
- `codex_c` is the current expert-agent fixture used for coding work
- similar demos could substitute another expert agent such as `coding_expert_c`, `data_analyst_c`, or a non-coding specialist
- all agents run in the same CommonGround project
- requester-facing task details and delegated work orders flow through CommonGround turn payloads
- this fixture also seeds a local repo-lookup file for `nanobot_b`; that fixture-only context is not a platform contract
- the platform contract stays broad: agents are expected to understand the delegated task details without a demo-specific schema

## Fixture Scope

This directory provides:

- gateway config scaffolding for the three agents
- workspace rules for `nanobot_a`, `nanobot_b`, and `codex_c`
- sample root work-order payloads for single-hop and multi-hop scenarios
- a runnable e2e runner: `scripts/demo/run_multihop_agent_handoff_e2e.py`
- local-only coding flow with repo clone, edit, branch, commit, and validation
- parent-agent discovery through the CommonGround companion read tools

The scripted E2E exercises:

- `nanobot_a -> nanobot_b -> codex_c -> parent resume -> finish`
- `nanobot_a` uses project discovery before each child dispatch
- `codex_c` executes the terminal coding task locally and returns JSON
- the runner collects parent/child turns, lineage, and log-based evidence

Out of scope for this fixture:

- `git push`
- `gh pr create`
- Slack entrypoint / reply loop

## Directory Layout

- `nanobot_a.config.json`: gateway config for the orchestrator
- `nanobot_b.config.json`: gateway config for the peer personal agent
- `codex_c.config.json`: gateway config for the expert coding agent
- `workspace_a/AGENTS.md`: rules for `nanobot_a`
- `workspace_b/AGENTS.md`: rules for `nanobot_b`
- `workspace_c/AGENTS.md`: rules for `codex_c`
- `request_samples/root_request_payload.json`: sample multi-hop root work order
- `request_samples/root_request_payload.single_hop.json`: sample single-hop root work order
- `request_samples/root_request_payload.local_subagent.json`: sample local-subagent reporting smoke work order

## Intended Scenario

The first concrete scenario is local-only coding with one information-gathering hop:

1. `nanobot_a` receives a coding request but does not yet know the repo URL.
2. `nanobot_a` queries project discovery and decides to ask `nanobot_b`.
3. `nanobot_b` returns the repo clone URL and branch information.
4. `nanobot_a` selects an available coding expert from discovery results. In the current fixture that expert ends up being `codex_c`.
5. `codex_c` creates one stable checkout directory inside its own workspace, makes a small local-only change, validates it, and returns JSON.
6. `nanobot_a` summarizes the child results and finishes the root turn.

The validation command is intentionally lightweight:

- `python3 tests/smoke_test.py`

This avoids requiring Node/npm to be available in the runtime environment.

## Notes

- This demo uses the existing `common_ground.work_order.v1` envelope.
- The platform does not choose the next agent automatically.
- `nanobot_a` is expected to choose the next hop itself.
- This fixture is local-only. It does not push to GitHub, create pull requests, or use Slack as the entrypoint.
- The sample payloads are illustrative examples for this fixture project, not a required schema for future multi-agent work.
- The runner treats request payloads as opaque demo input. It does not parse task-specific fields such as `input.repo.clone_url`.
- In this fixture, `nanobot_b` also reads a runner-seeded `demo_context.json` file to answer the repo-lookup hop. That file is fixture-local knowledge, not a CommonGround platform contract.
- In this fixture, `codex_c` advertises both a broad conversation work-order offer and a `coding` turn offer for discovery. The `coding` offer is only an advisory fixture label, not a platform-level special contract.
- If a sample payload needs the bootstrapped local repo URL, use the string placeholder `${DEMO_REPO_URL}` anywhere in the payload; the runner replaces that token recursively before dispatch.

## Runner

This advanced fixture assumes a source checkout prepared through [CONTRIBUTING.md](../../../CONTRIBUTING.md).

Install the published package form when you only need the runtime dependencies:

```bash
uv tool install 'commonground-kernel[server,nanobot]'
```

The runner:

```bash
python -m scripts.demo.run_multihop_agent_handoff_e2e \
  --pg-dsn 'postgresql://USER:PASSWORD@HOST:PORT/DBNAME'
```

Provider/model resolution works like this:

- if `--personal-provider/--personal-model` are passed, use them
- otherwise, read `~/.nanobot/config.json` and use `agents.defaults`
- if no usable nanobot defaults are found, fall back to `gemini` / `gemini-flash-latest`

If you want to force a known-good local setup explicitly, pass provider overrides, for example:

```bash
python -m scripts.demo.run_multihop_agent_handoff_e2e \
  --pg-dsn 'postgresql://USER:PASSWORD@HOST:PORT/DBNAME' \
  --personal-provider azure_openai \
  --personal-model gpt-5.4-nano \
  --expert-provider azure_openai \
  --expert-model gpt-5.4-nano
```

By default the runner dispatches the multi-hop sample payload. To force the single-hop smoke path, pass:

```bash
export CG_REPO_ROOT="$(pwd)"
python -m scripts.demo.run_multihop_agent_handoff_e2e \
  --pg-dsn 'postgresql://USER:PASSWORD@HOST:PORT/DBNAME' \
  --payload-file "$CG_REPO_ROOT/examples/nanobot/multihop_agent_handoff_demo/request_samples/root_request_payload.single_hop.json"
```

To verify runtime-local subagent reporting instead of CommonGround child dispatch, pass:

```bash
python -m scripts.demo.run_multihop_agent_handoff_e2e \
  --pg-dsn 'postgresql://USER:PASSWORD@HOST:PORT/DBNAME' \
  --payload-file "$CG_REPO_ROOT/examples/nanobot/multihop_agent_handoff_demo/request_samples/root_request_payload.local_subagent.json"
```

That local-subagent smoke path asks `nanobot_a` to use the local `spawn` tool once. The spawned subagent is instructed to output exactly `CG_LOCAL_SUBAGENT_FIXED_OUTPUT_V1`, which should appear in the runner summary under `turn_records.parent` as a `local_subagent_result` record.

The runner will:

- start the three gateways
- bootstrap a local bare demo repo for the single-hop coding path
- dispatch the root turn
- wait for the parent terminal state
- collect parent/child turns and lineage

Expected successful outcome:

- one parent turn owned by `nanobot_a`
- one information child turn owned by `nanobot_b`
- one coding child turn owned by `codex_c`
- a local checkout under the child workspace
- a local branch and commit
- `python3 tests/smoke_test.py` passing

Out of scope:

- GitHub push / PR evidence
- Slack entrypoint integration
