# Architecture Overview

This document describes the system's layered model, component boundaries, and key data flows as an entry point for downstream protocol and implementation documentation.

## Layered Model (Cognitive View)
- **L0 Protocol Layer (Physics)**: defines how components communicate and their invariants (Subject, Payload, idempotency, and gatekeeping).
- **L1 Kernel Layer (OS)**: built-in system implementation mechanisms (PMO/Worker/UI Worker/BatchManager).
- **L2 Ecosystem Layer (User Space)**: configurable/extensible capabilities (tool services, project configuration, management API, skill/evaluation components).

Documentation entry points:
- L0: `04_protocol_l0/`
- L1: `03_kernel_l1/`
- L2: `02_building_agents/`

## Component Roles (By “Distance to Protocol”)

### L0 Protocol and Ground Truth (Closest to Protocol)
- **core/**: cross-service protocol and constants (events/Subject/DTO).
- **infra/l0_engine.py**: L0 primitive execution entrypoint (`enqueue` / `report` / `join` / `wakeup`), responsible for transactional DB persistence and NATS wakeup.
- **card-box-cg/**: Cards/Boxes as the authoritative fact source and lineage storage.

### L0 Implementation Substrate (I/O Adaptation)
- **infra/**: NATS/PG/LLM/CardBox I/O adaptation layer, carrying the thinnest implementation of protocol landing (`nats_client`, stores, `event_emitter`, etc.).

### L1 Kernel Implementation (Service Layer)
- **services/agent_worker/**: executes turns, calls LLMs, advances L0 state machine.
- **services/pmo/**: identity provisioning, execution orchestration (BatchManager).
- **services/ui_worker/**: receives `cmd.sys.ui.action`, dispatches `ui_action` to L0, and wakes processing via `cmd.agent.<worker_target>.wakeup`.

### L2 Ecosystem and Extension (Farthest from Protocol)
- **tool services (external or built-in)**: execute tool logic and callback with tool_result.
- **services/api/**: management API and part of control surfaces (for example, God API/Startup/Orchestration).
- **services/tools/**, **services/judge/**: built-in tooling and evaluation capabilities (part of tool service ecosystem).

## Source of Truth and Read/Write Boundaries
- **Cards/Boxes**: authoritative truth in `card-box-cg`.
- **resource.*** (configuration assets): authoritative truth in this repository's PG; written by PMO and management API.
- **state.*** (runtime/gates): authoritative truth in this repository's PG; written by PMO and Workers (including UI Worker), with PMO handling scheduling and reclamation.
- **NATS**: used for control plane, task control commands, and state event propagation; not the factual truth source. Fact-layer state is PG-driven.

## Box Visibility (Important)
- **Centralized storage ≠ default visibility**: Cards/Boxes are centrally stored within a project at the physical storage layer (CardBox/PG), not as separate storage per Agent.
- **LLM visibility is determined by assembly**: a single Agent turn’s LLM sees only the input snapshot assembled into `context_box_id` and the `output_box_id` added by this turn itself (rolling output); it does not automatically read other boxes in the project.
- **Cross-agent sharing requires explicit mechanisms**: `box_id` must be explicitly passed and context must be assembled/inherited explicitly (currently mainly done by PMO context packing; tools may also handle this in the future).
- Reference: `03_kernel_l1/pmo_orchestration.md` (PMO context packing / handover)

## Core Runtime Data Flow (Inbox + Wakeup)
1) **Enqueue/Report**: `L0Engine` or its wrapper (such as `infra/l0/tool_reports.py`) writes requests to `state.agent_inbox` and records `execution_edges`.
2) **Wakeup**: L0 publishes `cmd.agent.{target}.wakeup` based on `worker_target` (the `target` is the `worker_target` from `resource.roster`, for example, `worker_generic`).
3) **Worker**: acquires lock competitively → claims inbox in batches → hydrates `state/resource/cards` → executes LLM.
4) **Tool Call**: Worker writes `tool.call` card and publishes commands to the topic defined by `target_subject` in the tool definition (usually `cmd.sys.pmo.internal.*` or `cmd.tool.*`).
5) **Report**: Tool callbacks write `tool.result` card, which flows back through `infra/l0/tool_reports.py` to `L0Engine.report`, into `state.agent_inbox`, and triggers `cmd.agent.{target}.wakeup` again.
6) **Finalize**: Worker writes/updates `task.deliverable` when finishing a turn and publishes `evt.agent.*.task`.
> Constraint: **success/failure of one Turn is consolidated into one `evt.agent.*.task` result event** for that Turn. `BatchManager` dispatches multiple sub-turns, so multiple task results are generated.

## Graph-Theoretic View (Audit View, Not Orchestration)
- **Identity Graph**: structurally close to DAG (owner/derived_from should not form cycles), used to describe identity relationship changes.
- **Execution Graph**: high-frequency execution-path audit view, **may contain cycles** (recursion/callback chains), and is not used as orchestration input.
- Conclusion: we do not introduce user-visible DAG orchestration; graphs are used only for observability and audit.

## System Stability and Damping
Based on VSM cybernetics, dampers suppress inter-subsystem oscillation and prevent unlimited resource consumption (Entropy Damper).
- **Global Recursion Depth Limiter**:
  - Purpose: prevent infinite recursive call chains (Dead Loop) or excessive computation resource use between Agents.
  - Mechanism:
    1.  Propagate call depth as non-negative integer string via NATS header `CG-Recursion-Depth` (Entropy State).
    2.  Producer-side damping (Execution Gate): L0 inbound paths (such as `enqueue`, `report`) require `CG-Recursion-Depth` to be a non-negative integer; over-threshold inputs return `recursion_depth_exceeded`.
    3.  Consumer-side damping (Worker): validate depth when processing Inbox; missing/invalid values are `ProtocolViolationError` and execution is rejected.
    4.  If `depth >= threshold`, producer or consumer must reject dispatch and terminate the call chain to avoid unbounded resource usage.
  - Failure semantics:
    - Inbound enqueue/report overage: `enqueue`/`report` return `error_code=recursion_depth_exceeded`.
    - Runtime overage: Worker emits `evt.agent.{agent_id}.task` with `status=failed` and `error_code=recursion_depth_exceeded`.
    - Missing/invalid header: treated as `ProtocolViolationError`, usually transformed at runtime to failure with `error_code=protocol_violation`.
  - Propagation and increment:
    - Current L0 primitives default `enqueue_mode` processing to `"call"`; code does not preserve `transfer/notify` modes.
    - Depth is typically propagated along call relationships; default user entry depth is `0`.
    - For explicit child-call derivation, PMO increments manually, such as `services/pmo/internal_handlers/context.py::bump_recursion_depth` and `services/pmo/l1_orchestrators/batch_manager.py::child_depth = parent_depth + 1`.
    - `Join/Report` constrain and inherit original depth by `correlation_id`; `tool_result`/`timeout` paths remain consistent.
  - Differentiation: this threshold stabilizes cross-Agent call chains and complements, rather than replaces, per-Agent internal `max_steps`.

## Startup Entrypoints (Core Runtime)
- `uv run -m services.pmo.service`
- `uv run -m services.agent_worker.loop`
- `uv run -m services.ui_worker.loop`
- `uv run -m services.api`

## Cross-Repository Boundary (CommonGround ↔ card-box-cg)
- **CommonGround**: control flow and gating (L0/L1/partial L2).
- **card-box-cg**: fact source for content and lineage, providing cross-language access.

## Third-Party Integration Recommendations
- **Tool services**: recommend NATS + CardStore API, following UTP callbacks and `traceparent` propagation.
- **Configuration writes**: recommend writing `resource.*` through management API to avoid direct DB writes.

## Related Protocol Documents
- `04_protocol_l0/nats_protocol.md`
- `04_protocol_l0/state_machine.md`
- `04_protocol_l0/utp_tool_protocol.md`
