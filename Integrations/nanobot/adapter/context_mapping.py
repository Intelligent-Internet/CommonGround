from __future__ import annotations

import json
from typing import Any

from CommonGround.contracts import AgentRef, normalize_dispatch_anchor
from CommonGround.sdk import TurnContext

# These work-order fields are NanoBot runtime conventions, not Kernel schema.
WORK_ORDER_KIND_V1 = "common_ground.work_order.v1"
EXECUTION_PLAN_PHASE = "execution_plan"
DEFAULT_CHILD_WORK_ORDER_OBJECTIVE = "Return a concise confirmation that this dynamically provisioned leaf completed the delegated child turn."
DEFAULT_EXPECTED_OUTPUT = {"type": "text", "style": "concise"}
DEFAULT_DELEGATION_POLICY = {"may_delegate": False}
DEFAULT_MAX_CHILD_WORK_ORDERS = 4


def build_turn_session_key(*, agent_id: str, turn_id: str) -> str:
    return f"cg:{agent_id}:{turn_id}"


def extract_bootstrap_payload(context: TurnContext) -> Any:
    for item in context.semantic_items:
        if item.record.record_role == "bootstrap":
            return item.content.payload()
    raise ValueError(f"turn {context.turn.turn.turn_id} has no bootstrap semantic record")


def is_work_order_payload(payload: Any) -> bool:
    return isinstance(payload, dict) and payload.get("kind") == WORK_ORDER_KIND_V1


def build_child_work_order(
    *,
    root_payload: Any,
    parent_turn_id: str,
    target_agent: AgentRef | None = None,
    child_task: Any = None,
    task_index: int = 0,
) -> dict[str, Any]:
    payload = root_payload
    if is_work_order_payload(payload):
        work_order = dict(payload)
    else:
        if child_task is None:
            child_task = payload.get("child_task") if isinstance(payload, dict) else None
        work_order = {
            "kind": WORK_ORDER_KIND_V1,
            "objective": DEFAULT_CHILD_WORK_ORDER_OBJECTIVE,
            "input": {},
            "expected_output": dict(DEFAULT_EXPECTED_OUTPUT),
            "delegation_policy": dict(DEFAULT_DELEGATION_POLICY),
            "context": {"parent_request": payload},
        }
        if isinstance(child_task, str) and child_task:
            work_order["objective"] = child_task
        elif isinstance(child_task, dict):
            if isinstance(child_task.get("task_id"), str):
                work_order["task_id"] = child_task["task_id"]
            work_order["objective"] = _non_empty_str(child_task.get("objective")) or DEFAULT_CHILD_WORK_ORDER_OBJECTIVE
            work_order["input"] = child_task.get("input", {})
            if isinstance(child_task.get("expected_output"), dict):
                work_order["expected_output"] = dict(child_task["expected_output"])
            if isinstance(child_task.get("delegation_policy"), dict):
                work_order["delegation_policy"] = dict(child_task["delegation_policy"])
            if isinstance(child_task.get("context"), dict):
                work_order["context"] = dict(child_task["context"])

    work_order["kind"] = WORK_ORDER_KIND_V1
    work_order["task_id"] = _non_empty_str(work_order.get("task_id")) or f"task_{task_index + 1}"
    work_order["objective"] = _non_empty_str(work_order.get("objective")) or DEFAULT_CHILD_WORK_ORDER_OBJECTIVE
    work_order.setdefault("input", {})
    if not isinstance(work_order.get("expected_output"), dict):
        work_order["expected_output"] = dict(DEFAULT_EXPECTED_OUTPUT)
    else:
        work_order["expected_output"] = dict(work_order["expected_output"])
    if not isinstance(work_order.get("delegation_policy"), dict):
        work_order["delegation_policy"] = dict(DEFAULT_DELEGATION_POLICY)
    else:
        work_order["delegation_policy"] = {**DEFAULT_DELEGATION_POLICY, **work_order["delegation_policy"]}

    provenance = work_order.get("provenance")
    if not isinstance(provenance, dict):
        provenance = {}
    provenance = {**provenance, "parent_turn_id": parent_turn_id}
    if target_agent is not None:
        provenance["target_agent_ref"] = {"project_id": target_agent.project_id, "agent_id": target_agent.agent_id}
    work_order["provenance"] = provenance
    return work_order


def build_child_work_orders(
    *,
    root_payload: Any,
    parent_turn_id: str,
    max_work_orders: int = DEFAULT_MAX_CHILD_WORK_ORDERS,
) -> tuple[dict[str, Any], ...]:
    child_tasks = _extract_child_tasks(root_payload)
    if not child_tasks:
        child_tasks = (None,)
    work_orders: list[dict[str, Any]] = []
    for index, child_task in enumerate(child_tasks[:max(1, max_work_orders)]):
        work_orders.append(
            build_child_work_order(
                root_payload=root_payload,
                parent_turn_id=parent_turn_id,
                child_task=child_task,
                task_index=index,
            )
        )
    return tuple(work_orders)


def validate_unique_work_order_task_ids(work_orders: tuple[dict[str, Any], ...]) -> tuple[str, ...]:
    task_ids: list[str] = []
    seen: set[str] = set()
    duplicates: set[str] = set()
    for index, work_order in enumerate(work_orders):
        task_id = _non_empty_str(work_order.get("task_id")) if isinstance(work_order, dict) else None
        if task_id is None:
            raise ValueError(f"work order {index + 1} is missing non-empty task_id")
        try:
            normalized_task_id = normalize_dispatch_anchor(task_id, field_name=f"work order {index + 1} task_id")
        except ValueError as exc:
            raise ValueError(str(exc)) from exc
        if normalized_task_id != task_id:
            raise ValueError(f"work order {index + 1} task_id must already be normalized")
        task_id = normalized_task_id
        if task_id in seen:
            duplicates.add(task_id)
        seen.add(task_id)
        task_ids.append(task_id)
    if duplicates:
        raise ValueError(f"duplicate work order task_id: {', '.join(sorted(duplicates))}")
    return tuple(task_ids)


def plan_max_child_work_orders(root_payload: Any) -> int:
    if not isinstance(root_payload, dict):
        return DEFAULT_MAX_CHILD_WORK_ORDERS
    orchestration = root_payload.get("orchestration")
    if not isinstance(orchestration, dict):
        return DEFAULT_MAX_CHILD_WORK_ORDERS
    value = orchestration.get("max_child_tasks")
    if not isinstance(value, int):
        return DEFAULT_MAX_CHILD_WORK_ORDERS
    return min(max(1, value), DEFAULT_MAX_CHILD_WORK_ORDERS)


def should_plan_with_model(root_payload: Any) -> bool:
    if not isinstance(root_payload, dict):
        return False
    if any(key in root_payload for key in ("child_task", "child_tasks", "work_orders", "child_work_orders")):
        return False
    orchestration = root_payload.get("orchestration")
    if not isinstance(orchestration, dict):
        return False
    mode = orchestration.get("mode")
    return mode in {True, "auto", "multi_agent", "decompose"}


def should_delegate_to_leaf(root_payload: Any) -> bool:
    if not isinstance(root_payload, dict):
        return False
    if any(key in root_payload for key in ("child_task", "child_tasks", "work_orders", "child_work_orders")):
        return True
    orchestration = root_payload.get("orchestration")
    if not isinstance(orchestration, dict):
        return False
    mode = orchestration.get("mode")
    return mode in {True, "auto", "multi_agent", "decompose"}


def render_direct_response_prompt(*, turn_id: str, root_payload: Any) -> str:
    return "\n".join(
        [
            "You are directly completing a CommonGround root conversation turn.",
            f"turn_id: {turn_id}",
            "",
            "Use the root request and conversation_context directly.",
            "Treat explicit facts in the root request and conversation_context as current-session context.",
            "Do not discard, reinterpret, or weaken explicit context unless the user asks you to revise it.",
            "Do not create child tasks or delegate. Return the final user-facing answer.",
            "",
            "Root request:",
            _render_payload(root_payload),
        ]
    )


def render_execution_plan_prompt(*, turn_id: str, root_payload: Any, max_child_tasks: int) -> str:
    return "\n".join(
        [
            "You are planning a CommonGround NanoBot parent turn.",
            f"turn_id: {turn_id}",
            "",
            "Decide whether the request needs one direct child task or multiple parallel child tasks.",
            f"Use between 1 and {max_child_tasks} child tasks.",
            "Use multiple tasks when the user asks for research, comparison, decomposition, parallel subagents, or a multi-topic report.",
            "",
            "Return only valid JSON with this exact shape:",
            '{"child_tasks":[{"task_id":"task_1","objective":"...","input":{},"expected_output":{"type":"text","style":"detailed"},"delegation_policy":{"may_delegate":false}}]}',
            "",
            "Each child task must be independently executable by a leaf agent.",
            "Preserve explicit facts and constraints from the root request and conversation_context in child task inputs.",
            "Do not add uncertainty, hedging, or reinterpretation that is not supported by the root request or conversation_context.",
            "Do not include Markdown fences or explanatory prose.",
            "",
            "Root request:",
            _render_payload(root_payload),
        ]
    )


def parse_execution_plan_content(content: str, *, root_payload: Any, parent_turn_id: str, max_child_tasks: int) -> tuple[dict[str, Any], ...]:
    parsed = _loads_json_object(content)
    if parsed is None:
        return build_child_work_orders(root_payload=root_payload, parent_turn_id=parent_turn_id, max_work_orders=1)
    tasks = parsed.get("child_tasks")
    if not isinstance(tasks, list):
        tasks = parsed.get("work_orders")
    if not isinstance(tasks, list) or not tasks:
        return build_child_work_orders(root_payload=root_payload, parent_turn_id=parent_turn_id, max_work_orders=1)
    payload = {"child_tasks": tasks, "context": {"parent_request": root_payload}}
    return build_child_work_orders(root_payload=payload, parent_turn_id=parent_turn_id, max_work_orders=max_child_tasks)


def extract_last_payload(context: TurnContext, *, role: str | None = None) -> Any:
    for item in reversed(context.semantic_items):
        if role is None or item.record.record_role == role:
            return item.content.payload()
    raise ValueError(f"turn {context.turn.turn.turn_id} has no semantic payload matching role={role!r}")


def render_leaf_prompt(context: TurnContext) -> str:
    payload = extract_bootstrap_payload(context)
    turn_id = context.turn.turn.turn_id
    target_agent = context.turn.target_agent.agent_id
    parent_turn_id = context.turn.cause.id if context.turn.cause.kind == "turn" else None

    if is_work_order_payload(payload):
        return _render_work_order_prompt(turn_id=turn_id, target_agent=target_agent, parent_turn_id=parent_turn_id, work_order=payload)

    if parent_turn_id is not None:
        header = [
            "You are executing a CommonGround child turn.",
            f"turn_id: {turn_id}",
            f"target_agent: {target_agent}",
            f"parent_turn_id: {parent_turn_id}",
            "",
            "Complete the assigned child task and return the final result.",
            "",
            _render_payload(payload),
        ]
        return "\n".join(header)

    return "\n".join(
        [
            "You are executing a CommonGround turn.",
            f"turn_id: {turn_id}",
            f"target_agent: {target_agent}",
            "",
            "Complete the request and return the final result.",
            "",
            _render_payload(payload),
        ]
    )


def _render_work_order_prompt(*, turn_id: str, target_agent: str, parent_turn_id: Any, work_order: dict[str, Any]) -> str:
    lines = [
        "You are executing a CommonGround work order.",
        f"turn_id: {turn_id}",
        f"target_agent: {target_agent}",
    ]
    if parent_turn_id is not None:
        lines.append(f"parent_turn_id: {parent_turn_id}")
    lines.extend(
        [
            "",
            "Objective:",
            str(work_order.get("objective", "")),
            "",
            "Input:",
            _render_payload(work_order.get("input", {})),
            "",
            "Expected output:",
            _render_payload(work_order.get("expected_output", {})),
            "",
            "Delegation policy:",
            _render_payload(work_order.get("delegation_policy", {})),
        ]
    )
    delegation_policy = work_order.get("delegation_policy")
    if isinstance(delegation_policy, dict) and delegation_policy.get("may_delegate") is False:
        lines.extend(
            [
                "",
                "Do not create or delegate new subtasks in this turn. Complete the objective directly.",
            ]
        )
    lines.extend(["", "Provenance:", _render_payload(work_order.get("provenance", {}))])
    context = work_order.get("context")
    if context is not None:
        lines.extend(["", "Parent request context (not the objective):", _render_payload(context)])
    return "\n".join(lines)


def render_supervisor_multi_prompt(
    *,
    turn_id: str,
    root_payload: Any,
    child_results: tuple[dict[str, Any], ...],
    timed_out_child_results: tuple[dict[str, Any], ...] = (),
) -> str:
    lines = [
        "You are completing a CommonGround parent turn after child turns finished or timed out.",
        f"turn_id: {turn_id}",
        "",
        "Original root request:",
        _render_payload(root_payload),
        "",
        "Child turn results:",
        _render_payload({"items": list(child_results)}),
    ]
    if timed_out_child_results:
        lines.extend(
            [
                "",
                "Timed out child turns:",
                _render_payload({"items": list(timed_out_child_results)}),
            ]
        )
    lines.extend(
        [
            "",
            "Synthesize a complete user-facing answer from available child results.",
            "If one or more child turns timed out, still answer from the successful child results and clearly note the missing sections.",
            "Root request and conversation_context are authoritative when they conflict with child results.",
            "If a child result contradicts explicit root/context facts, correct it using the root/context facts.",
            "If the user requested a report, preserve the requested topic structure and include concrete recommendations.",
        ]
    )
    return "\n".join(lines)


def _render_payload(payload: Any) -> str:
    if isinstance(payload, str):
        return payload
    return json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True)


def _non_empty_str(value: Any) -> str | None:
    if isinstance(value, str) and value:
        return value
    return None


def _extract_child_tasks(root_payload: Any) -> tuple[Any, ...]:
    if is_work_order_payload(root_payload):
        return (root_payload,)
    if not isinstance(root_payload, dict):
        return ()
    for key in ("child_work_orders", "work_orders", "child_tasks"):
        value = root_payload.get(key)
        if isinstance(value, list):
            return tuple(item for item in value if item is not None)
    child_task = root_payload.get("child_task")
    if child_task is not None:
        return (child_task,)
    return ()


def _loads_json_object(content: str) -> dict[str, Any] | None:
    text = content.strip()
    if text.startswith("```"):
        lines = text.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].startswith("```"):
            lines = lines[:-1]
        text = "\n".join(lines).strip()
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None
