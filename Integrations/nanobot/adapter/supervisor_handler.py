from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any, Protocol

from CommonGround.contracts import (
    AgentRef,
    ClaimToken,
    DispatchAuthority,
    DispatchAuthorityMode,
    OperationMeta,
    TURN_KIND_PROVISION_AGENT_SPAWN_V1,
    TURN_KIND_CONVERSATION_V1,
    TurnOutcome,
    TurnRef,
    normalize_dispatch_anchor,
)
from CommonGround.agent_client import FinishTurnAction, HttpAgentClient, SuspendTurnAction
from CommonGround.sdk import TurnContext

from .context_mapping import (
    EXECUTION_PLAN_PHASE,
    build_child_work_orders,
    build_turn_session_key,
    extract_bootstrap_payload,
    extract_last_payload,
    parse_execution_plan_content,
    plan_max_child_work_orders,
    render_direct_response_prompt,
    render_execution_plan_prompt,
    render_supervisor_multi_prompt,
    should_delegate_to_leaf,
    should_plan_with_model,
    validate_unique_work_order_task_ids,
)
from ..runtime.feed_utils import fetch_turn_feed_items


class _DirectLoop(Protocol):
    async def process_direct(self, content: str, session_key: str = "cli:direct", channel: str = "cli", chat_id: str = "direct", on_progress=None, on_stream=None, on_stream_end=None):
        ...


class SupervisorTurnHandler:
    def __init__(
        self,
        *,
        loop: _DirectLoop,
        provisioner_agent: AgentRef,
        leaf_role: str = "nanobot.leaf.conversation.v1",
    ) -> None:
        self._loop = loop
        self._provisioner_agent = provisioner_agent
        self._leaf_role = leaf_role

    def handle_turn(self, context: TurnContext, client: HttpAgentClient, claim: ClaimToken):
        parent_turn = claim.turn_ref()
        root_payload = extract_bootstrap_payload(context)
        if _execution_plan_from_context(context) is None and not should_delegate_to_leaf(root_payload):
            prompt = render_direct_response_prompt(turn_id=claim.turn_id, root_payload=root_payload)
            response = asyncio.run(
                self._loop.process_direct(
                    prompt,
                    session_key=build_turn_session_key(agent_id=claim.agent_id, turn_id=claim.turn_id),
                    channel="cg",
                    chat_id=claim.turn_id,
                )
            )
            content = "" if response is None else ((response.content or "").strip())
            return FinishTurnAction(
                outcome=TurnOutcome.SUCCEEDED,
                final_payload={
                    "content": content,
                    "agent_id": claim.agent_id,
                    "turn_id": claim.turn_id,
                    "execution_mode": "direct",
                },
            )
        try:
            work_orders = self._work_orders_for_turn(context=context, client=client, claim=claim, root_payload=root_payload)
            task_ids = validate_unique_work_order_task_ids(work_orders)
            provision_dispatch_keys = _task_dispatch_keys(claim.turn_id, "provision:leaf", task_ids)
            child_dispatch_keys = _task_dispatch_keys(claim.turn_id, "leaf:conversation", task_ids)
        except ValueError as exc:
            return FinishTurnAction(
                outcome=TurnOutcome.FAILED,
                final_payload={"error": "invalid_work_order_task_ids", "message": str(exc)},
            )
        work_orders_by_task_id = dict(zip(task_ids, work_orders))

        provision_index = _correlated_turns_by_task_id(
            client,
            parent_turn,
            TURN_KIND_PROVISION_AGENT_SPAWN_V1,
            task_ids=task_ids,
            correlation_kind="provision",
        )
        if provision_index["error"] is not None:
            return FinishTurnAction(outcome=TurnOutcome.FAILED, final_payload=provision_index["error"])
        provision_entries_by_task_id = provision_index["turns"]
        missing_provision_task_ids = [task_id for task_id in task_ids if task_id not in provision_entries_by_task_id]
        if missing_provision_task_ids:
            dispatched = []
            for task_id in missing_provision_task_ids:
                work_order = work_orders_by_task_id[task_id]
                provision_turn = client.dispatch(
                    requested_by=claim.agent_ref(),
                    target_agent=self._provisioner_agent,
                    input_payload=self._provision_bootstrap(claim, work_order=work_order),
                    authority=DispatchAuthority(mode=DispatchAuthorityMode.CHILD_DERIVATION, parent_claim=claim),
                    dispatch_key=provision_dispatch_keys[task_id],
                    turn_kind=TURN_KIND_PROVISION_AGENT_SPAWN_V1,
                )
                dispatched.append(provision_turn.turn_id)
            return SuspendTurnAction(reason="await_leaf_provision", note=f"awaiting provision {', '.join(dispatched)}")

        provision_entries = tuple(provision_entries_by_task_id[task_id] for task_id in task_ids)
        provision_target_error = _validate_correlated_targets(
            provision_entries,
            {task_id: self._provisioner_agent for task_id in task_ids},
            correlation_kind="provision",
        )
        if provision_target_error is not None:
            return FinishTurnAction(outcome=TurnOutcome.FAILED, final_payload=provision_target_error)
        active_provisions = [entry["ref"].turn_id for entry in provision_entries if entry["snapshot"].outcome is None]
        if active_provisions:
            return SuspendTurnAction(reason="await_leaf_provision", note=f"provision {', '.join(active_provisions)} still active")
        failed_provisions = [
            entry["ref"].turn_id
            for entry in provision_entries
            if entry["snapshot"].outcome != TurnOutcome.SUCCEEDED
        ]
        if failed_provisions:
            return FinishTurnAction(
                outcome=TurnOutcome.FAILED,
                final_payload={"error": "leaf_provision_failed", "provision_turn_ids": list(failed_provisions)},
            )

        leaf_agents_by_task_id: dict[str, AgentRef] = {}
        for entry in provision_entries:
            provision_ref = entry["ref"]
            provision_payload = extract_last_payload(client.fetch_context(provision_ref), role="deliverable")
            provision_task_id = _non_empty_str(provision_payload.get("task_id")) if isinstance(provision_payload, dict) else None
            if provision_task_id != entry["task_id"]:
                return FinishTurnAction(
                    outcome=TurnOutcome.FAILED,
                    final_payload={
                        "error": "provision_task_id_mismatch",
                        "provision_turn_id": provision_ref.turn_id,
                        "expected_task_id": entry["task_id"],
                        "actual_task_id": provision_task_id,
                    },
                )
            leaf_agent = _new_agent_ref_from_provision_payload(provision_payload, project_id=claim.project_id)
            if leaf_agent is not None:
                leaf_agents_by_task_id[entry["task_id"]] = leaf_agent
        if len(leaf_agents_by_task_id) != len(work_orders):
            return FinishTurnAction(
                outcome=TurnOutcome.FAILED,
                final_payload={"error": "missing_provision_result", "provision_turn_ids": [entry["ref"].turn_id for entry in provision_entries]},
            )

        child_index = _correlated_turns_by_task_id(
            client,
            parent_turn,
            TURN_KIND_CONVERSATION_V1,
            task_ids=task_ids,
            correlation_kind="child",
        )
        if child_index["error"] is not None:
            return FinishTurnAction(outcome=TurnOutcome.FAILED, final_payload=child_index["error"])
        child_entries_by_task_id = child_index["turns"]
        missing_child_task_ids = [task_id for task_id in task_ids if task_id not in child_entries_by_task_id]
        if missing_child_task_ids:
            dispatched = []
            for task_id in missing_child_task_ids:
                leaf_agent = leaf_agents_by_task_id[task_id]
                child_turn = client.dispatch(
                    requested_by=claim.agent_ref(),
                    target_agent=leaf_agent,
                    input_payload=_targeted_work_order(
                        work_orders_by_task_id[task_id],
                        parent_turn_id=claim.turn_id,
                        target_agent=leaf_agent,
                    ),
                    authority=DispatchAuthority(mode=DispatchAuthorityMode.CHILD_DERIVATION, parent_claim=claim),
                    dispatch_key=child_dispatch_keys[task_id],
                    turn_kind=TURN_KIND_CONVERSATION_V1,
                )
                dispatched.append(child_turn.turn_id)
            return SuspendTurnAction(reason="await_child", note=f"awaiting child {', '.join(dispatched)}")

        child_entries = tuple(child_entries_by_task_id[task_id] for task_id in task_ids)
        child_target_error = _validate_correlated_targets(
            child_entries,
            leaf_agents_by_task_id,
            correlation_kind="child",
        )
        if child_target_error is not None:
            return FinishTurnAction(outcome=TurnOutcome.FAILED, final_payload=child_target_error)
        child_refs = tuple(entry["ref"] for entry in child_entries)
        now = datetime.now(UTC)
        timed_out_children = [
            entry["ref"].turn_id
            for entry in child_entries
            if _is_expired_running_child(entry["snapshot"], now=now)
        ]
        active_children = [
            entry["ref"].turn_id
            for entry in child_entries
            if entry["snapshot"].outcome is None and entry["ref"].turn_id not in timed_out_children
        ]
        if active_children:
            return SuspendTurnAction(reason="await_child", note=f"child {', '.join(active_children)} still active")
        failed_children = [
            entry["ref"].turn_id
            for entry in child_entries
            if entry["snapshot"].outcome is not None and entry["snapshot"].outcome != TurnOutcome.SUCCEEDED
        ]
        if failed_children:
            return FinishTurnAction(
                outcome=TurnOutcome.FAILED,
                final_payload={
                    "error": "child_failed",
                    "provision_turn_ids": [entry["ref"].turn_id for entry in provision_entries],
                    "child_turn_ids": list(failed_children),
                },
            )

        child_results = []
        timed_out_child_results = []
        stop_requested_child_turn_ids, child_stop_errors = _request_stop_for_timed_out_children(
            client,
            requested_by=claim.agent_ref(),
            child_refs=child_refs,
            timed_out_child_turn_ids=timed_out_children,
            parent_turn_id=claim.turn_id,
        )
        for entry in child_entries:
            task_id = entry["task_id"]
            child_ref = entry["ref"]
            work_order = work_orders_by_task_id[task_id]
            leaf_agent = leaf_agents_by_task_id[task_id]
            if child_ref.turn_id in timed_out_children:
                timed_out_child_results.append(
                    {
                        "task_id": task_id,
                        "objective": work_order.get("objective"),
                        "child_turn_id": child_ref.turn_id,
                        "leaf_agent_ref": {
                            "project_id": leaf_agent.project_id,
                            "agent_id": leaf_agent.agent_id,
                        },
                        "status": "timed_out",
                    }
                )
                continue
            child_context = client.fetch_context(child_ref)
            child_payload = extract_last_payload(child_context, role="deliverable")
            child_results.append(
                {
                    "task_id": task_id,
                    "objective": work_order.get("objective"),
                    "child_turn_id": child_ref.turn_id,
                    "leaf_agent_ref": {
                        "project_id": leaf_agent.project_id,
                        "agent_id": leaf_agent.agent_id,
                    },
                    "result": child_payload,
                }
            )
        if not child_results:
            return FinishTurnAction(
                outcome=TurnOutcome.FAILED,
                final_payload={
                    "error": "child_timed_out",
                    "provision_turn_ids": [entry["ref"].turn_id for entry in provision_entries],
                    "child_turn_ids": [ref.turn_id for ref in child_refs],
                    "timed_out_child_turn_ids": timed_out_children,
                    "stop_requested_child_turn_ids": stop_requested_child_turn_ids,
                    "child_stop_errors": child_stop_errors,
                },
            )
        prompt = render_supervisor_multi_prompt(
            turn_id=claim.turn_id,
            root_payload=root_payload,
            child_results=tuple(child_results),
            timed_out_child_results=tuple(timed_out_child_results),
        )
        response = asyncio.run(
            self._loop.process_direct(
                prompt,
                session_key=build_turn_session_key(agent_id=claim.agent_id, turn_id=claim.turn_id),
                channel="cg",
                chat_id=claim.turn_id,
            )
        )
        content = "" if response is None else ((response.content or "").strip())
        final_payload = {
            "content": content,
            "agent_id": claim.agent_id,
            "turn_id": claim.turn_id,
            "provision_turn_ids": [entry["ref"].turn_id for entry in provision_entries],
            "leaf_agent_refs": [
                {"project_id": leaf_agent.project_id, "agent_id": leaf_agent.agent_id}
                for leaf_agent in (leaf_agents_by_task_id[task_id] for task_id in task_ids)
            ],
            "child_turn_ids": [ref.turn_id for ref in child_refs],
            "timed_out_child_turn_ids": timed_out_children,
            "timed_out_child_results": timed_out_child_results,
            "stop_requested_child_turn_ids": stop_requested_child_turn_ids,
            "child_stop_errors": child_stop_errors,
            "partial": bool(timed_out_children),
            "child_results": child_results,
        }
        if len(child_results) == 1:
            final_payload.update(
                {
                    "provision_turn_id": provision_entries[0]["ref"].turn_id,
                    "leaf_agent_ref": {
                        "project_id": leaf_agents_by_task_id[task_ids[0]].project_id,
                        "agent_id": leaf_agents_by_task_id[task_ids[0]].agent_id,
                    },
                    "child_turn_id": child_refs[0].turn_id,
                    "child_result": child_results[0]["result"],
                }
            )
        return FinishTurnAction(
            outcome=TurnOutcome.SUCCEEDED,
            final_payload=final_payload,
        )

    def _work_orders_for_turn(self, *, context: TurnContext, client: HttpAgentClient, claim: ClaimToken, root_payload) -> tuple[dict, ...]:
        persisted = _execution_plan_from_context(context)
        if persisted is not None:
            return persisted
        max_work_orders = plan_max_child_work_orders(root_payload)
        if should_plan_with_model(root_payload):
            prompt = render_execution_plan_prompt(
                turn_id=claim.turn_id,
                root_payload=root_payload,
                max_child_tasks=max_work_orders,
            )
            response = asyncio.run(
                self._loop.process_direct(
                    prompt,
                    session_key=build_turn_session_key(agent_id=claim.agent_id, turn_id=f"{claim.turn_id}:plan"),
                    channel="cg",
                    chat_id=f"{claim.turn_id}:plan",
                )
            )
            content = "" if response is None else ((response.content or "").strip())
            work_orders = parse_execution_plan_content(
                content,
                root_payload=root_payload,
                parent_turn_id=claim.turn_id,
                max_child_tasks=max_work_orders,
            )
            validate_unique_work_order_task_ids(work_orders)
            client.append_record(
                claim,
                {
                    "phase": EXECUTION_PLAN_PHASE,
                    "source": "model",
                    "work_orders": list(work_orders),
                },
                role="progress",
            )
            return work_orders
        return build_child_work_orders(
            root_payload=root_payload,
            parent_turn_id=claim.turn_id,
            max_work_orders=max_work_orders,
        )

    def _provision_bootstrap(self, claim: ClaimToken, *, work_order: dict | None = None) -> dict:
        payload = {
            "agent": {"role": self._leaf_role},
            "lifecycle": {"source_turn_id": claim.turn_id},
            "parent_turn_id": claim.turn_id,
        }
        if work_order is not None:
            payload["work_order"] = {
                "task_id": work_order.get("task_id"),
                "objective": work_order.get("objective"),
                "parent_turn_id": claim.turn_id,
            }
        return payload


def _correlated_turns_by_task_id(
    client: HttpAgentClient,
    parent_turn: TurnRef,
    turn_kind: str,
    *,
    task_ids: tuple[str, ...],
    correlation_kind: str,
) -> dict[str, Any]:
    required_task_ids = set(task_ids)
    turns: dict[str, dict[str, Any]] = {}
    for event in fetch_turn_feed_items(client, parent_turn):
        if event.event_type != "turn.spawned" or event.subject_id == parent_turn.turn_id:
            continue
        ref = TurnRef(project_id=parent_turn.project_id, turn_id=event.subject_id)
        snapshot = client.get_turn(ref)
        if snapshot.turn_kind != turn_kind:
            continue
        context = client.fetch_context(ref)
        correlation = _turn_task_correlation(context)
        task_id = correlation["task_id"]
        correlated_parent_turn_id = correlation["parent_turn_id"]
        if task_id is None:
            continue
        if task_id not in required_task_ids:
            continue
        if correlated_parent_turn_id is None:
            return {
                "turns": {},
                "error": {
                    "error": f"{correlation_kind}_correlation_missing",
                    f"{correlation_kind}_turn_id": ref.turn_id,
                    "task_id": task_id,
                    "missing": "parent_turn_id",
                },
            }
        if correlated_parent_turn_id != parent_turn.turn_id:
            continue
        if task_id in turns:
            return {
                "turns": {},
                "error": {
                    "error": f"duplicate_{correlation_kind}_task_id",
                    "task_id": task_id,
                    f"{correlation_kind}_turn_ids": [turns[task_id]["ref"].turn_id, ref.turn_id],
                },
            }
        turns[task_id] = {"task_id": task_id, "ref": ref, "snapshot": snapshot}
    return {"turns": turns, "error": None}


def _task_dispatch_keys(parent_turn_id: str, namespace: str, task_ids: tuple[str, ...]) -> dict[str, str]:
    keys: dict[str, str] = {}
    for task_id in task_ids:
        keys[task_id] = normalize_dispatch_anchor(
            f"{parent_turn_id}:{namespace}:{task_id}",
            field_name=f"{namespace} dispatch_key for task_id {task_id!r}",
        )
    return keys


def _validate_correlated_targets(
    entries: tuple[dict[str, Any], ...],
    expected_targets_by_task_id: dict[str, AgentRef],
    *,
    correlation_kind: str,
) -> dict[str, Any] | None:
    for entry in entries:
        task_id = entry["task_id"]
        actual = _snapshot_target_agent_ref(entry["snapshot"])
        if actual is None:
            continue
        expected = expected_targets_by_task_id[task_id]
        if actual != expected:
            return {
                "error": f"{correlation_kind}_target_agent_mismatch",
                "task_id": task_id,
                f"{correlation_kind}_turn_id": entry["ref"].turn_id,
                "expected_target_agent_ref": _agent_ref_payload(expected),
                "actual_target_agent_ref": _agent_ref_payload(actual),
            }
    return None


def _snapshot_target_agent_ref(snapshot) -> AgentRef | None:
    target = getattr(snapshot, "target_agent", None)
    if isinstance(target, AgentRef):
        return target
    if isinstance(target, dict):
        project_id = target.get("project_id")
        agent_id = target.get("agent_id")
    else:
        project_id = getattr(target, "project_id", None)
        agent_id = getattr(target, "agent_id", None)
    if isinstance(project_id, str) and isinstance(agent_id, str):
        return AgentRef(project_id=project_id, agent_id=agent_id)
    return None


def _agent_ref_payload(agent: AgentRef) -> dict[str, str]:
    return {"project_id": agent.project_id, "agent_id": agent.agent_id}


def _execution_plan_from_context(context: TurnContext) -> tuple[dict, ...] | None:
    for item in reversed(context.semantic_items):
        if item.record.record_role != "progress":
            continue
        payload = item.content.payload()
        if not isinstance(payload, dict) or payload.get("phase") != EXECUTION_PLAN_PHASE:
            continue
        work_orders = payload.get("work_orders")
        if not isinstance(work_orders, list) or not work_orders:
            return None
        return tuple(work_order for work_order in work_orders if isinstance(work_order, dict)) or None
    return None


def _targeted_work_order(work_order: dict, *, parent_turn_id: str, target_agent: AgentRef) -> dict:
    targeted = dict(work_order)
    provenance = targeted.get("provenance")
    if not isinstance(provenance, dict):
        provenance = {}
    targeted["provenance"] = {
        **provenance,
        "task_id": targeted.get("task_id"),
        "parent_turn_id": parent_turn_id,
        "target_agent_ref": {"project_id": target_agent.project_id, "agent_id": target_agent.agent_id},
    }
    return targeted


def _turn_task_correlation(context: TurnContext) -> dict[str, str | None]:
    bootstrap = _payload_by_role(context, "bootstrap")
    if isinstance(bootstrap, dict):
        work_order = bootstrap.get("work_order")
        if isinstance(work_order, dict):
            task_id = _non_empty_str(work_order.get("task_id"))
            parent_turn_id = _non_empty_str(work_order.get("parent_turn_id")) or _non_empty_str(bootstrap.get("parent_turn_id"))
            lifecycle = bootstrap.get("lifecycle")
            if parent_turn_id is None and isinstance(lifecycle, dict):
                parent_turn_id = _non_empty_str(lifecycle.get("source_turn_id"))
            if task_id is not None:
                return {"task_id": task_id, "parent_turn_id": parent_turn_id}
        task_id = _non_empty_str(bootstrap.get("task_id"))
        provenance = bootstrap.get("provenance")
        parent_turn_id = _non_empty_str(bootstrap.get("parent_turn_id"))
        if parent_turn_id is None and isinstance(provenance, dict):
            parent_turn_id = _non_empty_str(provenance.get("parent_turn_id"))
        if task_id is not None:
            return {"task_id": task_id, "parent_turn_id": parent_turn_id}
    deliverable = _payload_by_role(context, "deliverable", last=True)
    if isinstance(deliverable, dict):
        task_id = _non_empty_str(deliverable.get("task_id"))
        parent_turn_id = _non_empty_str(deliverable.get("parent_turn_id"))
        provenance = deliverable.get("provenance")
        if parent_turn_id is None and isinstance(provenance, dict):
            parent_turn_id = _non_empty_str(provenance.get("parent_turn_id"))
        if task_id is not None:
            return {"task_id": task_id, "parent_turn_id": parent_turn_id}
    return {"task_id": None, "parent_turn_id": None}


def _payload_by_role(context: TurnContext, role: str, *, last: bool = False) -> Any:
    items = reversed(context.semantic_items) if last else context.semantic_items
    for item in items:
        if item.record.record_role == role:
            return item.content.payload()
    return None


def _non_empty_str(value: Any) -> str | None:
    if isinstance(value, str) and value:
        return value
    return None


def _new_agent_ref_from_provision_payload(payload, *, project_id: str) -> AgentRef | None:
    if not isinstance(payload, dict):
        return None
    raw_ref = payload.get("new_agent_ref")
    if not isinstance(raw_ref, dict):
        return None
    agent_id = raw_ref.get("agent_id")
    ref_project_id = raw_ref.get("project_id", project_id)
    if not isinstance(agent_id, str) or not isinstance(ref_project_id, str):
        return None
    if ref_project_id != project_id:
        return None
    return AgentRef(project_id=ref_project_id, agent_id=agent_id)


def _is_expired_running_child(snapshot, *, now: datetime) -> bool:
    if snapshot.outcome is not None:
        return False
    claim_expires_at = getattr(snapshot, "claim_expires_at", None)
    if claim_expires_at is None:
        return False
    return claim_expires_at <= now


def _request_stop_for_timed_out_children(
    client: HttpAgentClient,
    *,
    requested_by: AgentRef,
    child_refs: tuple[TurnRef, ...],
    timed_out_child_turn_ids: list[str],
    parent_turn_id: str,
) -> tuple[list[str], list[dict[str, str]]]:
    if not timed_out_child_turn_ids:
        return [], []
    timed_out = set(timed_out_child_turn_ids)
    stopped: list[str] = []
    errors: list[dict[str, str]] = []
    for child_ref in child_refs:
        if child_ref.turn_id not in timed_out:
            continue
        try:
            client.request_stop_turn(
                child_ref,
                requested_by=requested_by,
                reason="parent_partial_child_timeout",
                note=f"parent {parent_turn_id} completed partial synthesis without this timed-out child",
                meta=OperationMeta(
                    reason="parent_partial_child_timeout",
                    note=f"parent {parent_turn_id} completed partial synthesis without this timed-out child",
                    annotations={
                        "nanobot_orchestration": {
                            "parent_turn_id": parent_turn_id,
                            "child_turn_id": child_ref.turn_id,
                            "decision": "parent_partial_completed",
                        }
                    },
                ),
            )
            stopped.append(child_ref.turn_id)
        except Exception as exc:  # Cleanup should not prevent the parent from returning a partial result.
            errors.append({"child_turn_id": child_ref.turn_id, "error": str(exc)})
    return stopped, errors
