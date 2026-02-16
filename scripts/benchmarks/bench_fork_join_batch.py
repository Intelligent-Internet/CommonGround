"""
Bench: fork_join + BatchManager + L0 mailbox scheduling (no external tools).

This script is designed to isolate PMO/L0/Worker scheduling behavior:
- It calls PMO internal `fork_join` directly via NATS + CardBox tool.call cards.
- Child agents run a mock profile (LLM model "mock-direct") and always submit_result.
- No external tools (web_search, etc.) are invoked.

Usage (example):
  CG__worker__max_concurrency=32 CG__worker__worker_targets='["bench_worker"]' \\
    uv run -m scripts.benchmarks.bench_fork_join_batch --project proj_bench_001 --channel public \\
      --agents 16 --rounds 5 --parallel-batches 2 --disjoint-batches
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import uuid6

from core.config import PROTOCOL_VERSION
from core.errors import BadRequestError
from core.headers import ensure_recursion_depth
from core.trace import ensure_trace_headers, next_traceparent
from core.utp_protocol import Card, ToolCallContent, ToolResultContent
from core.utils import safe_str
from infra.cardbox_client import CardBoxClient
from infra.nats_client import NATSClient
from infra.project_bootstrap import ensure_project, ensure_schema, seed_tools
from infra.stores import ExecutionStore, ProfileStore, ProjectStore, ResourceStore, StateStore
from infra.stores.pool import create_pool
from services.api.profile_service import ProfileService
from services.api.profile_yaml import normalize_profile_yaml
from scripts.utils.config import load_toml_config, require_cardbox_dsn


REPO_ROOT = Path(__file__).resolve().parents[2]


def _iso(dt: Optional[datetime]) -> str:
    if not dt:
        return ""
    try:
        return dt.isoformat()
    except Exception:
        return str(dt)


def _ms_from_s(value_s: Optional[float]) -> Optional[int]:
    if value_s is None:
        return None
    return int(round(value_s * 1000.0))


def _ms_between(a: Optional[datetime], b: Optional[datetime]) -> Optional[int]:
    if not a or not b:
        return None
    try:
        return int(round((b - a).total_seconds() * 1000.0))
    except Exception:
        return None


def _percentile_ms(values_ms: Sequence[int], pct: float) -> Optional[int]:
    if not values_ms:
        return None
    pct = float(pct)
    if pct <= 0:
        return int(min(values_ms))
    if pct >= 100:
        return int(max(values_ms))
    ordered = sorted(int(v) for v in values_ms)
    idx = int((pct / 100.0) * (len(ordered) - 1))
    return ordered[idx]


def _render_table(headers: List[str], rows: List[List[str]]) -> str:
    if not rows:
        return ""
    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))
    header_line = " | ".join(h.ljust(widths[i]) for i, h in enumerate(headers))
    sep = "-+-".join("-" * widths[i] for i in range(len(headers)))
    body = [" | ".join(c.ljust(widths[i]) for i, c in enumerate(row)) for row in rows]
    return "\n".join([header_line, sep, *body])


@dataclass(frozen=True, slots=True)
class BatchRunTimings:
    wave: int
    batch_index: int
    tool_call_id: str
    trace_id: str
    batch_id: str
    batch_status: str
    task_count: int
    t0_s: float
    t_saved_tool_call_s: float
    t_published_s: float
    t_received_tool_result_s: float
    t_loaded_tool_result_s: float
    t_analyzed_s: float
    error_code: str = ""
    error_message: str = ""

    def as_row(self) -> List[str]:
        return [
            str(self.wave),
            str(self.batch_index),
            self.batch_id,
            self.batch_status,
            str(self.task_count),
            str(_ms_from_s(self.t_saved_tool_call_s - self.t0_s) or ""),
            str(_ms_from_s(self.t_published_s - self.t_saved_tool_call_s) or ""),
            str(_ms_from_s(self.t_received_tool_result_s - self.t_published_s) or ""),
            str(_ms_from_s(self.t_loaded_tool_result_s - self.t_received_tool_result_s) or ""),
            str(_ms_from_s(self.t_analyzed_s - self.t_loaded_tool_result_s) or ""),
            str(_ms_from_s(self.t_analyzed_s - self.t0_s) or ""),
            self.trace_id,
            self.tool_call_id,
        ]


@dataclass(frozen=True, slots=True)
class ChildTurnTiming:
    wave: int
    batch_id: str
    agent_id: str
    batch_task_id: str
    child_turn_id: str
    status: str
    attempt_count: int
    inbox_status: Optional[str]
    inbox_created_at: Optional[datetime]
    inbox_processed_at: Optional[datetime]
    step_started_at: Optional[datetime]
    step_ended_at: Optional[datetime]
    step_count: Optional[int]
    turn_epoch: Optional[int]

    def queue_delay_ms(self) -> Optional[int]:
        return _ms_between(self.inbox_created_at, self.step_started_at)

    def step_ms(self) -> Optional[int]:
        return _ms_between(self.step_started_at, self.step_ended_at)

    def e2e_ms(self) -> Optional[int]:
        return _ms_between(self.inbox_created_at, self.step_ended_at)


async def _wait_for_tool_result_inbox(
    *,
    execution_store: ExecutionStore,
    project_id: str,
    agent_id: str,
    tool_call_id: str,
    timeout_s: float,
    poll_interval_s: float,
) -> Tuple[Dict[str, Any], Optional[datetime]]:
    deadline = asyncio.get_event_loop().time() + float(timeout_s)
    while True:
        remaining = deadline - asyncio.get_event_loop().time()
        if remaining <= 0:
            raise TimeoutError("timeout waiting for tool_result inbox")
        rows = await execution_store.list_inbox_by_correlation(
            project_id=project_id,
            agent_id=agent_id,
            correlation_id=tool_call_id,
            limit=20,
        )
        for row in rows:
            if row.get("message_type") != "tool_result":
                continue
            inbox_id = safe_str(row.get("inbox_id"))
            created_at = row.get("created_at") if isinstance(row.get("created_at"), datetime) else None
            if inbox_id:
                await execution_store.update_inbox_status(
                    inbox_id=inbox_id,
                    project_id=project_id,
                    status="consumed",
                )
            payload = row.get("payload") or {}
            if isinstance(payload, dict):
                return dict(payload), created_at
        await asyncio.sleep(min(float(poll_interval_s), max(0.05, remaining)))


async def _call_pmo_internal_tool(
    *,
    cardbox: CardBoxClient,
    execution_store: ExecutionStore,
    nats: NATSClient,
    project_id: str,
    channel_id: str,
    caller_agent_id: str,
    tool_name: str,
    tool_args: Dict[str, Any],
    after_execution: str,
    timeout_s: float,
    poll_interval_s: float,
    trace_id: Optional[str] = None,
) -> Tuple[str, str, Dict[str, Any], Optional[str], BatchRunTimings, Optional[datetime]]:
    t0_s = time.perf_counter()
    tool_call_id = f"call_{uuid6.uuid7().hex}"
    step_id = f"step_bench_{uuid6.uuid7().hex}"
    agent_turn_id = f"turn_sys_{uuid6.uuid7().hex}"
    trace_id = trace_id or str(uuid6.uuid7())

    pmo_subject = f"cg.{PROTOCOL_VERSION}.{project_id}.{channel_id}.cmd.sys.pmo.internal.{tool_name}"
    tool_call_card = Card(
        card_id=uuid6.uuid7().hex,
        project_id=project_id,
        type="tool.call",
        content=ToolCallContent(
            tool_name=tool_name,
            arguments=tool_args,
            status="called",
            target_subject=pmo_subject,
        ),
        created_at=datetime.now(UTC),
        author_id=caller_agent_id,
        metadata={
            "agent_turn_id": agent_turn_id,
            "step_id": step_id,
            "tool_call_id": tool_call_id,
            "role": "assistant",
            "trace_id": trace_id,
        },
        tool_call_id=tool_call_id,
    )
    await cardbox.save_card(tool_call_card)
    t_saved_tool_call_s = time.perf_counter()

    payload: Dict[str, Any] = {
        "agent_id": caller_agent_id,
        "agent_turn_id": agent_turn_id,
        "turn_epoch": 1,
        "tool_call_id": tool_call_id,
        "tool_name": tool_name,
        "after_execution": after_execution,
        "tool_call_card_id": tool_call_card.card_id,
        "step_id": step_id,
    }
    headers, _, trace_id = ensure_trace_headers({}, trace_id=trace_id)
    headers = ensure_recursion_depth(headers, default_depth=0)
    traceparent, trace_id = next_traceparent(headers.get("traceparent"), trace_id)
    headers = dict(headers)
    headers["traceparent"] = traceparent

    # Create an execution edge for correlation_id -> recursion_depth lookup (PMO tool_result report).
    await execution_store.insert_execution_edge(
        edge_id=f"edge_{uuid6.uuid7().hex}",
        project_id=project_id,
        channel_id=channel_id,
        primitive="enqueue",
        edge_phase="request",
        source_agent_id=caller_agent_id,
        source_agent_turn_id=agent_turn_id,
        source_step_id=step_id,
        target_agent_id="sys.pmo",
        target_agent_turn_id=None,
        correlation_id=tool_call_id,
        enqueue_mode="call",
        recursion_depth=int(headers.get("CG-Recursion-Depth", "0")),
        trace_id=trace_id,
        parent_step_id=step_id,
        metadata={
            "message_type": "tool_call",
            "enqueue_mode": "call",
            "inbox_id": tool_call_id,
            "recursion_depth": int(headers.get("CG-Recursion-Depth", "0")),
        },
    )

    await nats.publish_event(pmo_subject, payload, headers=headers)
    t_published_s = time.perf_counter()

    inbox_payload, tool_result_inbox_created_at = await _wait_for_tool_result_inbox(
        execution_store=execution_store,
        project_id=project_id,
        agent_id=caller_agent_id,
        tool_call_id=tool_call_id,
        timeout_s=timeout_s,
        poll_interval_s=poll_interval_s,
    )
    t_received_tool_result_s = time.perf_counter()

    tool_result: Dict[str, Any] = {}
    tool_result_card_id = safe_str(inbox_payload.get("tool_result_card_id"))
    if tool_result_card_id:
        cards = await cardbox.get_cards([tool_result_card_id], project_id=project_id)
        tr = cards[0] if cards else None
        if tr:
            content_obj = getattr(tr, "content", None)
            if isinstance(content_obj, ToolResultContent):
                tool_result = {
                    "status": content_obj.status,
                    "after_execution": content_obj.after_execution,
                    "result": content_obj.result,
                    "error": content_obj.error,
                }
            elif isinstance(content_obj, dict):
                tool_result = dict(content_obj)
            else:
                tool_result = {"status": "failed", "result": {"error_message": "tool.result content not dict"}}
    t_loaded_tool_result_s = time.perf_counter()

    # Fill once we know the batch fields (caller will patch it).
    timings = BatchRunTimings(
        wave=0,
        batch_index=0,
        tool_call_id=tool_call_id,
        trace_id=trace_id,
        batch_id="",
        batch_status="",
        task_count=0,
        t0_s=t0_s,
        t_saved_tool_call_s=t_saved_tool_call_s,
        t_published_s=t_published_s,
        t_received_tool_result_s=t_received_tool_result_s,
        t_loaded_tool_result_s=t_loaded_tool_result_s,
        t_analyzed_s=t_loaded_tool_result_s,
        error_code="",
        error_message="",
    )
    return tool_call_id, step_id, tool_result, tool_result_card_id or None, timings, tool_result_inbox_created_at


async def _ensure_profiles(
    *,
    project_id: str,
    pool: Any,
    cardbox: CardBoxClient,
    caller_profile_name: str,
    worker_profile_name: str,
    worker_target: str,
    force_update: bool,
) -> Tuple[str, str]:
    project_store = ProjectStore(pool=pool)
    profile_store = ProfileStore(pool=pool)
    service = ProfileService(project_store=project_store, profile_store=profile_store, cardbox=cardbox)

    async def _ensure_one(name: str, data: Dict[str, Any]) -> str:
        existing = await profile_store.get_profile_by_name(project_id=project_id, name=name)
        if existing and not force_update:
            return safe_str(existing.get("profile_box_id"))
        normalized = normalize_profile_yaml(data)
        created = await service.create_profile_from_yaml(project_id=project_id, profile=normalized)
        return str(created.profile_box_id)

    caller_box_id = await _ensure_one(
        caller_profile_name,
        {
            "name": caller_profile_name,
            "worker_target": worker_target,
            "tags": ["bench", "bench_caller"],
            "description": "Bench caller profile (delegation_policy only).",
            "system_prompt_template": "",
            "allowed_tools": ["fork_join", "submit_result"],
            "must_end_with": ["submit_result"],
            "llm_config": {"model": "mock-direct"},
            "delegation_policy": {
                "target_tags": [],
                "target_profiles": [worker_profile_name],
            },
        },
    )
    if not caller_box_id:
        raise RuntimeError("failed to ensure caller profile")

    worker_box_id = await _ensure_one(
        worker_profile_name,
        {
            "name": worker_profile_name,
            "worker_target": worker_target,
            "tags": ["bench", "bench_worker"],
            "description": "Bench worker profile (mock-direct).",
            "system_prompt_template": "You are a bench worker. Always submit_result with output=ok.",
            "allowed_tools": [],
            "must_end_with": ["submit_result"],
            "llm_config": {"model": "mock-direct"},
        },
    )
    if not worker_box_id:
        raise RuntimeError("failed to ensure worker profile")

    return caller_box_id, worker_box_id


async def _ensure_roster_agents(
    *,
    project_id: str,
    pool: Any,
    state_store: StateStore,
    resource_store: ResourceStore,
    agent_ids: Iterable[str],
    profile_box_id: str,
    profile_name: str,
    worker_target: str,
    tags: List[str],
) -> None:
    for agent_id in agent_ids:
        await resource_store.upsert_project_agent(
            project_id=project_id,
            agent_id=str(agent_id),
            profile_box_id=str(profile_box_id),
            worker_target=str(worker_target),
            tags=list(tags),
            display_name=str(agent_id),
            owner_agent_id=None,
            metadata={"profile_name": str(profile_name)},
        )
        await state_store.init_if_absent(
            project_id=project_id,
            agent_id=str(agent_id),
            active_channel_id=None,
            profile_box_id=str(profile_box_id),
        )


async def _fetch_child_timings(
    *,
    pool: Any,
    project_id: str,
    pairs: List[Tuple[str, str]],
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    """Return ({turn_id: inbox_row}, {turn_id: step_agg_row})."""
    if not pairs:
        return {}, {}
    turn_ids = [turn_id for _agent_id, turn_id in pairs if turn_id]
    if not turn_ids:
        return {}, {}

    inbox_by_turn: Dict[str, Dict[str, Any]] = {}
    steps_by_turn: Dict[str, Dict[str, Any]] = {}

    async with pool.connection() as conn:
        res = await conn.execute(
            """
            SELECT agent_id, correlation_id, status, created_at, processed_at, payload
            FROM state.agent_inbox
            WHERE project_id=%s
              AND message_type='turn'
              AND correlation_id = ANY(%s)
            """,
            (project_id, turn_ids),
        )
        for row in await res.fetchall():
            cid = safe_str(row.get("correlation_id"))
            if cid:
                inbox_by_turn[cid] = dict(row)

        res = await conn.execute(
            """
            SELECT agent_id,
                   agent_turn_id,
                   MIN(started_at) AS step_started_at,
                   MAX(ended_at) AS step_ended_at,
                   COUNT(*)::int AS step_count
            FROM state.agent_steps
            WHERE project_id=%s
              AND agent_turn_id = ANY(%s)
            GROUP BY agent_id, agent_turn_id
            """,
            (project_id, turn_ids),
        )
        for row in await res.fetchall():
            tid = safe_str(row.get("agent_turn_id"))
            if tid:
                steps_by_turn[tid] = dict(row)

    return inbox_by_turn, steps_by_turn


async def _fetch_batch_rows_by_tool_call(
    *,
    pool: Any,
    project_id: str,
    tool_call_id: str,
) -> Tuple[str, str, List[Dict[str, Any]]]:
    """Return (batch_id, batch_status, task_rows) for a parent fork_join tool_call_id."""
    if not tool_call_id:
        return "", "", []

    async with pool.connection() as conn:
        res = await conn.execute(
            """
            SELECT batch_id, status
            FROM state.pmo_batches
            WHERE project_id=%s
              AND parent_tool_call_id=%s
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (project_id, tool_call_id),
        )
        batch_row = await res.fetchone()
        if not batch_row:
            return "", "", []
        batch_id = safe_str(batch_row.get("batch_id"))
        batch_status = safe_str(batch_row.get("status"))
        if not batch_id:
            return "", batch_status, []

        res = await conn.execute(
            """
            SELECT batch_task_id, agent_id, current_agent_turn_id, status, attempt_count
            FROM state.pmo_batch_tasks
            WHERE project_id=%s
              AND batch_id=%s
            ORDER BY created_at ASC
            """,
            (project_id, batch_id),
        )
        rows = [dict(row) for row in await res.fetchall()]
    return batch_id, batch_status, rows


def _require_distinct(values: Sequence[str], *, label: str) -> None:
    seen: set[str] = set()
    dups: set[str] = set()
    for v in values:
        if v in seen:
            dups.add(v)
        seen.add(v)
    if dups:
        raise BadRequestError(f"{label} must be distinct; duplicates={sorted(dups)}")


async def _run_one_batch(
    *,
    wave: int,
    batch_index: int,
    cardbox: CardBoxClient,
    execution_store: ExecutionStore,
    nats: NATSClient,
    pool: Any,
    project_id: str,
    channel_id: str,
    caller_agent_id: str,
    agent_ids: List[str],
    deadline_seconds: Optional[float],
    poll_interval_s: float,
    timeout_s: float,
) -> Tuple[BatchRunTimings, List[ChildTurnTiming]]:
    _require_distinct(agent_ids, label="agent_ids (within a batch)")

    instruction = "ping"
    tasks: List[Dict[str, Any]] = []
    for idx, agent_id in enumerate(agent_ids, start=1):
        task: Dict[str, Any] = {
            "target_strategy": "reuse",
            "target_ref": str(agent_id),
            "instruction": instruction,
        }
        tasks.append(task)

    tool_args: Dict[str, Any] = {
        "tasks": tasks,
        "fail_fast": True,
    }
    if deadline_seconds is not None and float(deadline_seconds) > 0:
        tool_args["deadline_seconds"] = float(deadline_seconds)

    tool_call_id, _step_id, tool_result, _tool_result_card_id, timings, _tool_inbox_created_at = await _call_pmo_internal_tool(
        cardbox=cardbox,
        execution_store=execution_store,
        nats=nats,
        project_id=project_id,
        channel_id=channel_id,
        caller_agent_id=caller_agent_id,
        tool_name="fork_join",
        tool_args=tool_args,
        after_execution="suspend",
        timeout_s=timeout_s,
        poll_interval_s=poll_interval_s,
    )

    result_obj = tool_result.get("result") if isinstance(tool_result, dict) else None
    if not isinstance(result_obj, dict):
        raise RuntimeError(f"unexpected fork_join tool.result payload: {tool_result!r}")

    error_code = ""
    error_message = ""
    error_obj = tool_result.get("error") if isinstance(tool_result, dict) else None
    if isinstance(error_obj, dict):
        error_code = safe_str(error_obj.get("code") or error_obj.get("error_code"))
        error_message = safe_str(error_obj.get("message") or error_obj.get("error_message"))
    if not error_code:
        error_code = safe_str(result_obj.get("error_code"))
    if not error_message:
        error_message = safe_str(result_obj.get("error_message"))

    batch_id = safe_str(result_obj.get("batch_id"))
    batch_status = safe_str(result_obj.get("status")) or safe_str(tool_result.get("status"))
    tasks_out = result_obj.get("tasks")
    tasks_list = tasks_out if isinstance(tasks_out, list) else []
    if not tasks_list:
        db_batch_id, db_batch_status, db_tasks = await _fetch_batch_rows_by_tool_call(
            pool=pool,
            project_id=project_id,
            tool_call_id=tool_call_id,
        )
        if db_batch_id and not batch_id:
            batch_id = db_batch_id
        if db_batch_status and not batch_status:
            batch_status = db_batch_status
        tasks_list = db_tasks

    child_pairs: List[Tuple[str, str]] = []
    child_rows: List[ChildTurnTiming] = []
    for t in tasks_list:
        if not isinstance(t, dict):
            continue
        agent_id = safe_str(t.get("agent_id"))
        batch_task_id = safe_str(t.get("batch_task_id"))
        child_turn_id = safe_str(t.get("current_agent_turn_id") or t.get("agent_turn_id"))
        status = safe_str(t.get("status"))
        attempt_count_raw = t.get("attempt_count")
        try:
            attempt_count = int(attempt_count_raw) if attempt_count_raw is not None else 0
        except Exception:
            attempt_count = 0
        if agent_id and child_turn_id:
            child_pairs.append((agent_id, child_turn_id))
        child_rows.append(
            ChildTurnTiming(
                wave=wave,
                batch_id=batch_id,
                agent_id=agent_id,
                batch_task_id=batch_task_id,
                child_turn_id=child_turn_id,
                status=status,
                attempt_count=attempt_count,
                inbox_status=None,
                inbox_created_at=None,
                inbox_processed_at=None,
                step_started_at=None,
                step_ended_at=None,
                step_count=None,
                turn_epoch=None,
            )
        )

    inbox_by_turn, steps_by_turn = await _fetch_child_timings(pool=pool, project_id=project_id, pairs=child_pairs)

    enriched: List[ChildTurnTiming] = []
    for row in child_rows:
        inbox = inbox_by_turn.get(row.child_turn_id) if row.child_turn_id else None
        steps = steps_by_turn.get(row.child_turn_id) if row.child_turn_id else None
        inbox_payload = inbox.get("payload") if isinstance(inbox, dict) else None
        epoch = None
        if isinstance(inbox_payload, dict) and inbox_payload.get("turn_epoch") is not None:
            try:
                epoch = int(inbox_payload.get("turn_epoch"))
            except Exception:
                epoch = None
        enriched.append(
            ChildTurnTiming(
                wave=row.wave,
                batch_id=row.batch_id,
                agent_id=row.agent_id,
                batch_task_id=row.batch_task_id,
                child_turn_id=row.child_turn_id,
                status=row.status,
                attempt_count=row.attempt_count,
                inbox_status=safe_str(inbox.get("status")) if isinstance(inbox, dict) else None,
                inbox_created_at=inbox.get("created_at") if isinstance(inbox, dict) else None,
                inbox_processed_at=inbox.get("processed_at") if isinstance(inbox, dict) else None,
                step_started_at=steps.get("step_started_at") if isinstance(steps, dict) else None,
                step_ended_at=steps.get("step_ended_at") if isinstance(steps, dict) else None,
                step_count=int(steps.get("step_count")) if isinstance(steps, dict) and steps.get("step_count") is not None else None,
                turn_epoch=epoch,
            )
        )

    t_analyzed_s = time.perf_counter()
    timings = BatchRunTimings(
        wave=wave,
        batch_index=batch_index,
        tool_call_id=tool_call_id,
        trace_id=timings.trace_id,
        batch_id=batch_id,
        batch_status=batch_status or safe_str(tool_result.get("status")),
        task_count=len(enriched),
        t0_s=timings.t0_s,
        t_saved_tool_call_s=timings.t_saved_tool_call_s,
        t_published_s=timings.t_published_s,
        t_received_tool_result_s=timings.t_received_tool_result_s,
        t_loaded_tool_result_s=timings.t_loaded_tool_result_s,
        t_analyzed_s=t_analyzed_s,
        error_code=error_code,
        error_message=error_message,
    )
    return timings, enriched


def _summarize_child_metrics(rows: List[ChildTurnTiming]) -> Dict[str, Any]:
    q = [v for v in (r.queue_delay_ms() for r in rows) if isinstance(v, int)]
    step = [v for v in (r.step_ms() for r in rows) if isinstance(v, int)]
    e2e = [v for v in (r.e2e_ms() for r in rows) if isinstance(v, int)]

    def _summary(values: List[int]) -> Dict[str, Any]:
        if not values:
            return {}
        return {
            "min": min(values),
            "p50": _percentile_ms(values, 50),
            "p95": _percentile_ms(values, 95),
            "max": max(values),
        }

    return {
        "queue_delay_ms": _summary(q),
        "step_ms": _summary(step),
        "e2e_ms": _summary(e2e),
    }


def _print_child_summary(*, wave: int, batch_id: str, rows: List[ChildTurnTiming]) -> None:
    metrics = _summarize_child_metrics(rows)
    q = metrics.get("queue_delay_ms") or {}
    s = metrics.get("step_ms") or {}
    e = metrics.get("e2e_ms") or {}
    print(
        f"[bench] child metrics wave={wave} batch={batch_id} "
        f"queue_ms(min/p50/p95/max)={q.get('min')}/{q.get('p50')}/{q.get('p95')}/{q.get('max')} "
        f"step_ms(min/p50/p95/max)={s.get('min')}/{s.get('p50')}/{s.get('p95')}/{s.get('max')} "
        f"e2e_ms(min/p50/p95/max)={e.get('min')}/{e.get('p50')}/{e.get('p95')}/{e.get('max')}"
    )


def _print_slowest_tasks(*, rows: List[ChildTurnTiming], limit: int) -> None:
    enriched: List[Tuple[int, ChildTurnTiming]] = []
    for r in rows:
        e2e = r.e2e_ms()
        if isinstance(e2e, int):
            enriched.append((e2e, r))
    if not enriched:
        return
    enriched.sort(key=lambda x: x[0], reverse=True)
    top = [r for _e2e, r in enriched[: max(1, int(limit))]]
    table_rows: List[List[str]] = []
    for r in top:
        table_rows.append(
            [
                safe_str(r.agent_id),
                safe_str(r.status),
                safe_str(r.child_turn_id),
                safe_str(r.inbox_status),
                safe_str(r.turn_epoch) if r.turn_epoch is not None else "",
                safe_str(r.queue_delay_ms() or ""),
                safe_str(r.step_ms() or ""),
                safe_str(r.e2e_ms() or ""),
                safe_str(r.step_count) if r.step_count is not None else "",
                _iso(r.inbox_created_at),
                _iso(r.step_started_at),
                _iso(r.step_ended_at),
            ]
        )
    print(_render_table(
        [
            "agent_id",
            "status",
            "turn_id",
            "inbox",
            "epoch",
            "queue_ms",
            "step_ms",
            "e2e_ms",
            "steps",
            "inbox_created_at",
            "step_started_at",
            "step_ended_at",
        ],
        table_rows,
    ))


async def main() -> None:
    parser = argparse.ArgumentParser(description="Bench fork_join + batch scheduling (mock agents, no external tools).")
    parser.add_argument("--project", required=True)
    parser.add_argument("--channel", default="public")
    parser.add_argument("--agents", type=int, default=8, help="tasks per batch (and agent count when reuse)")
    parser.add_argument("--rounds", type=int, default=3, help="number of waves")
    parser.add_argument("--parallel-batches", type=int, default=1, help="batches per wave")
    parser.add_argument(
        "--disjoint-batches",
        action="store_true",
        help="use disjoint agent sets per parallel batch (avoids cross-batch contention)",
    )
    parser.add_argument(
        "--overlap-waves",
        action="store_true",
        help="dispatch all waves without waiting for completion (creates backlog pressure)",
    )
    parser.add_argument(
        "--conversation-mode",
        choices=["continue", "fresh"],
        default="continue",
        help="deprecated: kept for backward compatibility; latest fork_join ignores this",
    )
    parser.add_argument(
        "--suppress-instruction",
        action="store_true",
        help="deprecated: latest fork_join requires non-empty tasks[].instruction",
    )
    parser.add_argument("--deadline-seconds", type=float, default=None)
    parser.add_argument("--timeout-s", type=float, default=300.0)
    parser.add_argument("--poll-interval-ms", type=int, default=100)
    parser.add_argument("--ensure-schema", action="store_true", help="run SQL bootstrap (init_db.sql etc)")
    parser.add_argument("--force-profile-update", action="store_true", help="overwrite bench profiles even if present")
    parser.add_argument(
        "--worker-target",
        default="worker_generic",
        help="recommended: use a dedicated target (e.g. bench_worker) with a dedicated agent_worker instance",
    )
    parser.add_argument("--caller-agent-id", default="bench_caller")
    parser.add_argument("--agent-prefix", default="bench_agent")
    parser.add_argument("--slowest-k", type=int, default=10, help="print top-K slowest child turns per batch")
    parser.add_argument("--json-out", default="", help="optional JSON dump path for analysis")
    args = parser.parse_args()

    project_id = str(args.project)
    channel_id = str(args.channel)
    agents = max(1, int(args.agents))
    rounds = max(1, int(args.rounds))
    parallel_batches = max(1, int(args.parallel_batches))
    poll_interval_s = max(0.01, float(args.poll_interval_ms) / 1000.0)

    conversation_mode = str(args.conversation_mode)
    suppress_instruction = bool(args.suppress_instruction)
    if conversation_mode != "continue":
        print("[bench] --conversation-mode is deprecated and ignored by latest fork_join.")
    if suppress_instruction:
        print(
            "[bench] --suppress-instruction is deprecated and ignored: "
            "latest fork_join requires non-empty tasks[].instruction."
        )
        suppress_instruction = False

    cfg = load_toml_config()
    if not cfg:
        raise RuntimeError("config.toml not found")
    pmo_cfg = cfg.get("pmo", {}) if isinstance(cfg, dict) else {}
    configured_max_tasks_raw = pmo_cfg.get("fork_join_max_tasks") if isinstance(pmo_cfg, dict) else None
    configured_max_tasks: Optional[int] = None
    try:
        if configured_max_tasks_raw is not None:
            configured_max_tasks = int(configured_max_tasks_raw)
    except Exception:
        configured_max_tasks = None
    if configured_max_tasks is not None and configured_max_tasks > 0 and agents > configured_max_tasks:
        print(
            "[bench] warning: --agents exceeds configured [pmo].fork_join_max_tasks "
            f"({agents} > {configured_max_tasks}). "
            "fork_join may fail with bad_request unless PMO runtime config is increased."
        )
        suggested_batches = (agents + configured_max_tasks - 1) // configured_max_tasks
        print(
            "[bench] hint: increase [pmo].fork_join_max_tasks and restart PMO, "
            f"or use --agents {configured_max_tasks} --parallel-batches {suggested_batches} --disjoint-batches."
        )
    dsn = require_cardbox_dsn(cfg, error="cardbox.postgres_dsn missing in config.toml")

    pool = create_pool(dsn, min_size=1, max_size=10)
    await pool.open()

    try:
        if args.ensure_schema:
            print("[bench] ensuring schema...")
            await ensure_schema(pool)

        print(f"[bench] ensuring project={project_id!r} tools...")
        await ensure_project(pool, project_id=project_id, title="Bench Project", owner_id="bench")
        await seed_tools(pool, project_id=project_id)

        cardbox = CardBoxClient(config=cfg.get("cardbox", {}))
        await cardbox.init()

        try:
            resource_store = ResourceStore(pool=pool)
            execution_store = ExecutionStore(pool=pool)
            state_store = StateStore(pool=pool)
            await execution_store.open()

            caller_profile_name = "Bench_ForkJoin_Caller"
            worker_profile_name = "Bench_ForkJoin_Worker"
            print(f"[bench] ensuring profiles caller={caller_profile_name!r} worker={worker_profile_name!r}...")
            caller_profile_box_id, worker_profile_box_id = await _ensure_profiles(
                project_id=project_id,
                pool=pool,
                cardbox=cardbox,
                caller_profile_name=caller_profile_name,
                worker_profile_name=worker_profile_name,
                worker_target=str(args.worker_target),
                force_update=bool(args.force_profile_update),
            )
            print(f"[bench] caller profile_box_id={caller_profile_box_id}")
            print(f"[bench] worker profile_box_id={worker_profile_box_id}")

            caller_agent_id = str(args.caller_agent_id)
            await _ensure_roster_agents(
                project_id=project_id,
                pool=pool,
                state_store=state_store,
                resource_store=resource_store,
                agent_ids=[caller_agent_id],
                profile_box_id=caller_profile_box_id,
                profile_name=caller_profile_name,
                worker_target=str(args.worker_target),
                tags=["bench", "bench_caller"],
            )

            # Pre-create target agents (reuse mode) for stable scheduling tests.
            total_agents = agents * (parallel_batches if args.disjoint_batches else 1)
            child_agent_ids = [
                f"{str(args.agent_prefix)}_{i+1:03d}"
                for i in range(total_agents)
            ]
            await _ensure_roster_agents(
                project_id=project_id,
                pool=pool,
                state_store=state_store,
                resource_store=resource_store,
                agent_ids=child_agent_ids,
                profile_box_id=worker_profile_box_id,
                profile_name=worker_profile_name,
                worker_target=str(args.worker_target),
                tags=["bench", "bench_worker"],
            )

            nats = NATSClient(config=cfg.get("nats", {}))
            await nats.connect()
            try:
                print(
                    "[bench] starting: "
                    f"waves={rounds} agents_per_batch={agents} parallel_batches={parallel_batches} "
                    f"disjoint_batches={bool(args.disjoint_batches)} overlap_waves={bool(args.overlap_waves)} "
                    f"conversation_mode={conversation_mode} suppress_instruction={suppress_instruction} "
                    f"worker_target={str(args.worker_target)!r}"
                )

                run_timings: List[BatchRunTimings] = []
                all_child_rows: List[ChildTurnTiming] = []

                async def _dispatch_wave(wave: int) -> List[Tuple[BatchRunTimings, List[ChildTurnTiming]]]:
                    tasks: List[asyncio.Task] = []
                    for bi in range(1, parallel_batches + 1):
                        if args.disjoint_batches:
                            start = (bi - 1) * agents
                            ids = child_agent_ids[start : start + agents]
                        else:
                            ids = child_agent_ids[:agents]
                        batch_task = asyncio.create_task(
                            _run_one_batch(
                                wave=wave,
                                batch_index=bi,
                                cardbox=cardbox,
                                execution_store=execution_store,
                                nats=nats,
                                pool=pool,
                                project_id=project_id,
                                channel_id=channel_id,
                                caller_agent_id=caller_agent_id,
                                agent_ids=list(ids),
                                deadline_seconds=args.deadline_seconds,
                                poll_interval_s=poll_interval_s,
                                timeout_s=float(args.timeout_s),
                            ),
                            name=f"bench:wave{wave}:batch{bi}",
                        )
                        tasks.append(batch_task)
                    results: List[Tuple[BatchRunTimings, List[ChildTurnTiming]]] = []
                    for res in await asyncio.gather(*tasks):
                        results.append(res)
                    return results

                if args.overlap_waves:
                    wave_tasks = [
                        asyncio.create_task(_dispatch_wave(w), name=f"bench:wave{w}")
                        for w in range(1, rounds + 1)
                    ]
                    wave_results = await asyncio.gather(*wave_tasks)
                    for results in wave_results:
                        for t, child in results:
                            run_timings.append(t)
                            all_child_rows.extend(child)
                else:
                    for wave in range(1, rounds + 1):
                        results = await _dispatch_wave(wave)
                        for t, child in results:
                            run_timings.append(t)
                            all_child_rows.extend(child)
                            print(
                                f"[bench] completed wave={t.wave} batch={t.batch_index} "
                                f"batch_id={t.batch_id} status={t.batch_status} total_ms={_ms_from_s(t.t_analyzed_s - t.t0_s)}"
                            )
                            if t.batch_status != "success":
                                if t.error_code or t.error_message:
                                    print(
                                        "[bench] failed reason: "
                                        f"error_code={t.error_code or 'unknown'} "
                                        f"error_message={t.error_message or 'unknown'}"
                                    )
                                    if "tasks fanout exceeds max limit" in (t.error_message or ""):
                                        print(
                                            "[bench] hint: this is PMO fork_join fanout limit; "
                                            "increase [pmo].fork_join_max_tasks and restart PMO."
                                        )
                                else:
                                    print("[bench] failed reason: tool.result did not include explicit error details.")
                            _print_child_summary(wave=t.wave, batch_id=t.batch_id, rows=child)
                            if int(args.slowest_k) > 0:
                                print(f"[bench] slowest child turns (top {int(args.slowest_k)})")
                                _print_slowest_tasks(rows=child, limit=int(args.slowest_k))

                # Final summary table
                print("\n[bench] batch stage timing table (ms)")
                print(
                    _render_table(
                        [
                            "wave",
                            "batch",
                            "batch_id",
                            "status",
                            "tasks",
                            "save_call",
                            "publish",
                            "wait_join",
                            "load_result",
                            "analyze",
                            "total",
                            "trace_id",
                            "tool_call_id",
                        ],
                        [t.as_row() for t in run_timings],
                    )
                )

                # Aggregate child summary
                print("\n[bench] aggregate child metrics (all batches)")
                metrics = _summarize_child_metrics(all_child_rows)
                print(json.dumps(metrics, ensure_ascii=False, indent=2))

                if args.json_out:
                    out_path = Path(str(args.json_out))
                    payload = {
                        "project_id": project_id,
                        "channel_id": channel_id,
                        "config": {
                            "waves": rounds,
                            "agents_per_batch": agents,
                            "parallel_batches": parallel_batches,
                            "disjoint_batches": bool(args.disjoint_batches),
                            "overlap_waves": bool(args.overlap_waves),
                            "deadline_seconds": args.deadline_seconds,
                            "timeout_s": float(args.timeout_s),
                            "poll_interval_ms": int(args.poll_interval_ms),
                            "worker_target": str(args.worker_target),
                        },
                        "batches": [
                            {
                                "wave": t.wave,
                                "batch_index": t.batch_index,
                                "batch_id": t.batch_id,
                                "batch_status": t.batch_status,
                                "task_count": t.task_count,
                                "trace_id": t.trace_id,
                                "tool_call_id": t.tool_call_id,
                                "error_code": t.error_code,
                                "error_message": t.error_message,
                                "timing_ms": {
                                    "save_call": _ms_from_s(t.t_saved_tool_call_s - t.t0_s),
                                    "publish": _ms_from_s(t.t_published_s - t.t_saved_tool_call_s),
                                    "wait_join": _ms_from_s(t.t_received_tool_result_s - t.t_published_s),
                                    "load_result": _ms_from_s(t.t_loaded_tool_result_s - t.t_received_tool_result_s),
                                    "analyze": _ms_from_s(t.t_analyzed_s - t.t_loaded_tool_result_s),
                                    "total": _ms_from_s(t.t_analyzed_s - t.t0_s),
                                },
                            }
                            for t in run_timings
                        ],
                        "child_turns": [
                            {
                                "wave": r.wave,
                                "batch_id": r.batch_id,
                                "agent_id": r.agent_id,
                                "batch_task_id": r.batch_task_id,
                                "child_turn_id": r.child_turn_id,
                                "status": r.status,
                                "attempt_count": r.attempt_count,
                                "turn_epoch": r.turn_epoch,
                                "inbox_status": r.inbox_status,
                                "inbox_created_at": _iso(r.inbox_created_at),
                                "inbox_processed_at": _iso(r.inbox_processed_at),
                                "step_started_at": _iso(r.step_started_at),
                                "step_ended_at": _iso(r.step_ended_at),
                                "step_count": r.step_count,
                                "timing_ms": {
                                    "queue_delay": r.queue_delay_ms(),
                                    "step": r.step_ms(),
                                    "e2e": r.e2e_ms(),
                                },
                            }
                            for r in all_child_rows
                        ],
                        "aggregate_child_metrics": metrics,
                    }
                    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2))
                    print(f"[bench] wrote json: {out_path}")
            finally:
                await nats.close()
        finally:
            await cardbox.close()
    finally:
        await pool.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
