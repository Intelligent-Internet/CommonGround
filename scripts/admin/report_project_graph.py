"""
Generate a project-level JSON report focused on:
- agent creation / derivation relationships (identity_edges, roster)
- batch fanout/join relationships (pmo_batches, pmo_batch_tasks)
- tool call relationships (tool.call/tool.result cards)
- OTel-derived durations from Jaeger (LLM/tool/worker spans)

This script is intended as a data backend for a UI viewer.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import httpx
from psycopg_pool import AsyncConnectionPool
from psycopg.rows import dict_row

from core.app_config import load_app_config, config_to_dict
from core.utils import set_loop_policy, safe_str
from infra.cardbox_client import CardBoxClient
from infra.stores.resource_store import ResourceStore


set_loop_policy()


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _parse_dt(value: Optional[str]) -> Optional[datetime]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    # Support RFC3339 "Z"
    if text.endswith("Z") and "+" not in text:
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except Exception as exc:  # noqa: BLE001
        raise ValueError(f"invalid datetime: {value!r}") from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def _dt_to_iso(dt: Optional[datetime]) -> Optional[str]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _chunks(seq: Sequence[str], n: int) -> Iterable[List[str]]:
    if n <= 0:
        yield list(seq)
        return
    for i in range(0, len(seq), n):
        yield list(seq[i : i + n])


def _tags_to_dict(tags: Any) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    if not isinstance(tags, list):
        return out
    for t in tags:
        if not isinstance(t, dict):
            continue
        k = t.get("key")
        if not isinstance(k, str) or not k:
            continue
        out[k] = t.get("value")
    return out


@dataclass(frozen=True)
class JaegerSpan:
    trace_id: str
    span_id: str
    operation: str
    service: str
    start_us: int
    duration_us: int
    tags: Dict[str, Any]
    parent_span_id: Optional[str]

    @property
    def duration_ms(self) -> float:
        return float(self.duration_us) / 1000.0

    @property
    def start_iso(self) -> str:
        dt = datetime.fromtimestamp(self.start_us / 1_000_000, tz=UTC)
        return dt.isoformat().replace("+00:00", "Z")


async def _fetch_jaeger_trace(
    client: httpx.AsyncClient,
    *,
    jaeger_base_url: str,
    trace_id: str,
) -> List[JaegerSpan]:
    url = jaeger_base_url.rstrip("/") + f"/api/traces/{trace_id}"
    r = await client.get(url, timeout=15.0)
    r.raise_for_status()
    payload = r.json()
    data = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data, list) or not data:
        return []
    trace_obj = data[0]
    if not isinstance(trace_obj, dict):
        return []
    processes = trace_obj.get("processes")
    if not isinstance(processes, dict):
        processes = {}
    spans = trace_obj.get("spans")
    if not isinstance(spans, list):
        return []

    # Precompute parent mapping from child-of references.
    parent_of: Dict[str, str] = {}
    for sp in spans:
        if not isinstance(sp, dict):
            continue
        sid = safe_str(sp.get("spanID"))
        refs = sp.get("references")
        if not sid or not isinstance(refs, list):
            continue
        parent = None
        for ref in refs:
            if not isinstance(ref, dict):
                continue
            if ref.get("refType") == "CHILD_OF" and ref.get("spanID"):
                parent = safe_str(ref.get("spanID"))
                break
        if parent:
            parent_of[sid] = parent

    out: List[JaegerSpan] = []
    for sp in spans:
        if not isinstance(sp, dict):
            continue
        sid = safe_str(sp.get("spanID"))
        op = safe_str(sp.get("operationName")) or "unknown"
        pid = safe_str(sp.get("processID"))
        proc = processes.get(pid) if pid else None
        service = safe_str(proc.get("serviceName")) if isinstance(proc, dict) else ""
        start_us = sp.get("startTime")
        dur_us = sp.get("duration")
        if not sid or start_us is None or dur_us is None:
            continue
        try:
            start_us_i = int(start_us)
            dur_us_i = int(dur_us)
        except Exception:
            continue
        tags = _tags_to_dict(sp.get("tags"))
        out.append(
            JaegerSpan(
                trace_id=str(trace_id),
                span_id=sid,
                operation=op,
                service=service or "unknown",
                start_us=start_us_i,
                duration_us=dur_us_i,
                tags=tags,
                parent_span_id=parent_of.get(sid),
            )
        )
    return out


def _index_otel(spans: List[JaegerSpan]) -> Dict[str, Any]:
    by_span_id: Dict[str, JaegerSpan] = {s.span_id: s for s in spans}
    children: Dict[str, List[str]] = {}
    for s in spans:
        if s.parent_span_id:
            children.setdefault(s.parent_span_id, []).append(s.span_id)

    def _descendants(root_id: str) -> List[JaegerSpan]:
        out: List[JaegerSpan] = []
        stack = list(children.get(root_id, []))
        while stack:
            sid = stack.pop()
            sp = by_span_id.get(sid)
            if sp:
                out.append(sp)
                stack.extend(children.get(sid, []))
        return out

    llm_calls: List[Dict[str, Any]] = []
    agent_tool_exec: Dict[str, JaegerSpan] = {}
    tool_handle_cmd: Dict[str, JaegerSpan] = {}
    tool_http_total_us_by_call: Dict[str, int] = {}

    for s in spans:
        if s.operation == "agent.llm.chat":
            llm_calls.append(
                {
                    "trace_id": s.trace_id,
                    "service": s.service,
                    "start": s.start_iso,
                    "duration_ms": s.duration_ms,
                    "agent_id": safe_str(s.tags.get("cg.agent_id")),
                    "agent_turn_id": safe_str(s.tags.get("cg.agent_turn_id")),
                    "step_id": safe_str(s.tags.get("cg.step_id")),
                    "model": safe_str(s.tags.get("cg.llm.model")),
                    "stream": bool(s.tags.get("cg.llm.stream")),
                }
            )
        if s.operation in ("agent.tool.execute", "agent.tool.execute_blocking"):
            tcid = safe_str(s.tags.get("cg.tool_call_id"))
            if tcid and tcid not in agent_tool_exec:
                agent_tool_exec[tcid] = s
        if s.operation == "tool.handle_cmd":
            tcid = safe_str(s.tags.get("cg.tool_call_id"))
            if tcid and tcid not in tool_handle_cmd:
                tool_handle_cmd[tcid] = s
                http_desc = [d for d in _descendants(s.span_id) if d.operation == "tool.http_request"]
                if http_desc:
                    tool_http_total_us_by_call[tcid] = sum(d.duration_us for d in http_desc)

    return {
        "llm_calls": llm_calls,
        "agent_tool_exec": agent_tool_exec,
        "tool_handle_cmd": tool_handle_cmd,
        "tool_http_total_us_by_call": tool_http_total_us_by_call,
        "span_count": len(spans),
    }


async def _query_rows(
    pool: AsyncConnectionPool,
    *,
    sql: str,
    params: Tuple[Any, ...],
) -> List[Dict[str, Any]]:
    async with pool.connection() as conn:
        res = await conn.execute(sql, params)
        rows = await res.fetchall()
        return list(rows or [])


def _time_clause(*, col: str, since: Optional[datetime], until: Optional[datetime]) -> Tuple[str, Tuple[Any, ...]]:
    clauses: List[str] = []
    params: List[Any] = []
    if since is not None:
        clauses.append(f"{col} >= %s")
        params.append(since)
    if until is not None:
        clauses.append(f"{col} <= %s")
        params.append(until)
    if not clauses:
        return "TRUE", tuple()
    return " AND ".join(clauses), tuple(params)


def _json_default(obj: Any) -> Any:
    if isinstance(obj, datetime):
        return obj.astimezone(UTC).isoformat().replace("+00:00", "Z")
    return str(obj)


async def main() -> None:
    ap = argparse.ArgumentParser(description="Project-level observability report (JSON).")
    ap.add_argument("--project", required=True, help="project_id")
    ap.add_argument("--since", default=None, help="ISO8601 (UTC recommended). default: no lower bound")
    ap.add_argument("--until", default=None, help="ISO8601 (UTC recommended). default: no upper bound")
    ap.add_argument("--jaeger", default="http://localhost:16686", help="Jaeger base url")
    ap.add_argument("--max-otel-traces", type=int, default=2000, help="cap Jaeger fetches (0=disable OTel)")
    ap.add_argument("--out", default=None, help="output path (default: stdout)")
    ap.add_argument("--tool-call-chunk", type=int, default=200, help="tool_call_id chunk size for CardBox queries")
    args = ap.parse_args()

    project_id = str(args.project)
    since = _parse_dt(args.since)
    until = _parse_dt(args.until)

    cfg = config_to_dict(load_app_config())
    cardbox_cfg = cfg.get("cardbox", {}) if isinstance(cfg, dict) else {}
    dsn = safe_str(cardbox_cfg.get("postgres_dsn"))
    if not dsn:
        raise RuntimeError("cardbox.postgres_dsn missing in config.toml")

    pool = AsyncConnectionPool(dsn, open=False, kwargs={"row_factory": dict_row})
    await pool.open()
    cardbox = CardBoxClient(config=cardbox_cfg)
    await cardbox.init()
    resource_store = ResourceStore(pool=pool, dsn=dsn)
    await resource_store.open()

    warnings: List[str] = []
    try:
        # Identity edges (agent creation/derivation)
        clause, p = _time_clause(col="created_at", since=since, until=until)
        identity_edges = await _query_rows(
            pool,
            sql=(
                "SELECT edge_id, project_id, channel_id, action, source_agent_id, target_agent_id, "
                "trace_id, parent_step_id, metadata, created_at "
                "FROM state.identity_edges "
                f"WHERE project_id=%s AND {clause} "
                "ORDER BY created_at ASC"
            ),
            params=(project_id, *p),
        )

        # Steps (tool_call_ids live here)
        step_clause, sp = _time_clause(col="started_at", since=since, until=until)
        steps = await _query_rows(
            pool,
            sql=(
                "SELECT project_id, agent_id, step_id, agent_turn_id, channel_id, parent_step_id, trace_id, status, "
                "profile_box_id::text AS profile_box_id, context_box_id::text AS context_box_id, "
                "output_box_id::text AS output_box_id, tool_call_ids, error, metadata, started_at, ended_at "
                "FROM state.agent_steps "
                f"WHERE project_id=%s AND {step_clause} "
                "ORDER BY started_at ASC"
            ),
            params=(project_id, *sp),
        )

        # Batches + tasks (fork_join / delegate)
        b_clause, bp = _time_clause(col="created_at", since=since, until=until)
        batches = await _query_rows(
            pool,
            sql=(
                "SELECT batch_id, project_id, parent_agent_id, parent_agent_turn_id, parent_tool_call_id, "
                "parent_turn_epoch, parent_channel_id, task_count, status, deadline_at, metadata, created_at, updated_at "
                "FROM state.pmo_batches "
                f"WHERE project_id=%s AND {b_clause} "
                "ORDER BY created_at ASC"
            ),
            params=(project_id, *bp),
        )
        t_clause, tp = _time_clause(col="created_at", since=since, until=until)
        batch_tasks = await _query_rows(
            pool,
            sql=(
                "SELECT batch_task_id, batch_id, project_id, agent_id, status, attempt_count, current_agent_turn_id, "
                "current_turn_epoch, payload_hash, profile_box_id::text AS profile_box_id, "
                "context_box_id::text AS context_box_id, output_box_id::text AS output_box_id, error_detail, metadata, "
                "created_at, updated_at "
                "FROM state.pmo_batch_tasks "
                f"WHERE project_id=%s AND {t_clause} "
                "ORDER BY created_at ASC"
            ),
            params=(project_id, *tp),
        )

        # Collect IDs
        agent_ids: set[str] = set()
        trace_ids: set[str] = set()
        tool_call_ids: set[str] = set()

        for e in identity_edges:
            if safe_str(e.get("source_agent_id")):
                agent_ids.add(str(e["source_agent_id"]))
            if safe_str(e.get("target_agent_id")):
                agent_ids.add(str(e["target_agent_id"]))
            if safe_str(e.get("trace_id")):
                trace_ids.add(str(e["trace_id"]))

        for s in steps:
            if safe_str(s.get("agent_id")):
                agent_ids.add(str(s["agent_id"]))
            if safe_str(s.get("trace_id")):
                trace_ids.add(str(s["trace_id"]))
            tcids = s.get("tool_call_ids")
            if isinstance(tcids, list):
                for tcid in tcids:
                    if safe_str(tcid):
                        tool_call_ids.add(str(tcid))

        for b in batches:
            if safe_str(b.get("parent_agent_id")):
                agent_ids.add(str(b["parent_agent_id"]))
            if safe_str(b.get("parent_tool_call_id")):
                tool_call_ids.add(str(b["parent_tool_call_id"]))
            meta = b.get("metadata")
            if isinstance(meta, dict) and safe_str(meta.get("trace_id")):
                trace_ids.add(str(meta["trace_id"]))

        for t in batch_tasks:
            if safe_str(t.get("agent_id")):
                agent_ids.add(str(t["agent_id"]))
            meta = t.get("metadata")
            if isinstance(meta, dict):
                if safe_str(meta.get("child_trace_id")):
                    trace_ids.add(str(meta["child_trace_id"]))

        # Fetch roster rows for involved agents
        roster_rows: List[Dict[str, Any]] = []
        if agent_ids:
            ids_list = sorted(agent_ids)
            roster_rows = await _query_rows(
                pool,
                sql=(
                    "SELECT project_id, agent_id, profile_box_id::text AS profile_box_id, worker_target, tags, "
                    "display_name, owner_agent_id, status, metadata, created_at, last_seen_at "
                    "FROM resource.project_agents "
                    "WHERE project_id=%s AND agent_id = ANY(%s)"
                ),
                params=(project_id, ids_list),
            )
        roster_by_agent = {safe_str(r.get("agent_id")): r for r in roster_rows if safe_str(r.get("agent_id"))}

        # Fetch tool cards (tool.call + tool.result + others by tool_call_id)
        tool_cards: List[Dict[str, Any]] = []
        cards_by_id: Dict[str, Any] = {}
        tool_cards_by_call: Dict[str, List[str]] = {}
        if tool_call_ids:
            ids = sorted(tool_call_ids)
            for chunk in _chunks(ids, int(args.tool_call_chunk)):
                cards = await cardbox.get_cards_by_tool_call_ids(project_id=project_id, tool_call_ids=chunk)
                for c in cards:
                    cid = safe_str(getattr(c, "card_id", None))
                    if not cid or cid in cards_by_id:
                        continue
                    dump = c.model_dump(mode="python")
                    cards_by_id[cid] = dump
                    tool_cards.append(dump)
                    tcid = safe_str(getattr(c, "tool_call_id", None))
                    if tcid:
                        tool_cards_by_call.setdefault(tcid, []).append(cid)

        # Fetch OTel traces from Jaeger for durations
        otel: Dict[str, Any] = {"jaeger_base_url": str(args.jaeger), "traces_fetched": 0, "missing_traces": []}
        otel_index_by_trace: Dict[str, Any] = {}
        max_otel_traces = int(args.max_otel_traces)
        if max_otel_traces != 0:
            want = sorted(trace_ids)
            if max_otel_traces > 0 and len(want) > max_otel_traces:
                warnings.append(f"otel traces truncated: want={len(want)} max={max_otel_traces}")
                want = want[:max_otel_traces]
            sem = asyncio.Semaphore(20)

            async with httpx.AsyncClient() as client:
                async def _one(tid: str) -> None:
                    async with sem:
                        try:
                            spans = await _fetch_jaeger_trace(client, jaeger_base_url=str(args.jaeger), trace_id=tid)
                        except Exception:  # noqa: BLE001
                            otel["missing_traces"].append(tid)
                            return
                        otel_index_by_trace[tid] = _index_otel(spans)

                await asyncio.gather(*[_one(tid) for tid in want])
                otel["traces_fetched"] = len(otel_index_by_trace)

        # Build tool call report entries
        tool_calls_out: List[Dict[str, Any]] = []
        for tcid, card_ids in sorted(tool_cards_by_call.items()):
            cards = [cards_by_id[cid] for cid in card_ids if cid in cards_by_id]
            call_card = next((c for c in cards if c.get("type") == "tool.call"), None)
            result_cards = [c for c in cards if c.get("type") == "tool.result"]
            tool_name = None
            call_args = None
            call_meta = None
            caller_agent_id = None
            if call_card:
                caller_agent_id = safe_str(call_card.get("author_id"))
                call_meta = call_card.get("metadata") if isinstance(call_card.get("metadata"), dict) else {}
                content = call_card.get("content") if isinstance(call_card.get("content"), dict) else {}
                tool_name = safe_str(content.get("tool_name"))
                call_args = content.get("arguments")

            # Attach OTel durations
            dur_agent_ms = None
            dur_tool_ms = None
            dur_http_ms = None
            # Locate in any trace index (tool_call_id spans are keyed only by tcid)
            for _tid, idx in otel_index_by_trace.items():
                agent_span = idx.get("agent_tool_exec", {}).get(tcid)
                tool_span = idx.get("tool_handle_cmd", {}).get(tcid)
                if agent_span and dur_agent_ms is None:
                    dur_agent_ms = agent_span.duration_ms
                if tool_span and dur_tool_ms is None:
                    dur_tool_ms = tool_span.duration_ms
                http_us = idx.get("tool_http_total_us_by_call", {}).get(tcid)
                if http_us is not None and dur_http_ms is None:
                    dur_http_ms = float(http_us) / 1000.0
                if dur_agent_ms is not None and dur_tool_ms is not None and dur_http_ms is not None:
                    break

            tool_calls_out.append(
                {
                    "tool_call_id": tcid,
                    "tool_name": tool_name,
                    "caller_agent_id": caller_agent_id,
                    "caller_step_id": safe_str((call_meta or {}).get("step_id")),
                    "caller_agent_turn_id": safe_str((call_meta or {}).get("agent_turn_id")),
                    "trace_id": safe_str((call_meta or {}).get("trace_id")),
                    "parent_step_id": safe_str((call_meta or {}).get("parent_step_id")),
                    "call_args": call_args,
                    "card_ids": {
                        "all": list(card_ids),
                        "tool_call": safe_str(call_card.get("card_id")) if call_card else None,
                        "tool_results": [safe_str(c.get("card_id")) for c in result_cards if safe_str(c.get("card_id"))],
                    },
                    "otel": {
                        "agent_tool_execute_ms": dur_agent_ms,
                        "tool_service_handle_ms": dur_tool_ms,
                        "tool_http_total_ms": dur_http_ms,
                    },
                }
            )

        # Graph for UI
        nodes: List[Dict[str, Any]] = []
        edges: List[Dict[str, Any]] = []

        tool_call_node_ids: set[str] = {safe_str(tc.get("tool_call_id")) for tc in tool_calls_out if safe_str(tc.get("tool_call_id"))}
        batch_child_agent_ids: set[str] = {
            safe_str(t.get("agent_id")) for t in batch_tasks if safe_str(t.get("agent_id"))
        }

        for aid in sorted(agent_ids):
            nodes.append(
                {
                    "id": f"agent:{aid}",
                    "type": "agent",
                    "agent_id": aid,
                    "roster": roster_by_agent.get(aid),
                }
            )
        for tc in tool_calls_out:
            nodes.append(
                {
                    "id": f"tool_call:{tc['tool_call_id']}",
                    "type": "tool_call",
                    **tc,
                }
            )
        for b in batches:
            bid = safe_str(b.get("batch_id"))
            if not bid:
                continue
            nodes.append({"id": f"batch:{bid}", "type": "batch", **b})

        # Agent->Agent edges (identity)
        for e in identity_edges:
            src = safe_str(e.get("source_agent_id"))
            tgt = safe_str(e.get("target_agent_id"))
            if not tgt:
                continue
            # Reduce clutter for fork_join fanout: if an agent is a batch task, prefer showing
            # batch -> agent rather than an additional agent -> agent edge for provisioning.
            if tgt in batch_child_agent_ids:
                continue
            edges.append(
                {
                    "id": safe_str(e.get("edge_id")) or f"edge:{len(edges)+1}",
                    "type": "agent_creates_agent",
                    "from": f"agent:{src}" if src else None,
                    "to": f"agent:{tgt}",
                    "action": safe_str(e.get("action")),
                    "trace_id": safe_str(e.get("trace_id")),
                    "parent_step_id": safe_str(e.get("parent_step_id")),
                    "created_at": _dt_to_iso(e.get("created_at")) if isinstance(e.get("created_at"), datetime) else safe_str(e.get("created_at")),
                    "metadata": e.get("metadata") if isinstance(e.get("metadata"), dict) else {},
                }
            )

        # Agent->ToolCall edges
        for tc in tool_calls_out:
            caller = safe_str(tc.get("caller_agent_id"))
            if not caller:
                continue
            edges.append(
                {
                    "id": f"edge:agent_calls_tool:{tc['tool_call_id']}",
                    "type": "agent_calls_tool",
                    "from": f"agent:{caller}",
                    "to": f"tool_call:{tc['tool_call_id']}",
                    "tool_name": safe_str(tc.get("tool_name")),
                    "step_id": safe_str(tc.get("caller_step_id")),
                    "agent_turn_id": safe_str(tc.get("caller_agent_turn_id")),
                    "trace_id": safe_str(tc.get("trace_id")),
                    "otel": tc.get("otel") or {},
                }
            )

        # Batch edges
        for b in batches:
            bid = safe_str(b.get("batch_id"))
            parent = safe_str(b.get("parent_agent_id"))
            ptcid = safe_str(b.get("parent_tool_call_id"))

            # Prefer a single unambiguous chain:
            # agent -> tool_call -> batch -> child agents
            #
            # If we have a tool_call node, emit tool_call->batch and suppress agent->batch to avoid
            # drawing principal connected to BOTH tool_call and batch.
            if bid and ptcid and ptcid in tool_call_node_ids:
                edges.append(
                    {
                        "id": f"edge:tool_call_creates_batch:{bid}",
                        "type": "tool_call_creates_batch",
                        "from": f"tool_call:{ptcid}",
                        "to": f"batch:{bid}",
                        "tool_call_id": ptcid,
                    }
                )
            elif bid and parent:
                edges.append(
                    {
                        "id": f"edge:agent_creates_batch:{bid}",
                        "type": "agent_creates_batch",
                        "from": f"agent:{parent}",
                        "to": f"batch:{bid}",
                        "parent_tool_call_id": ptcid,
                    }
                )
        for t in batch_tasks:
            bid = safe_str(t.get("batch_id"))
            aid = safe_str(t.get("agent_id"))
            if bid and aid:
                edges.append(
                    {
                        "id": f"edge:batch_contains_agent:{safe_str(t.get('batch_task_id')) or aid}",
                        "type": "batch_contains_agent",
                        "from": f"batch:{bid}",
                        "to": f"agent:{aid}",
                        "batch_task_id": safe_str(t.get("batch_task_id")),
                        "status": safe_str(t.get("status")),
                        "current_agent_turn_id": safe_str(t.get("current_agent_turn_id")),
                        "child_trace_id": safe_str((t.get("metadata") or {}).get("child_trace_id")) if isinstance(t.get("metadata"), dict) else None,
                    }
                )

        report: Dict[str, Any] = {
            "schema_version": "cg.project_report.v1",
            "generated_at": _dt_to_iso(_utc_now()),
            "project_id": project_id,
            "time_range": {"since": _dt_to_iso(since), "until": _dt_to_iso(until)},
            "summary": {
                "agent_count": len(agent_ids),
                "identity_edge_count": len(identity_edges),
                "batch_count": len(batches),
                "batch_task_count": len(batch_tasks),
                "step_count": len(steps),
                "tool_call_count": len(tool_calls_out),
                "trace_id_count": len(trace_ids),
            },
            "warnings": warnings,
            "agents": {"roster": roster_rows, "identity_edges": identity_edges},
            "batches": {"batches": batches, "tasks": batch_tasks},
            "steps": steps,
            "tools": {"tool_calls": tool_calls_out},
            "cards": {
                "included": {
                    "tool_call_cards": True,
                    "tool_result_cards": True,
                    "other_tool_call_related_cards": True,
                },
                "cards_by_id": cards_by_id,
            },
            "otel": {"index_by_trace_id": {k: {"llm_calls": v.get("llm_calls"), "span_count": v.get("span_count")} for k, v in otel_index_by_trace.items()}, **otel},
            "graph": {"nodes": nodes, "edges": edges},
        }

        blob = json.dumps(report, ensure_ascii=False, indent=2, default=_json_default)
        if args.out:
            with open(str(args.out), "w", encoding="utf-8") as f:
                f.write(blob)
        else:
            print(blob)
    finally:
        await resource_store.close()
        await pool.close()
        await cardbox.close(timeout=30.0)


if __name__ == "__main__":
    asyncio.run(main())
