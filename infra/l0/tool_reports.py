from __future__ import annotations

from typing import Any, Dict

from core.cg_context import CGContext
from core.errors import ProtocolViolationError

from infra.l0_engine import L0Engine, ReportIntent, TurnRef


async def publish_tool_result_report(
    *,
    nats: Any,
    execution_store: Any,
    cardbox: Any | None = None,
    resource_store: Any | None = None,
    state_store: Any | None = None,
    l0_engine: L0Engine | None = None,
    source_ctx: CGContext,
    target_ctx: CGContext,
    payload: Dict[str, Any],
    correlation_id: str | None = None,
    default_depth: int = 0,
) -> None:
    """Publish a standard L0 report for a tool.result (Inbox + Wakeup fanout path)."""
    resolved_correlation_id = str(correlation_id or "").strip()
    if not resolved_correlation_id:
        try:
            resolved_correlation_id = target_ctx.require_tool_call_id
        except ProtocolViolationError as exc:
            raise RuntimeError("publish_tool_result_report requires correlation_id or target_ctx.tool_call_id") from exc
    if not resolved_correlation_id:
        raise RuntimeError("publish_tool_result_report requires correlation_id or target_ctx.tool_call_id")
    target_ctx = target_ctx.with_tool_call(resolved_correlation_id)
    transport_ctx = target_ctx.with_trace_transport(
        base_headers=target_ctx.headers,
        trace_id=target_ctx.trace_id,
        default_depth=int(default_depth),
    )
    engine = l0_engine
    if engine is None and resource_store is not None:
        engine = L0Engine(
            nats=nats,
            execution_store=execution_store,
            resource_store=resource_store,
            state_store=state_store,
            cardbox=cardbox,
        )
    if engine is None:
        raise RuntimeError("publish_tool_result_report requires l0_engine or resource_store")

    target_ref = TurnRef(
        project_id=transport_ctx.project_id,
        agent_id=transport_ctx.agent_id,
        agent_turn_id=transport_ctx.require_agent_turn_id,
        expected_turn_epoch=int(transport_ctx.turn_epoch) if int(transport_ctx.turn_epoch) > 0 else None,
    )
    wakeup_signals = []
    async with execution_store.pool.connection() as conn:
        async with conn.transaction():
            result = await engine.report_intent(
                source_ctx=source_ctx,
                intent=ReportIntent(
                    target=target_ref,
                    message_type="tool_result",
                    payload=payload,
                    correlation_id=resolved_correlation_id,
                ),
                conn=conn,
                wakeup=True,
            )
            if result.status != "accepted":
                code = str(result.error_code or "")
                raise RuntimeError(
                    f"publish_tool_result_report rejected: status={result.status}"
                    + (f" code={code}" if code else "")
                )
            wakeup_signals = list(result.wakeup_signals or ())
    if wakeup_signals:
        await engine.publish_wakeup_signals(wakeup_signals)
