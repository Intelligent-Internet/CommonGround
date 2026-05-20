from __future__ import annotations

from datetime import UTC, datetime
import json

import psycopg

from CommonGround.adapters import ExternalAgentAdapter
from CommonGround.contracts import AgentRef, DispatchAuthority, DispatchAuthorityMode, TURN_KIND_CONVERSATION_V1, TurnRef


def register_agent(
    kernel_app,
    agent: AgentRef,
    *,
    role: str | None = None,
    description: str | None = None,
    capabilities: tuple[str, ...] = (),
    grants: tuple[str, ...] = (),
    accepts_work: bool = True,
    enabled: bool = True,
    public_metadata: dict[str, object] | None = None,
    last_seen_at: datetime | None = None,
) -> AgentRef:
    kernel_app.topology.register_agent(
        agent,
        role=role,
        description=description,
        capabilities=capabilities,
        grants=grants,
        accepts_work=accepts_work,
        enabled=enabled,
    )
    if last_seen_at is not None:
        if kernel_app.clock is None:
            raise ValueError("kernel_app.clock is required when seeding last_seen_at")
        original_now = kernel_app.clock.now()
        kernel_app.clock.set(last_seen_at)
        try:
            kernel_app.topology.update_agent_presence(agent)
        finally:
            kernel_app.clock.set(original_now)
    if public_metadata is not None:
        kernel_app.topology.update_agent_public_metadata(agent, public_metadata)
    return agent


def dispatch_root_turn(
    kernel_app,
    *,
    requested_by: AgentRef,
    target_agent: AgentRef,
    request_id: str,
    turn_kind: str = TURN_KIND_CONVERSATION_V1,
) -> TurnRef:
    return kernel_app.sdk.dispatch(
        requested_by=requested_by,
        target_agent=target_agent,
        input_payload={"task": request_id},
        authority=DispatchAuthority(mode=DispatchAuthorityMode.ROOT_REQUEST, request_id=request_id),
        dispatch_key=request_id,
        turn_kind=turn_kind,
    )


def dispatch_child_turn(
    kernel_app,
    *,
    parent_agent: AgentRef,
    target_agent: AgentRef,
    parent_claim,
    dispatch_key: str,
    turn_kind: str = TURN_KIND_CONVERSATION_V1,
) -> TurnRef:
    adapter = ExternalAgentAdapter(agent=parent_agent, sdk=kernel_app.sdk)
    return adapter.dispatch(
        parent_claim,
        target_agent=target_agent,
        input_payload={"task": dispatch_key},
        dispatch_key=dispatch_key,
        turn_kind=turn_kind,
    )


def set_invalid_public_metadata(*, test_pg_dsn: str, agent: AgentRef, public_metadata: dict[str, object]) -> None:
    with psycopg.connect(test_pg_dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            update cg_agents
            set public_metadata = %s::jsonb
            where project_id = %s and agent_id = %s
            """,
            (json.dumps(public_metadata), agent.project_id, agent.agent_id),
        )
        conn.commit()


def utc_datetime(year: int, month: int, day: int, hour: int = 0, minute: int = 0, second: int = 0) -> datetime:
    return datetime(year, month, day, hour, minute, second, tzinfo=UTC)
