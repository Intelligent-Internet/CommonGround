from __future__ import annotations

import logging
import time
from pathlib import Path

import httpx

from CommonGround.contracts import AgentRef, TurnState
from CommonGround.agent_client import HttpAgentClient, PollingWorker
from CommonGround.projection_client.http_client import ProjectionHttpClient
from CommonGround.turn_offers import upsert_turn_offer

from ..adapter.leaf_handler import LeafTurnHandler
from ..turn_offer_metadata import conversation_turn_offer
from .client_auth import build_agent_client
from .factory import build_agent_loop
from .presence import PresenceHeartbeater

logger = logging.getLogger(__name__)

_TERMINAL_AUTH_FAILURES = {
    "agent credential status is not active: revoked": "credential_revoked",
    "authenticated agent is disabled": "agent_disabled",
}


def run_leaf_worker_forever(
    *,
    base_url: str,
    agent: AgentRef,
    config_path: str | Path | None = None,
    workspace: str | Path | None = None,
    repo_root: str | Path | None = None,
    idle_sleep_seconds: float = 0.5,
    presence_interval_seconds: float = 15.0,
    reconcile_interval_seconds: float | None = None,
    credential_token: str | None = None,
    projection_client_factory=None,
) -> None:
    if projection_client_factory is None:
        projection_client_factory = ProjectionHttpClient
    client = build_agent_client(HttpAgentClient, base_url=base_url, agent=agent, token=credential_token)
    projection_client = build_agent_client(projection_client_factory, base_url=base_url, agent=agent, token=credential_token)
    existing_snapshot = client.get_agent(agent)
    public_metadata = {} if existing_snapshot is None else dict(existing_snapshot.public_metadata)
    public_metadata = upsert_turn_offer(public_metadata, conversation_turn_offer())
    client.update_agent_public_metadata(agent, public_metadata=public_metadata)
    loop = build_agent_loop(config_path=config_path, workspace=workspace, repo_root=repo_root)
    worker = PollingWorker(client=client, agent=agent, handler=LeafTurnHandler(loop=loop))
    presence_heartbeater = PresenceHeartbeater(
        heartbeat_fn=lambda: client.heartbeat_agent_presence(agent),
        interval_seconds=presence_interval_seconds,
    )
    last_reconcile_at = 0.0
    logger.info("starting leaf worker agent=%s project=%s base_url=%s workspace=%s", agent.agent_id, agent.project_id, base_url, workspace)
    try:
        presence_heartbeater.start()
        while True:
            now = time.monotonic()
            if reconcile_interval_seconds is not None and reconcile_interval_seconds > 0 and now - last_reconcile_at >= reconcile_interval_seconds:
                client.reconcile_expired_claim(agent)
                logger.debug("reconciled expired claims for agent=%s", agent.agent_id)
                last_reconcile_at = now
            try:
                if _should_stop_after_retire(client, projection_client, agent):
                    logger.info("stopping leaf worker agent=%s after observed retirement drain", agent.agent_id)
                    break
                result = worker.run_once()
            except httpx.HTTPStatusError as exc:
                terminal_reason = _terminal_auth_failure_reason(exc)
                if terminal_reason is not None:
                    logger.info(
                        "stopping leaf worker agent=%s after terminal auth failure=%s",
                        agent.agent_id,
                        terminal_reason,
                    )
                    break
                raise
            if result.claimed_turn is not None:
                logger.info("worker result agent=%s turn=%s action=%s", agent.agent_id, result.claimed_turn.turn_id, result.action)
            if result.action == "idle":
                time.sleep(idle_sleep_seconds)
    finally:
        presence_heartbeater.stop()
        logger.info("stopping leaf worker agent=%s", agent.agent_id)
        projection_client.close()
        client.close()


def _terminal_auth_failure_reason(exc: httpx.HTTPStatusError) -> str | None:
    response = exc.response
    if response is None or response.status_code not in {401, 403}:
        return None
    try:
        payload = response.json()
    except ValueError:
        return None
    if not isinstance(payload, dict):
        return None
    detail = payload.get("message")
    if not isinstance(detail, str):
        return None
    return _TERMINAL_AUTH_FAILURES.get(detail)


def _should_stop_after_retire(client: HttpAgentClient, projection_client, agent: AgentRef) -> bool:
    snapshot = client.get_agent(agent)
    if snapshot is None or snapshot.accepts_work:
        return False
    return not _has_open_turns(projection_client, agent)


def _has_open_turns(projection_client, agent: AgentRef) -> bool:
    for state in (TurnState.QUEUED, TurnState.RUNNING, TurnState.SUSPENDED):
        page = projection_client.list_turns(
            project_id=agent.project_id,
            target_agent_id=agent.agent_id,
            state=state.value,
            limit=1,
        )
        if page.items:
            return True
    return False
