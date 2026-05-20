from __future__ import annotations

import logging
import time

from CommonGround.agent_client import HttpAgentClient, PollingWorker
from CommonGround.contracts import AgentRef, TurnRef
from CommonGround.turn_offers import upsert_turn_offer

from ..adapter.self_root_frontside import SelfRootFrontsideHandler
from ..turn_offer_metadata import conversation_turn_offer
from .client_auth import build_agent_client
from .presence import PresenceHeartbeater
from .supervisor_runner import _resume_ready_parent_turns

logger = logging.getLogger(__name__)


def run_self_root_frontside_worker_forever(
    *,
    base_url: str,
    agent: AgentRef,
    idle_sleep_seconds: float = 0.5,
    presence_interval_seconds: float = 15.0,
    reconcile_interval_seconds: float | None = None,
    credential_token: str | None = None,
) -> None:
    client = build_agent_client(HttpAgentClient, base_url=base_url, agent=agent, token=credential_token)
    existing_snapshot = client.get_agent(agent)
    public_metadata = {} if existing_snapshot is None else dict(existing_snapshot.public_metadata)
    public_metadata = upsert_turn_offer(public_metadata, conversation_turn_offer())
    client.update_agent_public_metadata(agent, public_metadata=public_metadata)

    worker = PollingWorker(client=client, agent=agent, handler=SelfRootFrontsideHandler())
    presence_heartbeater = PresenceHeartbeater(
        heartbeat_fn=lambda: client.heartbeat_agent_presence(agent),
        interval_seconds=presence_interval_seconds,
    )
    watched_turns: set[TurnRef] = set()
    agent_feed_after_ledger_seq = 0
    last_reconcile_at = 0.0
    logger.info("starting self-root frontside worker agent=%s project=%s base_url=%s", agent.agent_id, agent.project_id, base_url)
    try:
        presence_heartbeater.start()
        while True:
            now = time.monotonic()
            if reconcile_interval_seconds is not None and reconcile_interval_seconds > 0 and now - last_reconcile_at >= reconcile_interval_seconds:
                client.reconcile_expired_claim(agent)
                logger.debug("reconciled expired claims for self-root frontside=%s", agent.agent_id)
                last_reconcile_at = now
            agent_feed_after_ledger_seq = _resume_ready_parent_turns(
                client,
                agent=agent,
                watched_turns=watched_turns,
                agent_feed_after_ledger_seq=agent_feed_after_ledger_seq,
            )
            result = worker.run_once()
            if result.claimed_turn is not None and result.action == "suspended":
                watched_turns.add(result.claimed_turn)
                logger.info("watching suspended self-root parent turn=%s", result.claimed_turn.turn_id)
            if result.claimed_turn is not None:
                logger.info("worker result agent=%s turn=%s action=%s", agent.agent_id, result.claimed_turn.turn_id, result.action)
            if result.action == "idle":
                time.sleep(idle_sleep_seconds)
    finally:
        presence_heartbeater.stop()
        logger.info("stopping self-root frontside worker agent=%s", agent.agent_id)
        client.close()


__all__ = ["run_self_root_frontside_worker_forever"]
