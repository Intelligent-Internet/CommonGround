from __future__ import annotations

import logging
import time
from pathlib import Path

from CommonGround.agent_client import HttpAgentClient, PollingWorker
from CommonGround.contracts import AgentRef, TurnRef, TurnState
from CommonGround.projection_client import ProjectionHttpClient
from CommonGround.turn_offers import upsert_turn_offer

from ..adapter.provision_handler import OpsSubstrate, ProvisionAgentSpawnHandler, _assigned_agent_id
from ..provision_lifecycle import DEFAULT_EPHEMERAL_LIFECYCLE_TTL_SECONDS, cleanup_ephemeral_provision_agents
from ..provision_role_policy import published_available_roles
from ..turn_offer_metadata import provision_turn_offer
from .client_auth import build_agent_client
from ..substrate.process_substrate import ProcessOpsSubstrate
from .feed_utils import fetch_agent_feed_items_since
from .presence import PresenceHeartbeater

logger = logging.getLogger(__name__)


def run_provisioner_worker_forever(
    *,
    base_url: str,
    agent: AgentRef,
    substrate: OpsSubstrate | None = None,
    repo_root: str | Path | None = None,
    config_path: str | Path | None = None,
    workspace_root: str | Path | None = None,
    idle_sleep_seconds: float = 0.5,
    presence_interval_seconds: float = 15.0,
    reconcile_interval_seconds: float | None = None,
    max_iterations: int | None = None,
    client_factory=HttpAgentClient,
    projection_client_factory=ProjectionHttpClient,
    worker_factory=PollingWorker,
    credential_token: str | None = None,
    provision_lifecycle_cleanup_interval_seconds: float | None = 30.0,
    provision_lifecycle_ttl_seconds: int = DEFAULT_EPHEMERAL_LIFECYCLE_TTL_SECONDS,
) -> None:
    client = build_agent_client(client_factory, base_url=base_url, agent=agent, token=credential_token)
    lifecycle_cleanup_enabled = (
        provision_lifecycle_cleanup_interval_seconds is not None and provision_lifecycle_cleanup_interval_seconds > 0
    )
    projection_client = None
    try:
        if lifecycle_cleanup_enabled:
            projection_client = build_agent_client(
                projection_client_factory,
                base_url=base_url,
                agent=agent,
                token=credential_token,
            )
        existing_snapshot = client.get_agent(agent)
        public_metadata = {} if existing_snapshot is None else dict(existing_snapshot.public_metadata)
        published_roles = published_available_roles()
        public_metadata = upsert_turn_offer(public_metadata, provision_turn_offer(roles=published_roles))
        client.update_agent_public_metadata(
            agent,
            public_metadata=public_metadata,
        )
        handler = ProvisionAgentSpawnHandler(
            substrate=substrate if substrate is not None else ProcessOpsSubstrate(),
            base_url=base_url,
            repo_root=None if repo_root is None else str(repo_root),
            config_path=None if config_path is None else str(config_path),
            workspace_root=None if workspace_root is None else str(workspace_root),
            lifecycle_ttl_seconds=provision_lifecycle_ttl_seconds,
        )
        worker = worker_factory(client=client, agent=agent, handler=handler)
        presence_heartbeater = PresenceHeartbeater(
            heartbeat_fn=lambda: client.heartbeat_agent_presence(agent),
            interval_seconds=presence_interval_seconds,
        )
        watched_turns: set[TurnRef] = set()
        agent_feed_after_ledger_seq = 0
        last_reconcile_at = 0.0
        last_lifecycle_cleanup_at = time.monotonic()
        iterations = 0
        logger.info("starting provisioner worker agent=%s project=%s base_url=%s", agent.agent_id, agent.project_id, base_url)
        presence_heartbeater.start()
        while max_iterations is None or iterations < max_iterations:
            iterations += 1
            now = time.monotonic()
            if reconcile_interval_seconds is not None and reconcile_interval_seconds > 0 and now - last_reconcile_at >= reconcile_interval_seconds:
                client.reconcile_expired_claim(agent)
                logger.debug("reconciled expired claims for provisioner=%s", agent.agent_id)
                last_reconcile_at = now
            if (
                projection_client is not None
                and provision_lifecycle_cleanup_interval_seconds is not None
                and now - last_lifecycle_cleanup_at >= provision_lifecycle_cleanup_interval_seconds
            ):
                summary = cleanup_ephemeral_provision_agents(
                    client,
                    projection_client,
                    owner_agent=agent,
                )
                if summary.eligible_agents or summary.revoked_credentials:
                    logger.info(
                        "provision lifecycle cleanup agent=%s scanned=%s eligible=%s drained=%s grace=%s waiting=%s timed_out=%s revoked_credentials=%s",
                        agent.agent_id,
                        summary.scanned_agents,
                        summary.eligible_agents,
                        summary.drained_agents,
                        summary.grace_period_agents,
                        summary.waiting_agents,
                        summary.timed_out_agents,
                        summary.revoked_credentials,
                    )
                last_lifecycle_cleanup_at = now
            agent_feed_after_ledger_seq = _resume_ready_provision_turns(
                client,
                agent=agent,
                watched_turns=watched_turns,
                agent_feed_after_ledger_seq=agent_feed_after_ledger_seq,
            )
            result = worker.run_once()
            if result.claimed_turn is not None and result.action == "suspended":
                watched_turns.add(result.claimed_turn)
                logger.info("watching suspended provision turn=%s", result.claimed_turn.turn_id)
            if result.claimed_turn is not None:
                logger.info("worker result agent=%s turn=%s action=%s", agent.agent_id, result.claimed_turn.turn_id, result.action)
            if result.action == "idle" and (max_iterations is None or iterations < max_iterations):
                time.sleep(idle_sleep_seconds)
    finally:
        if "presence_heartbeater" in locals():
            presence_heartbeater.stop()
        logger.info("stopping provisioner worker agent=%s", agent.agent_id)
        if projection_client is not None:
            projection_client.close()
        client.close()


def _resume_ready_provision_turns(
    client: HttpAgentClient,
    *,
    agent: AgentRef,
    watched_turns: set[TurnRef],
    agent_feed_after_ledger_seq: int = 0,
) -> int:
    candidates, next_agent_feed_after = _suspended_turn_candidates(
        client,
        agent=agent,
        watched_turns=watched_turns,
        after_ledger_seq=agent_feed_after_ledger_seq,
    )
    for turn in candidates:
        snapshot = client.get_turn(turn)
        if snapshot.state != TurnState.SUSPENDED:
            watched_turns.discard(turn)
            continue
        context = client.fetch_context(turn)
        assigned_agent_id = _assigned_agent_id_from_context(context)
        if assigned_agent_id is None:
            continue
        if client.get_agent(AgentRef(project_id=turn.project_id, agent_id=assigned_agent_id)) is None:
            continue
        client.resume_turn(agent, turn, note="agent_registered")
        logger.info("resumed provision turn=%s after agent registration", turn.turn_id)
        watched_turns.discard(turn)
    return next_agent_feed_after


def _suspended_turn_candidates(
    client: HttpAgentClient,
    *,
    agent: AgentRef,
    watched_turns: set[TurnRef],
    after_ledger_seq: int,
) -> tuple[tuple[TurnRef, ...], int]:
    candidates = set(watched_turns)
    feed_items, next_after = fetch_agent_feed_items_since(client, agent, after_ledger_seq=after_ledger_seq)
    for event in feed_items:
        if event.event_type == "turn.suspended" and event.subject_kind == "turn":
            turn = TurnRef(project_id=event.project_id, turn_id=event.subject_id)
            candidates.add(turn)
            watched_turns.add(turn)
    return tuple(candidates), next_after


def _assigned_agent_id_from_context(context) -> str | None:
    turn_id = context.turn.turn.turn_id
    for item in context.semantic_items:
        if item.record.record_role != "progress":
            continue
        payload = item.content.payload()
        if not isinstance(payload, dict):
            continue
        if payload.get("phase") in {"registration_birth", "launch_plan", "launch_result"}:
            assigned_agent_id = payload.get("assigned_agent_id")
            if isinstance(assigned_agent_id, str) and assigned_agent_id:
                return assigned_agent_id
            new_agent_ref = payload.get("new_agent_ref")
            if isinstance(new_agent_ref, dict) and isinstance(new_agent_ref.get("agent_id"), str):
                return new_agent_ref["agent_id"]
    return _assigned_agent_id(turn_id)
