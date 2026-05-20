from __future__ import annotations

from datetime import UTC, datetime
import logging
import time
from pathlib import Path

from CommonGround.contracts import AgentRef, TURN_KIND_CONVERSATION_V1, TurnRef, TurnState
from CommonGround.agent_client import HttpAgentClient, PollingWorker
from CommonGround.turn_offers import upsert_turn_offer

from ..adapter.supervisor_handler import SupervisorTurnHandler
from ..turn_offer_metadata import conversation_turn_offer
from .client_auth import build_agent_client
from .feed_utils import fetch_agent_feed_items_since, fetch_turn_feed_items
from .factory import build_agent_loop
from .presence import PresenceHeartbeater

logger = logging.getLogger(__name__)


def run_supervisor_worker_forever(
    *,
    base_url: str,
    agent: AgentRef,
    provisioner_agent: AgentRef,
    config_path: str | Path | None = None,
    workspace: str | Path | None = None,
    repo_root: str | Path | None = None,
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
    loop = build_agent_loop(config_path=config_path, workspace=workspace, repo_root=repo_root)
    handler = SupervisorTurnHandler(
        loop=loop,
        provisioner_agent=provisioner_agent,
    )
    worker = PollingWorker(client=client, agent=agent, handler=handler)
    presence_heartbeater = PresenceHeartbeater(
        heartbeat_fn=lambda: client.heartbeat_agent_presence(agent),
        interval_seconds=presence_interval_seconds,
    )
    watched_turns: set[TurnRef] = set()
    agent_feed_after_ledger_seq = 0
    last_reconcile_at = 0.0
    logger.info(
        "starting supervisor worker agent=%s provisioner=%s project=%s base_url=%s workspace=%s",
        agent.agent_id,
        provisioner_agent.agent_id,
        agent.project_id,
        base_url,
        workspace,
    )
    try:
        presence_heartbeater.start()
        while True:
            now = time.monotonic()
            if reconcile_interval_seconds is not None and reconcile_interval_seconds > 0 and now - last_reconcile_at >= reconcile_interval_seconds:
                client.reconcile_expired_claim(agent)
                logger.debug("reconciled expired claims for supervisor=%s", agent.agent_id)
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
                logger.info("watching suspended parent turn=%s", result.claimed_turn.turn_id)
            if result.claimed_turn is not None:
                logger.info("worker result agent=%s turn=%s action=%s", agent.agent_id, result.claimed_turn.turn_id, result.action)
            if result.action == "idle":
                time.sleep(idle_sleep_seconds)
    finally:
        presence_heartbeater.stop()
        logger.info("stopping supervisor worker agent=%s", agent.agent_id)
        client.close()


def _resume_ready_parent_turns(
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
        feed_items = fetch_turn_feed_items(client, turn)
        spawned_child_ids = [
            event.subject_id
            for event in feed_items
            if event.event_type == "turn.spawned" and event.subject_id != turn.turn_id
        ]
        finished_child_ids = {
            event.subject_id
            for event in feed_items
            if event.event_type == "turn.finished" and event.subject_id != turn.turn_id
        }
        if not spawned_child_ids:
            continue
        child_snapshots = {
            child_id: client.get_turn(TurnRef(project_id=turn.project_id, turn_id=child_id))
            for child_id in spawned_child_ids
        }
        if all(child_id in finished_child_ids for child_id in spawned_child_ids):
            resume_note = "children_finished"
        elif _ready_for_partial_child_resume(child_snapshots.values(), now=datetime.now(UTC)):
            resume_note = "children_finished_or_expired"
        else:
            continue
        client.resume_turn(agent, turn, note=resume_note)
        logger.info("resumed parent turn=%s after child readiness note=%s", turn.turn_id, resume_note)
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


def _ready_for_partial_child_resume(child_snapshots, *, now: datetime) -> bool:
    snapshots = tuple(child_snapshots)
    if any(not hasattr(snapshot, "turn_kind") for snapshot in snapshots):
        return False
    conversation_children = [snapshot for snapshot in snapshots if snapshot.turn_kind == TURN_KIND_CONVERSATION_V1]
    if not conversation_children:
        return False
    non_conversation_children = [snapshot for snapshot in snapshots if snapshot.turn_kind != TURN_KIND_CONVERSATION_V1]
    if any(snapshot.outcome is None for snapshot in non_conversation_children):
        return False
    return all(
        snapshot.outcome is not None or _is_expired_running_child(snapshot, now=now)
        for snapshot in conversation_children
    )


def _is_expired_running_child(snapshot, *, now: datetime) -> bool:
    if snapshot.outcome is not None:
        return False
    claim_expires_at = getattr(snapshot, "claim_expires_at", None)
    if claim_expires_at is None:
        return False
    return claim_expires_at <= now
