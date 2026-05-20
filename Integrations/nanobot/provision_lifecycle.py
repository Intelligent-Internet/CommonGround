from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Mapping, Protocol

import httpx

from CommonGround.contracts import AGENT_CREDENTIAL_STATUS_ACTIVE, AgentRef, OperationMeta, TurnRef, TurnState


PROVISION_LIFECYCLE_METADATA_KEY = "provision_lifecycle"
PROVISION_LIFECYCLE_KIND_V1 = "nanobot.provision_lifecycle.v1"
PROVISION_LIFECYCLE_MODE_EPHEMERAL = "ephemeral"
PROVISION_RETIRE_TRIGGER_SOURCE_TURN_TERMINAL_OR_TTL = "source_turn_terminal_or_ttl"
DEFAULT_EPHEMERAL_LIFECYCLE_TTL_SECONDS = 300
_OPEN_TURN_STATES = (TurnState.QUEUED, TurnState.RUNNING, TurnState.SUSPENDED)


@dataclass(frozen=True, slots=True)
class ProvisionLifecyclePolicy:
    owner_agent_id: str
    mode: str
    source_turn_id: str
    ttl_seconds: int


@dataclass(frozen=True, slots=True)
class ProvisionLifecycleCleanupSummary:
    scanned_agents: int = 0
    eligible_agents: int = 0
    drained_agents: int = 0
    grace_period_agents: int = 0
    waiting_agents: int = 0
    timed_out_agents: int = 0
    revoked_credentials: int = 0


class ProvisionLifecycleAgentClient(Protocol):
    def get_turn(self, turn: TurnRef):
        ...

    def drain_agent(
        self,
        agent: AgentRef,
        *,
        requested_by: AgentRef | None = None,
        meta: OperationMeta | None = None,
    ) -> None:
        ...

    def list_agent_credentials(self, agent: AgentRef) -> Mapping[str, Any]:
        ...

    def revoke_agent_credential(self, agent: AgentRef, credential_id: str) -> Mapping[str, Any]:
        ...


class ProvisionLifecycleProjectionClient(Protocol):
    def list_agents(self, *, project_id: str, limit: int = 100):
        ...

    def list_turns(
        self,
        *,
        project_id: str,
        target_agent_id: str | None = None,
        state: str | None = None,
        limit: int = 100,
    ):
        ...


def build_ephemeral_lifecycle_metadata(
    *,
    owner_agent: AgentRef,
    source_turn_id: str,
    ttl_seconds: int = DEFAULT_EPHEMERAL_LIFECYCLE_TTL_SECONDS,
) -> dict[str, object]:
    return {
        "kind": PROVISION_LIFECYCLE_KIND_V1,
        "lifecycle_owner_agent_id": owner_agent.agent_id,
        "lifecycle_mode": PROVISION_LIFECYCLE_MODE_EPHEMERAL,
        "source_turn_id": source_turn_id,
        "retire_trigger": PROVISION_RETIRE_TRIGGER_SOURCE_TURN_TERMINAL_OR_TTL,
        "retire_policy": {
            "drain": True,
            "revoke_credentials": True,
            "ttl_seconds": ttl_seconds,
        },
    }


def parse_provision_lifecycle_policy(public_metadata: Mapping[str, Any]) -> ProvisionLifecyclePolicy | None:
    raw_policy = public_metadata.get(PROVISION_LIFECYCLE_METADATA_KEY)
    if not isinstance(raw_policy, Mapping):
        return None
    if raw_policy.get("kind") != PROVISION_LIFECYCLE_KIND_V1:
        return None
    owner_agent_id = raw_policy.get("lifecycle_owner_agent_id")
    mode = raw_policy.get("lifecycle_mode")
    source_turn_id = raw_policy.get("source_turn_id")
    if not isinstance(owner_agent_id, str) or not owner_agent_id:
        return None
    if not isinstance(mode, str) or not mode:
        return None
    if not isinstance(source_turn_id, str) or not source_turn_id:
        return None
    ttl_seconds = _policy_ttl_seconds(raw_policy)
    if ttl_seconds is None:
        return None
    return ProvisionLifecyclePolicy(
        owner_agent_id=owner_agent_id,
        mode=mode,
        source_turn_id=source_turn_id,
        ttl_seconds=ttl_seconds,
    )


def cleanup_ephemeral_provision_agents(
    client: ProvisionLifecycleAgentClient,
    projection_client: ProvisionLifecycleProjectionClient,
    *,
    owner_agent: AgentRef,
    now: datetime | None = None,
    agent_limit: int = 500,
    turn_limit: int = 500,
) -> ProvisionLifecycleCleanupSummary:
    now = _aware_now(now)
    page = projection_client.list_agents(project_id=owner_agent.project_id, limit=agent_limit)
    scanned = 0
    eligible = 0
    drained = 0
    grace_period = 0
    waiting = 0
    timed_out = 0
    revoked = 0

    for entry in page.items:
        scanned += 1
        public_metadata = getattr(entry, "public_metadata", {})
        if not isinstance(public_metadata, Mapping):
            continue
        policy = parse_provision_lifecycle_policy(public_metadata)
        if policy is None or policy.mode != PROVISION_LIFECYCLE_MODE_EPHEMERAL:
            continue
        if policy.owner_agent_id != owner_agent.agent_id:
            continue
        target_agent = AgentRef(project_id=owner_agent.project_id, agent_id=entry.agent_id)
        source_turn = _get_source_turn(client, owner_agent.project_id, policy.source_turn_id)
        if source_turn is None or source_turn.state != TurnState.CLOSED:
            continue
        eligible += 1

        open_turns = _list_open_turns(
            projection_client,
            project_id=owner_agent.project_id,
            target_agent_id=target_agent.agent_id,
            limit=turn_limit,
        )
        timeout_reached = _timeout_reached(source_turn.closed_at, now=now, ttl_seconds=policy.ttl_seconds)
        if open_turns and not timeout_reached:
            waiting += 1
            continue
        if open_turns and timeout_reached:
            timed_out += 1

        if getattr(entry, "accepts_work", False):
            client.drain_agent(
                target_agent,
                requested_by=owner_agent,
                meta=_cleanup_meta(policy=policy, action="drain", timed_out=False),
            )
            drained += 1
            grace_period += 1
            continue

        revoked += _revoke_active_credentials(client, target_agent)

    return ProvisionLifecycleCleanupSummary(
        scanned_agents=scanned,
        eligible_agents=eligible,
        drained_agents=drained,
        grace_period_agents=grace_period,
        waiting_agents=waiting,
        timed_out_agents=timed_out,
        revoked_credentials=revoked,
    )


def _policy_ttl_seconds(raw_policy: Mapping[str, Any]) -> int | None:
    retire_policy = raw_policy.get("retire_policy")
    ttl_value: Any
    if isinstance(retire_policy, Mapping):
        ttl_value = retire_policy.get("ttl_seconds", DEFAULT_EPHEMERAL_LIFECYCLE_TTL_SECONDS)
    else:
        ttl_value = DEFAULT_EPHEMERAL_LIFECYCLE_TTL_SECONDS
    if isinstance(ttl_value, bool):
        return None
    if isinstance(ttl_value, int) and ttl_value > 0:
        return ttl_value
    return None


def _get_source_turn(client: ProvisionLifecycleAgentClient, project_id: str, turn_id: str):
    try:
        return client.get_turn(TurnRef(project_id=project_id, turn_id=turn_id))
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 404:
            return None
        raise


def _list_open_turns(
    projection_client: ProvisionLifecycleProjectionClient,
    *,
    project_id: str,
    target_agent_id: str,
    limit: int,
) -> tuple[object, ...]:
    items = []
    for state in _OPEN_TURN_STATES:
        page = projection_client.list_turns(
            project_id=project_id,
            target_agent_id=target_agent_id,
            state=state.value,
            limit=limit,
        )
        items.extend(page.items)
    return tuple(items)


def _timeout_reached(closed_at: datetime | None, *, now: datetime, ttl_seconds: int) -> bool:
    if closed_at is None:
        return False
    return _aware_now(closed_at) + timedelta(seconds=ttl_seconds) <= now


def _revoke_active_credentials(client: ProvisionLifecycleAgentClient, agent: AgentRef) -> int:
    payload = client.list_agent_credentials(agent)
    credentials = payload.get("credentials", ())
    revoked = 0
    for credential in credentials:
        if not isinstance(credential, Mapping):
            continue
        if credential.get("status") != AGENT_CREDENTIAL_STATUS_ACTIVE:
            continue
        credential_id = credential.get("credential_id")
        if not isinstance(credential_id, str) or not credential_id:
            continue
        client.revoke_agent_credential(agent, credential_id)
        revoked += 1
    return revoked


def _cleanup_meta(*, policy: ProvisionLifecyclePolicy, action: str, timed_out: bool) -> OperationMeta:
    return OperationMeta(
        reason=f"provision_lifecycle_{action}",
        annotations={
            "provision_lifecycle": {
                "kind": PROVISION_LIFECYCLE_KIND_V1,
                "source_turn_id": policy.source_turn_id,
                "lifecycle_mode": policy.mode,
                "retire_trigger": PROVISION_RETIRE_TRIGGER_SOURCE_TURN_TERMINAL_OR_TTL,
                "timed_out": timed_out,
            }
        },
    )


def _aware_now(value: datetime | None) -> datetime:
    if value is None:
        return datetime.now(UTC)
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value
