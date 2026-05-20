from __future__ import annotations

from dataclasses import dataclass

from CommonGround.contracts import NotFoundError, TURN_KIND_CONVERSATION_V1


NANOBOT_LEAF_CONVERSATION_ROLE = "nanobot.leaf.conversation.v1"


@dataclass(frozen=True, slots=True)
class RolePolicy:
    role: str
    allowed_capabilities: tuple[str, ...]
    allowed_grants: tuple[str, ...]
    allowed_accepts_work: bool = True
    description: str = ""


_ROLE_POLICIES = {
    NANOBOT_LEAF_CONVERSATION_ROLE: RolePolicy(
        role=NANOBOT_LEAF_CONVERSATION_ROLE,
        allowed_capabilities=(TURN_KIND_CONVERSATION_V1,),
        allowed_grants=(),
        allowed_accepts_work=True,
        description="NanoBot leaf worker for conversation turns.",
    ),
}


def resolve_role_policy(role: str) -> RolePolicy:
    policy = _ROLE_POLICIES.get(role)
    if policy is None:
        raise NotFoundError(f"provision role not found: {role}")
    return policy


def published_available_roles() -> list[dict[str, str | None]]:
    return [
        {
            "role": policy.role,
            "description": policy.description or None,
        }
        for policy in _ROLE_POLICIES.values()
    ]
