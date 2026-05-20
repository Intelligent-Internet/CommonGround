from __future__ import annotations

from typing import Any

from CommonGround.contracts import TURN_KIND_CONVERSATION_V1, TURN_KIND_PROVISION_AGENT_SPAWN_V1


def conversation_turn_offer(*, purpose: str | None = None) -> dict[str, Any]:
    return {
        "turn_kind": TURN_KIND_CONVERSATION_V1,
        "purpose": purpose or "Handle a general conversation turn and return the final deliverable.",
        "calling": _dispatch_calling(),
        "input_contract": {
            "required_fields": [],
            "example_payload": {"task": "Summarize the latest status."},
        },
        "variants": {},
        "notes": "Conversation-specific stop/resume/finish semantics remain turn-owned semantics.",
    }


def provision_turn_offer(*, roles: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "turn_kind": TURN_KIND_PROVISION_AGENT_SPAWN_V1,
        "purpose": "Provision and admit a new leaf agent.",
        "calling": _dispatch_calling(),
        "input_contract": {
            "required_fields": ["agent.role"],
            "example_payload": {"agent": {"role": "nanobot.leaf.conversation.v1"}},
        },
        "variants": {
            "roles": roles,
        },
        "notes": "Agent birth uses the service-authorized registration surface; provenance stays tied to the provision turn.",
    }


def _dispatch_calling() -> dict[str, Any]:
    return {
        "operation": "dispatch",
        "authority_modes": [
            {
                "mode": "root_request",
                "required_authority": ["trusted_requester_identity"],
            },
            {
                "mode": "child_derivation",
                "required_authority": ["active_parent_claim"],
                "binds_cause_to_current_turn": True,
            },
        ],
    }
