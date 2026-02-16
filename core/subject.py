"""Helpers for constructing and parsing NATS subjects with protocol version."""

from dataclasses import dataclass
from typing import Optional
from .config import PROTOCOL_VERSION


@dataclass
class SubjectParts:
    protocol_version: str
    project_id: str
    channel_id: str
    category: str
    component: str
    target: str
    suffix: str


def format_subject(
    project_id: str,
    channel_id: str,
    category: str,
    component: str,
    target: str,
    suffix: str,
    protocol_version: str = PROTOCOL_VERSION,
    ) -> str:
    return ".".join(
        [
            "cg",
            protocol_version,
            project_id,
            channel_id,
            category,
            component,
            target,
            suffix,
        ]
    )


def subject_prefix(protocol_version: str = PROTOCOL_VERSION) -> str:
    return ".".join(["cg", protocol_version])


def scope_prefix(
    project_id: str,
    channel_id: str,
    protocol_version: str = PROTOCOL_VERSION,
) -> str:
    return ".".join(["cg", protocol_version, project_id, channel_id])


def scope_pattern(
    project_id: str = "*",
    channel_id: str = "*",
    protocol_version: str = PROTOCOL_VERSION,
) -> str:
    return f"{scope_prefix(project_id, channel_id, protocol_version)}.>"


def subject_pattern(
    *,
    project_id: str = "*",
    channel_id: str = "*",
    category: str,
    component: str,
    target: str,
    suffix: str,
    protocol_version: str = PROTOCOL_VERSION,
) -> str:
    return format_subject(
        project_id=project_id,
        channel_id=channel_id,
        category=category,
        component=component,
        target=target,
        suffix=suffix,
        protocol_version=protocol_version,
    )


def cmd_subject(
    project_id: str,
    channel_id: str,
    component: str,
    target: str,
    suffix: str,
    protocol_version: str = PROTOCOL_VERSION,
) -> str:
    return format_subject(project_id, channel_id, "cmd", component, target, suffix, protocol_version)


def evt_subject(
    project_id: str,
    channel_id: str,
    component: str,
    target: str,
    suffix: str,
    protocol_version: str = PROTOCOL_VERSION,
) -> str:
    return format_subject(project_id, channel_id, "evt", component, target, suffix, protocol_version)


def evt_agent_task_subject(project_id: str, channel_id: str, agent_id: str) -> str:
    return format_subject(project_id, channel_id, "evt", "agent", agent_id, "task")


def evt_agent_step_subject(project_id: str, channel_id: str, agent_id: str) -> str:
    return format_subject(project_id, channel_id, "evt", "agent", agent_id, "step")


def evt_agent_state_subject(project_id: str, channel_id: str, agent_id: str) -> str:
    return format_subject(project_id, channel_id, "evt", "agent", agent_id, "state")


def parse_subject(subject: str) -> Optional[SubjectParts]:
    parts = subject.split(".")
    if len(parts) < 8 or parts[0] != "cg":
        return None

    return SubjectParts(
        protocol_version=parts[1],
        project_id=parts[2],
        channel_id=parts[3],
        category=parts[4],
        component=parts[5],
        target=parts[6],
        suffix=".".join(parts[7:]),
    )
