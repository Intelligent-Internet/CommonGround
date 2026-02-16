"""Helpers to build LLM tool specs from DB definitions."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from infra.llm.tool_specs import build_tool_specs as _build_tool_specs

from .builtin_tools import BUILTIN_AFTER_EXEC

logger = logging.getLogger("AgentWorker.ToolSpec")


def build_tool_specs(tool_defs: List[Any]) -> List[Dict[str, Any]]:
    """Convert `resource.tools` rows to OpenAI-style tool specs."""
    return _build_tool_specs(tool_defs)


def filter_allowed_tools(
    tool_defs: List[Any],
    *,
    allowed_tools: Optional[List[str]],
    project_id: str,
    agent_id: str,
) -> List[Any]:
    """Filter tool_defs by allowed_tools and emit warnings for mismatches."""
    if allowed_tools is None:
        return tool_defs

    allowed_set = set(allowed_tools or [])
    builtin_set = set(BUILTIN_AFTER_EXEC.keys())
    # `allowed_tools` primarily controls external tools (resource.tools). Built-in tools
    # (e.g. submit_result) are not in resource.tools and should not trigger warnings here.
    external_allowed_set = {name for name in allowed_set if name not in builtin_set}
    available_names = {t.tool_name for t in tool_defs}

    if external_allowed_set:
        missing = external_allowed_set - available_names
        if missing:
            logger.warning(
                "Profile allowed_tools contains unknown tool(s): %s",
                sorted(missing),
            )
        if not (external_allowed_set & available_names):
            logger.warning(
                "Profile allowed_tools has no matches in resource.tools (project=%s agent=%s).",
                project_id,
                agent_id,
            )
    elif not allowed_set:
        logger.info(
            "Profile allowed_tools is empty; no external tools will be exposed (project=%s agent=%s).",
            project_id,
            agent_id,
        )
    else:
        # allowlist contains only builtin tool(s)
        logger.info(
            "Profile allowed_tools contains only builtin tool(s): %s; no external tools will be exposed (project=%s agent=%s).",
            sorted(allowed_set & builtin_set),
            project_id,
            agent_id,
        )

    return [t for t in tool_defs if t.tool_name in external_allowed_set]
