from __future__ import annotations

from typing import Any, Dict, List

from core.utils import safe_str


def is_submit_result_tool_spec(tool_spec: Any) -> bool:
    if not isinstance(tool_spec, dict):
        return False
    function = tool_spec.get("function")
    if not isinstance(function, dict):
        return False
    return safe_str(function.get("name")) == "submit_result"


def apply_step_budget_controls(
    *,
    item: Any,
    messages: Any,
    tools: Any,
    max_steps: int,
    logger: Any,
) -> tuple[List[Dict[str, Any]], Any]:
    llm_messages: List[Dict[str, Any]] = list(messages) if isinstance(messages, list) else []
    remaining_steps = max(0, int(max_steps) - int(item.step_count))
    if remaining_steps > 2:
        return llm_messages, tools

    submit_tools: List[Dict[str, Any]] = []
    if isinstance(tools, list):
        submit_tools = [t for t in tools if is_submit_result_tool_spec(t)]
    has_submit_result = bool(submit_tools)

    if remaining_steps == 1:
        if has_submit_result:
            alert = (
                f"[SYSTEM ALERT] Execution limit reached: final step (1/{max_steps} remaining). "
                "You MUST call submit_result now. submit_result MUST be the ONLY tool call."
            )
            if isinstance(tools, list) and len(tools) != len(submit_tools):
                logger.info(
                    "Step budget guard: final step turn=%s agent=%s; restricting tools to submit_result only.",
                    item.agent_turn_id,
                    item.agent_id,
                )
            tools = submit_tools
        else:
            alert = (
                f"[SYSTEM ALERT] Execution limit reached: final step (1/{max_steps} remaining). "
                "No submit_result tool is available in this profile; provide the best possible final answer now."
            )
    else:
        if has_submit_result:
            alert = (
                f"[SYSTEM ALERT] Execution limits approaching: {remaining_steps} steps remaining "
                f"(max_steps={max_steps}). Start consolidating results now and plan to call submit_result "
                "in the next step."
            )
        else:
            alert = (
                f"[SYSTEM ALERT] Execution limits approaching: {remaining_steps} steps remaining "
                f"(max_steps={max_steps}). Start consolidating results now."
            )

    llm_messages.append({"role": "system", "content": alert})
    return llm_messages, tools
