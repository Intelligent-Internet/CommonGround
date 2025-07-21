import logging
import json
import re
import uuid
from typing import Any, Dict, Callable, List
from ..utils.context_helpers import get_nested_value_from_context
from ..framework.profile_utils import get_profile_by_instance_id

logger = logging.getLogger(__name__)

INGESTOR_REGISTRY: Dict[str, Callable] = {}

def register_ingestor(name: str) -> Callable:
    """Decorator to register a new ingestor function."""
    def decorator(func: Callable[[Any, Dict, Dict], str]) -> Callable[[Any, Dict, Dict], str]:
        if name in INGESTOR_REGISTRY:
            logger.warning("ingestor_being_overridden", extra={"ingestor_name": name})
        INGESTOR_REGISTRY[name] = func
        return func
    return decorator

def _apply_simple_template_interpolation(text_content: str, context: Dict) -> str:
    """
    A helper function for simple template interpolation in a given text content.
    """
    if not isinstance(text_content, str) or '{{' not in text_content:
        return text_content

    def replace_match(match):
        path_from_template = match.group(1).strip()
        value = get_nested_value_from_context(context, path_from_template, default=f"{{{{ {path_from_template} }}}}")
        return str(value) if value is not None else ""

    return re.sub(r"\{\{\s*([\w\._-]+)\s*\}\}", replace_match, text_content)


@register_ingestor("templated_content_ingestor")
def templated_content_ingestor(payload: Any, params: Dict, context: Dict) -> str:
    """
    An intelligent ingestor that uses 'content_key' from the payload to find a template
    in the Agent's text_definitions, and then performs variable interpolation on it.
    """
    if not isinstance(payload, dict) or "content_key" not in payload:
        logger.warning("templated_content_ingestor_invalid_payload", extra={"payload": payload})
        return f"[Error: Ingestor received an invalid payload: {payload}]"

    content_key = payload["content_key"]
    loaded_profile = context.get("loaded_profile", {})
    text_definitions = loaded_profile.get("text_definitions", {})
    
    template_string = text_definitions.get(content_key)
    
    if not template_string:
        logger.error("content_key_not_found_in_profile", extra={"content_key": content_key, "profile_name": loaded_profile.get('name')})
        return f"[Error: Template '{content_key}' not found]"

    rendered_content = _apply_simple_template_interpolation(template_string, context)
    
    wrapper_tags = params.get("wrapper_tags")
    if wrapper_tags and isinstance(wrapper_tags, list) and len(wrapper_tags) == 2:
        return f"{wrapper_tags[0]}{rendered_content}{wrapper_tags[1]}"
        
    return rendered_content

@register_ingestor("generic_message_ingestor")
def generic_message_ingestor(payload: Any, params: Dict, context: Dict) -> str:
    """Uses a template and payload to generate a content string."""
    template = params.get("content_template", "{{ payload }}")
    if isinstance(payload, dict):
        # A simple template replacement, can be extended to a more complex template engine in the future
        for key, value in payload.items():
            template = template.replace(f"{{{{ payload.{key} }}}}", str(value))
    return template.replace("{{ payload }}", str(payload))

@register_ingestor("tool_result_ingestor")
def tool_result_ingestor(payload: Any, params: Dict, context: Dict) -> str:
    """Intelligently formats tool results, prioritizing Markdown over JSON when appropriate."""
    if not isinstance(payload, dict):
        return str(payload)

    tool_name = payload.get("tool_name")
    content = payload.get("content", {})
    is_error = payload.get("is_error", False)

    # If it's a dehydrated token, return it directly
    if isinstance(content, str) and content.startswith("<#CGKB-"):
        return content
    
    # For errors, use JSON to preserve full context with clear wrapping tags
    if is_error:
        error_report = {
            "tool_execution_failed": True,
            "tool_name": tool_name,
            "error_details": content
        }
        return f"<tool_error_report>\n{json.dumps(error_report, indent=2, ensure_ascii=False)}\n</tool_error_report>"

    # Special handling for dispatch_submodules
    if tool_name == "dispatch_submodules":
        return dispatch_result_ingestor(payload, params, context)

    # Prioritize "main_content_for_llm" if specified in content
    if isinstance(content, dict) and "main_content_for_llm" in content:
        main_content = content["main_content_for_llm"]
        return "\n".join(_recursive_markdown_formatter(main_content, {}, level=0))

    # (Rule 5: (New) Add an escape hatch if the tool really needs to return raw JSON)
    if isinstance(content, dict) and "_raw_json" in content:
        return json.dumps(content["_raw_json"], indent=2, ensure_ascii=False)

    # Default behavior - use Markdown formatter
    return "\n".join(_recursive_markdown_formatter(content, {}, level=0))

@register_ingestor("markdown_formatter_ingestor")
def markdown_formatter_ingestor(payload: Any, params: Dict, context: Dict) -> str:
    """Converts a payload (usually a dictionary) into a Markdown list."""
    if not isinstance(payload, dict):
        return str(payload)
    
    lines = []
    title = params.get("title", "### Contextual Information")
    lines.append(title)
    
    key_renames = params.get("key_renames", {})
    exclude_keys = params.get("exclude_keys", [])

    for key, value in payload.items():
        if key in exclude_keys:
            continue
        display_key = key_renames.get(key, key.replace('_', ' ').title())
        lines.append(f"*   **{display_key}**: {value}")
        
    return "\n".join(lines)

@register_ingestor("work_modules_ingestor")
def work_modules_ingestor(payload: Any, params: Dict, context: Dict) -> str:
    """Formats work_modules dictionary as Markdown using the generic formatter."""
    if not isinstance(payload, dict):
        return "Work modules data is not in the expected format (dictionary)."
    
    lines = [params.get("title", "### Current Work Modules Status")]
    if not payload:
        lines.append("No work modules are currently defined.")
    else:
        # Use the formatter to handle the entire dictionary
        formatted_modules = _recursive_markdown_formatter(payload, {}, level=0)
        lines.extend(formatted_modules)
        
    return "\n".join(lines)

@register_ingestor("available_associates_ingestor")
def available_associates_ingestor(payload: Any, params: Dict, context: Dict) -> str:
    """Formats available Associate list as Markdown with separated data preparation and presentation logic."""
    profile_instance_ids = payload
    if not isinstance(profile_instance_ids, list):
        return "Available associates list is not in the expected format (list)."

    agent_profiles_store = context.get('refs', {}).get('run', {}).get('config', {}).get('agent_profiles_store')
    if not agent_profiles_store:
        logger.error("available_associates_ingestor: 'agent_profiles_store' not found.")
        return "Error: Profile store not available."

    # Step 1: Prepare a clean, structured list of Python objects
    profiles_for_llm = []
    for instance_id in profile_instance_ids:
        profile_dict = get_profile_by_instance_id(agent_profiles_store, instance_id)
        if not profile_dict or profile_dict.get("is_deleted") or not profile_dict.get("is_active") or profile_dict.get("type") != "associate":
            continue
        
        profiles_for_llm.append({
            "profile_name": profile_dict.get('name', 'Unknown'),
            "description": profile_dict.get('description_for_human', 'No description.'),
            "key_toolsets": profile_dict.get("tool_access_policy", {}).get("allowed_toolsets", [])
        })

    # Step 2: Hand the prepared data to the formatter for presentation
    lines = [params.get("title", "### Available Associate Agent Profiles")]
    if not profiles_for_llm:
        lines.append("No 'associate' type profiles are currently available.")
    else:
        sorted_profiles = sorted(profiles_for_llm, key=lambda p: p.get('profile_name', ''))
        formatted_profiles = _recursive_markdown_formatter(sorted_profiles, {}, level=0)
        lines.extend(formatted_profiles)
        
    return "\n".join(lines)

@register_ingestor("principal_history_summary_ingestor")
def principal_history_summary_ingestor(payload: Any, params: Dict, context: Dict) -> str:
    """
    Receives the Principal's message history and generates a concise summary to inject into the Partner's prompt.
    """
    if not isinstance(payload, list) or not payload:
        return "<principal_activity_log>\nPrincipal has no recorded activity yet.\n</principal_activity_log>"

    max_messages = params.get("max_messages", 10) if params else 10
    messages_to_format = payload[-max_messages:]

    output_parts = ["<principal_activity_log>"]
    for msg in messages_to_format:
        role = msg.get("role", "unknown_role")
        content_summary = str(msg.get("content", ""))
        tool_calls = msg.get("tool_calls")
        
        entry = f"\n- **[{role.upper()}]**: {content_summary[:200]}{'...' if len(content_summary) > 200 else ''}"
        
        if tool_calls and isinstance(tool_calls, list):
            tools_called_parts = []
            for tc in tool_calls:
                func_name = tc.get("function", {}).get("name", "N/A")
                args_str = tc.get("function", {}).get("arguments", "{}")
                args_summary = args_str[:70] + '...' if len(args_str) > 70 else args_str
                tools_called_parts.append(f"{func_name}({args_summary})")
            if tools_called_parts:
                entry += f" -> Calls: [{', '.join(tools_called_parts)}]"
        
        output_parts.append(entry)
    
    if len(payload) > max_messages:
        output_parts.append(f"\n... (omitting {len(payload) - max_messages} older messages)")
        
    output_parts.append("\n</principal_activity_log>")
    return "\n".join(output_parts)

@register_ingestor("json_history_ingestor")
def json_history_ingestor(payload: Any, params: Dict, context: Dict) -> str:
    """Serializes the message history list into a JSON string and wraps it with tags."""
    if not isinstance(payload, list):
        logger.warning("json_history_ingestor_expected_list", extra={"payload_type": type(payload).__name__})
        return "[Error: Message history for JSON ingestion was not a list.]"
    
    try:
        history_json_string = json.dumps(payload, ensure_ascii=False, indent=2)
        return f"<message_history_json>\n{history_json_string}\n</message_history_json>"
    except Exception as e:
        logger.error("message_history_serialization_failed", extra={"error": str(e)}, exc_info=True)
        return f"[Error: Failed to serialize message history to JSON: {e}]"

@register_ingestor("tagged_content_ingestor")
def tagged_content_ingestor(payload: Any, params: Dict, context: Dict) -> str:
    """Wraps the payload content with specified XML tags."""
    wrapper_tags = params.get("wrapper_tags")
    content = str(payload)
    
    if wrapper_tags and isinstance(wrapper_tags, list) and len(wrapper_tags) == 2:
        return f"{wrapper_tags[0]}{content}{wrapper_tags[1]}"
    
    logger.warning("tagged_content_ingestor_missing_wrapper_tags")
    return content

@register_ingestor("observer_failure_ingestor")
def observer_failure_ingestor(payload: Any, params: Dict, context: Dict) -> str:
    """Formats Observer failure events for injection into the LLM context."""
    if not isinstance(payload, dict):
        return "[Error: Observer failure payload was malformed.]"

    failed_observer_id = payload.get("failed_observer_id", "unknown_observer")
    error_message = payload.get("error_message", "unknown_error")

    # Consistent with the error format above
    return (
        f"<system_error context_source='internal_observer'>\n"
        f"  <error_details>\n"
        f"    <summary>A critical internal error occurred while I was observing the state to generate context. My internal rule (Observer ID: '{failed_observer_id}') failed to execute.</summary>\n"
        f"    <reason>{error_message}</reason>\n"
        f"  </error_details>\n"
        f"  <instruction>\n"
        f"    **Action Required: You MUST inform the user about this internal error.**\n"
        f"    1.  First, formulate your primary response to the user based on the rest of the available, uncorrupted context.\n"
        f"    2.  Then, at the end of your response, you MUST append a notification to the user about this issue. For example, add a line like: 'Note: An internal error occurred while processing some background context, which might affect the completeness of this response.'\n"
        f"    3.  You MUST NOT stop your work. Continue the task to the best of your ability with the remaining information.\n"
        f"  </instruction>\n"
        f"</system_error>"
    )

@register_ingestor("dispatch_result_ingestor")
def dispatch_result_ingestor(payload: Any, params: Dict, context: Dict) -> str:
    """Formats 'dispatch_submodules' results into detailed, human and LLM-readable Markdown reports."""
    if not isinstance(payload, dict) or "content" not in payload:
        return "[Error: Dispatch result format is invalid or content is missing]"
    
    content = payload.get("content", {})
    
    # Overall operation summary
    overall_status = content.get('status', 'UNKNOWN')
    message = content.get('message', 'No message.')
    summary_parts = [
        "**Dispatch Operation Summary**",
        f"- **Overall Status**: `{overall_status}`",
        f"- **Details**: {message}"
    ]
    
    # Failed preparation tasks
    failed_prep = content.get("failed_preparation_details", [])
    if failed_prep:
        summary_parts.append("\n**Assignments Failed Before Execution:**")
        # Use the formatter to show failure details
        summary_parts.extend(_recursive_markdown_formatter(failed_prep, {}, level=0))

    # Detailed work records of executed modules
    exec_results = content.get("assignment_execution_results", [])
    if exec_results:
        summary_parts.append("\n**Executed Modules - Detailed Work Records:**")
        for result in exec_results:
            module_id = result.get('module_id', 'N/A')
            exec_status = result.get('execution_status', 'unknown')
            
            summary_parts.append(f"\n--- Start of Record for Module `{module_id}` (Status: `{exec_status}`) ---")
            
            # Display final deliverables
            deliverables = result.get('deliverables', {})
            summary_parts.append("#### Final Deliverable (from Associate):")
            summary_parts.extend(_recursive_markdown_formatter(deliverables, {}, level=0))

            # Display the full net-added message history
            new_messages = result.get('new_messages_from_associate', [])
            if new_messages:
                summary_parts.append("\n#### Full Work Log from Associate:")
                for msg in new_messages:
                    role = msg.get("role", "unknown").upper()
                    msg_content = str(msg.get("content", "[No Content]")).strip()
                    tool_calls = msg.get("tool_calls")
                    
                    if tool_calls:
                        summary_parts.append(f"**[{role} -> TOOL_CALL]**:")
                        summary_parts.extend(_recursive_markdown_formatter(tool_calls, {}, level=1))
                    elif msg_content:
                       summary_parts.append(f"**[{role}]**: {msg_content}")
            
            summary_parts.append(f"--- End of Record for Module `{module_id}` ---\n")
    
    return "\n".join(summary_parts)

@register_ingestor("user_prompt_ingestor")
def user_prompt_ingestor(payload: Any, params: Dict, context: Dict) -> str:
    """A simple ingestor to extract the user prompt from the payload."""
    if isinstance(payload, dict):
        return payload.get("prompt", "")
    return str(payload)

def _recursive_markdown_formatter(data: Any, schema: Dict, level: int = 0) -> List[str]:
    """
    Intelligently formats data recursively into LLM-friendly Markdown.
    Prioritizes schema-based rendering with graceful handling of empty structures.
    """
    lines = []
    indent = "  " * level

    # Prioritize rendering using schema
    if schema.get("type") == "object" and "properties" in schema and isinstance(data, dict):
        for prop_name, prop_schema in schema.get("properties", {}).items():
            if prop_name in data:
                title = prop_schema.get("x-handover-title", prop_name.replace('_', ' ').title())
                value = data[prop_name]
                lines.append(f"{indent}* **{title}:**")
                sub_lines = _recursive_markdown_formatter(value, prop_schema, level + 1)
                lines.extend(sub_lines)
        return lines
    
    # Smart fallback logic when no detailed schema is available
    if isinstance(data, dict):
        if not data:
            lines.append(f"{indent}  (empty)")
            return lines
        for key, value in sorted(data.items()):
            title = str(key).replace('_', ' ').title()
            lines.append(f"{indent}* **{title}:**")
            sub_lines = _recursive_markdown_formatter(value, {}, level + 1)
            lines.extend(sub_lines)
    elif isinstance(data, list):
        if not data:
            lines.append(f"{indent}  (empty)")
            return lines
        for item in data:
            # List items themselves don't add indent
            sub_lines = _recursive_markdown_formatter(item, schema.get("items", {}), level)
            lines.extend(sub_lines)
    elif isinstance(data, str):
        # Handle multi-line strings correctly
        for line in data.strip().split('\n'):
            lines.append(f"{indent}  {line}")
    else:
        # Handle other primitive types
        lines.append(f"{indent}  {str(data)}")
        
    return lines

@register_ingestor("protocol_aware_ingestor")
def protocol_aware_ingestor(payload: Any, params: Dict, context: Dict) -> str:
    """Renders a payload generated by HandoverService using its accompanying schema."""
    if not isinstance(payload, dict) or "data" not in payload or "schema_for_rendering" not in payload:
        logger.warning("protocol_aware_ingestor_invalid_payload", extra={"payload": payload})
        return "[Error: Malformed handover payload]"

    data = payload["data"]
    schema = payload["schema_for_rendering"]
    
    # Use top-level title from schema if available
    top_level_title = schema.get("x-handover-title", "Agent Briefing")
    
    lines = [f"## {top_level_title}"]
    
    # Start the recursive formatting
    formatted_lines = _recursive_markdown_formatter(data, schema, level=0)
    lines.extend(formatted_lines)
    
    return "\n".join(lines)

logger.info("ingestor_registry_initialized", extra={"count": len(INGESTOR_REGISTRY), "ingestors": list(INGESTOR_REGISTRY.keys())})
