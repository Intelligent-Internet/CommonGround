"""
Manages the registration and discovery of all tools available to agents.

This includes internal Python-based tools, tools from MCP servers, and
handles dynamic availability based on Agent Profiles.
"""

import inspect
import logging
import importlib
from pathlib import Path
import sys
import yaml

logger = logging.getLogger(__name__)

from typing import Optional, Dict, List, Any
from mcp.client.session_group import ClientSessionGroup

MCP_PROMPT_OVERRIDE_FILE = Path("mcp_prompt_override.yaml")
MCP_TOOLS_CACHE_FILE = Path(".current_mcp_tools.yaml")

_TOOL_REGISTRY = {}


def _sanitize_schema_for_api(schema: Any) -> Any:
    """
    Recursively removes any keys starting with 'x-' from a JSON schema-like dictionary.
    This is used to clean up our internal metadata (like x-handover-title)
    before sending the schema to an external LLM API.
    """
    if isinstance(schema, dict):
        # Create a new dictionary, only including keys that do not start with 'x-'
        return {
            key: _sanitize_schema_for_api(value)
            for key, value in schema.items()
            if not str(key).startswith('x-')
        }
    elif isinstance(schema, list):
        # Recursively process each item in the list
        return [_sanitize_schema_for_api(item) for item in schema]
    else:
        # Return primitives and other types as-is
        return schema


def tool_registry(
    name: str,
    description: str,
    parameters: Dict,
    ends_flow: bool = False,
    toolset_name: Optional[str] = None,
    handover_protocol: Optional[str] = None,
    context_segment_contributions: Optional[List[Dict]] = None,
    default_knowledge_item_type: Optional[str] = None,
    source_uri_field_in_output: Optional[str] = None,
    title_field_in_output: Optional[str] = None
):
    """
    A decorator to register a Node or Flow class as a tool callable by an LLM.

    Args:
        name: The name of the tool.
        description: The tool's description in English.
        parameters: The tool's parameters in OpenAI Function Call format.
        ends_flow: If True, executing this tool terminates the current flow.
        toolset_name: The name of the toolset this tool belongs to.
        context_segment_contributions: A list of context contributions for prompts.
        default_knowledge_item_type: The default KB item type this tool produces.
        source_uri_field_in_output: Field in the output containing the source URI.
        title_field_in_output: Field in the output containing the title.
    """
    def decorator(cls):
        from pocketflow import BaseNode
        if not (inspect.isclass(cls) and issubclass(cls, BaseNode)):
            raise TypeError(f"@tool_registry can only be applied to subclasses of BaseNode, not {cls}")
        
        actual_toolset_name = toolset_name if toolset_name else name

        tool_info = {
            "name": name,
            "description": description,
            "parameters": parameters,
            "handover_protocol": handover_protocol,
            "node_class": cls,
            "ends_flow": ends_flow,
            "toolset_name": actual_toolset_name,
            "implementation_type": "internal",
            "context_segment_contributions": context_segment_contributions or [],
            "default_knowledge_item_type": default_knowledge_item_type,
            "source_uri_field_in_output": source_uri_field_in_output,
            "title_field_in_output": title_field_in_output
        }
        
        if name in _TOOL_REGISTRY:
            logger.warning("tool_registration_overwrite", extra={"description": "Tool name already exists and will be overwritten", "tool_name": name})
        _TOOL_REGISTRY[name] = tool_info
        
        cls._tool_info = tool_info
        
        logger.debug("tool_registered", extra={"description": "Registered tool", "tool_name": name, "toolset_name": actual_toolset_name, "ends_flow": ends_flow})
        return cls
    
    return decorator

def get_registered_tools():
    """Gets all registered tool definitions."""
    return list(_TOOL_REGISTRY.values())

def get_tool_by_name(name):
    """Gets a registered tool by its name."""
    return _TOOL_REGISTRY.get(name)

def get_tool_node_class(name):
    """Gets the Node or Flow class corresponding to a tool name."""
    tool = get_tool_by_name(name)
    return tool["node_class"] if tool else None

def get_tools_by_toolset_names(toolset_names: List[str]) -> List[Dict]:
    """
    Retrieves tool definitions based on a list of toolset names.

    Args:
        toolset_names: A list of toolset names.

    Returns:
        A list of tool definitions belonging to the specified toolsets.
    """
    if not toolset_names:
        return []
    
    toolset_names_set = set(toolset_names)
    
    return [
        tool_info for tool_info in get_registered_tools()
        if tool_info.get("toolset_name") in toolset_names_set
    ]

def get_all_toolsets_with_tools() -> Dict[str, List[Dict]]:
    """
    Gets all toolsets and their associated tool information.
    
    Returns:
        A dictionary where keys are toolset names and values are lists of
        tool information for that toolset.
    """
    tools_to_process = get_registered_tools()

    toolsets_data = {}
    for tool_info in tools_to_process:
        toolset_name = tool_info.get("toolset_name", tool_info["name"])
        if toolset_name not in toolsets_data:
            toolsets_data[toolset_name] = []
        
        final_description_for_client = tool_info.get("description", "")
        
        toolsets_data[toolset_name].append({
            "name": tool_info["name"],
            "description": final_description_for_client,
            "parameters": tool_info.get("parameters", {}),
        })
    return toolsets_data

def format_tools_for_llm_api(tools_list: List[Dict]) -> List[Dict]:
    """
    Formats a list of tools into the format required by the LLM API,
    appending toolset information to the description and sanitizing the schema.
    
    Args:
        tools_list: A list of tool information dictionaries.
    
    Returns:
        A list of tools formatted for the LLM API.
    """
    api_tools = []
    for tool_info in tools_list:
        description = tool_info.get("description", "")
        
        toolset_name = tool_info.get("toolset_name", tool_info["name"])
        description_with_toolset = f"{description} (Belongs to toolset: '{toolset_name}')"
            
        parameters = tool_info.get("parameters", {})
        if not isinstance(parameters, dict):
             logger.warning("tool_invalid_parameters", extra={"description": "Tool has non-dict parameters; using empty object", "tool_name": tool_info.get('name', 'unknown'), "parameters": str(parameters)})
             parameters = {"type": "object", "properties": {}}
        
        # Sanitize the schema to remove all custom fields starting with 'x-' before sending to the API
        sanitized_parameters = _sanitize_schema_for_api(parameters)
        
        api_tool = {
            "type": "function",
            "function": {
                "name": tool_info.get("name", ""),
                "description": description_with_toolset,
                "parameters": sanitized_parameters
            }
        }
        api_tools.append(api_tool)
    return api_tools

def format_tools_for_prompt(tools_list) -> str:
    """
    Formats a list of tools into a string for system prompts.
    
    Args:
        tools_list: A list of tool information dictionaries.
    
    Returns:
        A string describing the tools for a system prompt.
    """
    formatted_text = "### Registered Tools\n\n"
    
    for tool in tools_list:
        name = tool.get("name", "")
        description = tool.get("description", "")
        
        formatted_text += f"**{name}**: {description}\n\n"
    
    return formatted_text

def format_tools_for_prompt_by_toolset(tools_by_toolset: Dict[str, List[Dict]]) -> str:
    """
    Formats a dictionary of tools grouped by toolset into a prompt string.
    
    Args:
        tools_by_toolset: A dictionary of toolsets and their tools.
    
    Returns:
        A string describing the tools, grouped by toolset, for a system prompt.
    """
    prompt_parts = []
    title = "### Available Toolsets and Tools\n\n"
    prompt_parts.append(title)

    for toolset_name, tools_in_set in sorted(tools_by_toolset.items()):
        if not tools_in_set:
            continue

        prompt_parts.append(f"#### Toolset: {toolset_name}\n")

        for tool_info in tools_in_set:
            name = tool_info.get("name", "")
            description = tool_info.get("description", "")
            
            prompt_parts.append(f"*   **{name}**: {description}\n")
        prompt_parts.append("\n")

    return "".join(prompt_parts)

def format_simplified_tools_for_prompt_by_toolset(tools_by_toolset: Dict[str, List[Dict]]) -> str:
    """
    Formats tools grouped by toolset into a simplified prompt string (no parameters).
    
    Args:
        tools_by_toolset: A dictionary of toolsets and their tools.
    
    Returns:
        A simplified Markdown list of tools for a system prompt.
    """
    prompt_parts = []
    title = "### Associate Agent Available Tools Reference (You cannot call these directly)\n\n"
    prompt_parts.append(title)

    for toolset_name, tools_in_set in sorted(tools_by_toolset.items()):
        if not tools_in_set:
            continue

        prompt_parts.append(f"#### Toolset: {toolset_name}\n")

        for tool_info in tools_in_set:
            name = tool_info.get("name", "")
            description = tool_info.get("description", "")
            
            prompt_parts.append(f"*   **{name}**: {description}\n")
        prompt_parts.append("\n")

    return "".join(prompt_parts)

def register_native_mcp_tool(
    name: str,
    description: str,
    parameters: Dict,
    server_name: str,
    default_knowledge_item_type: Optional[str] = None,
    source_uri_field_in_output: Optional[str] = None,
    title_field_in_output: Optional[str] = None
):
    """
    Registers a tool discovered from a native MCP server.

    Args:
        name: The original name of the tool.
        description: The tool's description.
        parameters: The tool's parameters in JSON Schema format.
        server_name: The name of the MCP server hosting the tool.
        default_knowledge_item_type: Default KB item type the tool produces.
        source_uri_field_in_output: Field in output containing the source URI.
        title_field_in_output: Field in output containing the title.
    """
    unique_tool_name = f"{server_name}_{name}"

    if unique_tool_name in _TOOL_REGISTRY:
        logger.warning("mcp_tool_registration_overwrite", extra={"description": "Native MCP tool name already exists and will be overwritten", "tool_name": unique_tool_name})

    if not isinstance(parameters, dict):
        logger.error("mcp_tool_invalid_parameters", extra={"description": "Tool has invalid non-dict parameters, skipping registration", "tool_name": name, "parameters": str(parameters)})
        return

    _TOOL_REGISTRY[unique_tool_name] = {
        "name": unique_tool_name,
        "original_name": name,
        "description": description,
        "parameters": parameters,
        "ends_flow": False,
        "implementation_type": "native_mcp",
        "node_class": None,
        "mcp_server_name": server_name,
        "toolset_name": server_name,
        "default_knowledge_item_type": default_knowledge_item_type,
        "source_uri_field_in_output": source_uri_field_in_output,
        "title_field_in_output": title_field_in_output
    }
    logger.debug("mcp_tool_registered", extra={"description": "Registered native MCP tool", "unique_tool_name": unique_tool_name, "original_name": name, "server_name": server_name})

def _apply_mcp_prompt_overrides():
    """Checks for and applies prompt overrides from mcp_prompt_override.yaml."""
    if not MCP_PROMPT_OVERRIDE_FILE.exists():
        logger.debug("prompt_override_file_not_found", extra={"description": "Prompt override file not found, skipping", "file_path": str(MCP_PROMPT_OVERRIDE_FILE)})
        return

    logger.info("prompt_override_file_found", extra={"description": "Found prompt override file, applying overrides", "file_path": str(MCP_PROMPT_OVERRIDE_FILE)})
    try:
        with open(MCP_PROMPT_OVERRIDE_FILE, 'r', encoding='utf-8') as f:
            overrides = yaml.safe_load(f)

        if not isinstance(overrides, dict):
            logger.warning("prompt_override_invalid_format", extra={"description": "Prompt override file does not contain a valid dictionary. Skipping", "file_path": str(MCP_PROMPT_OVERRIDE_FILE)})
            return

        for tool_name, new_prompt in overrides.items():
            if tool_name in _TOOL_REGISTRY:
                tool_info = _TOOL_REGISTRY[tool_name]
                if tool_info.get("implementation_type") == "native_mcp":
                    tool_info["description"] = new_prompt
                    logger.info("prompt_override_applied", extra={"description": "Overrode prompt for MCP tool", "tool_name": tool_name})
                else:
                    logger.warning("prompt_override_non_mcp_tool", extra={"description": "Attempted to override prompt for non-MCP tool. Skipping", "tool_name": tool_name})
            else:
                logger.warning("prompt_override_tool_not_found", extra={"description": "Tool from override file not found in registry. Skipping", "tool_name": tool_name})
    except Exception as e:
        logger.error("prompt_override_processing_error", extra={"description": "Error processing prompt override file", "file_path": str(MCP_PROMPT_OVERRIDE_FILE), "error": str(e)}, exc_info=True)


def _cache_mcp_tools_to_yaml():
    """Caches all registered MCP tools and their final prompts to .current_mcp_tools.yaml."""
    mcp_tools_to_cache = {}
    for tool_name, tool_info in _TOOL_REGISTRY.items():
        if tool_info.get("implementation_type") == "native_mcp":
            description = tool_info.get("description", "No description available.")
            
            mcp_tools_to_cache[tool_name] = description

    if not mcp_tools_to_cache:
        logger.debug("mcp_tools_cache_empty", extra={"description": "No native MCP tools found to cache"})
        if MCP_TOOLS_CACHE_FILE.exists() or not mcp_tools_to_cache:
             try:
                with open(MCP_TOOLS_CACHE_FILE, 'w', encoding='utf-8') as f:
                    yaml.dump({}, f)
                logger.info("mcp_tools_cache_cleared", extra={"description": "No native MCP tools found. Cleared or created empty cache file", "cache_file": str(MCP_TOOLS_CACHE_FILE)})
             except Exception as e:
                logger.error("mcp_tools_cache_clear_failed", extra={"description": "Failed to clear/write empty cache file", "cache_file": str(MCP_TOOLS_CACHE_FILE), "error": str(e)}, exc_info=True)
        return

    try:
        with open(MCP_TOOLS_CACHE_FILE, 'w', encoding='utf-8') as f:
            yaml.dump(mcp_tools_to_cache, f, allow_unicode=True, sort_keys=True, indent=2)
        logger.debug("mcp_tools_cached", extra={"description": "Successfully cached MCP tools", "tool_count": len(mcp_tools_to_cache), "cache_file": str(MCP_TOOLS_CACHE_FILE)})
    except Exception as e:
        logger.error("mcp_tools_cache_write_failed", extra={"description": "Failed to write MCP tools cache", "cache_file": str(MCP_TOOLS_CACHE_FILE), "error": str(e)}, exc_info=True)

import copy
from .handover_service import HandoverService


async def initialize_registry(discovery_session_group: Optional[ClientSessionGroup], custom_nodes_path_str="agent_core/nodes/custom_nodes"):
    """
    Initializes the tool registry by discovering internal and native MCP tools.

    Args:
        discovery_session_group: A connected ClientSessionGroup for MCP discovery.
        custom_nodes_path_str: The path to the custom nodes directory.
    """
    global _TOOL_REGISTRY
    logger.info("tool_registry_init_begin", extra={"description": "Initializing tool registry"})
    _TOOL_REGISTRY.clear()

    HandoverService.load_protocols()

    # Discover and register tools from the custom_nodes directory.
    logger.debug("custom_tools_scan_begin", extra={"description": "Scanning custom tools directory", "directory": custom_nodes_path_str})
    tools_dir = Path(custom_nodes_path_str)
    if tools_dir.exists() and tools_dir.is_dir():
        try:
            for py_file in tools_dir.glob("*.py"):
                if py_file.name.startswith("_"):
                    continue

                # Use the full module path: agent_core.nodes.custom_nodes.module_name
                module_name = f"agent_core.nodes.custom_nodes.{py_file.stem}"
                try:
                    # Force reload by removing from sys.modules if already imported
                    if module_name in sys.modules:
                        logger.debug("module_force_reload", extra={"description": "Removing module from sys.modules to force reload", "module_name": module_name})
                        del sys.modules[module_name]
                    
                    importlib.import_module(module_name)
                    logger.debug("custom_tool_module_imported", extra={"description": "Successfully imported custom tool module", "module_name": module_name})
                except ImportError as e:
                    logger.error("custom_tool_module_import_error", extra={"description": "Error importing module. Check paths and dependencies", "module_name": module_name, "error": str(e)})
                except Exception as e:
                    logger.error("custom_tool_module_unknown_error", extra={"description": "Unknown error processing module", "module_name": module_name, "error": str(e)}, exc_info=True)
        except Exception as e:
            logger.error("custom_tools_scan_error", extra={"description": "Error scanning custom nodes directory", "directory": custom_nodes_path_str, "error": str(e)}, exc_info=True)
    else:
        logger.warning("custom_tools_directory_not_found", extra={"description": "Custom nodes directory does not exist or is not a directory", "directory": custom_nodes_path_str})

    logger.debug("custom_tools_found", extra={"description": "Found custom tools", "tool_count": len(_TOOL_REGISTRY)})

    for tool_name, tool_info in list(_TOOL_REGISTRY.items()):
        protocol_name = tool_info.get("handover_protocol")
        if protocol_name:
            logger.debug("tool_handover_protocol_merge", extra={"description": "Tool uses handover protocol. Merging schemas", "tool_name": tool_name, "protocol_name": protocol_name})
            protocol_schema = HandoverService.get_protocol_schema(protocol_name)
            if protocol_schema:
                # Deep copy to avoid modifying the original object
                merged_params = copy.deepcopy(tool_info["parameters"])                

                # Heuristic: Try to find a nested 'items' for array-based tools (like dispatch_submodules)
                # This allows handover parameters to be defined once in YAML and merged into the correct location.
                target_schema_for_merge = merged_params
                if 'properties' in merged_params:
                    for prop_value in merged_params['properties'].values():
                        if isinstance(prop_value, dict) and prop_value.get('type') == 'array' and 'items' in prop_value and isinstance(prop_value.get('items'), dict) and 'properties' in prop_value['items']:
                            target_schema_for_merge = prop_value['items']
                            logger.debug("handover_protocol_nested_array_found", extra={"description": "Found nested array, will merge handover params into its 'items' schema"})
                            break 

                # Merge properties from protocol into the target schema
                target_schema_for_merge.setdefault("properties", {}).update(
                    protocol_schema.get("properties", {})
                )                
                # Merge required fields from protocol into the target schema
                req_list = target_schema_for_merge.setdefault("required", [])
                req_set = set(req_list)
                req_set.update(protocol_schema.get("required", []))
                target_schema_for_merge["required"] = sorted(list(req_set))
                # Update tool information
                _TOOL_REGISTRY[tool_name]["parameters"] = merged_params
                logger.debug("handover_protocol_merged", extra={"description": "Successfully merged handover parameters into tool", "tool_name": tool_name})
            else:
                logger.error("handover_protocol_not_found", extra={"description": "Handover protocol for tool not found. Parameters not merged", "protocol_name": protocol_name, "tool_name": tool_name})

    # Discover and register native MCP tools.
    if discovery_session_group and discovery_session_group.sessions:
        logger.debug("mcp_tools_discovery_begin", extra={"description": "Discovering native MCP tools from connected sessions", "session_count": len(discovery_session_group.sessions)})

        for session in discovery_session_group.sessions:
            server_name = getattr(session, 'server_name_from_config', None)
            
            if not server_name:
                logger.warning("mcp_session_unnamed_skip", extra={"description": "Found a connected but unnamed MCP session, skipping tool discovery", "session": str(session)})
                continue

            try:
                tools_response = await session.list_tools()
                for tool in tools_response.tools:
                    register_native_mcp_tool(
                        name=tool.name,
                        description=tool.description,
                        parameters=tool.inputSchema,
                        server_name=server_name
                    )
            except Exception as e:
                logger.error("mcp_tools_discovery_error", extra={"description": "Error discovering tools from server", "server_name": server_name, "error": str(e)}, exc_info=True)
    else:
        logger.info("mcp_tools_discovery_skipped", extra={"description": "No valid MCP session group provided, skipping native MCP tool discovery"})

    _apply_mcp_prompt_overrides()

    _cache_mcp_tools_to_yaml()

    tools_count = len(_TOOL_REGISTRY)
    actual_custom_python_tool_count = sum(1 for t in _TOOL_REGISTRY.values() if t.get("implementation_type") == "internal")
    actual_native_tool_count = sum(1 for t in _TOOL_REGISTRY.values() if t.get("implementation_type") == "native_mcp")

    logger.info("tool_registry_init_complete", extra={"description": "Tool registry initialization complete", "total_tools": tools_count})
    logger.info("tool_registry_summary_custom", extra={"description": "Custom Python tools summary", "custom_tool_count": actual_custom_python_tool_count})
    logger.info("tool_registry_summary_mcp", extra={"description": "Native MCP tools summary", "mcp_tool_count": actual_native_tool_count})

    return _TOOL_REGISTRY

def get_tools_for_profile(loaded_profile: Dict, context: Dict, agent_id: str) -> List[Dict]:
    """
    Gets the list of tools available to an agent based on its loaded profile
    and the current context. Tool access is governed by the profile's
    `tool_access_policy` and any overrides from the Principal.
    """
    sub_context_state = context["state"]
    profile_id = loaded_profile.get("profile_id", "UnknownProfile")
    
    logger.debug("agent_tools_profile_evaluation", extra={"description": "Determining tools based on profile's tool_access_policy", "agent_id": agent_id, "profile_id": profile_id})
    
    is_associate_agent = "Associate" in agent_id
    
    final_tools_list = []
    processed_tool_names = set()

    tool_access_policy = loaded_profile.get("tool_access_policy", {})
    profile_allowed_toolsets = tool_access_policy.get("allowed_toolsets", [])
    profile_allowed_individual_tools = tool_access_policy.get("allowed_individual_tools", [])
    logger.debug("agent_profile_tool_policy", extra={"description": "Profile's tool access policy", "agent_id": agent_id, "profile_id": profile_id, "allowed_toolsets": profile_allowed_toolsets, "allowed_individual_tools": profile_allowed_individual_tools})

    # Check for Principal-specified toolset override for Associates from the state within the SubContext
    principal_override_toolsets_for_associate = sub_context_state.get("allowed_toolsets")

    candidate_tool_sources = [] 

    if is_associate_agent and principal_override_toolsets_for_associate is not None:
        logger.info("agent_principal_toolset_override", extra={"description": "Using Principal-specified toolsets override", "agent_id": agent_id, "profile_id": profile_id, "override_toolsets": principal_override_toolsets_for_associate})
        # If principal_override_toolsets_for_associate is an empty list [], it means NO registry tools.
        for toolset_name in principal_override_toolsets_for_associate:
            tools_in_set = get_tools_by_toolset_names([toolset_name])
            logger.debug("agent_override_toolset_tools", extra={"description": "Tools found for overridden toolset", "agent_id": agent_id, "profile_id": profile_id, "toolset_name": toolset_name, "tool_names": [t['name'] for t in tools_in_set]})
            candidate_tool_sources.append( (f"toolset '{toolset_name}' from Principal override", tools_in_set) )
    else:
        # Principal or Associate (no override from Principal)
        logger.debug("agent_profile_policy_apply", extra={"description": "Applying profile's tool_access_policy", "agent_id": agent_id, "profile_id": profile_id, "allowed_toolsets": profile_allowed_toolsets, "allowed_individual_tools": profile_allowed_individual_tools})
        if profile_allowed_toolsets:
            logger.debug("agent_profile_toolsets_processing", extra={"description": "Processing profile's allowed_toolsets", "agent_id": agent_id, "profile_id": profile_id, "allowed_toolsets": profile_allowed_toolsets})
            for toolset_name in profile_allowed_toolsets:
                tools_in_set = get_tools_by_toolset_names([toolset_name])
                logger.debug("agent_toolset_tools_found", extra={"description": "Tools found for toolset", "agent_id": agent_id, "profile_id": profile_id, "toolset_name": toolset_name, "tool_names": [t['name'] for t in tools_in_set]})
                candidate_tool_sources.append( (f"toolset '{toolset_name}' from profile", tools_in_set) )
        
        if profile_allowed_individual_tools:
            logger.debug("agent_individual_tools_processing", extra={"description": "Processing profile's allowed_individual_tools", "agent_id": agent_id, "profile_id": profile_id, "allowed_individual_tools": profile_allowed_individual_tools})
            individual_tool_infos = []
            for tool_name in profile_allowed_individual_tools:
                tool_info = get_tool_by_name(tool_name)
                if tool_info:
                    logger.debug("agent_individual_tool_found", extra={"description": "Found individual tool", "agent_id": agent_id, "profile_id": profile_id, "tool_name": tool_name, "scope": tool_info.get('scope')})
                    individual_tool_infos.append(tool_info)
                else:
                    logger.warning("agent_individual_tool_not_found", extra={"description": "Individual tool from profile not found in registry", "agent_id": agent_id, "profile_id": profile_id, "tool_name": tool_name})
            if individual_tool_infos:
                candidate_tool_sources.append( (f"individual tools from profile", individual_tool_infos) )
    
    logger.debug("agent_candidate_tool_sources", extra={"description": "Candidate tool sources", "agent_id": agent_id, "profile_id": profile_id, "sources": [(s[0], [t['name'] for t in s[1]]) for s in candidate_tool_sources]})

    # Process all candidate tools, ensuring no duplicates. Scope filtering is removed.
    for source_desc, tools_from_source in candidate_tool_sources:
        for tool_info in tools_from_source: 
            tool_name = tool_info["name"]
            if tool_name not in processed_tool_names:
                # Scope check removed. If a tool is in candidate_tool_sources, it's considered applicable based on profile.
                final_tools_list.append(tool_info)
                processed_tool_names.add(tool_name)
                logger.debug("agent_tool_added_to_final_list", extra={"description": "Added tool to final list (scope check removed)", "agent_id": agent_id, "profile_id": profile_id, "tool_name": tool_name, "source": source_desc})
            # else: logger.debug(f"Tool '{tool_name}' from {source_desc} already processed.")

    # Client-declared MCP tools are NO LONGER PROCESSED HERE as per V5 plan.
    
    logger.debug("agent_final_tools_determined", extra={"description": "Final applicable tools determined", "agent_id": agent_id, "profile_id": profile_id, "tool_count": len(final_tools_list), "tool_names": [t['name'] for t in final_tools_list]})
    return final_tools_list

def connect_tools_to_node(node, context: Optional[Dict] = None):
    """
    Connects registered tools to a specified node. Tool availability is
    determined by the Agent's Profile.

    Args:
        node: The primary node instance (typically an AgentNode).
        context: The current SubContext, used for profile loading and tool decisions.

    Returns:
        A dictionary of connected tool nodes: {tool_name: node_instance}.
    """
    tool_nodes = {}
    agent_operational_scope_for_log = "principal" if "Principal" in node.agent_id else "agent" if hasattr(node, 'agent_id') else "unknown_node_type"
    node_identifier_for_log = node.agent_id if hasattr(node, 'agent_id') else node.__class__.__name__
    logger.debug("node_tool_connection_begin", extra={"description": "Connecting tools to node. Tool availability determined by Profile", "node_identifier": node_identifier_for_log, "node_class": node.__class__.__name__, "agent_scope": agent_operational_scope_for_log})
    
    from pocketflow import Flow, AsyncFlow, Node, AsyncNode 
    from ..nodes.mcp_proxy_node import MCPProxyNode
    from ..nodes.base_agent_node import AgentNode 

    tools_to_connect_definitions: List[Dict] = []
    if isinstance(node, AgentNode):
        if not node.loaded_profile: 
            logger.error("agent_node_profile_not_set", extra={"description": "AgentNode profile not set prior to connect_tools_to_node. This indicates an issue with AgentNode initialization. No tools will be connected", "agent_id": node.agent_id})
            return {} 
        # Get the definitive list of tool definitions for this AgentNode instance based on its profile.
        # Pass the context object to get_tools_for_profile
        tools_to_connect_definitions = get_tools_for_profile(node.loaded_profile, context, node.agent_id)
    else:
        logger.warning("non_agent_node_tool_connection", extra={"description": "Node is not an AgentNode. Profile-based tool connection logic will not be applied. No tools will be connected for this node type here", "node_identifier": node_identifier_for_log, "node_class": node.__class__.__name__})
        return {} # Or handle differently if non-AgentNodes can have tools via another mechanism

    # Client-declared MCP tools are no longer handled here as per V5 plan.
    # All tools should come from the registry, filtered by profile/override via get_tools_for_profile.

    logger.debug("node_tools_connection_attempt", extra={"description": "Will attempt to connect tools based on Profile", "node_identifier": node_identifier_for_log, "tool_count": len(tools_to_connect_definitions), "tool_names": [t['name'] for t in tools_to_connect_definitions]})

    try:
        # 2. Iterate through the final list of tool definitions (determined by profile) and instantiate/connect them
        for tool_info in tools_to_connect_definitions:
            name = tool_info["name"]
            impl_type = tool_info.get("implementation_type", "internal") # Default to "internal"
            
            # For "internal" type, node_class should be in tool_info from @tool_registry decorator
            # For "internal_profile_agent", node_class is always AgentNode.
            # For "native_mcp", node_class is MCPProxyNode.
            
            node_class_from_registry = tool_info.get("node_class") # This is set for "internal" type by decorator
            ends_flow_tool = tool_info.get("ends_flow", False)
            action_name = name # PocketFlow action is the tool name
            
            logger.debug("tool_connection_attempt", extra={"description": "Attempting to connect tool", "tool_name": name, "implementation_type": impl_type, "ends_flow": ends_flow_tool})
                
            node_instance = None
            
            if impl_type == "internal":
                if node_class_from_registry:
                    try:
                        if issubclass(node_class_from_registry, AgentNode):
                             logger.error("tool_invalid_agent_node_class", extra={"description": "Tool (impl_type 'internal') has AgentNode as its class. It should be 'internal_profile_agent'. Skipping", "tool_name": name})
                             continue
                        
                        if issubclass(node_class_from_registry, (Flow, AsyncFlow)):
                            node_instance = node_class_from_registry() 
                        elif issubclass(node_class_from_registry, (Node, AsyncNode)):
                            node_instance = node_class_from_registry(max_retries=tool_info.get("max_retries",1), wait=tool_info.get("wait",1))
                        else:
                            logger.warning("tool_invalid_node_class", extra={"description": "Tool node_class is not a Node or Flow subclass. Skipping", "tool_name": name, "node_class": node_class_from_registry.__name__})
                            continue
                        tool_nodes[name] = node_instance
                        logger.debug("internal_tool_node_instantiated", extra={"description": "Instantiated 'internal' tool node", "tool_name": name, "node_class": node_class_from_registry.__name__})
                    except Exception as e:
                        logger.error("internal_tool_node_instantiation_error", extra={"description": "Error instantiating 'internal' tool node", "tool_name": name, "node_class": node_class_from_registry.__name__, "error": str(e)}, exc_info=True)
                        continue
                else: 
                    logger.warning("internal_tool_missing_node_class", extra={"description": "'internal' tool is missing node_class definition in registry. Skipping", "tool_name": name})
                    continue
            
            elif impl_type == "native_mcp":
                server_name = tool_info.get("mcp_server_name")
                original_name = tool_info.get("original_name")
                unique_name = tool_info.get("name")

                if not server_name or not original_name or not unique_name:
                    logger.warning("mcp_tool_missing_definitions", extra={"description": "Native MCP tool is missing required definitions", "tool_name": name})
                    continue
                try:
                    node_instance = MCPProxyNode(
                        unique_tool_name=unique_name,
                        original_tool_name=original_name,
                        server_name=server_name,
                        tool_info=tool_info,
                        max_retries=3,
                        wait=1
                    )
                    
                    tool_nodes[name] = node_instance
                    logger.debug("mcp_proxy_node_instantiated", extra={"description": "Instantiated MCPProxyNode for tool", "unique_name": unique_name, "original_name": original_name, "server_name": server_name})
                except Exception as e:
                    logger.error("mcp_proxy_node_instantiation_error", extra={"description": "Error instantiating MCPProxyNode", "tool_name": name, "error": str(e)}, exc_info=True)
                    continue
            else:
                logger.warning("tool_unknown_implementation_type", extra={"description": "Tool has unknown implementation type", "tool_name": name, "implementation_type": impl_type})
                continue
                
            if node_instance:
                try:
                    if not hasattr(node, 'successors'):
                        setattr(node, 'successors', {})
                    
                    node.next(node_instance, action=action_name)
                    logger.debug("tool_node_connected", extra={"description": "Connected tool node", "source_node": node.__class__.__name__, "action_name": action_name, "target_node": node_instance.__class__.__name__})
                    
                    if not ends_flow_tool:
                        if not hasattr(node_instance, 'successors'):
                            setattr(node_instance, 'successors', {})
                        node_instance.next(node)
                        logger.debug("tool_return_connection", extra={"description": "Connected tool return path", "source_node": node_instance.__class__.__name__, "target_node": node.__class__.__name__})
                    else:
                        logger.info("tool_ends_flow_no_return", extra={"description": "Tool is marked with ends_flow=True. No return connection will be made", "tool_name": name, "target_node": node.__class__.__name__})

                except Exception as e:
                    logger.error("tool_node_connection_error", extra={"description": "Error connecting nodes for tool", "tool_name": name, "error": str(e)}, exc_info=True)
                
    except Exception as e:
        logger.error("tool_connection_general_error", extra={"description": "General error during tool connection for node", "node_identifier": node_identifier_for_log, "error": str(e)}, exc_info=True)
    
    logger.debug("node_tool_connection_complete", extra={"description": "Finished connecting tools for node", "node_identifier": node_identifier_for_log, "connected_tool_count": len(tool_nodes)})
    return tool_nodes
