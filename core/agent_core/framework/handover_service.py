
import yaml
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List
import copy
import re

from ..utils.context_helpers import get_nested_value_from_context, VModelAccessor

logger = logging.getLogger(__name__)

PROTOCOLS_DIR = Path(__file__).parent.parent.parent / "agent_profiles/handover_protocols"

class HandoverService:
    _protocols: Dict[str, Dict] = {}

    @classmethod
    def load_protocols(cls):
        if cls._protocols:
            return
        logger.info("handover_protocols_loading_started")
        for yaml_file in PROTOCOLS_DIR.glob("*.yaml"):
            try:
                with open(yaml_file, 'r', encoding='utf-8') as f:
                    protocol_data = yaml.safe_load(f)
                    protocol_name = protocol_data.get("protocol_name")
                    if protocol_name:
                        cls._protocols[protocol_name] = protocol_data
                        logger.info("handover_protocol_loaded", extra={"protocol_name": protocol_name})
            except Exception as e:
                logger.error("handover_protocol_load_failed", extra={"filename": yaml_file.name, "error": str(e)})

    @classmethod
    def get_protocol_schema(cls, protocol_name: str) -> Optional[Dict]:
        return cls._protocols.get(protocol_name, {}).get("context_parameters")

    @classmethod
    def _extract_from_tool_params(cls, schema: Dict, tool_params: Dict) -> Dict:
        extracted = {}
        if schema.get("type") == "object" and "properties" in schema:
            for prop_name in schema["properties"]:
                if prop_name in tool_params:
                    extracted[prop_name] = tool_params[prop_name]
        return extracted

    @classmethod
    def _resolve_path(cls, path_template: str, replacements: dict, source_context: dict) -> Optional[str]:
        """Resolves a path template with placeholders."""
        resolved_path = path_template
        for placeholder, value_path in replacements.items():
            resolved_value = get_nested_value_from_context(source_context, value_path)
            if resolved_value is not None:
                resolved_path = resolved_path.replace(f"{{{{ {placeholder} }}}}", str(resolved_value))
            else:
                # If any placeholder cannot be resolved, the path is invalid
                return None
        return resolved_path if "{{" not in resolved_path else None

    @classmethod
    async def execute(cls, protocol_name: str, source_context: dict) -> dict:
        protocol = cls._protocols.get(protocol_name)
        if not protocol:
            raise ValueError(f"Handover protocol '{protocol_name}' not found.")

        final_payload = {}
        schema_for_rendering = {"type": "object", "properties": {}}

        # Step 1: Handle direct parameters from tool call
        context_params_schema = protocol.get("context_parameters", {})
        tool_params = source_context.get("state", {}).get("current_action", {}).get("parameters", source_context.get("state", {}).get("current_action", {}))
        
        direct_params_data = cls._extract_from_tool_params(context_params_schema, tool_params)
        final_payload.update(direct_params_data)
        if "properties" in context_params_schema:
            schema_for_rendering["properties"].update(copy.deepcopy(context_params_schema["properties"]))
        
        # Step 2: Process inheritance rules
        inheritance_rules = protocol.get("inheritance", [])
        if isinstance(inheritance_rules, list):
            v_accessor = VModelAccessor(source_context)
            
            for rule in inheritance_rules:
                condition = rule.get("condition", "True")
                try:
                    if not eval(condition, {"v": v_accessor, "len": len, "get_nested_value_from_context": get_nested_value_from_context, "context_obj": source_context}):
                        continue
                except Exception as e:
                    logger.warning("inheritance_condition_evaluation_failed", extra={"condition": condition, "error": str(e)})
                    continue

                source_config = rule.get("from_source", {})
                payload_key = rule.get("as_payload_key")
                if not source_config or not payload_key:
                    continue

                inherited_data = None
                
                # Handle iterative inheritance
                iterate_on_map = source_config.get("iterate_on")
                if "path_to_iterate" in source_config and isinstance(iterate_on_map, dict):
                    path_template = source_config["path_to_iterate"]
                    aggregated_results = []
                    
                    for placeholder, value_path in iterate_on_map.items():
                        iteration_values = get_nested_value_from_context(source_context, value_path)
                        if isinstance(iteration_values, list):
                            for value in iteration_values:
                                resolved_path = path_template.replace(f"{{{{ {placeholder} }}}}", str(value))
                                data_part = get_nested_value_from_context(source_context, resolved_path)
                                if data_part is not None:
                                    aggregated_results.extend(data_part if isinstance(data_part, list) else [data_part])
                    if aggregated_results:
                        inherited_data = aggregated_results
                
                # Handle single path inheritance
                elif "path" in source_config and "replace" in source_config:
                    resolved_path = cls._resolve_path(source_config["path"], source_config["replace"], source_context)
                    if resolved_path:
                        inherited_data = get_nested_value_from_context(source_context, resolved_path)

                # If the inherited data is a list of messages (identified by the payload_key),
                # filter out any messages that are marked with the `_no_handover` internal flag.
                if inherited_data and payload_key == "inherited_messages" and isinstance(inherited_data, list):
                    original_count = len(inherited_data)
                    inherited_data = [
                        msg for msg in inherited_data if not msg.get("_internal", {}).get("_no_handover")
                    ]
                    # Log only if messages were actually filtered out.
                    if original_count > len(inherited_data):
                        logger.debug(
                            "handover_service_filtered_messages", 
                            extra={
                                "protocol_name": protocol_name, 
                                "payload_key": payload_key, 
                                "original_count": original_count, 
                                "filtered_count": len(inherited_data)
                            }
                        )

                # Inject data and rendering schema
                if inherited_data is not None:
                    final_payload[payload_key] = inherited_data
                    minimal_schema = {"x-handover-title": rule.get("x-handover-title", payload_key)}
                    if "schema" in rule:
                        minimal_schema.update(rule["schema"])
                    schema_for_rendering["properties"][payload_key] = minimal_schema

        # Step 3: Build the final InboxItem
        target_item_config = protocol["target_inbox_item"]
        return {
            "source": target_item_config["source"],
            "payload": {
                "data": final_payload,
                "schema_for_rendering": schema_for_rendering,
            },
        }

HandoverService.load_protocols()
