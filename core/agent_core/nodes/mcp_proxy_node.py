import logging
import asyncio
import anyio
from typing import Dict, Any
from pathlib import Path

# Import the new base class
from .base_tool_node import BaseToolNode

logger = logging.getLogger(__name__)

class MCPProxyNode(BaseToolNode):
    """
    [Refactored] A proxy node for transparently calling tools on a native MCP server.
    Now inherits from BaseToolNode, which encapsulates standard prep and post logic.
    """
    def __init__(self, unique_tool_name: str, original_tool_name: str, server_name: str, tool_info: Dict, **kwargs):
        # 1. [Core Fix] Set the _tool_info attribute before calling the parent class constructor
        self._tool_info = tool_info
        
        # 2. Now it's safe to call the parent's constructor, which will perform checks
        # Note: We no longer need max_retries and wait, as BaseToolNode handles them
        # But for safety, we pass them via kwargs
        super().__init__(**kwargs)
        
        # 3. Continue with this class's initialization logic
        self.unique_tool_name = unique_tool_name
        self.original_tool_name = original_tool_name
        self.server_name = server_name
        logger.debug("mcp_proxy_node_initialized", extra={"unique_tool_name": self.unique_tool_name, "server_name": self.server_name})

    # The prep_async method has been removed; its logic is now in exec_async

    async def exec_async(self, prep_res: Dict) -> Dict[str, Any]:
        """
        [Refactored] This is the core execution logic for this tool node.
        It follows the BaseToolNode contract, receiving prep_res and returning a standard result dictionary.
        """
        # 1. Get standard input from prep_res
        tool_params = prep_res.get("tool_params", {})
        shared_context = prep_res.get("shared_context", {})
        
        # 2. Get specific resources needed by this node from shared_context
        session_group = shared_context.get("runtime_objects", {}).get("mcp_session_group")
        if not session_group:
            error_msg = f"MCPProxyNode ({self.unique_tool_name}): Agent's context-specific MCP Session Group not found."
            logger.error("mcp_proxy_session_group_not_found", extra={"unique_tool_name": self.unique_tool_name})
            return {"status": "error", "error_message": error_msg}

        # 2.5 Normalize Windows paths in common parameters to avoid relative-path issues
        def _normalize_paths_in_params(params: Dict[str, Any], tool_info: Dict[str, Any]) -> None:
            if not isinstance(params, dict) or not isinstance(tool_info, dict):
                return

            properties = tool_info.get("parameters", {}).get("properties", {})
            for param_name, schema in properties.items():
                # Heuristic: Find string params that look like paths
                is_path_like = (
                    schema.get("type") == "string" and 
                    ("path" in param_name.lower() or schema.get("format") == "uri-reference")
                )

                if is_path_like:
                    value = params.get(param_name)
                    if isinstance(value, str) and value.strip():
                        p = Path(value)
                        if not p.is_absolute():
                            abs_p = (Path.cwd() / p).resolve()
                            params[param_name] = str(abs_p)
                            try:
                                logger.debug("mcp_param_path_normalized", extra={
                                    "unique_tool_name": self.unique_tool_name, 
                                    "param": param_name, 
                                    "original_path": value,
                                    "abs_path": str(abs_p)
                                })
                            except Exception:
                                pass

        _normalize_paths_in_params(tool_params, self._tool_info)

        # 3. Execute the original business logic
        logger.info("mcp_proxy_tool_call_begin", extra={"unique_tool_name": self.unique_tool_name})
        
        try:
            # Call the MCP tool with a 60-second timeout
            result = await asyncio.wait_for(
                session_group.call_tool(self.original_tool_name, tool_params),
                timeout=60.0 
            )
            
            if result is None or not hasattr(result, 'content'):
                 raise ValueError("MCP tool returned an invalid or null response.")

            logger.info("mcp_proxy_tool_call_success", extra={"unique_tool_name": self.unique_tool_name, "original_tool_name": self.original_tool_name})

            # Extract content
            content_text = ""
            if result.content and len(result.content) > 0:
                content_item = result.content[0]
                if hasattr(content_item, 'text') and content_item.text is not None:
                     content_text = content_item.text
                else:
                     content_text = str(content_item)

            # 4. Construct a success return value that conforms to the BaseToolNode contract
            return {
                "status": "success",
                "payload": {
                    "status": "success",
                    "tool_name_invoked": self.unique_tool_name,
                    "server_name": self.server_name,
                    "response_preview": content_text
                }
                # Note: _knowledge_items_to_add has been removed
            }

        except anyio.ClosedResourceError as e:
            error_msg = f"The connection to the external service '{self.server_name}' required by the tool '{self.unique_tool_name}' was unexpectedly closed. This is a non-recoverable connection error."
            logger.error("mcp_proxy_connection_closed", extra={"unique_tool_name": self.unique_tool_name}, exc_info=True)
            # Construct a failure return value that conforms to the BaseToolNode contract
            return {
                "status": "error",
                "error_message": error_msg,
                "payload": { # More detailed error information can be placed in the payload for LLM analysis
                    "type": "CRITICAL_CONNECTION_FAILURE",
                    "summary": error_msg,
                    "instruction_for_llm": (
                        f"**Action Required: You MUST NOT try to call the '{self.unique_tool_name}' tool or any other tool from the '{self.server_name}' toolset again.**\n"
                        "1. Acknowledge this critical failure in your reasoning.\n"
                        "2. Call the 'finish_flow' tool with a reason explaining that the task could not be completed due to a persistent external service connection error."
                    )
                }
            }
        except asyncio.TimeoutError:
            error_msg = f"Call to native MCP tool '{self.original_tool_name}' (server: {self.server_name}) timed out."
            logger.error("mcp_proxy_tool_timeout", extra={"unique_tool_name": self.unique_tool_name, "original_tool_name": self.original_tool_name, "server_name": self.server_name})
            return {"status": "error", "error_message": error_msg}
        except Exception as e:
            error_msg = f"Error calling native MCP tool '{self.original_tool_name}' (server: {self.server_name}): {str(e)}"
            logger.error("mcp_proxy_tool_call_error", extra={"unique_tool_name": self.unique_tool_name, "original_tool_name": self.original_tool_name, "server_name": self.server_name}, exc_info=True)
            return {"status": "error", "error_message": error_msg}

    # The post_async method has been removed because BaseToolNode handles it
