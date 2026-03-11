"""
Tools module for LiveKit Voice Agent.

Provides:
- EndCallTool: Custom tool for properly ending calls (deletes room, triggers webhooks)
- create_http_callback_tool: Factory for creating HTTP callback tools from schemas
- get_provider_tools: Returns provider-specific tools (xAI, Gemini)
- build_agent_tools: Build all tools from agent profile config
"""

from livekit.agents import function_tool, get_job_context
from livekit.agents.llm import Tool, Toolset
from livekit.agents.voice.events import RunContext
from livekit import api as lk_api
import logging
import json
import os
import asyncio
import requests

logger = logging.getLogger("livekit.tools")


class EndCallTool(Toolset):
    """
    Custom EndCallTool that properly ends the call for all participants.
    
    Unlike LiveKit's built-in EndCallTool which only calls session.shutdown(),
    this tool also deletes the room which:
    1. Disconnects all participants (including the user)
    2. Triggers room_finished webhook for analytics
    3. Triggers egress_ended webhook for recording finalization
    
    This ensures the call properly ends from the user's perspective.
    """
    
    @function_tool(name="end_call")
    async def _end_call(self, ctx: RunContext) -> None:
        """
        Ends the current call and disconnects all participants.

        IMPORTANT: Before calling this tool, you MUST say a brief, friendly goodbye message
        to the user that acknowledges the conversation. For example:
        - "Goodbye! It was great chatting with you. Take care!"
        - "Thanks for calling! Have a wonderful day."
        - "Alright, goodbye! Feel free to reach out anytime."
        
        Only call this tool AFTER you have said your goodbye message.

        Call when the user signals they want to end the conversation:
        - Explicit signals: "bye", "goodbye", "see you", "that's all", "I'm done"
        - Implicit signals: "thanks, I'll call back later", "I think I'm good", 
          "that's everything I needed", "I should let you go", "I gotta run"
        - Declining further help: "no, that's it", "nothing else, thanks"
        - Time-based signals: "I need to go now", "I don't have more time"

        Do NOT call when:
        - The user asks to pause, hold, or transfer
        - Intent is unclear or ambiguous
        - The user is still asking questions or seems engaged
        - The user says "wait" or "hold on" or similar

        This is the final action. Once called, the call ends immediately.
        """
        logger.info("[EndCallTool] end_call tool invoked")
        
        # Wait for any pending speech to complete before ending
        # This gives the agent time to say goodbye before the call cuts
        logger.info("[EndCallTool] Waiting 5 seconds for goodbye message to complete...")
        await asyncio.sleep(5)
        logger.info("[EndCallTool] Delay complete, proceeding with call termination")
        
        # Get room info before shutdown - try multiple methods
        room_name = None
        
        # Method 1: Try get_job_context() - most reliable way
        try:
            job_ctx = get_job_context()
            if job_ctx and job_ctx.room:
                room_name = job_ctx.room.name
                logger.info(f"[EndCallTool] Got room name from job context: {room_name}")
        except Exception as e:
            logger.debug(f"[EndCallTool] Could not get room from job context: {e}")
        
        # Method 2: Try ctx.session._room_io (AgentSession's room IO)
        if not room_name:
            try:
                if hasattr(ctx, 'session') and hasattr(ctx.session, '_room_io') and ctx.session._room_io:
                    room_io = ctx.session._room_io
                    if hasattr(room_io, '_room') and room_io._room:
                        room_name = room_io._room.name
                        logger.info(f"[EndCallTool] Got room name from session room_io: {room_name}")
            except Exception as e:
                logger.debug(f"[EndCallTool] Could not get room from session room_io: {e}")
        
        # Method 3: Try direct ctx.room access (standard pipeline)
        if not room_name:
            try:
                if hasattr(ctx, 'room') and ctx.room:
                    room_name = ctx.room.name
                    logger.info(f"[EndCallTool] Got room name from ctx.room: {room_name}")
            except Exception as e:
                logger.debug(f"[EndCallTool] Could not get room from ctx.room: {e}")
        
        if not room_name:
            logger.warning("[EndCallTool] Could not determine room name from any source")
        
        logger.info(f"[EndCallTool] Ending call for room: {room_name}")
        
        # First, shutdown the agent session (stops agent responding)
        try:
            ctx.session.shutdown()
            logger.info("[EndCallTool] Agent session shutdown initiated")
        except Exception as e:
            logger.error(f"[EndCallTool] Error during session shutdown: {e}")
        
        # Then delete the room to disconnect the user and trigger proper webhooks
        # This ensures:
        # 1. User is disconnected (call ends from their perspective)
        # 2. room_finished webhook is triggered (analytics)
        # 3. egress_ended webhook is triggered (recording finalization)
        if room_name:
            try:
                await self._delete_room(room_name)
            except Exception as e:
                logger.error(f"[EndCallTool] Failed to delete room: {e}")
                # Try alternative: call the cleanup API endpoint
                try:
                    await self._call_cleanup_api(room_name)
                except Exception as api_err:
                    logger.error(f"[EndCallTool] Cleanup API also failed: {api_err}")
    
    async def _delete_room(self, room_name: str) -> None:
        """Delete the LiveKit room to end the call for all participants."""
        lk_url = (os.getenv("LIVEKIT_URL") or "").strip()
        lk_api_key = (os.getenv("LIVEKIT_API_KEY") or "").strip()
        lk_api_secret = (os.getenv("LIVEKIT_API_SECRET") or "").strip()
        
        if not all([lk_url, lk_api_key, lk_api_secret]):
            logger.warning("[EndCallTool] LiveKit credentials not available for room deletion")
            return
        
        # Convert ws URL to http
        if lk_url.startswith("wss://"):
            http_url = "https://" + lk_url[len("wss://"):]
        elif lk_url.startswith("ws://"):
            http_url = "http://" + lk_url[len("ws://"):]
        else:
            http_url = lk_url
        
        client = lk_api.LiveKitAPI(url=http_url, api_key=lk_api_key, api_secret=lk_api_secret)
        try:
            delete_req = lk_api.DeleteRoomRequest(room=room_name)
            await client.room.delete_room(delete_req)
            logger.info(f"[EndCallTool] Room {room_name} deleted successfully")
        except Exception as e:
            logger.error(f"[EndCallTool] Error deleting room {room_name}: {e}")
            raise
        finally:
            await client.aclose()
    
    async def _call_cleanup_api(self, room_name: str) -> None:
        """Fallback: Call the config API to clean up the room."""
        api_base = os.getenv("CONFIG_API_BASE", os.getenv("API_BASE", "http://localhost:5057"))
        internal_key = os.getenv("INTERNAL_API_KEY", "")
        
        headers = {}
        if internal_key:
            headers["X-Internal-Key"] = internal_key
        
        try:
            # Call playground/stop endpoint to trigger proper cleanup
            response = requests.post(
                f"{api_base}/api/playground/stop",
                json={"room": room_name},
                headers=headers,
                timeout=5
            )
            if response.ok:
                logger.info(f"[EndCallTool] Cleanup API called successfully for room {room_name}")
            else:
                logger.warning(f"[EndCallTool] Cleanup API returned {response.status_code}")
        except Exception as e:
            logger.error(f"[EndCallTool] Error calling cleanup API: {e}")
            raise
    
    @property
    def tools(self) -> list[Tool]:
        return [self._end_call]

__all__ = [
    "EndCallTool",
    "create_http_callback_tool",
    "get_provider_tools",
    "get_xai_builtin_tools",
    "build_agent_tools",
]


def create_http_callback_tool(tool_schema: dict):
    """Convert OpenAI tool schema to a LiveKit function_tool with HTTP callback support.
    
    The schema format:
    {
        "name": "get_weather",
        "description": "Get weather for a location",
        "parameters": {
            "type": "object",
            "properties": {
                "location": {"type": "string", "description": "City name"}
            },
            "required": ["location"]
        },
        "implementation": {
            "type": "http_callback",
            "url": "https://api.example.com/weather",
            "method": "POST",
            "headers": {"Authorization": "Bearer token"},
            "timeout": 10
        }
    }
    """
    tool_name = tool_schema.get("name", "unknown_tool")
    tool_desc = tool_schema.get("description", "")
    params = tool_schema.get("parameters", {})
    implementation = tool_schema.get("implementation", {})
    
    # Create a callable that makes HTTP requests to external systems
    async def tool_impl(**kwargs):
        """Dynamic tool implementation - calls external HTTP endpoints."""
        logger.info(f"Tool '{tool_name}' called with arguments: {kwargs}")
        
        # If implementation details are provided, make HTTP callback
        if implementation and isinstance(implementation, dict):
            impl_type = implementation.get("type", "http_callback")
            
            if impl_type == "http_callback":
                try:
                    url = implementation.get("url")
                    method = implementation.get("method", "POST").upper()
                    headers = implementation.get("headers", {})
                    timeout = implementation.get("timeout", 10)
                    
                    if not url:
                        logger.error(f"Tool '{tool_name}': No URL provided in implementation")
                        return json.dumps({
                            "error": "Tool configuration error: missing URL",
                            "tool": tool_name
                        })
                    
                    # Prepare request payload
                    payload = {
                        "tool": tool_name,
                        "arguments": kwargs
                    }
                    
                    # Make HTTP request
                    logger.info(f"Tool '{tool_name}': Calling {method} {url}")
                    
                    if method == "GET":
                        response = requests.get(url, params=kwargs, headers=headers, timeout=timeout)
                    elif method == "POST":
                        response = requests.post(url, json=payload, headers=headers, timeout=timeout)
                    elif method == "PUT":
                        response = requests.put(url, json=payload, headers=headers, timeout=timeout)
                    elif method == "PATCH":
                        response = requests.patch(url, json=payload, headers=headers, timeout=timeout)
                    else:
                        response = requests.post(url, json=payload, headers=headers, timeout=timeout)
                    
                    # Check response
                    if response.ok:
                        logger.info(f"Tool '{tool_name}': Success (status {response.status_code})")
                        try:
                            # Try to return JSON response
                            return json.dumps(response.json())
                        except Exception:
                            # If not JSON, return text
                            return response.text
                    else:
                        logger.error(f"Tool '{tool_name}': HTTP error {response.status_code}")
                        return json.dumps({
                            "error": f"HTTP {response.status_code}",
                            "tool": tool_name,
                            "message": response.text[:200]
                        })
                        
                except requests.exceptions.Timeout:
                    logger.error(f"Tool '{tool_name}': Request timeout")
                    return json.dumps({
                        "error": "Request timeout",
                        "tool": tool_name
                    })
                except Exception as e:
                    logger.error(f"Tool '{tool_name}': Exception during HTTP call: {e}")
                    return json.dumps({
                        "error": str(e),
                        "tool": tool_name
                    })
        
        # Fallback: return mock response if no implementation provided
        result = {
            "tool": tool_name,
            "status": "called",
            "arguments": kwargs,
            "message": f"Tool '{tool_name}' was called with arguments: {kwargs} (no implementation provided)"
        }
        logger.warning(f"Tool '{tool_name}': No implementation provided, returning mock response")
        return json.dumps(result)
    
    # Set function metadata for LiveKit
    tool_impl.__name__ = tool_name
    tool_impl.__doc__ = tool_desc
    
    # Decorate with @function_tool
    return function_tool(description=tool_desc)(tool_impl)


def get_xai_builtin_tools(config: dict) -> list:
    """Get xAI built-in tool instances for direct session configuration.
    
    These tools (XSearch, WebSearch, FileSearch) are NOT hashable and cannot be
    passed through the normal VoiceAssistant.update_tools() path which uses set().
    
    Instead, they should be passed directly to the realtime session's update_tools
    method after the session is initialized.
    
    Args:
        config: Dictionary with tool enablement flags (x_search, web_search, file_search)
        
    Returns:
        List of xAI built-in tool instances (XSearch, WebSearch, FileSearch)
    """
    tools = []
    try:
        from livekit.plugins import xai
        if config.get("x_search"):
            tools.append(xai.realtime.XSearch())
            logger.info("[tools] Created xAI X Search tool for session")
        if config.get("web_search"):
            tools.append(xai.realtime.WebSearch())
            logger.info("[tools] Created xAI Web Search tool for session")
        if config.get("file_search"):
            tools.append(xai.realtime.FileSearch())
            logger.info("[tools] Created xAI File Search tool for session")
    except ImportError as e:
        logger.warning(f"[tools] xAI plugin not available: {e}")
    except AttributeError as e:
        logger.warning(f"[tools] xAI realtime tools not available: {e}")
    
    return tools


def get_provider_tools(provider: str, config: dict) -> list:
    """Return provider-specific tools based on config.
    
    Args:
        provider: The realtime provider name ('xai', 'google', 'openai')
        config: Dictionary with tool enablement flags
        
    Returns:
        List of provider-specific tool instances
        
    Note:
        xAI built-in tools (XSearch, WebSearch, FileSearch) are NOT returned here
        because they are not hashable and cause errors with VoiceAssistant.update_tools().
        Use get_xai_builtin_tools() to get them for direct session configuration.
    """
    tools = []
    
    if provider == "xai":
        # xAI built-in tools (XSearch, WebSearch, FileSearch) are NOT returned here
        # They are not hashable and VoiceAssistant.update_tools() uses set() internally
        # Use get_xai_builtin_tools() and pass directly to session.update_tools()
        if config.get("x_search"):
            logger.info("[tools] xAI X Search enabled (use get_xai_builtin_tools for session)")
        if config.get("web_search"):
            logger.info("[tools] xAI Web Search enabled (use get_xai_builtin_tools for session)")
        if config.get("file_search"):
            logger.info("[tools] xAI File Search enabled (use get_xai_builtin_tools for session)")
            
    elif provider == "google":
        try:
            from livekit.plugins import google
            if config.get("google_search"):
                tools.append(google.tools.GoogleSearch())
                logger.info("[tools] Enabled Google Search tool")
            # Note: Code Execution (ToolCodeExecution) is NOT supported by Gemini Live API
            # Only Search and Function Calling are supported for gemini-2.5-flash-native-audio models
        except ImportError as e:
            logger.warning(f"[tools] Google plugin not available: {e}")
        except AttributeError as e:
            logger.warning(f"[tools] Google tools not available: {e}")
    
    return tools


def build_agent_tools(profile: dict, realtime_provider: str = None) -> list:
    """Build tools list from agent configuration.
    
    Args:
        profile: The agent profile dictionary containing tools config
        realtime_provider: The realtime provider name if in realtime mode ('xai', 'google', 'openai')
        
    Returns:
        List of tool instances to pass to the agent session
    """
    tools = []
    tools_config = profile.get("tools", {})
    
    # Universal tools - Custom EndCallTool that properly ends the call
    # This tool deletes the room, disconnecting all participants and triggering
    # proper webhooks for egress finalization and analytics
    if tools_config.get("end_call", {}).get("enabled"):
        tools.append(EndCallTool())
        logger.info("[tools] Enabled EndCallTool")
    
    # Provider-specific tools (xAI, Gemini)
    if realtime_provider:
        provider_tools_cfg = tools_config.get("provider_tools", {})
        provider_tools = get_provider_tools(realtime_provider, provider_tools_cfg)
        tools.extend(provider_tools)
    
    # Custom HTTP callback tools from external configuration
    custom_tools = tools_config.get("custom_tools", [])
    if isinstance(custom_tools, list):
        for tool_schema in custom_tools:
            try:
                tool_fn = create_http_callback_tool(tool_schema)
                tools.append(tool_fn)
                logger.info(f"[tools] Registered custom tool: {tool_schema.get('name')}")
            except Exception as e:
                logger.error(f"[tools] Failed to create custom tool: {e}")
    
    logger.info(f"[tools] Built {len(tools)} tools for agent")
    return tools
