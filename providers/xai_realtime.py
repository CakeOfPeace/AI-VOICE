"""
Custom xAI Realtime implementation with function call support.

The standard LiveKit xAI plugin doesn't handle xAI's function call events because
xAI uses a different event type than OpenAI:
- OpenAI: response.output_item.done (with item_type == "function_call")
- xAI: response.function_call_arguments.done

This module extends the xAI plugin to properly handle function calls by:
1. Listening to the openai_server_event_received event
2. When response.function_call_arguments.done is received, creating an llm.FunctionCall
3. Sending it through the existing function_ch channel for LiveKit to process

References:
- xAI Function Calling: https://docs.x.ai/docs/guides/function-calling
- LiveKit Realtime API: https://docs.livekit.io/agents/openai/
"""

import logging
from typing import Any

from livekit.agents import llm
from livekit.plugins import xai

logger = logging.getLogger("livekit.xai_realtime")


class RealtimeModel(xai.realtime.RealtimeModel):
    """
    Extended xAI RealtimeModel that creates sessions with function call support.
    """
    
    def session(self) -> "RealtimeSession":
        """Create a new realtime session with function call handling."""
        sess = RealtimeSession(self)
        self._sessions.add(sess)
        return sess


class RealtimeSession(xai.realtime.RealtimeSession):
    """
    Extended xAI RealtimeSession that handles xAI's function call events.
    
    xAI sends function calls via 'response.function_call_arguments.done' events
    instead of OpenAI's 'response.output_item.done' with type 'function_call'.
    
    This session listens for xAI's function call events and routes them to
    LiveKit's standard function call processing pipeline.
    """
    
    def __init__(self, realtime_model: RealtimeModel) -> None:
        super().__init__(realtime_model)
        # Register handler for xAI-specific events
        self.on("openai_server_event_received", self._handle_xai_event)
        logger.info("[xai_realtime] Session initialized with function call support")
    
    def _handle_xai_event(self, event: dict[str, Any]) -> None:
        """
        Handle xAI-specific events that aren't part of the OpenAI Realtime API.
        
        Specifically handles:
        - response.function_call_arguments.done: When xAI wants to call a function
        """
        event_type = event.get("type", "")
        
        if event_type == "response.function_call_arguments.done":
            self._handle_xai_function_call(event)
    
    def _handle_xai_function_call(self, event: dict[str, Any]) -> None:
        """
        Handle xAI's function call event.
        
        xAI event format:
        {
            "type": "response.function_call_arguments.done",
            "name": "function_name",
            "call_id": "call_xxx",
            "arguments": "{\"arg1\": \"value1\"}"
        }
        
        This converts it to LiveKit's FunctionCall format and sends it through
        the function_ch channel for processing by the agent framework.
        """
        function_name = event.get("name")
        call_id = event.get("call_id")
        arguments = event.get("arguments", "{}")
        
        if not function_name or not call_id:
            logger.warning(
                f"[xai_realtime] Invalid function call event: missing name or call_id",
                extra={"event": event}
            )
            return
        
        logger.info(
            f"[xai_realtime] Function call received: {function_name}",
            extra={"call_id": call_id, "arguments": arguments}
        )
        
        # Check if we have an active generation to send the function call to
        if self._current_generation is None:
            logger.warning(
                f"[xai_realtime] Function call received but no active generation: {function_name}"
            )
            return
        
        # Send the function call through LiveKit's standard channel
        try:
            self._current_generation.function_ch.send_nowait(
                llm.FunctionCall(
                    call_id=call_id,
                    name=function_name,
                    arguments=arguments,
                )
            )
            logger.info(f"[xai_realtime] Function call queued for execution: {function_name}")
        except Exception as e:
            logger.error(
                f"[xai_realtime] Failed to queue function call: {e}",
                extra={"function_name": function_name, "call_id": call_id}
            )


# Re-export xAI tools for convenience
WebSearch = xai.realtime.WebSearch
XSearch = xai.realtime.XSearch
FileSearch = xai.realtime.FileSearch
GrokVoices = xai.realtime.GrokVoices

__all__ = [
    "RealtimeModel",
    "RealtimeSession",
    "WebSearch",
    "XSearch",
    "FileSearch",
    "GrokVoices",
]
