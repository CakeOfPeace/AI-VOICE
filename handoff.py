"""
Agent handoff support.

Provides tools for transferring calls between AI agents (multi-agent handoff)
and to human operators (warm transfer via SIP).

LiveKit natively supports agent handoffs: when a function_tool returns an Agent
instance, the session automatically transfers control to that agent.
"""

import os
import json
import logging
from typing import Any

from livekit.agents import Agent, function_tool, RunContext, get_job_context

logger = logging.getLogger("handoff")


def create_agent_handoff_tool(
    target_agent_id: str,
    target_agent_name: str,
    handoff_condition: str,
    build_agent_fn,
):
    """Create a function_tool that hands off the call to another agent.

    The LLM decides when to call this tool based on the condition description.
    When called, it loads the target agent profile, builds a new Agent instance
    with the conversation history, and returns it to trigger LiveKit's native handoff.

    Args:
        target_agent_id: ID of the target agent in the DB
        target_agent_name: Display name (used in tool description)
        handoff_condition: Description of when this handoff should happen
        build_agent_fn: Callable(profile, chat_ctx) -> Agent that builds the target agent
    """

    async def _transfer(**kwargs):
        reason = kwargs.get("reason", handoff_condition)
        logger.info(f"[handoff] Transferring call to agent '{target_agent_name}' (id={target_agent_id}). Reason: {reason}")

        try:
            from storage import get_storage_backend
            from pathlib import Path
            storage = get_storage_backend(Path(os.getenv("AGENTS_DIR", Path(__file__).parent / "agents")))
            agents_data = storage.load_agents()
            target_profile = None
            for a in agents_data.get("agents", []):
                if a.get("id") == target_agent_id:
                    target_profile = a
                    break

            if not target_profile:
                logger.error(f"[handoff] Target agent '{target_agent_id}' not found")
                return json.dumps({"error": f"Agent '{target_agent_name}' not found."})

            job_ctx = get_job_context()
            chat_ctx = None
            if hasattr(job_ctx, 'agent') and hasattr(job_ctx.agent, 'session'):
                chat_ctx = getattr(job_ctx.agent.session, 'chat_ctx', None)

            target_agent = build_agent_fn(target_profile, chat_ctx)
            return target_agent, f"Transferring to {target_agent_name}: {reason}"

        except Exception as exc:
            logger.error(f"[handoff] Failed to create target agent: {exc}")
            return json.dumps({"error": f"Transfer failed: {str(exc)}"})

    tool_name = f"transfer_to_{target_agent_name.lower().replace(' ', '_').replace('-', '_')}"
    _transfer.__name__ = tool_name
    tool_desc = (
        f"Transfer the caller to {target_agent_name}. "
        f"Use this tool when: {handoff_condition}. "
        f"Before transferring, briefly inform the caller that you are connecting them to {target_agent_name}."
    )

    schema = {
        "name": tool_name,
        "description": tool_desc,
        "parameters": {
            "type": "object",
            "properties": {
                "reason": {
                    "type": "string",
                    "description": "Brief reason for the transfer.",
                },
            },
            "required": ["reason"],
        },
    }

    return function_tool(raw_schema=schema)(_transfer)


def create_warm_transfer_tool(outbound_trunk_id: str = None):
    """Create a function_tool for warm-transferring to a human operator via SIP.

    Uses LiveKit's WarmTransferTask (beta) to:
    1. Place the caller on hold
    2. Dial the supervisor/operator
    3. Provide them with a call summary
    4. Connect them to the caller
    """

    async def _transfer_to_human(**kwargs):
        phone_number = kwargs.get("phone_number", "")
        reason = kwargs.get("reason", "Caller requested to speak with a human.")

        if not phone_number:
            return json.dumps({"error": "Phone number is required for warm transfer."})

        trunk_id = outbound_trunk_id or os.getenv("SIP_OUTBOUND_TRUNK_ID") or os.getenv("LIVEKIT_SIP_TRUNK_ID")
        if not trunk_id:
            return json.dumps({"error": "No outbound SIP trunk configured for transfers."})

        logger.info(f"[handoff] Initiating warm transfer to {phone_number}. Reason: {reason}")

        try:
            from livekit.agents.beta.workflows import WarmTransferTask

            job_ctx = get_job_context()
            chat_ctx = None
            if hasattr(job_ctx, 'agent') and hasattr(job_ctx.agent, 'session'):
                chat_ctx = getattr(job_ctx.agent.session, 'chat_ctx', None)

            result = await WarmTransferTask(
                target_phone_number=phone_number,
                sip_trunk_id=trunk_id,
                chat_ctx=chat_ctx,
            )
            return json.dumps({"success": True, "message": f"Transferred to {phone_number}."})

        except ImportError:
            logger.error("[handoff] WarmTransferTask not available (requires livekit-agents beta)")
            return json.dumps({"error": "Warm transfer is not available in the current SDK version."})
        except Exception as exc:
            logger.error(f"[handoff] Warm transfer failed: {exc}")
            return json.dumps({"error": f"Transfer failed: {str(exc)}"})

    _transfer_to_human.__name__ = "transfer_to_human_operator"

    schema = {
        "name": "transfer_to_human_operator",
        "description": (
            "Transfer the caller to a human operator via phone. "
            "Use this when the caller explicitly asks to speak to a person, or when "
            "the issue requires human judgment that you cannot handle. "
            "Before transferring, inform the caller that you are connecting them."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "phone_number": {
                    "type": "string",
                    "description": "The operator's phone number in E.164 format, e.g. '+15551234567'.",
                },
                "reason": {
                    "type": "string",
                    "description": "Brief reason for the transfer.",
                },
            },
            "required": ["phone_number", "reason"],
        },
    }

    return function_tool(raw_schema=schema)(_transfer_to_human)


def build_handoff_tools_from_workflow(workflow: dict, current_agent_id: str, build_agent_fn) -> list:
    """Generate handoff tools from a workflow definition.

    Reads the workflow's nodes and edges to find agent_handoff nodes
    connected to the current agent, and creates a transfer tool for each.

    Args:
        workflow: The workflow JSON dict with 'nodes' and 'edges'
        current_agent_id: The currently active agent's ID
        build_agent_fn: Callable(profile, chat_ctx) -> Agent

    Returns:
        List of function_tool instances for handoffs
    """
    tools = []
    if not workflow:
        return tools

    nodes = {n["id"]: n for n in workflow.get("nodes", [])}
    edges = workflow.get("edges", [])

    handoff_node_ids = set()
    for edge in edges:
        src = edge.get("source", "")
        tgt = edge.get("target", "")
        src_node = nodes.get(src, {})
        tgt_node = nodes.get(tgt, {})

        if src_node.get("data", {}).get("agent_id") == current_agent_id:
            if tgt_node.get("type") == "agent_handoff" and tgt not in handoff_node_ids:
                handoff_node_ids.add(tgt)
                data = tgt_node.get("data", {})
                tool = create_agent_handoff_tool(
                    target_agent_id=data.get("target_agent_id", ""),
                    target_agent_name=data.get("label", "Agent"),
                    handoff_condition=data.get("handoff_condition", edge.get("label", "Transfer when appropriate")),
                    build_agent_fn=build_agent_fn,
                )
                tools.append(tool)

    include_warm = any(
        n.get("type") == "warm_transfer"
        for n in workflow.get("nodes", [])
    )
    if include_warm:
        tools.append(create_warm_transfer_tool())

    if tools:
        logger.info(f"[handoff] Built {len(tools)} handoff tools from workflow for agent {current_agent_id}")

    return tools
