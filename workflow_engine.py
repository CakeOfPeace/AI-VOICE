"""
Workflow Engine.

Reads a workflow definition (JSON with nodes + edges) and generates
the corresponding agent tools (integration tools + handoff tools).

The workflow definition is created by the frontend Visual Flow Builder
and stored per-agent in the storage backend.

Workflow JSON structure:
{
    "version": 1,
    "nodes": [
        {"id": "agent-main", "type": "agent", "position": {...}, "data": {"agent_id": "...", "label": "..."}},
        {"id": "int-calcom", "type": "integration", "position": {...}, "data": {"integration_id": "cal_com", ...}},
        {"id": "agent-billing", "type": "agent_handoff", "position": {...}, "data": {"target_agent_id": "...", ...}},
    ],
    "edges": [
        {"id": "e1", "source": "agent-main", "target": "int-calcom", "label": "Booking requests"},
    ]
}
"""

import os
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger("workflow_engine")


def load_workflow_tools(agent_id: str, agent_profile: dict, build_agent_fn=None) -> list:
    """Load workflow definition and generate all tools for the agent.

    This is the main entry point called during agent initialization.
    It reads the agent's workflow from storage, then:
    1. For integration nodes: loads credentials and generates function_tools
    2. For agent_handoff nodes: generates transfer tools via the handoff module

    Args:
        agent_id: The agent's unique ID
        agent_profile: The agent's full profile dict (for context)
        build_agent_fn: Callable(profile, chat_ctx) -> Agent (for handoff tools)

    Returns:
        List of function_tool instances ready to attach to the agent session
    """
    tools = []

    try:
        from storage import get_storage_backend
        storage = get_storage_backend(Path(os.getenv("AGENTS_DIR", Path(__file__).parent / "agents")))
    except Exception as exc:
        logger.error(f"[workflow] Failed to load storage backend: {exc}")
        return tools

    workflow = storage.load_agent_workflow(agent_id)
    if not workflow or not workflow.get("nodes"):
        logger.debug(f"[workflow] No workflow defined for agent {agent_id}")
        return tools

    logger.info(f"[workflow] Loading workflow for agent {agent_id}: {len(workflow.get('nodes', []))} nodes, {len(workflow.get('edges', []))} edges")

    integration_tools = _build_integration_tools_from_workflow(agent_id, workflow, storage)
    tools.extend(integration_tools)

    if build_agent_fn:
        handoff_tools = _build_handoff_tools_from_workflow(agent_id, workflow, build_agent_fn)
        tools.extend(handoff_tools)

    logger.info(f"[workflow] Generated {len(tools)} tools from workflow ({len(integration_tools)} integration, {len(tools) - len(integration_tools)} handoff)")
    return tools


def _build_integration_tools_from_workflow(agent_id: str, workflow: dict, storage) -> list:
    """Generate integration function_tools from workflow integration nodes."""
    tools = []

    try:
        from integrations import get_integration
    except ImportError:
        logger.warning("[workflow] integrations module not available")
        return tools

    agent_integrations = storage.load_agent_integrations(agent_id)
    creds_by_id = {}
    for entry in agent_integrations:
        iid = entry.get("integration_id", "")
        creds_by_id[iid] = entry.get("credentials", {})

    for node in workflow.get("nodes", []):
        if node.get("type") != "integration":
            continue

        data = node.get("data", {})
        iid = data.get("integration_id", "")
        integration = get_integration(iid)
        if not integration:
            logger.warning(f"[workflow] Unknown integration '{iid}' in workflow node '{node.get('id')}'")
            continue

        creds = creds_by_id.get(iid, {})
        if not creds:
            logger.warning(f"[workflow] No credentials configured for integration '{iid}' -- tools will likely fail")

        action_filter = set()
        if data.get("actions"):
            action_filter = set(data["actions"]) if isinstance(data["actions"], list) else set()

        integration_tools = integration.to_function_tools(creds)

        if action_filter:
            integration_tools = [
                t for t in integration_tools
                if any(a in getattr(t, '__name__', '') for a in action_filter)
            ]

        tools.extend(integration_tools)
        logger.info(f"[workflow] Loaded {len(integration_tools)} tool(s) from integration '{iid}'")

    return tools


def _build_handoff_tools_from_workflow(agent_id: str, workflow: dict, build_agent_fn) -> list:
    """Generate handoff tools from workflow agent_handoff nodes."""
    try:
        from handoff import build_handoff_tools_from_workflow
        return build_handoff_tools_from_workflow(workflow, agent_id, build_agent_fn)
    except ImportError:
        logger.warning("[workflow] handoff module not available")
        return []
    except Exception as exc:
        logger.error(f"[workflow] Failed to build handoff tools: {exc}")
        return []


def get_workflow_summary(agent_id: str) -> dict:
    """Get a summary of the agent's workflow for debugging/display.

    Returns:
        Dict with integration_count, handoff_count, node_count, edge_count
    """
    try:
        from storage import get_storage_backend
        storage = get_storage_backend(Path(os.getenv("AGENTS_DIR", Path(__file__).parent / "agents")))
        workflow = storage.load_agent_workflow(agent_id)
    except Exception:
        return {"integration_count": 0, "handoff_count": 0, "node_count": 0, "edge_count": 0}

    if not workflow:
        return {"integration_count": 0, "handoff_count": 0, "node_count": 0, "edge_count": 0}

    nodes = workflow.get("nodes", [])
    edges = workflow.get("edges", [])
    integrations = [n for n in nodes if n.get("type") == "integration"]
    handoffs = [n for n in nodes if n.get("type") == "agent_handoff"]

    return {
        "node_count": len(nodes),
        "edge_count": len(edges),
        "integration_count": len(integrations),
        "handoff_count": len(handoffs),
        "integration_ids": [n.get("data", {}).get("integration_id") for n in integrations],
        "handoff_agents": [n.get("data", {}).get("label") for n in handoffs],
    }
