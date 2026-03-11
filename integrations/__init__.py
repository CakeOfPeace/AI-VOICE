"""
Integration registry.

Auto-discovers all integration modules in this package and exposes:
- INTEGRATION_REGISTRY: dict of id -> IntegrationBase instance
- get_all_integrations(): catalog metadata for frontend
- build_integration_tools(): generate function_tools from enabled integrations
"""

import importlib
import logging
import pkgutil
from pathlib import Path
from typing import Any

from .base import IntegrationBase

logger = logging.getLogger("integrations")

INTEGRATION_REGISTRY: dict[str, IntegrationBase] = {}


def register(cls):
    """Class decorator that registers an integration in the global registry."""
    instance = cls()
    INTEGRATION_REGISTRY[instance.id] = instance
    logger.info(f"[integrations] Registered: {instance.id} ({instance.name})")
    return cls


def _auto_discover():
    """Import every module in this package so @register decorators fire."""
    package_dir = Path(__file__).resolve().parent
    for info in pkgutil.iter_modules([str(package_dir)]):
        if info.name in ("base", "__init__"):
            continue
        try:
            importlib.import_module(f".{info.name}", package=__name__)
        except Exception as exc:
            logger.warning(f"[integrations] Failed to load module '{info.name}': {exc}")


def get_all_integrations() -> list[dict]:
    """Return catalog metadata for every registered integration."""
    return [integration.to_catalog_entry() for integration in INTEGRATION_REGISTRY.values()]


def get_integration(integration_id: str) -> IntegrationBase | None:
    """Look up a single integration by id."""
    return INTEGRATION_REGISTRY.get(integration_id)


def build_integration_tools(enabled_integrations: list[dict]) -> list:
    """Build LiveKit function_tools from a list of user-enabled integrations.

    Each item in *enabled_integrations* looks like:
    {
        "integration_id": "cal_com",
        "credentials": {"api_key": "..."},
        "actions": ["create_booking", "check_availability"],  # optional filter
    }
    """
    tools = []
    for entry in enabled_integrations:
        iid = entry.get("integration_id")
        integration = INTEGRATION_REGISTRY.get(iid)
        if not integration:
            logger.warning(f"[integrations] Unknown integration '{iid}', skipping")
            continue

        creds = entry.get("credentials", {})
        action_filter = set(entry.get("actions", []))

        all_tools = integration.to_function_tools(creds)
        if action_filter:
            all_tools = [t for t in all_tools if any(a in t.__name__ for a in action_filter)]

        tools.extend(all_tools)
        logger.info(f"[integrations] Loaded {len(all_tools)} tool(s) from '{iid}'")

    return tools


# Run auto-discovery on import
_auto_discover()

__all__ = [
    "INTEGRATION_REGISTRY",
    "register",
    "get_all_integrations",
    "get_integration",
    "build_integration_tools",
    "IntegrationBase",
]
