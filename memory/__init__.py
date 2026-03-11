"""
Agent memory module powered by Mem0.

Provides persistent, cross-call memory for each agent+caller pair.
Mem0 extracts key facts from conversations and stores them in a vector DB.
On subsequent calls, relevant memories are retrieved and injected into agent context.
"""

import logging
import asyncio
from typing import Any

logger = logging.getLogger("memory")

_mem0_available = False
try:
    from mem0 import Memory
    _mem0_available = True
except ImportError:
    logger.warning("[memory] mem0ai not installed -- agent memory is disabled. Install with: pip install mem0ai")
    Memory = None


class AgentMemoryManager:
    """Manages persistent memory for agent-caller pairs via Mem0."""

    def __init__(self, config: dict = None):
        if not _mem0_available:
            self._mem = None
            logger.warning("[memory] Mem0 not available, memory operations will be no-ops.")
            return

        try:
            if config:
                self._mem = Memory.from_config(config)
            else:
                self._mem = Memory()
            logger.info("[memory] Mem0 memory manager initialized")
        except Exception as exc:
            logger.error(f"[memory] Failed to initialize Mem0: {exc}")
            self._mem = None

    @property
    def available(self) -> bool:
        return self._mem is not None

    def load_memories(self, agent_id: str, caller_id: str, query: str = None) -> list[dict]:
        """Retrieve relevant memories for this agent+caller pair.

        Args:
            agent_id: The agent identifier
            caller_id: Caller identifier (phone number for SIP, participant identity for web)
            query: Optional semantic search query to find relevant memories

        Returns:
            List of memory dicts with 'memory', 'id', 'score' keys
        """
        if not self._mem:
            return []

        try:
            if query:
                results = self._mem.search(
                    query,
                    agent_id=agent_id,
                    user_id=caller_id,
                    limit=10,
                )
                return results.get("results", results) if isinstance(results, dict) else results
            else:
                results = self._mem.get_all(
                    agent_id=agent_id,
                    user_id=caller_id,
                )
                return results.get("results", results) if isinstance(results, dict) else results
        except Exception as exc:
            logger.error(f"[memory] Failed to load memories for agent={agent_id}, caller={caller_id}: {exc}")
            return []

    def save_memories(self, agent_id: str, caller_id: str, messages: list[dict]) -> None:
        """Extract and store memories from a conversation transcript.

        Mem0 internally uses an LLM to extract key facts from the messages,
        handles deduplication, and stores embeddings in the vector DB.

        Args:
            agent_id: The agent identifier
            caller_id: Caller identifier
            messages: List of message dicts [{"role": "user/assistant", "content": "..."}]
        """
        if not self._mem:
            return
        if not messages:
            return

        try:
            self._mem.add(
                messages,
                agent_id=agent_id,
                user_id=caller_id,
            )
            logger.info(f"[memory] Saved memories for agent={agent_id}, caller={caller_id} ({len(messages)} messages)")
        except Exception as exc:
            logger.error(f"[memory] Failed to save memories: {exc}")

    def get_memory_context(self, agent_id: str, caller_id: str) -> str:
        """Build a context string from memories to inject into agent instructions.

        Returns an empty string if no memories exist or Mem0 is unavailable.
        """
        memories = self.load_memories(agent_id, caller_id)
        if not memories:
            return ""

        lines = []
        for m in memories:
            text = m.get("memory", "") if isinstance(m, dict) else str(m)
            if text:
                lines.append(f"- {text}")

        if not lines:
            return ""

        context = (
            "You have the following information from previous interactions with this caller:\n"
            + "\n".join(lines)
            + "\n\nUse this context naturally in the conversation. "
            "Do not explicitly tell the caller you are reading from memory."
        )
        logger.info(f"[memory] Injecting {len(lines)} memories for agent={agent_id}, caller={caller_id}")
        return context

    def delete_memories(self, agent_id: str, caller_id: str) -> None:
        """Delete all memories for an agent+caller pair."""
        if not self._mem:
            return
        try:
            self._mem.delete_all(agent_id=agent_id, user_id=caller_id)
            logger.info(f"[memory] Deleted all memories for agent={agent_id}, caller={caller_id}")
        except Exception as exc:
            logger.error(f"[memory] Failed to delete memories: {exc}")


_global_memory_manager: AgentMemoryManager | None = None


def get_memory_manager(config: dict = None) -> AgentMemoryManager:
    """Get or create the global memory manager singleton."""
    global _global_memory_manager
    if _global_memory_manager is None:
        _global_memory_manager = AgentMemoryManager(config)
    return _global_memory_manager


__all__ = ["AgentMemoryManager", "get_memory_manager"]
