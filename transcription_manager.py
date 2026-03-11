import asyncio
import logging
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Optional

logger = logging.getLogger(__name__)


@dataclass
class TranscriptSegment:
    role: str  # "user" or "assistant"
    text: str
    timestamp_ms: int
    confidence: Optional[float] = None


class TranscriptionManager:
    """Lightweight manager that receives role-labelled text chunks during a call."""

    def __init__(self, call_id: str) -> None:
        self.call_id = call_id
        self._segments: list[TranscriptSegment] = []
        self._listeners: list[Callable[[TranscriptSegment], Awaitable[None]]] = []
        self._closed = False
        self._lock = asyncio.Lock()

    async def emit(self, segment: TranscriptSegment) -> None:
        if self._closed:
            logger.warning("transcription manager already closed for %s", self.call_id)
            return
        async with self._lock:
            self._segments.append(segment)
        for listener in list(self._listeners):
            try:
                await listener(segment)
            except Exception as exc:
                logger.warning("transcription listener error: %s", exc)

    def register_listener(self, fn: Callable[[TranscriptSegment], Awaitable[None]]) -> None:
        self._listeners.append(fn)

    async def close(self) -> list[TranscriptSegment]:
        self._closed = True
        async with self._lock:
            snapshot = list(self._segments)
        logger.info("Transcription manager closed for %s with %d segments", self.call_id, len(snapshot))
        return snapshot


