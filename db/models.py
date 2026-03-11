"""SQLAlchemy models used by the storage abstraction."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    """Declarative base class."""


class KVStore(Base):
    """Generic key-value store for configuration JSON payloads."""

    __tablename__ = "kv_store"
    namespace: Mapped[str] = mapped_column(String(64), primary_key=True)
    key: Mapped[str] = mapped_column(String(128), primary_key=True)
    data: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )


class LivekitCall(Base):
    """Per-call store replacing livekit_store.json entries."""

    __tablename__ = "livekit_calls"
    call_id: Mapped[str] = mapped_column(String(160), primary_key=True)
    room_name: Mapped[str | None] = mapped_column(String(160), index=True)
    agent_id: Mapped[str | None] = mapped_column(String(64), index=True)
    data: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    created_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_received_at: Mapped[int | None] = mapped_column(BigInteger)
    transcript_id: Mapped[int | None] = mapped_column(ForeignKey("call_transcripts.id", ondelete="SET NULL"))

    transcript: Mapped["CallTranscript"] = relationship(
        "CallTranscript", back_populates="call", uselist=False, lazy="joined"
    )


class CallTranscript(Base):
    """Transcript entries captured per call."""

    __tablename__ = "call_transcripts"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    call_id: Mapped[str] = mapped_column(String(160), unique=True, nullable=False)
    entries: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)
    agent: Mapped[dict | None] = mapped_column(JSONB)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    saved_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    call: Mapped[LivekitCall] = relationship("LivekitCall", back_populates="transcript", lazy="selectin")


class CallMetric(Base):
    """Derived metrics and OpenAI analysis results for each call."""

    __tablename__ = "call_metrics"
    __table_args__ = (UniqueConstraint("call_id", name="uq_call_metrics_call"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    call_id: Mapped[str] = mapped_column(String(160), nullable=False)
    agent_id: Mapped[str | None] = mapped_column(String(64), index=True)
    duration_seconds: Mapped[int | None] = mapped_column(Integer)
    call_source: Mapped[str | None] = mapped_column(String(32))
    caller_talk_seconds: Mapped[int | None] = mapped_column(Integer)
    agent_talk_seconds: Mapped[int | None] = mapped_column(Integer)
    sentiment: Mapped[str | None] = mapped_column(String(32))
    tags: Mapped[list | None] = mapped_column(JSONB)
    analysis: Mapped[dict | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class AgentBatchAnalysis(Base):
    """Aggregated analysis for every N calls of an agent."""

    __tablename__ = "agent_batch_analysis"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    agent_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    start_call_id: Mapped[str] = mapped_column(String(160))
    end_call_id: Mapped[str] = mapped_column(String(160))
    call_count: Mapped[int] = mapped_column(Integer, nullable=False)
    analysis: Mapped[dict] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class TelephonyEvent(Base):
    """Structured log of telephony webhooks (formerly telephony_events.json)."""

    __tablename__ = "telephony_events"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class ScheduledCall(Base):
    """Outbound calls scheduled to be triggered at a future time."""

    __tablename__ = "scheduled_calls"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    agent_id: Mapped[str] = mapped_column(String(64), nullable=False)
    to_number: Mapped[str] = mapped_column(String(32), nullable=False)
    from_number: Mapped[str] = mapped_column(String(32), nullable=False)
    scheduled_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    status: Mapped[str] = mapped_column(String(32), default="pending")  # pending, initiated, completed, failed
    context: Mapped[dict | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    call_sid: Mapped[str | None] = mapped_column(String(64))
    error_message: Mapped[str | None] = mapped_column(String(512))


class CallbackResult(Base):
    """Stores callback payloads sent to external systems (questionnaire results, recording notifications).
    
    This provides a permanent record of what was sent, useful for:
    - Debugging when external systems claim they didn't receive data
    - Retrying failed callbacks
    - Auditing and data recovery
    """

    __tablename__ = "callback_results"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    call_id: Mapped[str] = mapped_column(String(160), nullable=False, index=True)
    interview_id: Mapped[str | None] = mapped_column(String(128), index=True)  # External system's ID
    callback_type: Mapped[str] = mapped_column(String(32), nullable=False)  # 'questionnaire' or 'recording_ready'
    callback_url: Mapped[str | None] = mapped_column(String(512))
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)  # Full callback payload
    http_status: Mapped[int | None] = mapped_column(Integer)  # Response status code
    http_response: Mapped[str | None] = mapped_column(String(2048))  # Response body (truncated)
    sent_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    success: Mapped[bool] = mapped_column(default=False)  # Whether callback was acknowledged
