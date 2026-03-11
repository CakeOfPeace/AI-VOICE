"""Call analytics aggregation and GPT analysis helpers."""

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
import copy
from collections import Counter
from typing import Any, Dict, List, Optional
import sys

from openai import AsyncOpenAI
from sqlalchemy import case, func, select, or_
from sqlalchemy.orm import selectinload

from db.core import get_session_factory
from db import models

SKIP_CALL_ID_PREFIXES: tuple[str, ...] = ("EV_", "EG_")
AGENT_IDENTITY_PREFIXES: tuple[str, ...] = (
    "agent-",
    "agent_",
    "agent",
    "assistant-",
    "assistant_",
    "assistant",
    "eg_",
    "pa_",
    "bot-",
    "bot_",
    "lk_agent_",
)


@dataclass
class AnalysisResult:
    note: str
    sentiment: str
    tags: List[str] = field(default_factory=list)


class CallAnalyticsService:
    def __init__(self):
        self.SessionFactory = get_session_factory()
        self.openai_model = os.getenv("CALL_ANALYSIS_MODEL", "gpt-5-nano")
        self._client = None

    @property
    def openai(self) -> AsyncOpenAI | None:
        if not self._client:
            api_key = os.getenv("OPENAI_API_KEY", "").strip()
            if not api_key:
                return None
            base_url = os.getenv("OPENAI_BASE_URL", "").strip() or None
            self._client = AsyncOpenAI(api_key=api_key, base_url=base_url)
        return self._client

    def _log(self, message: str) -> None:
        print(f"[analysis] {message}", file=sys.stderr, flush=True)

    @staticmethod
    def _should_skip_call(call_id: str | None) -> bool:
        cid = str(call_id or "")
        return any(cid.startswith(prefix) for prefix in SKIP_CALL_ID_PREFIXES)

    @staticmethod
    def _participant_window(participants: dict | None) -> tuple[bool, bool, int | None, int | None]:
        has_agent = False
        caller_start = None
        caller_end = None
        caller_seen = False
        if not isinstance(participants, dict):
            return has_agent, caller_seen, caller_start, caller_end
        for identity, pdata in participants.items():
            ident = str(identity or "").strip().lower()
            role = str((pdata or {}).get("role") or "").strip().lower()
            joined = (
                pdata.get("first_joined_at_ts")
                or pdata.get("joined_at_ts")
                or pdata.get("joinedAt")
                or pdata.get("joined_at")
                or pdata.get("joinedAtMs")
            )
            left = (
                pdata.get("left_at_ts")
                or pdata.get("left_at")
                or pdata.get("last_seen_ts")
                or pdata.get("lastSeenTs")
            )
            def _to_sec(value):
                if value is None:
                    return None
                try:
                    val = int(float(value))
                    if len(str(val)) > 11:
                        val = int(val / 1000)
                    return val
                except Exception:
                    return None
            joined_sec = _to_sec(joined)
            left_sec = _to_sec(left)
            if ident.startswith(AGENT_IDENTITY_PREFIXES) or role.startswith(("agent", "assistant")):
                has_agent = True
                continue
            caller_seen = True
            if joined_sec is not None:
                caller_start = joined_sec if caller_start is None else min(caller_start, joined_sec)
            if left_sec is not None:
                caller_end = left_sec if caller_end is None else max(caller_end, left_sec)
        return has_agent, caller_seen, caller_start, caller_end

    def _has_agent_and_caller(self, call_payload: dict | None) -> bool:
        payload = call_payload or {}
        participants = payload.get("participants") or {}
        has_agent, caller_seen, caller_start, caller_end = self._participant_window(participants)
        if has_agent and caller_seen and caller_start is not None and caller_end is not None and caller_end > caller_start:
            return True
        transcript = payload.get("transcript") or {}
        entries = transcript.get("entries") if isinstance(transcript, dict) else []
        if entries:
            roles = {str((entry or {}).get("role") or "").lower() for entry in entries}
            if any(r.startswith(("agent", "assistant")) for r in roles) and any(r.startswith(("user", "caller")) for r in roles):
                return has_agent
        return False

    @staticmethod
    def _clean_response_text(text: str) -> str:
        text = (text or "").strip()
        if text.startswith("```"):
            # Remove fenced code blocks e.g. ```json ... ```
            parts = text.split("```")
            if len(parts) >= 2:
                text = parts[1]
            if len(parts) >= 3 and not text.strip():
                text = parts[2]
        return text.strip()

    async def analyze_transcript(self, call_id: str, transcript: dict) -> AnalysisResult | None:
        if not transcript or not transcript.get("entries"):
            self._log(f"No transcript entries for {call_id}; skipping analysis")
            return None
        if not self.openai:
            self._log("OpenAI client unavailable; cannot analyze transcript")
            return None
        parts = []
        for entry in transcript.get("entries", []):
            role = entry.get("role") or "caller"
            text = entry.get("text") or ""
            parts.append(f"{role.upper()}: {text}")
        prompt = "\n".join(parts[:600])
        try:
            response = await self.openai.responses.create(
                model=self.openai_model,
                input=[
                    {
                        "role": "system",
                        "content": [
                            {
                                "type": "input_text",
                                "text": (
                                    "You are a QA analyst for phone calls. Read the transcript and return ONLY valid JSON in this form:\n"
                                    "{\n"
                                    '  "note": "Single-paragraph summary covering: overall outcome, caller name (or Not captured during this call), contact info, request, dislikes/pain points, follow-up actions, QA/policy observations. '
                                    'Do not repeat the transcript verbatim. Mention \"Not captured during this call\" for any missing field.",\n'
                                    '  "sentiment": "positive|neutral|negative",\n'
                                    '  "tags": ["lowercase keywords describing the call"]\n'
                                    "}\n"
                                    "The note must be one paragraph with sentences separated by spaces (no line breaks) and should highlight only the important information collected."
                                ),
                            },
                        ],
                    },
                    {
                        "role": "user",
                        "content": [{"type": "input_text", "text": prompt}],
                    },
                ],
            )
        except Exception as exc:
            self._log(f"OpenAI error for {call_id}: {exc}")
            return None
        text_chunks: list[str] = []
        if getattr(response, "output", None):
            for item in response.output:
                contents = getattr(item, "content", None) or []
                for content in contents:
                    txt = getattr(content, "text", None)
                    if txt:
                        text_chunks.append(txt)
        text = self._clean_response_text("\n".join(text_chunks))
        if not text:
            self._log(f"Empty analysis response for {call_id}")
            return None
        try:
            import json

            parsed = json.loads(text)
        except Exception as exc:
            self._log(f"JSON parse error for {call_id}: {exc}; raw={text[:160]}")
            return None
        tags = parsed.get("tags") or []
        if isinstance(tags, str):
            tags = [tags]
        note = parsed.get("note") or ""
        if isinstance(note, str):
            note = " ".join(note.split())
        sentiment = (parsed.get("sentiment") or "unknown").lower()
        if not note:
            self._log(f"No note in analysis for {call_id}; raw={text[:160]}")
            return None
        return AnalysisResult(
            note=note.strip(),
            sentiment=sentiment,
            tags=tags,
        )

    def _estimate_talk_seconds(self, transcript: dict) -> tuple[Optional[int], Optional[int]]:
        entries = transcript.get("entries") or []
        if not entries:
            return None, None
        def _ts(entry: dict) -> Optional[int]:
            if entry.get("timestamp_ms"):
                try:
                    return int(entry["timestamp_ms"])
                except Exception:
                    pass
            ts = entry.get("timestamp")
            if isinstance(ts, str) and ts:
                try:
                    from datetime import datetime

                    return int(datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp() * 1000)
                except Exception:
                    return None
            return None
        totals = {"user": 0.0, "assistant": 0.0}
        last_role = None
        last_ts = None
        for entry in entries:
            role = (entry.get("role") or "").lower() or "user"
            ts = _ts(entry)
            if last_role and last_ts and ts and ts >= last_ts:
                delta = min(ts - last_ts, 60_000) / 1000.0
                totals[last_role] = totals.get(last_role, 0.0) + delta
            last_role = "assistant" if role.startswith("agent") or role == "assistant" else "user"
            last_ts = ts or last_ts
        caller = int(totals.get("user", 0.0)) if totals.get("user") else None
        agent = int(totals.get("assistant", 0.0)) if totals.get("assistant") else None
        return caller, agent

    def _detect_source(self, call_payload: dict) -> str:
        routing = call_payload.get("routing") or {}
        if routing.get("call_type"):
            return str(routing["call_type"]).lower()
        room_name = str(call_payload.get("room_name") or call_payload.get("id") or "").lower()
        if room_name.startswith("play-"):
            return "web"
        if room_name.startswith("twilio-"):
            return "twilio"
        return "unknown"

    def _duration_from_transcript(self, transcript: dict) -> Optional[int]:
        entries = transcript.get("entries") or []
        stamps: list[int] = []
        for entry in entries:
            ms = entry.get("timestamp_ms")
            if ms is None:
                continue
            try:
                stamps.append(int(ms))
            except Exception:
                continue
        if not stamps:
            return None
        total = max(stamps) - min(stamps)
        if total <= 0:
            return None
        return max(1, int(total / 1000))

    def _duration_from_timeline(self, call_payload: dict) -> Optional[int]:
        timeline = call_payload.get("timeline") or []
        if not timeline:
            event_times = call_payload.get("event_times") or []
            stamps = []
            for item in event_times:
                try:
                    stamps.append(int(item.get("ts")))
                except Exception:
                    continue
            if not stamps:
                return None
            total = max(stamps) - min(stamps)
            if total <= 0:
                return None
            return max(1, total)

        joins: list[int] = []
        leaves: list[int] = []
        generic: list[int] = []
        for item in timeline:
            try:
                t = int(item.get("t"))
            except Exception:
                continue
            event = str(item.get("event") or "").lower()
            generic.append(t)
            if "participant_joined" in event:
                joins.append(t)
            elif "participant_left" in event:
                leaves.append(t)
            elif "track_unpublished" in event:
                leaves.append(t)
        if not joins and generic:
            joins = [min(generic)]
        if not leaves and generic:
            leaves = [max(generic)]
        if not joins or not leaves:
            return None
        total = max(leaves) - min(joins)
        if total <= 0:
            return None
        return max(1, total)

    def _duration_from_recordings(self, call_payload: dict) -> Optional[int]:
        recordings = call_payload.get("recordings") or []
        spans: list[int] = []
        for rec in recordings:
            try:
                start = int(rec.get("started_at") or 0)
                end = int(rec.get("updated_at") or rec.get("ended_at") or 0)
                if start and end and end > start:
                    spans.append(end - start)
            except Exception:
                continue
        if not spans:
            return None
        return max(1, max(spans))

    def _duration_from_participants(self, call_payload: dict, enforce_caller: bool = False) -> Optional[int]:
        participants = call_payload.get("participants") or {}
        if not participants:
            return None
        has_agent, caller_seen, caller_start, caller_end = self._participant_window(participants)
        if enforce_caller and (not has_agent or not caller_seen or caller_start is None or caller_end is None or caller_end <= caller_start):
            return None
        if caller_start is not None and caller_end is not None and caller_end > caller_start:
            return max(1, caller_end - caller_start)
            return None

    def _build_call_payload(self, call: models.LivekitCall) -> dict:
        payload = copy.deepcopy(call.data or {})
        if call.transcript and "transcript" not in payload:
            payload["transcript"] = {
                "entries": call.transcript.entries or [],
                "agent": call.transcript.agent,
                "started_at": call.transcript.started_at.isoformat() if call.transcript.started_at else None,
                "ended_at": call.transcript.ended_at.isoformat() if call.transcript.ended_at else None,
                "saved_at": call.transcript.saved_at.isoformat() if call.transcript.saved_at else None,
            }
        return payload

    def _apply_analysis(self, call_id: str, transcript: dict | None) -> bool:
        if not transcript or not transcript.get("entries"):
            return False
        payload = {"transcript": transcript}
        if not self._has_agent_and_caller(payload):
            self._log(f"Skipping analysis for {call_id}: missing agent/caller roles")
            return False
        try:
            result = asyncio.run(self.analyze_transcript(call_id, transcript))
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            result = loop.run_until_complete(self.analyze_transcript(call_id, transcript))
            loop.close()
            asyncio.set_event_loop(None)
        if not result:
            return False
        with self.SessionFactory() as session:
            metric = session.scalar(select(models.CallMetric).where(models.CallMetric.call_id == call_id))
            if not metric:
                return False
            metric.analysis = {
                "note": result.note,
                "sentiment": result.sentiment,
                "tags": result.tags,
            }
            metric.sentiment = result.sentiment
            metric.tags = result.tags
            session.commit()
        return True

    async def upsert_metrics(self, call_id: str, call_payload: dict) -> None:
        if not call_id or self._should_skip_call(call_id):
            return
        if not self._has_agent_and_caller(call_payload):
            self._log(f"Skipping analytics for {call_id}: participants missing agent/caller")
            return
        transcript = call_payload.get("transcript")
        # Ensure we don't skip just because transcript is missing IF we have recordings/timeline
        # BUT for AI analysis we DO need transcript.
        # This function does BOTH basic metrics AND AI analysis.
        
        duration: Optional[int] = None
        started = call_payload.get("created_at")
        ended = call_payload.get("ended_at")
        agent_runtime = call_payload.get("routing") or {}
        agent_id = agent_runtime.get("agent_id") or (call_payload.get("agent_runtime") or {}).get("agent_id")
        analysis: dict | None = None
        sentiment = None
        tags: list[str] | None = None
        caller_talk = None
        agent_talk = None
        if transcript and transcript.get("entries"):
            caller_talk, agent_talk = self._estimate_talk_seconds(transcript)
            result = await self.analyze_transcript(call_id, transcript)
            if result:
                analysis = {
                    "note": result.note,
                    "sentiment": result.sentiment,
                    "tags": result.tags,
                }
                sentiment = result.sentiment
                tags = result.tags
            if not duration or duration <= 0:
                duration = self._duration_from_transcript(transcript)
        
        # Fallbacks for duration
        if not duration or duration <= 0:
            duration = self._duration_from_participants(call_payload, enforce_caller=True)
        if not duration or duration <= 0:
            duration = self._duration_from_timeline(call_payload)
        if not duration or duration <= 0:
            duration = self._duration_from_recordings(call_payload)
        if (not duration or duration <= 0) and started and ended:
            try:
                duration = max(0, int(int(ended) - int(started)))
            except Exception:
                duration = duration
        source = self._detect_source(call_payload or {})
        
        with self.SessionFactory() as session:
            existing = session.scalar(select(models.CallMetric).where(models.CallMetric.call_id == call_id))
            if existing is None:
                existing = models.CallMetric(call_id=call_id)
                session.add(existing)
            existing.agent_id = agent_id
            existing.duration_seconds = duration or existing.duration_seconds
            existing.call_source = source or existing.call_source
            if sentiment:
                existing.sentiment = sentiment
            if tags:
                existing.tags = tags
            if analysis:
                existing.analysis = analysis
            if caller_talk:
                existing.caller_talk_seconds = caller_talk
            if agent_talk:
                existing.agent_talk_seconds = agent_talk
            session.commit()

    async def process_call(self, call_id: str, retry_for_transcript: bool = False) -> None:
        """
        Event-driven entry point to analyze a single call immediately.
        Fetches the call + transcript from DB.
        If transcript is missing and retry_for_transcript is True, waits briefly.
        Then runs metrics calculation (upsert_metrics) and batch analysis check.
        """
        if not call_id:
            return
            
        call_row = None
        # 1. Fetch Call & Transcript
        # Simple polling if requested (e.g. triggered by room_finished but transcript might lag by 1-2s)
        for _ in range(3 if retry_for_transcript else 1):
            with self.SessionFactory() as session:
                call_row = session.scalar(
                    select(models.LivekitCall)
                    .options(selectinload(models.LivekitCall.transcript))
                    .where(models.LivekitCall.call_id == call_id)
                )
            # If we have a transcript, we are good to go
            if call_row and call_row.transcript and call_row.transcript.entries:
                break
            if retry_for_transcript:
                await asyncio.sleep(1.0)
        
        if not call_row:
            self._log(f"process_call: Call {call_id} not found in DB")
            return

        # 2. Build Payload & Analyze
        payload = self._build_call_payload(call_row)
        
        # We enforce a transcript check here before "upserting metrics" if we want *full* analysis.
        # But upsert_metrics handles partial data gracefully (calculates duration etc even without transcript).
        # However, the user request says: "analysis and metrics calculation should only happen after the call has ended [and full transcript captured]"
        # So if no transcript, we might want to skip AI part or skip entirely?
        # Let's proceed, upsert_metrics will skip AI if no transcript.
        try:
            await self.upsert_metrics(call_id, payload)
        except Exception as e:
            self._log(f"process_call error upserting metrics: {e}")
            return

        # 3. Check for Batch Analysis Trigger
        # Only if we successfully generated an analysis (check DB or the payload we just used)
        # We can check the agent_id from the payload or the row
        agent_id = call_row.agent_id or (payload.get("routing") or {}).get("agent_id")
        if agent_id:
            try:
                await self.check_and_run_batch_analysis(str(agent_id))
            except Exception as e:
                self._log(f"process_call batch analysis error: {e}")

    async def check_and_run_batch_analysis(self, agent_id: str, batch_size: int = 5) -> None:
        """
        Check if the agent has enough new analyzed calls to trigger a batch summary.
        Triggers every 'batch_size' calls (e.g. 5, 10, 15...).
        """
        if not agent_id or not self.openai:
            return

        with self.SessionFactory() as session:
            # Count total analyzed calls for this agent
            count_stmt = select(func.count(models.CallMetric.id)).where(
                models.CallMetric.agent_id == agent_id,
                func.coalesce(models.CallMetric.analysis["note"].astext, "") != ""
            )
            total_analyzed = session.scalar(count_stmt) or 0
            
            if total_analyzed == 0 or total_analyzed % batch_size != 0:
                return

            # It's time! Fetch the last N calls
            calls_stmt = (
                select(models.CallMetric)
                .where(
                    models.CallMetric.agent_id == agent_id,
                    func.coalesce(models.CallMetric.analysis["note"].astext, "") != ""
                )
                .order_by(models.CallMetric.created_at.desc())
                .limit(batch_size)
            )
            recent_metrics = session.scalars(calls_stmt).all()
            # Sort chronological for the prompt
            recent_metrics = sorted(recent_metrics, key=lambda m: m.created_at or datetime.min)
            
            if not recent_metrics:
                return

            # Check if we already created a batch analysis for this exact set? 
            # Or just blindly create one for "the 5th, 10th, 15th call".
            # The count check `total % 5 == 0` is robust enough for now assuming monotonic growth.
            
            # Prepare Data for LLM
            summaries = []
            for m in recent_metrics:
                note = (m.analysis or {}).get("note", "No summary")
                date = m.created_at.strftime("%Y-%m-%d %H:%M") if m.created_at else "?"
                summaries.append(f"- [{date}] Call {m.call_id}: {note}")
            
            joined_summaries = "\n".join(summaries)
            
            # Retrieve agent context/name if possible (for tailored analysis)
            # We can't easily query the JSON file backend from here, but we might have it in the DB if we migrated agents?
            # models.LivekitCall doesn't store full agent config.
            # We'll rely on the agent_id or generic business prompt. 
            # Improvement: The user mentioned "Restaurant agent", "Business specific".
            # We can try to infer from the summaries or just ask for general business insights.
            
            system_prompt = (
                "You are a business intelligence analyst. "
                "Analyze the following batch of recent customer call summaries for a specific agent/business. "
                "Identify trends, recurring pain points, successful outcomes, and specific actionable business insights.\n"
                "Output your analysis as a JSON object with this structure:\n"
                "{\n"
                '  "summary": "Brief overview of this batch of calls.",\n'
                '  "trends": ["Bullet point 1", "Bullet point 2"],\n'
                '  "insights": ["Bullet point 1", "Bullet point 2"],\n'
                '  "recommendations": ["Action item 1", "Action item 2"]\n'
                "}"
            )
            
            user_prompt = f"Agent ID: {agent_id}\n\nRecent Call Summaries:\n{joined_summaries}"

            try:
                response = await self.openai.responses.create(
                    model=self.openai_model,
                    input=[
                        {"role": "system", "content": [{"type": "input_text", "text": system_prompt}]},
                        {"role": "user", "content": [{"type": "input_text", "text": user_prompt}]},
                    ],
                )
                
                text_chunks = []
                if getattr(response, "output", None):
                    for item in response.output:
                        contents = getattr(item, "content", None) or []
                        for content in contents:
                            txt = getattr(content, "text", None)
                            if txt:
                                text_chunks.append(txt)
                
                raw_text = self._clean_response_text("\n".join(text_chunks))
                import json
                analysis_json = json.loads(raw_text)
                
                # Save to DB
                batch_entry = models.AgentBatchAnalysis(
                    agent_id=agent_id,
                    start_call_id=recent_metrics[0].call_id,  # First in chronological
                    end_call_id=recent_metrics[-1].call_id,   # Last in chronological
                    call_count=len(recent_metrics),
                    analysis=analysis_json,
                    created_at=datetime.now(timezone.utc)
                )
                session.add(batch_entry)
                session.commit()
                self._log(f"Batch analysis completed for agent {agent_id} (count={total_analyzed})")

            except Exception as e:
                self._log(f"Error generating batch analysis: {e}")

    def aggregate_agent_metrics(
        self,
        agent_names: dict[str, str] | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[dict]:
        with self.SessionFactory() as session:
            stmt = select(
                models.CallMetric.agent_id,
                func.count(models.CallMetric.id).label("total_calls"),
                func.coalesce(func.sum(models.CallMetric.duration_seconds), 0).label("total_duration"),
                func.sum(case((models.CallMetric.call_source == "twilio", 1), else_=0)).label("twilio_calls"),
                func.sum(case((models.CallMetric.call_source == "sip", 1), else_=0)).label("sip_calls"),
                func.sum(case((models.CallMetric.call_source == "web", 1), else_=0)).label("web_calls"),
                func.sum(
                    case(
                        (
                            func.coalesce(models.CallMetric.analysis["note"].astext, "") != "",
                            1,
                        ),
                        else_=0,
                    )
                ).label("analyzed_calls"),
            ).group_by(models.CallMetric.agent_id)
            if start:
                stmt = stmt.where(models.CallMetric.created_at >= start)
            if end:
                stmt = stmt.where(models.CallMetric.created_at < end)
            rows = session.execute(stmt).all()

            sentiment_stmt = (
                select(
                    models.CallMetric.agent_id,
                    func.lower(models.CallMetric.sentiment).label("sent"),
                    func.count(models.CallMetric.id),
                )
                .where(models.CallMetric.agent_id.isnot(None))
                .group_by(models.CallMetric.agent_id, func.lower(models.CallMetric.sentiment))
            )
            if start:
                sentiment_stmt = sentiment_stmt.where(models.CallMetric.created_at >= start)
            if end:
                sentiment_stmt = sentiment_stmt.where(models.CallMetric.created_at < end)
            sentiment_rows = session.execute(sentiment_stmt).all()

        sentiment_map: dict[str | None, dict[str, int]] = {}
        for agent_id, sent, count in sentiment_rows:
            if not sent:
                sent = "unknown"
            bucket = sentiment_map.setdefault(agent_id, {})
            bucket[sent] = bucket.get(sent, 0) + int(count or 0)

        metrics = []
        for agent_id, total_calls, total_duration, twilio_calls, sip_calls, web_calls, analyzed_calls in rows:
            name = None
            if agent_names and agent_id:
                name = agent_names.get(agent_id)
            unknown_calls = int(total_calls or 0) - int((twilio_calls or 0) + (sip_calls or 0) + (web_calls or 0))
            if unknown_calls < 0:
                unknown_calls = 0
            metrics.append(
                {
                    "agent_id": agent_id,
                    "agent_name": name or agent_id or "Unassigned",
                    "total_calls": int(total_calls or 0),
                    "total_duration_seconds": int(total_duration or 0),
                    "analyzed_calls": int(analyzed_calls or 0),
                    "calls_by_source": {
                        "twilio": int(twilio_calls or 0),
                        "local_number": int(sip_calls or 0),
                        "web_call": int(web_calls or 0),
                        "unknown": unknown_calls,
                    },
                    "sentiment_breakdown": sentiment_map.get(agent_id, {}),
                }
            )
        return metrics

    def aggregate_summary(self, start: datetime | None = None, end: datetime | None = None) -> dict:
        with self.SessionFactory() as session:
            stmt_calls = select(func.count(models.CallMetric.id))
            stmt_duration = select(func.coalesce(func.sum(models.CallMetric.duration_seconds), 0))
            if start:
                stmt_calls = stmt_calls.where(models.CallMetric.created_at >= start)
                stmt_duration = stmt_duration.where(models.CallMetric.created_at >= start)
            if end:
                stmt_calls = stmt_calls.where(models.CallMetric.created_at < end)
                stmt_duration = stmt_duration.where(models.CallMetric.created_at < end)
            total_calls = session.scalar(stmt_calls) or 0
            total_duration = session.scalar(stmt_duration) or 0
            avg = int(total_duration / total_calls) if total_calls else 0
        return {
            "total_calls": int(total_calls),
            "average_duration_seconds": avg,
            "total_duration_seconds": int(total_duration),
        }

    def count_active_agents(self, start: datetime | None = None, end: datetime | None = None) -> int:
        with self.SessionFactory() as session:
            stmt = select(func.count(func.distinct(models.CallMetric.agent_id))).where(models.CallMetric.agent_id.isnot(None))
            if start:
                stmt = stmt.where(models.CallMetric.created_at >= start)
            if end:
                stmt = stmt.where(models.CallMetric.created_at < end)
            return int(session.scalar(stmt) or 0)

    def top_agents(
        self,
        limit: int = 3,
        start: datetime | None = None,
        end: datetime | None = None,
        agent_names: dict[str, str] | None = None,
    ) -> list[dict]:
        metrics = self.aggregate_agent_metrics(agent_names=agent_names, start=start, end=end)
        metrics.sort(key=lambda item: (item.get("total_calls") or 0, item.get("total_duration_seconds") or 0), reverse=True)
        return metrics[:limit]

    def latest_analysis(
        self,
        limit: int = 1,
        start: datetime | None = None,
        end: datetime | None = None,
        agent_names: dict[str, str] | None = None,
    ) -> list[dict]:
        with self.SessionFactory() as session:
            stmt = (
                select(models.CallMetric, models.LivekitCall)
                .join(models.LivekitCall, models.LivekitCall.call_id == models.CallMetric.call_id)
                .where(func.coalesce(models.CallMetric.analysis["note"].astext, "") != "")
                .order_by(models.CallMetric.created_at.desc())
                .limit(limit)
            )
            if start:
                stmt = stmt.where(models.CallMetric.created_at >= start)
            if end:
                stmt = stmt.where(models.CallMetric.created_at < end)
            rows = session.execute(stmt).all()
        results = []
        for metric, call in rows:
            analysis = metric.analysis or {}
            agent_name = None
            if agent_names and metric.agent_id:
                agent_name = agent_names.get(metric.agent_id)
            if not agent_name:
                agent_name = (call.data or {}).get("routing", {}).get("agent_name") if call and call.data else None
            results.append(
                {
                    "call_id": metric.call_id,
                    "agent_id": metric.agent_id,
                    "agent_name": agent_name or metric.agent_id,
                    "created_at": metric.created_at.isoformat() if metric.created_at else None,
                    "analysis": analysis,
                    "duration_seconds": metric.duration_seconds,
                    "call_source": metric.call_source,
                }
            )
        return results

    def _build_filters(self, agent_id: str | None, start: datetime | None, end: datetime | None):
        filters = []
        if agent_id:
            filters.append(models.CallMetric.agent_id == agent_id)
        if start:
            filters.append(models.CallMetric.created_at >= start)
        if end:
            filters.append(models.CallMetric.created_at < end)
        return filters

    def fetch_reports(
        self,
        agent_id: str | None,
        start: datetime | None,
        end: datetime | None,
        page: int,
        per_page: int,
        agent_names: dict[str, str] | None = None,
        require_analysis: bool = False,
    ) -> dict:
        filters = self._build_filters(agent_id, start, end)
        offset = max(0, (page - 1) * per_page)
        with self.SessionFactory() as session:
            total_stmt = select(func.count(models.CallMetric.id))
            for flt in filters:
                total_stmt = total_stmt.where(flt)
            if require_analysis:
                total_stmt = total_stmt.where(func.coalesce(models.CallMetric.analysis["note"].astext, "") != "")
            total = session.scalar(total_stmt) or 0

            stmt = (
                select(models.CallMetric, models.LivekitCall)
                .join(
                    models.LivekitCall,
                    models.LivekitCall.call_id == models.CallMetric.call_id,
                    isouter=True,
                )
                .order_by(models.CallMetric.created_at.desc())
                .limit(per_page)
                .offset(offset)
            )
            for flt in filters:
                stmt = stmt.where(flt)
            if require_analysis:
                stmt = stmt.where(func.coalesce(models.CallMetric.analysis["note"].astext, "") != "")
            rows = session.execute(stmt).all()

        reports = [self._row_to_report(metric, call, agent_names) for metric, call in rows]
        return {
            "reports": reports,
            "pagination": {
                "total": int(total),
                "page": page,
                "per_page": per_page,
                "has_more": offset + per_page < total,
            },
        }

    def fetch_report(self, call_id: str, agent_names: dict[str, str] | None = None) -> dict | None:
        with self.SessionFactory() as session:
            row = (
                session.execute(
                    select(models.CallMetric, models.LivekitCall)
                    .join(
                        models.LivekitCall,
                        models.LivekitCall.call_id == models.CallMetric.call_id,
                        isouter=True,
                    )
                    .where(models.CallMetric.call_id == call_id)
                )
            .first()
            )
        if not row:
            return None
        metric, call = row
        return self._row_to_report(metric, call, agent_names)

    def daily_summary(
        self,
        agent_id: str | None,
        start: datetime | None,
        end: datetime | None,
    ) -> List[dict]:
        filters = self._build_filters(agent_id, start, end)
        with self.SessionFactory() as session:
            stmt = select(models.CallMetric)
            for flt in filters:
                stmt = stmt.where(flt)
            metrics = session.execute(stmt).scalars().all()

        buckets: dict[str, dict] = {}
        for metric in metrics:
            if not metric.created_at:
                continue
            day_key = metric.created_at.astimezone(timezone.utc).date().isoformat()
            bucket = buckets.setdefault(
                day_key,
                {
                    "date": day_key,
                    "total_calls": 0,
                    "total_duration_seconds": 0,
                    "sentiment_breakdown": Counter(),
                    "tag_counter": Counter(),
                    "notes": [],
                },
            )
            bucket["total_calls"] += 1
            bucket["total_duration_seconds"] += int(metric.duration_seconds or 0)
            bucket["sentiment_breakdown"][(metric.sentiment or "unknown").lower()] += 1
            analysis = metric.analysis or {}
            tags = analysis.get("tags") or metric.tags or []
            if isinstance(tags, str):
                tags = [tags]
            bucket["tag_counter"].update(tags)
            note = analysis.get("note")
            if isinstance(note, str) and note:
                bucket["notes"].append(note.strip())

        summaries: list[dict] = []
        for day, data in buckets.items():
            total_calls = data["total_calls"] or 1
            avg_duration = int(data["total_duration_seconds"] / total_calls)
            top_tags = [
                {"tag": tag, "count": count}
                for tag, count in data["tag_counter"].most_common(5)
            ]
            summaries.append(
                {
                    "date": day,
                    "total_calls": data["total_calls"],
                    "total_duration_seconds": data["total_duration_seconds"],
                    "avg_duration_seconds": avg_duration,
                    "sentiment_breakdown": dict(data["sentiment_breakdown"]),
                    "top_tags": top_tags,
                    "notes_sample": data["notes"][:3],
                }
            )
        summaries.sort(key=lambda item: item["date"])
        return summaries

    def _row_to_report(
        self,
        metric: models.CallMetric,
        call: models.LivekitCall | None,
        agent_names: dict[str, str] | None = None,
    ) -> dict:
        analysis = metric.analysis or {}
        call_data = call.data if call and call.data else {}
        room_name = None
        if call:
            room_name = call.room_name or call_data.get("room_name") or call_data.get("room", {}).get("name")
        routing = call_data.get("routing") or {}
        agent_name = routing.get("agent_name")
        if not agent_name and agent_names and metric.agent_id:
            agent_name = agent_names.get(metric.agent_id)
        if not agent_name and call_data.get("agent_runtime"):
            agent_name = (
                call_data.get("agent_runtime") or {}
            ).get("agent_name")
        tags = analysis.get("tags") or metric.tags or []
        if isinstance(tags, str):
            tags = [tags]
        sentiment = analysis.get("sentiment") or metric.sentiment
        analysis_payload = {
            "note": analysis.get("note"),
            "sentiment": sentiment,
            "tags": tags,
        }
        return {
            "call_id": metric.call_id,
            "agent_id": metric.agent_id,
            "agent_name": agent_name or metric.agent_id,
            "room_name": room_name,
            "created_at": metric.created_at.isoformat() if metric.created_at else None,
            "duration_seconds": metric.duration_seconds,
            "caller_talk_seconds": metric.caller_talk_seconds,
            "agent_talk_seconds": metric.agent_talk_seconds,
            "call_source": metric.call_source,
            "sentiment": sentiment,
            "tags": tags,
            "analysis": analysis_payload,
            "last_event": call_data.get("last_event"),
        }

    def process_missing_calls(self, max_batch: int = 25) -> int:
        processed = 0
        with self.SessionFactory() as session:
            stmt = (
                select(models.LivekitCall)
                .options(selectinload(models.LivekitCall.transcript))
                .outerjoin(models.CallMetric, models.CallMetric.call_id == models.LivekitCall.call_id)
                .where(models.CallMetric.call_id.is_(None))
                .order_by(models.LivekitCall.created_at.asc())
                .limit(max_batch)
            )
            for prefix in SKIP_CALL_ID_PREFIXES:
                stmt = stmt.where(~models.LivekitCall.call_id.startswith(prefix))
            rows = session.execute(stmt).scalars().all()
        for row in rows:
            payload = self._build_call_payload(row)
            try:
                asyncio.run(self.upsert_metrics(row.call_id, payload))
                processed += 1
            except Exception as exc:
                print(f"[analytics] process_missing_calls failed for {row.call_id}: {exc}")
        if rows and processed == 0:
            self._log(
                f"process_missing_calls saw {len(rows)} candidate rows but wrote none; check transcripts/payloads."
            )
        return processed

    def process_missing_analysis(self, max_batch: int = 25) -> int:
        processed = 0
        with self.SessionFactory() as session:
            stmt = (
                select(models.LivekitCall)
                .options(selectinload(models.LivekitCall.transcript))
                .join(models.CallMetric, models.CallMetric.call_id == models.LivekitCall.call_id)
                .where(
                    or_(
                        models.CallMetric.analysis.is_(None),
                        func.coalesce(models.CallMetric.analysis["note"].astext, "") == "",
                    )
                )
                .order_by(models.CallMetric.created_at.asc())
                .limit(max_batch)
            )
            rows = session.execute(stmt).scalars().all()
        for row in rows:
            payload = self._build_call_payload(row)
            try:
                if self._apply_analysis(row.call_id, payload.get("transcript")):
                    processed += 1
            except Exception as exc:
                print(f"[analysis] process_missing_analysis failed for {row.call_id}: {exc}")
        return processed

    def calls_over_time(self) -> list[dict]:
        with self.SessionFactory() as session:
            rows = session.execute(
                select(
                    func.date_trunc("day", models.LivekitCall.created_at).label("day"),
                    func.count(models.LivekitCall.call_id),
                )
                .where(models.LivekitCall.created_at.isnot(None))
                .group_by("day")
                .order_by("day")
            ).all()
            points = []
            for day, count in rows:
                try:
                    date_str = day.date().isoformat()
                except Exception:
                    date_str = str(day)
                points.append({"date": date_str, "count": int(count or 0)})
            return points

    def get_call_analysis(self, call_id: str) -> dict | None:
        with self.SessionFactory() as session:
            metric = session.scalar(select(models.CallMetric).where(models.CallMetric.call_id == call_id))
            if not metric:
                return None
        analysis = metric.analysis or {}
        tags = analysis.get("tags") or metric.tags or []
        if isinstance(tags, str):
            tags = [tags]
        sentiment = analysis.get("sentiment") or metric.sentiment
        payload = {
            "note": analysis.get("note"),
            "sentiment": sentiment,
            "tags": tags,
        }
        return {
            "call_id": call_id,
            "note": payload["note"],
            "sentiment": payload["sentiment"],
            "tags": payload["tags"],
            "duration_seconds": metric.duration_seconds,
            "caller_talk_seconds": metric.caller_talk_seconds,
            "agent_talk_seconds": metric.agent_talk_seconds,
            "analysis": payload,
        }
