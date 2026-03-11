"""Database engine/session helpers."""

from __future__ import annotations

import os
from functools import lru_cache
from typing import Callable

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, Session


def _build_database_url() -> str:
    if url := os.getenv("DATABASE_URL"):
        return url
    user = os.getenv("DB_USER", "livekit")
    password = os.getenv("DB_PASSWORD", "livekit")
    host = os.getenv("DB_HOST", "127.0.0.1")
    port = os.getenv("DB_PORT", "5432")
    name = os.getenv("DB_NAME", "livekit_agents")
    sslmode = os.getenv("DB_SSLMODE", "disable")
    return f"postgresql+psycopg://{user}:{password}@{host}:{port}/{name}?sslmode={sslmode}"


@lru_cache(maxsize=1)
def get_engine(echo: bool | None = None) -> Engine:
    url = _build_database_url()
    pool_size = int(os.getenv("DB_POOL_MAX", "20") or "20")
    max_overflow = int(os.getenv("DB_POOL_OVERFLOW", "20") or "20")
    pool_timeout = float(os.getenv("DB_POOL_TIMEOUT", "30") or "30")
    connect_args = {}
    return create_engine(
        url,
        echo=bool(echo),
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_timeout=pool_timeout,
        pool_pre_ping=True,
        future=True,
        connect_args=connect_args,
    )


@lru_cache(maxsize=1)
def get_session_factory() -> Callable[[], Session]:
    engine = get_engine()
    min_pool = int(os.getenv("DB_POOL_MIN", "1") or "1")
    # SQLAlchemy doesn't expose min pool directly; we can adjust by inflating pool size
    _ = min_pool  # placeholder to document intent
    return sessionmaker(bind=engine, expire_on_commit=False, autoflush=False, future=True)

