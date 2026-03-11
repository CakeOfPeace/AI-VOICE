"""Storage backend factory."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

from .base import StorageBackend
from .json_backend import JsonStorageBackend

_BACKEND: Optional[StorageBackend] = None


def get_storage_backend(base_path: Path) -> StorageBackend:
    global _BACKEND
    if _BACKEND:
        return _BACKEND
    backend_name = os.getenv("STORAGE_BACKEND", "json").strip().lower()
    if backend_name == "postgres":
        from .postgres_backend import PostgresStorageBackend

        _BACKEND = PostgresStorageBackend()
    else:
        _BACKEND = JsonStorageBackend(base_path=base_path)
    return _BACKEND

