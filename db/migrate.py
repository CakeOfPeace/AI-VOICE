"""Simple migration helper to create database tables."""

from __future__ import annotations

from db.core import get_engine
from db.models import Base


def main():
    engine = get_engine()
    Base.metadata.create_all(engine)
    print("Postgres schema created/updated.")


if __name__ == "__main__":
    main()

