# app/db.py
"""Async SQLAlchemy. Поддерживает SQLite (MVP) и PostgreSQL (production)."""
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import declarative_base

from .config import settings

_connect_args = {}
if settings.DATABASE_URL.startswith("sqlite"):
    _connect_args = {"check_same_thread": False}

engine = create_async_engine(
    settings.DATABASE_URL,
    echo=False,
    future=True,
    connect_args=_connect_args,
    pool_pre_ping=True,
)

async_session_maker = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
)

Base = declarative_base()


async def init_db() -> None:
    from . import models  # noqa: F401
    from sqlalchemy import text
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        # Лёгкая миграция: добавить busy_until, если колонки ещё нет.
        # PostgreSQL поддерживает IF NOT EXISTS. Для SQLite оборачиваем в try.
        if settings.DATABASE_URL.startswith("postgresql"):
            await conn.execute(text(
                "ALTER TABLE managers ADD COLUMN IF NOT EXISTS busy_until TIMESTAMP"
            ))
        else:
            try:
                await conn.execute(text(
                    "ALTER TABLE managers ADD COLUMN busy_until TIMESTAMP"
                ))
            except Exception:
                pass  # колонка уже есть