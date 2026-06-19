# app/manager_portal.py
"""
Портал менеджера: вход по логину/паролю, сессия в cookie, получение
«текущего звонка» и карточки клиента из Bitrix, действия (стадия/коммент/задача).

Не зависит от HTTP Basic дашборда — у менеджера своя сессия (cookie token).
Пароли хранятся как salted SHA-256 (без внешних зависимостей).
"""
import hashlib
import logging
import secrets
from datetime import datetime, timedelta
from typing import Optional

from sqlalchemy import select

from .config import settings
from .db import async_session_maker
from .models import CallSession, Manager, ManagerSession

logger = logging.getLogger(__name__)

# Сколько живёт сессия входа без активности.
SESSION_TTL_HOURS = 24

# Активный звонок «свежий», если CallSession started_at не старше этого.
ACTIVE_CALL_WINDOW_SECONDS = 120


# ── Пароли ───────────────────────────────────────────────────
def hash_password(password: str, salt: Optional[str] = None) -> str:
    """salted SHA-256 → 'salt$hash'. Без внешних библиотек."""
    if salt is None:
        salt = secrets.token_hex(8)
    h = hashlib.sha256((salt + password).encode("utf-8")).hexdigest()
    return f"{salt}${h}"


def verify_password(password: str, stored: Optional[str]) -> bool:
    if not stored or "$" not in stored:
        return False
    salt, _ = stored.split("$", 1)
    return secrets.compare_digest(hash_password(password, salt), stored)


# ── Аутентификация / сессии ──────────────────────────────────
async def authenticate(login: str, password: str) -> Optional[Manager]:
    """Проверить логин+пароль. Вернуть менеджера или None."""
    if not login or not password:
        return None
    async with async_session_maker() as session:
        result = await session.execute(
            select(Manager).where(Manager.login == login.strip())
        )
        mgr = result.scalar_one_or_none()
        if mgr and verify_password(password, mgr.password_hash):
            return mgr
    return None


async def create_session(manager_id: int) -> str:
    """Создать сессию входа, вернуть токен для cookie."""
    token = secrets.token_urlsafe(32)
    async with async_session_maker() as session:
        session.add(ManagerSession(
            token=token, manager_id=manager_id,
            created_at=datetime.utcnow(), last_seen=datetime.utcnow(),
        ))
        await session.commit()
    return token


async def get_session_manager(token: Optional[str]) -> Optional[Manager]:
    """По токену cookie вернуть менеджера (если сессия жива)."""
    if not token:
        return None
    async with async_session_maker() as session:
        sess = await session.get(ManagerSession, token)
        if not sess:
            return None
        # TTL
        if datetime.utcnow() - sess.last_seen > timedelta(hours=SESSION_TTL_HOURS):
            await session.delete(sess)
            await session.commit()
            return None
        sess.last_seen = datetime.utcnow()
        mgr = await session.get(Manager, sess.manager_id)
        await session.commit()
        return mgr


async def destroy_session(token: Optional[str]) -> None:
    if not token:
        return
    async with async_session_maker() as session:
        sess = await session.get(ManagerSession, token)
        if sess:
            await session.delete(sess)
            await session.commit()


# ── Текущий звонок менеджера ─────────────────────────────────
async def get_current_call(manager_id: int) -> Optional[dict]:
    """Вернуть активный звонок для менеджера (для всплытия карточки).

    Берём самую свежую CallSession этого менеджера в состоянии
    CALLBACK_CREATED / CONNECTED за последние ACTIVE_CALL_WINDOW_SECONDS.
    """
    cutoff = datetime.utcnow() - timedelta(seconds=ACTIVE_CALL_WINDOW_SECONDS)
    async with async_session_maker() as session:
        result = await session.execute(
            select(CallSession)
            .where(CallSession.manager_id == manager_id)
            .where(CallSession.state.in_(["CALLBACK_CREATED", "CONNECTED"]))
            .where(CallSession.started_at >= cutoff)
            .order_by(CallSession.started_at.desc())
        )
        s = result.scalars().first()
        if not s:
            return None
        return {
            "lead_id": s.lead_id,
            "phone": s.phone,
            "state": s.state,
            "started_at": s.started_at.isoformat() + "Z" if s.started_at else None,
            "connected": s.state == "CONNECTED",
        }
