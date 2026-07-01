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

from sqlalchemy import select, func

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
    """Вернуть активный звонок оператора (для всплытия карточки).

    Карточка показывается, как только звонок НАЗНАЧЕН оператору
    (CALLBACK_CREATED — его телефон звонит), ещё до соединения с клиентом,
    и пока идёт разговор (CONNECTED). Берём самую свежую сессию этого
    оператора в окне ACTIVE_CALL_WINDOW_SECONDS.
    """
    now = datetime.utcnow()
    cutoff = now - timedelta(seconds=ACTIVE_CALL_WINDOW_SECONDS)
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


# ── Мои лиды (история) ───────────────────────────────────────
_STATUS_RU = {
    "callback_created": "Звонок создан",
    "connected": "Соединён",
    "talk_finished": "Разговор завершён",
    "no_answer": "Не ответил",
    "no_managers": "Нет на линии",
    "scheduled": "В очереди",
    "max_attempts_reached": "Исчерпаны попытки",
    "failed": "Ошибка",
    "duplicate_phone": "Дубль номера",
}


async def get_my_leads(manager_id: int, limit: int = 40) -> list:
    """История лидов этого менеджера из call_logs (последние N)."""
    from .models import CallLog
    async with async_session_maker() as session:
        result = await session.execute(
            select(CallLog)
            .where(CallLog.manager_id == manager_id)
            .order_by(CallLog.timestamp.desc())
            .limit(limit)
        )
        rows = result.scalars().all()
        out = []
        for r in rows:
            out.append({
                "lead_id": r.lead_id,
                "phone": r.phone,
                "name": r.lead_name,
                "type": r.type,
                "status": r.status,
                "status_ru": _STATUS_RU.get(r.status, r.status),
                "talk_seconds": r.talk_seconds,
                "timestamp": (r.timestamp.isoformat() + "Z") if r.timestamp else None,
            })
        return out


async def get_my_stats(manager_id: int) -> dict:
    """Личная статистика менеджера для раздела «Профиль»."""
    from .models import CallLog
    async with async_session_maker() as session:
        mgr = await session.get(Manager, manager_id)
        if not mgr:
            return {}
        # считаем из call_logs: соединения и недозвоны этого менеджера
        connected = await session.execute(
            select(func.count()).select_from(CallLog)
            .where(CallLog.manager_id == manager_id)
            .where(CallLog.status.in_(["connected", "talk_finished"]))
        )
        no_answer = await session.execute(
            select(func.count()).select_from(CallLog)
            .where(CallLog.manager_id == manager_id)
            .where(CallLog.status == "no_answer")
        )
        c = connected.scalar() or 0
        na = no_answer.scalar() or 0
        total = c + na
        conv = round(100 * c / total) if total else 0
        return {
            "name": mgr.name,
            "sipnumber": mgr.sipnumber,
            "online": bool(mgr.online),
            "accepted_calls": int(mgr.accepted_calls or 0),
            "missed": int(mgr.missed or 0),
            "connected": c,
            "no_answer": na,
            "conversion": conv,
            "priority_score": round(float(mgr.priority_score or 0.5), 2),
        }

# ── Коллеги для перевода звонка ──────────────────────────────
def _colleague_is_free(m: Manager, now: datetime) -> bool:
    """Свободен ли оператор для приёма перевода прямо сейчас."""
    return (
        bool(m.online)
        and (getattr(m, "busy_until", None) is None or m.busy_until <= now)
        and not getattr(m, "on_call", False)
        and not getattr(m, "awaiting_ready", False)
        and not getattr(m, "paused", False)
    )


async def get_colleagues(exclude_manager_id: int) -> list:
    """Список ОНЛАЙН-коллег (кроме себя) для перевода звонка.

    Оффлайн-операторов не показываем — перекинуть можно только на того, кто
    на линии. У каждого — статус free/busy, чтобы оператор видел, уйдёт звонок
    сразу или встанет в ожидание к этому коллеге.
    """
    now = datetime.utcnow()
    async with async_session_maker() as session:
        result = await session.execute(
            select(Manager)
            .where(Manager.online.is_(True))
            .where(Manager.id != exclude_manager_id)
        )
        managers = list(result.scalars().all())
    managers.sort(key=lambda m: (m.name or "").lower())
    out = []
    for m in managers:
        free = _colleague_is_free(m, now)
        out.append({
            "id": m.id,
            "name": m.name,
            "sipnumber": m.sipnumber,
            "free": free,
            "status": "Свободен" if free else "Занят",
        })
    return out
