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
    """Вернуть состояние рабочего экрана оператора (для всплытия карточки).

    Возможные ответы:
      • активный звонок (идёт) — state CALLBACK_CREATED, badge «Звонок…»;
      • режим «Завершение» (wrap_up=True) — разговор закончился, оператор
        держится до нажатия «Готов(а) звонить»; показываем карточку только что
        отговорённого лида + кнопку «Готово»;
      • None — оператор свободен, экран ожидания.

    «Завершение» определяется по busy_until-маркеру (см. dispatcher.is_wrap_up).
    Это же значение исключает оператора из раздачи, так что новый звонок ему
    не прилетит, пока он не нажал «Готово».
    """
    from .dispatcher import is_wrap_up

    now = datetime.utcnow()
    async with async_session_maker() as session:
        mgr = await session.get(Manager, manager_id)
        wrap = bool(mgr and is_wrap_up(getattr(mgr, "busy_until", None)))

        if wrap:
            # В завершении: показываем последнюю сессию этого оператора (только
            # что закончившийся разговор), окно шире — карточку можно заполнять
            # сколько нужно, она не пропадёт.
            states = ["CONNECTED", "CALLBACK_CREATED"]
            cutoff = now - timedelta(hours=12)
        else:
            # Не в завершении: показываем ТОЛЬКО активный (ещё идущий) звонок.
            # Завершённые (CONNECTED) сессии сюда не попадают — экран ожидания.
            states = ["CALLBACK_CREATED"]
            cutoff = now - timedelta(seconds=ACTIVE_CALL_WINDOW_SECONDS)

        result = await session.execute(
            select(CallSession)
            .where(CallSession.manager_id == manager_id)
            .where(CallSession.state.in_(states))
            .where(CallSession.started_at >= cutoff)
            .order_by(CallSession.started_at.desc())
        )
        s = result.scalars().first()

    if not s:
        if wrap:
            # Держим оператора, но свежей сессии не нашли — всё равно сигналим
            # «Завершение», чтобы он видел кнопку «Готово» и не завис молча.
            return {
                "lead_id": None, "phone": None, "state": "WRAP_UP",
                "connected": True, "wrap_up": True,
            }
        return None

    return {
        "lead_id": s.lead_id,
        "phone": s.phone,
        "state": s.state,
        "started_at": s.started_at.isoformat() + "Z" if s.started_at else None,
        "connected": wrap or (s.state == "CONNECTED"),
        "wrap_up": wrap,
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