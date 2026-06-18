# app/dispatcher.py
"""
Сердце проекта.

process_new_lead(lead_id, phone, lead_name?, lead_source?):
  1. Создать CallSession (это даст Sipuni webhook'у привязку к лиду).
  2. Получить online менеджеров.
  3. По очереди создать callback каждому через Sipuni.
  4. При callback_created: остановить цикл, ждать webhook от Sipuni
     (если настроен) — он переведёт session в CONNECTED либо NO_ANSWER.
  5. Если callback ни одному не создан → автодозвон.

Idempotency: in-process set активных лидов.
"""
import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from sqlalchemy import delete, select
from sqlalchemy.exc import IntegrityError

from .bitrix_client import add_deal_comment, add_lead_comment, update_lead_status
from .config import settings
from .db import async_session_maker
from .models import AutodialQueue, CallLog, CallSession, LeadLock, Manager
from .priority import record_outcome, sort_managers
from .sipuni_client import make_outbound_call
from .telegram import send_alert

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# Нормализация телефона
# ─────────────────────────────────────────────
def normalize_phone(phone: Optional[str]) -> str:
    """Привести телефон к единому виду — последние 10 цифр.

    Нужно, чтобы один и тот же номер в разных форматах считался одинаковым:
      +77075502088   → 7075502088
      77075502088    → 7075502088
      87075502088    → 7075502088
      7 707 550 2088 → 7075502088
    Так защита от дублей и матчинг сессий работают надёжно.
    """
    if not phone:
        return ""
    digits = "".join(ch for ch in str(phone) if ch.isdigit())
    # последние 10 цифр = номер без кода страны (7/8) и разделителей
    return digits[-10:] if len(digits) >= 10 else digits


# ─────────────────────────────────────────────
# Рабочее время дозвона
# ─────────────────────────────────────────────
# Звонки клиентам разрешены только в этом окне по местному времени (Алматы).
# Вне окна сделки не звонят, а ждут открытия (ставятся в очередь на 11:00).
# Сервер живёт в UTC, Алматы = UTC+5 (без перехода на летнее время).
_TZ_OFFSET_HOURS = 5          # Алматы UTC+5
_WORK_START_HOUR = 11         # с 11:00
_WORK_END_HOUR = 21           # до 21:00 (в 21:00 уже не звоним)


def _local_now() -> datetime:
    """Текущее местное время (Алматы)."""
    return datetime.utcnow() + timedelta(hours=_TZ_OFFSET_HOURS)


def _within_working_hours(local_dt: Optional[datetime] = None) -> bool:
    """True, если сейчас рабочее время (11:00–21:00 Алматы)."""
    local_dt = local_dt or _local_now()
    return _WORK_START_HOUR <= local_dt.hour < _WORK_END_HOUR


def _next_window_open_utc() -> datetime:
    """Ближайший момент открытия окна (11:00 Алматы), в UTC.

    Используется как next_call_time, чтобы отложенная сделка позвонила
    ровно когда откроется рабочее время, а не раньше.
    """
    local = _local_now()
    today_open = local.replace(
        hour=_WORK_START_HOUR, minute=0, second=0, microsecond=0
    )
    if local.hour < _WORK_START_HOUR:
        target_local = today_open                       # ещё утро — сегодня в 11
    else:
        target_local = today_open + timedelta(days=1)   # уже день/вечер — завтра в 11
    # обратно в UTC
    return target_local - timedelta(hours=_TZ_OFFSET_HOURS)


# ─────────────────────────────────────────────
# Idempotency — блокировка лида через БД (переживает рестарт, работает
# при нескольких репликах). Старая версия с Set в памяти теряла блокировки
# при каждом деплое.
# ─────────────────────────────────────────────
# Сколько живёт блокировка, прежде чем считается «протухшей» и может быть
# перехвачена. Защита от вечного залипания при сбое процесса.
_LEAD_LOCK_TTL_SECONDS = 600  # 10 минут


async def _acquire_lead(lead_id: int) -> bool:
    """Захватить лид. True — захватили, False — уже обрабатывается.

    Атомарно через БД: вставка строки с lead_id (PK). Если строка уже есть
    и свежая — занято. Если протухла (TTL) — перехватываем.
    """
    now = datetime.utcnow()
    stale_before = now - timedelta(seconds=_LEAD_LOCK_TTL_SECONDS)
    async with async_session_maker() as session:
        # Сначала убираем протухшую блокировку этого лида, если есть.
        await session.execute(
            delete(LeadLock).where(
                LeadLock.lead_id == lead_id,
                LeadLock.acquired_at < stale_before,
            )
        )
        await session.commit()
        # Пытаемся вставить свежую блокировку.
        session.add(LeadLock(lead_id=lead_id, acquired_at=now))
        try:
            await session.commit()
            return True
        except IntegrityError:
            # Строка уже есть и не протухла → лид занят.
            await session.rollback()
            return False


async def _release_lead(lead_id: int) -> None:
    """Снять блокировку лида."""
    async with async_session_maker() as session:
        await session.execute(
            delete(LeadLock).where(LeadLock.lead_id == lead_id)
        )
        await session.commit()


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────
def _next_delay_minutes(attempt_number: int) -> int:
    if attempt_number <= 1:
        return 5
    if attempt_number == 2:
        return 15
    return 30


async def _get_available_managers() -> List[Manager]:
    now = datetime.utcnow()
    async with async_session_maker() as session:
        result = await session.execute(
            select(Manager).where(Manager.online.is_(True))
        )
        managers = list(result.scalars().all())
    # Отсеиваем занятых: busy_until в будущем = ещё на звонке/в передышке.
    free = [
        m for m in managers
        if getattr(m, "busy_until", None) is None or m.busy_until <= now
    ]
    return sort_managers(free)


# Сколько секунд менеджер «занят» после старта звонка (страховка от залипания).
_BUSY_GUARD_SECONDS = 180
# Передышка после завершения звонка перед новым.
_COOLDOWN_SECONDS = 5


async def _set_busy(manager_id: int, seconds: int) -> None:
    """Пометить менеджера занятым на N секунд вперёд."""
    async with async_session_maker() as session:
        mgr = await session.get(Manager, manager_id)
        if mgr:
            mgr.busy_until = datetime.utcnow() + timedelta(seconds=seconds)
            await session.commit()


async def _release_after_cooldown(manager_id: int) -> None:
    """Освободить менеджера через _COOLDOWN_SECONDS после завершения звонка."""
    async with async_session_maker() as session:
        mgr = await session.get(Manager, manager_id)
        if mgr:
            mgr.busy_until = datetime.utcnow() + timedelta(seconds=_COOLDOWN_SECONDS)
            await session.commit()


async def _log_call(
    *,
    lead_id: int,
    phone: str,
    call_type: str,
    status: str,
    attempts: List[Dict],
    lead_name: Optional[str] = None,
    lead_source: Optional[str] = None,
    manager_id: Optional[int] = None,
    manager_name: Optional[str] = None,
    message: Optional[str] = None,
    reaction_seconds: Optional[float] = None,
    talk_seconds: Optional[float] = None,
) -> None:
    async with async_session_maker() as session:
        session.add(
            CallLog(
                lead_id=lead_id,
                phone=phone,
                lead_name=lead_name,
                lead_source=lead_source,
                type=call_type,
                status=status,
                manager_id=manager_id,
                manager_name=manager_name,
                message=message,
                reaction_seconds=reaction_seconds,
                talk_seconds=talk_seconds,
                details=json.dumps(attempts, ensure_ascii=False) if attempts else None,
            )
        )
        await session.commit()


async def _increment_missed(manager_id: int) -> None:
    async with async_session_maker() as session:
        mgr = await session.get(Manager, manager_id)
        if not mgr:
            return
        mgr.missed = (mgr.missed or 0) + 1
        if mgr.missed >= settings.MAX_MANAGER_MISSED:
            mgr.online = False
            logger.warning(
                "[discipline] %s (id=%d) offline (%d missed)",
                mgr.name, manager_id, mgr.missed,
            )
            await send_alert(
                f"⚠️ Менеджер <b>{mgr.name}</b> снят с линии "
                f"({mgr.missed} пропусков подряд)"
            )
        await session.commit()


async def _reset_missed(manager_id: int) -> None:
    async with async_session_maker() as session:
        mgr = await session.get(Manager, manager_id)
        if mgr:
            mgr.missed = 0
            await session.commit()


async def _mark_accepted(manager_id: int) -> None:
    async with async_session_maker() as session:
        mgr = await session.get(Manager, manager_id)
        if mgr:
            mgr.accepted_calls = (mgr.accepted_calls or 0) + 1
            await session.commit()


# ─────────────────────────────────────────────
# Очередь автодозвона
# ─────────────────────────────────────────────
async def schedule_autodial(
    lead_id: int,
    phone: str,
    current_attempts: int,
    lead_name: Optional[str] = None,
    lead_source: Optional[str] = None,
    next_call_time: Optional[datetime] = None,
) -> None:
    next_attempt = current_attempts + 1

    # Если время звонка задано извне (например, отложка до открытия рабочего
    # окна) — это не «провал попытки», номер попытки не увеличиваем.
    forced_time = next_call_time is not None
    if forced_time:
        next_attempt = current_attempts

    if next_attempt > settings.MAX_AUTODIAL_ATTEMPTS:
        async with async_session_maker() as session:
            result = await session.execute(
                select(AutodialQueue).where(AutodialQueue.lead_id == lead_id)
            )
            item = result.scalar_one_or_none()
            if item:
                item.state = "FAILED"
                await session.commit()

        logger.info("[autodial] лид %d: исчерпаны попытки", lead_id)
        await _log_call(
            lead_id=lead_id, phone=phone, call_type="autodial",
            status="max_attempts_reached", attempts=[],
            lead_name=lead_name, lead_source=lead_source,
            message=f"Не дозвонились после {settings.MAX_AUTODIAL_ATTEMPTS} попыток",
        )
        await update_lead_status(lead_id, "failed")
        await add_lead_comment(
            lead_id,
            f"Автодозвон: не удалось связаться после "
            f"{settings.MAX_AUTODIAL_ATTEMPTS} попыток.",
        )
        await send_alert(
            f"🔴 Лид #{lead_id} ({lead_name or 'без имени'}, {phone}) — "
            f"не удалось дозвониться после {settings.MAX_AUTODIAL_ATTEMPTS} попыток"
        )
        return

    delay_min = _next_delay_minutes(next_attempt)
    if forced_time:
        next_call_time = next_call_time  # уже задано извне (открытие окна)
    else:
        next_call_time = datetime.utcnow() + timedelta(minutes=delay_min)

    async with async_session_maker() as session:
        result = await session.execute(
            select(AutodialQueue).where(AutodialQueue.lead_id == lead_id)
        )
        item = result.scalar_one_or_none()
        if item:
            item.attempts = next_attempt
            item.phone = phone
            item.lead_name = lead_name or item.lead_name
            item.lead_source = lead_source or item.lead_source
            item.next_call_time = next_call_time
            item.state = "SCHEDULED"
        else:
            session.add(
                AutodialQueue(
                    lead_id=lead_id,
                    phone=phone,
                    lead_name=lead_name,
                    lead_source=lead_source,
                    attempts=next_attempt,
                    next_call_time=next_call_time,
                    state="SCHEDULED",
                )
            )
        try:
            await session.commit()
        except Exception as e:
            await session.rollback()
            logger.warning("[autodial] schedule conflict #%d: %s", lead_id, e)

    logger.info(
        "[autodial] лид %d: попытка %d/%d через %d мин",
        lead_id, next_attempt, settings.MAX_AUTODIAL_ATTEMPTS, delay_min,
    )


# ─────────────────────────────────────────────
# Основная логика
# ─────────────────────────────────────────────
async def _phone_recently_handled(phone: str, exclude_lead_id: int) -> bool:
    """True, если на этот номер уже звоним/в очереди/звонили недавно.

    Защита от дублей: один телефон = один звонок, даже если в Bitrix
    создано несколько сделок с одним номером.
    Проверяем: активная сессия, очередь автодозвона, звонок за последние 24ч.
    """
    if not phone:
        return False
    norm = normalize_phone(phone)
    if not norm:
        return False
    now = datetime.utcnow()
    cutoff = now - timedelta(hours=24)
    # Зависшие сессии (рестарт во время звонка) не должны блокировать номер
    # навечно — учитываем только свежие активные сессии.
    session_stale = now - timedelta(minutes=15)
    async with async_session_maker() as session:
        # 1) уже есть активная (и свежая) сессия на этот номер (идёт звонок)
        active = await session.execute(
            select(CallSession.phone, CallSession.lead_id).where(
                CallSession.state.in_(["CALLBACK_CREATED", "CONNECTED"]),
                CallSession.started_at >= session_stale,
                CallSession.lead_id != exclude_lead_id,
            )
        )
        for ph, _lid in active.all():
            if normalize_phone(ph) == norm:
                return True
        # 2) номер уже в очереди автодозвона (ждёт повтора)
        queued = await session.execute(
            select(AutodialQueue.phone, AutodialQueue.lead_id).where(
                AutodialQueue.state.in_(["SCHEDULED", "IN_PROGRESS"]),
                AutodialQueue.lead_id != exclude_lead_id,
            )
        )
        for ph, _lid in queued.all():
            if normalize_phone(ph) == norm:
                return True
        # 3) на этот номер уже звонили за последние 24 часа
        recent = await session.execute(
            select(CallLog.phone, CallLog.lead_id).where(
                CallLog.timestamp >= cutoff,
                CallLog.lead_id != exclude_lead_id,
            )
        )
        for ph, _lid in recent.all():
            if normalize_phone(ph) == norm:
                return True
    return False


async def process_new_lead(
    lead_id: int,
    client_phone: str,
    lead_name: Optional[str] = None,
    lead_source: Optional[str] = None,
    is_autodial: bool = False,
    received_at: Optional[datetime] = None,
) -> Dict:
    """
    received_at — когда webhook пришёл от Bitrix. Используется для метрики
    «время реакции» (от webhook'а до первого callback'а).
    """
    if not await _acquire_lead(lead_id):
        logger.info("[dispatch] лид %d уже обрабатывается", lead_id)
        return {"ok": True, "status": "already_in_progress", "lead_id": lead_id}

    # Защита от дублей по телефону (только для новых лидов, не для автодозвона).
    if not is_autodial and await _phone_recently_handled(client_phone, lead_id):
        logger.info(
            "[dispatch] лид %d: номер %s уже обрабатывается/звонили — дубль, пропуск",
            lead_id, client_phone,
        )
        await _release_lead(lead_id)
        return {"ok": True, "status": "duplicate_phone", "lead_id": lead_id}

    # Рабочее время: новые лиды вне окна 11:00–21:00 не звонят сразу,
    # а откладываются до открытия окна (звонок не разбудит клиента ночью).
    if not is_autodial and not _within_working_hours():
        next_open = _next_window_open_utc()
        logger.info(
            "[dispatch] лид %d: вне рабочего времени — отложен до %s UTC",
            lead_id, next_open.strftime("%Y-%m-%d %H:%M"),
        )
        await _log_call(
            lead_id=lead_id, phone=client_phone, call_type="initial",
            status="scheduled", attempts=[],
            lead_name=lead_name, lead_source=lead_source,
            message="Вне рабочего времени — отложено до открытия окна",
        )
        await schedule_autodial(
            lead_id, client_phone, current_attempts=0,
            lead_name=lead_name, lead_source=lead_source,
            next_call_time=next_open,
        )
        await _release_lead(lead_id)
        return {"ok": True, "status": "outside_working_hours", "lead_id": lead_id}

    received_at = received_at or datetime.utcnow()

    try:
        managers = await _get_available_managers()
        call_type = "autodial" if is_autodial else "initial"

        if not managers:
            logger.warning("[dispatch] лид %d: нет online менеджеров", lead_id)
            reaction = (datetime.utcnow() - received_at).total_seconds()
            await _log_call(
                lead_id=lead_id, phone=client_phone, call_type=call_type,
                status="no_managers", attempts=[],
                lead_name=lead_name, lead_source=lead_source,
                reaction_seconds=reaction,
                message="Нет online менеджеров",
            )
            await send_alert(
                f"🟡 Лид #{lead_id} ({lead_name or 'без имени'}, {client_phone}): "
                f"нет менеджеров на линии"
            )
            if not is_autodial:
                await schedule_autodial(
                    lead_id, client_phone, current_attempts=0,
                    lead_name=lead_name, lead_source=lead_source,
                )
                await update_lead_status(lead_id, "retry")
                await add_lead_comment(
                    lead_id,
                    "Автодозвон: нет менеджеров на линии — лид в очереди.",
                )
            return {"ok": True, "status": "no_managers_available", "lead_id": lead_id}

        await update_lead_status(lead_id, "dialing")

        attempts: List[Dict] = []

        for manager in managers:
            logger.info(
                "[dispatch] лид %d → callback %s (ext=%s)",
                lead_id, manager.name, manager.sipnumber,
            )

            callback_start = datetime.utcnow()
            sipuni_resp = await make_outbound_call(manager.sipnumber, client_phone)
            attempts.append({
                "manager_id": manager.id,
                "manager_name": manager.name,
                "sipnumber": manager.sipnumber,
                "sipuni_response": sipuni_resp,
            })

            if sipuni_resp.get("callback_created"):
                reaction = (callback_start - received_at).total_seconds()

                # Создаём CallSession — будем ждать Sipuni webhook
                async with async_session_maker() as session:
                    session.add(
                        CallSession(
                            lead_id=lead_id,
                            phone=client_phone,
                            manager_id=manager.id,
                            manager_name=manager.name,
                            manager_sipnumber=manager.sipnumber,
                            state="CALLBACK_CREATED",
                            callback_at=callback_start,
                            is_autodial=is_autodial,
                            attempts_used=len(attempts),
                        )
                    )
                    # Чистим очередь
                    await session.execute(
                        delete(AutodialQueue).where(AutodialQueue.lead_id == lead_id)
                    )
                    await session.commit()

                # MVP: считаем callback успехом для priority и accepted_calls.
                # Когда придёт Sipuni webhook со статусом «не ответил» —
                # значения скорректируются обратно.
                await _reset_missed(manager.id)
                await _mark_accepted(manager.id)
                await record_outcome(manager.id, success=True)
                # Менеджер занят на время звонка (страховка 3 мин от залипания,
                # реально снимется через 5с после завершения звонка).
                await _set_busy(manager.id, _BUSY_GUARD_SECONDS)

                await _log_call(
                    lead_id=lead_id, phone=client_phone, call_type=call_type,
                    status="callback_created", attempts=attempts,
                    lead_name=lead_name, lead_source=lead_source,
                    manager_id=manager.id, manager_name=manager.name,
                    reaction_seconds=reaction,
                    message=f"Sipuni callback → {manager.name} (ext {manager.sipnumber})",
                )

                await update_lead_status(lead_id, "connected")
                await add_lead_comment(
                    lead_id,
                    f"Автодозвон: callback назначен менеджеру {manager.name} "
                    f"(ext. {manager.sipnumber}). Время реакции {reaction:.1f}с.",
                )

                logger.info(
                    "[dispatch] лид %d: callback %s | реакция %.1fс",
                    lead_id, manager.name, reaction,
                )
                return {
                    "ok": True,
                    "status": "callback_created",
                    "lead_id": lead_id,
                    "manager_id": manager.id,
                    "manager_name": manager.name,
                    "reaction_seconds": reaction,
                    "attempts": attempts,
                }

            # Не приняли — пробуем следующего
            await record_outcome(manager.id, success=False)
            await _increment_missed(manager.id)
            await asyncio.sleep(settings.MANAGER_ANSWER_TIMEOUT_SECONDS)

        # Никто не принял
        reaction = (datetime.utcnow() - received_at).total_seconds()
        await _log_call(
            lead_id=lead_id, phone=client_phone, call_type=call_type,
            status="no_answer", attempts=attempts,
            lead_name=lead_name, lead_source=lead_source,
            reaction_seconds=reaction,
            message="Никто не принял callback",
        )

        if not is_autodial:
            await schedule_autodial(
                lead_id, client_phone, current_attempts=0,
                lead_name=lead_name, lead_source=lead_source,
            )
            await update_lead_status(lead_id, "retry")
            await add_lead_comment(
                lead_id,
                "Автодозвон: никто не ответил — в очереди повторного дозвона.",
            )
        else:
            await update_lead_status(lead_id, "retry")

        logger.info("[dispatch] лид %d: никто, попыток=%d", lead_id, len(attempts))
        return {
            "ok": True,
            "status": "no_manager_answered",
            "lead_id": lead_id,
            "attempts": attempts,
        }
    finally:
        await _release_lead(lead_id)


# ─────────────────────────────────────────────
# Sipuni webhook handler (вызывается из main)
# ─────────────────────────────────────────────
async def handle_sipuni_status(
    sipnumber: Optional[str],
    client_phone: Optional[str],
    talk_seconds: Optional[float],
    answered: bool,
    raw: Dict,
) -> Dict:
    """
    Найти активную CallSession по sipnumber + phone и закрыть её
    реальным статусом. Если answered=False — поправить счётчики менеджера
    (раньше мы инкрементнули accepted_calls по факту callback, теперь
    откатываем) и поставить лид в автодозвон.
    """
    if not client_phone:
        logger.warning("[sipuni-webhook] нет client_phone: %s", raw)
        return {"ok": False, "error": "no_client_phone"}

    # Нормализация — Sipuni может слать с/без +, с пробелами и т.п.
    norm_phone = normalize_phone(client_phone)

    async with async_session_maker() as session:
        # Ищем последнюю «висящую» сессию по этому телефону
        result = await session.execute(
            select(CallSession)
            .where(
                CallSession.state == "CALLBACK_CREATED",
            )
            .order_by(CallSession.id.desc())
            .limit(20)
        )
        candidates = list(result.scalars().all())

    matched: Optional[CallSession] = None
    for s in candidates:
        s_norm = normalize_phone(s.phone)
        # Совпадение по нормализованному номеру (последние 10 цифр)
        if s_norm and norm_phone and s_norm == norm_phone:
            if sipnumber and s.manager_sipnumber and str(s.manager_sipnumber) != str(sipnumber):
                continue
            matched = s
            break

    if not matched:
        logger.info(
            "[sipuni-webhook] no matching session: phone=%s sipnumber=%s",
            client_phone, sipnumber,
        )
        return {"ok": True, "matched": False}

    # Обновляем сессию
    async with async_session_maker() as session:
        s = await session.get(CallSession, matched.id)
        if not s:
            return {"ok": True, "matched": False}

        now = datetime.utcnow()
        if answered:
            s.state = "CONNECTED"
            s.connected_at = now
            s.talk_seconds = talk_seconds
            await session.commit()

            await _log_call(
                lead_id=s.lead_id, phone=s.phone, call_type="sipuni_webhook",
                status="connected", attempts=[],
                manager_id=s.manager_id, manager_name=s.manager_name,
                talk_seconds=talk_seconds,
                message=f"Реально соединились ({talk_seconds:.0f}с разговор)" if talk_seconds else "Соединились",
            )
            # Пишем комментарий — универсально для лида и сделки
            comment_text = (
                f"Автодозвон: соединились с клиентом ✅ "
                f"Менеджер: {s.manager_name}. "
                f"Разговор: {int(talk_seconds or 0)} сек."
            )
            await add_lead_comment(s.lead_id, comment_text)
            await add_deal_comment(s.lead_id, comment_text)
            if s.manager_id:
                await _release_after_cooldown(s.manager_id)
            logger.info(
                "[sipuni-webhook] лид %d CONNECTED (%.0fс)",
                s.lead_id, talk_seconds or 0,
            )
            return {"ok": True, "matched": True, "state": "CONNECTED"}

        # Не ответил — откатываем accepted и инкрементим missed
        s.state = "NO_ANSWER"
        s.talk_seconds = talk_seconds
        await session.commit()

        if s.manager_id:
            async with async_session_maker() as s2:
                mgr = await s2.get(Manager, s.manager_id)
                if mgr:
                    mgr.accepted_calls = max(0, (mgr.accepted_calls or 1) - 1)
                    mgr.missed = (mgr.missed or 0) + 1
                    if mgr.missed >= settings.MAX_MANAGER_MISSED:
                        mgr.online = False
                    await s2.commit()
            await record_outcome(s.manager_id, success=False)
            await _release_after_cooldown(s.manager_id)

        # Лид в очередь
        await _log_call(
            lead_id=s.lead_id, phone=s.phone, call_type="sipuni_webhook",
            status="no_answer", attempts=[],
            manager_id=s.manager_id, manager_name=s.manager_name,
            talk_seconds=talk_seconds,
            message="Sipuni: менеджер не ответил по факту",
        )

        await schedule_autodial(
            s.lead_id, s.phone, current_attempts=s.attempts_used,
        )
        await update_lead_status(s.lead_id, "retry")
        await add_lead_comment(
            s.lead_id,
            f"Sipuni: менеджер {s.manager_name} не ответил по факту. "
            f"Лид поставлен в автодозвон.",
        )
        logger.info(
            "[sipuni-webhook] лид %d NO_ANSWER, менеджер=%s",
            s.lead_id, s.manager_name,
        )
        return {"ok": True, "matched": True, "state": "NO_ANSWER"}


# ─────────────────────────────────────────────
# Worker
# ─────────────────────────────────────────────
async def autodial_worker() -> None:
    interval = settings.AUTODIAL_POLL_INTERVAL_SECONDS
    logger.info("[autodial_worker] запущен, интервал=%ds", interval)

    while True:
        try:
            # Вне рабочего окна 11:00–21:00 (Алматы) очередь не обзваниваем —
            # отложенные лиды дождутся открытия. Гарантия: ночью клиентам
            # не звоним ни по новым лидам, ни из очереди.
            if not _within_working_hours():
                await asyncio.sleep(interval)
                continue

            now = datetime.utcnow()
            async with async_session_maker() as session:
                result = await session.execute(
                    select(AutodialQueue).where(
                        AutodialQueue.state == "SCHEDULED",
                        AutodialQueue.next_call_time <= now,
                    )
                )
                items = list(result.scalars().all())
                for item in items:
                    item.state = "IN_PROGRESS"
                await session.commit()

            if items:
                logger.info(
                    "[autodial_worker] обрабатываем %d задач", len(items)
                )

            for item in items:
                lead_id = item.lead_id
                phone = item.phone
                attempts = item.attempts
                lead_name = item.lead_name
                lead_source = item.lead_source
                try:
                    res = await process_new_lead(
                        lead_id, phone,
                        lead_name=lead_name,
                        lead_source=lead_source,
                        is_autodial=True,
                    )
                    if res.get("status") != "callback_created":
                        await schedule_autodial(
                            lead_id, phone, current_attempts=attempts,
                            lead_name=lead_name, lead_source=lead_source,
                        )
                except Exception as e:
                    logger.error(
                        "[autodial_worker] ошибка для лида %d: %s",
                        lead_id, e, exc_info=True,
                    )
                    async with async_session_maker() as session:
                        r = await session.execute(
                            select(AutodialQueue).where(
                                AutodialQueue.lead_id == lead_id
                            )
                        )
                        q = r.scalar_one_or_none()
                        if q and q.state == "IN_PROGRESS":
                            q.state = "SCHEDULED"
                            q.next_call_time = datetime.utcnow() + timedelta(minutes=5)
                            await session.commit()

        except asyncio.CancelledError:
            logger.info("[autodial_worker] остановлен")
            raise
        except Exception as e:
            logger.error("[autodial_worker] критическая ошибка: %s", e, exc_info=True)

        await asyncio.sleep(interval)