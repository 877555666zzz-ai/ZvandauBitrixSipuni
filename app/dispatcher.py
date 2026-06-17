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
from typing import Dict, List, Optional, Set

from sqlalchemy import delete, select

from .bitrix_client import add_deal_comment, add_lead_comment, update_lead_status
from .config import settings
from .db import async_session_maker
from .models import AutodialQueue, CallLog, CallSession, Manager
from .priority import record_outcome, sort_managers
from .sipuni_client import make_outbound_call
from .telegram import send_alert

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# Idempotency
# ─────────────────────────────────────────────
_active_leads: Set[int] = set()
_active_lock = asyncio.Lock()


async def _acquire_lead(lead_id: int) -> bool:
    async with _active_lock:
        if lead_id in _active_leads:
            return False
        _active_leads.add(lead_id)
        return True


async def _release_lead(lead_id: int) -> None:
    async with _active_lock:
        _active_leads.discard(lead_id)


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
) -> None:
    next_attempt = current_attempts + 1

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
    norm = phone.strip()
    now = datetime.utcnow()
    cutoff = now - timedelta(hours=24)
    async with async_session_maker() as session:
        # 1) уже есть активная сессия на этот номер (идёт звонок)
        active = await session.execute(
            select(CallSession).where(
                CallSession.phone == norm,
                CallSession.state.in_(["CALLBACK_CREATED", "CONNECTED"]),
                CallSession.lead_id != exclude_lead_id,
            )
        )
        if active.scalars().first():
            return True
        # 2) номер уже в очереди автодозвона (ждёт повтора)
        queued = await session.execute(
            select(AutodialQueue).where(
                AutodialQueue.phone == norm,
                AutodialQueue.state.in_(["SCHEDULED", "IN_PROGRESS"]),
                AutodialQueue.lead_id != exclude_lead_id,
            )
        )
        if queued.scalars().first():
            return True
        # 3) на этот номер уже звонили за последние 24 часа
        recent = await session.execute(
            select(CallLog).where(
                CallLog.phone == norm,
                CallLog.timestamp >= cutoff,
                CallLog.lead_id != exclude_lead_id,
            )
        )
        if recent.scalars().first():
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
    norm_phone = "".join(ch for ch in client_phone if ch.isdigit())

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
        s_norm = "".join(ch for ch in (s.phone or "") if ch.isdigit())
        # Совпадение по последним 10 цифрам (страна-код может различаться)
        if s_norm[-10:] == norm_phone[-10:] and norm_phone:
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