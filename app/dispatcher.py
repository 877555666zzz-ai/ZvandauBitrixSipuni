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

from .bitrix_client import add_deal_comment, add_lead_comment, update_lead_status, update_deal_stage, assign_deal_responsible
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


async def _get_available_managers(exclude_manager_id: Optional[int] = None) -> List[Manager]:
    now = datetime.utcnow()
    async with async_session_maker() as session:
        result = await session.execute(
            select(Manager).where(Manager.online.is_(True))
        )
        managers = list(result.scalars().all())
    # Отсеиваем занятых: busy_until в будущем = ещё на звонке.
    # Отсеиваем «на звонке» (on_call) — пока звонок не завершён, второй ему НЕ шлём.
    # Отсеиваем «ждём Готов» (awaiting_ready) — оператор после звонка дозаполняет
    #   карточку; следующий звонок ему придёт только когда сам нажмёт «Готов».
    # Отсеиваем на ручной паузе (paused) — оператор «отошёл».
    free = [
        m for m in managers
        if (getattr(m, "busy_until", None) is None or m.busy_until <= now)
        and not getattr(m, "on_call", False)
        and not getattr(m, "awaiting_ready", False)
        and not getattr(m, "paused", False)
        and (exclude_manager_id is None or m.id != exclude_manager_id)
    ]
    return sort_managers(free)


def _is_manager_free(m: Manager, now: Optional[datetime] = None) -> bool:
    """Свободен ли конкретный оператор для нового звонка (те же условия, что и
    в _get_available_managers, но для одного менеджера)."""
    now = now or datetime.utcnow()
    return (
        bool(m.online)
        and (getattr(m, "busy_until", None) is None or m.busy_until <= now)
        and not getattr(m, "on_call", False)
        and not getattr(m, "awaiting_ready", False)
        and not getattr(m, "paused", False)
    )


async def _get_target_manager(manager_id: int) -> Optional[Manager]:
    """Загрузить конкретного менеджера (для адресного дозвона/перевода)."""
    async with async_session_maker() as session:
        return await session.get(Manager, manager_id)


# Сколько секунд менеджер «занят» после старта звонка (страховка от залипания).
_BUSY_GUARD_SECONDS = 180
# «Передышка» после завершения звонка перед новым — чтобы оператор успел
# дозаполнить карточку. Берётся из настроек (по умолчанию 60с). Оператор может
# закончить раньше кнопкой «Готов принимать».
_COOLDOWN_SECONDS = settings.MANAGER_BREATHER_SECONDS


# Глобальный замок диспетчеризации: атомарно «выбрать свободного + зарезервировать»,
# чтобы два почти одновременных лида не выбрали одного оператора и не прислали ему
# два звонка. Под замком — ТОЛЬКО быстрый выбор+резерв (без сетевых вызовов).
_DISPATCH_LOCK = asyncio.Lock()


async def _set_busy(manager_id: int, seconds: int, on_call: Optional[bool] = None) -> None:
    """Пометить менеджера занятым на N секунд вперёд.

    on_call=True  — оператор сейчас на звонке (не показываем передышку);
    on_call=False — звонок закончился (с этого момента busy_until = передышка).
    on_call=None  — флаг не трогаем.
    """
    async with async_session_maker() as session:
        mgr = await session.get(Manager, manager_id)
        if mgr:
            mgr.busy_until = datetime.utcnow() + timedelta(seconds=seconds)
            if on_call is not None:
                mgr.on_call = on_call
            await session.commit()


async def _release_after_cooldown(manager_id: int) -> None:
    """Звонок завершён. Передышки больше НЕТ — оператор переходит в режим
    «ждём кнопку Готов»: автодозвон не шлёт ему звонки, пока он сам не нажмёт
    «Готов принимать» на странице. Это даёт время дозаполнить карточку."""
    async with async_session_maker() as session:
        mgr = await session.get(Manager, manager_id)
        if mgr:
            mgr.busy_until = None
            mgr.on_call = False
            mgr.awaiting_ready = True
            await session.commit()


async def mark_busy_from_sipuni(sipnumber: Optional[str], event_finished: bool) -> None:
    """Отметить занятость нашего оператора по ЛЮБОМУ звонку из Sipuni.

    Sipuni шлёт события обо всех звонках на АТС — наших, чужих, входящих,
    из других воронок. Если sipnumber совпадает с одним из НАШИХ операторов,
    значит этот оператор реально на линии (неважно по какому поводу) — и наш
    автодозвон не должен слать ему звонок поверх разговора.

      event начался (event_finished=False) → оператор занят;
      event завершён (event_finished=True) → освобождаем через передышку.

    Операторов не из нашей базы (чужие sip-номера) игнорируем — их занятость
    нам неважна.
    """
    if not sipnumber:
        return
    sip = str(sipnumber).strip()
    async with async_session_maker() as session:
        result = await session.execute(
            select(Manager).where(Manager.sipnumber == sip)
        )
        mgr = result.scalars().first()
        if not mgr:
            return  # это не наш оператор — пропускаем
        if event_finished:
            # звонок завершён → оператор в режим «ждём кнопку Готов»
            mgr.busy_until = None
            mgr.on_call = False
            mgr.awaiting_ready = True
            logger.info(
                "[busy] оператор %s (sip=%s) завершил звонок — ждём кнопку «Готов принимать»",
                mgr.name, sip,
            )
        else:
            # звонок начался/идёт → занят и на звонке (страховка от залипания)
            mgr.busy_until = datetime.utcnow() + timedelta(seconds=_BUSY_GUARD_SECONDS)
            mgr.on_call = True
            logger.info(
                "[busy] оператор %s (sip=%s) занят — на звонке",
                mgr.name, sip,
            )
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
    target_manager_id: Optional[int] = None,
    last_manager_sipnumber: Optional[str] = None,
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
        # Все попытки исчерпаны, лид так и не ответил → стадия НДЗ 2
        await update_deal_stage(lead_id, settings.BITRIX_STAGE_NDZ2)
        # Вешаем ответственным последнего оператора, к которому шла попытка
        # (клиент не ответил, но лид должен остаться закреплён за оператором).
        if last_manager_sipnumber:
            await assign_deal_responsible(lead_id, last_manager_sipnumber)
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
            if target_manager_id is not None:
                item.target_manager_id = target_manager_id
        else:
            session.add(
                AutodialQueue(
                    lead_id=lead_id,
                    phone=phone,
                    lead_name=lead_name,
                    lead_source=lead_source,
                    target_manager_id=target_manager_id,
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
# Очередь ОЖИДАНИЯ свободного менеджера (быстрая полоса)
# ─────────────────────────────────────────────
# Отличие от schedule_autodial (перезвон по таймеру):
#   WAITING   — лид готов прямо сейчас, ждёт лишь когда освободится менеджер.
#               next_call_time = now, номер попытки НЕ растёт (это не провал
#               дозвона, а просто ожидание свободного оператора). Воркер
#               проверяет такие лиды часто и отдаёт первому же освободившемуся
#               менеджеру (FIFO — кто раньше встал в очередь, того и первым).
#   SCHEDULED — перезвон через +5/+15/+30 мин (до клиента не дозвонились).
#
# Возвращает True, если лид ТОЛЬКО ЧТО поставлен в ожидание (его раньше не было
# в очереди в состоянии WAITING). False — если он уже ждал (это повторная
# проверка воркером). Нужно, чтобы не спамить Bitrix-комментарием/алертом на
# каждой итерации ожидания.
async def _enqueue_waiting(
    lead_id: int,
    phone: str,
    attempts: int = 0,
    lead_name: Optional[str] = None,
    lead_source: Optional[str] = None,
    insert_if_missing: bool = True,
    target_manager_id: Optional[int] = None,
) -> bool:
    now = datetime.utcnow()
    async with async_session_maker() as session:
        result = await session.execute(
            select(AutodialQueue).where(AutodialQueue.lead_id == lead_id)
        )
        item = result.scalar_one_or_none()
        was_waiting = bool(item and item.state == "WAITING")
        if item:
            item.phone = phone
            item.lead_name = lead_name or item.lead_name
            item.lead_source = lead_source or item.lead_source
            item.next_call_time = now          # готов сразу
            item.state = "WAITING"
            if target_manager_id is not None:
                item.target_manager_id = target_manager_id
            # attempts НЕ трогаем — ожидание не списывает попытку дозвона
        elif insert_if_missing:
            session.add(
                AutodialQueue(
                    lead_id=lead_id,
                    phone=phone,
                    lead_name=lead_name,
                    lead_source=lead_source,
                    target_manager_id=target_manager_id,
                    attempts=attempts,
                    next_call_time=now,
                    state="WAITING",
                )
            )
        else:
            # Строки в очереди нет, и вставлять нельзя. Так бывает, когда лид
            # отменили/очистили из дашборда, ПОКА воркер обрабатывал его батч.
            # Не воскрешаем — отмена/очистка должны побеждать воркер.
            return False
        try:
            await session.commit()
        except Exception as e:
            await session.rollback()
            logger.warning("[waiting] enqueue conflict #%d: %s", lead_id, e)
    return not was_waiting


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
        # 2) номер уже в очереди автодозвона (ждёт повтора или свободного менеджера)
        queued = await session.execute(
            select(AutodialQueue.phone, AutodialQueue.lead_id).where(
                AutodialQueue.state.in_(["SCHEDULED", "IN_PROGRESS", "WAITING"]),
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
    from_queue: bool = False,
    target_manager_id: Optional[int] = None,
    is_transfer: bool = False,
) -> Dict:
    """
    received_at — когда webhook пришёл от Bitrix. Используется для метрики
    «время реакции» (от webhook'а до первого callback'а).

    from_queue=True — лид пришёл из очереди (воркер его перепроверяет). Тогда,
    если свободного менеджера нет, мы НЕ создаём новую строку очереди заново,
    а лишь обновляем существующую. Если строки уже нет (лид отменили/очистили
    из дашборда), лид не воскрешаем — отмена/очистка побеждают воркер.

    target_manager_id — адресный дозвон (перевод звонка): лид уходит ТОЛЬКО
    этому оператору. Свободен → звоним сразу; занят → лид ждёт именно его;
    на других НЕ раскидываем.

    is_transfer=True — это перевод активного звонка. Обходит проверку «дубль
    номера» и «рабочие часы» (клиент уже на линии, его нельзя отложить).
    """
    # Перевод звонка приравниваем к автодозвону для служебных проверок
    # (не пишем дубль-логи, не откладываем по рабочим часам).
    skip_new_lead_checks = is_autodial or is_transfer

    if not await _acquire_lead(lead_id):
        logger.info("[dispatch] лид %d уже обрабатывается", lead_id)
        return {"ok": True, "status": "already_in_progress", "lead_id": lead_id}

    # Защита от дублей по телефону (только для новых лидов, не для автодозвона/перевода).
    if not skip_new_lead_checks and await _phone_recently_handled(client_phone, lead_id):
        logger.info(
            "[dispatch] лид %d: номер %s уже обрабатывается/звонили — дубль, пропуск",
            lead_id, client_phone,
        )
        await _release_lead(lead_id)
        return {"ok": True, "status": "duplicate_phone", "lead_id": lead_id}

    # Рабочее время: новые лиды вне окна 11:00–21:00 не звонят сразу,
    # а откладываются до открытия окна (звонок не разбудит клиента ночью).
    if not skip_new_lead_checks and not _within_working_hours():
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
        call_type = "autodial" if is_autodial else "initial"

        # Атомарно выбираем и резервируем ОДНОГО свободного оператора под общим
        # замком — чтобы два почти одновременных лида НЕ выбрали одного и того же
        # и не прислали ему два звонка. В замке только быстрый выбор+резерв;
        # сетевой вызов Sipuni делаем уже ВНЕ замка.
        async with _DISPATCH_LOCK:
            if target_manager_id is not None:
                # Адресный дозвон (перевод): берём ТОЛЬКО целевого оператора.
                tgt = await _get_target_manager(target_manager_id)
                if tgt is not None and not bool(tgt.online):
                    # Цель ушла с линии, пока лид ждал → не держим клиента в
                    # вечном ожидании, передаём как обычный лид первому свободному.
                    logger.info(
                        "[dispatch] лид %d: цель перевода (id=%s) оффлайн → в общий пул",
                        lead_id, target_manager_id,
                    )
                    target_manager_id = None
                    managers = await _get_available_managers()
                    manager = managers[0] if managers else None
                else:
                    # Свободен → резервируем; занят → manager=None (ждём ИМЕННО его).
                    manager = tgt if (tgt and _is_manager_free(tgt)) else None
            else:
                managers = await _get_available_managers()
                manager = managers[0] if managers else None
            if manager is not None:
                await _set_busy(manager.id, _BUSY_GUARD_SECONDS, on_call=True)

        if manager is None:
            # Нет СВОБОДНОГО менеджера (все заняты/на звонке/на паузе/оффлайн).
            # Обычный лид ждёт первого освободившегося; перевод — ждёт ИМЕННО
            # целевого оператора. Клиенту в этой ветке ещё НЕ звонили (звоним
            # сначала менеджеру), поэтому держать лид в ожидании безопасно.
            first_time = await _enqueue_waiting(
                lead_id, client_phone,
                attempts=0,
                lead_name=lead_name, lead_source=lead_source,
                insert_if_missing=not from_queue,
                target_manager_id=target_manager_id,
            )
            if first_time:
                logger.warning(
                    "[dispatch] лид %d: нет свободных менеджеров → очередь ожидания",
                    lead_id,
                )
                reaction = (datetime.utcnow() - received_at).total_seconds()
                await _log_call(
                    lead_id=lead_id, phone=client_phone, call_type=call_type,
                    status="no_managers", attempts=[],
                    lead_name=lead_name, lead_source=lead_source,
                    reaction_seconds=reaction,
                    message="Нет свободных менеджеров — лид в очереди ожидания",
                )
                await update_lead_status(lead_id, "retry")
                await add_lead_comment(
                    lead_id,
                    "Все менеджеры сейчас заняты — лид в очереди ожидания, "
                    "позвоним как только освободится оператор.",
                )
                await send_alert(
                    f"🟡 Лид #{lead_id} ({lead_name or 'без имени'}, {client_phone}): "
                    f"все менеджеры заняты — лид в очереди ожидания"
                )
            else:
                logger.info(
                    "[dispatch] лид %d: всё ещё нет свободных — ждёт в очереди",
                    lead_id,
                )
            return {"ok": True, "status": "no_managers_available", "lead_id": lead_id}

        # ── Звоним ОДНОМУ зарезервированному оператору (вне замка) ──────────
        await update_lead_status(lead_id, "dialing")
        attempts: List[Dict] = []
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
                        is_transfer=is_transfer,
                        target_manager_id=target_manager_id,
                        attempts_used=len(attempts),
                    )
                )
                await session.execute(
                    delete(AutodialQueue).where(AutodialQueue.lead_id == lead_id)
                )
                await session.commit()

            await _reset_missed(manager.id)
            await _mark_accepted(manager.id)
            await record_outcome(manager.id, success=True)
            # Подтверждаем «на звонке» (резерв уже стоял; обновляем guard).
            # И сразу помечаем «ждём Готов»: даже если оператор повесит трубку
            # без события завершения от Sipuni, ему всё равно не пойдёт второй
            # звонок до нажатия «Готов принимать».
            async with async_session_maker() as ms:
                mgr_obj = await ms.get(Manager, manager.id)
                if mgr_obj:
                    mgr_obj.busy_until = datetime.utcnow() + timedelta(seconds=_BUSY_GUARD_SECONDS)
                    mgr_obj.on_call = True
                    mgr_obj.awaiting_ready = True
                    await ms.commit()

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

        # Sipuni не принял callback (линия оператора занята/недоступна).
        # Снимаем резерв этого оператора и кладём лид в очередь ОЖИДАНИЯ —
        # воркер перевыберет свободного через ~15с. Перебирать остальных прямо
        # сейчас НЕ нужно (это и создавало риск второго звонка по устаревшему
        # списку): если оператор не ОТВЕТИТ — каскад сам передаст лида дальше.
        await _set_busy(manager.id, 0, on_call=False)
        await record_outcome(manager.id, success=False)
        await _increment_missed(manager.id)

        await _enqueue_waiting(
            lead_id, client_phone,
            attempts=0,
            lead_name=lead_name, lead_source=lead_source,
            insert_if_missing=not from_queue,
            target_manager_id=target_manager_id,
        )
        await update_lead_status(lead_id, "retry")
        await _log_call(
            lead_id=lead_id, phone=client_phone, call_type=call_type,
            status="no_answer", attempts=attempts,
            lead_name=lead_name, lead_source=lead_source,
            message="Sipuni не принял callback — лид в очереди ожидания",
        )
        logger.info(
            "[dispatch] лид %d: Sipuni не принял callback (%s) → очередь ожидания",
            lead_id, manager.name,
        )
        return {
            "ok": True,
            "status": "no_managers_available",
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
            # Назначить оператора ответственным за сделку (тот, кто принял звонок)
            if s.manager_sipnumber:
                await assign_deal_responsible(s.lead_id, s.manager_sipnumber)
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

        await _log_call(
            lead_id=s.lead_id, phone=s.phone, call_type="sipuni_webhook",
            status="no_answer", attempts=[],
            manager_id=s.manager_id, manager_name=s.manager_name,
            talk_seconds=talk_seconds,
            message="Sipuni: менеджер не ответил по факту",
        )

        # ── ПЕРЕВОД: адресный лид НЕ каскадим на других ─────────────
        # При переводе клиент закреплён за конкретным оператором. Если он не
        # ответил — не раскидываем на коллег, а повторяем к нему же.
        if getattr(s, "is_transfer", False) and s.target_manager_id is not None:
            if s.connected_at is not None:
                # взял трубку, но клиент не ответил → таймерный перезвон к нему же
                await schedule_autodial(
                    s.lead_id, s.phone, current_attempts=s.attempts_used,
                    target_manager_id=s.target_manager_id,
                )
                await update_lead_status(s.lead_id, "retry")
                await add_lead_comment(
                    s.lead_id,
                    f"Перевод на {s.manager_name}: клиент не ответил — повтор к нему же.",
                )
                return {"ok": True, "matched": True, "state": "TRANSFER_RETRY"}
            # оператор не взял трубку → ждём, пока он освободится (не чужим)
            await _enqueue_waiting(
                s.lead_id, s.phone, attempts=s.attempts_used,
                target_manager_id=s.target_manager_id,
            )
            await update_lead_status(s.lead_id, "retry")
            await add_lead_comment(
                s.lead_id,
                f"Перевод на {s.manager_name}: не взял трубку — ждём, когда освободится.",
            )
            return {"ok": True, "matched": True, "state": "TRANSFER_WAITING"}

        # Различаем причину недозвона ДО каскада:
        #   • connected_at пусто  → МЕНЕДЖЕР не взял трубку (клиента не набирали).
        #   • connected_at задано → менеджер взял, но КЛИЕНТ не ответил.
        manager_answered = s.connected_at is not None

        # ── КАСКАД (только если НЕ ВЗЯЛ ТРУБКУ МЕНЕДЖЕР) ─────────────
        # Менеджер не поднял → сразу пробуем следующего свободного (А→Б→В),
        # без пауз — это вопрос доступности оператора, клиента ещё не беспокоили.
        # ВАЖНО: если менеджер ВЗЯЛ, а клиент не ответил — это реальный недозвон
        # до клиента, его НЕЛЬЗЯ каскадить (иначе клиента дёргают подряд без пауз).
        # Такой случай уходит ниже на таймерный перезвон (+5/15/30).
        if not manager_answered:
            next_free = await _get_available_managers(exclude_manager_id=s.manager_id)
            if next_free:
                logger.info(
                    "[sipuni-webhook] лид %d: %s не взял трубку → каскад к следующему (%s)",
                    s.lead_id, s.manager_name, next_free[0].name,
                )
                await add_lead_comment(
                    s.lead_id,
                    f"Менеджер {s.manager_name} не взял трубку — передаём следующему "
                    f"свободному оператору.",
                )
                # Повторный запуск распределения: переберёт оставшихся свободных.
                # attempts_used сохраняем — это та же попытка дозвона, не новая.
                await process_new_lead(
                    s.lead_id, s.phone,
                    lead_name=None, lead_source=None,
                    is_autodial=True,  # не пишем дубль в working-hours/duplicate
                    received_at=datetime.utcnow(),
                )
                return {"ok": True, "matched": True, "state": "NO_ANSWER_CASCADED"}

        # Дошли сюда — значит недозвон не удалось каскадить.
        #  • manager_answered=True  → менеджер брал, КЛИЕНТ не ответил →
        #    таймерный перезвон (+5/15/30) и стадия НДЗ.
        #  • manager_answered=False → менеджер не взял и свободных больше нет →
        #    очередь ОЖИДАНИЯ (дозвонимся, как только освободится оператор).
        if manager_answered:
            logger.info(
                "[sipuni-webhook] лид %d: менеджер ответил, клиент не ответил → "
                "таймерный перезвон", s.lead_id,
            )
            await schedule_autodial(
                s.lead_id, s.phone, current_attempts=s.attempts_used,
                last_manager_sipnumber=s.manager_sipnumber,
            )
            await update_lead_status(s.lead_id, "retry")
            await update_deal_stage(s.lead_id, settings.BITRIX_STAGE_NDZ)
            await add_lead_comment(
                s.lead_id,
                "Клиент не ответил — лид поставлен в автодозвон на повтор.",
            )
            return {"ok": True, "matched": True, "state": "NO_ANSWER"}

        # Менеджер не взял трубку — клиента не набирали → очередь ожидания.
        logger.info(
            "[sipuni-webhook] лид %d: оператор не взял трубку, свободных нет → "
            "очередь ожидания", s.lead_id,
        )
        await _enqueue_waiting(
            s.lead_id, s.phone, attempts=s.attempts_used,
        )
        await update_lead_status(s.lead_id, "retry")
        await add_lead_comment(
            s.lead_id,
            "Операторы заняты/не ответили — лид в очереди ожидания, позвоним "
            "как только освободится оператор.",
        )
        return {"ok": True, "matched": True, "state": "WAITING"}


# ─────────────────────────────────────────────
# Перевод активного звонка на другого оператора
# ─────────────────────────────────────────────
async def initiate_transfer(
    requesting_manager_id: int,
    target_manager_id: int,
) -> Dict:
    """
    Перекинуть текущий звонок оператора requesting_manager_id на оператора
    target_manager_id (перевод через дозвон).

    Логика:
      1. Находим активный звонок инициатора (его CallSession).
      2. Проверяем цель: онлайн и не он сам.
      3. Закрываем сессию инициатора (TRANSFERRED), отпускаем его в «дозаполнение».
      4. Запускаем адресный дозвон клиента на целевого оператора:
         свободен → звоним сразу; занят → лид ждёт именно его.
    """
    if requesting_manager_id == target_manager_id:
        return {"ok": False, "error": "same_manager"}

    # 1. Активная сессия инициатора (самая свежая)
    now = datetime.utcnow()
    cutoff = now - timedelta(seconds=180)
    async with async_session_maker() as session:
        result = await session.execute(
            select(CallSession)
            .where(CallSession.manager_id == requesting_manager_id)
            .where(CallSession.state.in_(["CALLBACK_CREATED", "CONNECTED"]))
            .where(CallSession.started_at >= cutoff)
            .order_by(CallSession.started_at.desc())
        )
        active = result.scalars().first()

    if not active:
        return {"ok": False, "error": "no_active_call"}

    lead_id = active.lead_id
    phone = active.phone

    # 2. Цель: должна быть онлайн
    target = await _get_target_manager(target_manager_id)
    if not target:
        return {"ok": False, "error": "target_not_found"}
    if not bool(target.online):
        return {"ok": False, "error": "target_offline"}

    # 3. Закрываем сессию инициатора и отпускаем его (как после обычного звонка)
    async with async_session_maker() as session:
        s = await session.get(CallSession, active.id)
        if s:
            s.state = "TRANSFERRED"
            await session.commit()
    await _release_after_cooldown(requesting_manager_id)

    await add_lead_comment(
        lead_id,
        f"Звонок переведён на оператора {target.name} (ext. {target.sipnumber}).",
    )
    logger.info(
        "[transfer] лид %d: %s → %s (ext=%s)",
        lead_id, requesting_manager_id, target.name, target.sipnumber,
    )

    # 4. Адресный дозвон на целевого оператора (обходит дубль/рабочие часы)
    res = await process_new_lead(
        lead_id, phone,
        is_transfer=True,
        target_manager_id=target_manager_id,
        received_at=datetime.utcnow(),
    )

    status = res.get("status")
    if status == "callback_created":
        outcome = "calling"          # звоним цели прямо сейчас
    elif status == "no_managers_available":
        outcome = "queued"           # цель занята → ждёт её
    else:
        outcome = status or "unknown"

    return {
        "ok": True,
        "lead_id": lead_id,
        "target_manager_id": target_manager_id,
        "target_name": target.name,
        "outcome": outcome,
    }
# Heartbeat: воркер обновляет эту метку в начале каждой итерации цикла.
# Дашборд по ней понимает, жив ли воркер (если метка свежая — жив).
_worker_last_tick: Optional[datetime] = None


def worker_last_tick() -> Optional[datetime]:
    """Когда воркер последний раз «тикнул» (UTC). None — ещё не запускался."""
    return _worker_last_tick


async def autodial_worker() -> None:
    global _worker_last_tick
    interval = settings.AUTODIAL_POLL_INTERVAL_SECONDS
    logger.info("[autodial_worker] запущен, интервал=%ds", interval)

    while True:
        _worker_last_tick = datetime.utcnow()
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
                        # WAITING — ждут свободного менеджера (next_call_time=now,
                        #           т.е. всегда «готовы»); SCHEDULED — перезвон по
                        #           таймеру, когда пришло время.
                        AutodialQueue.state.in_(["WAITING", "SCHEDULED"]),
                        AutodialQueue.next_call_time <= now,
                    ).order_by(
                        # FIFO: сперва у кого время раньше, затем по порядку
                        # постановки в очередь (id). Так первый вставший лид
                        # уходит первому освободившемуся менеджеру.
                        AutodialQueue.next_call_time, AutodialQueue.id
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
                target_mgr = item.target_manager_id
                try:
                    res = await process_new_lead(
                        lead_id, phone,
                        lead_name=lead_name,
                        lead_source=lead_source,
                        is_autodial=True,
                        from_queue=True,
                        target_manager_id=target_mgr,
                        is_transfer=target_mgr is not None,
                    )
                    status = res.get("status")
                    if status == "callback_created":
                        # Дозвонились до менеджера — process_new_lead уже убрал
                        # лид из очереди. Ничего не делаем.
                        pass
                    elif status == "no_managers_available":
                        # Свободного менеджера нет — process_new_lead уже вернул
                        # лид в очередь ОЖИДАНИЯ (WAITING). НЕ переводим его в
                        # таймерный перезвон, иначе лид простаивал бы. Проверим
                        # снова на следующей итерации воркера (через интервал),
                        # когда, возможно, кто-то освободится.
                        pass
                    else:
                        # Менеджеры были, но callback не создался (сбой Sipuni)
                        # или иной исход → перезвон по таймеру (+5/+15/+30).
                        # Это реальная неудача попытки дозвона.
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