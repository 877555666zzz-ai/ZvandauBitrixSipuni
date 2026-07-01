# app/main.py
"""FastAPI приложение AutoCall."""
import asyncio
import logging
import os
import secrets
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Optional

from fastapi import (
    BackgroundTasks,
    Cookie,
    Depends,
    FastAPI,
    HTTPException,
    Query,
    Request,
    Response,
    status,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from sqlalchemy import delete, desc, func, select

from .bitrix_client import (
    add_deal_comment,
    add_lead_comment,
    add_deal_task,
    assign_deal_responsible,
    extract_deal_meta,
    extract_lead_meta,
    extract_phone,
    extract_phone_from_deal,
    find_deal_phone,
    get_deal,
    get_deal_card,
    set_deal_title_to_phone,
    get_deal_company_phone,
    get_deal_contact_phone,
    get_lead,
    update_deal_stage,
)
from .config import settings
from .db import async_session_maker, init_db
from .dispatcher import (
    autodial_worker,
    handle_sipuni_status,
    initiate_transfer,
    mark_busy_from_sipuni,
    normalize_phone,
    process_new_lead,
    worker_last_tick,
)
from .models import AutodialQueue, CallLog, CallSession, Manager
from . import manager_portal as portal
from .sipuni_client import make_outbound_call, parse_sipuni_webhook

logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ─── Auth ───────────────────────────────────────────────────
_basic = HTTPBasic(auto_error=False)


def require_auth(
    credentials: Optional[HTTPBasicCredentials] = Depends(_basic),
) -> None:
    """HTTP Basic. Если DASHBOARD_USER/PASSWORD не заданы — auth выключен."""
    if not settings.auth_enabled:
        return
    if credentials is None:
        raise HTTPException(
            status_code=401,
            detail="Authorization required",
            headers={"WWW-Authenticate": "Basic"},
        )
    user_ok = secrets.compare_digest(
        credentials.username, settings.DASHBOARD_USER or ""
    )
    pass_ok = secrets.compare_digest(
        credentials.password, settings.DASHBOARD_PASSWORD or ""
    )
    if not (user_ok and pass_ok):
        raise HTTPException(
            status_code=401,
            detail="Invalid credentials",
            headers={"WWW-Authenticate": "Basic"},
        )


# ─── Lifespan ───────────────────────────────────────────────
@asynccontextmanager
async def lifespan(_: FastAPI):
    await init_db()

    if settings.SEED_DEFAULT_MANAGERS:
        async with async_session_maker() as session:
            count = (
                await session.execute(select(func.count()).select_from(Manager))
            ).scalar_one()
            if count == 0:
                session.add_all([
                    Manager(name="Менеджер 1", sipnumber="100", online=False),
                    Manager(name="Менеджер 2", sipnumber="101", online=False),
                ])
                await session.commit()
                logger.info("[startup] seed-менеджеры")

    worker_task = asyncio.create_task(autodial_worker())
    logger.info(
        "[startup] AutoCall (env=%s, auth=%s, tg=%s, db=%s)",
        settings.ENVIRONMENT,
        "on" if settings.auth_enabled else "off",
        "on" if settings.telegram_enabled else "off",
        "postgres" if settings.is_postgres else "sqlite",
    )
    try:
        yield
    finally:
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass
        logger.info("[shutdown]")


app = FastAPI(
    title="AutoCall · Bitrix24 + Sipuni",
    version="3.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

if os.path.isdir("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")


@app.exception_handler(Exception)
async def unhandled_exception_handler(_: Request, exc: Exception):
    logger.exception("Unhandled: %s", exc)
    return JSONResponse(
        status_code=500, content={"ok": False, "error": "internal_error"}
    )


# ─── Root / health ──────────────────────────────────────────
@app.get("/", include_in_schema=False)
async def root(_: None = Depends(require_auth)):
    path = os.path.join("static", "dashboard.html")
    if os.path.isfile(path):
        return FileResponse(path)
    return JSONResponse({"ok": True, "service": "autocall"})


@app.get("/health")
async def health():
    return {
        "ok": True,
        "service": "autocall",
        "environment": settings.ENVIRONMENT,
        "time": datetime.utcnow().isoformat() + "Z",
        "version": "3.0.0",
    }


# ─── Managers ───────────────────────────────────────────────
class ManagerCreate(BaseModel):
    name: str
    sipnumber: str
    online: bool = True


class ManagerUpdate(BaseModel):
    name: Optional[str] = None
    sipnumber: Optional[str] = None
    online: Optional[bool] = None


def _mgr_dict(m: Manager) -> dict:
    paused = bool(getattr(m, "paused", False))
    on_call = bool(getattr(m, "on_call", False))
    awaiting_ready = bool(getattr(m, "awaiting_ready", False))
    if not m.online:
        status = "НЕ АКТИВЕН"
    elif on_call:
        status = "НА ЗВОНКЕ"
    elif paused:
        status = "ПАУЗА"
    elif awaiting_ready:
        status = "ЖДЁТ ГОТОВ"
    else:
        status = "НА ЛИНИИ"
    return {
        "id": m.id,
        "name": m.name,
        "sipnumber": m.sipnumber,
        "online": bool(m.online),
        "paused": paused,
        "on_call": on_call,
        "awaiting_ready": awaiting_ready,
        "missed": int(m.missed or 0),
        "accepted_calls": int(m.accepted_calls or 0),
        "priority_score": round(float(m.priority_score or 0.5), 3),
        "status": status,
    }


@app.get("/managers")
async def list_managers(_: None = Depends(require_auth)):
    async with async_session_maker() as session:
        result = await session.execute(select(Manager).order_by(Manager.id))
        return [_mgr_dict(m) for m in result.scalars().all()]


@app.post("/managers", status_code=201)
async def create_manager(data: ManagerCreate, _: None = Depends(require_auth)):
    async with async_session_maker() as session:
        mgr = Manager(
            name=data.name, sipnumber=data.sipnumber, online=data.online,
            missed=0, accepted_calls=0,
        )
        session.add(mgr)
        await session.commit()
        await session.refresh(mgr)
    logger.info("[manager] добавлен %s (ext=%s)", mgr.name, mgr.sipnumber)
    return _mgr_dict(mgr)


@app.put("/managers/{manager_id}")
async def update_manager(
    manager_id: int, data: ManagerUpdate, _: None = Depends(require_auth)
):
    async with async_session_maker() as session:
        mgr = await session.get(Manager, manager_id)
        if not mgr:
            raise HTTPException(status_code=404, detail="manager_not_found")
        if data.name is not None:
            mgr.name = data.name
        if data.sipnumber is not None:
            mgr.sipnumber = data.sipnumber
        if data.online is not None:
            mgr.online = data.online
            if data.online:
                mgr.missed = 0
        await session.commit()
        await session.refresh(mgr)
    return _mgr_dict(mgr)


@app.delete("/managers/{manager_id}")
async def delete_manager(manager_id: int, _: None = Depends(require_auth)):
    async with async_session_maker() as session:
        mgr = await session.get(Manager, manager_id)
        if not mgr:
            raise HTTPException(status_code=404, detail="manager_not_found")
        await session.delete(mgr)
        await session.commit()
    return {"ok": True, "deleted": manager_id}


@app.post("/managers/{manager_id}/online")
async def set_manager_online(manager_id: int, _: None = Depends(require_auth)):
    async with async_session_maker() as session:
        mgr = await session.get(Manager, manager_id)
        if not mgr:
            raise HTTPException(status_code=404, detail="manager_not_found")
        mgr.online = True
        mgr.missed = 0
        # «На линию» = полностью активен: снимаем ручную паузу и передышку,
        # чтобы оператор сразу был готов принимать.
        mgr.paused = False
        mgr.busy_until = None
        mgr.on_call = False
        mgr.awaiting_ready = False
        await session.commit()
        await session.refresh(mgr)
    return _mgr_dict(mgr)


@app.post("/managers/{manager_id}/offline")
async def set_manager_offline(manager_id: int, _: None = Depends(require_auth)):
    async with async_session_maker() as session:
        mgr = await session.get(Manager, manager_id)
        if not mgr:
            raise HTTPException(status_code=404, detail="manager_not_found")
        mgr.online = False
        await session.commit()
        await session.refresh(mgr)
    return _mgr_dict(mgr)


@app.get("/managers/{manager_id}/stats")
async def manager_stats(
    manager_id: int,
    days: int = Query(7, ge=1, le=365),
    _: None = Depends(require_auth),
):
    """Stats для интерфейса менеджера."""
    since = datetime.utcnow() - timedelta(days=days)
    async with async_session_maker() as session:
        mgr = await session.get(Manager, manager_id)
        if not mgr:
            raise HTTPException(status_code=404, detail="manager_not_found")

        logs = (await session.execute(
            select(CallLog)
            .where(CallLog.manager_id == manager_id, CallLog.timestamp >= since)
            .order_by(desc(CallLog.timestamp))
            .limit(200)
        )).scalars().all()

    connected = sum(1 for l in logs if l.status in ("connected", "callback_created"))
    no_answer = sum(1 for l in logs if l.status == "no_answer")

    return {
        "manager": _mgr_dict(mgr),
        "period_days": days,
        "calls_total": len(logs),
        "calls_connected": connected,
        "calls_no_answer": no_answer,
        "recent_calls": [
            {
                "timestamp": (l.timestamp.isoformat() + "Z") if l.timestamp else None,
                "lead_id": l.lead_id,
                "lead_name": l.lead_name,
                "phone": l.phone,
                "status": l.status,
                "type": l.type,
                "talk_seconds": l.talk_seconds,
            }
            for l in logs[:50]
        ],
    }


# ─── Logs + queue ───────────────────────────────────────────
@app.get("/logs")
async def get_logs(
    limit: int = Query(100, ge=1, le=1000), _: None = Depends(require_auth)
):
    async with async_session_maker() as session:
        logs = (await session.execute(
            select(CallLog).order_by(CallLog.id.desc()).limit(limit)
        )).scalars().all()

        queue = (await session.execute(
            select(AutodialQueue)
            .where(AutodialQueue.state.in_(["SCHEDULED", "IN_PROGRESS", "WAITING"]))
            .order_by(AutodialQueue.next_call_time)
        )).scalars().all()

    return {
        "total_logs": len(logs),
        "logs": [
            {
                "id": l.id,
                "timestamp": (l.timestamp.isoformat() + "Z") if l.timestamp else None,
                "lead_id": l.lead_id,
                "lead_name": l.lead_name,
                "lead_source": l.lead_source,
                "phone": l.phone,
                "type": l.type,
                "status": l.status,
                "manager_id": l.manager_id,
                "manager_name": l.manager_name,
                "reaction_seconds": l.reaction_seconds,
                "talk_seconds": l.talk_seconds,
                "message": l.message,
            }
            for l in logs
        ],
        "autodial_queue": [
            {
                "lead_id": q.lead_id,
                "lead_name": q.lead_name,
                "lead_source": q.lead_source,
                "phone": q.phone,
                "attempts": q.attempts,
                "next_call_time": (q.next_call_time.isoformat() + "Z") if q.next_call_time else None,
                "state": q.state,
            }
            for q in queue
        ],
    }


# ─── Analytics ──────────────────────────────────────────────
@app.get("/analytics")
async def get_analytics(
    days: int = Query(7, ge=1, le=365), _: None = Depends(require_auth)
):
    since = datetime.utcnow() - timedelta(days=days)
    async with async_session_maker() as session:
        logs = (await session.execute(
            select(CallLog).where(CallLog.timestamp >= since)
        )).scalars().all()

        managers = (await session.execute(
            select(Manager).order_by(Manager.id)
        )).scalars().all()

        total_queued = (await session.execute(
            select(func.count()).select_from(AutodialQueue)
        )).scalar_one()

        waiting_now = (await session.execute(
            select(func.count()).select_from(AutodialQueue)
            .where(AutodialQueue.state == "WAITING")
        )).scalar_one()

        retry_now = (await session.execute(
            select(func.count()).select_from(AutodialQueue)
            .where(AutodialQueue.state.in_(["SCHEDULED", "IN_PROGRESS"]))
        )).scalar_one()

    total = len(logs)
    connected = sum(1 for l in logs if l.status in ("connected", "callback_created"))
    real_connected = sum(1 for l in logs if l.status == "connected")
    no_answer = sum(1 for l in logs if l.status == "no_answer")
    no_managers = sum(1 for l in logs if l.status == "no_managers")
    failed = sum(1 for l in logs if l.status in ("failed", "max_attempts_reached"))
    initial_calls = sum(1 for l in logs if l.type == "initial")
    autodial_calls = sum(1 for l in logs if l.type == "autodial")
    conversion_rate = round(connected / total * 100, 1) if total else 0.0

    # Время реакции
    reactions = [l.reaction_seconds for l in logs if l.reaction_seconds is not None]
    if reactions:
        reactions_sorted = sorted(reactions)
        avg_reaction = round(sum(reactions) / len(reactions), 2)
        median_reaction = round(reactions_sorted[len(reactions_sorted) // 2], 2)
        p90_reaction = round(
            reactions_sorted[min(len(reactions_sorted) - 1,
                                 int(len(reactions_sorted) * 0.9))], 2
        )
        # По ТЗ §13: % лидов с реакцией ≤ 60 сек
        under_60 = sum(1 for r in reactions if r <= 60)
        pct_under_60 = round(under_60 / len(reactions) * 100, 1)
    else:
        avg_reaction = median_reaction = p90_reaction = pct_under_60 = 0

    # Нагрузка по часам (локальное время Алматы, UTC+5): сколько лидов
    # приходило в каждый час суток — видно пики для планирования смен.
    hourly = [0] * 24
    for l in logs:
        if l.type in ("initial", "autodial") and l.timestamp:
            local_hour = (l.timestamp + timedelta(hours=5)).hour
            hourly[local_hour] += 1

    # Конверсия по каждому менеджеру: connected vs no_answer из логов.
    per_mgr: dict = {}
    for l in logs:
        if l.manager_id is None:
            continue
        d = per_mgr.setdefault(l.manager_id, {"connected": 0, "no_answer": 0})
        if l.status == "connected":
            d["connected"] += 1
        elif l.status == "no_answer":
            d["no_answer"] += 1

    return {
        "period_days": days,
        "total_calls": total,
        "connected": connected,
        "real_connected": real_connected,  # с подтверждением от Sipuni
        "no_answer": no_answer,
        "no_managers": no_managers,
        "failed": failed,
        "initial_calls": initial_calls,
        "autodial_calls": autodial_calls,
        "conversion_rate": conversion_rate,
        "total_ever_queued": total_queued,
        "waiting_now": waiting_now,
        "retry_now": retry_now,
        "hourly_load": hourly,
        "reaction_time": {
            "avg_seconds": avg_reaction,
            "median_seconds": median_reaction,
            "p90_seconds": p90_reaction,
            "percent_under_60s": pct_under_60,
        },
        "managers": [
            {
                "id": m.id,
                "name": m.name,
                "sipnumber": m.sipnumber,
                "online": bool(m.online),
                "accepted_calls": int(m.accepted_calls or 0),
                "missed": int(m.missed or 0),
                "priority_score": round(float(m.priority_score or 0.5), 3),
                "connected": per_mgr.get(m.id, {}).get("connected", 0),
                "no_answer": per_mgr.get(m.id, {}).get("no_answer", 0),
                "conversion": (
                    round(
                        100 * per_mgr[m.id]["connected"]
                        / (per_mgr[m.id]["connected"] + per_mgr[m.id]["no_answer"])
                    )
                    if m.id in per_mgr
                    and (per_mgr[m.id]["connected"] + per_mgr[m.id]["no_answer"]) > 0
                    else 0
                ),
            }
            for m in managers
        ],
    }


# ─── Dashboard extras (Заход 1) ─────────────────────────────
@app.post("/autodial/cancel/{lead_id}")
async def cancel_autodial(lead_id: int, _: None = Depends(require_auth)):
    """Отменить/дропнуть лид: убрать из очереди (любое состояние) + закрыть
    активную сессию + написать комментарий в Bitrix. Стадию сделки НЕ трогаем.
    Нужно, например, когда в воронку случайно попал холодный лид.
    """
    removed_queue = 0
    closed_sessions = 0
    async with async_session_maker() as session:
        # убрать из очереди — в любом состоянии (WAITING/SCHEDULED/IN_PROGRESS)
        q = await session.execute(
            select(AutodialQueue).where(AutodialQueue.lead_id == lead_id)
        )
        for row in q.scalars().all():
            await session.delete(row)
            removed_queue += 1
        # закрыть незавершённые сессии этого лида, чтобы будущий Sipuni webhook
        # их не подхватил (переводим в ERROR — он нигде не матчится)
        s = await session.execute(
            select(CallSession).where(
                CallSession.lead_id == lead_id,
                CallSession.state.in_(["PENDING", "CALLBACK_CREATED"]),
            )
        )
        for sess in s.scalars().all():
            sess.state = "ERROR"
            closed_sessions += 1
        await session.commit()

    # Комментарий в Bitrix (и в лид, и в сделку) — не валим запрос если не вышло
    try:
        await add_deal_comment(lead_id, "Автодозвон отменён вручную из дашборда.")
    except Exception:
        pass
    try:
        await add_lead_comment(lead_id, "Автодозвон отменён вручную из дашборда.")
    except Exception:
        pass

    logger.info(
        "[cancel] лид %d: убрано из очереди=%d, закрыто сессий=%d",
        lead_id, removed_queue, closed_sessions,
    )
    return {
        "ok": True,
        "lead_id": lead_id,
        "removed_from_queue": removed_queue,
        "closed_sessions": closed_sessions,
    }


@app.post("/autodial/clear")
async def clear_autodial_queue(_: None = Depends(require_auth)):
    """Очистить ВСЮ очередь автодозвона одним кликом — убрать все лиды в любом
    состоянии (WAITING + SCHEDULED + IN_PROGRESS). Нужно, когда в очередь
    налетел мусор (например, холодные лиды, пока менеджеров не было на линии).

    Без комментов в Bitrix — массово это были бы десятки запросов. Просто
    убираем из очереди и закрываем незавершённые сессии.
    """
    async with async_session_maker() as session:
        # сколько было — для ответа
        cnt = (await session.execute(
            select(func.count()).select_from(AutodialQueue)
        )).scalar_one()
        # вычищаем всю очередь
        await session.execute(delete(AutodialQueue))
        # закрываем незавершённые сессии, чтобы Sipuni webhook их не подхватил
        s = await session.execute(
            select(CallSession).where(
                CallSession.state.in_(["PENDING", "CALLBACK_CREATED"])
            )
        )
        closed = 0
        for sess in s.scalars().all():
            sess.state = "ERROR"
            closed += 1
        await session.commit()

    logger.info("[clear] очередь очищена: убрано %d, закрыто сессий %d", cnt, closed)
    return {"ok": True, "removed_from_queue": int(cnt), "closed_sessions": closed}


@app.post("/autodial/retry-now/{lead_id}")
async def retry_now(lead_id: int, _: None = Depends(require_auth)):
    """«Дозвониться сейчас» — выдернуть лид из таймерного перезвона в очередь
    ОЖИДАНИЯ немедленно: ставим next_call_time=now и state=WAITING, чтобы воркер
    взял его на ближайшей итерации (≈15с), не дожидаясь +5/15/30 минут.
    """
    async with async_session_maker() as session:
        result = await session.execute(
            select(AutodialQueue).where(AutodialQueue.lead_id == lead_id)
        )
        item = result.scalar_one_or_none()
        if not item:
            raise HTTPException(status_code=404, detail="lead_not_in_queue")
        item.state = "WAITING"
        item.next_call_time = datetime.utcnow()
        await session.commit()
    logger.info("[retry-now] лид %d → очередь ожидания (немедленно)", lead_id)
    return {"ok": True, "lead_id": lead_id, "state": "WAITING"}


@app.get("/live/active-calls")
async def live_active_calls(_: None = Depends(require_auth)):
    """Кто сейчас на звонке. Берём свежие сессии в работе (CALLBACK_CREATED —
    система дозвонилась оператору, идёт звонок). Длительность — от момента
    callback. Старше 5 минут не показываем (значит звонок давно завершился).
    """
    now = datetime.utcnow()
    cutoff = now - timedelta(minutes=5)
    async with async_session_maker() as session:
        rows = (await session.execute(
            select(CallSession)
            .where(CallSession.state.in_(["CALLBACK_CREATED", "CONNECTED"]))
            .where(CallSession.started_at >= cutoff)
            .order_by(CallSession.started_at.desc())
        )).scalars().all()
    out = []
    for s in rows:
        base = s.callback_at or s.connected_at or s.started_at
        dur = int((now - base).total_seconds()) if base else None
        out.append({
            "lead_id": s.lead_id,
            "phone": s.phone,
            "manager_id": s.manager_id,
            "manager_name": s.manager_name,
            "state": s.state,
            "connected": s.state == "CONNECTED",
            "duration_seconds": dur,
        })
    return {"count": len(out), "active": out}


@app.post("/managers/online-all")
async def all_managers_online(_: None = Depends(require_auth)):
    """Поставить всех менеджеров на линию (начало смены — одним кликом)."""
    async with async_session_maker() as session:
        rows = (await session.execute(select(Manager))).scalars().all()
        changed = 0
        for m in rows:
            if not m.online:
                changed += 1
            m.online = True
            m.missed = 0
            m.paused = False
            m.busy_until = None
            m.on_call = False
            m.awaiting_ready = False
        await session.commit()
    logger.info("[bulk] все на линии (изменено %d из %d)", changed, len(rows))
    return {"ok": True, "set_online": changed, "total": len(rows)}


@app.post("/managers/offline-all")
async def all_managers_offline(_: None = Depends(require_auth)):
    """Снять всех менеджеров с линии (конец смены — одним кликом)."""
    async with async_session_maker() as session:
        rows = (await session.execute(select(Manager))).scalars().all()
        changed = 0
        for m in rows:
            if m.online:
                changed += 1
            m.online = False
        await session.commit()
    logger.info("[bulk] все сняты с линии (изменено %d из %d)", changed, len(rows))
    return {"ok": True, "set_offline": changed, "total": len(rows)}


@app.get("/system/status")
async def system_status(_: None = Depends(require_auth)):
    """Индикатор «система жива»: жив ли воркер (по heartbeat), когда был
    последний лид, сколько ждут/в перезвоне, сколько на линии.
    """
    now = datetime.utcnow()
    tick = worker_last_tick()
    interval = settings.AUTODIAL_POLL_INTERVAL_SECONDS
    # Воркер считаем живым, если тикал не позже 3 интервалов назад.
    worker_alive = bool(tick and (now - tick).total_seconds() < interval * 3 + 10)

    async with async_session_maker() as session:
        last_lead = (await session.execute(
            select(CallLog)
            .where(CallLog.type.in_(["initial", "autodial"]))
            .order_by(desc(CallLog.timestamp)).limit(1)
        )).scalars().first()
        online_cnt = (await session.execute(
            select(func.count()).select_from(Manager).where(Manager.online.is_(True))
        )).scalar_one()
        waiting_cnt = (await session.execute(
            select(func.count()).select_from(AutodialQueue)
            .where(AutodialQueue.state == "WAITING")
        )).scalar_one()
        retry_cnt = (await session.execute(
            select(func.count()).select_from(AutodialQueue)
            .where(AutodialQueue.state.in_(["SCHEDULED", "IN_PROGRESS"]))
        )).scalar_one()

    last_ts = last_lead.timestamp if last_lead else None
    return {
        "server_time": now.isoformat() + "Z",
        "worker_alive": worker_alive,
        "worker_last_tick": (tick.isoformat() + "Z") if tick else None,
        "worker_interval_seconds": interval,
        "last_lead_at": (last_ts.isoformat() + "Z") if last_ts else None,
        "seconds_since_last_lead": (
            int((now - last_ts).total_seconds()) if last_ts else None
        ),
        "managers_online": online_cnt,
        "waiting": waiting_cnt,
        "retry_scheduled": retry_cnt,
    }


@app.get("/logs/export.csv")
async def export_logs_csv(
    days: int = Query(30, ge=1, le=365), _: None = Depends(require_auth)
):
    """Экспорт журнала звонков в CSV за период (для отчётности)."""
    import csv
    import io

    since = datetime.utcnow() - timedelta(days=days)
    async with async_session_maker() as session:
        logs = (await session.execute(
            select(CallLog)
            .where(CallLog.timestamp >= since)
            .order_by(desc(CallLog.timestamp))
        )).scalars().all()

    buf = io.StringIO()
    buf.write("\ufeff")  # BOM — чтобы Excel корректно открыл кириллицу
    w = csv.writer(buf)
    w.writerow([
        "timestamp_utc", "lead_id", "phone", "lead_name", "type", "status",
        "manager_name", "reaction_seconds", "talk_seconds", "message",
    ])
    for l in logs:
        w.writerow([
            (l.timestamp.isoformat() + "Z") if l.timestamp else "",
            l.lead_id, l.phone, l.lead_name or "", l.type, l.status,
            l.manager_name or "",
            l.reaction_seconds if l.reaction_seconds is not None else "",
            l.talk_seconds if l.talk_seconds is not None else "",
            (l.message or "").replace("\n", " ").replace("\r", " "),
        ])

    return Response(
        content=buf.getvalue(),
        media_type="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": f'attachment; filename="autocall_logs_{days}d.csv"'
        },
    )


# ─── Bitrix webhook ─────────────────────────────────────────
async def _check_bitrix_secret(request: Request) -> None:
    if not settings.WEBHOOK_SECRET:
        return
    incoming = (
        request.query_params.get("secret")
        or request.headers.get("X-Webhook-Secret")
        or ""
    )
    if not secrets.compare_digest(incoming, settings.WEBHOOK_SECRET):
        raise HTTPException(status_code=403, detail="invalid_webhook_secret")


@app.post("/bitrix/webhook/lead")
async def bitrix_lead_webhook(
    request: Request,
    background_tasks: BackgroundTasks,
    _: None = Depends(_check_bitrix_secret),
):
    received_at = datetime.utcnow()
    lead_id: Optional[int] = None

    try:
        form = await request.form()
        raw = (
            form.get("data[FIELDS][ID]")
            or form.get("FIELDS[ID]")
            or form.get("lead_id")
        )
        if raw:
            lead_id = int(str(raw))
    except Exception:
        pass

    if lead_id is None:
        try:
            body = await request.json()
            if isinstance(body, dict):
                fields = (body.get("data") or {}).get("FIELDS") or {}
                raw = fields.get("ID") or body.get("lead_id")
                if raw:
                    lead_id = int(str(raw))
        except Exception:
            pass

    if lead_id is None:
        return JSONResponse(
            status_code=400, content={"ok": False, "error": "lead_id_not_found"}
        )

    logger.info("[webhook] лид #%d", lead_id)

    async def _process(l_id: int, ts: datetime) -> None:
        try:
            lead = await get_lead(l_id)
        except Exception as e:
            logger.error("[webhook] get_lead(%d) failed: %s", l_id, e)
            return

        phone = extract_phone(lead)
        if not phone:
            logger.warning("[webhook] лид #%d: нет телефона", l_id)
            return

        meta = extract_lead_meta(lead)
        await process_new_lead(
            l_id, phone,
            lead_name=meta.get("name"),
            lead_source=meta.get("source"),
            received_at=ts,
        )

    background_tasks.add_task(_process, lead_id, received_at)
    return {"ok": True, "lead_id": lead_id, "queued": True}


# ─── Webhook для сделок (Яндекс 360, category=12) ──────────────────────────
@app.post("/bitrix/webhook/deal")
async def bitrix_deal_webhook(
    request: Request,
    background_tasks: BackgroundTasks,
    _: None = Depends(_check_bitrix_secret),
):
    """
    Принимает новые сделки из воронки «Яндекс 360» (category=12).
    Триггер: сделка создана или попала в стадию «Тёплые лиды» (C12:NEW).
    Стадии НЕ двигаем — только звоним и пишем комментарии.
    """
    received_at = datetime.utcnow()
    deal_id: Optional[int] = None

    # Bitrix шлёт form-data или JSON — пробуем оба варианта
    try:
        form = await request.form()
        raw = (
            form.get("data[FIELDS][ID]")
            or form.get("FIELDS[ID]")
            or form.get("deal_id")
        )
        if raw:
            deal_id = int(str(raw))
    except Exception:
        pass

    if deal_id is None:
        try:
            body = await request.json()
            if isinstance(body, dict):
                fields = (body.get("data") or {}).get("FIELDS") or {}
                raw = fields.get("ID") or body.get("deal_id")
                if raw:
                    deal_id = int(str(raw))
        except Exception:
            pass

    if deal_id is None:
        return JSONResponse(
            status_code=400, content={"ok": False, "error": "deal_id_not_found"}
        )

    logger.info("[webhook-deal] сделка #%d", deal_id)

    async def _process_deal(d_id: int, ts: datetime) -> None:
        try:
            deal = await get_deal(d_id)
        except Exception as e:
            logger.error("[webhook-deal] get_deal(%d) failed: %s", d_id, e)
            return

        result = deal.get("result") or {}

        # Фильтр: только воронка Яндекс 360 (category_id=12)
        category_id = str(result.get("CATEGORY_ID") or "")
        if category_id != "12":
            logger.info(
                "[webhook-deal] сделка #%d: category=%s, не наша воронка — игнор",
                d_id, category_id,
            )
            return

        # Фильтр: только стадия «Тёплые лиды»
        stage = result.get("STAGE_ID") or ""
        if stage != "C12:NEW":
            logger.info(
                "[webhook-deal] сделка #%d: стадия=%s, не C12:NEW — игнор",
                d_id, stage,
            )
            return

        # Телефон: всеядный поиск — сделка → контакт(ы) → компания (поля, UF, названия)
        phone = await find_deal_phone(deal)
        if not phone:
            logger.warning("[webhook-deal] сделка #%d: нет телефона", d_id)
            await add_deal_comment(
                d_id,
                "Автодозвон: не удалось запустить — в сделке нет номера телефона.",
            )
            return

        meta = extract_deal_meta(deal)
        logger.info(
            "[webhook-deal] сделка #%d | телефон=%s | название=%s",
            d_id, phone, meta.get("name"),
        )

        # Если у сделки нет осмысленного названия — назвать её номером
        # (оператору удобнее видеть номер, а не «Сделка #...»).
        await set_deal_title_to_phone(d_id, phone, meta.get("name") or "")

        # Комментарий «взяли в работу»
        await add_deal_comment(
            d_id,
            f"Автодозвон: сделка взята в работу, запускаем звонок.",
        )

        # Запускаем ту же логику что и для лидов
        await process_new_lead(
            d_id, phone,
            lead_name=meta.get("name"),
            lead_source=meta.get("source"),
            received_at=ts,
        )

    background_tasks.add_task(_process_deal, deal_id, received_at)
    return {"ok": True, "deal_id": deal_id, "queued": True}


# ─── Sipuni status webhook ──────────────────────────────────
async def _check_sipuni_secret(request: Request) -> None:
    if not settings.SIPUNI_WEBHOOK_SECRET:
        return
    incoming = (
        request.query_params.get("secret")
        or request.headers.get("X-Sipuni-Secret")
        or ""
    )
    if not secrets.compare_digest(incoming, settings.SIPUNI_WEBHOOK_SECRET):
        raise HTTPException(status_code=403, detail="invalid_sipuni_secret")


@app.api_route("/sipuni/webhook/status", methods=["GET", "POST"])
async def sipuni_status_webhook(
    request: Request,
    _: None = Depends(_check_sipuni_secret),
):
    """
    Принимает webhook от Sipuni со статусом звонка. Поддерживает GET и POST,
    JSON и form-data — Sipuni («События на АТС») шлёт GET с query-параметрами,
    но оставляем POST/JSON/form на случай других схем интеграции.
    """
    body: dict = {}
    # 1) query-параметры (Sipuni "События на АТС" шлёт всё в URL через GET)
    for k, v in request.query_params.items():
        body[k] = v
    # 2) тело запроса, если есть (POST JSON или form) — дополняет/перекрывает
    try:
        json_body = await request.json()
        if isinstance(json_body, dict):
            body.update(json_body)
    except Exception:
        try:
            form = await request.form()
            for k, v in form.items():
                body[k] = v
        except Exception:
            pass

    if not isinstance(body, dict):
        body = {}

    parsed = parse_sipuni_webhook(body)
    logger.info("[sipuni-webhook] %s", parsed)

    # По ЛЮБОМУ звонку оператора (наш, чужой, входящий, из другой воронки)
    # обновляем его занятость — чтобы автодозвон не слал звонок поверх
    # разговора. Срабатывает и на начало, и на завершение события.
    await mark_busy_from_sipuni(
        sipnumber=parsed.get("sipnumber"),
        event_finished=bool(parsed.get("event_finished")),
    )

    # event=3 — оператор реально взял трубку (соединение плеч). Приходит
    # РАНЬШЕ финала. Назначаем ответственного сразу, чтобы оператор мог
    # открыть карточку прямо во время разговора, не дожидаясь конца.
    if parsed.get("event_connected"):
        sip = parsed.get("sipnumber")
        phone = parsed.get("client_phone")
        if sip and phone:
            try:
                async with async_session_maker() as session:
                    norm = normalize_phone(phone)
                    result = await session.execute(
                        select(CallSession).where(
                            CallSession.manager_sipnumber == str(sip),
                            CallSession.state.in_(["CALLBACK_CREATED", "CONNECTED"]),
                        ).order_by(CallSession.started_at.desc())
                    )
                    for s in result.scalars().all():
                        if normalize_phone(s.phone) == norm:
                            # Помечаем, что менеджер реально взял трубку — это
                            # сигнал для handle_sipuni_status: если потом клиент
                            # не ответит, это недозвон ДО КЛИЕНТА (таймерный
                            # перезвон), а не «менеджер не взял» (очередь ожидания).
                            if s.connected_at is None:
                                s.connected_at = datetime.utcnow()
                                await session.commit()
                            await assign_deal_responsible(s.lead_id, str(sip))
                            logger.info(
                                "[sipuni-webhook] лид %d: оператор взял трубку → "
                                "назначен ответственным (sip=%s)", s.lead_id, sip,
                            )
                            break
            except Exception as e:
                logger.warning("[sipuni-webhook] event=3 assign failed: %s", e)
        return {"ok": True, "event": "connected"}

    # Sipuni шлёт event=1 (звонок начат) и event=2 (завершён).
    # Реагируем только на финальное событие, начало просто подтверждаем.
    if not parsed.get("event_finished"):
        return {"ok": True, "ignored": "not_final_event"}

    return await handle_sipuni_status(
        sipnumber=parsed.get("sipnumber"),
        client_phone=parsed.get("client_phone"),
        talk_seconds=parsed.get("talk_seconds"),
        answered=bool(parsed.get("answered")),
        raw=body,
    )


@app.get("/manager", response_class=HTMLResponse, include_in_schema=False)
async def manager_portal_page():
    """Единая страница портала: вход → рабочий экран."""
    path = os.path.join("static", "manager.html")
    if os.path.exists(path):
        return FileResponse(path)
    return HTMLResponse("<h1>manager.html не найден</h1>", status_code=500)


class PortalLogin(BaseModel):
    login: str
    password: str


def _mgr_public(m: Manager) -> dict:
    paused = bool(getattr(m, "paused", False))
    on_call = bool(getattr(m, "on_call", False))
    awaiting_ready = bool(getattr(m, "awaiting_ready", False))
    return {"id": m.id, "name": m.name, "sipnumber": m.sipnumber,
            "online": bool(m.online),
            "paused": paused,
            "on_call": on_call,
            "awaiting_ready": awaiting_ready,
            # «готов» = на линии, не на паузе, не ждём «Готов»
            # (on_call здесь НЕ блокирует ready, чтобы кнопка работала и во
            # время разговора — оператор может заранее нажать «Готов»).
            "ready": bool(m.online) and not paused and not awaiting_ready}


@app.post("/manager/api/login", include_in_schema=False)
async def portal_login(data: PortalLogin, response: Response):
    mgr = await portal.authenticate(data.login, data.password)
    if not mgr:
        raise HTTPException(status_code=401, detail="bad credentials")
    token = await portal.create_session(mgr.id)
    response.set_cookie(
        "mgr_session", token, httponly=True, samesite="lax",
        max_age=portal.SESSION_TTL_HOURS * 3600,
    )
    return _mgr_public(mgr)


@app.post("/manager/api/logout", include_in_schema=False)
async def portal_logout(response: Response,
                        mgr_session: Optional[str] = Cookie(default=None)):
    await portal.destroy_session(mgr_session)
    response.delete_cookie("mgr_session")
    return {"ok": True}


@app.get("/manager/api/me", include_in_schema=False)
async def portal_me(mgr_session: Optional[str] = Cookie(default=None)):
    mgr = await portal.get_session_manager(mgr_session)
    if not mgr:
        raise HTTPException(status_code=401, detail="no session")
    return _mgr_public(mgr)


@app.get("/manager/api/current-call", include_in_schema=False)
async def portal_current_call(mgr_session: Optional[str] = Cookie(default=None)):
    mgr = await portal.get_session_manager(mgr_session)
    if not mgr:
        raise HTTPException(status_code=401, detail="no session")
    call = await portal.get_current_call(mgr.id)
    return call or {}


@app.get("/manager/api/colleagues", include_in_schema=False)
async def portal_colleagues(mgr_session: Optional[str] = Cookie(default=None)):
    """Онлайн-коллеги (кроме себя) для перевода звонка — со статусом free/busy."""
    mgr = await portal.get_session_manager(mgr_session)
    if not mgr:
        raise HTTPException(status_code=401, detail="no session")
    return await portal.get_colleagues(mgr.id)


class PortalTransfer(BaseModel):
    target_manager_id: int


@app.post("/manager/api/transfer", include_in_schema=False)
async def portal_transfer(data: PortalTransfer,
                          mgr_session: Optional[str] = Cookie(default=None)):
    """Перекинуть текущий звонок оператора на выбранного онлайн-коллегу."""
    mgr = await portal.get_session_manager(mgr_session)
    if not mgr:
        raise HTTPException(status_code=401, detail="no session")
    res = await initiate_transfer(mgr.id, data.target_manager_id)
    if not res.get("ok"):
        # Понятные сообщения об ошибке для интерфейса
        err = res.get("error")
        msg = {
            "same_manager": "Нельзя перевести самому себе",
            "no_active_call": "Нет активного звонка для перевода",
            "target_not_found": "Оператор не найден",
            "target_offline": "Оператор не на линии",
        }.get(err, "Не удалось перевести звонок")
        raise HTTPException(status_code=400, detail=msg)
    return res


@app.post("/manager/api/ready-now", include_in_schema=False)
async def portal_ready_now(mgr_session: Optional[str] = Cookie(default=None)):
    """«Готов принимать звонки» — оператор закончил заполнять карточку и готов
    к следующему. Снимает флаг ожидания, on_call и страховку busy_until.
    Паузу НЕ трогает (если на паузе — останется на паузе)."""
    mgr = await portal.get_session_manager(mgr_session)
    if not mgr:
        raise HTTPException(status_code=401, detail="no session")
    async with async_session_maker() as session:
        m = await session.get(Manager, mgr.id)
        if m:
            m.busy_until = None
            m.on_call = False
            m.awaiting_ready = False
            await session.commit()
    logger.info("[ready] оператор %s готов принимать звонки", mgr.name)
    return {"ok": True, "ready": True, "awaiting_ready": False}


@app.post("/manager/api/pause", include_in_schema=False)
async def portal_pause(mgr_session: Optional[str] = Cookie(default=None)):
    """«Пауза» — оператор отошёл. Бессрочно: звонки не идут, пока не нажмёт
    «Возобновить»."""
    mgr = await portal.get_session_manager(mgr_session)
    if not mgr:
        raise HTTPException(status_code=401, detail="no session")
    async with async_session_maker() as session:
        m = await session.get(Manager, mgr.id)
        if m:
            m.paused = True
            await session.commit()
    logger.info("[pause] оператор %s ушёл на паузу", mgr.name)
    return {"ok": True, "paused": True}


@app.post("/manager/api/resume", include_in_schema=False)
async def portal_resume(mgr_session: Optional[str] = Cookie(default=None)):
    """«Возобновить» — снять паузу. Если оператор ещё «ждёт Готов» после
    предыдущего звонка — этот флаг останется (он отдельный)."""
    mgr = await portal.get_session_manager(mgr_session)
    if not mgr:
        raise HTTPException(status_code=401, detail="no session")
    async with async_session_maker() as session:
        m = await session.get(Manager, mgr.id)
        if m:
            m.paused = False
            await session.commit()
    logger.info("[pause] оператор %s снял паузу", mgr.name)
    return {"ok": True, "paused": False}


@app.get("/manager/api/card/{lead_id}", include_in_schema=False)
async def portal_card(lead_id: int,
                      mgr_session: Optional[str] = Cookie(default=None)):
    mgr = await portal.get_session_manager(mgr_session)
    if not mgr:
        raise HTTPException(status_code=401, detail="no session")
    return await get_deal_card(lead_id)


@app.get("/manager/api/my-leads", include_in_schema=False)
async def portal_my_leads(mgr_session: Optional[str] = Cookie(default=None)):
    mgr = await portal.get_session_manager(mgr_session)
    if not mgr:
        raise HTTPException(status_code=401, detail="no session")
    return await portal.get_my_leads(mgr.id)


@app.get("/manager/api/my-stats", include_in_schema=False)
async def portal_my_stats(mgr_session: Optional[str] = Cookie(default=None)):
    mgr = await portal.get_session_manager(mgr_session)
    if not mgr:
        raise HTTPException(status_code=401, detail="no session")
    return await portal.get_my_stats(mgr.id)


class PortalStage(BaseModel):
    lead_id: int
    stage_id: str


class PortalComment(BaseModel):
    lead_id: int
    text: str


class PortalTask(BaseModel):
    lead_id: int
    title: str


@app.post("/manager/api/action/stage", include_in_schema=False)
async def portal_action_stage(data: PortalStage,
                              mgr_session: Optional[str] = Cookie(default=None)):
    mgr = await portal.get_session_manager(mgr_session)
    if not mgr:
        raise HTTPException(status_code=401, detail="no session")
    res = await update_deal_stage(data.lead_id, data.stage_id)
    await add_deal_comment(
        data.lead_id, f"Оператор {mgr.name} сменил стадию вручную."
    )
    return {"ok": res is not None}


@app.post("/manager/api/action/comment", include_in_schema=False)
async def portal_action_comment(data: PortalComment,
                                mgr_session: Optional[str] = Cookie(default=None)):
    mgr = await portal.get_session_manager(mgr_session)
    if not mgr:
        raise HTTPException(status_code=401, detail="no session")
    res = await add_deal_comment(
        data.lead_id, f"[{mgr.name}] {data.text}"
    )
    return {"ok": res is not None}


@app.post("/manager/api/action/task", include_in_schema=False)
async def portal_action_task(data: PortalTask,
                             mgr_session: Optional[str] = Cookie(default=None)):
    mgr = await portal.get_session_manager(mgr_session)
    if not mgr:
        raise HTTPException(status_code=401, detail="no session")
    res = await add_deal_task(data.lead_id, data.title)
    return {"ok": res is not None}


# ─── Test endpoints ─────────────────────────────────────────
if settings.ENABLE_TEST_ENDPOINTS:
    @app.get("/test/sipuni_call")
    async def test_sipuni_call(
        manager_id: int, client_phone: str, _: None = Depends(require_auth)
    ):
        async with async_session_maker() as session:
            mgr = await session.get(Manager, manager_id)
            if not mgr:
                raise HTTPException(status_code=404, detail="manager_not_found")
            result = await make_outbound_call(mgr.sipnumber, client_phone)
        return {"manager": _mgr_dict(mgr), "sipuni_response": result}

    @app.post("/test/lead")
    async def test_lead(
        lead_id: int,
        client_phone: str,
        background_tasks: BackgroundTasks,
        _: None = Depends(require_auth),
    ):
        background_tasks.add_task(process_new_lead, lead_id, client_phone)
        return {"ok": True, "lead_id": lead_id, "phone": client_phone, "queued": True}

    logger.info("[startup] /test endpoints ON")