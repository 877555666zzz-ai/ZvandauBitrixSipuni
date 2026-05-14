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
    Depends,
    FastAPI,
    HTTPException,
    Query,
    Request,
    status,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from sqlalchemy import desc, func, select

from .bitrix_client import extract_lead_meta, extract_phone, get_lead
from .config import settings
from .db import async_session_maker, init_db
from .dispatcher import (
    autodial_worker,
    handle_sipuni_status,
    process_new_lead,
)
from .models import AutodialQueue, CallLog, CallSession, Manager
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
    return {
        "id": m.id,
        "name": m.name,
        "sipnumber": m.sipnumber,
        "online": bool(m.online),
        "missed": int(m.missed or 0),
        "accepted_calls": int(m.accepted_calls or 0),
        "priority_score": round(float(m.priority_score or 0.5), 3),
        "status": "НА ЛИНИИ" if m.online else "НЕ АКТИВЕН",
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
                "timestamp": l.timestamp.isoformat() if l.timestamp else None,
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
            .where(AutodialQueue.state.in_(["SCHEDULED", "IN_PROGRESS"]))
            .order_by(AutodialQueue.next_call_time)
        )).scalars().all()

    return {
        "total_logs": len(logs),
        "logs": [
            {
                "id": l.id,
                "timestamp": l.timestamp.isoformat() if l.timestamp else None,
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
                "next_call_time": q.next_call_time.isoformat() if q.next_call_time else None,
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
            }
            for m in managers
        ],
    }


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


@app.post("/sipuni/webhook/status")
async def sipuni_status_webhook(
    request: Request,
    _: None = Depends(_check_sipuni_secret),
):
    """
    Принимает webhook от Sipuni со статусом звонка. Поддерживает JSON
    и form-data, потому что Sipuni может слать и так и так в зависимости
    от настроек интеграции.
    """
    body: dict = {}
    try:
        body = await request.json()
    except Exception:
        try:
            form = await request.form()
            body = {k: v for k, v in form.items()}
        except Exception:
            body = {}

    if not isinstance(body, dict):
        body = {}

    parsed = parse_sipuni_webhook(body)
    logger.info("[sipuni-webhook] %s", parsed)

    return await handle_sipuni_status(
        sipnumber=parsed.get("sipnumber"),
        client_phone=parsed.get("client_phone"),
        talk_seconds=parsed.get("talk_seconds"),
        answered=bool(parsed.get("answered")),
        raw=body,
    )


# ─── Manager personal page ──────────────────────────────────
_MANAGER_PAGE = """<!DOCTYPE html>
<html lang="ru"><head><meta charset="utf-8">
<title>AutoCall · Менеджер</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
  *{box-sizing:border-box;margin:0;padding:0}
  body{font-family:-apple-system,BlinkMacSystemFont,Segoe UI,sans-serif;background:#0f172a;color:#f1f5f9;padding:20px;min-height:100vh}
  .card{max-width:480px;margin:40px auto;background:#1e293b;border-radius:16px;padding:28px;box-shadow:0 10px 30px rgba(0,0,0,.3)}
  h1{font-size:22px;margin-bottom:4px}
  .ext{color:#94a3b8;font-size:14px;margin-bottom:24px}
  .status{padding:14px;border-radius:10px;text-align:center;font-weight:600;margin-bottom:20px;font-size:18px}
  .online{background:#16a34a;color:#fff}
  .offline{background:#475569;color:#cbd5e1}
  .btn{display:block;width:100%;padding:16px;border:none;border-radius:10px;font-size:17px;font-weight:600;cursor:pointer;margin-bottom:10px}
  .btn-on{background:#22c55e;color:#fff}
  .btn-off{background:#ef4444;color:#fff}
  .btn:active{transform:scale(.98)}
  .stats{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-top:22px}
  .stat{background:#0f172a;padding:14px;border-radius:8px;text-align:center}
  .stat-val{font-size:24px;font-weight:700}
  .stat-lbl{font-size:12px;color:#94a3b8;margin-top:4px}
  .err{background:#7f1d1d;color:#fecaca;padding:12px;border-radius:8px;margin-top:14px;font-size:14px;display:none}
  .recent{margin-top:24px}
  .recent h3{font-size:14px;color:#94a3b8;margin-bottom:10px;text-transform:uppercase;letter-spacing:.5px}
  .row{display:flex;justify-content:space-between;padding:8px 0;border-bottom:1px solid #334155;font-size:13px}
  .row:last-child{border-bottom:none}
  .row .ph{color:#cbd5e1}
  .row .st{color:#94a3b8}
  .row .st.connected,.row .st.callback_created{color:#4ade80}
  .row .st.no_answer{color:#fbbf24}
  .row .st.no_managers,.row .st.failed{color:#f87171}
</style></head>
<body>
<div class="card">
  <h1 id="name">…</h1>
  <div class="ext" id="ext">ext —</div>
  <div class="status" id="status">…</div>
  <button class="btn btn-on" id="onBtn">НА ЛИНИИ</button>
  <button class="btn btn-off" id="offBtn">СНЯТЬ С ЛИНИИ</button>
  <div class="err" id="err"></div>
  <div class="stats">
    <div class="stat"><div class="stat-val" id="cAccepted">—</div><div class="stat-lbl">Принято всего</div></div>
    <div class="stat"><div class="stat-val" id="cMissed">—</div><div class="stat-lbl">Пропусков подряд</div></div>
    <div class="stat"><div class="stat-val" id="cWeek">—</div><div class="stat-lbl">Звонков за 7 дней</div></div>
    <div class="stat"><div class="stat-val" id="cConn">—</div><div class="stat-lbl">Соединено за 7 дней</div></div>
  </div>
  <div class="recent">
    <h3>Последние звонки</h3>
    <div id="recent"></div>
  </div>
</div>
<script>
const url=new URL(location.href);
const id=url.pathname.split('/').pop();
const tok=url.searchParams.get('token')||'';
const hdr=tok?{'X-Manager-Token':tok}:{};
const q=tok?'?token='+encodeURIComponent(tok):'';

async function api(path, opts={}){
  const r=await fetch(path+q,{...opts,headers:{...hdr,'Content-Type':'application/json',...(opts.headers||{})}});
  if(!r.ok){throw new Error('HTTP '+r.status)}
  return r.json();
}
function showErr(m){const e=document.getElementById('err');e.textContent=m;e.style.display='block';setTimeout(()=>e.style.display='none',3000)}

async function refresh(){
  try{
    const s=await api('/managers/'+id+'/stats?days=7');
    const m=s.manager;
    document.getElementById('name').textContent=m.name;
    document.getElementById('ext').textContent='ext '+m.sipnumber;
    const st=document.getElementById('status');
    if(m.online){st.textContent='НА ЛИНИИ';st.className='status online'}
    else{st.textContent='НЕ АКТИВЕН';st.className='status offline'}
    document.getElementById('cAccepted').textContent=m.accepted_calls;
    document.getElementById('cMissed').textContent=m.missed;
    document.getElementById('cWeek').textContent=s.calls_total;
    document.getElementById('cConn').textContent=s.calls_connected;
    const r=document.getElementById('recent');
    r.innerHTML=s.recent_calls.slice(0,10).map(c=>{
      const t=c.timestamp?new Date(c.timestamp).toLocaleString('ru'):'';
      const name=c.lead_name||c.phone||('#'+c.lead_id);
      return `<div class="row"><span class="ph">${t} · ${name}</span><span class="st ${c.status}">${c.status}</span></div>`;
    }).join('')||'<div class="row"><span class="ph">пока нет звонков</span></div>';
  }catch(e){showErr('Ошибка: '+e.message)}
}
document.getElementById('onBtn').onclick=async()=>{try{await api('/managers/'+id+'/online',{method:'POST'});refresh()}catch(e){showErr(e.message)}};
document.getElementById('offBtn').onclick=async()=>{try{await api('/managers/'+id+'/offline',{method:'POST'});refresh()}catch(e){showErr(e.message)}};
refresh();
setInterval(refresh,15000);
</script>
</body></html>"""


@app.get("/manager/{manager_id}", response_class=HTMLResponse, include_in_schema=False)
async def manager_page(manager_id: int, request: Request):
    """
    Личная страница менеджера. Доступ:
      - если MANAGER_PAGE_TOKEN задан → нужен ?token=<TOKEN>
      - иначе открыта всем (для dev)
    """
    if settings.MANAGER_PAGE_TOKEN:
        token = (
            request.query_params.get("token")
            or request.headers.get("X-Manager-Token")
            or ""
        )
        if not secrets.compare_digest(token, settings.MANAGER_PAGE_TOKEN):
            return HTMLResponse(
                "<h1>403</h1><p>Нужен правильный ?token=...</p>",
                status_code=403,
            )

    async with async_session_maker() as session:
        mgr = await session.get(Manager, manager_id)
        if not mgr:
            return HTMLResponse("<h1>404</h1><p>Менеджер не найден</p>", status_code=404)

    return HTMLResponse(_MANAGER_PAGE)


# ─── Manager page API (без HTTP Basic, но с MANAGER_PAGE_TOKEN) ──
# Личная страница менеджера должна работать на телефонах без вводов
# логина-пароля. Поэтому она ходит в API через тот же token, что и страница.
# При установленном DASHBOARD_USER/PASSWORD основные /managers/... требуют
# Basic, но мы добавляем «обход» через X-Manager-Token / ?token=
def _manager_token_ok(request: Request) -> bool:
    if not settings.MANAGER_PAGE_TOKEN:
        return False
    tok = (
        request.query_params.get("token")
        or request.headers.get("X-Manager-Token")
        or ""
    )
    return bool(tok) and secrets.compare_digest(tok, settings.MANAGER_PAGE_TOKEN)


# Переопределяем зависимость через middleware-style hook: если Basic-auth
# отказала, но прошёл manager token — пускаем дальше для GET stats и POST online/offline.
@app.middleware("http")
async def manager_token_bypass(request: Request, call_next):
    """
    Если включён HTTP Basic и пришёл X-Manager-Token / ?token=, пускаем
    запросы к /managers/{id}/stats|online|offline без Basic-auth.
    """
    path = request.url.path
    if (
        settings.auth_enabled
        and settings.MANAGER_PAGE_TOKEN
        and _manager_token_ok(request)
        and path.startswith("/managers/")
        and any(path.endswith(s) for s in ("/stats", "/online", "/offline"))
    ):
        # Подделываем Authorization header корректным значением, чтобы
        # require_auth его пропустила.
        import base64
        cred = f"{settings.DASHBOARD_USER}:{settings.DASHBOARD_PASSWORD}".encode()
        token = base64.b64encode(cred).decode()
        new_headers = list(request.scope["headers"])
        new_headers = [h for h in new_headers if h[0] != b"authorization"]
        new_headers.append((b"authorization", f"Basic {token}".encode()))
        request.scope["headers"] = new_headers
    return await call_next(request)


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
