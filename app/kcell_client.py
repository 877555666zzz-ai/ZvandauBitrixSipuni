# app/kcell_client.py
"""
Kcell Virtual PBX — CRM REST API client.

Официальный CRM REST API Kcell (Виртуальная АТС), команды:
  makeCall  — инициировать исходящий звонок (менеджер → клиент)
  history   — получить детали звонка по call_id (длительность, статус)
  event     — Kcell шлёт нам на CRM webhook уведомления о звонке (см.
              parse_kcell_event ниже; сама выдача события идёт со стороны
              main.py, здесь только разбор тела запроса)

Общий конверт запроса/ответа — команда (cmd) + статус (status) + call_id:
  {
    "cmd": "makeCall" | "event" | "history",
    "callid": "<call id>",
    "status": "ACCEPTED" | "CANCELLED",
    "duration": <секунды, опционально>,
    ...
  }

Все callback'и работают через callid — это первичный ключ для сопоставления
звонка с CallSession, номер телефона используется только как fallback.

makeCall — асинхронный: ACCEPTED означает «Kcell принял заявку и начал
дозвон», а не «разговор состоялся». Реальный итог звонка приходит позже
через event-webhook на наш KCELL_CRM_URL.
"""
import logging
from typing import Any, Dict, List, Optional

import httpx

from .config import settings

logger = logging.getLogger(__name__)


def _headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {settings.KCELL_API_KEY}",
        "Content-Type": "application/json",
    }


def _base_url() -> str:
    return settings.KCELL_API_BASE.rstrip("/")


async def make_outbound_call(manager_sipnumber: str, client_number: str) -> dict:
    """
    Инициировать исходящий звонок через команду makeCall Kcell CRM REST API.
    Kcell сначала звонит менеджеру (manager_sipnumber), при ответе —
    соединяет с клиентом (client_number).

    Возвращает единый формат, совместимый с интерфейсом, который ожидает
    dispatcher.py (чтобы алгоритм диспетчера не менялся):
      {
        "callback_created": bool,  — заявка принята Kcell (status=ACCEPTED)
        "call_id": str | None,     — callid, для сопоставления event-webhook
        "raw": str,                — сырой текст ответа
        "data": dict | None,       — распарсенный JSON ответа
        "error": str | None,
      }
    """
    if not settings.KCELL_ENABLED:
        logger.warning("[kcell] KCELL_ENABLED=false — makeCall пропущен")
        return {
            "callback_created": False, "call_id": None,
            "raw": "", "data": None, "error": "kcell_disabled",
        }

    url = f"{_base_url()}/makeCall"
    payload: Dict[str, Any] = {
        "cmd": "makeCall",
        "from": manager_sipnumber,
        "to": client_number,
        "crmName": settings.KCELL_CRM_NAME,
    }

    try:
        async with httpx.AsyncClient(timeout=settings.KCELL_TIMEOUT_SECONDS) as client:
            r = await client.post(url, headers=_headers(), json=payload)
            r.raise_for_status()
            raw_text = r.text or ""
    except httpx.HTTPStatusError as e:
        logger.error("[kcell] makeCall HTTP %s: %s", e.response.status_code, e)
        return {
            "callback_created": False, "call_id": None,
            "raw": "", "data": None, "error": f"HTTP {e.response.status_code}",
        }
    except httpx.TimeoutException:
        logger.error("[kcell] makeCall timeout: from=%s to=%s", manager_sipnumber, client_number)
        return {
            "callback_created": False, "call_id": None,
            "raw": "", "data": None, "error": "timeout",
        }
    except Exception as e:
        logger.error("[kcell] makeCall unexpected error: %s", e)
        return {
            "callback_created": False, "call_id": None,
            "raw": "", "data": None, "error": str(e),
        }

    parsed: Optional[Dict[str, Any]] = None
    try:
        parsed = r.json()
    except Exception:
        parsed = None

    status = None
    call_id = None
    if isinstance(parsed, dict):
        status = str(parsed.get("status") or "").strip().upper()
        call_id = parsed.get("callid") or parsed.get("call_id")

    success = status == "ACCEPTED"

    logger.info(
        "[kcell] makeCall from=%s to=%s status=%s callid=%s",
        manager_sipnumber, client_number, status, call_id,
    )
    return {
        "callback_created": success,
        "call_id": str(call_id) if call_id else None,
        "raw": raw_text,
        "data": parsed,
        "error": None if success else (status or "cancelled"),
    }


async def get_call_history(call_id: str) -> Optional[Dict[str, Any]]:
    """
    Команда history — получить детали звонка по callid (итоговый статус,
    длительность разговора). Используется, когда event-webhook пришёл без
    duration или нужно перепроверить итог звонка.
    """
    if not call_id:
        return None
    url = f"{_base_url()}/history"
    payload = {"cmd": "history", "callid": call_id}
    try:
        async with httpx.AsyncClient(timeout=settings.KCELL_TIMEOUT_SECONDS) as client:
            r = await client.post(url, headers=_headers(), json=payload)
            r.raise_for_status()
            data = r.json()
            return data if isinstance(data, dict) else None
    except Exception as e:
        logger.error("[kcell] history(callid=%s) error: %s", call_id, e)
        return None


def parse_kcell_event(body: Dict[str, Any]) -> Dict[str, Any]:
    """
    Разобрать событие (cmd=event), которое Kcell шлёт на наш CRM webhook
    (KCELL_CRM_URL), в единый формат:
      {
        "call_id": str | None,         — callid, первичный ключ сопоставления
        "sipnumber": str | None,       — внутренний номер менеджера
        "client_phone": str | None,    — номер клиента
        "talk_seconds": float | None,  — длительность разговора
        "answered": bool,              — итог: ACCEPTED (разговор состоялся)
        "event_finished": bool,        — финальное событие звонка?
        "event_connected": bool,       — менеджер снял трубку (промежуточное)?
        "raw": dict,
      }

    Конверт события Kcell CRM REST API:
      cmd="event", callid=<id>, status=ACCEPTED|CANCELLED,
      duration=<секунды разговора, задаётся только в финальном событии>,
      from=<sipnumber менеджера>, to=<номер клиента>

    duration отсутствует/None → это промежуточное событие (менеджер снял
    трубку, разговор с клиентом ещё не завершён). duration задано → это
    финальное событие, итог звонка окончательный.
    """
    call_id = body.get("callid") or body.get("call_id")
    sipnumber = body.get("from") or body.get("sipnumber") or body.get("manager")
    client_phone = body.get("to") or body.get("client_phone") or body.get("phone")

    status_raw = body.get("status")
    status = str(status_raw).strip().upper() if status_raw is not None else None

    duration = body.get("duration")
    talk_seconds: Optional[float] = None
    if duration is not None:
        try:
            talk_seconds = float(duration)
        except (TypeError, ValueError):
            talk_seconds = None

    # duration присутствует (даже 0) → финальное событие звонка.
    event_finished = duration is not None
    # duration отсутствует и статус ACCEPTED → менеджер снял трубку,
    # звонок ещё идёт (промежуточное событие, аналог «соединение плеч»).
    event_connected = (duration is None) and (status == "ACCEPTED")

    answered = bool(event_finished and status == "ACCEPTED")

    return {
        "call_id": str(call_id) if call_id else None,
        "sipnumber": str(sipnumber) if sipnumber else None,
        "client_phone": str(client_phone) if client_phone else None,
        "talk_seconds": talk_seconds,
        "answered": answered,
        "event_finished": event_finished,
        "event_connected": event_connected,
        "raw": body,
    }
