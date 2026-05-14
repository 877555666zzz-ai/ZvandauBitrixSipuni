# app/sipuni_client.py
"""
Sipuni client.

callback/call_number — асинхронный, возвращает success на момент принятия
заявки. Реальный статус приходит через webhook (см. parse_sipuni_webhook).
"""
import hashlib
import logging
from typing import Any, Dict, List, Optional

import httpx

from .config import settings

logger = logging.getLogger(__name__)


def _make_hash(params_order: List[str], params: Dict[str, Any]) -> str:
    values = [str(params.get(name, "")) for name in params_order]
    values.append(settings.SIPUNI_SECRET)
    return hashlib.md5("+".join(values).encode("utf-8")).hexdigest()


async def make_outbound_call(manager_sipnumber: str, client_number: str) -> dict:
    """
    Создать callback. reverse=0 = Sipuni сначала звонит менеджеру.
    Возвращает callback_created / raw / data / error.
    """
    url = f"{settings.SIPUNI_API_BASE.rstrip('/')}/callback/call_number"

    params: Dict[str, Any] = {
        "user": settings.SIPUNI_USER,
        "phone": client_number,
        "sipnumber": manager_sipnumber,
        "reverse": 0,
        "antiaon": 0,
    }
    params["hash"] = _make_hash(
        ["antiaon", "phone", "reverse", "sipnumber", "user"], params
    )

    try:
        async with httpx.AsyncClient(timeout=settings.SIPUNI_TIMEOUT_SECONDS) as client:
            r = await client.post(url, data=params)
            r.raise_for_status()
            raw_text = r.text or ""
    except httpx.HTTPStatusError as e:
        logger.error("[sipuni] HTTP %s: %s", e.response.status_code, e)
        return {"callback_created": False, "raw": "", "data": None,
                "error": f"HTTP {e.response.status_code}"}
    except httpx.TimeoutException:
        logger.error("[sipuni] timeout calling sipnumber=%s", manager_sipnumber)
        return {"callback_created": False, "raw": "", "data": None, "error": "timeout"}
    except Exception as e:
        logger.error("[sipuni] unexpected error: %s", e)
        return {"callback_created": False, "raw": "", "data": None, "error": str(e)}

    parsed: Optional[Dict[str, Any]] = None
    try:
        parsed = r.json()
    except Exception:
        parsed = None

    success = False
    if isinstance(parsed, dict):
        success = bool(
            parsed.get("success") or parsed.get("result") in (1, "1", True)
        )
    if not success and raw_text.strip() == "1":
        success = True

    logger.info(
        "[sipuni] callback sipnumber=%s client=%s accepted=%s raw=%r",
        manager_sipnumber, client_number, success, raw_text[:200],
    )
    return {
        "callback_created": success,
        "raw": raw_text,
        "data": parsed,
        "error": None if success else "callback_rejected",
    }


def parse_sipuni_webhook(body: Dict[str, Any]) -> Dict[str, Any]:
    """
    Распарсить вебхук Sipuni со статусом звонка в единый формат:
      {
        "sipnumber": str | None,       — внутренний номер менеджера
        "client_phone": str | None,    — номер клиента
        "talk_seconds": float | None,  — длительность разговора в секундах
        "answered": bool,              — был ли реальный ответ
        "raw": dict,                   — оригинальное тело
      }

    Sipuni шлёт разные форматы (event 1.6, 1.7, через «Функции» или нативные
    webhooks). Покрываем самые частые ключи. Если в твоём кабинете формат
    другой — поправь маппинг тут, остальной код не трогая.
    """
    def _first(keys: List[str]) -> Optional[Any]:
        for k in keys:
            if k in body and body[k] not in (None, ""):
                return body[k]
            # пробуем nested ключ
            if "." in k:
                cur: Any = body
                for part in k.split("."):
                    if isinstance(cur, dict) and part in cur:
                        cur = cur[part]
                    else:
                        cur = None
                        break
                if cur not in (None, ""):
                    return cur
        return None

    sipnumber = _first(["sipnumber", "manager", "internal", "from_internal", "src"])
    client_phone = _first(["phone", "client_phone", "external", "to", "number", "dst"])

    duration_raw = _first(["duration", "talk_duration", "bill_seconds",
                           "billsec", "talkSeconds"])
    talk_seconds: Optional[float]
    try:
        talk_seconds = float(duration_raw) if duration_raw is not None else None
    except (TypeError, ValueError):
        talk_seconds = None

    # Sipuni шлёт разные индикаторы факта разговора
    status_raw = _first(["status", "callStatus", "disposition", "state"])
    answered = False
    if talk_seconds and talk_seconds >= settings.MIN_TALK_DURATION_SECONDS:
        answered = True
    if isinstance(status_raw, str) and status_raw.lower() in (
        "answered", "answer", "talk", "completed", "success", "1"
    ):
        answered = True

    return {
        "sipnumber": str(sipnumber) if sipnumber else None,
        "client_phone": str(client_phone) if client_phone else None,
        "talk_seconds": talk_seconds,
        "answered": bool(answered),
        "raw": body,
    }


async def get_operators_status() -> str:
    url = f"{settings.SIPUNI_API_BASE.rstrip('/')}/statistic/operators"
    params = {"user": settings.SIPUNI_USER}
    params["hash"] = _make_hash(["user"], params)
    try:
        async with httpx.AsyncClient(timeout=settings.SIPUNI_TIMEOUT_SECONDS) as client:
            r = await client.post(url, data=params)
            r.raise_for_status()
            return r.text
    except Exception as e:
        logger.error("[sipuni] get_operators_status error: %s", e)
        return ""
