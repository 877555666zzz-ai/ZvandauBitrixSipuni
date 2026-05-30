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
    Распарсить вебхук Sipuni («События на АТС») в единый формат:
      {
        "sipnumber": str | None,       — внутренний номер менеджера
        "client_phone": str | None,    — номер клиента
        "talk_seconds": float | None,  — длительность разговора в секундах
        "answered": bool,              — был ли реальный ответ
        "event_finished": bool,        — это финальное событие (event=2)?
        "raw": dict,                   — оригинальное тело
      }

    Реальный формат Sipuni «События на АТС» (GET query):
      event=1  — звонок начат; event=2 — звонок завершён (финальное)
      status=ANSWER|NOANSWER|BUSY|... — итог (только в event=2)
      short_src_num — внутренний номер менеджера (напр. 205)
      dst_num / short_dst_num — номер клиента
      call_start_timestamp + timestamp — для вычисления длительности
      last_called — внутренний номер, которого реально звали

    Если формат в кабинете другой — правится только этот маппинг.
    """
    def _first(keys: List[str]) -> Optional[Any]:
        for k in keys:
            if k in body and body[k] not in (None, ""):
                return body[k]
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

    # Менеджер: предпочитаем короткий внутренний номер
    sipnumber = _first([
        "short_src_num", "last_called", "sipnumber",
        "manager", "internal", "from_internal", "src",
    ])
    # Клиент: внешний номер назначения
    client_phone = _first([
        "dst_num", "short_dst_num", "phone", "client_phone",
        "external", "to", "number", "dst",
    ])

    # Событие: 1 = начат, 2 = завершён (финальное)
    event_raw = _first(["event"])
    event_finished = str(event_raw) == "2"

    # Длительность разговора: timestamp(финал) - call_start_timestamp
    talk_seconds: Optional[float] = None
    ts_end = _first(["timestamp"])
    ts_start = _first(["call_start_timestamp"])
    try:
        if ts_end is not None and ts_start is not None:
            talk_seconds = max(0.0, float(ts_end) - float(ts_start))
    except (TypeError, ValueError):
        talk_seconds = None
    # запасной вариант — явное поле длительности, если придёт
    if talk_seconds is None:
        dur = _first(["duration", "talk_duration", "billsec", "bill_seconds"])
        try:
            talk_seconds = float(dur) if dur is not None else None
        except (TypeError, ValueError):
            talk_seconds = None

    # Факт ответа: статус ANSWER (Sipuni шлёт NOANSWER/ANSWER/BUSY/...)
    status_raw = _first(["status", "callStatus", "disposition", "state"])
    answered = False
    if isinstance(status_raw, str) and status_raw.strip().upper() in (
        "ANSWER", "ANSWERED", "TALK", "COMPLETED", "SUCCESS",
    ):
        answered = True

    return {
        "sipnumber": str(sipnumber) if sipnumber else None,
        "client_phone": str(client_phone) if client_phone else None,
        "talk_seconds": talk_seconds,
        "answered": bool(answered),
        "event_finished": event_finished,
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
