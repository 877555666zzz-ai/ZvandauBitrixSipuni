# app/bitrix_client.py
"""Bitrix24 client. Status_id берётся из ENV."""
import logging
from typing import Any, Dict, List, Optional

import httpx

from .config import settings

logger = logging.getLogger(__name__)

_RETRIES = 2


async def _post(url: str, payload: dict) -> dict:
    last_exc: Optional[Exception] = None
    for attempt in range(1, _RETRIES + 2):
        try:
            async with httpx.AsyncClient(timeout=settings.BITRIX_TIMEOUT_SECONDS) as client:
                r = await client.post(url, json=payload)
                r.raise_for_status()
                return r.json()
        except Exception as e:
            last_exc = e
            if attempt <= _RETRIES:
                logger.warning("[bitrix] attempt %d failed: %s", attempt, e)
    logger.error("[bitrix] all retries failed: %s", last_exc)
    raise last_exc  # type: ignore[misc]


# ─────────────────────────────────────────────
async def get_lead(lead_id: int) -> dict:
    url = settings.bitrix_base_url + "crm.lead.get.json"
    return await _post(url, {"id": lead_id})


def extract_phone(lead: dict) -> Optional[str]:
    """Достать первый телефон. Приоритет WORK→MOBILE→HOME→OTHER."""
    if not isinstance(lead, dict):
        return None
    result = lead.get("result") or {}
    phones = result.get("PHONE") or []
    if not isinstance(phones, list) or not phones:
        return None

    preferred_order = ("WORK", "MOBILE", "HOME", "OTHER", "FAX")
    by_type: Dict[str, str] = {}
    for ph in phones:
        if not isinstance(ph, dict):
            continue
        v = (ph.get("VALUE") or "").strip()
        if v:
            by_type.setdefault(ph.get("VALUE_TYPE") or "OTHER", v)

    for t in preferred_order:
        if t in by_type:
            return by_type[t]
    for ph in phones:
        if isinstance(ph, dict):
            v = (ph.get("VALUE") or "").strip()
            if v:
                return v
    return None


def extract_lead_meta(lead: dict) -> Dict[str, Optional[str]]:
    """
    Достать имя клиента и источник из результата crm.lead.get.
    Имя собирается из NAME + LAST_NAME + TITLE по убыванию приоритета.
    Источник — SOURCE_ID (это код, человекочитаемое имя надо тянуть
    отдельным crm.status.list, для MVP оставляем код).
    """
    if not isinstance(lead, dict):
        return {"name": None, "source": None}
    result = lead.get("result") or {}

    name_parts: List[str] = []
    for key in ("NAME", "SECOND_NAME", "LAST_NAME"):
        v = (result.get(key) or "").strip()
        if v:
            name_parts.append(v)
    name = " ".join(name_parts) if name_parts else (result.get("TITLE") or "").strip()
    source = result.get("SOURCE_ID") or None

    return {
        "name": name or None,
        "source": str(source) if source else None,
    }


# ─────────────────────────────────────────────
async def update_lead_status_id(lead_id: int, status_id: str) -> dict:
    url = settings.bitrix_base_url + "crm.lead.update.json"
    return await _post(url, {"id": lead_id, "fields": {"STATUS_ID": status_id}})


_STATUS_ATTR = {
    "dialing": "BITRIX_STATUS_DIALING",
    "connected": "BITRIX_STATUS_CONNECTED",
    "no_answer": "BITRIX_STATUS_NO_ANSWER",
    "retry": "BITRIX_STATUS_RETRY",
    "failed": "BITRIX_STATUS_FAILED",
}


async def update_lead_status(lead_id: int, semantic_status: str) -> Optional[dict]:
    attr = _STATUS_ATTR.get(semantic_status)
    if not attr:
        logger.warning("[bitrix] unknown semantic status: %s", semantic_status)
        return None

    status_id = getattr(settings, attr, None)
    if not status_id:
        logger.info("[bitrix] status_id для '%s' не задан в .env", semantic_status)
        return None

    try:
        return await update_lead_status_id(lead_id, status_id)
    except Exception as e:
        logger.error("[bitrix] update_lead_status(%d, %s) failed: %s",
                     lead_id, semantic_status, e)
        return None


async def add_lead_comment(lead_id: int, comment: str) -> Optional[dict]:
    url = settings.bitrix_base_url + "crm.timeline.comment.add.json"
    payload = {
        "fields": {
            "ENTITY_ID": lead_id,
            "ENTITY_TYPE": "lead",
            "COMMENT": comment,
        }
    }
    try:
        return await _post(url, payload)
    except Exception as e:
        logger.warning("[bitrix] timeline.comment.add failed (%s), fallback", e)
        try:
            url2 = settings.bitrix_base_url + "crm.lead.update.json"
            return await _post(url2, {"id": lead_id, "fields": {"COMMENTS": comment}})
        except Exception as e2:
            logger.error("[bitrix] add_lead_comment fallback failed: %s", e2)
            return None
