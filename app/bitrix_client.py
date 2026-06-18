# app/bitrix_client.py
"""Bitrix24 client. Status_id берётся из ENV."""
import asyncio
import logging
import time
from typing import Any, Dict, List, Optional

import httpx

from .config import settings

logger = logging.getLogger(__name__)

_RETRIES = 2

# ── Throttle запросов к Bitrix ───────────────────────────────
# Bitrix ограничивает частоту REST-запросов (порядка 2/сек). При пике сделок
# мы делаем много запросов (get + comment + update на каждую). Чтобы не ловить
# ошибки лимита, выдерживаем минимальный интервал между запросами.
_MIN_INTERVAL_SECONDS = 0.5  # ~2 запроса в секунду
_throttle_lock = asyncio.Lock()
_last_request_ts = 0.0


async def _throttle() -> None:
    """Подождать, чтобы не превысить частоту запросов к Bitrix."""
    global _last_request_ts
    async with _throttle_lock:
        now = time.monotonic()
        wait = _MIN_INTERVAL_SECONDS - (now - _last_request_ts)
        if wait > 0:
            await asyncio.sleep(wait)
        _last_request_ts = time.monotonic()


async def _post(url: str, payload: dict) -> dict:
    last_exc: Optional[Exception] = None
    for attempt in range(1, _RETRIES + 2):
        try:
            await _throttle()
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


# ─── Сделки (Яндекс 360, category=12) ────────
async def get_deal(deal_id: int) -> dict:
    url = settings.bitrix_base_url + "crm.deal.get.json"
    return await _post(url, {"id": deal_id})


import re

# Минимальная длина номера в цифрах, чтобы считать строку телефоном.
_MIN_PHONE_DIGITS = 10


def _digits_only(s: str) -> str:
    """Оставить только цифры (и ведущий +)."""
    if not s:
        return ""
    s = s.strip()
    plus = s.startswith("+")
    digits = re.sub(r"\D", "", s)
    return ("+" + digits) if plus else digits


def _looks_like_phone(s: str) -> Optional[str]:
    """Если строка похожа на телефон (>=10 цифр) — вернуть нормализованный, иначе None."""
    if not s or not isinstance(s, str):
        return None
    digits = re.sub(r"\D", "", s)
    if len(digits) >= _MIN_PHONE_DIGITS:
        # сохраняем как есть (с + если был), Sipuni сам разберётся
        cleaned = _digits_only(s)
        return cleaned or None
    return None


def _phone_from_field(value: Any) -> Optional[str]:
    """Вытащить телефон из значения поля Bitrix (список PHONE или строка)."""
    if isinstance(value, list):
        for ph in value:
            if isinstance(ph, dict):
                v = (ph.get("VALUE") or "").strip()
                if v:
                    return v
            elif isinstance(ph, str) and ph.strip():
                return ph.strip()
    elif isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _scan_entity_for_phone(entity: dict) -> Optional[str]:
    """Просканировать сущность (сделку/контакт/компанию) по всем вероятным местам."""
    if not isinstance(entity, dict):
        return None
    # 1) стандартные телефонные поля
    for field in ("PHONE", "UF_CRM_PHONE", "UF_PHONE"):
        v = _phone_from_field(entity.get(field))
        if v:
            return v
    # 2) любые UF-поля, в значении которых что-то похожее на телефон
    for key, value in entity.items():
        if not isinstance(key, str):
            continue
        if key.startswith("UF_") or "PHONE" in key.upper():
            if isinstance(value, (str, list)):
                cand = _phone_from_field(value) if isinstance(value, list) else value
                ph = _looks_like_phone(cand) if isinstance(cand, str) else None
                if ph:
                    return ph
    # 3) название/заголовок (TITLE, NAME) — иногда номер прямо там
    for field in ("TITLE", "NAME", "SECOND_NAME", "LAST_NAME", "COMPANY_TITLE"):
        v = entity.get(field)
        if isinstance(v, str):
            ph = _looks_like_phone(v)
            if ph:
                return ph
    return None


def extract_phone_from_deal(deal: dict) -> Optional[str]:
    """Достать телефон из самой сделки (поля + UF + название)."""
    if not isinstance(deal, dict):
        return None
    result = deal.get("result") or {}
    return _scan_entity_for_phone(result)


async def get_deal_contact_phone(deal: dict) -> Optional[str]:
    """Телефон через контакт(ы) сделки. Поддержка одного и нескольких контактов."""
    if not isinstance(deal, dict):
        return None
    result = deal.get("result") or {}
    contact_ids: List[Any] = []
    # одиночный контакт
    cid = result.get("CONTACT_ID")
    if cid and str(cid) != "0":
        contact_ids.append(cid)
    # несколько контактов (если есть)
    multi = result.get("CONTACT_IDS")
    if isinstance(multi, list):
        contact_ids.extend([c for c in multi if c and str(c) != "0"])
    for contact_id in contact_ids:
        try:
            url = settings.bitrix_base_url + "crm.contact.get.json"
            contact = await _post(url, {"id": contact_id})
            entity = (contact.get("result") or {})
            v = _scan_entity_for_phone(entity)
            if v:
                return v
        except Exception as e:
            logger.warning("[bitrix] get_deal_contact_phone failed (%s): %s", contact_id, e)
    return None


async def get_deal_company_phone(deal: dict) -> Optional[str]:
    """Телефон через компанию сделки (поле PHONE + название + UF)."""
    if not isinstance(deal, dict):
        return None
    result = deal.get("result") or {}
    company_id = result.get("COMPANY_ID")
    if not company_id or str(company_id) == "0":
        return None
    try:
        url = settings.bitrix_base_url + "crm.company.get.json"
        company = await _post(url, {"id": company_id})
        entity = (company.get("result") or {})
        # для компании TITLE — это её название, часто туда пишут номер
        v = _scan_entity_for_phone(entity)
        if v:
            return v
    except Exception as e:
        logger.warning("[bitrix] get_deal_company_phone failed: %s", e)
    return None


async def find_deal_phone(deal: dict) -> Optional[str]:
    """Всеядный поиск телефона сделки: перебирает все источники по очереди.

    Порядок: поля самой сделки → контакт(ы) → компания.
    На каждом шаге сканируются стандартные поля, UF-поля и названия.
    Возвращает первый найденный номер или None.
    """
    # 1) сама сделка (поля, UF, TITLE)
    phone = extract_phone_from_deal(deal)
    if phone:
        logger.info("[bitrix] телефон найден в самой сделке")
        return phone
    # 2) контакт(ы)
    phone = await get_deal_contact_phone(deal)
    if phone:
        logger.info("[bitrix] телефон найден в контакте сделки")
        return phone
    # 3) компания
    phone = await get_deal_company_phone(deal)
    if phone:
        logger.info("[bitrix] телефон найден в компании сделки")
        return phone
    return None


def extract_deal_meta(deal: dict) -> Dict[str, Optional[str]]:
    """Достать имя и источник из сделки."""
    if not isinstance(deal, dict):
        return {"name": None, "source": None}
    result = deal.get("result") or {}
    name = (result.get("TITLE") or "").strip() or None
    source = result.get("SOURCE_ID") or None
    return {
        "name": name,
        "source": str(source) if source else None,
    }


async def add_deal_comment(deal_id: int, comment: str) -> Optional[dict]:
    """Добавить комментарий в таймлайн сделки."""
    url = settings.bitrix_base_url + "crm.timeline.comment.add.json"
    payload = {
        "fields": {
            "ENTITY_ID": deal_id,
            "ENTITY_TYPE": "deal",
            "COMMENT": comment,
        }
    }
    try:
        return await _post(url, payload)
    except Exception as e:
        logger.error("[bitrix] add_deal_comment(%d) failed: %s", deal_id, e)
        return None


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