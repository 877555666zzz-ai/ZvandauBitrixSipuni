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


async def update_deal_stage(deal_id: int, stage_id: str) -> Optional[dict]:
    """Перевести сделку в стадию воронки (STAGE_ID, напр. 'C12:PREPARATION').

    Используется чтобы кидать недозвонов в НДЗ / НДЗ 2.
    Никогда не пробрасывает исключение — если Bitrix недоступен,
    логируем и возвращаем None, чтобы не ронять обработку звонка.
    """
    if not stage_id:
        return None
    url = settings.bitrix_base_url + "crm.deal.update.json"
    try:
        result = await _post(url, {"id": deal_id, "fields": {"STAGE_ID": stage_id}})
        logger.info("[bitrix] сделка %s → стадия %s", deal_id, stage_id)
        return result
    except Exception as e:
        logger.error("[bitrix] update_deal_stage(%s, %s) failed: %s",
                     deal_id, stage_id, e)
        return None


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

# ─── Полная карточка сделки + действия (для портала менеджера) ───
_STAGE_NAMES_CACHE: Dict[str, str] = {}


async def get_deal_card(deal_id: int) -> Dict[str, Any]:
    """Собрать всё полезное по сделке для показа менеджеру.

    Тянет саму сделку + (при наличии) контакт. Возвращает словарь с
    именем, телефоном, названием, стадией, источником, суммой, ссылкой.
    Никогда не бросает исключение — при ошибке возвращает что собралось.
    """
    card: Dict[str, Any] = {
        "deal_id": deal_id, "title": None, "phone": None, "name": None,
        "stage_id": None, "source": None, "amount": None, "currency": None,
        "assigned_by": None, "comments": None, "bitrix_url": None,
    }
    portal = (settings.BITRIX_PORTAL_URL or "").rstrip("/")
    if portal:
        card["bitrix_url"] = f"{portal}/crm/deal/details/{deal_id}/"
    try:
        deal = await get_deal(deal_id)
        result = deal.get("result") or {}
        card["title"] = (result.get("TITLE") or "").strip() or None
        card["stage_id"] = result.get("STAGE_ID")
        card["source"] = result.get("SOURCE_ID")
        card["amount"] = result.get("OPPORTUNITY")
        card["currency"] = result.get("CURRENCY_ID")
        card["assigned_by"] = result.get("ASSIGNED_BY_ID")
        card["comments"] = (result.get("COMMENTS") or "").strip() or None
        # телефон — через всеядный поиск
        card["phone"] = await find_deal_phone(deal)
        # имя из контакта, если есть
        cid = result.get("CONTACT_ID")
        if cid and str(cid) != "0":
            try:
                url = settings.bitrix_base_url + "crm.contact.get.json"
                contact = await _post(url, {"id": cid})
                c = contact.get("result") or {}
                parts = [
                    (c.get("NAME") or "").strip(),
                    (c.get("LAST_NAME") or "").strip(),
                ]
                nm = " ".join(p for p in parts if p)
                card["name"] = nm or None
            except Exception:
                pass
    except Exception as e:
        logger.error("[bitrix] get_deal_card(%s) failed: %s", deal_id, e)
    return card


async def add_deal_task(deal_id: int, title: str,
                        responsible_id: Optional[int] = None,
                        description: str = "") -> Optional[dict]:
    """Поставить задачу, привязанную к сделке."""
    url = settings.bitrix_base_url + "tasks.task.add.json"
    fields: Dict[str, Any] = {
        "TITLE": title,
        "DESCRIPTION": description,
        "UF_CRM_TASK": [f"D_{deal_id}"],  # привязка к сделке
    }
    if responsible_id:
        fields["RESPONSIBLE_ID"] = responsible_id
    try:
        return await _post(url, {"fields": fields})
    except Exception as e:
        logger.error("[bitrix] add_deal_task(%s) failed: %s", deal_id, e)
        return None


async def set_deal_title_to_phone(deal_id: int, phone: str, current_title: str = "") -> None:
    """Назвать сделку номером телефона, если у неё нет осмысленного названия.

    Многие наши сделки приходят как «Сделка #12345» или вовсе без названия —
    оператору удобнее видеть номер. Переименовываем только если текущее
    название пустое или похоже на автогенерированное («Сделка #...»).
    """
    if not phone:
        return
    title = (current_title or "").strip()
    # переименовываем только автоназвания / пустые
    looks_auto = (not title) or title.lower().startswith("сделка #") or title.lower().startswith("без названия")
    if not looks_auto:
        return
    url = settings.bitrix_base_url + "crm.deal.update.json"
    try:
        await _post(url, {"id": deal_id, "fields": {"TITLE": phone}})
        logger.info("[bitrix] сделка %s переименована в номер %s", deal_id, phone)
    except Exception as e:
        logger.error("[bitrix] set_deal_title_to_phone(%s) failed: %s", deal_id, e)


# ─── Назначение ответственного за сделку (по оператору) ──────
# Запасная таблица sip → Bitrix user ID (на случай если user.search недоступен).
# Заполнена из реальных данных портала.
_SIP_TO_BITRIX_ID = {
    "210": 350,   # Айдана Kazbekova
    "234": 346,   # Саида Сабина
    "240": 338,   # Сабина Зуфарова
}

_sip_id_cache: Dict[str, int] = {}


async def _find_bitrix_user_by_sip(sipnumber: str) -> Optional[int]:
    """Найти Bitrix user ID по внутреннему телефону (UF_PHONE_INNER).

    Сначала кэш, потом запасная таблица, потом API (user.search) через
    user-webhook. Возвращает ID или None.
    """
    if not sipnumber:
        return None
    sip = str(sipnumber).strip()
    if sip in _sip_id_cache:
        return _sip_id_cache[sip]
    # запасная таблица — мгновенно и без сети
    if sip in _SIP_TO_BITRIX_ID:
        _sip_id_cache[sip] = _SIP_TO_BITRIX_ID[sip]
        return _SIP_TO_BITRIX_ID[sip]
    # API-поиск по UF_PHONE_INNER через user-webhook
    base = settings.BITRIX_USER_WEBHOOK_URL
    if not base:
        return None
    base = base if base.endswith("/") else base + "/"
    url = base + "user.search.json"
    try:
        async with httpx.AsyncClient(timeout=settings.BITRIX_TIMEOUT_SECONDS) as client:
            r = await client.post(url, json={"UF_PHONE_INNER": sip})
            r.raise_for_status()
            data = r.json()
            users = data.get("result") or []
            if users:
                uid = int(users[0].get("ID"))
                _sip_id_cache[sip] = uid
                return uid
    except Exception as e:
        logger.warning("[bitrix] user.search по sip=%s не удался: %s", sip, e)
    return None


async def assign_deal_responsible(deal_id: int, sipnumber: str) -> bool:
    """Назначить ответственным за сделку оператора, которому попал звонок.

    Ищет Bitrix user ID по sipnumber, ставит ASSIGNED_BY_ID.
    Возвращает True если получилось. Никогда не бросает исключение.
    """
    uid = await _find_bitrix_user_by_sip(sipnumber)
    if not uid:
        logger.info("[bitrix] ответственный не назначен: нет Bitrix ID для sip=%s", sipnumber)
        return False
    # обновление через user-webhook (у него есть права), иначе основной
    base = settings.BITRIX_USER_WEBHOOK_URL or settings.bitrix_base_url
    base = base if base.endswith("/") else base + "/"
    url = base + "crm.deal.update.json"
    try:
        async with httpx.AsyncClient(timeout=settings.BITRIX_TIMEOUT_SECONDS) as client:
            r = await client.post(url, json={"id": deal_id, "fields": {"ASSIGNED_BY_ID": uid}})
            r.raise_for_status()
        logger.info("[bitrix] сделка %s → ответственный %s (sip=%s)", deal_id, uid, sipnumber)
        return True
    except Exception as e:
        logger.error("[bitrix] assign_deal_responsible(%s, sip=%s) failed: %s", deal_id, sipnumber, e)
        return False