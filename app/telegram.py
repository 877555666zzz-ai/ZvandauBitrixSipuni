# app/telegram.py
"""
Простые алерты в Telegram. Опционально: если TELEGRAM_BOT_TOKEN и
TELEGRAM_CHAT_ID не заданы — все вызовы no-op.
"""
import logging

import httpx

from .config import settings

logger = logging.getLogger(__name__)


async def send_alert(text: str) -> None:
    """Послать сообщение в чат. Никогда не пробрасывает исключение."""
    if not settings.telegram_enabled:
        return
    url = f"https://api.telegram.org/bot{settings.TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": settings.TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            await client.post(url, json=payload)
    except Exception as e:
        # Не валим основную логику из-за телеги
        logger.warning("[telegram] send_alert failed: %s", e)
