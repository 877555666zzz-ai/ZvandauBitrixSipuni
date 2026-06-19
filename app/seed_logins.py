# app/seed_logins.py
"""Разовый скрипт: проставить менеджерам логины и пароли для портала.

Запуск (локально или через Railway shell):
    python -m app.seed_logins

Логины/пароли заданы ниже. Меняй при необходимости.
"""
import asyncio

from sqlalchemy import select

from .db import async_session_maker, init_db
from .models import Manager
from .manager_portal import hash_password

# id менеджера → (логин, пароль)
CREDENTIALS = {
    14: ("aidana", "aidana123"),
    15: ("sabina", "sabina123"),
    16: ("saida", "saida123"),
}


async def main() -> None:
    await init_db()
    async with async_session_maker() as session:
        for mgr_id, (login, password) in CREDENTIALS.items():
            mgr = await session.get(Manager, mgr_id)
            if not mgr:
                print(f"  ! менеджер id={mgr_id} не найден — пропуск")
                continue
            mgr.login = login
            mgr.password_hash = hash_password(password)
            print(f"  ✓ {mgr.name} (id={mgr_id}): логин={login} пароль={password}")
        await session.commit()
    print("Готово. Менеджеры могут входить на /manager")


if __name__ == "__main__":
    asyncio.run(main())
