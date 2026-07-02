# app/reset_dashboard.py
"""Сброс дашборда к чистому листу перед запуском/после теста.

Запуск (Railway → Shell, либо локально):
    python -m app.reset_dashboard

Что делает:
  • удаляет ВСЮ историю звонков (call_logs)  → пустеет «История звонков» и аналитика
  • удаляет все сессии звонков (call_sessions)
  • очищает очередь автодозвона (autodial_queue)
  • снимает зависшие блокировки лидов (lead_locks)
  • обнуляет счётчики менеджеров: принято/пропущено, приоритет, статусы занятости

Что НЕ трогает:
  • самих менеджеров (имя, номер Sipuni, логин/пароль) — остаются
  • online/offline менеджеров — остаётся как есть

ВНИМАНИЕ: удаление статистики необратимо. Запускай на тестовых данных
(перед боевым запуском) или когда осознанно нужен чистый лист.
"""
import asyncio

from sqlalchemy import delete, update

from .db import async_session_maker, init_db
from .models import AutodialQueue, CallLog, CallSession, LeadLock, Manager


async def main() -> None:
    await init_db()
    async with async_session_maker() as session:
        logs = (await session.execute(delete(CallLog))).rowcount
        sess = (await session.execute(delete(CallSession))).rowcount
        queue = (await session.execute(delete(AutodialQueue))).rowcount
        locks = (await session.execute(delete(LeadLock))).rowcount
        # Обнуляем счётчики менеджеров, но НЕ удаляем их
        await session.execute(
            update(Manager).values(
                missed=0,
                accepted_calls=0,
                priority_score=0.5,
                priority_total=0.0,
                priority_ok=0.0,
                priority_updated_at=None,
                busy_until=None,
                on_call=False,
                awaiting_ready=False,
                paused=False,
            )
        )
        await session.commit()

    print("Дашборд очищен:")
    print(f"  • удалено логов звонков:    {logs}")
    print(f"  • удалено сессий:           {sess}")
    print(f"  • очищено из очереди:       {queue}")
    print(f"  • снято блокировок лидов:   {locks}")
    print("  • счётчики менеджеров обнулены (принято/пропущено/приоритет)")
    print("\nМенеджеры и их логины сохранены. Дашборд теперь с чистого листа.")


if __name__ == "__main__":
    asyncio.run(main())
