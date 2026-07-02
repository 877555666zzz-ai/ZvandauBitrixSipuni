# app/seed_operators.py
"""Разовый скрипт: завести операторов отдела «Продажи» с их номерами Sipuni
и логинами/паролями для портала.

Запуск (локально или через Railway → Shell):
    python -m app.seed_operators

Идемпотентно: если оператор с таким номером Sipuni уже есть — обновит имя,
логин и пароль; если нет — создаст. Повторный запуск безопасен.

По умолчанию новые операторы создаются ОФФЛАЙН (online=False), чтобы звонки
не пошли на них раньше времени. Они сами нажмут «На линии» в портале, либо
включи всех разом из дашборда (кнопка «Все на линию»).
"""
import asyncio

from sqlalchemy import select

from .db import async_session_maker, init_db
from .manager_portal import hash_password
from .models import Manager

# (Имя, номер Sipuni, логин, пароль)
OPERATORS = [
    ("Бекмухамбетова Дина",        "211", "Dina",     "Dina123"),
    ("Тилеумурат Луиза",           "238", "Luiza",    "Luiza123"),
    ("Штрапова Наталья",           "237", "Natalya",  "Natalya123"),
    ("Миркасимова Любовь",         "205", "Luba",     "Luba123"),
    ("Ерсаинова Балнур",           "248", "Balnur",   "Balnur123"),
    ("Мартынова Наталья (Мария)",  "212", "Mariya",   "Mariya123"),
    ("Айнель Ибраева",             "241", "Ainel",    "Ainel123"),
    ("Валиджанов Зафар",           "218", "Zafar",    "Zafar123"),
    ("Жумахмедова Аиша",           "275", "Aisha",    "Aisha123"),
    ("Мягков Ярослав",             "213", "Yaroslav", "Yaroslav123"),
    ("Аня",                        "269", "Anya",     "Anya123"),
]

# online=False — операторы создаются снятыми с линии (включат сами / из дашборда)
CREATE_ONLINE = False


async def main() -> None:
    await init_db()
    created, updated = 0, 0
    async with async_session_maker() as session:
        for name, sipnumber, login, password in OPERATORS:
            # Ищем по номеру Sipuni (он уникален для оператора)
            result = await session.execute(
                select(Manager).where(Manager.sipnumber == str(sipnumber))
            )
            mgr = result.scalar_one_or_none()
            if mgr:
                mgr.name = name
                mgr.login = login
                mgr.password_hash = hash_password(password)
                updated += 1
                print(f"  ~ обновлён: {name} (ext {sipnumber}, логин {login})")
            else:
                session.add(Manager(
                    name=name,
                    sipnumber=str(sipnumber),
                    login=login,
                    password_hash=hash_password(password),
                    online=CREATE_ONLINE,
                ))
                created += 1
                print(f"  + создан:   {name} (ext {sipnumber}, логин {login})")
        await session.commit()
    print(f"\nГотово. Создано: {created}, обновлено: {updated}.")
    print("Операторы входят на /manager своим логином/паролем.")
    if not CREATE_ONLINE:
        print("Все созданы ОФФЛАЙН — включи их кнопкой «На линии» (портал) "
              "или «Все на линию» (дашборд), когда будете готовы принимать.")


if __name__ == "__main__":
    asyncio.run(main())
