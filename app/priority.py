# app/priority.py
"""
Приоритезация менеджеров с persistent score в БД.

score = ok / total  (диапазон 0..1, дефолт 0.5)

Со временем total «утекает» (decay), чтобы старые удачные/неудачные звонки
постепенно теряли вес. Decay применяется лениво — при следующем update().
"""
import logging
import math
import random
from datetime import datetime
from typing import List

from sqlalchemy import select

from .db import async_session_maker
from .models import Manager

logger = logging.getLogger(__name__)

# Период полураспада статистики в часах. 72 = за 3 дня старая статистика
# теряет половину веса.
HALF_LIFE_HOURS = 72.0


def _decay(value: float, hours_passed: float) -> float:
    if hours_passed <= 0:
        return value
    return value * math.pow(0.5, hours_passed / HALF_LIFE_HOURS)


async def record_outcome(manager_id: int, success: bool) -> None:
    """Записать исход звонка в persistent priority."""
    async with async_session_maker() as session:
        mgr = await session.get(Manager, manager_id)
        if not mgr:
            return

        now = datetime.utcnow()
        # Decay предыдущей статистики
        if mgr.priority_updated_at:
            hours = (now - mgr.priority_updated_at).total_seconds() / 3600.0
            mgr.priority_total = _decay(mgr.priority_total or 0.0, hours)
            mgr.priority_ok = _decay(mgr.priority_ok or 0.0, hours)

        mgr.priority_total = (mgr.priority_total or 0.0) + 1.0
        if success:
            mgr.priority_ok = (mgr.priority_ok or 0.0) + 1.0

        if mgr.priority_total > 0:
            mgr.priority_score = mgr.priority_ok / mgr.priority_total
        else:
            mgr.priority_score = 0.5

        mgr.priority_updated_at = now
        await session.commit()


def sort_managers(managers: List[Manager]) -> List[Manager]:
    """
    Случайный выбор среди свободных: НЕ судим по рейтингу/принятым/пропускам.
    Просто перемешиваем список — кому достанется звонок, решает случай.
    Так нагрузка не липнет ни к «звезде», ни к «аутсайдеру».

    На вход приходят уже отфильтрованные (online + свободные) менеджеры,
    поэтому здесь остаётся только честно перетасовать их.
    """
    shuffled = list(managers)
    random.shuffle(shuffled)
    return shuffled
