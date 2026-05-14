# app/models.py
"""
Таблицы:
  managers          — менеджеры + persistent priority
  autodial_queue    — очередь повторных дозвонов
  call_logs         — журнал событий (включает имя/источник лида, время реакции)
  call_sessions    — активные «акты дозвона», нужны для Sipuni webhook
"""
from datetime import datetime

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.sql import func

from .db import Base


# ─────────────────────────────────────────────
class Manager(Base):
    __tablename__ = "managers"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False)
    sipnumber = Column(String, nullable=False)

    online = Column(Boolean, default=True, nullable=False)
    missed = Column(Integer, default=0, nullable=False)
    accepted_calls = Column(Integer, default=0, nullable=False)

    # Persistent priority. score = ok / total, total с decay.
    # Чем выше score, тем чаще идут лиды. Дефолт 0.5 — нейтрально.
    priority_score = Column(Float, default=0.5, nullable=False)
    priority_total = Column(Float, default=0.0, nullable=False)
    priority_ok = Column(Float, default=0.0, nullable=False)
    priority_updated_at = Column(DateTime, nullable=True)

    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(
        DateTime, server_default=func.now(), onupdate=func.now(), nullable=False
    )


# ─────────────────────────────────────────────
class AutodialQueue(Base):
    __tablename__ = "autodial_queue"

    id = Column(Integer, primary_key=True, index=True)
    lead_id = Column(Integer, nullable=False)
    phone = Column(String, nullable=False)
    lead_name = Column(String, nullable=True)
    lead_source = Column(String, nullable=True)

    attempts = Column(Integer, default=0, nullable=False)
    next_call_time = Column(DateTime, nullable=False)

    # SCHEDULED | IN_PROGRESS | DONE | FAILED
    state = Column(String, default="SCHEDULED", nullable=False)

    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(
        DateTime, server_default=func.now(), onupdate=func.now(), nullable=False
    )

    __table_args__ = (
        UniqueConstraint("lead_id", name="uq_autodial_queue_lead_id"),
        Index("ix_autodial_queue_next_call_time", "next_call_time"),
        Index("ix_autodial_queue_state", "state"),
    )


# ─────────────────────────────────────────────
class CallLog(Base):
    __tablename__ = "call_logs"

    id = Column(Integer, primary_key=True, index=True)
    timestamp = Column(
        DateTime, server_default=func.now(), default=datetime.utcnow, nullable=False
    )

    lead_id = Column(Integer, nullable=False)
    phone = Column(String, nullable=False)
    lead_name = Column(String, nullable=True)
    lead_source = Column(String, nullable=True)

    # initial | autodial | test | sipuni_webhook
    type = Column(String, nullable=False)

    # callback_created | connected | no_answer | no_managers
    # | failed | scheduled | max_attempts_reached | talk_finished
    status = Column(String, nullable=False)

    manager_id = Column(Integer, nullable=True)
    manager_name = Column(String, nullable=True)

    # Время реакции с момента webhook'а от Bitrix до первого callback'а
    reaction_seconds = Column(Float, nullable=True)
    # Длительность реального разговора (приходит из Sipuni webhook)
    talk_seconds = Column(Float, nullable=True)

    message = Column(String, nullable=True)
    details = Column(Text, nullable=True)

    __table_args__ = (
        Index("ix_call_logs_timestamp", "timestamp"),
        Index("ix_call_logs_lead_id", "lead_id"),
    )


# ─────────────────────────────────────────────
class CallSession(Base):
    """
    Активный «акт дозвона». Создаётся когда мы только начинаем обзванивать
    менеджеров для конкретного лида. Завершается либо когда Sipuni webhook
    подтвердил факт разговора, либо когда никто не ответил.

    Нужно, чтобы при приходе Sipuni webhook'а понять, к какому лиду
    относится этот звонок (по phone + sipnumber + времени).
    """
    __tablename__ = "call_sessions"

    id = Column(Integer, primary_key=True, index=True)
    lead_id = Column(Integer, nullable=False, index=True)
    phone = Column(String, nullable=False, index=True)
    manager_id = Column(Integer, nullable=True)
    manager_name = Column(String, nullable=True)
    manager_sipnumber = Column(String, nullable=True, index=True)

    # PENDING | CALLBACK_CREATED | CONNECTED | NO_ANSWER | ERROR
    state = Column(String, default="PENDING", nullable=False)

    # Когда пришёл webhook от Bitrix
    started_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    # Когда Sipuni принял callback
    callback_at = Column(DateTime, nullable=True)
    # Когда фактически был разговор (из Sipuni webhook)
    connected_at = Column(DateTime, nullable=True)
    # Длительность разговора
    talk_seconds = Column(Float, nullable=True)

    is_autodial = Column(Boolean, default=False, nullable=False)
    attempts_used = Column(Integer, default=0, nullable=False)

    __table_args__ = (
        Index("ix_call_sessions_lead_state", "lead_id", "state"),
    )
