# app/models.py
"""
Таблицы:
  managers          — менеджеры + persistent priority
  autodial_queue    — очередь повторных дозвонов
  call_logs         — журнал событий (включает имя/источник лида, время реакции)
  call_sessions    — активные «акты дозвона», нужны для Kcell event-webhook
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
class Department(Base):
    """Отдел (проект): «Яндекс 360», «Яндекс Такси Казахстан» и т.п.

    Каждый отдел жёстко привязан к своей воронке (CATEGORY_ID) Битрикс24 и
    к своим стадиям. Менеджеры отдела видят и обрабатывают лиды ТОЛЬКО из
    этой воронки/стадии — изоляция проектов друг от друга.
    """
    __tablename__ = "departments"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False, unique=True)

    # ID воронки (CATEGORY_ID) Битрикс24, за которой закреплён отдел.
    deal_category_id = Column(String, nullable=False)

    # Стадия-триггер: с какой стадии автодозвон ФАКТИЧЕСКИ забирает лиды прямо
    # сейчас. Формат как в BITRIX_STAGE_TRIGGER, напр. "C12:NEW" или "NEW".
    # Это одно из значений stage_warm / stage_cold ниже — какое именно,
    # определяет active_stage. Отдельное поле сохранено, чтобы весь остальной
    # код (dispatcher/webhook) продолжал читать одно понятное значение, не
    # зная про переключатель тёплые/холодные.
    stage_trigger = Column(String, nullable=False)

    # Два предустановленных варианта стадии-триггера этого отдела — «Тёплые»
    # и «Холодные» лиды. Администратор переключает их одной кнопкой в UI, не
    # вспоминая ID стадии каждый раз. NULL = вариант ещё не настроен.
    stage_warm = Column(String, nullable=True)
    stage_cold = Column(String, nullable=True)

    # Какой из двух вариантов сейчас активен (совпадает с stage_trigger):
    # "warm" | "cold". Нужно только для отображения переключателя в UI.
    active_stage = Column(String, nullable=True)

    # Стадия НДЗ (недозвон, промежуточный) и НДЗ 2 (после исчерпания попыток).
    # Необязательны: если не заданы — стадию сделки не двигаем.
    stage_ndz = Column(String, nullable=True)
    stage_ndz2 = Column(String, nullable=True)

    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(
        DateTime, server_default=func.now(), onupdate=func.now(), nullable=False
    )


# ─────────────────────────────────────────────
class Manager(Base):
    __tablename__ = "managers"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False)
    sipnumber = Column(String, nullable=False)

    # Отдел, к которому принадлежит менеджер. Определяет, из какой воронки/
    # стадии ему будут идти лиды (изоляция проектов). NULL = менеджер не
    # привязан ни к одному отделу — участвует в общем пуле (обратная
    # совместимость со старыми конфигурациями без отделов).
    department_id = Column(Integer, nullable=True, index=True)

    # Вход на личную страницу менеджера: логин уникален, пароль — хэш.
    # NULL = у менеджера ещё нет доступа к личной странице.
    login = Column(String, nullable=True, unique=True, index=True)
    password_hash = Column(String, nullable=True)

    online = Column(Boolean, default=True, nullable=False)
    # Ручная бессрочная пауза: оператор «отошёл» — звонки не идут, пока сам
    # не нажмёт «Возобновить». В отличие от busy_until (передышка на N секунд),
    # пауза не имеет таймера и держится до явного снятия.
    paused = Column(Boolean, default=False, nullable=False)
    # Оператор ПРЯМО СЕЙЧАС на звонке (callback назначен / идёт разговор).
    # Отличает «занят на звонке» от «передышка после звонка»: пока on_call=True,
    # передышку НЕ показываем и отсчёт не идёт — он начнётся только когда звонок
    # полностью завершится (on_call станет False).
    on_call = Column(Boolean, default=False, nullable=False)
    # «Завершение карточки»: после приёма звонка оператор должен сам нажать
    # «Готов принимать», только тогда ему пойдёт следующий звонок. Это даёт
    # время дозаполнить карточку клиента в Bitrix и сменить стадию.
    # Снимается только кнопкой «Готов принимать» (или админом из дашборда).
    awaiting_ready = Column(Boolean, default=False, nullable=False)
    missed = Column(Integer, default=0, nullable=False)
    accepted_calls = Column(Integer, default=0, nullable=False)

    # Занятость: менеджер недоступен для новых звонков до этого времени (UTC).
    # Ставится при callback, снимается через 5с после завершения звонка.
    # NULL или прошедшее время = свободен.
    busy_until = Column(DateTime, nullable=True)

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

    # Адресный дозвон (перевод звонка на конкретного оператора). Если задан —
    # лид уходит ТОЛЬКО этому менеджеру: свободен → звоним сразу, занят → ждём
    # именно его, на других НЕ раскидываем. NULL = обычный лид (любому свободному).
    target_manager_id = Column(Integer, nullable=True)

    # Отдел, которому принадлежит лид (изоляция воронок). NULL = общий пул
    # (обратная совместимость, если отделы не настроены).
    department_id = Column(Integer, nullable=True, index=True)

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

    # initial | autodial | test | kcell_event
    type = Column(String, nullable=False)

    # callback_created | connected | no_answer | no_managers
    # | failed | scheduled | max_attempts_reached | talk_finished
    status = Column(String, nullable=False)

    manager_id = Column(Integer, nullable=True)
    manager_name = Column(String, nullable=True)

    # Время реакции с момента webhook'а от Bitrix до первого callback'а
    reaction_seconds = Column(Float, nullable=True)
    # Длительность реального разговора (приходит из Kcell event-webhook)
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
    менеджеров для конкретного лида. Завершается либо когда Kcell event-webhook
    подтвердил факт разговора, либо когда никто не ответил.

    kcell_call_id (callid) — первичный ключ сопоставления входящего события
    Kcell с этой сессией. phone используется только как fallback, если
    callid в событии почему-то не пришёл.
    """
    __tablename__ = "call_sessions"

    id = Column(Integer, primary_key=True, index=True)
    lead_id = Column(Integer, nullable=False, index=True)
    phone = Column(String, nullable=False, index=True)
    manager_id = Column(Integer, nullable=True)
    manager_name = Column(String, nullable=True)
    manager_sipnumber = Column(String, nullable=True, index=True)

    # callid из ответа Kcell makeCall — по нему сопоставляем event-webhook
    # с этой сессией (основной ключ поиска, приоритетнее phone/sipnumber).
    kcell_call_id = Column(String, nullable=True, index=True, unique=False)

    # PENDING | CALLBACK_CREATED | CONNECTED | NO_ANSWER | ERROR
    state = Column(String, default="PENDING", nullable=False)

    # Когда пришёл webhook от Bitrix
    started_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    # Когда Kcell принял заявку на звонок (makeCall → status=ACCEPTED)
    callback_at = Column(DateTime, nullable=True)
    # Когда фактически был разговор (из Kcell event-webhook)
    connected_at = Column(DateTime, nullable=True)
    # Длительность разговора
    talk_seconds = Column(Float, nullable=True)

    is_autodial = Column(Boolean, default=False, nullable=False)
    attempts_used = Column(Integer, default=0, nullable=False)

    # Перевод звонка на конкретного оператора. is_transfer=True + target_manager_id
    # говорят обработчику событий Kcell: при недозвоне НЕ каскадить на других,
    # а повторять к этому же оператору (перевод адресный).
    is_transfer = Column(Boolean, default=False, nullable=False)
    target_manager_id = Column(Integer, nullable=True)

    # Отдел, которому принадлежит лид (нужно, чтобы каскад/повтор/НДЗ-стадии
    # оставались строго в рамках воронки этого отдела).
    department_id = Column(Integer, nullable=True, index=True)

    __table_args__ = (
        Index("ix_call_sessions_lead_state", "lead_id", "state"),
    )


# ─────────────────────────────────────────────
class LeadLock(Base):
    """Блокировка лида на время обработки — в БД, а не в памяти процесса.

    Зачем: раньше «занятые» лиды хранились в Set в памяти. При рестарте
    сервиса (деплой Railway) или при нескольких репликах эта защита терялась,
    и один лид мог обработаться дважды (двойной звонок клиенту).

    Теперь блокировка живёт в БД:
      - lead_id уникален → если строка уже есть, лид занят (атомарно на уровне БД);
      - acquired_at → для авто-протухания «зависших» блокировок (TTL),
        чтобы лид не остался заблокированным навечно при сбое.
    """
    __tablename__ = "lead_locks"

    lead_id = Column(Integer, primary_key=True)  # PK = уникальность = атомарность
    acquired_at = Column(DateTime, default=datetime.utcnow, nullable=False)

# ─────────────────────────────────────────────
class ManagerSession(Base):
    """Сессия входа менеджера на личную страницу.

    Менеджер логинится по логину/паролю → создаётся сессия с токеном,
    токен кладётся в cookie. Страница и её API ходят с этим токеном.
    Сессия живёт до явного выхода или истечения TTL.
    """
    __tablename__ = "manager_sessions"

    token = Column(String, primary_key=True)       # случайный токен сессии
    manager_id = Column(Integer, nullable=False, index=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    last_seen = Column(DateTime, default=datetime.utcnow, nullable=False)