# app/config.py
"""Все настройки приложения через environment variables."""
from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # ── Окружение ────────────────────────────────────────────
    ENVIRONMENT: str = "dev"
    APP_PORT: int = 8000
    LOG_LEVEL: str = "INFO"

    # ── База данных ──────────────────────────────────────────
    DATABASE_URL: str = "sqlite+aiosqlite:///./app.db"

    # ── Bitrix24 ─────────────────────────────────────────────
    BITRIX_WEBHOOK_URL: str
    BITRIX_PORTAL_URL: Optional[str] = None

    BITRIX_STATUS_DIALING: Optional[str] = None
    BITRIX_STATUS_CONNECTED: Optional[str] = None
    BITRIX_STATUS_NO_ANSWER: Optional[str] = None
    BITRIX_STATUS_RETRY: Optional[str] = None
    BITRIX_STATUS_FAILED: Optional[str] = None

    # ── Sipuni ───────────────────────────────────────────────
    SIPUNI_USER: str
    SIPUNI_SECRET: str
    SIPUNI_API_BASE: str = "https://sipuni.com/api"

    # Секрет для входящих webhook'ов от Sipuni.
    SIPUNI_WEBHOOK_SECRET: Optional[str] = None

    # ── Бизнес-параметры ─────────────────────────────────────
    MANAGER_ANSWER_TIMEOUT_SECONDS: int = 30
    MAX_MANAGER_MISSED: int = 3
    MAX_AUTODIAL_ATTEMPTS: int = 6
    AUTODIAL_POLL_INTERVAL_SECONDS: int = 30
    BITRIX_TIMEOUT_SECONDS: float = 8.0
    SIPUNI_TIMEOUT_SECONDS: float = 10.0

    # Длительность звонка в сек, ниже которой не считаем «настоящим разговором»
    MIN_TALK_DURATION_SECONDS: int = 5

    # ── Безопасность ─────────────────────────────────────────
    WEBHOOK_SECRET: Optional[str] = None
    ENABLE_TEST_ENDPOINTS: bool = False
    CORS_ALLOW_ORIGINS: str = "*"
    SEED_DEFAULT_MANAGERS: bool = False

    # HTTP Basic auth на dashboard и API.
    # Если оба заданы — auth включён. Webhook'и (Bitrix/Sipuni) и /health
    # auth не требуют (они защищены своими секретами).
    DASHBOARD_USER: Optional[str] = None
    DASHBOARD_PASSWORD: Optional[str] = None

    # Auth на «личной» странице менеджера: по умолчанию страница доступна
    # по URL вида /manager/{id}?token=...  если задан MANAGER_PAGE_TOKEN.
    MANAGER_PAGE_TOKEN: Optional[str] = None

    # ── Стадии воронки «Яндекс 360» (category 12) ────────────
    # Недозвоны кидаем в НДЗ; после исчерпания всех попыток — в НДЗ 2.
    BITRIX_STAGE_NDZ: str = "C12:PREPARATION"        # НДЗ
    BITRIX_STAGE_NDZ2: str = "C12:UC_DGYTDZ"         # НДЗ 2 (все попытки исчерпаны)

    # Отдельный webhook с правами на пользователей (user.*) — для назначения
    # ответственного за сделку. Основной BITRIX_WEBHOOK_URL прав user не имеет.
    # Если не задан — назначение ответственного просто пропускается.
    BITRIX_USER_WEBHOOK_URL: Optional[str] = None

    # ── Telegram-алерты (опционально) ────────────────────────
    TELEGRAM_BOT_TOKEN: Optional[str] = None
    TELEGRAM_CHAT_ID: Optional[str] = None

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    @property
    def bitrix_base_url(self) -> str:
        url = str(self.BITRIX_WEBHOOK_URL).strip()
        return url if url.endswith("/") else url + "/"

    @property
    def cors_origins(self) -> list[str]:
        if not self.CORS_ALLOW_ORIGINS or self.CORS_ALLOW_ORIGINS.strip() == "*":
            return ["*"]
        return [o.strip() for o in self.CORS_ALLOW_ORIGINS.split(",") if o.strip()]

    @property
    def is_production(self) -> bool:
        return self.ENVIRONMENT.lower() in ("production", "prod")

    @property
    def is_postgres(self) -> bool:
        return self.DATABASE_URL.startswith(("postgresql", "postgres"))

    @property
    def auth_enabled(self) -> bool:
        return bool(self.DASHBOARD_USER and self.DASHBOARD_PASSWORD)

    @property
    def telegram_enabled(self) -> bool:
        return bool(self.TELEGRAM_BOT_TOKEN and self.TELEGRAM_CHAT_ID)


settings = Settings()  # type: ignore[call-arg]