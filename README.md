# AutoCall · Звандау · v3

Автоматическая обработка входящих лидов из Bitrix24 → дозвон через Sipuni →
распределение на свободного менеджера. С аналитикой, контролем дисциплины и
Telegram-алертами.

---

## Что нового в v3

- ✅ **Sipuni status webhook** — реальный `connected` отличается от `callback_created`
- ✅ **HTTP Basic auth** на dashboard и API
- ✅ **Личные страницы менеджеров** `/manager/{id}` с кнопкой online/offline
- ✅ **Имя и источник лида** из Bitrix → в БД и dashboard
- ✅ **Метрика «время реакции»** в `/analytics` (avg/median/p90, % под 60 сек)
- ✅ **Persistent priority** менеджеров с decay (выживает рестарт)
- ✅ **Telegram-алерты** при FAILED / нет менеджеров / снятие с линии
- ✅ **Postgres-готовность** (asyncpg в requirements)
- ✅ **CallSession** — отслеживание активных «актов дозвона»

---

## Содержание

- [Структура](#структура)
- [Локальный запуск](#локальный-запуск)
- [Переменные окружения](#переменные-окружения)
- [Деплой на Railway](#деплой-на-railway)
- [Настройка Bitrix24](#настройка-bitrix24)
- [Настройка Sipuni](#настройка-sipuni)
- [Telegram-алерты](#telegram-алерты)
- [Личные страницы менеджеров](#личные-страницы-менеджеров)
- [API](#api)

---

## Структура

```
.
├── app/
│   ├── __init__.py
│   ├── config.py
│   ├── db.py
│   ├── models.py            # +CallSession, +lead_name/source, +reaction/talk
│   ├── sipuni_client.py     # +parse_sipuni_webhook
│   ├── bitrix_client.py     # +extract_lead_meta
│   ├── priority.py          # persistent ManagerPriority с decay
│   ├── telegram.py          # алерты
│   ├── dispatcher.py        # +handle_sipuni_status
│   └── main.py              # +auth, +sipuni webhook, +/manager/{id}
├── static/
│   └── dashboard.html
├── .env.example
├── .gitignore
├── Procfile
├── requirements.txt
├── runtime.txt
└── README.md
```

---

## Локальный запуск

```bash
python3.11 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# Минимум что нужно заполнить: BITRIX_WEBHOOK_URL, SIPUNI_USER, SIPUNI_SECRET
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

Открыть:
- Dashboard: <http://localhost:8000/>
- Health: <http://localhost:8000/health>
- Swagger: <http://localhost:8000/docs>
- Страница менеджера: <http://localhost:8000/manager/1>

---

## Переменные окружения

Полный список — в `.env.example`. Обязательный минимум:

| Переменная | Описание |
|---|---|
| `BITRIX_WEBHOOK_URL` | URL входящего webhook твоего портала, со слэшем в конце |
| `SIPUNI_USER` | ID интеграции Sipuni |
| `SIPUNI_SECRET` | Секрет интеграции Sipuni |

Для production обязательно ещё:

| Переменная | Зачем |
|---|---|
| `WEBHOOK_SECRET` | защита `/bitrix/webhook/lead` |
| `SIPUNI_WEBHOOK_SECRET` | защита `/sipuni/webhook/status` |
| `DASHBOARD_USER` + `DASHBOARD_PASSWORD` | HTTP Basic на dashboard |
| `MANAGER_PAGE_TOKEN` | защита личных страниц менеджеров |
| `ENVIRONMENT=production` | флаг |
| `ENABLE_TEST_ENDPOINTS=false` | выключить `/test/*` |
| `DATABASE_URL=postgresql+asyncpg://...` | вместо SQLite |
| `BITRIX_STATUS_*` | status_id из вашей воронки |

---

## Деплой на Railway

### 1. Поднять Postgres (важно!)

В проекте Railway: **+ New** → **Database** → **PostgreSQL**.
Railway создаст переменные `PGHOST`, `PGUSER` и т.п. + строку подключения.

В **Variables** твоего web-сервиса поставь:
```
DATABASE_URL=postgresql+asyncpg://${{Postgres.PGUSER}}:${{Postgres.PGPASSWORD}}@${{Postgres.PGHOST}}:${{Postgres.PGPORT}}/${{Postgres.PGDATABASE}}
```
(если Railway даёт `${{Postgres.DATABASE_URL}}` — лучше просто заменить
префикс `postgresql://` на `postgresql+asyncpg://`).

### 2. Деплой кода

- New → Deploy from GitHub → выбрать репо
- Railway сам найдёт `Procfile` и `runtime.txt`
- Variables → вбить всё из `.env.example`

Минимум для прода в Variables:
```
ENVIRONMENT=production
ENABLE_TEST_ENDPOINTS=false
LOG_LEVEL=INFO
BITRIX_WEBHOOK_URL=https://...
SIPUNI_USER=...
SIPUNI_SECRET=...
WEBHOOK_SECRET=<сгенерируй: openssl rand -hex 24>
SIPUNI_WEBHOOK_SECRET=<сгенерируй>
DASHBOARD_USER=admin
DASHBOARD_PASSWORD=<сильный пароль>
MANAGER_PAGE_TOKEN=<сгенерируй>
CORS_ALLOW_ORIGINS=*
DATABASE_URL=postgresql+asyncpg://...
```

### 3. Public domain

Settings → Networking → **Generate Domain** → получишь
`https://autocall-production.up.railway.app`.

Проверка: `curl https://<domain>/health` → должен вернуть `{"ok": true, ...}`.

---

## Настройка Bitrix24

### Входящий webhook (мы → Bitrix)

Битрикс24 → Разработчикам → Другое → **Входящий вебхук**
→ права `crm` → создать → скопировать URL → в `BITRIX_WEBHOOK_URL`.

### Исходящий webhook (Bitrix → мы)

Битрикс24 → Разработчикам → **Исходящий вебхук**:
- Событие: **`ONCRMLEADADD`**
- URL: `https://<your-railway-domain>/bitrix/webhook/lead?secret=<WEBHOOK_SECRET>`

Проверка:
```bash
curl -X POST "https://<domain>/bitrix/webhook/lead?secret=<SECRET>" \
  -d "data[FIELDS][ID]=12345"
```
→ `{"ok": true, "lead_id": 12345, "queued": true}`

### Узнать статусы воронки

```
https://<portal>.bitrix24.ru/rest/1/<TOKEN>/crm.status.list?filter[ENTITY_ID]=STATUS
```

Скопировать нужные `STATUS_ID` в `BITRIX_STATUS_*`.

---

## Настройка Sipuni

### 1. Базовая интеграция

Sipuni → Настройки → Интеграции → API:
- скопировать `user` и `secret` → `SIPUNI_USER`, `SIPUNI_SECRET`
- внутренние номера менеджеров (`100`, `205`, и т.п.) → в `/managers` через
  dashboard или API

### 2. Status webhook (главное в v3!)

Это закрывает главную смысловую дыру MVP. Без него `connected` в логах =
просто «Sipuni принял заявку», и аналитика по соединениям врёт.

Sipuni → Настройки → Интеграции → **Веб-хуки** (или Функции, в зависимости от
тарифа):
- Событие: **завершение исходящего звонка** (или «Окончание разговора»)
- URL: `https://<domain>/sipuni/webhook/status?secret=<SIPUNI_WEBHOOK_SECRET>`
- Метод: POST
- Формат: JSON или form-data — поддерживаются оба

Что Sipuni должен прислать (любое подмножество — наш парсер умеет):
- `sipnumber` или `manager` или `internal` — внутренний номер
- `phone` или `client_phone` или `external` — номер клиента
- `duration` или `bill_seconds` или `billsec` — длительность разговора (сек)
- `status` или `disposition` — `answered`/`completed`/`success` если ответил

Если в твоём кабинете Sipuni ключи другие — добавь свои варианты в
`parse_sipuni_webhook()` в `app/sipuni_client.py`. Это безопасно, остальной
код трогать не нужно.

### 3. Тест

С `ENABLE_TEST_ENDPOINTS=true`:
```bash
curl "https://<domain>/test/sipuni_call?manager_id=1&client_phone=77001234567" \
  -u <DASHBOARD_USER>:<DASHBOARD_PASSWORD>
```

Sipuni должен позвонить на менеджера; после ответа набрать клиента; после
завершения разговора прийдёт webhook → ты увидишь в `/logs` запись
`status=connected` (а не `callback_created`).

---

## Telegram-алерты

Если в `.env` заданы `TELEGRAM_BOT_TOKEN` и `TELEGRAM_CHAT_ID`, бот пишет
в чат на следующие события:

| Событие | Сообщение |
|---|---|
| Менеджер пропустил 3 звонка подряд → снят с линии | ⚠️ Менеджер ... снят с линии |
| Лид пришёл, но нет ни одного online-менеджера | 🟡 Нет менеджеров на линии |
| 6 неудачных попыток автодозвона | 🔴 Не дозвонились |

### Как получить bot_token и chat_id

1. В Telegram: `@BotFather` → `/newbot` → имя бота → копируешь токен.
2. Создай чат (или используй существующий), **добавь бота в него**.
3. `@userinfobot` в этом же чате покажет chat_id (для группы — со знаком минус).
4. В Railway Variables: `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`.

---

## Личные страницы менеджеров

Каждый менеджер получает свою страницу:
```
https://<domain>/manager/{id}?token=<MANAGER_PAGE_TOKEN>
```

На странице:
- его статус (НА ЛИНИИ / НЕ АКТИВЕН) большими буквами
- две кнопки: «НА ЛИНИИ» / «СНЯТЬ С ЛИНИИ»
- статистика: принято всего, пропусков подряд, за неделю
- история последних 10 звонков

Если `MANAGER_PAGE_TOKEN` не задан — страница открыта всем (для dev).
В production обязательно задать.

**Удобно:** отправь каждому менеджеру ссылку с QR-кодом на его страницу,
он добавит в закладки на телефоне. Никакого dashboard'а не нужен.

---

## API

Все эндпоинты (кроме `/health` и webhook'ов) требуют HTTP Basic, если
`DASHBOARD_USER` + `DASHBOARD_PASSWORD` заданы.

| Метод | URL | Описание |
|---|---|---|
| GET | `/` | Dashboard |
| GET | `/health` | Healthcheck (без auth) |
| GET | `/managers` | Список менеджеров |
| POST | `/managers` | Добавить |
| PUT | `/managers/{id}` | Обновить |
| DELETE | `/managers/{id}` | Удалить |
| POST | `/managers/{id}/online` | НА ЛИНИИ |
| POST | `/managers/{id}/offline` | СНЯТЬ |
| GET | `/managers/{id}/stats?days=7` | Stats для личной страницы |
| GET | `/manager/{id}?token=...` | Личная HTML-страница менеджера |
| GET | `/logs?limit=100` | Логи + очередь |
| GET | `/analytics?days=7` | Аналитика (с reaction_time) |
| POST | `/bitrix/webhook/lead?secret=...` | Webhook от Bitrix |
| POST | `/sipuni/webhook/status?secret=...` | Webhook от Sipuni |
| GET | `/test/sipuni_call` | Только если `ENABLE_TEST_ENDPOINTS=true` |
| POST | `/test/lead` | Только если `ENABLE_TEST_ENDPOINTS=true` |

---

## Чек-лист первого запуска

1. [ ] `cp .env.example .env`, заполнить `BITRIX_WEBHOOK_URL`, `SIPUNI_USER`, `SIPUNI_SECRET`
2. [ ] `pip install -r requirements.txt`
3. [ ] `uvicorn app.main:app --reload` → проверить `/health`
4. [ ] Добавить пару менеджеров через `/managers` или поставить `SEED_DEFAULT_MANAGERS=true`
5. [ ] `ENABLE_TEST_ENDPOINTS=true` → `/test/sipuni_call?manager_id=1&client_phone=<свой_тел>` → должен прозвонить
6. [ ] Деплой на Railway + Postgres
7. [ ] Сгенерировать `WEBHOOK_SECRET`, `SIPUNI_WEBHOOK_SECRET`, `MANAGER_PAGE_TOKEN`
8. [ ] Настроить Bitrix outgoing webhook → `/bitrix/webhook/lead?secret=...`
9. [ ] Настроить Sipuni webhook на завершение звонка → `/sipuni/webhook/status?secret=...`
10. [ ] (опционально) Telegram-бот
11. [ ] Создать тестовый лид в Bitrix → проверить, что прозвон пошёл
12. [ ] Раздать менеджерам их персональные URL `/manager/{id}?token=...`

---

## Что осталось на будущее

- Distributed lock (Redis SETNX / pg_advisory_lock) для multi-worker — сейчас
  идемпотентность только в памяти одного процесса. Для одного worker'а
  (default) это нормально.
- E2E-тесты с моками Sipuni/Bitrix
- Sentry / структурированные логи в JSON
- Bitrix24 SSO вместо HTTP Basic
- Дневной отчёт в Telegram (агрегаты за день)
- Sipuni call recordings — сохранять ссылки в CallLog
