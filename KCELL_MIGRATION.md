# Kcell + многопроектность

## Что изменено
- Бизнес-логика dispatcher/очереди/повторов/приоритетов сохранена.
- Телефонный транспорт заменён: `sipuni_client.py` больше не импортируется рабочим кодом; используется `kcell_client.py`.
- Kcell `makeCall`: внутренний номер менеджера (`Manager.sipnumber`, например 701–715) + номер клиента.
- Webhook Kcell: `/kcell/webhook` принимает `event` и `history` REST API.
- Добавлен экран проектов `/`: каждый Department — отдельный проект.
- `/project/{department_id}` показывает только сотрудников и очередь выбранного проекта.
- Bitrix-изоляция по `Department(deal_category_id + stage_trigger)` остаётся как в версии после ТЗ.

## Что заполнить в окружении
`KCELL_API_URL` — адрес REST API, который Kcell показывает в настройке «Другая CRM (REST API)».
`KCELL_TOKEN` — token для запросов сайта -> Kcell.
`KCELL_CRM_TOKEN` — crm_token для проверки callbacks Kcell -> сайт.

В Kcell укажите callback URL вашего сайта: `https://ВАШ-ДОМЕН/kcell/webhook`.

## Важно
`Manager.sipnumber` оставлено с прежним именем специально: это позволяет не менять алгоритм автодозвона. Теперь в поле хранится внутренний номер Kcell (701, 702, ...), а не Sipuni.
