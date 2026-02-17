# Telegram Bot: 40 дней без воскресений

Ниже очень простой запуск. Делай шаги по порядку.

## 1) Что уже готово

- Токен уже сохранён в файле `.env`.
- Основные файлы бота уже на месте.

## 2) Первый запуск (тестовый режим)

Открой Terminal и выполни команды по одной:

```bash
cd "/Users/asyaburcewa/Documents/facting tg bot"
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
set -a
source .env
set +a
python bot.py
```

Если всё хорошо, в терминале будет видно, что бот запущен.

## 3) Как протестировать в Telegram (Phase 1)

1. Открой своего бота в Telegram.
2. Нажми `Start` или отправь `/start`.
3. Для теста сценариев отправь команду `/test`.
4. Выбери сценарий кнопкой:
   - `До старта поста`
   - `Во время поста`
   - `В апреле`
   - `После окончания`
5. Нажимай `Дальше →`, чтобы пройти весь сценарий шаг за шагом.

Важно: в тест-режиме нет таймеров, всё идёт только по кнопке `Дальше →`.

## 4) Реальный режим (Phase 2, когда тест завершён)

Нужно 2 окна Terminal.

Окно 1 (бот):

```bash
cd "/Users/asyaburcewa/Documents/facting tg bot"
source .venv/bin/activate
set -a
source .env
set +a
python bot.py
```

Окно 2 (воркер расписания):

```bash
cd "/Users/asyaburcewa/Documents/facting tg bot"
source .venv/bin/activate
set -a
source .env
set +a
python worker.py
```

Пока открыты оба окна, работает реальный режим с расписанием.

## 5) Как остановить

В каждом окне Terminal нажми `Ctrl + C`.

---

## 6) Деплой на Vercel + бесплатный scheduler (cron-job.org)

Это вариант для работы 24/7 без локального компьютера.

### Что используется

- Vercel: принимает webhook от Telegram
- Supabase Postgres (free): хранит базу
- cron-job.org (free): вызывает worker endpoint раз в минуту

### 6.1 Создай бесплатную базу Supabase

1. Зарегистрируйся на [supabase.com](https://supabase.com).
2. Создай новый project.
3. Открой `Project Settings -> Database`.
4. Скопируй connection string (`postgresql://...`), обязательно с `sslmode=require`.

### 6.2 Залей проект в GitHub

1. Создай репозиторий.
2. Залей туда эту папку (`facting tg bot`).

### 6.3 Импорт в Vercel

1. Зайди на [vercel.com](https://vercel.com).
2. `Add New -> Project`.
3. Выбери GitHub-репозиторий с ботом.
4. Deploy.

### 6.4 Добавь переменные окружения в Vercel

`Project Settings -> Environment Variables`:

- `BOT_TOKEN` = токен Telegram-бота
- `DATABASE_URL` = строка Postgres из Supabase
- `TELEGRAM_WEBHOOK_SECRET` = любая длинная случайная строка
- `CRON_SECRET` = любая длинная случайная строка

После добавления нажми `Redeploy`.

### 6.5 Подключи webhook Telegram

Подставь свои значения и открой в браузере:

```text
https://api.telegram.org/bot<BOT_TOKEN>/setWebhook?url=https://<YOUR-VERCEL-DOMAIN>/api/webhook&secret_token=<TELEGRAM_WEBHOOK_SECRET>
```

Если всё ок, Telegram вернёт `"ok":true`.

### 6.6 Настрой cron-job.org (раз в минуту)

1. Зарегистрируйся на [cron-job.org](https://cron-job.org).
2. Создай новый cron job:
   - URL:
     `https://<YOUR-VERCEL-DOMAIN>/api/cron/tick?token=<CRON_SECRET>`
   - Method: `GET`
   - Schedule: every `1 minute`
3. Сохрани и включи job.

### 6.7 Проверка

1. В Telegram отправь `/start` и пройди онбординг.
2. В cron-job.org посмотри, что job получает `200`.
3. В нужное время проверь, что приходят автоматические сообщения.
