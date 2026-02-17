import asyncio
import html
import json
import logging
import os
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup

import db

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

START_DATE = date(2026, 2, 18)
END_DATE = date(2026, 4, 4)
HALFWAY_DATE = date(2026, 3, 13)
DB_PATH = os.getenv("DB_PATH") or os.getenv("DATABASE_URL", "bot.sqlite3")
POLL_SECONDS = int(os.getenv("WORKER_POLL_SECONDS", "60"))

with open("texts/copy.ru.json", "r", encoding="utf-8") as f:
    COPY = json.load(f)
with open("texts/quotes.json", "r", encoding="utf-8") as f:
    QUOTES = json.load(f)
with open("texts/presence_lines.json", "r", encoding="utf-8") as f:
    PRESENCE = json.load(f)


def parse_hhmm(value: str) -> tuple[int, int]:
    h, m = value.split(":", 1)
    return int(h), int(m)


def due_by_now(local_now: datetime, hhmm: str) -> bool:
    h, m = parse_hhmm(hhmm)
    target = time(hour=h, minute=m)
    return local_now.time().replace(second=0, microsecond=0) >= target


def days_left(local_date: date) -> int:
    return max((END_DATE - local_date).days, 0)


def build_menu_rows(paused: bool) -> list[list[str]]:
    pause_key = "resume" if paused else "pause"
    return [[COPY["buttons"]["time_change"]], [COPY["buttons"][pause_key]]]


def menu_markup(user: dict) -> ReplyKeyboardMarkup:
    paused = bool(int(user.get("paused", 0)) == 1)
    return ReplyKeyboardMarkup(build_menu_rows(paused), resize_keyboard=True, one_time_keyboard=False)


def evening_status_markup(user: dict) -> ReplyKeyboardMarkup:
    rows = [
        [COPY["buttons"]["status_full"]],
        [COPY["buttons"]["status_partial"]],
        [COPY["buttons"]["status_none"]],
    ] + build_menu_rows(bool(int(user.get("paused", 0)) == 1))
    return ReplyKeyboardMarkup(rows, resize_keyboard=True, one_time_keyboard=False)


async def send_morning(bot: Bot, user: dict, local_date: date) -> None:
    user_id = user["user_id"]

    start_date = date.fromisoformat(user["start_date"])
    today_row = db.ensure_day_row(DB_PATH, user_id, local_date, start_date)
    day_number = int(today_row["day_number"])

    if local_date == END_DATE:
        if not db.has_sent_message(DB_PATH, user_id, local_date, "morning_status"):
            await bot.send_message(chat_id=user_id, text=COPY["morning"]["last_day"], reply_markup=menu_markup(user))
            db.record_sent_message(DB_PATH, user_id, local_date, "morning_status")
        return

    status_text = COPY["morning"]["base"].format(
        day_number=day_number,
        days_left=days_left(local_date),
    )
    y_row = db.get_day(DB_PATH, user_id, local_date - timedelta(days=1))
    if y_row and y_row.get("status") is None:
        status_text = f"{COPY['morning']['yesterday_missed']}\n\n{status_text}"
    if local_date >= HALFWAY_DATE:
        status_text = f"{status_text}\n\n{COPY['morning']['halfway']}"

    if not db.has_sent_message(DB_PATH, user_id, local_date, "morning_status"):
        await bot.send_message(chat_id=user_id, text=status_text, reply_markup=menu_markup(user))
        db.record_sent_message(DB_PATH, user_id, local_date, "morning_status")

    if not db.has_sent_message(DB_PATH, user_id, local_date, "morning_quote"):
        idx = day_number - 1
        quote = QUOTES[idx] if 0 <= idx < len(QUOTES) else "—"
        quote_text = COPY["morning"]["quote_message"].format(quote=f"<i>{html.escape(quote)}</i>")
        await bot.send_message(
            chat_id=user_id,
            text=quote_text,
            reply_markup=menu_markup(user),
            parse_mode="HTML",
        )
        db.record_sent_message(DB_PATH, user_id, local_date, "morning_quote")


async def send_presence(bot: Bot, user: dict, local_date: date) -> None:
    user_id = user["user_id"]
    if db.has_sent_message(DB_PATH, user_id, local_date, "presence"):
        return

    day_row = db.get_day(DB_PATH, user_id, local_date)
    if not day_row or not day_row.get("day_number"):
        return

    day_number = int(day_row["day_number"])
    if day_number % 4 != 0 or day_number > 44:
        return

    idx = (day_number // 4) - 1
    if idx < 0 or idx >= len(PRESENCE):
        return

    kb = InlineKeyboardMarkup(
        [[InlineKeyboardButton(COPY["buttons"]["thanks"], callback_data="presence:thanks")]]
    )
    await bot.send_message(chat_id=user_id, text=PRESENCE[idx], reply_markup=kb)
    db.record_sent_message(DB_PATH, user_id, local_date, "presence")


async def send_evening_prompt(bot: Bot, user: dict, local_date: date) -> None:
    user_id = user["user_id"]
    if db.has_sent_message(DB_PATH, user_id, local_date, "evening_prompt"):
        return

    await bot.send_message(
        chat_id=user_id,
        text=COPY["evening"]["prompt"],
        reply_markup=evening_status_markup(user),
    )
    db.record_sent_message(DB_PATH, user_id, local_date, "evening_prompt")


async def send_evening_reminder(bot: Bot, user: dict, local_date: date) -> None:
    user_id = user["user_id"]
    if db.has_sent_message(DB_PATH, user_id, local_date, "evening_reminder"):
        return

    row = db.get_day(DB_PATH, user_id, local_date)
    if row and row.get("status") is not None:
        return

    await bot.send_message(
        chat_id=user_id,
        text=COPY["evening"]["reminder"],
        reply_markup=evening_status_markup(user),
    )
    db.record_sent_message(DB_PATH, user_id, local_date, "evening_reminder")


async def send_final_summary(bot: Bot, user: dict, local_date: date) -> None:
    user_id = user["user_id"]
    if local_date != END_DATE:
        return
    if db.has_sent_message(DB_PATH, user_id, local_date, "final_summary"):
        return

    stats = db.get_stats(DB_PATH, user_id)
    lines = [COPY["final"]["title"], COPY["final"]["stats"].format(**stats)]
    reflection = (user.get("reflection_text") or "").strip()
    if reflection:
        lines.append(COPY["final"]["reflection"].format(reflection_text=reflection))
        lines.append(COPY["final"]["reflection_invite"])
    kb = InlineKeyboardMarkup(
        [[InlineKeyboardButton(COPY["buttons"]["thanks"], callback_data="final:thanks")]]
    )
    await bot.send_message(chat_id=user_id, text="\n\n".join(lines), reply_markup=kb)
    db.record_sent_message(DB_PATH, user_id, local_date, "final_summary")


async def process_user(bot: Bot, user: dict) -> None:
    user_id = user["user_id"]
    tz = ZoneInfo(user["timezone"])
    now_utc = datetime.now(timezone.utc)
    local_now = now_utc.astimezone(tz)
    local_date = local_now.date()

    db.apply_due_time_changes(DB_PATH, user_id, local_date)
    user = db.get_user(DB_PATH, user_id)

    if int(user["paused"]) == 1:
        return

    start_date = date.fromisoformat(user["start_date"])
    if local_date < start_date or local_date < START_DATE:
        return
    if local_date > END_DATE:
        return

    db.ensure_day_row(DB_PATH, user_id, local_date, start_date)

    if due_by_now(local_now, user["morning_time"]):
        await send_morning(bot, user, local_date)

    if due_by_now(local_now, "12:00"):
        await send_presence(bot, user, local_date)

    if due_by_now(local_now, user["evening_time"]):
        await send_evening_prompt(bot, user, local_date)

    eh, em = parse_hhmm(user["evening_time"])
    reminder_time = (datetime.combine(local_date, time(eh, em)) + timedelta(minutes=30)).time()
    if local_now.time().replace(second=0, microsecond=0) >= reminder_time:
        await send_evening_reminder(bot, user, local_date)

    await send_final_summary(bot, user, local_date)


async def loop_worker() -> None:
    token = os.getenv("BOT_TOKEN")
    if not token:
        raise RuntimeError("BOT_TOKEN is required")

    db.init_db(DB_PATH)
    bot = Bot(token=token)

    while True:
        await run_tick_once(bot=bot)
        await asyncio.sleep(POLL_SECONDS)


async def run_tick_once(bot: Bot | None = None) -> None:
    token = os.getenv("BOT_TOKEN")
    if not token:
        raise RuntimeError("BOT_TOKEN is required")

    db.init_db(DB_PATH)
    local_bot = bot or Bot(token=token)
    try:
        try:
            users = db.list_active_users(DB_PATH)
            for user in users:
                try:
                    await process_user(local_bot, user)
                except Exception:
                    LOGGER.exception("user loop failed: %s", user.get("user_id"))
        except Exception:
            LOGGER.exception("worker loop failed")
    finally:
        if bot is None:
            await local_bot.session.close()


if __name__ == "__main__":
    asyncio.run(loop_worker())
