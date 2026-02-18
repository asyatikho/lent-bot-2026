import html
import json
import logging
import os
import re
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    Update,
)
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

import db
from ptb_persistence import DbPersistence

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

START_DATE = date(2026, 2, 18)
END_DATE = date(2026, 4, 4)
DB_PATH = os.getenv("DB_PATH") or os.getenv("DATABASE_URL", "bot.sqlite3")
RESTART_ONBOARDING_COMMAND = "/restart_onboarding"

with open("texts/copy.ru.json", "r", encoding="utf-8") as f:
    COPY = json.load(f)
with open("texts/quotes.json", "r", encoding="utf-8") as f:
    QUOTES = json.load(f)
with open("texts/presence_lines.json", "r", encoding="utf-8") as f:
    PRESENCE = json.load(f)

(
    ONB_START_GATE,
    ONB_REFLECTION_INPUT,
    ONB_REFLECTION_CONFIRM,
    ONB_TIMEZONE,
    ONB_TIMEZONE_CONFIRM,
    ONB_TIMEZONE_CUSTOM,
    ONB_MORNING,
    ONB_EVENING,
    CHANGE_TARGET,
    CHANGE_VALUE,
    TEST_PICK,
    TEST_RUN,
) = range(12)

TIME_RE = re.compile(r"^([01]\d|2[0-3]):([0-5]\d)$")
OTHER_TIMEZONE_OPTIONS = COPY["timezone_other_options"]


def admin_ids() -> set[int]:
    raw = os.getenv("ADMIN_USER_ID", "").strip()
    if not raw:
        return set()
    out: set[int] = set()
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            out.add(int(part))
        except ValueError:
            continue
    return out


def is_admin(user_id: int) -> bool:
    return user_id in admin_ids()


def _onb_draft_key(user_id: int) -> str:
    return f"onboarding_draft:{user_id}"


def get_onb_draft(user_id: int) -> dict:
    raw = db.get_runtime_state(DB_PATH, _onb_draft_key(user_id))
    if not raw:
        return {}
    try:
        data = json.loads(raw)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def set_onb_draft(user_id: int, **fields) -> None:
    draft = get_onb_draft(user_id)
    draft.update(fields)
    db.set_runtime_state(DB_PATH, _onb_draft_key(user_id), json.dumps(draft, ensure_ascii=False))


def clear_onb_draft(user_id: int) -> None:
    db.set_runtime_state(DB_PATH, _onb_draft_key(user_id), "{}")


def get_onboarding_state_for_user(user_id: int) -> int | None:
    raw = db.get_runtime_state(DB_PATH, "conv:onboarding_conv")
    if not raw:
        return None
    try:
        rows = json.loads(raw)
    except Exception:
        return None
    if not isinstance(rows, list):
        return None

    for row in rows:
        if not isinstance(row, dict):
            continue
        key = row.get("key")
        if not isinstance(key, list):
            continue
        try:
            key_ints = [int(x) for x in key]
        except Exception:
            continue
        if user_id not in key_ints:
            continue
        state = row.get("state")
        try:
            return int(state)
        except Exception:
            return None
    return None


def resolve_timezone_label_from_draft(draft: dict) -> str | None:
    label = draft.get("timezone_label")
    if label:
        return str(label)
    tz = draft.get("timezone")
    if not tz:
        return None
    for item in COPY["timezone_options"]:
        if item.get("tz") == tz:
            return item.get("label")
    for item in OTHER_TIMEZONE_OPTIONS:
        if item.get("tz") == tz:
            return item.get("label")
    return str(tz)


def build_menu_rows(paused: bool) -> list[list[str]]:
    pause_key = "resume" if paused else "pause"
    return [[COPY["buttons"]["time_change"]], [COPY["buttons"][pause_key]]]


def menu_markup_for_user(user: dict | None) -> ReplyKeyboardMarkup:
    paused = bool(user and int(user.get("paused", 0)) == 1)
    return ReplyKeyboardMarkup(build_menu_rows(paused), resize_keyboard=True, one_time_keyboard=False)


def menu_markup_for_user_id(user_id: int) -> ReplyKeyboardMarkup:
    user = db.get_user(DB_PATH, user_id)
    return menu_markup_for_user(user)


def choice_markup(rows: list[list[str]]) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(rows, resize_keyboard=True, one_time_keyboard=True)


def timezone_markup() -> InlineKeyboardMarkup:
    rows = []
    for idx, item in enumerate(COPY["timezone_options"]):
        rows.append([InlineKeyboardButton(item["label"], callback_data=f"tz:{idx}")])
    return InlineKeyboardMarkup(rows)


def other_timezone_markup() -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for idx, item in enumerate(OTHER_TIMEZONE_OPTIONS):
        rows.append([InlineKeyboardButton(item["label"], callback_data=f"tzother:pick:{idx}")])
    rows.append([InlineKeyboardButton(COPY["buttons"]["back"], callback_data="tzother:back")])
    return InlineKeyboardMarkup(rows)


def test_other_timezone_markup() -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for idx, item in enumerate(OTHER_TIMEZONE_OPTIONS):
        rows.append([InlineKeyboardButton(item["label"], callback_data=f"test:tzother:pick:{idx}")])
    rows.append([InlineKeyboardButton(COPY["buttons"]["back"], callback_data="test:tzother:back")])
    return InlineKeyboardMarkup(rows)


def reflection_prompt_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(COPY["buttons"]["skip"], callback_data="onb:skip")],
        ]
    )


def reflection_confirm_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(COPY["buttons"]["save"], callback_data="onb:save")],
            [InlineKeyboardButton(COPY["buttons"]["edit"], callback_data="onb:edit")],
            [InlineKeyboardButton(COPY["buttons"]["back"], callback_data="onb:back_to_prompt")],
        ]
    )


def timezone_confirm_markup(prefix: str = "onb") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(COPY["buttons"]["save"], callback_data=f"{prefix}:tz_save")],
            [InlineKeyboardButton(COPY["buttons"]["edit"], callback_data=f"{prefix}:tz_edit")],
        ]
    )


def evening_choice_markup(user: dict) -> ReplyKeyboardMarkup:
    rows = [
        [COPY["buttons"]["status_full"]],
        [COPY["buttons"]["status_partial"]],
        [COPY["buttons"]["status_none"]],
    ] + build_menu_rows(bool(int(user.get("paused", 0)) == 1))
    return ReplyKeyboardMarkup(rows, resize_keyboard=True, one_time_keyboard=False)


def post_answer_markup(user: dict) -> ReplyKeyboardMarkup:
    rows = [[COPY["buttons"]["edit_answer"]]] + build_menu_rows(bool(int(user.get("paused", 0)) == 1))
    return ReplyKeyboardMarkup(rows, resize_keyboard=True, one_time_keyboard=False)


def local_today_for_user(user: dict) -> date:
    tz = ZoneInfo(user["timezone"]) if user and user.get("timezone") else timezone.utc
    return datetime.now(timezone.utc).astimezone(tz).date()


def parse_hhmm(value: str) -> tuple[int, int]:
    h, m = value.split(":", 1)
    return int(h), int(m)


def normalize_button_text(text: str | None) -> str:
    if text is None:
        return ""
    return re.sub(r"\s+", " ", text.replace("\ufe0f", "").strip())


def local_time_is_due(local_now: datetime, hhmm: str) -> bool:
    h, m = parse_hhmm(hhmm)
    return local_now.time().replace(second=0, microsecond=0) >= datetime(
        local_now.year, local_now.month, local_now.day, h, m
    ).time()


def parse_status_from_text(text: str) -> str | None:
    normalized = normalize_button_text(text)
    if normalized == normalize_button_text(COPY["buttons"]["status_full"]):
        return "full"
    if normalized == normalize_button_text(COPY["buttons"]["status_partial"]):
        return "partial"
    if normalized == normalize_button_text(COPY["buttons"]["status_none"]):
        return "none"
    return None


async def send_onboarding_catchup_messages(update: Update, user: dict, local_now: datetime) -> None:
    local_date = local_now.date()
    if local_date < START_DATE or local_date > END_DATE:
        return
    if int(user.get("paused", 0)) == 1:
        return

    user_id = user["user_id"]
    start_date = date.fromisoformat(user["start_date"])
    if local_date < start_date:
        return

    day_row = db.ensure_day_row(DB_PATH, user_id, local_date, start_date)
    day_number = int(day_row["day_number"])

    if local_time_is_due(local_now, user["morning_time"]) and not db.has_sent_message(
        DB_PATH, user_id, local_date, "morning_status"
    ):
        if local_date == END_DATE:
            status_text = COPY["morning"]["last_day"]
        else:
            days_left = max((END_DATE - local_date).days, 0)
            status_text = COPY["morning"]["base"].format(day_number=day_number, days_left=days_left)
            y_row = db.get_day(DB_PATH, user_id, local_date - timedelta(days=1))
            if y_row and y_row.get("status") is None:
                status_text = f"{COPY['morning']['yesterday_missed']}\n\n{status_text}"
            if local_date >= date(2026, 3, 13):
                status_text = f"{status_text}\n\n{COPY['morning']['halfway']}"

        await update.message.reply_text(status_text, reply_markup=menu_markup_for_user(user))
        db.record_sent_message(DB_PATH, user_id, local_date, "morning_status")

    if (
        local_date != END_DATE
        and local_time_is_due(local_now, user["morning_time"])
        and not db.has_sent_message(DB_PATH, user_id, local_date, "morning_quote")
    ):
        idx = day_number - 1
        quote = QUOTES[idx] if 0 <= idx < len(QUOTES) else "—"
        quote_text = COPY["morning"]["quote_message"].format(quote=f"<i>{html.escape(quote)}</i>")
        await update.message.reply_text(
            quote_text,
            parse_mode="HTML",
            reply_markup=menu_markup_for_user(user),
        )
        db.record_sent_message(DB_PATH, user_id, local_date, "morning_quote")

    if local_time_is_due(local_now, user["evening_time"]) and not db.has_sent_message(
        DB_PATH, user_id, local_date, "evening_prompt"
    ):
        await update.message.reply_text(
            COPY["evening"]["prompt"],
            reply_markup=evening_choice_markup(user),
        )
        db.record_sent_message(DB_PATH, user_id, local_date, "evening_prompt")


def build_test_steps(scenario: str) -> list[str]:
    if scenario == "after":
        return [COPY["common"]["already_finished"]]

    if scenario == "before":
        finish_steps = [COPY["onboarding"]["finish_before_start"]]
    elif scenario == "during":
        finish_steps = [COPY["onboarding"]["finish_base"], COPY["onboarding"]["finish_during"]]
    else:
        finish_steps = [
            COPY["onboarding"]["finish_base"],
            COPY["onboarding"]["finish_april"],
        ]

    steps = [
        COPY["onboarding"]["screen_1"],
        COPY["onboarding"]["screen_2"],
        COPY["onboarding"]["screen_3"],
        COPY["onboarding"]["screen_4"],
        COPY["onboarding"]["screen_5"],
        COPY["onboarding"]["screen_6"],
        COPY["onboarding"]["screen_7"],
    ]
    steps.extend(finish_steps)
    steps.append("__TEST_DAY_LOOP__")
    return steps


def test_day_params(scenario: str) -> tuple[int, int]:
    if scenario == "before":
        return 46, 46
    if scenario == "during":
        return 35, 35
    if scenario == "april":
        return 4, 3
    return 46, 46


def is_menu_button_text(text: str, user: dict) -> bool:
    normalized = normalize_button_text(text)
    lowered = normalized.lower()
    # Defensive fallback: Telegram clients may alter emoji presentation/spacing.
    if "изменить время" in lowered or "пауза" in lowered or "возобнов" in lowered:
        return True

    allowed = {
        normalize_button_text(COPY["buttons"]["time_change"]),
        normalize_button_text(COPY["buttons"]["pause"]),
        normalize_button_text(COPY["buttons"]["resume"]),
    }
    if int(user.get("paused", 0)) == 1:
        return normalized in {
            normalize_button_text(COPY["buttons"]["time_change"]),
            normalize_button_text(COPY["buttons"]["resume"]),
        }
    return normalized in {
        normalize_button_text(COPY["buttons"]["time_change"]),
        normalize_button_text(COPY["buttons"]["pause"]),
    }


async def send_onboarding_start(message, user_id: int) -> int:
    kb = InlineKeyboardMarkup(
        [[InlineKeyboardButton(COPY["buttons"]["start"], callback_data="onb:start")]]
    )
    await message.reply_text(COPY["onboarding"]["screen_1"], reply_markup=kb)
    return ONB_START_GATE


async def send_reflection_prompt(message) -> int:
    await message.reply_text(COPY["onboarding"]["screen_3"], reply_markup=reflection_prompt_markup())
    return ONB_REFLECTION_INPUT


async def send_timezone_step(message) -> int:
    await message.reply_text(COPY["onboarding"]["screen_4"], reply_markup=timezone_markup())
    return ONB_TIMEZONE


async def send_timezone_confirm_step(message, timezone_label: str, prefix: str = "onb") -> int:
    await message.reply_text(
        COPY["onboarding"]["timezone_confirm"].format(timezone_label=timezone_label),
        reply_markup=timezone_confirm_markup(prefix=prefix),
    )
    return ONB_TIMEZONE_CONFIRM


async def send_other_timezone_step(message) -> int:
    await message.reply_text(COPY["onboarding"]["screen_4"], reply_markup=other_timezone_markup())
    return ONB_TIMEZONE_CUSTOM


async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id
    user = db.get_user(DB_PATH, user_id)

    if user and int(user.get("onboarding_complete", 0)) == 1:
        mode = COPY["common"]["mode_paused"] if int(user.get("paused", 0)) == 1 else COPY["common"]["mode_active"]
        status_text = COPY["common"]["onboarding_done_status"].format(
            mode=mode,
            timezone=user.get("timezone") or "—",
            morning_time=user.get("morning_time") or "—",
            evening_time=user.get("evening_time") or "—",
            start_date=user.get("start_date") or "—",
            restart_command=RESTART_ONBOARDING_COMMAND,
        )
        await update.message.reply_text(
            status_text,
            reply_markup=menu_markup_for_user(user),
        )
        return ConversationHandler.END

    if date.today() > END_DATE:
        await update.message.reply_text(COPY["common"]["already_finished"])
        return ConversationHandler.END

    db.upsert_user(DB_PATH, user_id)
    clear_onb_draft(user_id)
    return await send_onboarding_start(update.message, user_id)


async def restart_onboarding_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if date.today() > END_DATE:
        await update.message.reply_text(COPY["common"]["already_finished"])
        return ConversationHandler.END

    user_id = update.effective_user.id
    db.delete_user(DB_PATH, user_id)
    db.upsert_user(DB_PATH, user_id)
    clear_onb_draft(user_id)
    await update.message.reply_text(COPY["common"]["restart_started"], reply_markup=ReplyKeyboardRemove())
    return await send_onboarding_start(update.message, user_id)


async def admin_stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text(COPY["admin"]["forbidden"])
        return
    stats = db.get_admin_stats(DB_PATH)
    distribution = stats.get("marks_distribution", [])
    if distribution:
        lines = [
            COPY["admin"]["marks_distribution_line"].format(
                marks_count=row["marks_count"],
                users_count=row["users_count"],
            )
            for row in distribution
            if int(row.get("users_count", 0)) > 0
        ]
        if lines:
            marks_distribution_block = (
                f"{COPY['admin']['marks_distribution_title']}\n" + "\n".join(lines)
            )
        else:
            marks_distribution_block = COPY["admin"]["marks_distribution_empty"]
    else:
        marks_distribution_block = COPY["admin"]["marks_distribution_empty"]
    stats["marks_distribution_block"] = marks_distribution_block
    await update.message.reply_text(COPY["admin"]["stats"].format(**stats))


async def admin_nudge_onboarding_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text(COPY["admin"]["forbidden"])
        return

    targets = db.list_onboarding_incomplete_users(DB_PATH)
    sent = 0
    failed = 0
    for row in targets:
        try:
            target_user_id = int(row["user_id"])
            state = get_onboarding_state_for_user(target_user_id)
            draft = get_onb_draft(target_user_id)

            if state == ONB_START_GATE:
                kb = InlineKeyboardMarkup(
                    [[InlineKeyboardButton(COPY["buttons"]["start"], callback_data="onb:start")]]
                )
                await context.bot.send_message(chat_id=target_user_id, text=COPY["onboarding"]["screen_1"], reply_markup=kb)
            elif state == ONB_REFLECTION_INPUT:
                await context.bot.send_message(
                    chat_id=target_user_id,
                    text=COPY["onboarding"]["screen_3"],
                    reply_markup=reflection_prompt_markup(),
                )
            elif state == ONB_REFLECTION_CONFIRM:
                reflection_text = (draft.get("reflection_candidate") or "").strip()
                if reflection_text:
                    await context.bot.send_message(
                        chat_id=target_user_id,
                        text=COPY["onboarding"]["reflection_confirm"].format(reflection_text=reflection_text),
                        reply_markup=reflection_confirm_markup(),
                    )
                else:
                    await context.bot.send_message(
                        chat_id=target_user_id,
                        text=COPY["onboarding"]["screen_3"],
                        reply_markup=reflection_prompt_markup(),
                    )
            elif state == ONB_TIMEZONE:
                await context.bot.send_message(
                    chat_id=target_user_id,
                    text=COPY["onboarding"]["screen_4"],
                    reply_markup=timezone_markup(),
                )
            elif state == ONB_TIMEZONE_CUSTOM:
                await context.bot.send_message(
                    chat_id=target_user_id,
                    text=COPY["onboarding"]["screen_4"],
                    reply_markup=other_timezone_markup(),
                )
            elif state == ONB_TIMEZONE_CONFIRM:
                tz_label = resolve_timezone_label_from_draft(draft)
                if tz_label:
                    await context.bot.send_message(
                        chat_id=target_user_id,
                        text=COPY["onboarding"]["timezone_confirm"].format(timezone_label=tz_label),
                        reply_markup=timezone_confirm_markup(),
                    )
                else:
                    await context.bot.send_message(
                        chat_id=target_user_id,
                        text=COPY["onboarding"]["screen_4"],
                        reply_markup=timezone_markup(),
                    )
            elif state == ONB_MORNING:
                await context.bot.send_message(chat_id=target_user_id, text=COPY["onboarding"]["screen_5"])
                await context.bot.send_message(chat_id=target_user_id, text=COPY["onboarding"]["screen_6"])
            elif state == ONB_EVENING:
                await context.bot.send_message(chat_id=target_user_id, text=COPY["onboarding"]["screen_7"])
            else:
                await context.bot.send_message(
                    chat_id=target_user_id,
                    text=COPY["onboarding"]["screen_4"],
                    reply_markup=timezone_markup(),
                )
            sent += 1
        except Exception:
            failed += 1

    await update.message.reply_text(
        COPY["admin"]["nudge_result"].format(targets=len(targets), sent=sent, failed=failed)
    )


async def unknown_command_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = db.get_user(DB_PATH, update.effective_user.id)
    if user and int(user.get("onboarding_complete", 0)) == 1:
        await update.message.reply_text(
            COPY["common"]["unknown_text"],
            reply_markup=menu_markup_for_user(user),
        )
        return
    await update.message.reply_text(COPY["common"]["unknown_text"])


async def onb_start_click(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    clear_onb_draft(update.effective_user.id)
    await query.message.reply_text(COPY["onboarding"]["screen_2"])
    return await send_reflection_prompt(query.message)


async def onb_reflection_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = (update.message.text or "").strip()
    if len(text) > 500:
        await update.message.reply_text(COPY["errors"]["reflection_too_long"])
        return ONB_REFLECTION_INPUT

    context.user_data["reflection_candidate"] = text
    set_onb_draft(update.effective_user.id, reflection_candidate=text)
    confirm_text = COPY["onboarding"]["reflection_confirm"].format(reflection_text=text)
    await update.message.reply_text(confirm_text, reply_markup=reflection_confirm_markup())
    return ONB_REFLECTION_CONFIRM


async def onb_reflection_skip(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    context.user_data["reflection_text"] = None
    context.user_data["reflection_skipped"] = 1
    set_onb_draft(update.effective_user.id, reflection_text=None, reflection_skipped=1)
    return await send_timezone_step(query.message)


async def onb_reflection_save(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    context.user_data["reflection_text"] = context.user_data.get("reflection_candidate")
    context.user_data["reflection_skipped"] = 0
    set_onb_draft(
        update.effective_user.id,
        reflection_text=context.user_data.get("reflection_candidate"),
        reflection_skipped=0,
    )
    await query.message.reply_text(COPY["onboarding"]["reflection_saved"])
    return await send_timezone_step(query.message)


async def onb_reflection_edit(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    return await send_reflection_prompt(query.message)


async def onb_reflection_back_to_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    return await send_reflection_prompt(query.message)


async def onb_reflection_back_to_welcome(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    await query.message.reply_text(COPY["onboarding"]["screen_2"])
    return await send_reflection_prompt(query.message)


async def onb_timezone_pick(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    idx_str = query.data.split(":", 1)[1]

    try:
        idx = int(idx_str)
        entry = COPY["timezone_options"][idx]
    except Exception:
        await query.message.reply_text(COPY["errors"]["timezone_unknown"])
        return await send_timezone_step(query.message)

    if entry["tz"] == "other":
        return await send_other_timezone_step(query.message)

    context.user_data["timezone"] = entry["tz"]
    context.user_data["timezone_label"] = entry["label"]
    set_onb_draft(update.effective_user.id, timezone=entry["tz"], timezone_label=entry["label"])
    return await send_timezone_confirm_step(query.message, entry["label"])


async def onb_timezone_custom_pick(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    data = query.data

    if data == "tzother:back":
        return await send_timezone_step(query.message)

    if data.startswith("tzother:pick:"):
        try:
            idx = int(data.split(":")[-1])
            chosen = OTHER_TIMEZONE_OPTIONS[idx]
            tz = chosen["tz"]
            ZoneInfo(tz)
        except Exception:
            await query.message.reply_text(COPY["errors"]["timezone_unknown"])
            return await send_other_timezone_step(query.message)

        context.user_data["timezone"] = tz
        context.user_data["timezone_label"] = chosen["label"]
        set_onb_draft(update.effective_user.id, timezone=tz, timezone_label=chosen["label"])
        return await send_timezone_confirm_step(query.message, chosen["label"])

    await query.message.reply_text(COPY["errors"]["timezone_unknown"])
    return await send_other_timezone_step(query.message)


async def onb_set_morning(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    value = (update.message.text or "").strip()
    if not TIME_RE.match(value):
        await update.message.reply_text(COPY["errors"]["invalid_time"])
        return ONB_MORNING
    context.user_data["morning_time"] = value
    set_onb_draft(update.effective_user.id, morning_time=value)
    await update.message.reply_text(COPY["onboarding"]["morning_saved"])
    await update.message.reply_text(COPY["onboarding"]["screen_7"])
    return ONB_EVENING


async def onb_set_evening(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    value = (update.message.text or "").strip()
    if not TIME_RE.match(value):
        await update.message.reply_text(COPY["errors"]["invalid_time"])
        return ONB_EVENING

    draft = get_onb_draft(update.effective_user.id)
    tz_val = context.user_data.get("timezone") or draft.get("timezone")
    morning_val = context.user_data.get("morning_time") or draft.get("morning_time")
    reflection_text = context.user_data.get("reflection_text", draft.get("reflection_text"))
    reflection_skipped = int(context.user_data.get("reflection_skipped", draft.get("reflection_skipped", 1)))

    if not tz_val or not morning_val:
        await update.message.reply_text(COPY["errors"]["wrong_input"])
        return await send_timezone_step(update.message)

    user_id = update.effective_user.id
    tz = tz_val
    local_today = datetime.now(timezone.utc).astimezone(ZoneInfo(tz)).date()

    if local_today > END_DATE:
        await update.message.reply_text(COPY["common"]["already_finished"])
        return ConversationHandler.END

    start_date = START_DATE if local_today < START_DATE else local_today
    db.upsert_user(
        DB_PATH,
        user_id,
        timezone=tz,
        morning_time=morning_val,
        evening_time=value,
        morning_time_effective_from=local_today.isoformat(),
        evening_time_effective_from=local_today.isoformat(),
        onboarding_complete=1,
        paused=0,
        start_date=start_date.isoformat(),
        reflection_text=reflection_text,
        reflection_skipped=reflection_skipped,
    )

    if START_DATE <= local_today <= END_DATE:
        db.ensure_day_row(DB_PATH, user_id, local_today, start_date)

    await update.message.reply_text(COPY["onboarding"]["evening_saved"])

    if local_today < START_DATE:
        finish_text = COPY["onboarding"]["finish_before_start"]
    else:
        messages = [COPY["onboarding"]["finish_base"]]
        if local_today.month == 4 and 1 <= local_today.day <= 4:
            messages.append(COPY["onboarding"]["finish_april"])
        else:
            messages.append(COPY["onboarding"]["finish_during"])
        finish_text = "\n\n".join(messages)

    user = db.get_user(DB_PATH, user_id)
    await update.message.reply_text(finish_text, reply_markup=menu_markup_for_user(user))
    local_now = datetime.now(timezone.utc).astimezone(ZoneInfo(tz))
    await send_onboarding_catchup_messages(update, user, local_now)
    clear_onb_draft(user_id)
    return ConversationHandler.END


async def onb_wrong_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text(COPY["errors"]["wrong_input"])
    return await send_onboarding_start(update.message, update.effective_user.id)


async def try_handle_time_in_stale_onboarding_state(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int | None:
    """Accept HH:MM even if conversation state lagged on serverless runtime."""
    value = (update.message.text or "").strip()
    if not TIME_RE.match(value):
        return None

    draft = get_onb_draft(update.effective_user.id)
    tz_known = bool(context.user_data.get("timezone") or draft.get("timezone"))
    morning_known = bool(context.user_data.get("morning_time") or draft.get("morning_time"))
    if not tz_known:
        return None

    if not morning_known:
        return await onb_set_morning(update, context)
    return await onb_set_evening(update, context)


async def onb_wrong_reflection_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    time_handled_state = await try_handle_time_in_stale_onboarding_state(update, context)
    if time_handled_state is not None:
        return time_handled_state

    text = context.user_data.get("reflection_candidate", "")
    await update.message.reply_text(COPY["errors"]["wrong_input"])
    confirm_text = COPY["onboarding"]["reflection_confirm"].format(reflection_text=text)
    await update.message.reply_text(confirm_text, reply_markup=reflection_confirm_markup())
    return ONB_REFLECTION_CONFIRM


async def onb_wrong_timezone(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    time_handled_state = await try_handle_time_in_stale_onboarding_state(update, context)
    if time_handled_state is not None:
        return time_handled_state

    await update.message.reply_text(COPY["errors"]["wrong_input"])
    return await send_timezone_step(update.message)


async def onb_timezone_confirm_save(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    await query.message.reply_text(COPY["onboarding"]["screen_5"])
    await query.message.reply_text(COPY["onboarding"]["screen_6"])
    return ONB_MORNING


async def onb_timezone_confirm_edit(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    return await send_timezone_step(query.message)


async def onb_wrong_timezone_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    time_handled_state = await try_handle_time_in_stale_onboarding_state(update, context)
    if time_handled_state is not None:
        return time_handled_state

    await update.message.reply_text(COPY["errors"]["wrong_input"])
    label = context.user_data.get("timezone_label") or context.user_data.get("timezone") or "—"
    return await send_timezone_confirm_step(update.message, label)


async def onb_stale_callback_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Recover onboarding flow if callback arrived while conversation state is stale."""
    query = update.callback_query
    data = query.data or ""

    if data == "onb:start":
        return await onb_start_click(update, context)
    if data == "onb:skip":
        return await onb_reflection_skip(update, context)
    if data == "onb:save":
        return await onb_reflection_save(update, context)
    if data == "onb:edit":
        return await onb_reflection_edit(update, context)
    if data == "onb:back_to_prompt":
        return await onb_reflection_back_to_prompt(update, context)
    if data == "onb:back_to_welcome":
        return await onb_reflection_back_to_welcome(update, context)
    if data.startswith("tz:"):
        return await onb_timezone_pick(update, context)
    if data.startswith("tzother:"):
        return await onb_timezone_custom_pick(update, context)
    if data == "onb:tz_save":
        return await onb_timezone_confirm_save(update, context)
    if data == "onb:tz_edit":
        return await onb_timezone_confirm_edit(update, context)

    await query.answer()
    return ConversationHandler.END


async def change_time_entry(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    rows = [
        [COPY["buttons"]["change_morning"]],
        [COPY["buttons"]["change_evening"]],
        [COPY["buttons"]["back"]],
    ]
    await update.message.reply_text(COPY["common"]["choose_time_target"], reply_markup=choice_markup(rows))
    return CHANGE_TARGET


async def change_time_target(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = update.message.text
    if text == COPY["buttons"]["back"]:
        await update.message.reply_text(
            COPY["common"]["back_keep"],
            reply_markup=menu_markup_for_user_id(update.effective_user.id),
        )
        return ConversationHandler.END

    if text == COPY["buttons"]["change_morning"]:
        context.user_data["change_target"] = "morning"
    elif text == COPY["buttons"]["change_evening"]:
        context.user_data["change_target"] = "evening"
    else:
        rows = [
            [COPY["buttons"]["change_morning"]],
            [COPY["buttons"]["change_evening"]],
            [COPY["buttons"]["back"]],
        ]
        await update.message.reply_text(COPY["errors"]["wrong_input"])
        await update.message.reply_text(COPY["common"]["choose_time_target"], reply_markup=choice_markup(rows))
        return CHANGE_TARGET

    await update.message.reply_text(
        COPY["common"]["prompt_new_time"],
        reply_markup=menu_markup_for_user_id(update.effective_user.id),
    )
    return CHANGE_VALUE


async def change_time_value(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    value = (update.message.text or "").strip()
    if not TIME_RE.match(value):
        await update.message.reply_text(COPY["errors"]["invalid_time"])
        await update.message.reply_text(
            COPY["common"]["prompt_new_time"],
            reply_markup=menu_markup_for_user_id(update.effective_user.id),
        )
        return CHANGE_VALUE

    user = db.get_user(DB_PATH, update.effective_user.id)
    if not user or not user.get("timezone"):
        await update.message.reply_text(
            COPY["common"]["back_keep"],
            reply_markup=menu_markup_for_user_id(update.effective_user.id),
        )
        return ConversationHandler.END

    local_today = local_today_for_user(user)
    effective_from = local_today + timedelta(days=1)
    db.queue_time_change(
        DB_PATH,
        update.effective_user.id,
        context.user_data["change_target"],
        value,
        effective_from,
    )
    await update.message.reply_text(
        COPY["common"]["change_applies_tomorrow"],
        reply_markup=menu_markup_for_user_id(update.effective_user.id),
    )
    return ConversationHandler.END


async def pause_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = db.get_user(DB_PATH, update.effective_user.id)
    if not user:
        return
    db.set_pause(DB_PATH, update.effective_user.id, True)
    user = db.get_user(DB_PATH, update.effective_user.id)
    await update.message.reply_text(COPY["common"]["pause_on"], reply_markup=menu_markup_for_user(user))


async def resume_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = db.get_user(DB_PATH, update.effective_user.id)
    if not user:
        return
    db.set_pause(DB_PATH, update.effective_user.id, False)
    user = db.get_user(DB_PATH, update.effective_user.id)
    await update.message.reply_text(COPY["common"]["pause_off"], reply_markup=menu_markup_for_user(user))


async def evening_status_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = (update.message.text or "").strip()
    user = db.get_user(DB_PATH, update.effective_user.id)
    if not user or int(user.get("onboarding_complete", 0)) == 0:
        await update.message.reply_text(COPY["common"]["unknown_text"])
        return

    if text == COPY["buttons"]["edit_answer"]:
        await update.message.reply_text(COPY["evening"]["repeat_prompt"], reply_markup=evening_choice_markup(user))
        return

    status = parse_status_from_text(text)
    tz = ZoneInfo(user["timezone"])
    local_date = datetime.now(timezone.utc).astimezone(tz).date()
    start_date = date.fromisoformat(user["start_date"])

    if status is None:
        has_evening = db.has_sent_message(DB_PATH, update.effective_user.id, local_date, "evening_prompt")
        day = db.get_day(DB_PATH, update.effective_user.id, local_date)
        waiting_answer = has_evening and (day is None or day.get("status") is None)
        if waiting_answer and not is_menu_button_text(text, user):
            await update.message.reply_text(COPY["errors"]["wrong_input"])
            await update.message.reply_text(COPY["evening"]["repeat_prompt"], reply_markup=evening_choice_markup(user))
            return
        if not waiting_answer and not is_menu_button_text(text, user):
            await update.message.reply_text(
                COPY["common"]["unknown_text"],
                reply_markup=menu_markup_for_user(user),
            )
        return

    if local_date < start_date or local_date > END_DATE:
        return

    db.ensure_day_row(DB_PATH, update.effective_user.id, local_date, start_date)
    allowed = db.can_update_evening_status(
        DB_PATH,
        update.effective_user.id,
        local_date,
        datetime.now(timezone.utc),
    )
    if not allowed:
        return

    db.set_day_status(DB_PATH, update.effective_user.id, local_date, status)
    user = db.get_user(DB_PATH, update.effective_user.id)
    await update.message.reply_text(COPY["evening"]["accepted"], reply_markup=post_answer_markup(user))


async def thanks_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    user = db.get_user(DB_PATH, update.effective_user.id)
    await query.message.reply_text(COPY["common"]["presence_reply"], reply_markup=menu_markup_for_user(user))


async def final_thanks_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    user_id = update.effective_user.id
    db.upsert_user(DB_PATH, user_id)

    if not db.has_sent_message(DB_PATH, user_id, END_DATE, "final_followup"):
        await query.message.reply_text(COPY["final"]["closing"])
        await query.message.reply_text(COPY["final"]["contacts"], reply_markup=ReplyKeyboardRemove())
        db.record_sent_message(DB_PATH, user_id, END_DATE, "final_followup")


async def test_final_thanks_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    if context.user_data.get("test_final_followup_sent", False):
        return
    context.user_data["test_final_followup_sent"] = True
    await query.message.reply_text(COPY["final"]["closing"])
    await query.message.reply_text(COPY["final"]["contacts"], reply_markup=ReplyKeyboardRemove())


async def test_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    kb = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(COPY["buttons"]["scenario_before"], callback_data="test:before")],
            [InlineKeyboardButton(COPY["buttons"]["scenario_during"], callback_data="test:during")],
            [InlineKeyboardButton(COPY["buttons"]["scenario_april"], callback_data="test:april")],
            [InlineKeyboardButton(COPY["buttons"]["scenario_after"], callback_data="test:after")],
        ]
    )
    await update.message.reply_text(COPY["test_mode"]["intro"])
    await update.message.reply_text(COPY["common"]["test_pick"], reply_markup=kb)
    return TEST_PICK


async def test_pick_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    scenario = query.data.split(":", 1)[1]
    context.user_data["test_scenario"] = scenario
    context.user_data["test_steps"] = build_test_steps(scenario)
    context.user_data["test_index"] = 0
    context.user_data["test_waiting_reflection"] = False
    context.user_data["test_waiting_reflection_confirm"] = False
    context.user_data["test_waiting_time_input"] = None
    context.user_data["test_waiting_timezone_confirm"] = False
    context.user_data["test_waiting_evening_status"] = False
    context.user_data["test_waiting_day_next"] = False
    context.user_data["test_pending_day_status"] = None
    context.user_data["test_day_loop_active"] = False
    context.user_data["test_stats"] = {"full": 0, "partial": 0, "none": 0}
    context.user_data["test_prev_unmarked"] = False
    context.user_data["test_final_followup_sent"] = False
    await send_test_step(query.message, context)
    return TEST_RUN


async def send_test_day_prompt(message, context: ContextTypes.DEFAULT_TYPE) -> None:
    day = int(context.user_data["test_day"])
    total_days = int(context.user_data["test_total_days"])
    days_left_start = int(context.user_data["test_days_left_start"])

    if day == total_days:
        morning_text = COPY["morning"]["last_day"]
    else:
        days_left = max(days_left_start - (day - 1), 0)
        morning_text = COPY["morning"]["base"].format(day_number=day, days_left=days_left)
        if context.user_data.get("test_prev_unmarked", False):
            morning_text = f"{COPY['morning']['yesterday_missed']}\n\n{morning_text}"

    await message.reply_text(morning_text)

    if day != total_days:
        quote_idx = max(day - 1, 0)
        quote = QUOTES[quote_idx] if quote_idx < len(QUOTES) else "—"
        quote_text = COPY["morning"]["quote_message"].format(quote=f"<i>{html.escape(quote)}</i>")
        await message.reply_text(quote_text, parse_mode="HTML")

    if PRESENCE and day % 4 == 0 and day <= 44:
        presence_index = (day // 4) - 1
        if 0 <= presence_index < len(PRESENCE):
            await message.reply_text(PRESENCE[presence_index])

    rows = [
        [COPY["buttons"]["status_full"]],
        [COPY["buttons"]["status_partial"]],
        [COPY["buttons"]["status_none"]],
        [COPY["buttons"]["next"]],
        [COPY["buttons"]["skip_to_final"]],
    ]
    await message.reply_text(COPY["evening"]["prompt"], reply_markup=choice_markup(rows))
    context.user_data["test_waiting_evening_status"] = True
    context.user_data["test_waiting_day_next"] = False
    context.user_data["test_pending_day_status"] = None


async def send_test_final(message, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data["test_waiting_evening_status"] = False
    context.user_data["test_waiting_day_next"] = False
    pending = context.user_data.get("test_pending_day_status")
    if pending in {"full", "partial", "none"}:
        stats = context.user_data.get("test_stats", {"full": 0, "partial": 0, "none": 0})
        stats[pending] = int(stats.get(pending, 0)) + 1
        context.user_data["test_stats"] = stats
        context.user_data["test_pending_day_status"] = None
        context.user_data["test_prev_unmarked"] = False
    stats = context.user_data.get("test_stats", {"full": 0, "partial": 0, "none": 0})
    total = int(stats.get("full", 0)) + int(stats.get("partial", 0)) + int(stats.get("none", 0))
    await message.reply_text(COPY["final"]["title"])
    stats_text = COPY["final"]["stats"].format(
        total=total,
        full=int(stats.get("full", 0)),
        partial=int(stats.get("partial", 0)),
        none=int(stats.get("none", 0)),
    )
    reflection = (context.user_data.get("test_reflection_text") or "").strip()
    kb = InlineKeyboardMarkup(
        [[InlineKeyboardButton(COPY["buttons"]["thanks"], callback_data="test:final:thanks")]]
    )
    if reflection:
        await message.reply_text(stats_text)
        await message.reply_text(COPY["final"]["reflection"].format(reflection_text=reflection))
        await message.reply_text(COPY["final"]["reflection_invite"], reply_markup=kb)
    else:
        await message.reply_text(stats_text, reply_markup=kb)
    return ConversationHandler.END


async def send_test_step(message, context: ContextTypes.DEFAULT_TYPE) -> None:
    steps = context.user_data["test_steps"]
    idx = context.user_data["test_index"]
    text = steps[idx]
    quote_prefix = COPY["morning"]["quote_message"].split("{quote}", 1)[0]
    if text == "__TEST_DAY_LOOP__":
        scenario = context.user_data.get("test_scenario", "before")
        total_days, days_left_start = test_day_params(scenario)
        context.user_data["test_day_loop_active"] = True
        context.user_data["test_day"] = 1
        context.user_data["test_total_days"] = total_days
        context.user_data["test_days_left_start"] = days_left_start
        context.user_data["test_waiting_reflection"] = False
        context.user_data["test_waiting_reflection_confirm"] = False
        context.user_data["test_waiting_time_input"] = None
        context.user_data["test_waiting_evening_status"] = False
        await send_test_day_prompt(message, context)
        return

    if text == COPY["onboarding"]["screen_3"]:
        context.user_data["test_waiting_reflection"] = True
        context.user_data["test_waiting_reflection_confirm"] = False
        context.user_data["test_waiting_time_input"] = None
        context.user_data["test_waiting_timezone_confirm"] = False
        context.user_data["test_waiting_evening_status"] = False
        await message.reply_text(text)
    elif text == COPY["onboarding"]["screen_4"]:
        context.user_data["test_waiting_reflection"] = False
        context.user_data["test_waiting_reflection_confirm"] = False
        context.user_data["test_waiting_time_input"] = None
        context.user_data["test_waiting_timezone_confirm"] = False
        context.user_data["test_waiting_evening_status"] = False
        rows = []
        for i, item in enumerate(COPY["timezone_options"]):
            rows.append([InlineKeyboardButton(item["label"], callback_data=f"test:tz:{i}")])
        await message.reply_text(text, reply_markup=InlineKeyboardMarkup(rows))
    elif text == COPY["onboarding"]["screen_6"]:
        context.user_data["test_waiting_reflection"] = False
        context.user_data["test_waiting_reflection_confirm"] = False
        context.user_data["test_waiting_time_input"] = "morning"
        context.user_data["test_waiting_timezone_confirm"] = False
        context.user_data["test_waiting_evening_status"] = False
        await message.reply_text(text)
    elif text == COPY["onboarding"]["screen_7"]:
        context.user_data["test_waiting_reflection"] = False
        context.user_data["test_waiting_reflection_confirm"] = False
        context.user_data["test_waiting_time_input"] = "evening"
        context.user_data["test_waiting_timezone_confirm"] = False
        context.user_data["test_waiting_evening_status"] = False
        await message.reply_text(text)
    elif idx < len(steps) - 1:
        context.user_data["test_waiting_reflection"] = False
        context.user_data["test_waiting_reflection_confirm"] = False
        context.user_data["test_waiting_time_input"] = None
        context.user_data["test_waiting_timezone_confirm"] = False
        context.user_data["test_waiting_evening_status"] = False
        kb = InlineKeyboardMarkup(
            [[InlineKeyboardButton(COPY["buttons"]["next"], callback_data="test:next")]]
        )
        if text.startswith(quote_prefix):
            await message.reply_text(text, reply_markup=kb, parse_mode="HTML")
        else:
            await message.reply_text(text, reply_markup=kb)
    else:
        context.user_data["test_waiting_reflection"] = False
        context.user_data["test_waiting_reflection_confirm"] = False
        context.user_data["test_waiting_time_input"] = None
        context.user_data["test_waiting_timezone_confirm"] = False
        context.user_data["test_waiting_evening_status"] = False
        if text.startswith(quote_prefix):
            await message.reply_text(text, parse_mode="HTML")
        else:
            await message.reply_text(text)


async def test_next_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    context.user_data["test_index"] += 1
    await send_test_step(query.message, context)
    if context.user_data.get("test_day_loop_active", False):
        return TEST_RUN
    steps = context.user_data["test_steps"]
    idx = context.user_data["test_index"]
    if idx >= len(steps) - 1:
        return ConversationHandler.END
    return TEST_RUN


async def test_timezone_pick_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    idx = int(query.data.split(":")[-1])
    entry = COPY["timezone_options"][idx]
    if entry["tz"] == "other":
        await query.message.reply_text(COPY["onboarding"]["screen_4"], reply_markup=test_other_timezone_markup())
        return TEST_RUN

    context.user_data["test_waiting_timezone_confirm"] = True
    context.user_data["test_timezone_label"] = entry["label"]
    context.user_data["test_timezone_tz"] = entry["tz"]
    await query.message.reply_text(
        COPY["onboarding"]["timezone_confirm"].format(timezone_label=entry["label"]),
        reply_markup=timezone_confirm_markup(prefix="test"),
    )
    return TEST_RUN


async def test_other_timezone_pick_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    data = query.data

    if data == "test:tzother:back":
        rows = []
        for i, item in enumerate(COPY["timezone_options"]):
            rows.append([InlineKeyboardButton(item["label"], callback_data=f"test:tz:{i}")])
        await query.message.reply_text(COPY["onboarding"]["screen_4"], reply_markup=InlineKeyboardMarkup(rows))
        return TEST_RUN

    if data.startswith("test:tzother:pick:"):
        idx = int(data.split(":")[-1])
        chosen = OTHER_TIMEZONE_OPTIONS[idx]
        context.user_data["test_waiting_timezone_confirm"] = True
        context.user_data["test_timezone_label"] = chosen["label"]
        context.user_data["test_timezone_tz"] = chosen["tz"]
        await query.message.reply_text(
            COPY["onboarding"]["timezone_confirm"].format(timezone_label=chosen["label"]),
            reply_markup=timezone_confirm_markup(prefix="test"),
        )
        return TEST_RUN

    return TEST_RUN


async def test_timezone_confirm_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    if not context.user_data.get("test_waiting_timezone_confirm", False):
        return TEST_RUN

    action = query.data.split(":")[-1]
    if action == "tz_edit":
        context.user_data["test_waiting_timezone_confirm"] = False
        rows = []
        for i, item in enumerate(COPY["timezone_options"]):
            rows.append([InlineKeyboardButton(item["label"], callback_data=f"test:tz:{i}")])
        await query.message.reply_text(COPY["onboarding"]["screen_4"], reply_markup=InlineKeyboardMarkup(rows))
        return TEST_RUN

    if action == "tz_save":
        context.user_data["test_waiting_timezone_confirm"] = False
        context.user_data["test_index"] += 1
        await send_test_step(query.message, context)
        steps = context.user_data["test_steps"]
        idx = context.user_data["test_index"]
        if idx >= len(steps) - 1:
            return ConversationHandler.END
        return TEST_RUN

    return TEST_RUN


async def test_reflection_input_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if context.user_data.get("test_waiting_timezone_confirm", False):
        label = context.user_data.get("test_timezone_label", "—")
        await update.message.reply_text(COPY["errors"]["wrong_input"])
        await update.message.reply_text(
            COPY["onboarding"]["timezone_confirm"].format(timezone_label=label),
            reply_markup=timezone_confirm_markup(prefix="test"),
        )
        return TEST_RUN

    if context.user_data.get("test_day_loop_active", False):
        text = (update.message.text or "").strip()
        if text == COPY["buttons"]["skip_to_final"]:
            return await send_test_final(update.message, context)

    if context.user_data.get("test_waiting_day_next", False):
        text = (update.message.text or "").strip()
        if text == COPY["buttons"]["skip_to_final"]:
            return await send_test_final(update.message, context)
        if text == COPY["buttons"]["edit_answer"]:
            context.user_data["test_waiting_day_next"] = False
            context.user_data["test_waiting_evening_status"] = True
            context.user_data["test_pending_day_status"] = None
            rows = [
                [COPY["buttons"]["status_full"]],
                [COPY["buttons"]["status_partial"]],
                [COPY["buttons"]["status_none"]],
                [COPY["buttons"]["next"]],
                [COPY["buttons"]["skip_to_final"]],
            ]
            await update.message.reply_text(COPY["evening"]["repeat_prompt"], reply_markup=choice_markup(rows))
            return TEST_RUN
        if text != COPY["buttons"]["next"]:
            await update.message.reply_text(COPY["errors"]["wrong_input"])
            rows = [
                [COPY["buttons"]["edit_answer"]],
                [COPY["buttons"]["next"]],
                [COPY["buttons"]["skip_to_final"]],
            ]
            await update.message.reply_text(COPY["buttons"]["next"], reply_markup=choice_markup(rows))
            return TEST_RUN

        context.user_data["test_waiting_day_next"] = False
        pending = context.user_data.get("test_pending_day_status")
        if pending in {"full", "partial", "none"}:
            stats = context.user_data.get("test_stats", {"full": 0, "partial": 0, "none": 0})
            stats[pending] = int(stats.get(pending, 0)) + 1
            context.user_data["test_stats"] = stats
            context.user_data["test_prev_unmarked"] = False
            context.user_data["test_pending_day_status"] = None
        current_day = int(context.user_data.get("test_day", 1))
        total_days = int(context.user_data.get("test_total_days", 46))
        if current_day >= total_days:
            return await send_test_final(update.message, context)

        context.user_data["test_day"] = current_day + 1
        await send_test_day_prompt(update.message, context)
        return TEST_RUN

    if context.user_data.get("test_waiting_evening_status", False):
        text = (update.message.text or "").strip()
        status = parse_status_from_text(text)
        if status is not None:
            rows = [
                [COPY["buttons"]["edit_answer"]],
                [COPY["buttons"]["next"]],
                [COPY["buttons"]["skip_to_final"]],
            ]
            await update.message.reply_text(COPY["evening"]["accepted"], reply_markup=choice_markup(rows))
            context.user_data["test_pending_day_status"] = status
            context.user_data["test_waiting_evening_status"] = False
            context.user_data["test_waiting_day_next"] = True
            return TEST_RUN

        if text == COPY["buttons"]["next"]:
            context.user_data["test_prev_unmarked"] = True
            context.user_data["test_waiting_evening_status"] = False
            context.user_data["test_pending_day_status"] = None
            rows = [[COPY["buttons"]["next"]], [COPY["buttons"]["skip_to_final"]]]
            await update.message.reply_text(COPY["evening"]["reminder"], reply_markup=choice_markup(rows))
            context.user_data["test_waiting_day_next"] = True
            return TEST_RUN

        if text == COPY["buttons"]["skip_to_final"]:
            context.user_data["test_waiting_evening_status"] = False
            return await send_test_final(update.message, context)

        await update.message.reply_text(COPY["errors"]["wrong_input"])
        rows = [
            [COPY["buttons"]["status_full"]],
            [COPY["buttons"]["status_partial"]],
            [COPY["buttons"]["status_none"]],
            [COPY["buttons"]["next"]],
            [COPY["buttons"]["skip_to_final"]],
        ]
        await update.message.reply_text(COPY["evening"]["prompt"], reply_markup=choice_markup(rows))
        return TEST_RUN

    if context.user_data.get("test_waiting_time_input"):
        value = (update.message.text or "").strip()
        if not TIME_RE.match(value):
            await update.message.reply_text(COPY["errors"]["invalid_time"])
            current_step = context.user_data["test_steps"][context.user_data["test_index"]]
            await update.message.reply_text(current_step)
            return TEST_RUN

        current_step = context.user_data["test_steps"][context.user_data["test_index"]]
        if current_step == COPY["onboarding"]["screen_6"]:
            await update.message.reply_text(COPY["onboarding"]["morning_saved"])
        elif current_step == COPY["onboarding"]["screen_7"]:
            await update.message.reply_text(COPY["onboarding"]["evening_saved"])

        context.user_data["test_waiting_time_input"] = None
        context.user_data["test_index"] += 1
        await send_test_step(update.message, context)
        steps = context.user_data["test_steps"]
        idx = context.user_data["test_index"]
        if idx >= len(steps) - 1:
            return ConversationHandler.END
        return TEST_RUN

    if not context.user_data.get("test_waiting_reflection", False):
        await update.message.reply_text(COPY["errors"]["wrong_input"])
        return TEST_RUN

    text = (update.message.text or "").strip()
    if len(text) > 500:
        await update.message.reply_text(COPY["errors"]["reflection_too_long"])
        await update.message.reply_text(COPY["onboarding"]["screen_3"])
        return TEST_RUN

    context.user_data["test_reflection_candidate"] = text
    context.user_data["test_waiting_reflection"] = False
    context.user_data["test_waiting_reflection_confirm"] = True
    confirm_text = COPY["onboarding"]["reflection_confirm"].format(reflection_text=text)
    kb = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(COPY["buttons"]["save"], callback_data="test:reflection:save")],
            [InlineKeyboardButton(COPY["buttons"]["edit"], callback_data="test:reflection:edit")],
        ]
    )
    await update.message.reply_text(confirm_text, reply_markup=kb)
    return TEST_RUN


async def test_reflection_confirm_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    if not context.user_data.get("test_waiting_reflection_confirm", False):
        return TEST_RUN

    action = query.data.split(":")[-1]
    if action == "edit":
        context.user_data["test_waiting_reflection"] = True
        context.user_data["test_waiting_reflection_confirm"] = False
        await query.message.reply_text(COPY["onboarding"]["screen_3"])
        return TEST_RUN

    context.user_data["test_reflection_text"] = context.user_data.get("test_reflection_candidate", "").strip()
    context.user_data["test_waiting_reflection"] = False
    context.user_data["test_waiting_reflection_confirm"] = False
    await query.message.reply_text(COPY["onboarding"]["reflection_saved"])
    context.user_data["test_index"] += 1
    await send_test_step(query.message, context)
    steps = context.user_data["test_steps"]
    idx = context.user_data["test_index"]
    if idx >= len(steps) - 1:
        return ConversationHandler.END
    return TEST_RUN


def build_app(token: str) -> Application:
    persistence = DbPersistence(DB_PATH)
    app = Application.builder().token(token).persistence(persistence).build()

    onboarding_conv = ConversationHandler(
        entry_points=[
            CommandHandler("start", start_cmd),
            CommandHandler("restart_onboarding", restart_onboarding_cmd),
        ],
        states={
            ONB_START_GATE: [
                CallbackQueryHandler(onb_start_click, pattern=r"^onb:start$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, onb_wrong_start),
            ],
            ONB_REFLECTION_INPUT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, onb_reflection_input),
                CallbackQueryHandler(onb_reflection_skip, pattern=r"^onb:skip$"),
                CallbackQueryHandler(onb_reflection_back_to_welcome, pattern=r"^onb:back_to_welcome$"),
                # Serverless safety: if state persistence lags, still accept timezone callbacks.
                CallbackQueryHandler(onb_timezone_pick, pattern=r"^tz:"),
                CallbackQueryHandler(onb_timezone_custom_pick, pattern=r"^tzother:"),
                CallbackQueryHandler(onb_timezone_confirm_save, pattern=r"^onb:tz_save$"),
                CallbackQueryHandler(onb_timezone_confirm_edit, pattern=r"^onb:tz_edit$"),
            ],
            ONB_REFLECTION_CONFIRM: [
                CallbackQueryHandler(onb_reflection_save, pattern=r"^onb:save$"),
                CallbackQueryHandler(onb_reflection_edit, pattern=r"^onb:edit$"),
                CallbackQueryHandler(onb_reflection_back_to_prompt, pattern=r"^onb:back_to_prompt$"),
                # Serverless safety: if state persistence lags, still accept timezone callbacks.
                CallbackQueryHandler(onb_timezone_pick, pattern=r"^tz:"),
                CallbackQueryHandler(onb_timezone_custom_pick, pattern=r"^tzother:"),
                CallbackQueryHandler(onb_timezone_confirm_save, pattern=r"^onb:tz_save$"),
                CallbackQueryHandler(onb_timezone_confirm_edit, pattern=r"^onb:tz_edit$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, onb_wrong_reflection_confirm),
            ],
            ONB_TIMEZONE: [
                CallbackQueryHandler(onb_timezone_pick, pattern=r"^tz:"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, onb_wrong_timezone),
            ],
            ONB_TIMEZONE_CONFIRM: [
                CallbackQueryHandler(onb_timezone_confirm_save, pattern=r"^onb:tz_save$"),
                CallbackQueryHandler(onb_timezone_confirm_edit, pattern=r"^onb:tz_edit$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, onb_wrong_timezone_confirm),
            ],
            ONB_TIMEZONE_CUSTOM: [
                CallbackQueryHandler(onb_timezone_custom_pick, pattern=r"^tzother:"),
                CallbackQueryHandler(onb_timezone_pick, pattern=r"^tz:"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, onb_wrong_timezone),
            ],
            ONB_MORNING: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, onb_set_morning),
                CallbackQueryHandler(onb_timezone_pick, pattern=r"^tz:"),
                CallbackQueryHandler(onb_timezone_custom_pick, pattern=r"^tzother:"),
                CallbackQueryHandler(onb_timezone_confirm_save, pattern=r"^onb:tz_save$"),
                CallbackQueryHandler(onb_timezone_confirm_edit, pattern=r"^onb:tz_edit$"),
            ],
            ONB_EVENING: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, onb_set_evening),
                CallbackQueryHandler(onb_timezone_pick, pattern=r"^tz:"),
                CallbackQueryHandler(onb_timezone_custom_pick, pattern=r"^tzother:"),
                CallbackQueryHandler(onb_timezone_confirm_save, pattern=r"^onb:tz_save$"),
                CallbackQueryHandler(onb_timezone_confirm_edit, pattern=r"^onb:tz_edit$"),
            ],
        },
        fallbacks=[
            CommandHandler("start", start_cmd),
            CommandHandler("restart_onboarding", restart_onboarding_cmd),
            CallbackQueryHandler(onb_stale_callback_router, pattern=r"^(onb:|tz:|tzother:)"),
        ],
        per_chat=True,
        per_user=True,
        allow_reentry=True,
    )

    change_time_conv = ConversationHandler(
        entry_points=[
            MessageHandler(
                filters.TEXT & filters.Regex(r"Изменить\s*время"),
                change_time_entry,
            )
        ],
        states={
            CHANGE_TARGET: [MessageHandler(filters.TEXT & ~filters.COMMAND, change_time_target)],
            CHANGE_VALUE: [MessageHandler(filters.TEXT & ~filters.COMMAND, change_time_value)],
        },
        fallbacks=[],
    )

    test_conv = ConversationHandler(
        entry_points=[CommandHandler("test", test_cmd)],
        states={
            TEST_PICK: [CallbackQueryHandler(test_pick_handler, pattern=r"^test:(before|during|april|after)$")],
            TEST_RUN: [
                CallbackQueryHandler(test_next_handler, pattern=r"^test:next$"),
                CallbackQueryHandler(test_timezone_pick_handler, pattern=r"^test:tz:\d+$"),
                CallbackQueryHandler(test_other_timezone_pick_handler, pattern=r"^test:tzother:"),
                CallbackQueryHandler(test_timezone_confirm_handler, pattern=r"^test:tz_(save|edit)$"),
                CallbackQueryHandler(test_reflection_confirm_handler, pattern=r"^test:reflection:(save|edit)$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, test_reflection_input_handler),
            ],
        },
        fallbacks=[],
    )

    app.add_handler(onboarding_conv)
    app.add_handler(test_conv)
    app.add_handler(change_time_conv)
    app.add_handler(CommandHandler("admin_stats", admin_stats_cmd))
    app.add_handler(CommandHandler("admin_nudge_onboarding", admin_nudge_onboarding_cmd))
    app.add_handler(MessageHandler(filters.TEXT & filters.Regex(r"Пауза"), pause_handler))
    app.add_handler(MessageHandler(filters.TEXT & filters.Regex(r"Возобнов"), resume_handler))
    app.add_handler(CallbackQueryHandler(thanks_callback, pattern=r"^presence:thanks$"))
    app.add_handler(CallbackQueryHandler(final_thanks_callback, pattern=r"^final:thanks$"))
    app.add_handler(CallbackQueryHandler(test_final_thanks_callback, pattern=r"^test:final:thanks$"))
    app.add_handler(MessageHandler(filters.COMMAND, unknown_command_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, evening_status_handler))

    return app


def main() -> None:
    token = os.getenv("BOT_TOKEN")
    if not token:
        raise RuntimeError("BOT_TOKEN is required")

    db.init_db(DB_PATH)
    app = build_app(token)
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
