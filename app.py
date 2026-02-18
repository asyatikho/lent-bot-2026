import asyncio
import os
import threading

from flask import Flask, jsonify, request
from telegram import Update

import db
from bot import DB_PATH, build_app
from worker import run_tick_once

app = Flask(__name__)

_LOCK = threading.Lock()
_LOOP = asyncio.new_event_loop()
_TG_APP = None


def _run(coro):
    with _LOCK:
        return _LOOP.run_until_complete(coro)


def _ensure_tg_app():
    global _TG_APP
    if _TG_APP is not None:
        return _TG_APP

    token = os.getenv("BOT_TOKEN")
    if not token:
        raise RuntimeError("BOT_TOKEN is required")

    with _LOCK:
        if _TG_APP is None:
            db.init_db(DB_PATH)
            _TG_APP = build_app(token)
            _LOOP.run_until_complete(_TG_APP.initialize())
    return _TG_APP


def _check_telegram_secret() -> bool:
    required = os.getenv("TELEGRAM_WEBHOOK_SECRET", "").strip()
    if not required:
        return True
    got = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
    return got == required


def _check_cron_secret() -> bool:
    required = os.getenv("CRON_SECRET", "").strip()
    if not required:
        return True
    got_qs = request.args.get("token", "")
    got_header = request.headers.get("X-Cron-Secret", "")
    return got_qs == required or got_header == required


@app.get("/")
def health():
    return jsonify({"ok": True, "service": "lent-bot-api"})


@app.post("/api/webhook")
def telegram_webhook():
    if not _check_telegram_secret():
        return jsonify({"ok": False, "error": "forbidden"}), 403

    try:
        payload = request.get_json(silent=True)
        if not isinstance(payload, dict):
            return jsonify({"ok": False, "error": "bad json"}), 400

        tg_app = _ensure_tg_app()
        update = Update.de_json(payload, tg_app.bot)
        _run(tg_app.process_update(update))
        return jsonify({"ok": True})
    except Exception as e:
        return (
            jsonify(
                {
                    "ok": False,
                    "error": type(e).__name__,
                    "message": str(e),
                }
            ),
            500,
        )


@app.get("/api/cron/tick")
def cron_tick():
    if not _check_cron_secret():
        return jsonify({"ok": False, "error": "forbidden"}), 403

    try:
        result = asyncio.run(run_tick_once())
        return jsonify({"ok": True, **result})
    except Exception as e:
        return (
            jsonify(
                {
                    "ok": False,
                    "error": type(e).__name__,
                    "message": str(e),
                    "has_bot_token": bool(os.getenv("BOT_TOKEN")),
                    "has_database_url": bool(os.getenv("DATABASE_URL") or os.getenv("DB_PATH")),
                }
            ),
            500,
        )
