import asyncio
import os

from flask import Flask, jsonify, request

from worker import run_tick_once

app = Flask(__name__)


def _authorized() -> bool:
    secret = os.getenv("CRON_SECRET", "").strip()
    if not secret:
        return True
    token = request.args.get("token", "")
    header = request.headers.get("X-Cron-Secret", "")
    return token == secret or header == secret


@app.get("/")
def tick():
    if not _authorized():
        return jsonify({"ok": False, "error": "forbidden"}), 403
    asyncio.run(run_tick_once())
    return jsonify({"ok": True})
