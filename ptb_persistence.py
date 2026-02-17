import json
from typing import Any

from telegram.ext import BasePersistence, PersistenceInput

import db


class DbPersistence(BasePersistence):
    def __init__(self, db_path: str):
        super().__init__(
            store_data=PersistenceInput(user_data=True, chat_data=True, bot_data=True, callback_data=True),
            update_interval=0,
        )
        self.db_path = db_path

    def _load_json(self, key: str, default: Any):
        raw = db.get_runtime_state(self.db_path, key)
        if not raw:
            return default
        try:
            return json.loads(raw)
        except Exception:
            return default

    def _save_json(self, key: str, value: Any) -> None:
        db.set_runtime_state(self.db_path, key, json.dumps(value, ensure_ascii=False))

    async def get_user_data(self) -> dict[int, dict[Any, Any]]:
        data = self._load_json("user_data", {})
        return {int(k): v for k, v in data.items()}

    async def get_chat_data(self) -> dict[int, dict[Any, Any]]:
        data = self._load_json("chat_data", {})
        return {int(k): v for k, v in data.items()}

    async def get_bot_data(self) -> dict[Any, Any]:
        return self._load_json("bot_data", {})

    async def get_callback_data(self):
        return self._load_json("callback_data", None)

    async def get_conversations(self, name: str):
        items = self._load_json(f"conv:{name}", [])
        out: dict[tuple[int | str, ...], object] = {}
        for item in items:
            try:
                out[tuple(item["key"])] = item["state"]
            except Exception:
                continue
        return out

    async def update_conversation(self, name: str, key: tuple[int | str, ...], new_state: object | None) -> None:
        current = await self.get_conversations(name)
        if new_state is None:
            current.pop(tuple(key), None)
        else:
            current[tuple(key)] = new_state
        payload = [{"key": list(k), "state": v} for k, v in current.items()]
        self._save_json(f"conv:{name}", payload)

    async def update_user_data(self, user_id: int, data: dict[Any, Any]) -> None:
        current = self._load_json("user_data", {})
        current[str(user_id)] = data
        self._save_json("user_data", current)

    async def update_chat_data(self, chat_id: int, data: dict[Any, Any]) -> None:
        current = self._load_json("chat_data", {})
        current[str(chat_id)] = data
        self._save_json("chat_data", current)

    async def update_bot_data(self, data: dict[Any, Any]) -> None:
        self._save_json("bot_data", data)

    async def update_callback_data(self, data) -> None:
        self._save_json("callback_data", data)

    async def drop_chat_data(self, chat_id: int) -> None:
        current = self._load_json("chat_data", {})
        current.pop(str(chat_id), None)
        self._save_json("chat_data", current)

    async def drop_user_data(self, user_id: int) -> None:
        current = self._load_json("user_data", {})
        current.pop(str(user_id), None)
        self._save_json("user_data", current)

    async def refresh_user_data(self, user_id: int, user_data: dict[Any, Any]) -> None:
        latest = (await self.get_user_data()).get(user_id)
        if latest is not None:
            user_data.clear()
            user_data.update(latest)

    async def refresh_chat_data(self, chat_id: int, chat_data: dict[Any, Any]) -> None:
        latest = (await self.get_chat_data()).get(chat_id)
        if latest is not None:
            chat_data.clear()
            chat_data.update(latest)

    async def refresh_bot_data(self, bot_data: dict[Any, Any]) -> None:
        latest = await self.get_bot_data()
        bot_data.clear()
        bot_data.update(latest)

    async def flush(self) -> None:
        return
