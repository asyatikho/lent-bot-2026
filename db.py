import sqlite3
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from typing import Any

try:
    import psycopg
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover - optional in local sqlite-only runs
    psycopg = None
    dict_row = None


def _is_postgres(db_path: str) -> bool:
    return db_path.startswith("postgres://") or db_path.startswith("postgresql://")


def _sql(db_path: str, query: str) -> str:
    if _is_postgres(db_path):
        return query.replace("?", "%s")
    return query


@contextmanager
def get_conn(db_path: str):
    if _is_postgres(db_path):
        if psycopg is None:
            raise RuntimeError("psycopg is required for Postgres DB_PATH")
        conn = psycopg.connect(db_path, row_factory=dict_row)
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()
        return

    conn = sqlite3.connect(db_path)
    conn.row_factory = lambda cursor, row: {col[0]: row[idx] for idx, col in enumerate(cursor.description)}
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db(db_path: str) -> None:
    with get_conn(db_path) as conn:
        if _is_postgres(db_path):
            statements = [
                """
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY,
                    timezone TEXT,
                    morning_time TEXT,
                    evening_time TEXT,
                    morning_time_effective_from TEXT,
                    evening_time_effective_from TEXT,
                    paused INTEGER NOT NULL DEFAULT 0,
                    onboarding_complete INTEGER NOT NULL DEFAULT 0,
                    start_date TEXT,
                    reflection_text TEXT,
                    reflection_skipped INTEGER NOT NULL DEFAULT 0
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS days (
                    user_id BIGINT NOT NULL,
                    local_date TEXT NOT NULL,
                    day_number INTEGER,
                    status TEXT,
                    PRIMARY KEY (user_id, local_date),
                    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS sent_messages (
                    user_id BIGINT NOT NULL,
                    local_date TEXT NOT NULL,
                    message_type TEXT NOT NULL,
                    PRIMARY KEY (user_id, local_date, message_type),
                    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS pending_time_changes (
                    user_id BIGINT NOT NULL,
                    time_type TEXT NOT NULL,
                    new_time TEXT NOT NULL,
                    effective_from TEXT NOT NULL,
                    PRIMARY KEY (user_id, time_type),
                    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS evening_answers (
                    user_id BIGINT NOT NULL,
                    local_date TEXT NOT NULL,
                    first_answered_at_utc TEXT NOT NULL,
                    PRIMARY KEY (user_id, local_date),
                    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS runtime_state (
                    state_key TEXT PRIMARY KEY,
                    state_json TEXT NOT NULL
                )
                """,
            ]
            for stmt in statements:
                conn.execute(stmt)
        else:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    timezone TEXT,
                    morning_time TEXT,
                    evening_time TEXT,
                    morning_time_effective_from TEXT,
                    evening_time_effective_from TEXT,
                    paused INTEGER NOT NULL DEFAULT 0,
                    onboarding_complete INTEGER NOT NULL DEFAULT 0,
                    start_date TEXT,
                    reflection_text TEXT,
                    reflection_skipped INTEGER NOT NULL DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS days (
                    user_id INTEGER NOT NULL,
                    local_date TEXT NOT NULL,
                    day_number INTEGER,
                    status TEXT,
                    PRIMARY KEY (user_id, local_date),
                    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS sent_messages (
                    user_id INTEGER NOT NULL,
                    local_date TEXT NOT NULL,
                    message_type TEXT NOT NULL,
                    PRIMARY KEY (user_id, local_date, message_type),
                    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS pending_time_changes (
                    user_id INTEGER NOT NULL,
                    time_type TEXT NOT NULL,
                    new_time TEXT NOT NULL,
                    effective_from TEXT NOT NULL,
                    PRIMARY KEY (user_id, time_type),
                    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS evening_answers (
                    user_id INTEGER NOT NULL,
                    local_date TEXT NOT NULL,
                    first_answered_at_utc TEXT NOT NULL,
                    PRIMARY KEY (user_id, local_date),
                    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS runtime_state (
                    state_key TEXT PRIMARY KEY,
                    state_json TEXT NOT NULL
                );
                """
            )


def get_user(db_path: str, user_id: int) -> dict[str, Any] | None:
    with get_conn(db_path) as conn:
        return conn.execute(_sql(db_path, "SELECT * FROM users WHERE user_id = ?"), (user_id,)).fetchone()


def upsert_user(db_path: str, user_id: int, **fields: Any) -> None:
    with get_conn(db_path) as conn:
        exists = conn.execute(_sql(db_path, "SELECT 1 FROM users WHERE user_id = ?"), (user_id,)).fetchone()
        if exists:
            keys = list(fields.keys())
            if not keys:
                return
            set_clause = ", ".join(f"{k} = ?" for k in keys)
            values = [fields[k] for k in keys]
            values.append(user_id)
            conn.execute(_sql(db_path, f"UPDATE users SET {set_clause} WHERE user_id = ?"), values)
        else:
            payload = {
                "user_id": user_id,
                "timezone": None,
                "morning_time": None,
                "evening_time": None,
                "morning_time_effective_from": None,
                "evening_time_effective_from": None,
                "paused": 0,
                "onboarding_complete": 0,
                "start_date": None,
                "reflection_text": None,
                "reflection_skipped": 0,
            }
            payload.update(fields)
            cols = ", ".join(payload.keys())
            placeholders = ", ".join("?" for _ in payload)
            conn.execute(
                _sql(db_path, f"INSERT INTO users ({cols}) VALUES ({placeholders})"),
                list(payload.values()),
            )


def list_active_users(db_path: str) -> list[dict[str, Any]]:
    with get_conn(db_path) as conn:
        rows = conn.execute(
            "SELECT * FROM users WHERE onboarding_complete = 1"
        ).fetchall()
    return rows


def record_sent_message(db_path: str, user_id: int, local_date: date, message_type: str) -> bool:
    with get_conn(db_path) as conn:
        cur = conn.execute(
            _sql(
                db_path,
                """
                INSERT INTO sent_messages (user_id, local_date, message_type)
                VALUES (?, ?, ?)
                ON CONFLICT (user_id, local_date, message_type) DO NOTHING
                """,
            ),
            (user_id, local_date.isoformat(), message_type),
        )
        return bool(getattr(cur, "rowcount", 0) == 1)


def has_sent_message(db_path: str, user_id: int, local_date: date, message_type: str) -> bool:
    with get_conn(db_path) as conn:
        row = conn.execute(
            _sql(db_path, "SELECT 1 FROM sent_messages WHERE user_id = ? AND local_date = ? AND message_type = ?"),
            (user_id, local_date.isoformat(), message_type),
        ).fetchone()
    return bool(row)


def get_day(db_path: str, user_id: int, local_date: date) -> dict[str, Any] | None:
    with get_conn(db_path) as conn:
        return conn.execute(
            _sql(db_path, "SELECT * FROM days WHERE user_id = ? AND local_date = ?"),
            (user_id, local_date.isoformat()),
        ).fetchone()


def ensure_day_row(db_path: str, user_id: int, local_date: date, start_date: date) -> dict[str, Any]:
    existing = get_day(db_path, user_id, local_date)
    if existing:
        return existing
    if local_date < start_date:
        day_number = None
    else:
        with get_conn(db_path) as conn:
            prev = conn.execute(
                _sql(
                    db_path,
                    """
                    SELECT COALESCE(MAX(day_number), 0) AS max_day
                    FROM days
                    WHERE user_id = ? AND local_date < ?
                    """,
                ),
                (user_id, local_date.isoformat()),
            ).fetchone()
            day_number = int(prev["max_day"]) + 1
            conn.execute(
                _sql(db_path, "INSERT INTO days (user_id, local_date, day_number, status) VALUES (?, ?, ?, NULL)"),
                (user_id, local_date.isoformat(), day_number),
            )
    return get_day(db_path, user_id, local_date)


def set_day_status(db_path: str, user_id: int, local_date: date, status: str) -> None:
    with get_conn(db_path) as conn:
        conn.execute(
            _sql(db_path, "UPDATE days SET status = ? WHERE user_id = ? AND local_date = ?"),
            (status, user_id, local_date.isoformat()),
        )


def can_update_evening_status(db_path: str, user_id: int, local_date: date, now_utc: datetime) -> bool:
    with get_conn(db_path) as conn:
        row = conn.execute(
            _sql(db_path, "SELECT first_answered_at_utc FROM evening_answers WHERE user_id = ? AND local_date = ?"),
            (user_id, local_date.isoformat()),
        ).fetchone()
        if row is None:
            conn.execute(
                _sql(db_path, "INSERT INTO evening_answers (user_id, local_date, first_answered_at_utc) VALUES (?, ?, ?)"),
                (user_id, local_date.isoformat(), now_utc.isoformat()),
            )
            return True
    first = datetime.fromisoformat(row["first_answered_at_utc"])
    return now_utc <= first + timedelta(minutes=10)


def get_evening_first_answer_time(db_path: str, user_id: int, local_date: date) -> datetime | None:
    with get_conn(db_path) as conn:
        row = conn.execute(
            _sql(db_path, "SELECT first_answered_at_utc FROM evening_answers WHERE user_id = ? AND local_date = ?"),
            (user_id, local_date.isoformat()),
        ).fetchone()
    if not row:
        return None
    return datetime.fromisoformat(row["first_answered_at_utc"])


def get_stats(db_path: str, user_id: int) -> dict[str, int]:
    with get_conn(db_path) as conn:
        rows = conn.execute(
            _sql(
                db_path,
                """
                SELECT
                    SUM(CASE WHEN status IN ('full', 'partial', 'none') THEN 1 ELSE 0 END) AS total,
                    SUM(CASE WHEN status = 'full' THEN 1 ELSE 0 END) AS full,
                    SUM(CASE WHEN status = 'partial' THEN 1 ELSE 0 END) AS partial,
                    SUM(CASE WHEN status = 'none' THEN 1 ELSE 0 END) AS none
                FROM days
                WHERE user_id = ?
                """,
            ),
            (user_id,),
        ).fetchone()
    return {
        "total": int(rows["total"] or 0),
        "full": int(rows["full"] or 0),
        "partial": int(rows["partial"] or 0),
        "none": int(rows["none"] or 0),
    }


def set_pause(db_path: str, user_id: int, paused: bool) -> None:
    upsert_user(db_path, user_id, paused=1 if paused else 0)


def queue_time_change(
    db_path: str,
    user_id: int,
    time_type: str,
    new_time: str,
    effective_from: date,
) -> None:
    with get_conn(db_path) as conn:
        conn.execute(
            _sql(
                db_path,
                """
                INSERT INTO pending_time_changes (user_id, time_type, new_time, effective_from)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(user_id, time_type) DO UPDATE SET
                    new_time = excluded.new_time,
                    effective_from = excluded.effective_from
                """,
            ),
            (user_id, time_type, new_time, effective_from.isoformat()),
        )


def apply_due_time_changes(db_path: str, user_id: int, local_date: date) -> None:
    with get_conn(db_path) as conn:
        rows = conn.execute(
            _sql(
                db_path,
                """
                SELECT * FROM pending_time_changes
                WHERE user_id = ? AND effective_from <= ?
                """,
            ),
            (user_id, local_date.isoformat()),
        ).fetchall()
        for row in rows:
            if row["time_type"] == "morning":
                conn.execute(
                    _sql(db_path, "UPDATE users SET morning_time = ?, morning_time_effective_from = ? WHERE user_id = ?"),
                    (row["new_time"], row["effective_from"], user_id),
                )
            elif row["time_type"] == "evening":
                conn.execute(
                    _sql(db_path, "UPDATE users SET evening_time = ?, evening_time_effective_from = ? WHERE user_id = ?"),
                    (row["new_time"], row["effective_from"], user_id),
                )
            conn.execute(
                _sql(db_path, "DELETE FROM pending_time_changes WHERE user_id = ? AND time_type = ?"),
                (user_id, row["time_type"]),
            )


def get_pending_time_change(db_path: str, user_id: int, time_type: str) -> dict[str, Any] | None:
    with get_conn(db_path) as conn:
        return conn.execute(
            _sql(db_path, "SELECT * FROM pending_time_changes WHERE user_id = ? AND time_type = ?"),
            (user_id, time_type),
        ).fetchone()


def get_last_day_number(db_path: str, user_id: int) -> int:
    with get_conn(db_path) as conn:
        row = conn.execute(
            _sql(db_path, "SELECT COALESCE(MAX(day_number), 0) AS n FROM days WHERE user_id = ?"),
            (user_id,),
        ).fetchone()
    return int(row["n"] or 0)


def delete_user(db_path: str, user_id: int) -> None:
    with get_conn(db_path) as conn:
        conn.execute(_sql(db_path, "DELETE FROM users WHERE user_id = ?"), (user_id,))


def get_runtime_state(db_path: str, state_key: str) -> str | None:
    with get_conn(db_path) as conn:
        row = conn.execute(
            _sql(db_path, "SELECT state_json FROM runtime_state WHERE state_key = ?"),
            (state_key,),
        ).fetchone()
    if not row:
        return None
    return row["state_json"]


def set_runtime_state(db_path: str, state_key: str, state_json: str) -> None:
    with get_conn(db_path) as conn:
        conn.execute(
            _sql(
                db_path,
                """
                INSERT INTO runtime_state (state_key, state_json)
                VALUES (?, ?)
                ON CONFLICT(state_key) DO UPDATE SET state_json = excluded.state_json
                """,
            ),
            (state_key, state_json),
        )
