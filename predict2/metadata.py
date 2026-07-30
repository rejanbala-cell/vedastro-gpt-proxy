from __future__ import annotations

from datetime import datetime
from typing import Any

from .db import connect


def get(key: str) -> str | None:
    with connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT metadata_value
                FROM app_metadata
                WHERE metadata_key = %s
                """,
                (key,),
            )
            row = cursor.fetchone()
    return str(row[0]) if row else None


def set_value(key: str, value: Any) -> None:
    text = str(value)
    with connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO app_metadata (
                    metadata_key, metadata_value, updated_at
                )
                VALUES (%s, %s, NOW())
                ON CONFLICT (metadata_key)
                DO UPDATE SET
                    metadata_value = EXCLUDED.metadata_value,
                    updated_at = NOW()
                """,
                (key, text),
            )
        connection.commit()


def get_datetime(key: str) -> datetime | None:
    raw = get(key)
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        return None
