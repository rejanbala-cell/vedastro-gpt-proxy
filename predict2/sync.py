from __future__ import annotations

import json
import threading
from datetime import datetime, timedelta, timezone
from typing import Any

from .config import settings
from .db import connect
from .football_data import FootballDataError, client
from .fixtures import upsert_matches
from .metadata import get_datetime, set_value


SYNC_LOCK_ID = 220260730
THREAD_LOCK = threading.Lock()


def _provider_window() -> tuple:
    start = datetime.now(timezone.utc).date()
    end = start + timedelta(
        days=settings.football_data_sync_days
    )
    return start, end


def sync_now(*, reason: str) -> dict[str, Any]:
    if not THREAD_LOCK.acquire(blocking=False):
        return {
            "status": "busy",
            "message": "A fixture sync is already running.",
        }

    advisory_connection = None
    advisory_locked = False
    started = datetime.now(timezone.utc)
    try:
        with connect() as connection:
            advisory_connection = connection
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT pg_try_advisory_lock(%s)",
                    (SYNC_LOCK_ID,),
                )
                advisory_locked = bool(cursor.fetchone()[0])

            if not advisory_locked:
                return {
                    "status": "busy",
                    "message": (
                        "Another service instance is syncing fixtures."
                    ),
                }

            date_from, date_to = _provider_window()
            response = client.matches(
                date_from=date_from,
                date_to=date_to,
            )
            import_result = upsert_matches(response.matches)
            completed = datetime.now(timezone.utc)

            audit = {
                "status": "ok",
                "reason": reason,
                "started_at": started.isoformat(),
                "completed_at": completed.isoformat(),
                "date_from": date_from.isoformat(),
                "date_to": date_to.isoformat(),
                "result_set": response.result_set,
                "response_headers": response.response_headers,
                **import_result,
            }
            set_value(
                "predict2_football_data_last_sync_at",
                completed.isoformat(),
            )
            set_value(
                "predict2_football_data_last_sync_audit",
                json.dumps(audit, ensure_ascii=False),
            )
            return audit

    except FootballDataError as exc:
        audit = {
            "status": "provider_error",
            "reason": reason,
            "started_at": started.isoformat(),
            "completed_at": datetime.now(
                timezone.utc
            ).isoformat(),
            "http_status": exc.status_code,
            "provider_message": exc.provider_message,
            "message": str(exc),
        }
        set_value(
            "predict2_football_data_last_sync_audit",
            json.dumps(audit, ensure_ascii=False),
        )
        return audit
    except Exception as exc:
        audit = {
            "status": "error",
            "reason": reason,
            "started_at": started.isoformat(),
            "completed_at": datetime.now(
                timezone.utc
            ).isoformat(),
            "error_type": type(exc).__name__,
            "message": str(exc),
        }
        try:
            set_value(
                "predict2_football_data_last_sync_audit",
                json.dumps(audit, ensure_ascii=False),
            )
        except Exception:
            pass
        return audit
    finally:
        if advisory_locked and advisory_connection is not None:
            try:
                with advisory_connection.cursor() as cursor:
                    cursor.execute(
                        "SELECT pg_advisory_unlock(%s)",
                        (SYNC_LOCK_ID,),
                    )
                advisory_connection.commit()
            except Exception:
                pass
        THREAD_LOCK.release()


def sync_if_stale_async() -> None:
    if not settings.football_data_enabled:
        return

    last = None
    try:
        last = get_datetime(
            "predict2_football_data_last_sync_at"
        )
    except Exception:
        pass

    now = datetime.now(timezone.utc)
    if (
        last is not None
        and (now - last).total_seconds()
        < settings.football_data_sync_interval_seconds
    ):
        return

    thread = threading.Thread(
        target=sync_now,
        kwargs={"reason": "startup_stale_check"},
        name="predict2-football-data-sync",
        daemon=True,
    )
    thread.start()
