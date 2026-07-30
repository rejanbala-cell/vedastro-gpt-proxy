from __future__ import annotations

import json
import threading
import traceback
import uuid
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
from typing import Any

from .config import settings
from .db import connect
from .football_data import FootballDataError, client
from .fixtures import upsert_matches
from .metadata import get_datetime, set_value


SYNC_LOCK_ID = 220260730
SYNC_CHUNK_DAYS = 14
THREAD_LOCK = threading.Lock()
STATE_LOCK = threading.Lock()

_SYNC_STATE: dict[str, Any] = {
    "status": "idle",
    "job_id": None,
    "reason": None,
    "started_at": None,
    "completed_at": None,
    "date_from": None,
    "date_to": None,
    "chunks_total": 0,
    "chunks_completed": 0,
    "received": 0,
    "imported": 0,
    "skipped": 0,
    "current_chunk": None,
    "error_type": None,
    "message": None,
    "provider_http_status": None,
    "provider_message": None,
}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _provider_window() -> tuple[date, date]:
    start = _utc_now().date()
    end = start + timedelta(
        days=settings.football_data_sync_days - 1
    )
    return start, end


def _chunks(
    start: date,
    end: date,
) -> list[tuple[date, date]]:
    output: list[tuple[date, date]] = []
    cursor = start
    while cursor <= end:
        chunk_end = min(
            cursor + timedelta(days=SYNC_CHUNK_DAYS - 1),
            end,
        )
        output.append((cursor, chunk_end))
        cursor = chunk_end + timedelta(days=1)
    return output


def _set_state(**updates: Any) -> dict[str, Any]:
    with STATE_LOCK:
        _SYNC_STATE.update(updates)
        return deepcopy(_SYNC_STATE)


def get_sync_state() -> dict[str, Any]:
    with STATE_LOCK:
        return deepcopy(_SYNC_STATE)


def _safe_persist_audit(audit: dict[str, Any]) -> None:
    try:
        set_value(
            "predict2_football_data_last_sync_audit",
            json.dumps(
                audit,
                ensure_ascii=False,
                default=str,
            ),
        )
    except Exception:
        # An audit-write failure must never hide the original sync error.
        pass


def _complete_success(
    *,
    job_id: str,
    started: datetime,
    date_from: date,
    date_to: date,
    chunks_total: int,
    totals: dict[str, int],
    provider_audits: list[dict[str, Any]],
) -> dict[str, Any]:
    completed = _utc_now()
    audit = _set_state(
        status="ok",
        job_id=job_id,
        completed_at=completed.isoformat(),
        current_chunk=None,
        chunks_completed=chunks_total,
        received=totals["received"],
        imported=totals["imported"],
        skipped=totals["skipped"],
        error_type=None,
        message=None,
        provider_http_status=None,
        provider_message=None,
    )
    audit.update({
        "started_at": started.isoformat(),
        "date_from": date_from.isoformat(),
        "date_to": date_to.isoformat(),
        "provider_chunks": provider_audits,
    })
    try:
        set_value(
            "predict2_football_data_last_sync_at",
            completed.isoformat(),
        )
    except Exception as exc:
        audit = _set_state(
            status="error",
            error_type=type(exc).__name__,
            message=(
                "Fixtures were imported, but the final sync timestamp "
                f"could not be saved: {exc}"
            ),
        )
    _safe_persist_audit(audit)
    return audit


def _run_sync_job(
    *,
    job_id: str,
    reason: str,
) -> None:
    advisory_connection = None
    advisory_locked = False
    started = _utc_now()
    date_from, date_to = _provider_window()
    windows = _chunks(date_from, date_to)
    totals = {"received": 0, "imported": 0, "skipped": 0}
    provider_audits: list[dict[str, Any]] = []

    try:
        advisory_context = connect()
        advisory_connection = advisory_context.__enter__()
        with advisory_connection.cursor() as cursor:
            cursor.execute(
                "SELECT pg_try_advisory_lock(%s)",
                (SYNC_LOCK_ID,),
            )
            advisory_locked = bool(cursor.fetchone()[0])

        if not advisory_locked:
            _set_state(
                status="busy",
                message=(
                    "Another Render worker is already syncing fixtures."
                ),
                completed_at=_utc_now().isoformat(),
            )
            return

        _set_state(
            status="running",
            job_id=job_id,
            reason=reason,
            started_at=started.isoformat(),
            completed_at=None,
            date_from=date_from.isoformat(),
            date_to=date_to.isoformat(),
            chunks_total=len(windows),
            chunks_completed=0,
            received=0,
            imported=0,
            skipped=0,
            error_type=None,
            message=None,
            provider_http_status=None,
            provider_message=None,
        )

        for index, (chunk_from, chunk_to) in enumerate(
            windows,
            start=1,
        ):
            _set_state(
                current_chunk={
                    "index": index,
                    "date_from": chunk_from.isoformat(),
                    "date_to": chunk_to.isoformat(),
                },
                message=(
                    f"Downloading fixture chunk {index} "
                    f"of {len(windows)}."
                ),
            )

            response = client.matches(
                date_from=chunk_from,
                date_to=chunk_to,
            )
            imported = upsert_matches(response.matches)

            totals["received"] += imported["received"]
            totals["imported"] += imported["imported"]
            totals["skipped"] += imported["skipped"]

            provider_audits.append({
                "index": index,
                "date_from": chunk_from.isoformat(),
                "date_to": chunk_to.isoformat(),
                "result_set": response.result_set,
                "response_headers": response.response_headers,
                **imported,
            })
            _set_state(
                chunks_completed=index,
                received=totals["received"],
                imported=totals["imported"],
                skipped=totals["skipped"],
                message=(
                    f"Imported chunk {index} of {len(windows)}."
                ),
            )

        _complete_success(
            job_id=job_id,
            started=started,
            date_from=date_from,
            date_to=date_to,
            chunks_total=len(windows),
            totals=totals,
            provider_audits=provider_audits,
        )

    except FootballDataError as exc:
        audit = _set_state(
            status="provider_error",
            completed_at=_utc_now().isoformat(),
            error_type=type(exc).__name__,
            message=str(exc),
            provider_http_status=exc.status_code,
            provider_message=exc.provider_message,
            current_chunk=get_sync_state().get("current_chunk"),
        )
        audit["retry_after_seconds"] = exc.retry_after_seconds
        audit["provider_chunks"] = provider_audits
        _safe_persist_audit(audit)

    except Exception as exc:
        audit = _set_state(
            status="error",
            completed_at=_utc_now().isoformat(),
            error_type=type(exc).__name__,
            message=str(exc) or repr(exc),
            traceback_tail=traceback.format_exc().splitlines()[-12:],
            current_chunk=get_sync_state().get("current_chunk"),
        )
        audit["provider_chunks"] = provider_audits
        _safe_persist_audit(audit)

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

        if advisory_connection is not None:
            try:
                advisory_context.__exit__(None, None, None)
            except Exception:
                pass

        if THREAD_LOCK.locked():
            try:
                THREAD_LOCK.release()
            except RuntimeError:
                pass


def start_sync(*, reason: str) -> dict[str, Any]:
    current = get_sync_state()
    if current.get("status") == "running":
        return {
            "status": "busy",
            "job": current,
        }

    if not THREAD_LOCK.acquire(blocking=False):
        return {
            "status": "busy",
            "job": get_sync_state(),
        }

    job_id = uuid.uuid4().hex
    _set_state(
        status="queued",
        job_id=job_id,
        reason=reason,
        started_at=None,
        completed_at=None,
        message="Fixture sync has been queued.",
        error_type=None,
        provider_http_status=None,
        provider_message=None,
    )
    thread = threading.Thread(
        target=_run_sync_job,
        kwargs={
            "job_id": job_id,
            "reason": reason,
        },
        name=f"predict2-sync-{job_id[:8]}",
        daemon=True,
    )
    thread.start()
    return {
        "status": "accepted",
        "job_id": job_id,
        "job": get_sync_state(),
    }


def sync_if_stale_async() -> dict[str, Any]:
    if not settings.football_data_enabled:
        return {"status": "disabled"}

    last = None
    try:
        last = get_datetime(
            "predict2_football_data_last_sync_at"
        )
    except Exception:
        pass

    now = _utc_now()
    if (
        last is not None
        and (now - last).total_seconds()
        < settings.football_data_sync_interval_seconds
    ):
        return {
            "status": "fresh",
            "last_sync_at": last.isoformat(),
        }

    return start_sync(reason="stale_check")
