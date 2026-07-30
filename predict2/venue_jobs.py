from __future__ import annotations

import json
import threading
import traceback
import uuid
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any

from .config import settings
from .db import connect
from .metadata import set_value
from .venue_service import (
    fixtures_needing_venue,
    get_fixture,
    resolve_fixture,
)


VENUE_LOCK_ID = 220260731
_THREAD_LOCK = threading.Lock()
_STATE_LOCK = threading.Lock()

_STATE: dict[str, Any] = {
    "status": "idle",
    "job_id": None,
    "reason": None,
    "started_at": None,
    "completed_at": None,
    "window_days": None,
    "limit": None,
    "fixtures_total": 0,
    "fixtures_completed": 0,
    "verified": 0,
    "unresolved": 0,
    "skipped": 0,
    "errors": 0,
    "current_fixture": None,
    "message": None,
    "error_type": None,
}


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _set(**updates: Any) -> dict[str, Any]:
    with _STATE_LOCK:
        _STATE.update(updates)
        return deepcopy(_STATE)


def get_venue_job_state() -> dict[str, Any]:
    with _STATE_LOCK:
        return deepcopy(_STATE)


def _persist(state: dict[str, Any]) -> None:
    try:
        set_value(
            "predict2_venue_last_job_audit",
            json.dumps(state, ensure_ascii=False, default=str),
        )
        if state.get("status") == "ok":
            set_value(
                "predict2_venue_last_job_at",
                state.get("completed_at"),
            )
    except Exception:
        pass


def _run(
    *,
    job_id: str,
    reason: str,
    window_days: int,
    limit: int,
    fixture_id: int | None,
) -> None:
    advisory_context = None
    advisory_connection = None
    advisory_locked = False

    try:
        advisory_context = connect()
        advisory_connection = advisory_context.__enter__()
        with advisory_connection.cursor() as cursor:
            cursor.execute(
                "SELECT pg_try_advisory_lock(%s)",
                (VENUE_LOCK_ID,),
            )
            advisory_locked = bool(cursor.fetchone()[0])

        if not advisory_locked:
            _set(
                status="busy",
                completed_at=_now().isoformat(),
                message=(
                    "Another Render worker is enriching venues."
                ),
            )
            return

        if fixture_id is not None:
            fixture = get_fixture(fixture_id)
            fixtures = [fixture] if fixture else []
        else:
            fixtures = fixtures_needing_venue(
                window_days=window_days,
                limit=limit,
            )

        _set(
            status="running",
            started_at=_now().isoformat(),
            completed_at=None,
            fixtures_total=len(fixtures),
            fixtures_completed=0,
            verified=0,
            unresolved=0,
            skipped=0,
            errors=0,
            message="Venue enrichment is running.",
        )

        counters = {
            "verified": 0,
            "unresolved": 0,
            "skipped": 0,
            "errors": 0,
        }
        results: list[dict[str, Any]] = []

        for index, fixture in enumerate(fixtures, start=1):
            if not fixture:
                counters["errors"] += 1
                continue
            _set(
                current_fixture={
                    "index": index,
                    "fixture_id": fixture["id"],
                    "home_team": fixture["home_team"],
                    "away_team": fixture["away_team"],
                },
                message=(
                    f"Verifying venue {index} of {len(fixtures)}."
                ),
            )
            try:
                result = resolve_fixture(
                    job_id=job_id,
                    fixture=fixture,
                )
            except Exception as exc:
                result = {
                    "status": "error",
                    "fixture_id": fixture["id"],
                    "error_type": type(exc).__name__,
                    "message": str(exc),
                    "traceback_tail": (
                        traceback.format_exc().splitlines()[-10:]
                    ),
                }

            status = str(result.get("status") or "error")
            if status == "verified":
                counters["verified"] += 1
            elif status == "unresolved":
                counters["unresolved"] += 1
            elif status == "skipped":
                counters["skipped"] += 1
            else:
                counters["errors"] += 1
            results.append(result)
            _set(
                fixtures_completed=index,
                verified=counters["verified"],
                unresolved=counters["unresolved"],
                skipped=counters["skipped"],
                errors=counters["errors"],
            )

        state = _set(
            status="ok",
            completed_at=_now().isoformat(),
            current_fixture=None,
            message=(
                "Venue enrichment completed. "
                f"{counters['verified']} verified, "
                f"{counters['unresolved']} unresolved."
            ),
            results=results,
        )
        _persist(state)

    except Exception as exc:
        state = _set(
            status="error",
            completed_at=_now().isoformat(),
            current_fixture=None,
            error_type=type(exc).__name__,
            message=str(exc) or repr(exc),
            traceback_tail=traceback.format_exc().splitlines()[-12:],
        )
        _persist(state)

    finally:
        if advisory_locked and advisory_connection is not None:
            try:
                with advisory_connection.cursor() as cursor:
                    cursor.execute(
                        "SELECT pg_advisory_unlock(%s)",
                        (VENUE_LOCK_ID,),
                    )
                advisory_connection.commit()
            except Exception:
                pass
        if advisory_context is not None:
            try:
                advisory_context.__exit__(None, None, None)
            except Exception:
                pass
        if _THREAD_LOCK.locked():
            try:
                _THREAD_LOCK.release()
            except RuntimeError:
                pass


def start_venue_job(
    *,
    reason: str,
    window_days: int | None = None,
    limit: int | None = None,
    fixture_id: int | None = None,
) -> dict[str, Any]:
    current = get_venue_job_state()
    if current.get("status") in {"queued", "running"}:
        return {"status": "busy", "job": current}

    if not _THREAD_LOCK.acquire(blocking=False):
        return {
            "status": "busy",
            "job": get_venue_job_state(),
        }

    job_id = uuid.uuid4().hex
    resolved_window = max(
        1,
        min(
            int(
                window_days
                or settings.venue_enrichment_window_days
            ),
            14,
        ),
    )
    resolved_limit = max(
        1,
        min(
            int(
                limit
                or settings.venue_enrichment_max_per_job
            ),
            30,
        ),
    )
    _set(
        status="queued",
        job_id=job_id,
        reason=reason,
        started_at=None,
        completed_at=None,
        window_days=resolved_window,
        limit=resolved_limit,
        fixtures_total=0,
        fixtures_completed=0,
        verified=0,
        unresolved=0,
        skipped=0,
        errors=0,
        current_fixture=None,
        message="Venue enrichment has been queued.",
        error_type=None,
        results=[],
    )
    thread = threading.Thread(
        target=_run,
        kwargs={
            "job_id": job_id,
            "reason": reason,
            "window_days": resolved_window,
            "limit": resolved_limit,
            "fixture_id": fixture_id,
        },
        name=f"predict2-venue-{job_id[:8]}",
        daemon=True,
    )
    thread.start()
    return {
        "status": "accepted",
        "job_id": job_id,
        "job": get_venue_job_state(),
    }
