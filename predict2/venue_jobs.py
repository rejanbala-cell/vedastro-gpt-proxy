from __future__ import annotations

import json
import threading
import traceback
import uuid
from datetime import datetime, timezone
from typing import Any, Callable

from .config import settings
from .db import connect
from .metadata import set_value
from .venue_service import (
    fixtures_needing_venue,
    get_fixture,
    resolve_fixture,
)


VENUE_RUN_LOCK_ID = 220260731
VENUE_START_LOCK_ID = 220260732

_JSON_COLUMNS = {
    "current_fixture",
    "traceback_json",
    "results_json",
}

_UPDATE_COLUMNS = {
    "status",
    "fixtures_total",
    "fixtures_completed",
    "verified",
    "unresolved",
    "skipped",
    "errors",
    "current_fixture",
    "current_stage",
    "stage_started_at",
    "last_progress_at",
    "message",
    "error_type",
    "traceback_json",
    "results_json",
    "started_at",
    "completed_at",
}


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _idle_state() -> dict[str, Any]:
    return {
        "status": "idle",
        "job_id": None,
        "reason": None,
        "fixture_id": None,
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
        "current_stage": None,
        "stage_started_at": None,
        "last_progress_at": None,
        "elapsed_seconds": 0,
        "message": None,
        "error_type": None,
        "traceback_tail": None,
        "results": [],
        "state_store": "postgresql",
    }


def _serialize(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        default=str,
    )


def _row_to_state(row: dict[str, Any] | None) -> dict[str, Any]:
    if not row:
        return _idle_state()

    started_at = row.get("started_at")
    completed_at = row.get("completed_at")
    elapsed = 0
    if isinstance(started_at, datetime):
        elapsed_end = (
            completed_at
            if isinstance(completed_at, datetime)
            else _now()
        )
        elapsed = max(
            0,
            int((elapsed_end - started_at).total_seconds()),
        )

    traceback_value = row.get("traceback_json")
    results_value = row.get("results_json")
    if isinstance(traceback_value, str):
        try:
            traceback_value = json.loads(traceback_value)
        except ValueError:
            traceback_value = [traceback_value]
    if isinstance(results_value, str):
        try:
            results_value = json.loads(results_value)
        except ValueError:
            results_value = []

    return {
        "status": row.get("status"),
        "job_id": row.get("job_id"),
        "reason": row.get("reason"),
        "fixture_id": row.get("fixture_id"),
        "started_at": (
            started_at.isoformat()
            if isinstance(started_at, datetime)
            else None
        ),
        "completed_at": (
            row["completed_at"].isoformat()
            if isinstance(row.get("completed_at"), datetime)
            else None
        ),
        "window_days": row.get("window_days"),
        "limit": row.get("limit_count"),
        "fixtures_total": row.get("fixtures_total") or 0,
        "fixtures_completed": row.get("fixtures_completed") or 0,
        "verified": row.get("verified") or 0,
        "unresolved": row.get("unresolved") or 0,
        "skipped": row.get("skipped") or 0,
        "errors": row.get("errors") or 0,
        "current_fixture": row.get("current_fixture"),
        "current_stage": row.get("current_stage"),
        "stage_started_at": (
            row["stage_started_at"].isoformat()
            if isinstance(row.get("stage_started_at"), datetime)
            else None
        ),
        "last_progress_at": (
            row["last_progress_at"].isoformat()
            if isinstance(row.get("last_progress_at"), datetime)
            else None
        ),
        "elapsed_seconds": elapsed,
        "message": row.get("message"),
        "error_type": row.get("error_type"),
        "traceback_tail": traceback_value,
        "results": (
            results_value
            if isinstance(results_value, list)
            else []
        ),
        "created_at": (
            row["created_at"].isoformat()
            if isinstance(row.get("created_at"), datetime)
            else None
        ),
        "state_store": "postgresql",
    }


def _job_row(
    job_id: str | None = None,
) -> dict[str, Any] | None:
    where = "WHERE job_id = %s" if job_id else ""
    params = (job_id,) if job_id else ()
    with connect(dict_rows=True) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT
                    job_id,
                    status,
                    reason,
                    fixture_id,
                    window_days,
                    limit_count,
                    fixtures_total,
                    fixtures_completed,
                    verified,
                    unresolved,
                    skipped,
                    errors,
                    current_fixture,
                    current_stage,
                    stage_started_at,
                    last_progress_at,
                    message,
                    error_type,
                    traceback_json,
                    results_json,
                    created_at,
                    started_at,
                    completed_at
                FROM predict2_venue_jobs
                {where}
                ORDER BY created_at DESC
                LIMIT 1
                """,
                params,
            )
            row = cursor.fetchone()
    return dict(row) if row else None


def recover_stale_venue_jobs() -> int:
    """
    Mark jobs abandoned after a process restart or dead worker.

    Every normal external stage has a bounded timeout well below this
    threshold, and each stage updates last_progress_at.
    """
    with connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE predict2_venue_jobs
                SET
                    status = 'error',
                    error_type = 'WorkerStopped',
                    message = (
                        'The venue worker stopped or the service '
                        'restarted before the job completed.'
                    ),
                    current_stage = NULL,
                    completed_at = NOW(),
                    last_progress_at = NOW()
                WHERE status IN ('queued', 'running')
                  AND last_progress_at < (
                      NOW() - (%s * INTERVAL '1 minute')
                  )
                """,
                (settings.venue_job_stale_minutes,),
            )
            updated = int(cursor.rowcount or 0)
        connection.commit()
    return updated


def get_venue_job_state(
    job_id: str | None = None,
) -> dict[str, Any]:
    recover_stale_venue_jobs()
    return _row_to_state(_job_row(job_id))


def _update_job(
    job_id: str,
    **updates: Any,
) -> None:
    invalid = set(updates) - _UPDATE_COLUMNS
    if invalid:
        raise ValueError(
            f"Unsupported venue job fields: {sorted(invalid)}"
        )
    if not updates:
        return

    assignments: list[str] = []
    values: list[Any] = []
    for column, value in updates.items():
        if column in _JSON_COLUMNS:
            assignments.append(f"{column} = %s::jsonb")
            values.append(_serialize(value))
        else:
            assignments.append(f"{column} = %s")
            values.append(value)

    values.append(job_id)
    with connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                UPDATE predict2_venue_jobs
                SET {", ".join(assignments)}
                WHERE job_id = %s
                """,
                values,
            )
        connection.commit()


def _progress(
    job_id: str,
    stage: str,
    message: str,
) -> None:
    now = _now()
    _update_job(
        job_id,
        current_stage=stage,
        stage_started_at=now,
        last_progress_at=now,
        message=message,
    )


def _persist_summary(state: dict[str, Any]) -> None:
    try:
        set_value(
            "predict2_venue_last_job_audit",
            _serialize(state),
        )
        if state.get("status") == "ok":
            set_value(
                "predict2_venue_last_job_at",
                state.get("completed_at"),
            )
    except Exception:
        pass


def _create_job(
    *,
    reason: str,
    window_days: int,
    limit: int,
    fixture_id: int | None,
) -> dict[str, Any]:
    job_id = uuid.uuid4().hex
    now = _now()

    with connect(dict_rows=True) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT pg_advisory_xact_lock(%s)",
                (VENUE_START_LOCK_ID,),
            )
            cursor.execute(
                """
                UPDATE predict2_venue_jobs
                SET
                    status = 'error',
                    error_type = 'WorkerStopped',
                    message = (
                        'The previous venue worker stopped before '
                        'the job completed.'
                    ),
                    current_stage = NULL,
                    completed_at = NOW(),
                    last_progress_at = NOW()
                WHERE status IN ('queued', 'running')
                  AND last_progress_at < (
                      NOW() - (%s * INTERVAL '1 minute')
                  )
                """,
                (settings.venue_job_stale_minutes,),
            )
            cursor.execute(
                """
                SELECT
                    job_id,
                    status,
                    reason,
                    fixture_id,
                    window_days,
                    limit_count,
                    fixtures_total,
                    fixtures_completed,
                    verified,
                    unresolved,
                    skipped,
                    errors,
                    current_fixture,
                    current_stage,
                    stage_started_at,
                    last_progress_at,
                    message,
                    error_type,
                    traceback_json,
                    results_json,
                    created_at,
                    started_at,
                    completed_at
                FROM predict2_venue_jobs
                WHERE status IN ('queued', 'running')
                ORDER BY created_at DESC
                LIMIT 1
                """
            )
            active = cursor.fetchone()
            if active:
                connection.commit()
                return {
                    "status": "busy",
                    "job": _row_to_state(dict(active)),
                }

            cursor.execute(
                """
                INSERT INTO predict2_venue_jobs (
                    job_id,
                    status,
                    reason,
                    fixture_id,
                    window_days,
                    limit_count,
                    current_stage,
                    stage_started_at,
                    last_progress_at,
                    message,
                    results_json,
                    created_at
                )
                VALUES (
                    %s,
                    'queued',
                    %s,
                    %s,
                    %s,
                    %s,
                    'queued',
                    %s,
                    %s,
                    'Venue enrichment has been queued.',
                    '[]'::jsonb,
                    %s
                )
                """,
                (
                    job_id,
                    reason,
                    fixture_id,
                    window_days,
                    limit,
                    now,
                    now,
                    now,
                ),
            )
        connection.commit()

    return {
        "status": "accepted",
        "job_id": job_id,
        "job": get_venue_job_state(job_id),
    }


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
                (VENUE_RUN_LOCK_ID,),
            )
            advisory_locked = bool(cursor.fetchone()[0])

        if not advisory_locked:
            _update_job(
                job_id,
                status="error",
                error_type="ConcurrentWorker",
                message=(
                    "Another Render worker already owns the venue "
                    "enrichment lock."
                ),
                current_stage=None,
                completed_at=_now(),
                last_progress_at=_now(),
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

        now = _now()
        _update_job(
            job_id,
            status="running",
            started_at=now,
            fixtures_total=len(fixtures),
            fixtures_completed=0,
            verified=0,
            unresolved=0,
            skipped=0,
            errors=0,
            current_stage="job_start",
            stage_started_at=now,
            last_progress_at=now,
            message="Venue enrichment is running.",
        )

        counters = {
            "verified": 0,
            "unresolved": 0,
            "skipped": 0,
            "errors": 0,
        }
        results: list[dict[str, Any]] = []

        if not fixtures:
            completed = _now()
            _update_job(
                job_id,
                status="ok",
                completed_at=completed,
                current_fixture=None,
                current_stage=None,
                stage_started_at=None,
                last_progress_at=completed,
                message=(
                    "No eligible unverified future fixtures were "
                    "found in the selected catalogue window."
                ),
                results_json=[],
            )
            _persist_summary(get_venue_job_state(job_id))
            return

        for index, fixture in enumerate(fixtures, start=1):
            if not fixture:
                counters["errors"] += 1
                results.append({
                    "status": "error",
                    "error_type": "FixtureNotFound",
                    "message": "The selected fixture no longer exists.",
                })
                continue

            _update_job(
                job_id,
                current_fixture={
                    "index": index,
                    "fixture_id": fixture["id"],
                    "home_team": fixture["home_team"],
                    "away_team": fixture["away_team"],
                },
                current_stage="fixture_start",
                stage_started_at=_now(),
                last_progress_at=_now(),
                message=(
                    f"Starting venue {index} of {len(fixtures)}."
                ),
            )

            progress: Callable[[str, str], None] = (
                lambda stage, message, current_job=job_id:
                    _progress(current_job, stage, message)
            )

            try:
                result = resolve_fixture(
                    job_id=job_id,
                    fixture=fixture,
                    progress=progress,
                )
            except Exception as exc:
                result = {
                    "status": "error",
                    "fixture_id": fixture["id"],
                    "error_type": type(exc).__name__,
                    "message": str(exc) or repr(exc),
                    "traceback_tail": (
                        traceback.format_exc().splitlines()[-12:]
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
            _update_job(
                job_id,
                fixtures_completed=index,
                verified=counters["verified"],
                unresolved=counters["unresolved"],
                skipped=counters["skipped"],
                errors=counters["errors"],
                results_json=results,
                last_progress_at=_now(),
            )

        completed = _now()
        _update_job(
            job_id,
            status="ok",
            completed_at=completed,
            current_fixture=None,
            current_stage=None,
            stage_started_at=None,
            last_progress_at=completed,
            message=(
                "Venue enrichment completed. "
                f"{counters['verified']} verified, "
                f"{counters['unresolved']} unresolved, "
                f"{counters['skipped']} skipped, "
                f"{counters['errors']} errors."
            ),
            results_json=results,
        )
        _persist_summary(get_venue_job_state(job_id))

    except Exception as exc:
        completed = _now()
        try:
            _update_job(
                job_id,
                status="error",
                completed_at=completed,
                current_fixture=None,
                current_stage=None,
                stage_started_at=None,
                last_progress_at=completed,
                error_type=type(exc).__name__,
                message=str(exc) or repr(exc),
                traceback_json=(
                    traceback.format_exc().splitlines()[-15:]
                ),
            )
            _persist_summary(get_venue_job_state(job_id))
        except Exception:
            pass

    finally:
        if advisory_locked and advisory_connection is not None:
            try:
                with advisory_connection.cursor() as cursor:
                    cursor.execute(
                        "SELECT pg_advisory_unlock(%s)",
                        (VENUE_RUN_LOCK_ID,),
                    )
                advisory_connection.commit()
            except Exception:
                pass
        if advisory_context is not None:
            try:
                advisory_context.__exit__(None, None, None)
            except Exception:
                pass


def start_venue_job(
    *,
    reason: str,
    window_days: int | None = None,
    limit: int | None = None,
    fixture_id: int | None = None,
) -> dict[str, Any]:
    resolved_window = max(
        1,
        min(
            int(
                window_days
                or settings.venue_enrichment_window_days
            ),
            90,
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

    created = _create_job(
        reason=reason,
        window_days=resolved_window,
        limit=resolved_limit,
        fixture_id=fixture_id,
    )
    if created.get("status") != "accepted":
        return created

    job_id = str(created["job_id"])
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
    try:
        thread.start()
    except Exception as exc:
        _update_job(
            job_id,
            status="error",
            completed_at=_now(),
            current_stage=None,
            error_type=type(exc).__name__,
            message=(
                "The database job was created but the worker "
                f"could not start: {exc}"
            ),
            last_progress_at=_now(),
        )
        return {
            "status": "error",
            "job_id": job_id,
            "job": get_venue_job_state(job_id),
        }

    return {
        "status": "accepted",
        "job_id": job_id,
        "job": get_venue_job_state(job_id),
    }
