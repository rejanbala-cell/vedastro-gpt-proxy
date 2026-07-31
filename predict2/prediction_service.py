from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

from .chart_adapter import astrology_signal, calculate_chart
from .config import settings
from .db import connect
from .prediction_evidence import market_consensus, performance_snapshot
from .venue_service import get_fixture, resolve_fixture


PREDICTION_LOCK_BASE = 230000000


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str)


def _outcome_label(outcome: str, fixture: dict[str, Any]) -> str:
    if outcome == "home":
        return f"Home win — {fixture['home_team']}"
    if outcome == "away":
        return f"Away win — {fixture['away_team']}"
    return "Draw"


def _existing(fixture_id: int) -> dict[str, Any] | None:
    with connect(dict_rows=True) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT *
                FROM predict2_prediction_runs
                WHERE fixture_id = %s
                  AND model_version = %s
                  AND status = 'completed'
                ORDER BY id DESC
                LIMIT 1
                """,
                (fixture_id, settings.prediction_model_version),
            )
            row = cursor.fetchone()
    return dict(row) if row else None


def _insert_running(
    *,
    event_id: str,
    fixture_id: int,
) -> int:
    with connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO predict2_prediction_runs (
                    event_id,
                    fixture_id,
                    model_version,
                    status
                )
                VALUES (%s, %s, %s, 'running')
                ON CONFLICT (fixture_id, model_version)
                DO UPDATE SET
                    event_id = EXCLUDED.event_id,
                    status = CASE
                        WHEN predict2_prediction_runs.status = 'completed'
                        THEN 'completed'
                        ELSE 'running'
                    END,
                    error_type = NULL,
                    error_message = NULL
                RETURNING id
                """,
                (
                    event_id,
                    fixture_id,
                    settings.prediction_model_version,
                ),
            )
            prediction_id = int(cursor.fetchone()[0])
        connection.commit()
    return prediction_id


def _complete(
    prediction_id: int,
    *,
    outcome: str,
    fixture: dict[str, Any],
    confidence: str,
    eligibility: str,
    method: str,
    favourite_side: str | None,
    favourite_team: str | None,
    underdog_team: str | None,
    market: dict[str, Any],
    performance: dict[str, Any],
    venue: dict[str, Any],
    chart: dict[str, Any] | None,
    decision: dict[str, Any],
    diagnostic: dict[str, Any],
) -> dict[str, Any]:
    label = _outcome_label(outcome, fixture)
    with connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE predict2_prediction_runs
                SET
                    status = 'completed',
                    completed_at = NOW(),
                    outcome = %s,
                    outcome_label = %s,
                    confidence = %s,
                    eligibility = %s,
                    method = %s,
                    favourite_side = %s,
                    favourite_team = %s,
                    underdog_team = %s,
                    market_json = %s::jsonb,
                    performance_json = %s::jsonb,
                    venue_json = %s::jsonb,
                    chart_json = %s::jsonb,
                    decision_json = %s::jsonb,
                    diagnostic_json = %s::jsonb
                WHERE id = %s
                """,
                (
                    outcome,
                    label,
                    confidence,
                    eligibility,
                    method,
                    favourite_side,
                    favourite_team,
                    underdog_team,
                    _json(market),
                    _json(performance),
                    _json(venue),
                    _json(chart) if chart is not None else None,
                    _json(decision),
                    _json(diagnostic),
                    prediction_id,
                ),
            )
        connection.commit()
    return get_prediction(fixture["id"]) or {}


def _fail(prediction_id: int, exc: Exception) -> None:
    with connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE predict2_prediction_runs
                SET
                    status = 'error',
                    completed_at = NOW(),
                    error_type = %s,
                    error_message = %s
                WHERE id = %s
                """,
                (
                    type(exc).__name__,
                    str(exc)[:2000],
                    prediction_id,
                ),
            )
        connection.commit()


def get_prediction(fixture_id: int) -> dict[str, Any] | None:
    row = _existing(fixture_id)
    if not row:
        return None
    for key in (
        "market_json",
        "performance_json",
        "venue_json",
        "chart_json",
        "decision_json",
        "diagnostic_json",
    ):
        if isinstance(row.get(key), str):
            try:
                row[key] = json.loads(row[key])
            except ValueError:
                pass
    for key in ("requested_at", "completed_at"):
        if isinstance(row.get(key), datetime):
            row[key] = row[key].isoformat()
    row["immutable"] = True
    row["duplicate_click_returns_existing"] = True
    return row


def _local_chart_input(fixture: dict[str, Any]) -> tuple[str, str]:
    zone = ZoneInfo(fixture["timezone_name"])
    local = fixture["kickoff_utc"].astimezone(zone)
    return (
        local.strftime("%H:%M %d/%m/%Y %z"),
        local.isoformat(),
    )


def _choose(
    *,
    fixture: dict[str, Any],
    market: dict[str, Any],
    performance: dict[str, Any],
    astro: dict[str, Any] | None,
) -> dict[str, Any]:
    performance_outcome = performance["baseline_outcome"]
    market_side = market.get("favourite_side")
    favourite_outcome = market_side if market_side in {"home", "away"} else None

    if not favourite_outcome:
        return {
            "outcome": performance_outcome,
            "method": "performance_only",
            "reason": "No verified multi-source market favourite.",
            "confidence": "LOW",
            "eligibility": "NO",
        }

    if not astro or astro.get("status") != "active":
        return {
            "outcome": performance_outcome,
            "method": "performance_only",
            "reason": "Astrology unavailable, invalid, or reliability-vetoed.",
            "confidence": "LOW",
            "eligibility": "NO",
        }

    direction = astro.get("direction")
    strength = int(astro.get("strength") or 0)

    # Favourite remains baseline when astrology is balanced.
    outcome = favourite_outcome
    reason = "Consensus favourite baseline; astrology balanced."

    if (
        direction == "opponent"
        and strength >= 2
        and performance_outcome in {"home", "away"}
        and performance_outcome != favourite_outcome
    ):
        outcome = performance_outcome
        reason = (
            "Opponent selected only because independent astrology and "
            "performance both oppose the market favourite."
        )
    elif (
        performance_outcome == "draw"
        and performance.get("high_draw_evidence")
        and direction == "balanced"
    ):
        outcome = "draw"
        reason = (
            "Draw selected from independent performance draw evidence "
            "with neutral astrology."
        )
    elif (
        direction == "favourite"
        and performance_outcome == favourite_outcome
    ):
        outcome = favourite_outcome
        reason = (
            "Market favourite confirmed by performance and astrology."
        )

    confidence = "MEDIUM" if strength >= 1 else "LOW"
    eligibility = "CONDITIONAL" if confidence == "MEDIUM" else "NO"
    return {
        "outcome": outcome,
        "method": "market_performance_astrology",
        "reason": reason,
        "confidence": confidence,
        "eligibility": eligibility,
    }


def run_prediction(fixture_id: int) -> dict[str, Any]:
    existing = _existing(fixture_id)
    if existing:
        return get_prediction(fixture_id) or existing

    with connect() as lock_connection:
        with lock_connection.cursor() as cursor:
            cursor.execute(
                "SELECT pg_advisory_lock(%s)",
                (PREDICTION_LOCK_BASE + int(fixture_id),),
            )
        try:
            existing = _existing(fixture_id)
            if existing:
                return get_prediction(fixture_id) or existing

            fixture = get_fixture(fixture_id)
            if not fixture:
                raise ValueError("Fixture not found.")
            if fixture["kickoff_utc"] <= datetime.now(timezone.utc):
                raise ValueError("Fixture has already kicked off.")

            event_id = (
                f"p2-{fixture['provider_fixture_id']}-"
                f"{uuid.uuid4().hex[:12]}"
            )
            prediction_id = _insert_running(
                event_id=event_id,
                fixture_id=fixture_id,
            )

            try:
                venue_audit: dict[str, Any] = {
                    "initial_ready": bool(
                        fixture.get("latitude") is not None
                        and fixture.get("longitude") is not None
                        and fixture.get("timezone_name")
                        and fixture.get("location_verified_at")
                    )
                }

                if not venue_audit["initial_ready"]:
                    venue_result = resolve_fixture(
                        job_id=f"predict-{event_id}",
                        fixture=fixture,
                    )
                    venue_audit["automatic_resolution"] = venue_result
                    fixture = get_fixture(fixture_id) or fixture

                venue_ready = bool(
                    fixture.get("latitude") is not None
                    and fixture.get("longitude") is not None
                    and fixture.get("timezone_name")
                    and fixture.get("location_verified_at")
                )
                venue_audit["final_ready"] = venue_ready
                venue_audit["venue_name"] = fixture.get("venue_name")
                venue_audit["venue_city"] = fixture.get("venue_city")
                venue_audit["latitude"] = fixture.get("latitude")
                venue_audit["longitude"] = fixture.get("longitude")
                venue_audit["timezone_name"] = fixture.get("timezone_name")
                venue_audit["source"] = fixture.get("location_source")

                market = market_consensus(fixture)
                performance = performance_snapshot(fixture)

                favourite_side = market.get("favourite_side")
                favourite_team = (
                    fixture["home_team"]
                    if favourite_side == "home"
                    else fixture["away_team"]
                    if favourite_side == "away"
                    else None
                )
                underdog_team = (
                    fixture["away_team"]
                    if favourite_side == "home"
                    else fixture["home_team"]
                    if favourite_side == "away"
                    else None
                )

                chart_package = None
                astro = None
                chart_call_made = False

                if (
                    venue_ready
                    and favourite_team
                    and underdog_team
                    and not market.get("near_pickem")
                ):
                    std_time, local_iso = _local_chart_input(fixture)
                    chart_call_made = True
                    chart_package = calculate_chart(
                        event_id=event_id,
                        std_time=std_time,
                        venue_name=fixture["venue_name"],
                        latitude=float(fixture["latitude"]),
                        longitude=float(fixture["longitude"]),
                        favourite_team=favourite_team,
                        underdog_team=underdog_team,
                    )
                    if (
                        chart_package.get("status") == "ok"
                        and chart_package.get("validation", {}).get("valid")
                    ):
                        astro = astrology_signal(
                            chart_package["chart"]
                        )
                    else:
                        astro = {
                            "status": "invalid",
                            "direction": "balanced",
                            "strength": 0,
                        }

                decision = _choose(
                    fixture=fixture,
                    market=market,
                    performance=performance,
                    astro=astro,
                )
                decision.update({
                    "astrology_signal": astro,
                    "chart_call_made": chart_call_made,
                    "chart_call_count": 1 if chart_call_made else 0,
                    "exact_outcome_market": (
                        "90 minutes plus stoppage time"
                    ),
                })

                diagnostic = {
                    "event_id": event_id,
                    "fixture_verified": True,
                    "kickoff_future": True,
                    "market_verified": (
                        market.get("status") == "verified"
                    ),
                    "performance_status": performance.get("status"),
                    "venue_ready": venue_ready,
                    "chart_call_made": chart_call_made,
                    "chart_validation": (
                        chart_package.get("validation")
                        if chart_package else None
                    ),
                    "responsible_notice": (
                        "Astrology is not scientifically validated and "
                        "sports outcomes remain uncertain."
                    ),
                }

                return _complete(
                    prediction_id,
                    outcome=decision["outcome"],
                    fixture=fixture,
                    confidence=decision["confidence"],
                    eligibility=decision["eligibility"],
                    method=decision["method"],
                    favourite_side=favourite_side,
                    favourite_team=favourite_team,
                    underdog_team=underdog_team,
                    market=market,
                    performance=performance,
                    venue=venue_audit,
                    chart=chart_package,
                    decision=decision,
                    diagnostic=diagnostic,
                )
            except Exception as exc:
                _fail(prediction_id, exc)
                raise
        finally:
            with lock_connection.cursor() as cursor:
                cursor.execute(
                    "SELECT pg_advisory_unlock(%s)",
                    (PREDICTION_LOCK_BASE + int(fixture_id),),
                )
            lock_connection.commit()
