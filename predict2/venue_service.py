from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from .config import settings
from .db import connect
from .geocoding import geocode_venue
from .tavily import TavilyError, client as tavily_client


def get_fixture(fixture_id: int) -> dict[str, Any] | None:
    with connect(dict_rows=True) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    id,
                    provider,
                    provider_fixture_id,
                    competition_name,
                    competition_country,
                    home_team,
                    away_team,
                    kickoff_utc,
                    venue_name,
                    venue_city,
                    latitude,
                    longitude,
                    timezone_name,
                    location_source,
                    location_confidence,
                    location_verified_at
                FROM fixtures
                WHERE id = %s
                  AND sport = 'soccer'
                  AND provider = 'football-data.org'
                """,
                (fixture_id,),
            )
            row = cursor.fetchone()
    return dict(row) if row else None


def fixtures_needing_venue(
    *,
    window_days: int,
    limit: int,
) -> list[dict[str, Any]]:
    with connect(dict_rows=True) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    f.id,
                    f.provider,
                    f.provider_fixture_id,
                    f.competition_name,
                    f.competition_country,
                    f.home_team,
                    f.away_team,
                    f.kickoff_utc,
                    f.venue_name,
                    f.venue_city,
                    f.latitude,
                    f.longitude,
                    f.timezone_name,
                    latest.status AS latest_attempt_status,
                    latest.completed_at AS latest_attempt_at
                FROM fixtures f
                LEFT JOIN LATERAL (
                    SELECT status, completed_at
                    FROM predict2_venue_attempts
                    WHERE fixture_id = f.id
                    ORDER BY id DESC
                    LIMIT 1
                ) latest ON TRUE
                WHERE f.sport = 'soccer'
                  AND f.provider = 'football-data.org'
                  AND f.kickoff_utc > NOW()
                  AND f.kickoff_utc <= (
                      NOW() + (%s * INTERVAL '1 day')
                  )
                  AND (
                      f.latitude IS NULL
                      OR f.longitude IS NULL
                      OR f.timezone_name IS NULL
                      OR f.location_verified_at IS NULL
                  )
                  AND (
                      latest.completed_at IS NULL
                      OR latest.completed_at < (
                          NOW() - (%s * INTERVAL '1 hour')
                      )
                  )
                ORDER BY
                    CASE WHEN f.venue_name IS NOT NULL THEN 0 ELSE 1 END,
                    f.kickoff_utc ASC
                LIMIT %s
                """,
                (window_days, settings.venue_enrichment_retry_hours, limit),
            )
            rows = cursor.fetchall()
    return [dict(row) for row in rows]


def _insert_attempt(
    *,
    job_id: str,
    fixture_id: int,
) -> int:
    with connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO predict2_venue_attempts (
                    job_id,
                    fixture_id,
                    status,
                    stage,
                    audit_json,
                    started_at
                )
                VALUES (%s, %s, 'running', 'start', '{}'::jsonb, NOW())
                RETURNING id
                """,
                (job_id, fixture_id),
            )
            attempt_id = int(cursor.fetchone()[0])
        connection.commit()
    return attempt_id


def _finish_attempt(
    *,
    attempt_id: int,
    status: str,
    stage: str,
    audit: dict[str, Any],
    venue_name: str | None = None,
    venue_city: str | None = None,
    country: str | None = None,
    latitude: float | None = None,
    longitude: float | None = None,
    timezone_name: str | None = None,
    confidence: float | None = None,
    source: str | None = None,
) -> None:
    with connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE predict2_venue_attempts
                SET
                    status = %s,
                    stage = %s,
                    venue_name = %s,
                    venue_city = %s,
                    country = %s,
                    latitude = %s,
                    longitude = %s,
                    timezone_name = %s,
                    confidence = %s,
                    source = %s,
                    audit_json = %s::jsonb,
                    completed_at = NOW()
                WHERE id = %s
                """,
                (
                    status,
                    stage,
                    venue_name,
                    venue_city,
                    country,
                    latitude,
                    longitude,
                    timezone_name,
                    confidence,
                    source,
                    json.dumps(
                        audit,
                        ensure_ascii=False,
                        default=str,
                    ),
                    attempt_id,
                ),
            )
        connection.commit()


def _commit_location(
    *,
    fixture_id: int,
    venue_name: str,
    selected: dict[str, Any],
    source: str,
) -> bool:
    with connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE fixtures
                SET
                    venue_name = %s,
                    venue_city = %s,
                    competition_country = COALESCE(
                        %s,
                        competition_country
                    ),
                    latitude = %s,
                    longitude = %s,
                    timezone_name = %s,
                    location_source = %s,
                    location_confidence = %s,
                    location_verified_at = NOW(),
                    updated_at = NOW()
                WHERE id = %s
                  AND kickoff_utc > NOW()
                """,
                (
                    venue_name,
                    selected.get("venue_city"),
                    selected.get("country"),
                    selected.get("latitude"),
                    selected.get("longitude"),
                    selected.get("timezone_name"),
                    source,
                    selected.get("confidence"),
                    fixture_id,
                ),
            )
            updated = int(cursor.rowcount or 0)
        connection.commit()
    return updated == 1


def resolve_fixture(
    *,
    job_id: str,
    fixture: dict[str, Any],
) -> dict[str, Any]:
    attempt_id = _insert_attempt(
        job_id=job_id,
        fixture_id=int(fixture["id"]),
    )
    audit: dict[str, Any] = {
        "fixture": {
            key: (
                value.isoformat()
                if isinstance(value, datetime)
                else value
            )
            for key, value in fixture.items()
        }
    }

    kickoff = fixture["kickoff_utc"]
    if kickoff <= datetime.now(timezone.utc):
        result = {
            "status": "skipped",
            "stage": "post_kickoff",
            "fixture_id": fixture["id"],
        }
        _finish_attempt(
            attempt_id=attempt_id,
            status="skipped",
            stage="post_kickoff",
            audit={**audit, "result": result},
        )
        return result

    venue_name = str(
        fixture.get("venue_name") or ""
    ).strip()
    venue_city = str(
        fixture.get("venue_city") or ""
    ).strip()
    country = str(
        fixture.get("competition_country") or ""
    ).strip()
    identity_source = "football-data.org exact fixture venue"
    tavily_result = None

    if venue_name:
        geocode = geocode_venue(
            venue_name=venue_name,
            city=venue_city,
            country=country,
        )
        audit["provider_venue_geocode"] = geocode
        if geocode.get("verified") is True:
            selected = geocode["selected"]
            source = (
                f"{identity_source}+"
                f"{selected.get('provider')}+timezonefinder"
            )
            committed = _commit_location(
                fixture_id=int(fixture["id"]),
                venue_name=venue_name,
                selected=selected,
                source=source,
            )
            status = "verified" if committed else "not_committed"
            result = {
                "status": status,
                "stage": "provider_venue_geocode",
                "fixture_id": fixture["id"],
                "venue_name": venue_name,
                "selected": selected,
                "source": source,
            }
            _finish_attempt(
                attempt_id=attempt_id,
                status=status,
                stage="provider_venue_geocode",
                audit={**audit, "result": result},
                venue_name=venue_name,
                venue_city=selected.get("venue_city"),
                country=selected.get("country"),
                latitude=selected.get("latitude"),
                longitude=selected.get("longitude"),
                timezone_name=selected.get("timezone_name"),
                confidence=selected.get("confidence"),
                source=source,
            )
            return result

    try:
        tavily_result = tavily_client.resolve_fixture_venue(
            fixture,
            provider_venue=venue_name or None,
        )
    except TavilyError as exc:
        tavily_result = {
            "status": "provider_error",
            "verified": False,
            "error_type": type(exc).__name__,
            "message": str(exc),
            "http_status": exc.status_code,
            "provider_message": exc.provider_message,
        }
    audit["tavily"] = tavily_result

    if not tavily_result.get("verified"):
        result = {
            "status": "unresolved",
            "stage": "venue_identity",
            "fixture_id": fixture["id"],
            "reason": tavily_result.get("status"),
        }
        _finish_attempt(
            attempt_id=attempt_id,
            status="unresolved",
            stage="venue_identity",
            audit={**audit, "result": result},
            venue_name=venue_name or None,
            source="tavily",
        )
        return result

    selected_identity = tavily_result["selected"]
    resolved_venue = str(
        selected_identity.get("venue_name") or venue_name
    ).strip()
    geocode = geocode_venue(
        venue_name=resolved_venue,
        city=venue_city,
        country=country,
    )
    audit["verified_venue_geocode"] = geocode

    if not geocode.get("verified"):
        result = {
            "status": "unresolved",
            "stage": "geocoding",
            "fixture_id": fixture["id"],
            "venue_name": resolved_venue,
            "reason": geocode.get("status"),
        }
        _finish_attempt(
            attempt_id=attempt_id,
            status="unresolved",
            stage="geocoding",
            audit={**audit, "result": result},
            venue_name=resolved_venue,
            source="tavily+geocoder",
        )
        return result

    selected = geocode["selected"]
    identity_source = (
        "football-data.org+tavily confirmation"
        if venue_name
        else "tavily fixture-page consensus"
    )
    source = (
        f"{identity_source}+"
        f"{selected.get('provider')}+timezonefinder"
    )
    committed = _commit_location(
        fixture_id=int(fixture["id"]),
        venue_name=resolved_venue,
        selected=selected,
        source=source,
    )
    status = "verified" if committed else "not_committed"
    result = {
        "status": status,
        "stage": "web_verified_geocode",
        "fixture_id": fixture["id"],
        "venue_name": resolved_venue,
        "selected": selected,
        "source": source,
    }
    _finish_attempt(
        attempt_id=attempt_id,
        status=status,
        stage="web_verified_geocode",
        audit={**audit, "result": result},
        venue_name=resolved_venue,
        venue_city=selected.get("venue_city"),
        country=selected.get("country"),
        latitude=selected.get("latitude"),
        longitude=selected.get("longitude"),
        timezone_name=selected.get("timezone_name"),
        confidence=selected.get("confidence"),
        source=source,
    )
    return result
