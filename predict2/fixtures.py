from __future__ import annotations

import json
from datetime import date, datetime, time, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .db import connect


ACTIVE_STATUSES = {
    "SCHEDULED",
    "TIMED",
    "IN_PLAY",
    "PAUSED",
    "LIVE",
}


def parse_utc(value: Any) -> datetime:
    text = str(value or "").strip()
    if not text:
        raise ValueError("Missing utcDate.")
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _team_name(value: Any) -> str:
    if not isinstance(value, dict):
        return ""
    return str(
        value.get("name")
        or value.get("shortName")
        or value.get("tla")
        or ""
    ).strip()


def normalize_match(row: dict[str, Any]) -> dict[str, Any]:
    match_id = row.get("id")
    if match_id is None:
        raise ValueError("Football-data.org match has no id.")

    home = _team_name(row.get("homeTeam"))
    away = _team_name(row.get("awayTeam"))
    if not home or not away:
        raise ValueError("Football-data.org match has incomplete teams.")

    competition = (
        row.get("competition")
        if isinstance(row.get("competition"), dict)
        else {}
    )
    area = (
        row.get("area")
        if isinstance(row.get("area"), dict)
        else {}
    )
    season = (
        row.get("season")
        if isinstance(row.get("season"), dict)
        else {}
    )
    status = str(row.get("status") or "SCHEDULED").upper()

    return {
        "provider": "football-data.org",
        "provider_fixture_id": str(match_id),
        "competition_name": str(
            competition.get("name")
            or competition.get("code")
            or "Unknown competition"
        ),
        "competition_country": str(
            area.get("name") or ""
        ).strip() or None,
        "season": str(
            season.get("id")
            or season.get("startDate")
            or ""
        ).strip() or None,
        "home_team": home,
        "away_team": away,
        "kickoff_utc": parse_utc(row.get("utcDate")),
        "venue_name": str(
            row.get("venue") or ""
        ).strip() or None,
        "fixture_status": status.lower(),
        "neutral_venue": False,
        "raw_fixture_json": row,
    }


def upsert_matches(rows: list[dict[str, Any]]) -> dict[str, int]:
    imported = 0
    skipped = 0

    normalized: list[dict[str, Any]] = []
    for row in rows:
        try:
            normalized.append(normalize_match(row))
        except (TypeError, ValueError):
            skipped += 1

    if not normalized:
        return {
            "received": len(rows),
            "imported": 0,
            "skipped": skipped,
        }

    sql = """
        INSERT INTO fixtures (
            provider,
            provider_fixture_id,
            sport,
            competition_name,
            competition_country,
            season,
            home_team,
            away_team,
            kickoff_utc,
            venue_name,
            fixture_status,
            neutral_venue,
            raw_fixture_json,
            updated_at
        )
        VALUES (
            %(provider)s,
            %(provider_fixture_id)s,
            'soccer',
            %(competition_name)s,
            %(competition_country)s,
            %(season)s,
            %(home_team)s,
            %(away_team)s,
            %(kickoff_utc)s,
            %(venue_name)s,
            %(fixture_status)s,
            %(neutral_venue)s,
            %(raw_fixture_json)s::jsonb,
            NOW()
        )
        ON CONFLICT (provider, provider_fixture_id)
        DO UPDATE SET
            competition_name = EXCLUDED.competition_name,
            competition_country = EXCLUDED.competition_country,
            season = EXCLUDED.season,
            home_team = EXCLUDED.home_team,
            away_team = EXCLUDED.away_team,
            kickoff_utc = EXCLUDED.kickoff_utc,
            venue_name = COALESCE(
                EXCLUDED.venue_name,
                fixtures.venue_name
            ),
            fixture_status = EXCLUDED.fixture_status,
            raw_fixture_json = EXCLUDED.raw_fixture_json,
            updated_at = NOW()
    """

    with connect() as connection:
        with connection.cursor() as cursor:
            for item in normalized:
                params = {
                    **item,
                    "raw_fixture_json": json.dumps(
                        item["raw_fixture_json"],
                        ensure_ascii=False,
                    ),
                }
                cursor.execute(sql, params)
                imported += 1
        connection.commit()

    return {
        "received": len(rows),
        "imported": imported,
        "skipped": skipped,
    }


def _window(window: str) -> tuple[datetime, datetime]:
    now = datetime.now(timezone.utc)
    today = now.date()

    if window == "today":
        start = datetime.combine(
            today, time.min, tzinfo=timezone.utc
        )
        end = start + timedelta(days=1)
    elif window == "tomorrow":
        start = datetime.combine(
            today + timedelta(days=1),
            time.min,
            tzinfo=timezone.utc,
        )
        end = start + timedelta(days=1)
    elif window == "3days":
        start = now
        end = now + timedelta(days=3)
    elif window == "3months":
        start = now
        end = now + timedelta(days=90)
    else:
        raise ValueError("Unsupported fixture window.")

    return start, end


def _local_kickoff(
    kickoff_utc: datetime,
    timezone_name: str | None,
) -> dict[str, Any]:
    if not timezone_name:
        return {
            "ready": False,
            "display": "Venue-local time pending",
            "timezone": None,
        }

    try:
        zone = ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError:
        return {
            "ready": False,
            "display": "Venue timezone invalid",
            "timezone": timezone_name,
        }

    local = kickoff_utc.astimezone(zone)
    return {
        "ready": True,
        "display": local.strftime("%H:%M %d/%m/%Y %z"),
        "timezone": timezone_name,
        "iso": local.isoformat(),
    }


def list_fixtures(
    *,
    window: str,
    search: str = "",
) -> list[dict[str, Any]]:
    start, end = _window(window)
    search_text = search.strip()

    params: list[Any] = [start, end]
    search_sql = ""
    if search_text:
        search_sql = """
        AND (
            home_team ILIKE %s
            OR away_team ILIKE %s
            OR competition_name ILIKE %s
            OR COALESCE(competition_country, '') ILIKE %s
            OR COALESCE(venue_name, '') ILIKE %s
        )
        """
        pattern = f"%{search_text}%"
        params.extend([pattern] * 5)

    query = f"""
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
            timezone_name,
            latitude,
            longitude,
            location_source,
            location_verified_at,
            fixture_status
        FROM fixtures
        WHERE sport = 'soccer'
          AND kickoff_utc >= %s
          AND kickoff_utc < %s
          {search_sql}
        ORDER BY kickoff_utc ASC
        LIMIT 1000
    """

    with connect(dict_rows=True) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            rows = cursor.fetchall()

    output: list[dict[str, Any]] = []
    for row in rows:
        kickoff = row["kickoff_utc"]
        output.append({
            **row,
            "kickoff_utc": kickoff.isoformat(),
            "kickoff_local": _local_kickoff(
                kickoff,
                row.get("timezone_name"),
            ),
            "prediction_ready": False,
            "prediction_status": (
                "PREDICT2 prediction engine not migrated yet"
            ),
        })
    return output
