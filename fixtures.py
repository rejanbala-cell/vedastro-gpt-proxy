from __future__ import annotations

import hashlib
import json
import math
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

import requests

try:
    from timezonefinder import TimezoneFinder
except ImportError:  # pragma: no cover
    TimezoneFinder = None

from .config import settings
from .db import connect
from .text_utils import normalize_text, text_similarity


class GeocodingError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        provider_message: str | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.provider_message = provider_message


SPORT_TYPES = {
    "stadium", "sports_centre", "sports_center",
    "sports centre", "sports center", "arena",
    "pitch", "recreation_ground", "sports_complex",
    "sports complex",
}

COUNTRY_ALIASES = {
    "england": {"england", "united kingdom", "uk"},
    "scotland": {"scotland", "united kingdom", "uk"},
    "wales": {"wales", "united kingdom", "uk"},
    "northern ireland": {
        "northern ireland", "united kingdom", "uk"
    },
    "usa": {"usa", "united states", "united states of america"},
    "korea republic": {"south korea", "republic of korea"},
}

_NOMINATIM_LOCK = threading.Lock()
_NOMINATIM_LAST_CALL = 0.0
_TIMEZONE_FINDER = TimezoneFinder() if TimezoneFinder else None


def timezone_driver_ready() -> bool:
    return _TIMEZONE_FINDER is not None


def _timezone_name(latitude: float, longitude: float) -> str | None:
    if _TIMEZONE_FINDER is None:
        return None
    return _TIMEZONE_FINDER.timezone_at(
        lat=latitude,
        lng=longitude,
    )


def _cache_key(query: str) -> str:
    return hashlib.sha256(
        normalize_text(query).encode("utf-8")
    ).hexdigest()


def _cache_get(query: str) -> dict[str, Any] | None:
    with connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT result_json
                FROM predict2_geocode_cache
                WHERE cache_key = %s
                  AND expires_at > NOW()
                """,
                (_cache_key(query),),
            )
            row = cursor.fetchone()
    if not row:
        return None
    value = row[0]
    return value if isinstance(value, dict) else json.loads(value)


def _cache_set(
    *,
    query: str,
    provider: str,
    result: dict[str, Any],
    days: int = 30,
) -> None:
    expires = datetime.now(timezone.utc) + timedelta(days=days)
    with connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO predict2_geocode_cache (
                    cache_key,
                    provider,
                    query_text,
                    result_json,
                    expires_at,
                    updated_at
                )
                VALUES (%s, %s, %s, %s::jsonb, %s, NOW())
                ON CONFLICT (cache_key)
                DO UPDATE SET
                    provider = EXCLUDED.provider,
                    query_text = EXCLUDED.query_text,
                    result_json = EXCLUDED.result_json,
                    expires_at = EXCLUDED.expires_at,
                    updated_at = NOW()
                """,
                (
                    _cache_key(query),
                    provider,
                    query,
                    json.dumps(result, ensure_ascii=False),
                    expires,
                ),
            )
        connection.commit()


def _city(address: dict[str, Any]) -> str:
    for key in (
        "city", "town", "village", "municipality",
        "county", "state_district",
    ):
        value = str(address.get(key) or "").strip()
        if value:
            return value
    return ""


def _country_values(expected: str) -> set[str]:
    normalized = normalize_text(expected)
    if not normalized:
        return set()
    return {
        normalize_text(value)
        for value in COUNTRY_ALIASES.get(
            normalized, {expected}
        )
    }


def _country_matches(expected: str, actual: str) -> bool:
    values = _country_values(expected)
    if not values:
        return True
    normalized_actual = normalize_text(actual)
    return any(
        value in normalized_actual or normalized_actual in value
        for value in values
    )


def _sports_place(candidate: dict[str, Any]) -> bool:
    place_type = normalize_text(candidate.get("type"))
    category = normalize_text(
        candidate.get("class") or candidate.get("category")
    )
    display = normalize_text(candidate.get("display_name"))
    return bool(
        place_type in {normalize_text(item) for item in SPORT_TYPES}
        or category in {"leisure", "sport"}
        or any(
            word in display
            for word in (
                "stadium", "arena", "sports centre",
                "sports center", "sports complex",
                "estadio", "stade", "stadion", "campo",
                "complexo desportivo",
            )
        )
    )


def _candidate_names(candidate: dict[str, Any]) -> list[str]:
    values = [
        candidate.get("name"),
        candidate.get("display_name"),
    ]
    namedetails = candidate.get("namedetails")
    if isinstance(namedetails, dict):
        values.extend(namedetails.values())
    return [
        str(value).strip()
        for value in values
        if str(value or "").strip()
    ]


def _evaluate_candidate(
    candidate: dict[str, Any],
    *,
    venue_name: str,
    expected_city: str,
    expected_country: str,
    provider: str,
) -> dict[str, Any] | None:
    try:
        latitude = float(candidate.get("lat"))
        longitude = float(candidate.get("lon"))
    except (TypeError, ValueError):
        return None
    if not (
        -90 <= latitude <= 90
        and -180 <= longitude <= 180
    ):
        return None

    address = (
        candidate.get("address")
        if isinstance(candidate.get("address"), dict)
        else {}
    )
    actual_city = _city(address)
    actual_country = str(address.get("country") or "").strip()
    names = _candidate_names(candidate)
    similarity = max(
        [text_similarity(venue_name, value) for value in names]
        or [0.0]
    )
    sports = _sports_place(candidate)
    country_ok = _country_matches(
        expected_country,
        actual_country or str(candidate.get("display_name") or ""),
    )
    city_ok = (
        True
        if not expected_city
        else text_similarity(expected_city, actual_city) >= 0.55
        or normalize_text(expected_city)
        in normalize_text(candidate.get("display_name"))
    )
    timezone_name = _timezone_name(latitude, longitude)

    score = (
        similarity * 50.0
        + (20.0 if sports else 0.0)
        + (15.0 if country_ok else 0.0)
        + (10.0 if city_ok else 0.0)
        + (5.0 if timezone_name else 0.0)
    )
    blockers = []
    if similarity < settings.venue_similarity_minimum:
        blockers.append("venue_name_mismatch")
    if not sports:
        blockers.append("not_a_sports_place")
    if not country_ok:
        blockers.append("country_mismatch")
    if not city_ok:
        blockers.append("city_mismatch")
    if not timezone_name:
        blockers.append("timezone_unavailable")

    return {
        "provider": provider,
        "display_name": candidate.get("display_name"),
        "latitude": latitude,
        "longitude": longitude,
        "timezone_name": timezone_name,
        "venue_city": actual_city or expected_city or None,
        "country": actual_country or expected_country or None,
        "venue_similarity": round(similarity, 4),
        "sports_place": sports,
        "country_match": country_ok,
        "city_match": city_ok,
        "confidence": round(score, 3),
        "blockers": blockers,
        "raw_type": candidate.get("type"),
        "raw_class": (
            candidate.get("class")
            or candidate.get("category")
        ),
    }


def _distance_km(left: dict[str, Any], right: dict[str, Any]) -> float:
    lat1 = math.radians(float(left["latitude"]))
    lon1 = math.radians(float(left["longitude"]))
    lat2 = math.radians(float(right["latitude"]))
    lon2 = math.radians(float(right["longitude"]))
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    value = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1)
        * math.cos(lat2)
        * math.sin(dlon / 2) ** 2
    )
    return 6371.0088 * 2 * math.asin(min(1.0, math.sqrt(value)))


def _select_candidate(
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    eligible = [
        row for row in rows
        if not row["blockers"]
        and row["confidence"]
        >= settings.geocode_confidence_minimum
    ]
    eligible.sort(
        key=lambda row: (
            row["confidence"],
            row["venue_similarity"],
        ),
        reverse=True,
    )
    if not eligible:
        return {
            "status": "no_eligible_candidate",
            "verified": False,
            "candidates": rows,
        }

    selected = eligible[0]
    if len(eligible) > 1:
        second = eligible[1]
        distance = _distance_km(selected, second)
        margin = (
            selected["confidence"] - second["confidence"]
        )
        if distance > 10.0 and margin < 5.0:
            return {
                "status": "candidate_conflict",
                "verified": False,
                "selected": selected,
                "conflict": second,
                "distance_km": round(distance, 3),
                "candidates": rows,
            }

    return {
        "status": "verified",
        "verified": True,
        "selected": selected,
        "candidates": rows,
    }


def _locationiq(
    query: str,
    *,
    venue_name: str,
    expected_city: str,
    expected_country: str,
) -> dict[str, Any]:
    if not settings.locationiq_key:
        return {
            "status": "not_configured",
            "verified": False,
            "provider": "LocationIQ",
        }
    try:
        response = requests.get(
            f"{settings.locationiq_base_url}/search",
            params={
                "key": settings.locationiq_key,
                "q": query,
                "format": "json",
                "addressdetails": 1,
                "namedetails": 1,
                "extratags": 1,
                "normalizecity": 1,
                "dedupe": 1,
                "limit": 8,
                "accept-language": "en",
            },
            headers={
                "Accept": "application/json",
                "User-Agent": settings.nominatim_user_agent,
            },
            timeout=(
                5,
                settings.locationiq_timeout_seconds,
            ),
        )
    except requests.RequestException as exc:
        return {
            "status": "transport_error",
            "verified": False,
            "provider": "LocationIQ",
            "error_type": type(exc).__name__,
        }

    if response.status_code != 200:
        return {
            "status": "http_error",
            "verified": False,
            "provider": "LocationIQ",
            "http_status": response.status_code,
        }
    try:
        payload = response.json()
    except ValueError:
        payload = []

    evaluated = [
        row
        for row in (
            _evaluate_candidate(
                candidate,
                venue_name=venue_name,
                expected_city=expected_city,
                expected_country=expected_country,
                provider="LocationIQ",
            )
            for candidate in (
                payload if isinstance(payload, list) else []
            )
        )
        if isinstance(row, dict)
    ]
    selected = _select_candidate(evaluated)
    return {
        **selected,
        "provider": "LocationIQ",
        "http_status": response.status_code,
    }


def _nominatim(
    query: str,
    *,
    venue_name: str,
    expected_city: str,
    expected_country: str,
) -> dict[str, Any]:
    if not settings.nominatim_enabled:
        return {
            "status": "disabled",
            "verified": False,
            "provider": "Nominatim",
        }

    global _NOMINATIM_LAST_CALL
    with _NOMINATIM_LOCK:
        elapsed = time.monotonic() - _NOMINATIM_LAST_CALL
        if elapsed < 1.1:
            time.sleep(1.1 - elapsed)
        try:
            response = requests.get(
                f"{settings.nominatim_base_url}/search",
                params={
                    "q": query,
                    "format": "jsonv2",
                    "addressdetails": 1,
                    "namedetails": 1,
                    "extratags": 1,
                    "dedupe": 1,
                    "limit": 8,
                    "accept-language": "en",
                },
                headers={
                    "Accept": "application/json",
                    "User-Agent": settings.nominatim_user_agent,
                    "Referer": settings.nominatim_referer,
                },
                timeout=(
                    5,
                    settings.nominatim_timeout_seconds,
                ),
            )
        except requests.RequestException as exc:
            _NOMINATIM_LAST_CALL = time.monotonic()
            return {
                "status": "transport_error",
                "verified": False,
                "provider": "Nominatim",
                "error_type": type(exc).__name__,
            }
        _NOMINATIM_LAST_CALL = time.monotonic()

    if response.status_code != 200:
        return {
            "status": "http_error",
            "verified": False,
            "provider": "Nominatim",
            "http_status": response.status_code,
        }
    try:
        payload = response.json()
    except ValueError:
        payload = []

    evaluated = [
        row
        for row in (
            _evaluate_candidate(
                candidate,
                venue_name=venue_name,
                expected_city=expected_city,
                expected_country=expected_country,
                provider="Nominatim",
            )
            for candidate in (
                payload if isinstance(payload, list) else []
            )
        )
        if isinstance(row, dict)
    ]
    selected = _select_candidate(evaluated)
    return {
        **selected,
        "provider": "Nominatim",
        "http_status": response.status_code,
        "attribution": "© OpenStreetMap contributors",
    }


def geocode_venue(
    *,
    venue_name: str,
    city: str = "",
    country: str = "",
    progress: Callable[[str, str], None] | None = None,
) -> dict[str, Any]:
    query = ", ".join(
        value for value in (venue_name, city, country)
        if str(value or "").strip()
    )
    if progress:
        progress(
            "geocode_cache",
            "Checking the verified geocode cache.",
        )
    cached = _cache_get(query)
    if isinstance(cached, dict):
        if progress:
            progress(
                "geocode_cache_hit",
                "A cached geocode result was found.",
            )
        return {**cached, "cached": True}

    if progress:
        progress(
            "locationiq",
            "Checking LocationIQ for the exact stadium.",
        )
    primary = _locationiq(
        query,
        venue_name=venue_name,
        expected_city=city,
        expected_country=country,
    )
    if primary.get("verified") is True:
        result = {
            **primary,
            "query": query,
            "fallback_used": False,
        }
        _cache_set(
            query=query,
            provider="LocationIQ",
            result=result,
        )
        return result

    if progress:
        progress(
            "nominatim",
            "LocationIQ did not verify the stadium; "
            "checking the rate-limited OpenStreetMap fallback.",
        )
    fallback = _nominatim(
        query,
        venue_name=venue_name,
        expected_city=city,
        expected_country=country,
    )
    result = {
        **fallback,
        "query": query,
        "fallback_used": True,
        "primary": primary,
    }
    _cache_set(
        query=query,
        provider=str(fallback.get("provider") or "none"),
        result=result,
        days=7 if not result.get("verified") else 30,
    )
    return result
