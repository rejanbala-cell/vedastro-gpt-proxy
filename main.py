from __future__ import annotations

import json
import hashlib
import math
import os
import threading
import time
import re
import statistics
import unicodedata
from datetime import date, datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from enum import Enum
from typing import Any, Callable
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

try:
    import swisseph as swe
except ImportError:
    swe = None

try:
    import psycopg
except ImportError:
    psycopg = None

try:
    from timezonefinder import timezone_at
except ImportError:
    timezone_at = None

import requests
from fastapi import FastAPI, Header, HTTPException, Query
from pydantic import BaseModel, Field
from vedastro import (
    Ayanamsa,
    Calculate,
    GeoLocation,
    HouseName,
    PlanetName,
    Time,
)


# ============================================================
# VERSION
# ============================================================

PROXY_VERSION = "1.20.0-db8a"


# ============================================================
# ENVIRONMENT SETTINGS
# ============================================================

VEDASTRO_API_KEY = os.getenv("VEDASTRO_API_KEY", "").strip()
PROXY_API_KEY = os.getenv("PROXY_API_KEY", "").strip()

# PostgreSQL is introduced in the 1.20.0 database checkpoint.
# This checkpoint only verifies connectivity; it does not create tables yet.
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
DATABASE_CONNECT_TIMEOUT_SECONDS = int(
    os.getenv("DATABASE_CONNECT_TIMEOUT_SECONDS", "8")
)
DATABASE_SCHEMA_VERSION = "1.20.0-db7"

# API-Football connectivity checkpoint.
# This version verifies the provider account through GET /status only.
API_FOOTBALL_KEY = os.getenv("API_FOOTBALL_KEY", "").strip()
API_FOOTBALL_BASE_URL = os.getenv(
    "API_FOOTBALL_BASE_URL",
    "https://v3.football.api-sports.io",
).rstrip("/")
API_FOOTBALL_TIMEOUT_SECONDS = int(
    os.getenv("API_FOOTBALL_TIMEOUT_SECONDS", "12")
)
API_FOOTBALL_HEALTH_CACHE_SECONDS = int(
    os.getenv("API_FOOTBALL_HEALTH_CACHE_SECONDS", "3600")
)

# Fixture-import checkpoint. One provider call imports today's fixtures.
# A database timestamp prevents repeated Render restarts from consuming calls.
SOCCER_DISPLAY_TIMEZONE = os.getenv(
    "SOCCER_DISPLAY_TIMEZONE",
    "Australia/Sydney",
).strip()
FIXTURE_SYNC_MIN_INTERVAL_SECONDS = int(
    os.getenv("FIXTURE_SYNC_MIN_INTERVAL_SECONDS", "21600")
)
FIXTURE_LIST_DEFAULT_LIMIT = int(
    os.getenv("FIXTURE_LIST_DEFAULT_LIMIT", "250")
)
FIXTURE_LIST_MAX_LIMIT = int(
    os.getenv("FIXTURE_LIST_MAX_LIMIT", "500")
)

# DB8 captures one pre-match 1X2 market snapshot for the verified checkpoint
# fixture. The provider publishes pre-match odds through GET /odds.
MARKET_CAPTURE_AUTO_RUN = os.getenv(
    "MARKET_CAPTURE_AUTO_RUN",
    "true",
).strip().lower() in {"1", "true", "yes", "on"}
MARKET_CAPTURE_TARGET_FIXTURE_ID = int(
    os.getenv("MARKET_CAPTURE_TARGET_FIXTURE_ID", "530")
)
MARKET_CAPTURE_MIN_INTERVAL_SECONDS = int(
    os.getenv("MARKET_CAPTURE_MIN_INTERVAL_SECONDS", "10800")
)
MARKET_MIN_BOOKMAKER_COUNT = int(
    os.getenv("MARKET_MIN_BOOKMAKER_COUNT", "3")
)

LOCATIONIQ_KEY = os.getenv("LOCATIONIQ_KEY", "").strip()
LOCATIONIQ_BASE_URL = os.getenv(
    "LOCATIONIQ_BASE_URL",
    "https://us1.locationiq.com/v1",
).rstrip("/")
LOCATIONIQ_TIMEOUT_SECONDS = int(
    os.getenv("LOCATIONIQ_TIMEOUT_SECONDS", "12")
)
LOCATIONIQ_HEALTH_CACHE_SECONDS = int(
    os.getenv("LOCATIONIQ_HEALTH_CACHE_SECONDS", "86400")
)
LOCATIONIQ_HEALTH_TEST_QUERY = os.getenv(
    "LOCATIONIQ_HEALTH_TEST_QUERY",
    "Sydney Opera House, Sydney, Australia",
).strip()

# DB6 safely previews one real stored venue. It never writes coordinates into
# fixtures yet. The preview is cached permanently in PostgreSQL.
LOCATION_PREVIEW_AUTO_RUN = os.getenv(
    "LOCATION_PREVIEW_AUTO_RUN",
    "true",
).strip().lower() in {"1", "true", "yes", "on"}
LOCATION_PREVIEW_LOOKAHEAD_DAYS = int(
    os.getenv("LOCATION_PREVIEW_LOOKAHEAD_DAYS", "7")
)
LOCATION_PREVIEW_AUTO_APPROVE_SCORE = float(
    os.getenv("LOCATION_PREVIEW_AUTO_APPROVE_SCORE", "85")
)
LOCATION_PREVIEW_QUEUE_SCAN_LIMIT = int(
    os.getenv("LOCATION_PREVIEW_QUEUE_SCAN_LIMIT", "100")
)

# DB6C uses a two-stage strategy:
# 1. verify/cache the fixture city and country
# 2. search for the venue only inside a bounded box around that city
LOCATION_GEOCODE_STRATEGY_VERSION = "city_bounded_v1"
LOCATION_CITY_VIEWBOX_LAT_DELTA = float(
    os.getenv("LOCATION_CITY_VIEWBOX_LAT_DELTA", "0.45")
)
LOCATION_MAX_CITY_DISTANCE_KM = float(
    os.getenv("LOCATION_MAX_CITY_DISTANCE_KM", "75")
)

# A bounded search can legitimately return HTTP 404 when no place was found.
# DB6D caches that negative result and advances through a small startup batch.
LOCATION_PREVIEW_STARTUP_MAX_FIXTURES = int(
    os.getenv("LOCATION_PREVIEW_STARTUP_MAX_FIXTURES", "3")
)
LOCATION_PREVIEW_STARTUP_MAX_PROVIDER_CALLS = int(
    os.getenv("LOCATION_PREVIEW_STARTUP_MAX_PROVIDER_CALLS", "6")
)

# DB7 contains one independently reviewed venue manifest. It commits only when
# every cached geocode guard matches. No other AUTO_APPROVED venue is written.
REVIEWED_LOCATION_COMMIT_ENABLED = os.getenv(
    "REVIEWED_LOCATION_COMMIT_ENABLED",
    "true",
).strip().lower() in {"1", "true", "yes", "on"}

DATABASE_EXPECTED_TABLES = (
    "app_metadata",
    "fixtures",
    "odds_snapshots",
    "performance_snapshots",
    "chart_runs",
    "prediction_runs",
    "official_results",
    "post_match_audits",
    "model_versions",
    "training_runs",
    "venue_geocodes",
    "location_contexts",
    "location_attempts",
    "location_reviews",
)

# This is the MINIMUM delay between the START of upstream calls.
# 0.20 = no more than about five new VedAstro calls started per second
# by this Render worker. It is not a timeout and not a subscription limit.
VEDASTRO_MIN_INTERVAL_SECONDS = float(
    os.getenv("VEDASTRO_MIN_INTERVAL_SECONDS", "0.20")
)

# Despite the historical environment-variable name, this is the maximum
# TOTAL number of attempts for one failed VedAstro method call.
# 2 = first attempt + one retry, but only for temporary/retryable failures.
VEDASTRO_MAX_RETRIES = int(
    os.getenv("VEDASTRO_MAX_RETRIES", "2")
)

# Parallel house/planet groups. Individual upstream requests are still paced
# by VEDASTRO_MIN_INTERVAL_SECONDS.
VEDASTRO_MAX_WORKERS = int(
    os.getenv("VEDASTRO_MAX_WORKERS", "6")
)

VEDASTRO_REQUEST_TIMEOUT_SECONDS = float(
    os.getenv("VEDASTRO_REQUEST_TIMEOUT_SECONDS", "120")
)

MAX_RESULT_CHARACTERS = int(
    os.getenv("MAX_RESULT_CHARACTERS", "700")
)

MAX_RESPONSE_CHARACTERS = int(
    os.getenv("MAX_RESPONSE_CHARACTERS", "70000")
)

# Hard target for Custom GPT Action responses. This is deliberately
# lower than the server-side maximum to avoid ResponseTooLargeError.
ACTION_RESPONSE_TARGET_CHARACTERS = int(
    os.getenv("ACTION_RESPONSE_TARGET_CHARACTERS", "36000")
)

# Leave room for final metadata and JSON serialization differences between
# local tests and the Custom GPT Action transport.
ACTION_RESPONSE_SAFETY_MARGIN_CHARACTERS = int(
    os.getenv("ACTION_RESPONSE_SAFETY_MARGIN_CHARACTERS", "1500")
)
ACTION_RESPONSE_SAFETY_TARGET_CHARACTERS = max(
    24000,
    ACTION_RESPONSE_TARGET_CHARACTERS
    - ACTION_RESPONSE_SAFETY_MARGIN_CHARACTERS,
)

# The deterministic limiter targets this smaller payload size before final
# character-count and transport metadata are added.
ACTION_RESPONSE_PAYLOAD_TARGET_CHARACTERS = max(
    23000,
    ACTION_RESPONSE_SAFETY_TARGET_CHARACTERS - 700,
)

# Reliability policy:
# - strict_book: preserves the most conservative book-locked gate.
# - practical_verified: exact same-local-date stations remain vetoes, while
#   non-same-day Vikala/near-station testimony becomes a LOW-confidence warning.
RELIABILITY_POLICY_MODE = os.getenv(
    "RELIABILITY_POLICY_MODE",
    "strict_book",
).strip().lower()

if RELIABILITY_POLICY_MODE not in {
    "strict_book",
    "practical_verified",
}:
    raise RuntimeError(
        "RELIABILITY_POLICY_MODE must be strict_book or practical_verified."
    )


# Optional directory containing Swiss Ephemeris .se1 files. Uranus, Neptune
# and Pluto work through the built-in Moshier fallback when no files are
# supplied. Ceres and Chiron require external asteroid ephemeris files.
SWISSEPH_EPHE_PATH = os.getenv(
    "SWISSEPH_EPHE_PATH",
    "",
).strip()

SWISSEPH_AVAILABLE = swe is not None
SWISSEPH_LOCK = threading.Lock()

if not VEDASTRO_API_KEY:
    raise RuntimeError("VEDASTRO_API_KEY is missing from Render.")

if not PROXY_API_KEY:
    raise RuntimeError("PROXY_API_KEY is missing from Render.")

if VEDASTRO_MIN_INTERVAL_SECONDS < 0:
    raise RuntimeError("VEDASTRO_MIN_INTERVAL_SECONDS cannot be negative.")

if VEDASTRO_MAX_RETRIES < 1:
    raise RuntimeError("VEDASTRO_MAX_RETRIES must be at least 1.")

if VEDASTRO_MAX_WORKERS < 1:
    raise RuntimeError("VEDASTRO_MAX_WORKERS must be at least 1.")

if ACTION_RESPONSE_TARGET_CHARACTERS < 12000:
    raise RuntimeError(
        "ACTION_RESPONSE_TARGET_CHARACTERS must be at least 12000."
    )

if SWISSEPH_AVAILABLE and SWISSEPH_EPHE_PATH:
    try:
        swe.set_ephe_path(SWISSEPH_EPHE_PATH)
    except Exception as error:
        raise RuntimeError(
            f"Invalid SWISSEPH_EPHE_PATH: {error}"
        ) from error


# ============================================================
# CONFIGURE AND PATCH THE OFFICIAL VEDASTRO CLIENT
# ============================================================

# Keep this for compatibility with any generated-client method that reads
# Calculate.api_key internally. The patched request method below also sends
# the paid key explicitly using the official x-api-key header.
Calculate.SetAPIKey(VEDASTRO_API_KEY)

# The generated VedAstro client stores ayanamsa globally, but this proxy
# serves concurrent requests. A thread-local payload override keeps the
# standard chart on Lahiri while allowing an isolated Krishnamurti KP layer.
_AYANAMSHA_CONTEXT = threading.local()
DEFAULT_REQUEST_AYANAMSHA = "LAHIRI"


def active_request_ayanamsa() -> str:
    return str(
        getattr(
            _AYANAMSHA_CONTEXT,
            "name",
            DEFAULT_REQUEST_AYANAMSHA,
        )
    ).upper()


@contextmanager
def use_request_ayanamsa(name: str):
    previous = getattr(_AYANAMSHA_CONTEXT, "name", None)
    _AYANAMSHA_CONTEXT.name = str(name).upper()

    try:
        yield
    finally:
        if previous is None:
            try:
                delattr(_AYANAMSHA_CONTEXT, "name")
            except AttributeError:
                pass
        else:
            _AYANAMSHA_CONTEXT.name = previous


PLANET_LITERAL_NAMES = {
    "Sun",
    "Moon",
    "Mars",
    "Mercury",
    "Jupiter",
    "Venus",
    "Saturn",
    "Rahu",
    "Ketu",
}


def _json_from_to_json(value: Any) -> Any:
    """Safely convert VedAstro data objects to JSON-compatible values."""

    converted = value.to_json()

    if isinstance(converted, str):
        stripped = converted.strip()
        if stripped.startswith("{") or stripped.startswith("["):
            try:
                return json.loads(stripped)
            except json.JSONDecodeError:
                return converted

    return converted


def fix_api_value(key: str, value: Any) -> Any:
    """
    Convert generated-client parameter values into VedAstro POST JSON.

    The June 2026 generated client can send a planet as a plain string:

        "planetName": "Moon"

    The live API expects a nested PlanetName object:

        "planetName": {"Name": "Moon"}
    """

    if isinstance(value, Enum):
        value = value.value

    if hasattr(value, "to_json") and callable(value.to_json):
        value = _json_from_to_json(value)

    if isinstance(value, dict):
        return {
            str(child_key): fix_api_value(str(child_key), child_value)
            for child_key, child_value in value.items()
        }

    if isinstance(value, (list, tuple, set)):
        return [fix_api_value(key, item) for item in value]

    if (
        isinstance(value, str)
        and "planet" in key.lower()
        and value in PLANET_LITERAL_NAMES
    ):
        return {"Name": value}

    return value


def make_request_fixed(
    cls,
    endpoint: str,
    params: dict[str, Any],
):
    """
    Replacement for Calculate._make_request.

    Important fixes:
    - paid subscriber key is sent in x-api-key on every request;
    - APIKey remains in the body as a backwards-compatible fallback;
    - Lahiri is the default, with a thread-local KP override;
    - planet parameters use the nested {"Name": "Moon"} shape.
    """

    payload = {
        str(key): fix_api_value(str(key), value)
        for key, value in dict(params).items()
    }

    payload["Ayanamsa"] = active_request_ayanamsa()

    # Header is the current recommended authentication method. The body field
    # remains because older VedAstro server/client paths also recognise it.
    payload["APIKey"] = VEDASTRO_API_KEY

    url = f"{cls.base_url.rstrip('/')}/{endpoint.lstrip('/')}"

    try:
        response = requests.post(
            url,
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "x-api-key": VEDASTRO_API_KEY,
            },
            json=payload,
            timeout=VEDASTRO_REQUEST_TIMEOUT_SECONDS,
        )
    except requests.RequestException as error:
        raise RuntimeError(f"VedAstro connection error: {error}") from error

    if not response.ok:
        body_preview = response.text[:1000]
        raise RuntimeError(
            f"VedAstro HTTP {response.status_code}: {body_preview}"
        )

    try:
        data = response.json()
    except ValueError as error:
        raise RuntimeError(
            "VedAstro returned a non-JSON response: "
            f"{response.text[:1000]}"
        ) from error

    if not isinstance(data, dict):
        raise RuntimeError(
            "VedAstro returned an unexpected response type: "
            f"{type(data).__name__}"
        )

    status = data.get("Status", data.get("status"))

    if isinstance(status, str) and status.lower() == "fail":
        error_payload = data.get(
            "Payload",
            data.get("payload", data),
        )
        raise RuntimeError(f"VedAstro API error: {error_payload}")

    if "Payload" in data:
        result_payload = data["Payload"]
    elif "payload" in data:
        result_payload = data["payload"]
    else:
        raise ValueError("Payload is missing in VedAstro response.")

    if result_payload is None:
        raise ValueError("Payload is null in VedAstro response.")

    if isinstance(result_payload, list):
        return result_payload

    if isinstance(result_payload, dict):
        values = list(result_payload.values())
        return values[0] if len(values) == 1 else result_payload

    return result_payload


Calculate._make_request = classmethod(make_request_fixed)

# Some older package builds still expose SetAyanamsa.
if hasattr(Calculate, "SetAyanamsa"):
    try:
        Calculate.SetAyanamsa(Ayanamsa.Lahiri)
    except Exception:
        pass


# ============================================================
# FASTAPI
# ============================================================

app = FastAPI(
    title="VedAstro GPT Proxy",
    version=PROXY_VERSION,
    description=(
        "Compact Lahiri event-chart proxy using the official "
        "VedAstro.Python client, paid-key header authentication, "
        "nested planet parameters, exact Placidus cusps, "
        "planet-to-cusp contacts, exact Navamsha cusp geometry, "
        "Krishnamurti KP sublords, outer-planet geometry, "
        "Gulika/Upaketu geometry, Chapter 7 name sounds, "
        "Chapter 8 nakshatra taras and strict validation."
    ),
)


# ============================================================
# PLANETS AND HOUSES
# ============================================================

PLANETS = {
    "Sun": PlanetName.Sun,
    "Moon": PlanetName.Moon,
    "Mars": PlanetName.Mars,
    "Mercury": PlanetName.Mercury,
    "Jupiter": PlanetName.Jupiter,
    "Venus": PlanetName.Venus,
    "Saturn": PlanetName.Saturn,
    "Rahu": PlanetName.Rahu,
    "Ketu": PlanetName.Ketu,
}

HOUSES = {
    f"House{i}": getattr(HouseName, f"House{i}")
    for i in range(1, 13)
}

DEFAULT_HOUSES = ["House1", "House7"]
DEFAULT_PLANETS = ["Sun", "Moon"]

ZODIAC_SIGNS = (
    "Aries",
    "Taurus",
    "Gemini",
    "Cancer",
    "Leo",
    "Virgo",
    "Libra",
    "Scorpio",
    "Sagittarius",
    "Capricorn",
    "Aquarius",
    "Pisces",
)


# Gambler's Dharma Chapter 4 cusp-contact policy.
# The currently supported invisible bodies are Rahu and Ketu. Outer planets
# and special points will be added in a later version.
VISIBLE_CUSP_ORB_DEGREES = 2.5
INVISIBLE_CUSP_ORB_DEGREES = 2.0
INVISIBLE_CUSP_BODIES = {"Rahu", "Ketu"}

# Primary contest cusps used by the book.
SENSITIVE_CUSP_DETAILS = {
    "House1": {"axis": "1/7", "side": "Favourite"},
    "House7": {"axis": "1/7", "side": "Underdog"},
    "House6": {"axis": "6/12", "side": "Favourite"},
    "House12": {"axis": "6/12", "side": "Underdog"},
    "House10": {"axis": "10/4", "side": "Favourite"},
    "House4": {"axis": "10/4", "side": "Underdog"},
}


# Gambler's Dharma Chapter 4 stolen-cusp method.
#
# Printed book pages 93-99:
# - power cusps: 1/7, 6/12 and 4/10
# - neutral cusps: 3/9 and 5/11
# - power-to-neutral weakens a planet's cusp effect
# - neutral-to-power activates the neutral cusp as the whole-sign power house
# - power-to-power redirects the contact to the new power house
STOLEN_CUSP_POWER_HOUSES = {1, 4, 6, 7, 10, 12}
STOLEN_CUSP_NEUTRAL_HOUSES = {3, 5, 9, 11}

STOLEN_CUSP_SIDE_BY_POWER_HOUSE = {
    1: "Favourite",
    6: "Favourite",
    10: "Favourite",
    7: "Underdog",
    12: "Underdog",
    4: "Underdog",
}

STOLEN_CUSP_AXIS_BY_POWER_HOUSE = {
    1: "1/7",
    7: "1/7",
    6: "6/12",
    12: "6/12",
    10: "10/4",
    4: "10/4",
}

STOLEN_CUSP_PDF_PAGES = [93, 94, 95, 96, 97, 98, 99]


# Gambler's Dharma Chapter 5 exact Navamsha geometry.
#
# Each 30-degree rashi sign is divided into nine 3°20' sections. The
# position inside one 3°20' section is expanded by a factor of nine to
# obtain the exact degree inside the corresponding D9 sign.
NAVAMSHA_DIVISIONS_PER_SIGN = 9
NAVAMSHA_SECTION_DEGREES = 30.0 / NAVAMSHA_DIVISIONS_PER_SIGN
NAVAMSHA_CUSP_ORB_DEGREES = 2.5

# First D9 sign for each D1 sign, using zero-based zodiac indexes:
# Fire -> Aries, Earth -> Capricorn, Air -> Libra, Water -> Cancer.
NAVAMSHA_START_SIGN_INDEX = {
    0: 0,   # Aries -> Aries
    1: 9,   # Taurus -> Capricorn
    2: 6,   # Gemini -> Libra
    3: 3,   # Cancer -> Cancer
    4: 0,   # Leo -> Aries
    5: 9,   # Virgo -> Capricorn
    6: 6,   # Libra -> Libra
    7: 3,   # Scorpio -> Cancer
    8: 0,   # Sagittarius -> Aries
    9: 9,   # Capricorn -> Capricorn
    10: 6,  # Aquarius -> Libra
    11: 3,  # Pisces -> Cancer
}


# Gambler's Dharma Chapter 6 / Krishnamurti Paddhati.
KP_AYANAMSHA_NAME = "KRISHNAMURTI"

VIMSHOTTARI_SEQUENCE = (
    "Ketu",
    "Venus",
    "Sun",
    "Moon",
    "Mars",
    "Rahu",
    "Jupiter",
    "Saturn",
    "Mercury",
)

VIMSHOTTARI_YEARS = {
    "Ketu": 7,
    "Venus": 20,
    "Sun": 6,
    "Moon": 10,
    "Mars": 7,
    "Rahu": 18,
    "Jupiter": 16,
    "Saturn": 19,
    "Mercury": 17,
}

NAKSHATRA_NAMES = (
    "Ashwini",
    "Bharani",
    "Krittika",
    "Rohini",
    "Mrigashirsha",
    "Ardra",
    "Punarvasu",
    "Pushya",
    "Ashlesha",
    "Magha",
    "Purva Phalguni",
    "Uttara Phalguni",
    "Hasta",
    "Chitra",
    "Swati",
    "Vishakha",
    "Anuradha",
    "Jyeshtha",
    "Mula",
    "Purva Ashadha",
    "Uttara Ashadha",
    "Shravana",
    "Dhanishta",
    "Shatabhisha",
    "Purva Bhadrapada",
    "Uttara Bhadrapada",
    "Revati",
)

NAKSHATRA_LORDS = tuple(
    VIMSHOTTARI_SEQUENCE[index % 9]
    for index in range(27)
)

SIGN_LORDS = {
    "Aries": "Mars",
    "Taurus": "Venus",
    "Gemini": "Mercury",
    "Cancer": "Moon",
    "Leo": "Sun",
    "Virgo": "Mercury",
    "Libra": "Venus",
    "Scorpio": "Mars",
    "Sagittarius": "Jupiter",
    "Capricorn": "Saturn",
    "Aquarius": "Saturn",
    "Pisces": "Jupiter",
}


# Gambler's Dharma Chapter 3: victory houses and contest yogas.
TIER1_FAVOURITE_VICTORY_HOUSES = {1, 3, 6, 10, 11}
TIER1_UNDERDOG_VICTORY_HOUSES = {4, 5, 7, 9, 12}

TIER1_NATURAL_MALEFICS = {
    "Sun",
    "Mars",
    "Saturn",
    "Rahu",
    "Ketu",
}

# Conservative automatic SKY and victory-house benefics. The Moon is
# deliberately kept as a separate manual candidate because the author says
# he normally excludes it from victory-house scoring and its benefic status
# also depends on phase.
TIER1_NATURAL_BENEFICS = {
    "Mercury",
    "Jupiter",
    "Venus",
}

TIER1_CLASSICAL_PKY_MALEFICS = {
    "Sun",
    "Mars",
    "Saturn",
}

TIER1_NODE_MALEFICS = {
    "Rahu",
    "Ketu",
}

TIER1_PLANETARY_WAR_PLANETS = (
    "Mercury",
    "Venus",
    "Mars",
    "Jupiter",
    "Saturn",
)

TIER1_SWISSEPH_BODY_IDS = {
    "Mercury": 2,
    "Venus": 3,
    "Mars": 4,
    "Jupiter": 5,
    "Saturn": 6,
}

TIER1_DIG_BALA_HOUSES = {
    "Sun": 10,
    "Moon": 4,
    "Mercury": 1,
    "Venus": 4,
    "Mars": 10,
    "Jupiter": 1,
    "Saturn": 7,
}

TIER1_PDF_PAGES = {
    "victory_houses": [39, 40, 47, 48, 49, 50, 51, 158, 159, 160],
    "sky_pky": [40, 41, 42, 44, 45, 46, 158],
    "parivartana": [52, 53, 55],
    "planetary_war": [52, 53, 55],
}


# Gambler's Dharma Chapter 5, printed PDF pages 108-140.
#
# Table 5.3 gives the D9 1/7 cusp effects.
# Table 5.4 gives the named D9 combinations.
# Table 5.5 establishes the hierarchy:
#   Tier 3 D9 cusp strength > Tier 2 rashi cusp/SKY/PKY >
#   Tier 1 victory houses and D9 combinations.
# Table 6.5 later values a Navamsha combination at 5 points.
# Book point ranges are kept as intervals. They are never collapsed to
# invented exact scores. Printed pages 158-159.
BOOK_TIER_POINT_INTERVALS = {
    1: [2.0, 4.0],
    2: [7.0, 9.0],
    3: [14.0, 18.0],
}

DECISION_SIDES = {"Favourite", "Underdog"}

NAVAMSHA_INTERPRETATION_PDF_PAGES = {
    "principle": [108, 109],
    "cusp_method": [109, 112, 113, 114, 115, 116],
    "combinations": [124, 126, 127],
    "double_whammy": [131, 132],
    "hierarchy": [136, 137, 140],
    "points": [173, 174],
}

D9_COMBINATION_TABLE = {
    frozenset(("Sun", "Ketu")): {
        "effect": "Loss",
        "rule_grade": "Table 5.4",
        "automatic_points": 5.0,
        "pdf_pages": [124, 126],
    },
    frozenset(("Venus", "Ketu")): {
        "effect": "Win",
        "rule_grade": "Table 5.4",
        "automatic_points": 5.0,
        "pdf_pages": [124, 126, 127],
    },
    frozenset(("Sun", "Jupiter")): {
        "effect": "Loss",
        "rule_grade": "Table 5.4",
        "automatic_points": 5.0,
        "pdf_pages": [124, 126],
    },
    frozenset(("Moon", "Rahu")): {
        "effect": "Win",
        "rule_grade": "Table 5.4",
        "automatic_points": 5.0,
        "pdf_pages": [124, 126],
    },
    frozenset(("Moon", "Saturn")): {
        "effect": "Loss",
        "rule_grade": "Table 5.4",
        "automatic_points": 5.0,
        "pdf_pages": [124, 126],
    },
    frozenset(("Venus", "Rahu")): {
        "effect": "Loss",
        "rule_grade": "Table 5.4",
        "automatic_points": 5.0,
        "pdf_pages": [124, 126],
    },
    frozenset(("Sun", "Saturn")): {
        "effect": "Loss",
        "rule_grade": "Explicit Chapter 5 text",
        "automatic_points": 5.0,
        "pdf_pages": [124],
    },
}

# The author explicitly says these appear promising but need more study.
# They are reported, not automatically scored.
D9_RESEARCH_COMBINATIONS = {
    frozenset(("Mars", "Saturn")): {
        "effect": "Win",
        "rule_grade": "Research tendency",
        "automatic_points": 0.0,
        "pdf_pages": [124],
    },
    frozenset(("Mars", "Jupiter")): {
        "effect": "Win",
        "rule_grade": "Research tendency",
        "automatic_points": 0.0,
        "pdf_pages": [124],
    },
}

D9_COMBINATION_ALLOWED_PLANETS = {
    "Sun",
    "Moon",
    "Mars",
    "Mercury",
    "Jupiter",
    "Venus",
    "Saturn",
    "Rahu",
    "Ketu",
}

D9_VISIBLE_CUSP_BODIES = {
    "Sun",
    "Moon",
    "Mars",
    "Mercury",
    "Jupiter",
    "Venus",
    "Saturn",
}

D9_INVISIBLE_CUSP_BODIES = {
    "Rahu",
    "Ketu",
    "Uranus",
    "Neptune",
    "Pluto",
    "Ceres",
    "Chiron",
    "Gulika",
    "Upaketu",
}


# Gambler's Dharma reliability and sandhi audit.
#
# Printed book pages:
# - 23: do not wager when planets are kutila/stationary
# - 26-28: fixed, mixed and nonfixed karma; rule of three
# - 32: understand major sandhis and avoid prediction
# - 217-219: sunrise/sunset, eclipses, solar ingress and stationary planets
RELIABILITY_AUDIT_PDF_PAGES = [
    23,
    26,
    27,
    28,
    32,
    217,
    218,
    219,
]

RELIABILITY_SWISS_BODY_IDS = {
    "Mercury": 2,
    "Venus": 3,
    "Mars": 4,
    "Jupiter": 5,
    "Saturn": 6,
    "Uranus": 7,
    "Neptune": 8,
    "Pluto": 9,
    "Chiron": 15,
    "Ceres": 17,
}

RELIABILITY_STATION_SEARCH_DAYS = 8.0
RELIABILITY_STATION_SCAN_STEP_DAYS = 0.5
RELIABILITY_ECLIPSE_AVOID_DAYS = 3.0

KP_COLUMN_WEIGHTS = {
    "A": 1.0,
    "B": 2.0,
    "C": 3.0,
    "D": 4.0,
}

KP_HOUSE_VALUES = {
    1: 1.0,
    2: 0.5,
    3: 1.0,
    4: -1.0,
    5: -1.0,
    6: 1.0,
    7: -1.0,
    8: -1.0,
    9: -1.0,
    10: 1.0,
    11: 1.0,
    12: -1.0,
}

KP_FAVOURITE_ARRAY_HOUSES = {1, 3, 6, 10, 11}
KP_UNDERDOG_ARRAY_HOUSES = {4, 5, 7, 9, 12}
KP_NEUTRAL_ARRAY_HOUSES = {2, 8}

KP_WEAK_FAVOURITE_ARRAY_HOUSES = {1, 3, 6, 8, 10, 11}
KP_WEAK_UNDERDOG_ARRAY_HOUSES = {2, 4, 5, 7, 9, 12}

PLANET_ORDER = {
    name: index
    for index, name in enumerate(PLANETS)
}


# Swiss Ephemeris body numbers. These are stable Swiss Ephemeris constants.
OUTER_BODY_IDS = {
    "Uranus": 7,
    "Neptune": 8,
    "Pluto": 9,
    "Chiron": 15,
    "Ceres": 17,
}

OUTER_BODY_ORDER = (
    "Uranus",
    "Neptune",
    "Pluto",
    "Ceres",
    "Chiron",
)

OUTER_CUSP_ORB_DEGREES = 2.0

# Qualitative Chapter 4 rules only. No points are assigned in this layer.
OUTER_BODY_BOOK_RULES = {
    "Uranus": {
        "direct": "Supports the team represented by the contacted cusp.",
        "retrograde": "Harms the team represented by the contacted cusp.",
        "stationary": "Uncertain/kutila; do not treat as a clean signal.",
        "axis_note": "Same motion rule on the primary rashi cusps.",
    },
    "Neptune": {
        "direct": "Harms the team represented by the contacted cusp.",
        "retrograde": "Supports the team represented by the contacted cusp.",
        "stationary": "Uncertain/kutila; do not treat as a clean signal.",
        "axis_note": "Negative on any cusp when direct; reversed when retrograde.",
    },
    "Pluto": {
        "direct": "Axis-dependent; motion does not reverse the rule.",
        "retrograde": "Axis-dependent; motion does not reverse the rule.",
        "stationary": "Axis-dependent but station adds uncertainty.",
        "axis_note": (
            "Negative on 1/7; positive on 4/10; "
            "6/12 effect is not explicitly defined by the book."
        ),
    },
    "Ceres": {
        "direct": "Supports the team represented by the contacted cusp.",
        "retrograde": "Harms the team represented by the contacted cusp.",
        "stationary": "Uncertain/kutila; do not treat as a clean signal.",
        "axis_note": "Beneficial direct, detrimental retrograde.",
    },
    "Chiron": {
        "direct": "Supports the team represented by the contacted cusp.",
        "retrograde": (
            "Harms the team represented by the contacted cusp; "
            "injuries or poor play may be indicated."
        ),
        "stationary": "Uncertain/kutila; injuries or poor play may be indicated.",
        "axis_note": "Beneficial direct, detrimental retrograde/stationary.",
    },
}


# Gambler's Dharma invisible-upagraha rules.
SPECIAL_POINT_NAMES = ("Gulika", "Upaketu")
SPECIAL_POINT_CUSP_ORB_DEGREES = 2.0
GULIKA_HOUSE_LORD_ORB_DEGREES = 1.0

# The current generated VedAstro.Python client does not expose these two
# methods, although the official VedAstro server/API Builder does. The proxy
# therefore calls the official endpoint directly through the already patched
# and authenticated Calculate._make_request method.
SPECIAL_POINT_ENDPOINTS = {
    "Gulika": (
        "GulikaLongitude",
        "MaandiLongitude",
        "MandiLongitude",
    ),
    "Upaketu": (
        "UpaketuLongitude",
        "UpaKetuLongitude",
    ),
}


# Gambler's Dharma Chapter 8 marker-star/yogatara method.
#
# These are the book's stated sidereal sign-degrees, not a dynamically
# precessed modern fixed-star catalogue. The author explicitly instructs
# readers to use these rashi positions and notes that some are rounded.
TARA_ORB_DEGREES = 1.0

TARA_TARGETS = {
    "House1": {
        "side": "Favourite",
        "role": "Lagna",
        "priority": "Primary",
        "priority_rank": 1,
    },
    "House10": {
        "side": "Favourite",
        "role": "Honour",
        "priority": "Secondary",
        "priority_rank": 2,
    },
    "House7": {
        "side": "Underdog",
        "role": "Lagna",
        "priority": "Primary",
        "priority_rank": 1,
    },
    "House4": {
        "side": "Underdog",
        "role": "Honour",
        "priority": "Secondary",
        "priority_rank": 2,
    },
}

# Outcome classes:
# - positive / mildly_positive: supports the represented side
# - negative / mildly_negative: harms the represented side
# - axis_dependent: Wasat's Saturn-like cusp rule
# - context_*: descriptive testimony only, no winner direction
# - research_only / none: no automatic contest interpretation
#
# "positions" are absolute sidereal longitudes. "range" is inclusive.

# Gambler's Dharma Chapter 7, Table 7.1.
#
# Each D1 sign contains nine 3°20' nama-pada sections. The values below
# faithfully preserve the book's IAST spellings and slash alternatives.
NAMA_PADA_SECTION_DEGREES = 30.0 / 9.0

NAMA_PADA_TABLE = {
    "Aries": (
        "cu", "ce", "co", "la", "li", "lu", "le", "lo", "a",
    ),
    "Taurus": (
        "i", "u", "e", "o", "va", "vi", "vu", "ve", "vo",
    ),
    "Gemini": (
        "ka", "ki", "ku", "gha", "ṅa/pha", "cha", "ke", "ko", "ha",
    ),
    "Cancer": (
        "hi", "hu", "he", "ho", "ḍa", "ḍi", "ḍu", "ḍe", "ḍo",
    ),
    "Leo": (
        "ma", "mi", "mu", "me", "mo", "ṭa", "ṭi", "ṭu", "ṭe",
    ),
    "Virgo": (
        "ṭo", "pa", "pi", "pu", "ḍa", "ṇa", "ṭha", "pe", "po",
    ),
    "Libra": (
        "ra", "ri", "ru", "re", "ro", "ta", "ti", "tu", "te",
    ),
    "Scorpio": (
        "to", "na", "ni", "nu", "ne", "no", "ya", "yi", "yu",
    ),
    "Sagittarius": (
        "ye", "yo", "ba", "bi", "bu", "dha", "bha", "ḍha", "be",
    ),
    "Capricorn": (
        "bo", "ja/śa", "ji/śi", "ju/śu", "je/śe",
        "jo/śo", "jha/śa", "ga", "gi",
    ),
    "Aquarius": (
        "gu", "ge", "go", "sa", "si", "su", "se", "so", "da",
    ),
    "Pisces": (
        "di", "du", "kha/jha", "ña", "tha", "de", "do", "ca", "ci",
    ),
}

NAMA_PADA_PDF_PAGES = {
    "chapter_opening": 167,
    "table_7_1": 169,
    "main_house10_rule": 170,
    "third_tier_points_example": 172,
    "planet_resonance": 174,
    "sun_research_rule": 175,
    "compound_name_rule": 176,
    "diphthong_rule": 177,
    "nasal_guidance": 179,
    "summary": 183,
}

# These substitutions are explicitly described by Chapter 7. They are used
# only to compare caller-confirmed sounds. They are never used to invent a
# pronunciation from a raw participant name.
NAMA_PADA_BOOK_SUBSTITUTIONS = {
    "w_to_v": True,
    "f_to_pha_or_pa": True,
    "z_to_jha": True,
    "e_to_ai_diphthong": True,
    "o_to_au_diphthong": True,
    "short_long_vowels_equivalent": True,
}

TARA_CATALOG = (
    {
        "nakshatra": "Ashvini",
        "marker": "Sheratan / Hamal",
        "positions": (10.0, 13.75),
        "book_position": "10° Aries; 13°45′ Aries",
        "effect_class": "context_speed",
        "effect": "Light and swift; gives speed.",
        "tier_hint": "Context only",
        "applies_to_cusps": True,
        "applies_to_lords": True,
        "pdf_pages": [203, 208],
    },
    {
        "nakshatra": "Bharani",
        "marker": "Bharani marker stars",
        "positions": (24.33333333,),
        "book_position": "24°20′ Aries",
        "effect_class": "context_harm",
        "effect": "Violence or injury; no proven win/loss direction.",
        "tier_hint": "Context only",
        "applies_to_cusps": True,
        "applies_to_lords": True,
        "pdf_pages": [203, 209],
    },
    {
        "nakshatra": "Krittika",
        "marker": "Alcyone",
        "positions": (36.0,),
        "book_position": "6° Taurus",
        "effect_class": "mildly_positive",
        "effect": "Mildly positive.",
        "tier_hint": "First tier",
        "applies_to_cusps": True,
        "applies_to_lords": True,
        "pdf_pages": [203, 209],
    },
    {
        "nakshatra": "Rohini",
        "marker": "Aldebaran",
        "positions": (46.0,),
        "book_position": "16° Taurus",
        "effect_class": "research_only",
        "effect": (
            "No settled cusp outcome; may help relevant lords, but "
            "the book says more research is needed."
        ),
        "tier_hint": "Research only",
        "applies_to_cusps": True,
        "applies_to_lords": True,
        "pdf_pages": [203, 209],
    },
    {
        "nakshatra": "Mrigashira",
        "marker": "Mrigashira marker star",
        "positions": (59.75,),
        "book_position": "29°45′ Taurus",
        "effect_class": "mildly_positive",
        "effect": "Mild positive boost.",
        "tier_hint": "First tier",
        "applies_to_cusps": True,
        "applies_to_lords": True,
        "pdf_pages": [203, 209],
    },
    {
        "nakshatra": "Ardra",
        "marker": "Betelgeuse",
        "positions": (65.0,),
        "book_position": "5° Gemini",
        "effect_class": "negative",
        "effect": (
            "Negative placement with serious obstacles; resilience "
            "may still overcome it."
        ),
        "tier_hint": "Book does not fix an exact point value",
        "applies_to_cusps": True,
        "applies_to_lords": True,
        "pdf_pages": [203, 210],
    },
    {
        "nakshatra": "Punarvasu",
        "marker": "Pollux",
        "positions": (89.5,),
        "book_position": "29°30′ Gemini",
        "effect_class": "positive",
        "effect": "Strong victory tara.",
        "tier_hint": "Second tier when tight",
        "applies_to_cusps": True,
        "applies_to_lords": True,
        "pdf_pages": [203, 204, 210],
    },
    {
        "nakshatra": "Pushya",
        "marker": "Pushya cluster",
        "positions": (103.5, 105.0),
        "book_position": "13°30′ or 15° Cancer",
        "effect_class": "none",
        "effect": "No dependable contest effect established.",
        "tier_hint": "None",
        "applies_to_cusps": False,
        "applies_to_lords": False,
        "pdf_pages": [203, 211],
    },
    {
        "nakshatra": "Ashlesha",
        "marker": "Ashlesha marker stars",
        "positions": (108.0, 110.66666667),
        "book_position": "18° or 20°40′ Cancer",
        "effect_class": "context_harm",
        "effect": "May paralyse, freeze or suffocate; not a proven winner rule.",
        "tier_hint": "Context only",
        "applies_to_cusps": True,
        "applies_to_lords": True,
        "pdf_pages": [203, 211],
    },
    {
        "nakshatra": "Magha",
        "marker": "Regulus",
        "positions": (126.0,),
        "book_position": "6° Leo",
        "effect_class": "positive",
        "effect": "Confers victory.",
        "tier_hint": "Second tier when tight",
        "applies_to_cusps": True,
        "applies_to_lords": True,
        "pdf_pages": [203, 206, 211],
    },
    {
        "nakshatra": "Purva Phalguni",
        "marker": "Zosma",
        "positions": (137.5,),
        "book_position": "17°30′ Leo",
        "effect_class": "mildly_negative",
        "effect": "Mildly negative or lazy influence.",
        "tier_hint": "First tier",
        "applies_to_cusps": True,
        "applies_to_lords": True,
        "pdf_pages": [203, 211],
    },
    {
        "nakshatra": "Uttara Phalguni",
        "marker": "Denebola",
        "positions": (147.75,),
        "book_position": "27°45′ Leo",
        "effect_class": "mildly_negative",
        "effect": "Mildly negative or lazy influence.",
        "tier_hint": "First tier",
        "applies_to_cusps": True,
        "applies_to_lords": True,
        "pdf_pages": [203, 211],
    },
    {
        "nakshatra": "Hasta",
        "marker": "Book gives conflicting candidate degrees",
        "positions": (),
        "book_position": "Ambiguous in the book text/table",
        "effect_class": "none",
        "effect": "No dependable contest effect established.",
        "tier_hint": "Unavailable",
        "applies_to_cusps": False,
        "applies_to_lords": False,
        "pdf_pages": [203, 211, 212],
    },
    {
        "nakshatra": "Chitra",
        "marker": "Spica",
        "positions": (179.91666667,),
        "book_position": "29°55′ Virgo",
        "effect_class": "negative",
        "effect": "Loss-producing on contest cusps.",
        "tier_hint": "Second tier when tight",
        "applies_to_cusps": True,
        "applies_to_lords": False,
        "lord_note": "The book says more research is needed for house lords.",
        "pdf_pages": [203, 212, 218, 219],
    },
    {
        "nakshatra": "Svati",
        "marker": "Arcturus",
        "positions": (180.25,),
        "book_position": "0°15′ Libra",
        "effect_class": "research_only",
        "effect": (
            "Difficult to separate from nearby Spica by longitude; "
            "no independent outcome rule applied."
        ),
        "tier_hint": "Research only",
        "applies_to_cusps": True,
        "applies_to_lords": True,
        "pdf_pages": [203, 212],
    },
    {
        "nakshatra": "Vishakha",
        "marker": "Zuben Elgenubi",
        "positions": (201.0,),
        "book_position": "21° Libra",
        "effect_class": "positive",
        "effect": "Victory-oriented positive influence.",
        "tier_hint": "Second tier when tight",
        "applies_to_cusps": True,
        "applies_to_lords": True,
        "pdf_pages": [203, 212],
    },
    {
        "nakshatra": "Anuradha",
        "marker": "Anuradha marker stars",
        "positions": (218.0, 220.0),
        "book_position": "8° or 10° Scorpio",
        "effect_class": "none",
        "effect": "No dependable contest effect established.",
        "tier_hint": "None",
        "applies_to_cusps": False,
        "applies_to_lords": False,
        "pdf_pages": [203, 212],
    },
    {
        "nakshatra": "Jyeshtha",
        "marker": "Antares",
        "positions": (226.0,),
        "book_position": "16° Scorpio",
        "effect_class": "research_only",
        "effect": (
            "May be destructive for relevant lords; Aldebaran/Antares "
            "opposition can cancel on cusps. More research is required."
        ),
        "tier_hint": "Research only",
        "applies_to_cusps": True,
        "applies_to_lords": True,
        "pdf_pages": [203, 209, 212, 215],
    },
    {
        "nakshatra": "Mula",
        "marker": "Multiple proposed Mula points",
        "positions": (238.5, 240.75, 243.0),
        "book_position": "28°30′ Scorpio; 0°45′ or 3° Sagittarius",
        "effect_class": "none",
        "effect": "No dependable contest effect established.",
        "tier_hint": "None",
        "applies_to_cusps": False,
        "applies_to_lords": False,
        "pdf_pages": [203, 213],
    },
    {
        "nakshatra": "Purva Ashadha",
        "marker": "Kaus Medius",
        "positions": (250.66666667,),
        "book_position": "10°40′ Sagittarius",
        "effect_class": "research_only",
        "effect": "At most a mild positive influence; evidence is limited.",
        "tier_hint": "Research only",
        "applies_to_cusps": True,
        "applies_to_lords": True,
        "pdf_pages": [203, 213],
    },
    {
        "nakshatra": "Uttara Ashadha",
        "marker": "Pelagus / Ascella",
        "positions": (258.5, 259.66666667),
        "book_position": "18°30′ or 19°40′ Sagittarius",
        "effect_class": "none",
        "effect": "No dependable contest effect established.",
        "tier_hint": "None",
        "applies_to_cusps": False,
        "applies_to_lords": False,
        "pdf_pages": [203, 213],
    },
    {
        "nakshatra": "Abhijit",
        "marker": "Vega",
        "positions": (261.5,),
        "book_position": "21°30′ Sagittarius",
        "effect_class": "positive",
        "effect": "Victory-producing.",
        "tier_hint": "Second tier when tight",
        "applies_to_cusps": True,
        "applies_to_lords": True,
        "pdf_pages": [201, 203, 213, 214],
    },
    {
        "nakshatra": "Shravana",
        "marker": "Altair",
        "positions": (278.0,),
        "book_position": "8° Capricorn",
        "effect_class": "context_speed",
        "effect": "Speed, surprise and performance beyond expectations.",
        "tier_hint": "First tier",
        "applies_to_cusps": True,
        "applies_to_lords": True,
        "pdf_pages": [203, 214],
    },
    {
        "nakshatra": "Dhanishta",
        "marker": "Beta / Alpha Delphini",
        "positions": (292.5, 294.0),
        "book_position": "22°30′ and 24° Capricorn",
        "effect_class": "none",
        "effect": "No dependable contest effect established.",
        "tier_hint": "None",
        "applies_to_cusps": False,
        "applies_to_lords": False,
        "pdf_pages": [204, 214],
    },
    {
        "nakshatra": "Shatabhisha",
        "marker": "Shatabhisha marker star",
        "positions": (317.75,),
        "book_position": "17°45′ Aquarius",
        "effect_class": "mildly_positive",
        "effect": "Mildly positive.",
        "tier_hint": "First tier",
        "applies_to_cusps": True,
        "applies_to_lords": True,
        "pdf_pages": [204, 214],
    },
    {
        "nakshatra": "Purva Bhadrapada",
        "marker": "Alpha Pegasi",
        "positions": (329.5,),
        "book_position": "29°30′ Aquarius",
        "effect_class": "none",
        "effect": "No appreciable sports-cusp effect.",
        "tier_hint": "None",
        "applies_to_cusps": False,
        "applies_to_lords": False,
        "pdf_pages": [204, 215],
    },
    {
        "nakshatra": "Uttara Bhadrapada",
        "marker": "Algenib",
        "positions": (345.0,),
        "book_position": "15° Pisces",
        "effect_class": "none",
        "effect": "No appreciable sports-cusp effect.",
        "tier_hint": "None",
        "applies_to_cusps": False,
        "applies_to_lords": False,
        "pdf_pages": [204, 215],
    },
    {
        "nakshatra": "Revati",
        "marker": "Revati marker star(s)",
        "positions": (356.0,),
        "book_position": "Around 26° Pisces",
        "effect_class": "none",
        "effect": "No appreciable sports-cusp effect.",
        "tier_hint": "None",
        "applies_to_cusps": False,
        "applies_to_lords": False,
        "pdf_pages": [204, 215],
    },
    {
        "nakshatra": "Additional fixed star",
        "marker": "Algol",
        "positions": (32.0,),
        "book_position": "2° Taurus",
        "effect_class": "context_harm",
        "effect": "Loss-of-head or crisis symbolism; not a proven winner rule.",
        "tier_hint": "Context only",
        "applies_to_cusps": True,
        "applies_to_lords": True,
        "pdf_pages": [204, 216, 217],
    },
    {
        "nakshatra": "Additional constellation",
        "marker": "Hyades",
        "positions": (),
        "range": (41.0, 43.0),
        "book_position": "11°-13° Taurus",
        "effect_class": "mildly_positive",
        "effect": "Mildly positive.",
        "tier_hint": "First tier",
        "applies_to_cusps": True,
        "applies_to_lords": True,
        "pdf_pages": [204, 209],
    },
    {
        "nakshatra": "Additional fixed star",
        "marker": "Wasat",
        "positions": (85.0,),
        "book_position": "25° Gemini",
        "effect_class": "axis_dependent",
        "effect": "Saturn-like on a cusp, with slightly less force.",
        "tier_hint": "Qualitative axis rule",
        "applies_to_cusps": True,
        "applies_to_lords": False,
        "pdf_pages": [204, 210],
    },
)


# ============================================================
# REQUEST MODELS
# ============================================================

class LocationInput(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    longitude: float = Field(ge=-180, le=180)
    latitude: float = Field(ge=-90, le=90)


class ParticipantNameInput(BaseModel):
    name: str = Field(
        min_length=1,
        max_length=200,
        description=(
            "Verified official participant or team name. The raw name is "
            "not automatically treated as a confirmed Sanskrit sound."
        ),
    )
    confirmed_opening_sounds: list[str] = Field(
        default_factory=list,
        max_length=20,
        description=(
            "Caller-reviewed opening sounds for the initial syllables of "
            "the participant's compound name, already expressed in a "
            "book-compatible approximation, for example ['sa', 'di', 'pa']."
        ),
    )


class ParticipantsInput(BaseModel):
    favourite: ParticipantNameInput | None = Field(
        default=None,
        description="Genuine market favourite assigned to House 1.",
    )
    underdog: ParticipantNameInput | None = Field(
        default=None,
        description="Genuine market underdog assigned to House 7.",
    )


class EventChartInput(BaseModel):
    event_id: str | None = None
    std_time: str = Field(
        description="Exact local event time: HH:MM DD/MM/YYYY +HH:MM",
        examples=["20:00 22/07/2026 +10:00"],
    )
    location: LocationInput
    houses: list[str] = Field(
        default_factory=lambda: DEFAULT_HOUSES.copy()
    )
    planets: list[str] = Field(
        default_factory=lambda: DEFAULT_PLANETS.copy()
    )
    participants: ParticipantsInput | None = Field(
        default=None,
        description=(
            "Optional favourite/underdog names and caller-confirmed "
            "opening sounds for Chapter 7 nama-pada comparison."
        ),
    )


# ============================================================
# JSON AND RESPONSE-SIZE HELPERS
# ============================================================

def json_safe(value: Any, depth: int = 0) -> Any:
    if depth > 10:
        return str(value)[:500]

    if value is None:
        return None

    if isinstance(value, (int, float, bool)):
        return value

    if isinstance(value, str):
        return value[:2000]

    if isinstance(value, Enum):
        return value.value

    if isinstance(value, dict):
        return {
            str(key): json_safe(item, depth + 1)
            for key, item in value.items()
        }

    if isinstance(value, (list, tuple, set)):
        return [json_safe(item, depth + 1) for item in list(value)[:50]]

    if hasattr(value, "to_json") and callable(value.to_json):
        try:
            return json_safe(_json_from_to_json(value), depth + 1)
        except Exception:
            pass

    if hasattr(value, "__dict__"):
        return {
            str(key): json_safe(item, depth + 1)
            for key, item in vars(value).items()
            if not str(key).startswith("_")
        }

    return str(value)[:2000]


def limit_data(
    value: Any,
    maximum_characters: int | None = None,
) -> Any:
    limit = maximum_characters or MAX_RESULT_CHARACTERS
    safe_value = json_safe(value)

    encoded = json.dumps(
        safe_value,
        ensure_ascii=False,
        default=str,
    )

    if len(encoded) <= limit:
        return safe_value

    return {
        "response_compacted": True,
        "preview": encoded[:limit],
    }


# ============================================================
# PACING AND RETRIES
# ============================================================

call_lock = threading.Lock()
last_call_time = 0.0


def wait_for_call_slot() -> None:
    global last_call_time

    with call_lock:
        elapsed = time.monotonic() - last_call_time
        remaining = VEDASTRO_MIN_INTERVAL_SECONDS - elapsed

        if remaining > 0:
            time.sleep(remaining)

        last_call_time = time.monotonic()


def is_retryable_error(message: str) -> bool:
    text = message.lower()

    return any(
        marker in text
        for marker in (
            "access denied",
            "rate limit",
            "too many",
            "timeout",
            "timed out",
            "temporarily",
            "connection",
            "gateway",
            "http 408",
            "http 425",
            "http 429",
            "http 500",
            "http 502",
            "http 503",
            "http 504",
        )
    )


def find_method(
    method_names: str | list[str],
) -> tuple[str | None, Callable[..., Any] | None]:
    names = [method_names] if isinstance(method_names, str) else method_names

    for name in names:
        if hasattr(Calculate, name):
            return name, getattr(Calculate, name)

    return None, None


def vedastro_call(
    method_names: str | list[str],
    *args: Any,
    required: bool = False,
) -> dict[str, Any]:
    names = [method_names] if isinstance(method_names, str) else method_names
    selected_name, method = find_method(names)

    if method is None:
        return {
            "status": "Fail",
            "required": required,
            "method": names[0],
            "attempts": 0,
            "error": f"Method unavailable. Tried: {names}",
        }

    final_error = ""
    attempts_made = 0

    for attempt in range(1, VEDASTRO_MAX_RETRIES + 1):
        attempts_made = attempt

        try:
            wait_for_call_slot()
            result = method(*args)

            return {
                "status": "Pass",
                "required": required,
                "method": selected_name,
                "attempt": attempt,
                "data": limit_data(result),
            }

        except Exception as error:
            final_error = str(error)

            if attempt >= VEDASTRO_MAX_RETRIES:
                break

            if not is_retryable_error(final_error):
                break

            # Small exponential pause between attempts of the same failed method.
            time.sleep(min(2 ** (attempt - 1), 4))

    return {
        "status": "Fail",
        "required": required,
        "method": selected_name,
        "attempts": attempts_made,
        "error": final_error or "Unknown VedAstro error",
    }


def vedastro_call_for_ayanamsa(
    ayanamsa_name: str,
    method_names: str | list[str],
    *args: Any,
    required: bool = False,
) -> dict[str, Any]:
    """Run one generated-client call with an isolated payload ayanamsa."""

    with use_request_ayanamsa(ayanamsa_name):
        result = vedastro_call(
            method_names,
            *args,
            required=required,
        )

    result["ayanamsa_requested"] = str(ayanamsa_name)
    return result


def direct_vedastro_time_endpoint_call(
    endpoint_names: tuple[str, ...] | list[str],
    event_time: Time,
    ayanamsa_name: str = "LAHIRI",
) -> dict[str, Any]:
    """
    Call a time-based official VedAstro server calculator that is absent from
    the generated Python client.

    Most current calculators use the parameter name ``time``. The inputTime
    and birthTime variants are attempted only when the installed server
    signature differs. Every attempt uses the existing shared throttle,
    authentication headers, retry policy and thread-local ayanamsa.
    """

    parameter_names = ("time", "inputTime", "birthTime")
    failures: list[dict[str, Any]] = []
    attempts = 0

    for endpoint in endpoint_names:
        for parameter_name in parameter_names:
            final_error = ""

            for attempt in range(1, VEDASTRO_MAX_RETRIES + 1):
                attempts += 1

                try:
                    wait_for_call_slot()

                    with use_request_ayanamsa(ayanamsa_name):
                        result = Calculate._make_request(
                            endpoint,
                            {
                                parameter_name: event_time.to_json(),
                            },
                        )

                    return {
                        "status": "Pass",
                        "method": endpoint,
                        "parameter_name": parameter_name,
                        "attempt": attempt,
                        "total_attempts": attempts,
                        "ayanamsa_requested": ayanamsa_name,
                        "data": limit_data(result),
                    }

                except Exception as error:
                    final_error = str(error)

                    if attempt >= VEDASTRO_MAX_RETRIES:
                        break

                    if not is_retryable_error(final_error):
                        break

                    time.sleep(min(2 ** (attempt - 1), 4))

            failures.append({
                "method": endpoint,
                "parameter_name": parameter_name,
                "error": final_error or "Unknown VedAstro error",
            })

    return {
        "status": "Fail",
        "method": endpoint_names[0],
        "attempts": attempts,
        "ayanamsa_requested": ayanamsa_name,
        "failures": failures,
        "error": (
            "All official VedAstro endpoint and time-parameter "
            "combinations failed."
        ),
    }


# ============================================================
# RESULT PARSING
# ============================================================

def unwrap_data(result: dict[str, Any]) -> Any:
    return result.get("data")


def find_named_value(
    value: Any,
    preferred_keys: tuple[str, ...],
) -> str | None:
    if isinstance(value, str):
        return value

    if isinstance(value, dict):
        for key in preferred_keys:
            for actual_key, child in value.items():
                if actual_key.lower() == key.lower():
                    if isinstance(child, (str, int, float)):
                        return str(child)

        for child in value.values():
            found = find_named_value(child, preferred_keys)
            if found:
                return found

    if isinstance(value, list):
        for child in value:
            found = find_named_value(child, preferred_keys)
            if found:
                return found

    return None


def find_numeric_value(
    value: Any,
    preferred_keys: tuple[str, ...],
) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)

    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None

    if isinstance(value, dict):
        for key in preferred_keys:
            for actual_key, child in value.items():
                if actual_key.lower() == key.lower():
                    found = find_numeric_value(child, preferred_keys)
                    if found is not None:
                        return found

        for child in value.values():
            found = find_numeric_value(child, preferred_keys)
            if found is not None:
                return found

    if isinstance(value, list):
        for child in value:
            found = find_numeric_value(child, preferred_keys)
            if found is not None:
                return found

    return None


def extract_sign_name(result: dict[str, Any]) -> str | None:
    data = unwrap_data(result)
    value = find_named_value(data, ("Name", "SignName", "ZodiacName"))

    if not value:
        return None

    for sign in (
        "Aries",
        "Taurus",
        "Gemini",
        "Cancer",
        "Leo",
        "Virgo",
        "Libra",
        "Scorpio",
        "Sagittarius",
        "Capricorn",
        "Aquarius",
        "Pisces",
    ):
        if sign.lower() in value.lower():
            return sign

    return None


def extract_total_degrees(result: dict[str, Any]) -> float | None:
    return find_numeric_value(
        unwrap_data(result),
        ("TotalDegrees", "totalDegrees"),
    )


def extract_nakshatra_name(result: dict[str, Any]) -> str | None:
    data = unwrap_data(result)

    if isinstance(data, str):
        raw = data
    else:
        raw = find_named_value(
            data,
            ("Name", "ConstellationName", "NakshatraName"),
        )

    if not raw:
        return None

    normalised = (
        raw.lower()
        .replace("-", " ")
        .replace("_", " ")
        .strip()
    )

    aliases = {
        "ashwini": "Ashwini",
        "aswini": "Ashwini",
        "bharani": "Bharani",
        "krittika": "Krittika",
        "kritika": "Krittika",
        "rohini": "Rohini",
        "mrigashirsha": "Mrigashirsha",
        "mrigasira": "Mrigashirsha",
        "ardra": "Ardra",
        "punarvasu": "Punarvasu",
        "pushya": "Pushya",
        "ashlesha": "Ashlesha",
        "aslesha": "Ashlesha",
        "magha": "Magha",
        "purva phalguni": "Purva Phalguni",
        "uttara phalguni": "Uttara Phalguni",
        "hasta": "Hasta",
        "chitra": "Chitra",
        "swati": "Swati",
        "swathi": "Swati",
        "vishakha": "Vishakha",
        "vishhaka": "Vishakha",
        "vishaka": "Vishakha",
        "visakha": "Vishakha",
        "anuradha": "Anuradha",
        "jyeshtha": "Jyeshtha",
        "jyeshta": "Jyeshtha",
        "jyestha": "Jyeshtha",
        "jyesta": "Jyeshtha",
        "mula": "Mula",
        "moola": "Mula",
        "purva ashadha": "Purva Ashadha",
        "purva ashada": "Purva Ashadha",
        "uttara ashadha": "Uttara Ashadha",
        "uttara ashada": "Uttara Ashadha",
        "shravana": "Shravana",
        "sravana": "Shravana",
        "dhanishta": "Dhanishta",
        "shatabhisha": "Shatabhisha",
        "satabhisha": "Shatabhisha",
        "purva bhadrapada": "Purva Bhadrapada",
        "uttara bhadrapada": "Uttara Bhadrapada",
        "revati": "Revati",
    }

    for alias, canonical in aliases.items():
        if alias in normalised:
            return canonical

    return None



# ============================================================
# EXACT PLACIDUS CUSP HELPERS
# ============================================================

def normalise_degrees(value: float) -> float:
    """Normalise a longitude into the 0 <= value < 360 range."""

    return float(value) % 360.0


def sign_details_from_longitude(longitude: float) -> dict[str, Any]:
    """Convert a 0-360 sidereal longitude into sign and degree-in-sign."""

    normalised = normalise_degrees(longitude)
    sign_index = int(normalised // 30.0)

    return {
        "sign": ZODIAC_SIGNS[sign_index],
        "degree_in_sign": round(normalised % 30.0, 8),
    }


def angular_distance(first: float, second: float) -> float:
    """Return the shortest circular distance between two longitudes."""

    difference = abs(normalise_degrees(first) - normalise_degrees(second))
    return min(difference, 360.0 - difference)


def _numeric_from_item(value: Any) -> float | None:
    """Read one numeric value from a primitive or common Angle JSON shape."""

    if isinstance(value, bool):
        return None

    if isinstance(value, (int, float)):
        return float(value)

    if isinstance(value, str):
        try:
            return float(value.strip())
        except ValueError:
            return None

    if isinstance(value, dict):
        for key in (
            "TotalDegrees",
            "totalDegrees",
            "Degrees",
            "degrees",
            "Value",
            "value",
        ):
            if key in value:
                parsed = _numeric_from_item(value[key])
                if parsed is not None:
                    return parsed

    return None


def _find_cusp_sequence(value: Any) -> list[float] | None:
    """
    Find the VedAstro cusp array.

    VedAstro may serialise GetAllHouseNirayanaMiddleLongitudes as:
    - a 12- or 13-item list,
    - a nested object containing that list, or
    - one comma-separated string such as "0, 305.03, ...".

    Swiss Ephemeris uses 13 slots: index 0 is unused and indexes 1-12 are
    House1-House12. Some serializers remove the unused first slot.
    """

    if isinstance(value, str):
        cleaned = value.strip().strip("[]()")
        if "," in cleaned:
            parts = [part.strip() for part in cleaned.split(",")]
            try:
                numbers = [float(part) for part in parts if part != ""]
            except ValueError:
                numbers = []

            if len(numbers) in {12, 13}:
                return numbers

    if isinstance(value, (list, tuple)):
        parsed = [_numeric_from_item(item) for item in value]

        if all(item is not None for item in parsed):
            numbers = [float(item) for item in parsed if item is not None]

            if len(numbers) in {12, 13}:
                return numbers

        for child in value:
            found = _find_cusp_sequence(child)
            if found is not None:
                return found

    if isinstance(value, dict):
        # Prefer likely payload keys before scanning every nested value.
        for preferred_key in (
            "cusps",
            "Cusps",
            "house_cusps",
            "HouseCusps",
            "Payload",
            "payload",
            "data",
        ):
            if preferred_key in value:
                found = _find_cusp_sequence(value[preferred_key])
                if found is not None:
                    return found

        for child in value.values():
            found = _find_cusp_sequence(child)
            if found is not None:
                return found

    return None


def parse_placidus_cusps(result: dict[str, Any]) -> dict[str, Any]:
    """Turn VedAstro's raw cusp array into a typed House1-House12 mapping."""

    if result.get("status") != "Pass":
        return {
            "status": "Unavailable",
            "method": result.get(
                "method",
                "GetAllHouseNirayanaMiddleLongitudes",
            ),
            "error": result.get("error", "VedAstro cusp calculation failed."),
            "cusps": {},
        }

    sequence = _find_cusp_sequence(unwrap_data(result))

    if sequence is None:
        return {
            "status": "Fail",
            "method": result.get(
                "method",
                "GetAllHouseNirayanaMiddleLongitudes",
            ),
            "error": "Could not parse a 12- or 13-value house cusp array.",
            "raw_data": limit_data(unwrap_data(result), 1200),
            "cusps": {},
        }

    if len(sequence) == 13:
        # Swiss Ephemeris convention: slot zero is unused.
        house_values = sequence[1:13]
        source_array_shape = "13 slots; index 0 discarded"
    else:
        house_values = sequence
        source_array_shape = "12 direct house values"

    if len(house_values) != 12:
        return {
            "status": "Fail",
            "method": result.get(
                "method",
                "GetAllHouseNirayanaMiddleLongitudes",
            ),
            "error": f"Expected 12 house values, received {len(house_values)}.",
            "cusps": {},
        }

    cusps: dict[str, dict[str, Any]] = {}

    for index, raw_longitude in enumerate(house_values, start=1):
        longitude = normalise_degrees(raw_longitude)
        sign_details = sign_details_from_longitude(longitude)

        cusps[f"House{index}"] = {
            "house": f"House{index}",
            "sidereal_longitude": round(longitude, 8),
            **sign_details,
        }

    # Opposite Placidus cusps must be approximately 180 degrees apart.
    axis_checks = []

    for first_house, opposite_house in (
        ("House1", "House7"),
        ("House4", "House10"),
        ("House6", "House12"),
    ):
        separation = angular_distance(
            cusps[first_house]["sidereal_longitude"],
            cusps[opposite_house]["sidereal_longitude"],
        )
        passed = abs(separation - 180.0) <= 0.02

        axis_checks.append({
            "axis": f"{first_house}/{opposite_house}",
            "separation_degrees": round(separation, 8),
            "status": "Pass" if passed else "Fail",
        })

    failed_axes = [
        check for check in axis_checks if check["status"] != "Pass"
    ]

    return {
        "status": "Pass" if not failed_axes else "Fail",
        "method": result.get(
            "method",
            "GetAllHouseNirayanaMiddleLongitudes",
        ),
        "ayanamsa": "Lahiri",
        "house_system": "Placidus",
        "source_array_shape": source_array_shape,
        "cusps": cusps,
        "axis_validation": axis_checks,
        "error": (
            None
            if not failed_axes
            else "One or more opposite cusp axes were not 180 degrees apart."
        ),
    }


def calculate_rashi_placidus(event_time: Time) -> dict[str, Any]:
    """
    Calculate exact Lahiri sidereal Placidus middle/cusp longitudes.

    VedAstro's GetAllHouseNirayanaMiddleLongitudes uses Swiss Ephemeris with
    house-system code 'P' and returns a 13-slot array whose first slot is unused.
    """

    raw_result = vedastro_call(
        "GetAllHouseNirayanaMiddleLongitudes",
        event_time,
    )

    parsed = parse_placidus_cusps(raw_result)
    parsed["raw_calculation"] = {
        key: value
        for key, value in raw_result.items()
        if key != "data"
    }
    return parsed


def cusp_orb_policy(planet_name: str) -> tuple[str, float]:
    """Return the book visibility class and maximum D1 cusp orb."""

    if planet_name in INVISIBLE_CUSP_BODIES:
        return "invisible", INVISIBLE_CUSP_ORB_DEGREES

    return "visible", VISIBLE_CUSP_ORB_DEGREES


def calculate_planet_cusp_contacts(
    planets: dict[str, dict[str, Any]],
    rashi_placidus: dict[str, Any],
) -> dict[str, Any]:
    """
    Calculate exact distances from every requested planet to the six primary
    Placidus contest cusps.

    This layer reports objective geometry only. It deliberately does not assign
    winner effects or Gambler's Dharma points; those depend on the planet, axis,
    motion, dignity and the book's cumulative interpretation.
    """

    if rashi_placidus.get("status") != "Pass":
        return {
            "status": "Unavailable",
            "method": "InternalPlanetToPlacidusCuspDistances",
            "ayanamsa": "Lahiri",
            "house_system": "Placidus",
            "error": "Exact Placidus cusps did not pass validation.",
            "distances_by_planet": {},
            "qualifying_contacts": [],
            "closest_planet_by_cusp": {},
        }

    cusp_source = rashi_placidus.get("cusps", {})
    sensitive_cusps: dict[str, dict[str, Any]] = {}

    for cusp_name, metadata in SENSITIVE_CUSP_DETAILS.items():
        cusp = cusp_source.get(cusp_name, {})
        cusp_longitude = cusp.get("sidereal_longitude")

        if not isinstance(cusp_longitude, (int, float)):
            return {
                "status": "Fail",
                "method": "InternalPlanetToPlacidusCuspDistances",
                "ayanamsa": "Lahiri",
                "house_system": "Placidus",
                "error": f"{cusp_name} exact longitude is missing.",
                "distances_by_planet": {},
                "qualifying_contacts": [],
                "closest_planet_by_cusp": {},
            }

        sensitive_cusps[cusp_name] = {
            "cusp": cusp_name,
            "axis": metadata["axis"],
            "side": metadata["side"],
            "sidereal_longitude": round(
                normalise_degrees(float(cusp_longitude)),
                8,
            ),
            "sign": cusp.get("sign"),
            "degree_in_sign": cusp.get("degree_in_sign"),
        }

    distances_by_planet: dict[str, dict[str, Any]] = {}
    unavailable_planets: list[dict[str, str]] = []
    all_contacts: list[dict[str, Any]] = []

    for planet_name, planet_result in planets.items():
        longitude_result = planet_result.get("sidereal_longitude", {})
        planet_longitude = extract_total_degrees(longitude_result)

        if planet_longitude is None:
            unavailable_planets.append({
                "planet": planet_name,
                "reason": "Could not parse exact sidereal longitude.",
            })
            continue

        planet_longitude = normalise_degrees(planet_longitude)
        visibility_class, orb_limit = cusp_orb_policy(planet_name)
        planet_distances: dict[str, float] = {}

        for cusp_name, cusp in sensitive_cusps.items():
            distance = angular_distance(
                planet_longitude,
                cusp["sidereal_longitude"],
            )
            planet_distances[cusp_name] = round(distance, 8)

            all_contacts.append({
                "planet": planet_name,
                "cusp": cusp_name,
                "axis": cusp["axis"],
                "side": cusp["side"],
                "planet_longitude": round(planet_longitude, 8),
                "cusp_longitude": cusp["sidereal_longitude"],
                "angular_distance": round(distance, 8),
                "visibility_class": visibility_class,
                "orb_limit": orb_limit,
                "within_orb": distance <= orb_limit + 1e-9,
                "orb_margin": round(orb_limit - distance, 8),
            })

        nearest_cusp = min(
            planet_distances,
            key=planet_distances.get,
        )

        distances_by_planet[planet_name] = {
            "planet": planet_name,
            "sidereal_longitude": round(planet_longitude, 8),
            **sign_details_from_longitude(planet_longitude),
            "visibility_class": visibility_class,
            "orb_limit": orb_limit,
            "distances": planet_distances,
            "nearest_sensitive_cusp": nearest_cusp,
            "nearest_distance": planet_distances[nearest_cusp],
            "nearest_within_orb": (
                planet_distances[nearest_cusp] <= orb_limit + 1e-9
            ),
        }

    closest_planet_by_cusp: dict[str, dict[str, Any]] = {}
    qualifying_contacts: list[dict[str, Any]] = []

    for cusp_name in sensitive_cusps:
        cusp_contacts = sorted(
            (
                contact
                for contact in all_contacts
                if contact["cusp"] == cusp_name
            ),
            key=lambda contact: (
                contact["angular_distance"],
                contact["planet"],
            ),
        )

        if not cusp_contacts:
            continue

        closest = dict(cusp_contacts[0])
        closest["qualifies"] = closest["within_orb"]
        closest_planet_by_cusp[cusp_name] = closest

        qualifying_for_cusp = [
            contact
            for contact in cusp_contacts
            if contact["within_orb"]
        ]

        for rank, contact in enumerate(qualifying_for_cusp, start=1):
            ranked_contact = dict(contact)
            ranked_contact["rank_on_cusp"] = rank
            ranked_contact["closest_qualifying_contact"] = rank == 1
            qualifying_contacts.append(ranked_contact)

    if not distances_by_planet:
        status = "Unavailable"
        error = "No requested planet longitude could be parsed."
    elif unavailable_planets:
        status = "Partial"
        error = "One or more requested planet longitudes were unavailable."
    else:
        status = "Pass"
        error = None

    cusp_order = list(SENSITIVE_CUSP_DETAILS)

    return {
        "status": status,
        "method": "InternalPlanetToPlacidusCuspDistances",
        "ayanamsa": "Lahiri",
        "house_system": "Placidus",
        "book_layer": "Tier 2 raw cusp geometry",
        "interpretation_applied": False,
        "orb_policy": {
            "visible_planets_degrees": VISIBLE_CUSP_ORB_DEGREES,
            "invisible_bodies_degrees": INVISIBLE_CUSP_ORB_DEGREES,
            "currently_supported_invisible_bodies": sorted(
                INVISIBLE_CUSP_BODIES
            ),
            "outer_planets_and_special_points": (
                "Not yet supported by the current request schema."
            ),
        },
        "sensitive_cusps": sensitive_cusps,
        "distances_by_planet": distances_by_planet,
        "qualifying_contacts": sorted(
            qualifying_contacts,
            key=lambda contact: (
                cusp_order.index(contact["cusp"]),
                contact["rank_on_cusp"],
            ),
        ),
        "closest_planet_by_cusp": closest_planet_by_cusp,
        "unavailable_planets": unavailable_planets,
        "error": error,
    }


def sign_name_from_value(value: Any) -> str | None:
    """Extract one zodiac sign name from an arbitrary serialised value."""

    if isinstance(value, str):
        lowered = value.lower()

        for sign in ZODIAC_SIGNS:
            if sign.lower() in lowered:
                return sign

        return None

    if isinstance(value, dict):
        for key in ("Name", "name", "SignName", "signName", "ZodiacName"):
            if key in value:
                found = sign_name_from_value(value[key])
                if found:
                    return found

        for child in value.values():
            found = sign_name_from_value(child)
            if found:
                return found

    if isinstance(value, (list, tuple)):
        for child in value:
            found = sign_name_from_value(child)
            if found:
                return found

    return None


def _find_house_sign_map(value: Any) -> dict[str, str] | None:
    """Find a House1-House12 sign mapping in VedAstro serialised output."""

    if isinstance(value, dict):
        normalised_keys = {
            str(key).lower(): key
            for key in value
        }

        if "house1" in normalised_keys:
            result: dict[str, str] = {}

            for number in range(1, 13):
                house_name = f"House{number}"
                actual_key = normalised_keys.get(house_name.lower())

                if actual_key is None:
                    continue

                sign = sign_name_from_value(value[actual_key])

                if sign:
                    result[house_name] = sign

            if result:
                return result

        for child in value.values():
            found = _find_house_sign_map(child)
            if found:
                return found

    if isinstance(value, (list, tuple)):
        for child in value:
            found = _find_house_sign_map(child)
            if found:
                return found

    return None


def parse_vedastro_navamsha_house_signs(
    result: dict[str, Any],
) -> dict[str, Any]:
    """Parse VedAstro AllHouseNavamshaSigns into a typed sign map."""

    if result.get("status") != "Pass":
        return {
            "status": "Unavailable",
            "method": result.get("method", "AllHouseNavamshaSigns"),
            "house_signs": {},
            "error": result.get(
                "error",
                "VedAstro Navamsha house-sign calculation failed.",
            ),
        }

    house_signs = _find_house_sign_map(unwrap_data(result))

    if not house_signs:
        return {
            "status": "Fail",
            "method": result.get("method", "AllHouseNavamshaSigns"),
            "house_signs": {},
            "raw_data": limit_data(unwrap_data(result), 1200),
            "error": "Could not parse VedAstro Navamsha house signs.",
        }

    missing = [
        house
        for house in ("House1", "House7")
        if house not in house_signs
    ]

    return {
        "status": "Pass" if not missing else "Partial",
        "method": result.get("method", "AllHouseNavamshaSigns"),
        "house_signs": house_signs,
        "missing_required_houses": missing,
        "error": (
            None
            if not missing
            else "VedAstro D9 House1 or House7 sign is missing."
        ),
    }


def exact_navamsha_position(
    d1_sidereal_longitude: float,
) -> dict[str, Any]:
    """
    Convert one exact Lahiri D1 longitude to its exact D9 longitude.

    This implements the Chapter 5 geometry without using the rounded 6.67
    shortcut: the offset inside a 3°20' Navamsha section is multiplied by
    exactly nine, expanding that section to a full 30-degree D9 sign.
    """

    d1_longitude = normalise_degrees(float(d1_sidereal_longitude))
    d1_sign_index = int(d1_longitude // 30.0)
    d1_degree_in_sign = d1_longitude - (d1_sign_index * 30.0)

    # Multiplying the D1 degree-in-sign by nine produces nine 30-degree
    # bands. The band number identifies the Navamsha section and the
    # remainder is the exact degree inside the D9 sign.
    expanded = d1_degree_in_sign * NAVAMSHA_DIVISIONS_PER_SIGN
    navamsha_index = int(expanded // 30.0)

    # Guard against a rare floating-point value such as 8.999999999999/9.
    navamsha_index = min(max(navamsha_index, 0), 8)

    d9_degree_in_sign = expanded - (navamsha_index * 30.0)

    if d9_degree_in_sign < 0 and abs(d9_degree_in_sign) < 1e-8:
        d9_degree_in_sign = 0.0

    if d9_degree_in_sign >= 30.0 - 1e-8:
        d9_degree_in_sign = 0.0
        navamsha_index = min(navamsha_index + 1, 8)

    start_sign_index = NAVAMSHA_START_SIGN_INDEX[d1_sign_index]
    d9_sign_index = (
        start_sign_index + navamsha_index
    ) % len(ZODIAC_SIGNS)
    d9_longitude = normalise_degrees(
        (d9_sign_index * 30.0) + d9_degree_in_sign
    )

    section_start_degree = (
        navamsha_index * NAVAMSHA_SECTION_DEGREES
    )
    section_end_degree = (
        section_start_degree + NAVAMSHA_SECTION_DEGREES
    )

    return {
        "d1_sidereal_longitude": round(d1_longitude, 8),
        "d1_sign": ZODIAC_SIGNS[d1_sign_index],
        "d1_degree_in_sign": round(d1_degree_in_sign, 8),
        "navamsha_number_in_d1_sign": navamsha_index + 1,
        "d1_section_start_degree": round(section_start_degree, 8),
        "d1_section_end_degree": round(section_end_degree, 8),
        "d9_sidereal_longitude": round(d9_longitude, 8),
        "d9_sign": ZODIAC_SIGNS[d9_sign_index],
        "d9_degree_in_sign": round(d9_degree_in_sign, 8),
    }


def calculate_exact_navamsha_cusps(
    event_time: Time,
    planets: dict[str, dict[str, Any]],
    rashi_placidus: dict[str, Any],
) -> dict[str, Any]:
    """
    Calculate exact D9 Lagna/7th cusp and requested-planet D9 degrees.

    Exact degrees are derived from the verified Lahiri D1 longitudes using
    the book's Chapter 5 transformation. Derived D9 signs are independently
    cross-checked against VedAstro's own D9 planet and house-sign methods.
    """

    if rashi_placidus.get("status") != "Pass":
        return {
            "status": "Unavailable",
            "method": "ExactChapter5NavamshaGeometry",
            "ayanamsa": "Lahiri",
            "error": "Exact D1 Placidus cusps did not pass validation.",
            "lagna": None,
            "seventh_cusp": None,
            "planets": {},
            "qualifying_contacts": [],
        }

    d1_cusps = rashi_placidus.get("cusps", {})
    d1_lagna_longitude = (
        d1_cusps.get("House1", {}).get("sidereal_longitude")
    )
    d1_seventh_longitude = (
        d1_cusps.get("House7", {}).get("sidereal_longitude")
    )

    if not isinstance(d1_lagna_longitude, (int, float)):
        return {
            "status": "Fail",
            "method": "ExactChapter5NavamshaGeometry",
            "ayanamsa": "Lahiri",
            "error": "D1 House1 exact cusp longitude is missing.",
            "lagna": None,
            "seventh_cusp": None,
            "planets": {},
            "qualifying_contacts": [],
        }

    if not isinstance(d1_seventh_longitude, (int, float)):
        return {
            "status": "Fail",
            "method": "ExactChapter5NavamshaGeometry",
            "ayanamsa": "Lahiri",
            "error": "D1 House7 exact cusp longitude is missing.",
            "lagna": None,
            "seventh_cusp": None,
            "planets": {},
            "qualifying_contacts": [],
        }

    lagna = exact_navamsha_position(float(d1_lagna_longitude))
    seventh = exact_navamsha_position(float(d1_seventh_longitude))

    lagna = {
        "cusp": "D9Lagna",
        "side": "Favourite",
        "source_d1_cusp": "House1",
        **lagna,
    }
    seventh = {
        "cusp": "D9House7",
        "side": "Underdog",
        "source_d1_cusp": "House7",
        **seventh,
    }

    d9_axis_separation = angular_distance(
        lagna["d9_sidereal_longitude"],
        seventh["d9_sidereal_longitude"],
    )
    axis_status = (
        "Pass"
        if abs(d9_axis_separation - 180.0) <= 0.0001
        else "Fail"
    )

    raw_house_sign_result = vedastro_call(
        ["AllHouseNavamshaSigns", "AllHouseNavamshaD9Signs"],
        event_time,
    )
    vedastro_house_signs = parse_vedastro_navamsha_house_signs(
        raw_house_sign_result
    )

    house_sign_validation: list[dict[str, Any]] = []

    for cusp, house_name in (
        (lagna, "House1"),
        (seventh, "House7"),
    ):
        returned_sign = vedastro_house_signs.get(
            "house_signs",
            {},
        ).get(house_name)
        derived_sign = cusp["d9_sign"]

        if returned_sign:
            passed = returned_sign == derived_sign
            validation_status = "Pass" if passed else "Fail"
        else:
            passed = None
            validation_status = "Unavailable"

        house_sign_validation.append({
            "cusp": cusp["cusp"],
            "vedastro_house": house_name,
            "derived_d9_sign": derived_sign,
            "vedastro_d9_sign": returned_sign,
            "matches": passed,
            "status": validation_status,
        })

    planet_positions: dict[str, dict[str, Any]] = {}
    planet_sign_validation: list[dict[str, Any]] = []
    unavailable_planets: list[dict[str, str]] = []
    all_contacts: list[dict[str, Any]] = []

    for planet_name, planet_result in planets.items():
        d1_longitude = extract_total_degrees(
            planet_result.get("sidereal_longitude", {})
        )

        if d1_longitude is None:
            unavailable_planets.append({
                "planet": planet_name,
                "reason": "Could not parse exact Lahiri D1 longitude.",
            })
            continue

        position = exact_navamsha_position(d1_longitude)
        returned_d9_sign = extract_sign_name(
            planet_result.get("d9_sign", {})
        )
        derived_d9_sign = position["d9_sign"]

        if returned_d9_sign:
            sign_matches = returned_d9_sign == derived_d9_sign
            sign_status = "Pass" if sign_matches else "Fail"
        else:
            sign_matches = None
            sign_status = "Unavailable"

        distances = {
            "D9Lagna": round(
                angular_distance(
                    position["d9_sidereal_longitude"],
                    lagna["d9_sidereal_longitude"],
                ),
                8,
            ),
            "D9House7": round(
                angular_distance(
                    position["d9_sidereal_longitude"],
                    seventh["d9_sidereal_longitude"],
                ),
                8,
            ),
        }

        nearest_cusp = min(distances, key=distances.get)
        nearest_distance = distances[nearest_cusp]

        planet_positions[planet_name] = {
            "planet": planet_name,
            **position,
            "vedastro_d9_sign": returned_d9_sign,
            "vedastro_sign_match": sign_matches,
            "distances": distances,
            "nearest_d9_cusp": nearest_cusp,
            "nearest_distance": nearest_distance,
            "orb_limit": NAVAMSHA_CUSP_ORB_DEGREES,
            "nearest_within_orb": (
                nearest_distance <= NAVAMSHA_CUSP_ORB_DEGREES + 1e-9
            ),
        }

        planet_sign_validation.append({
            "planet": planet_name,
            "derived_d9_sign": derived_d9_sign,
            "vedastro_d9_sign": returned_d9_sign,
            "matches": sign_matches,
            "status": sign_status,
        })

        for cusp_name, cusp_data in (
            ("D9Lagna", lagna),
            ("D9House7", seventh),
        ):
            distance = distances[cusp_name]

            all_contacts.append({
                "planet": planet_name,
                "cusp": cusp_name,
                "side": cusp_data["side"],
                "planet_d9_longitude": position[
                    "d9_sidereal_longitude"
                ],
                "cusp_d9_longitude": cusp_data[
                    "d9_sidereal_longitude"
                ],
                "angular_distance": distance,
                "orb_limit": NAVAMSHA_CUSP_ORB_DEGREES,
                "within_orb": (
                    distance <= NAVAMSHA_CUSP_ORB_DEGREES + 1e-9
                ),
                "orb_margin": round(
                    NAVAMSHA_CUSP_ORB_DEGREES - distance,
                    8,
                ),
            })

    qualifying_contacts: list[dict[str, Any]] = []
    closest_planet_by_cusp: dict[str, dict[str, Any]] = {}

    for cusp_name in ("D9Lagna", "D9House7"):
        contacts = sorted(
            (
                contact
                for contact in all_contacts
                if contact["cusp"] == cusp_name
            ),
            key=lambda contact: (
                contact["angular_distance"],
                contact["planet"],
            ),
        )

        if not contacts:
            continue

        closest = dict(contacts[0])
        closest["qualifies"] = closest["within_orb"]
        closest_planet_by_cusp[cusp_name] = closest

        qualifying = [
            contact
            for contact in contacts
            if contact["within_orb"]
        ]

        for rank, contact in enumerate(qualifying, start=1):
            ranked = dict(contact)
            ranked["rank_on_cusp"] = rank
            ranked["closest_qualifying_contact"] = rank == 1
            qualifying_contacts.append(ranked)

    failed_house_validations = [
        validation
        for validation in house_sign_validation
        if validation["status"] == "Fail"
    ]
    failed_planet_validations = [
        validation
        for validation in planet_sign_validation
        if validation["status"] == "Fail"
    ]
    unavailable_house_validations = [
        validation
        for validation in house_sign_validation
        if validation["status"] == "Unavailable"
    ]
    unavailable_planet_validations = [
        validation
        for validation in planet_sign_validation
        if validation["status"] == "Unavailable"
    ]

    failed_validations = (
        failed_house_validations + failed_planet_validations
    )

    # The exact D9 cusps are derived from the already verified D1 Placidus
    # cusps. VedAstro's whole-house D9 sign method is only an optional
    # secondary cross-check because some Python-client versions do not expose
    # AllHouseNavamshaSigns. Its absence must not downgrade otherwise verified
    # exact D9 geometry.
    optional_house_sign_cross_check = {
        "status": vedastro_house_signs.get("status", "Unavailable"),
        "required_for_layer_pass": False,
        "available": vedastro_house_signs.get("status") in {
            "Pass",
            "Partial",
        },
        "unavailable_validations": unavailable_house_validations,
        "note": (
            "Optional VedAstro whole-house D9 sign cross-check. "
            "Unavailable does not downgrade exact D9 geometry."
        ),
    }

    if axis_status == "Fail" or failed_validations:
        status = "Fail"
        error = (
            "D9 axis or an available VedAstro D9 sign "
            "cross-validation failed."
        )
    elif unavailable_planets or unavailable_planet_validations:
        status = "Partial"
        error = (
            "Exact D9 cusp geometry passed, but one or more requested "
            "planet D9 validations were unavailable."
        )
    else:
        status = "Pass"
        error = None

    return {
        "status": status,
        "method": "ExactChapter5NavamshaGeometry",
        "ayanamsa": "Lahiri",
        "book_layer": "Tier 3 raw Navamsha cusp geometry",
        "interpretation_applied": False,
        "degree_method": (
            "Exact 3°20' section expansion by factor 9; "
            "no rounded 6.67 shortcut."
        ),
        "source": (
            "Exact Lahiri D1 Placidus cusp and planet longitudes, "
            "cross-checked against VedAstro D9 signs."
        ),
        "orb_policy": {
            "d9_lagna_and_seventh_cusp_degrees": (
                NAVAMSHA_CUSP_ORB_DEGREES
            ),
            "applies_to_current_requested_bodies": True,
            "note": (
                "Chapter 5 applies a 2°30' D9 axis orb. This is "
                "separate from the tighter D1 invisible-graha orb."
            ),
        },
        "lagna": lagna,
        "seventh_cusp": seventh,
        "axis_validation": {
            "separation_degrees": round(d9_axis_separation, 8),
            "status": axis_status,
        },
        "vedastro_house_signs": vedastro_house_signs,
        "optional_house_sign_cross_check": (
            optional_house_sign_cross_check
        ),
        "house_sign_validation": house_sign_validation,
        "planets": planet_positions,
        "planet_sign_validation": planet_sign_validation,
        "qualifying_contacts": sorted(
            qualifying_contacts,
            key=lambda contact: (
                0 if contact["cusp"] == "D9Lagna" else 1,
                contact["rank_on_cusp"],
            ),
        ),
        "closest_planet_by_cusp": closest_planet_by_cusp,
        "unavailable_planets": unavailable_planets,
        "failed_validations": failed_validations,
        "error": error,
    }


def kp_nakshatra_and_sublord(
    sidereal_longitude: float,
) -> dict[str, Any]:
    """
    Calculate the KP nakshatra lord and sublord from one exact
    Krishnamurti sidereal longitude.

    Each 13°20' nakshatra is divided in Vimshottari proportions. The
    sublord sequence begins with the nakshatra lord and then follows the
    standard nine-planet Vimshottari order.
    """

    longitude = normalise_degrees(float(sidereal_longitude))
    nakshatra_size = 360.0 / 27.0
    index = int(longitude // nakshatra_size)
    index = min(max(index, 0), 26)

    nakshatra_start = index * nakshatra_size
    offset = longitude - nakshatra_start
    nakshatra_lord = NAKSHATRA_LORDS[index]

    lord_start_index = VIMSHOTTARI_SEQUENCE.index(nakshatra_lord)
    ordered_sublords = (
        VIMSHOTTARI_SEQUENCE[lord_start_index:]
        + VIMSHOTTARI_SEQUENCE[:lord_start_index]
    )

    cumulative = 0.0
    selected_sublord = ordered_sublords[-1]
    sub_start = 0.0
    sub_end = nakshatra_size

    for sublord in ordered_sublords:
        span = (
            nakshatra_size
            * VIMSHOTTARI_YEARS[sublord]
            / 120.0
        )
        next_cumulative = cumulative + span

        if (
            offset < next_cumulative - 1e-10
            or sublord == ordered_sublords[-1]
        ):
            selected_sublord = sublord
            sub_start = cumulative
            sub_end = next_cumulative
            break

        cumulative = next_cumulative

    return {
        "sidereal_longitude": round(longitude, 8),
        **sign_details_from_longitude(longitude),
        "nakshatra_index": index + 1,
        "nakshatra": NAKSHATRA_NAMES[index],
        "nakshatra_lord": nakshatra_lord,
        "offset_in_nakshatra": round(offset, 8),
        "sublord": selected_sublord,
        "sublord_segment_start_longitude": round(
            normalise_degrees(nakshatra_start + sub_start),
            8,
        ),
        "sublord_segment_end_longitude": round(
            normalise_degrees(nakshatra_start + sub_end),
            8,
        ),
    }


def house_number_for_longitude(
    sidereal_longitude: float,
    cusps: dict[str, dict[str, Any]],
) -> int | None:
    """Place a planet in one exact cyclic Placidus house interval."""

    longitude = normalise_degrees(float(sidereal_longitude))

    for house_number in range(1, 13):
        next_house_number = 1 if house_number == 12 else house_number + 1

        start_value = cusps.get(
            f"House{house_number}",
            {},
        ).get("sidereal_longitude")
        end_value = cusps.get(
            f"House{next_house_number}",
            {},
        ).get("sidereal_longitude")

        if not isinstance(start_value, (int, float)):
            return None

        if not isinstance(end_value, (int, float)):
            return None

        start = normalise_degrees(float(start_value))
        end = normalise_degrees(float(end_value))
        arc = (end - start) % 360.0
        offset = (longitude - start) % 360.0

        if offset < arc - 1e-9 or abs(offset) <= 1e-9:
            return house_number

    return None


def sort_planet_names(names: set[str] | list[str]) -> list[str]:
    return sorted(
        set(names),
        key=lambda name: (
            PLANET_ORDER.get(name, 999),
            name,
        ),
    )


def calculate_kp_planet_position(
    planet_name: str,
    event_time: Time,
) -> dict[str, Any]:
    """Calculate one exact planet longitude using KP ayanamsha."""

    result = vedastro_call_for_ayanamsa(
        KP_AYANAMSHA_NAME,
        "PlanetNirayanaLongitude",
        PLANETS[planet_name],
        event_time,
    )
    longitude = extract_total_degrees(result)

    if result.get("status") != "Pass" or longitude is None:
        return {
            "status": "Fail",
            "planet": planet_name,
            "longitude_result": result,
            "error": "Could not obtain exact KP sidereal longitude.",
        }

    details = kp_nakshatra_and_sublord(longitude)

    return {
        "status": "Pass",
        "planet": planet_name,
        "ayanamsa": "Krishnamurti",
        **details,
        "longitude_result": {
            key: value
            for key, value in result.items()
            if key != "data"
        },
    }


def build_kp_significator_matrix(
    cusps: dict[str, dict[str, Any]],
    planet_positions: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """
    Build Chapter 6 columns A-D for all twelve houses.

    A: lord of the sign containing the cusp
    B: planets tenanting a nakshatra of the cusp lord
    C: planets residing in the Placidus house
    D: planets tenanting a nakshatra of a house resident
    """

    matrix: dict[str, dict[str, Any]] = {}

    for house_number in range(1, 13):
        house_name = f"House{house_number}"
        cusp = cusps[house_name]
        cusp_sign = cusp["sign"]
        cusp_lord = SIGN_LORDS[cusp_sign]

        residents = sort_planet_names([
            planet_name
            for planet_name, position in planet_positions.items()
            if position.get("house") == house_number
        ])

        column_b = sort_planet_names([
            planet_name
            for planet_name, position in planet_positions.items()
            if position.get("nakshatra_lord") == cusp_lord
        ])

        resident_lords = set(residents)
        column_d = sort_planet_names([
            planet_name
            for planet_name, position in planet_positions.items()
            if position.get("nakshatra_lord") in resident_lords
        ])

        matrix[house_name] = {
            "house": house_name,
            "house_number": house_number,
            "cusp_longitude": cusp["sidereal_longitude"],
            "cusp_sign": cusp_sign,
            "column_A_cusp_sign_lord": [cusp_lord],
            "column_B_planets_in_cusp_lord_stars": column_b,
            "column_C_house_residents": residents,
            "column_D_planets_in_resident_stars": column_d,
            "columns": {
                "A": [cusp_lord],
                "B": column_b,
                "C": residents,
                "D": column_d,
            },
        }

    return matrix


def kp_effective_column_weights(
    row: dict[str, Any],
) -> tuple[dict[str, float], str]:
    """
    Apply the exact sparse-row rules stated in Chapter 6.

    - Standard A/B/C/D weights are 1/2/3/4.
    - If the complete row has only one planet-column membership, it gets 4.
    - If A and B are the only populated columns, use A=2 and B=4.
    """

    columns = row["columns"]
    populated = [
        column
        for column in ("A", "B", "C", "D")
        if columns[column]
    ]
    membership_count = sum(
        len(columns[column])
        for column in ("A", "B", "C", "D")
    )

    if membership_count == 1:
        only_column = populated[0]
        weights = dict(KP_COLUMN_WEIGHTS)
        weights[only_column] = 4.0
        return weights, f"single-membership {only_column}=4"

    if set(populated) == {"A", "B"}:
        weights = dict(KP_COLUMN_WEIGHTS)
        weights["A"] = 2.0
        weights["B"] = 4.0
        return weights, "sparse A/B rule: A=2, B=4"

    return dict(KP_COLUMN_WEIGHTS), "standard A=1, B=2, C=3, D=4"


def score_kp_sublord(
    sublord: str,
    matrix: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    """Score one 1st/7th cusp sublord across the Chapter 6 matrix."""

    breakdown: list[dict[str, Any]] = []
    total = 0.0

    for house_number in range(1, 13):
        house_name = f"House{house_number}"
        row = matrix[house_name]
        weights, weight_mode = kp_effective_column_weights(row)
        house_value = KP_HOUSE_VALUES[house_number]

        contributions = []

        for column in ("A", "B", "C", "D"):
            if sublord not in row["columns"][column]:
                continue

            raw_weight = weights[column]
            points = raw_weight * house_value
            total += points

            contributions.append({
                "column": column,
                "weight": raw_weight,
                "house_value": house_value,
                "points": round(points, 8),
            })

        if contributions:
            breakdown.append({
                "house": house_name,
                "house_number": house_number,
                "weight_mode": weight_mode,
                "contributions": contributions,
                "house_total": round(
                    sum(
                        item["points"]
                        for item in contributions
                    ),
                    8,
                ),
            })

    return {
        "sublord": sublord,
        "score": round(total, 8),
        "breakdown": breakdown,
    }


def compare_kp_cusp_sublords(
    cusp_sublords: dict[str, dict[str, Any]],
    matrix: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    """Compare the Lagna and seventh-cusp sublords using Chapter 6."""

    lagna_sublord = cusp_sublords["House1"]["sublord"]
    seventh_sublord = cusp_sublords["House7"]["sublord"]

    lagna_score = score_kp_sublord(lagna_sublord, matrix)
    seventh_score = score_kp_sublord(seventh_sublord, matrix)

    same_sublord = lagna_sublord == seventh_sublord

    if same_sublord:
        signed_differential = lagna_score["score"]
        differential_rule = (
            "Same 1st/7th sublord: use that planet's score as the "
            "favourite differential."
        )
    else:
        signed_differential = (
            lagna_score["score"] - seventh_score["score"]
        )
        differential_rule = (
            "Favourite differential = Lagna-sublord score minus "
            "seventh-cusp-sublord score."
        )

    if signed_differential > 3:
        indication = "Favourite"
    elif signed_differential < -3:
        indication = "Underdog"
    else:
        indication = "Balanced / virtually draw"

    return {
        "lagna_sublord": lagna_score,
        "seventh_cusp_sublord": seventh_score,
        "same_sublord": same_sublord,
        "differential_rule": differential_rule,
        "signed_favourite_differential": round(
            signed_differential,
            8,
        ),
        "three_point_close_threshold": True,
        "indication": indication,
    }


def detect_kp_sublord_array(
    cusp_sublords: dict[str, dict[str, Any]],
    planet_positions: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    """Detect the full and weaker Chapter 6 sublord-array patterns."""

    pointers: dict[str, dict[str, Any]] = {}
    unavailable = []

    for house_number in range(1, 13):
        house_name = f"House{house_number}"
        sublord = cusp_sublords[house_name]["sublord"]
        position = planet_positions.get(sublord)
        occupied_house = (
            position.get("house")
            if position
            else None
        )

        if occupied_house is None:
            unavailable.append({
                "cusp": house_name,
                "sublord": sublord,
                "reason": "Sublord planet house is unavailable.",
            })

        pointers[house_name] = {
            "cusp": house_name,
            "sublord": sublord,
            "sublord_planet_house": occupied_house,
        }

    if unavailable:
        return {
            "status": "Partial",
            "pointers": pointers,
            "full_array": None,
            "weaker_1_7_10_array": None,
            "unavailable": unavailable,
            "points_applied": False,
        }

    occupied_houses = [
        item["sublord_planet_house"]
        for item in pointers.values()
    ]

    favourite_full = all(
        house in (
            KP_FAVOURITE_ARRAY_HOUSES
            | KP_NEUTRAL_ARRAY_HOUSES
        )
        for house in occupied_houses
    ) and any(
        house in KP_FAVOURITE_ARRAY_HOUSES
        for house in occupied_houses
    )

    underdog_full = all(
        house in (
            KP_UNDERDOG_ARRAY_HOUSES
            | KP_NEUTRAL_ARRAY_HOUSES
        )
        for house in occupied_houses
    ) and any(
        house in KP_UNDERDOG_ARRAY_HOUSES
        for house in occupied_houses
    )

    if favourite_full and not underdog_full:
        full_side = "Favourite"
    elif underdog_full and not favourite_full:
        full_side = "Underdog"
    else:
        full_side = None

    key_houses = [
        pointers["House1"]["sublord_planet_house"],
        pointers["House7"]["sublord_planet_house"],
        pointers["House10"]["sublord_planet_house"],
    ]

    weak_favourite = all(
        house in KP_WEAK_FAVOURITE_ARRAY_HOUSES
        for house in key_houses
    )
    weak_underdog = all(
        house in KP_WEAK_UNDERDOG_ARRAY_HOUSES
        for house in key_houses
    )

    if weak_favourite and not weak_underdog:
        weak_side = "Favourite"
    elif weak_underdog and not weak_favourite:
        weak_side = "Underdog"
    else:
        weak_side = None

    return {
        "status": "Pass",
        "pointers": pointers,
        "full_array": {
            "detected": full_side is not None,
            "side": full_side,
            "favourite_houses": sorted(
                KP_FAVOURITE_ARRAY_HOUSES
            ),
            "underdog_houses": sorted(
                KP_UNDERDOG_ARRAY_HOUSES
            ),
            "neutral_houses": sorted(
                KP_NEUTRAL_ARRAY_HOUSES
            ),
            "book_tier": 2,
            "book_point_range_if_applied": [7, 9],
        },
        "weaker_1_7_10_array": {
            "detected": weak_side is not None,
            "side": weak_side,
            "cusp_sublords_checked": [
                "House1",
                "House7",
                "House10",
            ],
            "sublord_planet_houses": key_houses,
            "book_tier": 1,
        },
        "points_applied": False,
        "points_note": (
            "Array detection is returned separately. No automatic "
            "array points are added by the astronomy layer."
        ),
        "unavailable": [],
    }


def calculate_kp_sublords(
    event_time: Time,
    requested_planets: list[str],
) -> dict[str, Any]:
    """
    Calculate the book-compliant Krishnamurti KP sublord layer.

    Standard event-chart calculations remain Lahiri. Only this block sends
    KRISHNAMURTI in the upstream payload.
    """

    required_planets = list(PLANETS)
    missing_requested_planets = [
        planet
        for planet in required_planets
        if planet not in requested_planets
    ]

    ayanamsa_degree = vedastro_call_for_ayanamsa(
        KP_AYANAMSHA_NAME,
        "AyanamsaDegree",
        event_time,
    )

    raw_cusps = vedastro_call_for_ayanamsa(
        KP_AYANAMSHA_NAME,
        "GetAllHouseNirayanaMiddleLongitudes",
        event_time,
    )
    parsed_cusps = parse_placidus_cusps(raw_cusps)

    if parsed_cusps.get("status") == "Pass":
        parsed_cusps["ayanamsa"] = "Krishnamurti"
        parsed_cusps["requested_payload_ayanamsa"] = (
            KP_AYANAMSHA_NAME
        )

    planet_positions: dict[str, dict[str, Any]] = {}

    with ThreadPoolExecutor(
        max_workers=min(VEDASTRO_MAX_WORKERS, len(required_planets))
    ) as executor:
        futures = {
            executor.submit(
                calculate_kp_planet_position,
                planet_name,
                event_time,
            ): planet_name
            for planet_name in required_planets
        }

        for future in as_completed(futures):
            planet_name = futures[future]

            try:
                planet_positions[planet_name] = future.result()
            except Exception as error:
                planet_positions[planet_name] = {
                    "status": "Fail",
                    "planet": planet_name,
                    "error": str(error),
                }

    planet_positions = {
        planet_name: planet_positions[planet_name]
        for planet_name in required_planets
    }

    failed_planets = [
        result
        for result in planet_positions.values()
        if result.get("status") != "Pass"
    ]

    if (
        ayanamsa_degree.get("status") != "Pass"
        or parsed_cusps.get("status") != "Pass"
        or failed_planets
    ):
        return {
            "status": "Fail",
            "method": "KrishnamurtiKPSublords",
            "ayanamsa": "Krishnamurti",
            "requested_payload_ayanamsa": KP_AYANAMSHA_NAME,
            "ayanamsa_degree": ayanamsa_degree,
            "cusps": parsed_cusps,
            "planet_positions": planet_positions,
            "failed_planets": failed_planets,
            "missing_requested_planets": missing_requested_planets,
            "error": (
                "The KP ayanamsa, cusp or planet-longitude "
                "calculation failed."
            ),
        }

    cusp_map = parsed_cusps["cusps"]

    for planet_name, position in planet_positions.items():
        position["house"] = house_number_for_longitude(
            position["sidereal_longitude"],
            cusp_map,
        )

    missing_houses = [
        planet_name
        for planet_name, position in planet_positions.items()
        if position.get("house") is None
    ]

    cusp_sublords = {}

    for house_number in range(1, 13):
        house_name = f"House{house_number}"
        cusp = cusp_map[house_name]
        kp_details = kp_nakshatra_and_sublord(
            cusp["sidereal_longitude"]
        )

        cusp_sublords[house_name] = {
            "house": house_name,
            "cusp_longitude": cusp["sidereal_longitude"],
            "cusp_sign": cusp["sign"],
            "cusp_sign_lord": SIGN_LORDS[cusp["sign"]],
            "nakshatra": kp_details["nakshatra"],
            "nakshatra_lord": kp_details["nakshatra_lord"],
            "sublord": kp_details["sublord"],
            "sublord_segment_start_longitude": (
                kp_details["sublord_segment_start_longitude"]
            ),
            "sublord_segment_end_longitude": (
                kp_details["sublord_segment_end_longitude"]
            ),
        }

    matrix = build_kp_significator_matrix(
        cusp_map,
        planet_positions,
    )
    comparison = compare_kp_cusp_sublords(
        cusp_sublords,
        matrix,
    )
    array = detect_kp_sublord_array(
        cusp_sublords,
        planet_positions,
    )

    status = (
        "Pass"
        if not missing_houses and not missing_requested_planets
        else "Partial"
    )

    return {
        "status": status,
        "method": "KrishnamurtiKPSublords",
        "book_chapter": 6,
        "book_tier": 2,
        "ayanamsa": "Krishnamurti",
        "requested_payload_ayanamsa": KP_AYANAMSHA_NAME,
        "standard_chart_ayanamsa_unchanged": "Lahiri",
        "interpretation_applied": True,
        "scoring_policy": {
            "columns": {
                "A": 1,
                "B": 2,
                "C": 3,
                "D": 4,
            },
            "single_membership": 4,
            "only_A_and_B_populated": {
                "A": 2,
                "B": 4,
            },
            "positive_houses": [1, 3, 6, 10, 11],
            "house2": "half positive",
            "negative_houses": [4, 5, 7, 8, 9, 12],
            "house8": "full negative",
            "close_difference": "3 points or less",
        },
        "ayanamsa_degree": ayanamsa_degree,
        "cusps": parsed_cusps,
        "planet_positions": planet_positions,
        "cusp_sublords": cusp_sublords,
        "significator_matrix": matrix,
        "main_sublord_comparison": comparison,
        "sublord_array": array,
        "failed_planets": [],
        "missing_planet_houses": missing_houses,
        "missing_requested_planets": missing_requested_planets,
        "error": (
            None
            if status == "Pass"
            else (
                "KP geometry was calculated, but the original Action "
                "request did not contain every classical planet or one "
                "planet could not be placed in a Placidus house."
            )
        ),
    }


def parse_std_time_to_utc(std_time: str) -> dict[str, Any]:
    """Parse the proxy's exact local time format and convert it to UTC."""

    try:
        local_datetime = datetime.strptime(
            std_time,
            "%H:%M %d/%m/%Y %z",
        )
    except ValueError as error:
        raise ValueError(
            "std_time must use HH:MM DD/MM/YYYY +HH:MM format."
        ) from error

    utc_datetime = local_datetime.astimezone(timezone.utc)
    decimal_hour = (
        utc_datetime.hour
        + (utc_datetime.minute / 60.0)
        + (utc_datetime.second / 3600.0)
        + (utc_datetime.microsecond / 3_600_000_000.0)
    )

    if not SWISSEPH_AVAILABLE:
        julian_day_ut = None
    else:
        julian_day_ut = swe.julday(
            utc_datetime.year,
            utc_datetime.month,
            utc_datetime.day,
            decimal_hour,
            swe.GREG_CAL,
        )

    return {
        "local_datetime": local_datetime.isoformat(),
        "utc_datetime": utc_datetime.isoformat(),
        "julian_day_ut": julian_day_ut,
    }


def swisseph_ephemeris_mode(return_flags: int) -> str:
    """Describe which Swiss Ephemeris source actually supplied the result."""

    if not SWISSEPH_AVAILABLE:
        return "Unavailable"

    if return_flags & swe.FLG_JPLEPH:
        return "JPL ephemeris"

    if return_flags & swe.FLG_SWIEPH:
        return "Swiss Ephemeris file"

    if return_flags & swe.FLG_MOSEPH:
        return "Moshier fallback"

    return "Unknown"


def outer_motion_name(speed_longitude: float) -> str:
    """
    Report objective direction only.

    No arbitrary stationary threshold is invented here. Exact daily speed is
    returned so the later sandhi/kutila layer can apply a documented threshold.
    """

    if speed_longitude < 0:
        return "Retrograde"

    if speed_longitude > 0:
        return "Direct"

    return "Exactly stationary"


def outer_contact_effect(
    body_name: str,
    cusp_name: str,
    motion_name: str,
) -> dict[str, Any]:
    """Apply only the qualitative Chapter 4 outer-body cusp rule."""

    side = SENSITIVE_CUSP_DETAILS[cusp_name]["side"]
    axis = SENSITIVE_CUSP_DETAILS[cusp_name]["axis"]
    motion_key = motion_name.lower()

    if motion_key == "exactly stationary":
        motion_key = "stationary"

    if body_name == "Pluto":
        if axis == "1/7":
            direction = "Harms"
            summary = f"Harms the {side.lower()} side represented by {cusp_name}."
        elif axis == "10/4":
            direction = "Supports"
            summary = (
                f"Supports the {side.lower()} side represented by {cusp_name}."
            )
        else:
            direction = "Undefined"
            summary = (
                "The book does not explicitly define Pluto's 6/12 cusp effect."
            )
    else:
        rule_text = OUTER_BODY_BOOK_RULES[body_name].get(
            motion_key,
            OUTER_BODY_BOOK_RULES[body_name]["stationary"],
        )

        if rule_text.startswith("Supports"):
            direction = "Supports"
        elif rule_text.startswith("Harms"):
            direction = "Harms"
        else:
            direction = "Uncertain"

        summary = rule_text

    return {
        "body": body_name,
        "cusp": cusp_name,
        "axis": axis,
        "represented_side": side,
        "motion": motion_name,
        "direction": direction,
        "summary": summary,
        "points_applied": False,
    }


def calculate_one_outer_body(
    body_name: str,
    body_id: int,
    julian_day_ut: float,
    rashi_placidus: dict[str, Any],
) -> dict[str, Any]:
    """Calculate one Lahiri sidereal outer-body position and cusp geometry."""

    if not SWISSEPH_AVAILABLE:
        return {
            "status": "Unavailable",
            "body": body_name,
            "error": (
                "pyswisseph is not installed. Add it to requirements.txt."
            ),
        }

    flags = (
        swe.FLG_SWIEPH
        | swe.FLG_SPEED
        | swe.FLG_SIDEREAL
    )

    try:
        with SWISSEPH_LOCK:
            swe.set_sid_mode(swe.SIDM_LAHIRI)

            if SWISSEPH_EPHE_PATH:
                swe.set_ephe_path(SWISSEPH_EPHE_PATH)

            position, return_flags = swe.calc_ut(
                julian_day_ut,
                body_id,
                flags,
            )
            ayanamsa_degree = swe.get_ayanamsa_ut(julian_day_ut)
    except Exception as error:
        message = str(error)

        missing_file = None
        marker = "file '"

        if marker in message:
            remainder = message.split(marker, 1)[1]
            missing_file = remainder.split("'", 1)[0]

        return {
            "status": "Unavailable",
            "body": body_name,
            "engine": "Swiss Ephemeris via pyswisseph",
            "ayanamsa": "Lahiri",
            "error": message,
            "missing_ephemeris_file": missing_file,
            "remedy": (
                "Set SWISSEPH_EPHE_PATH to a directory containing the "
                "required Swiss Ephemeris asteroid files."
                if missing_file
                else None
            ),
        }

    longitude = normalise_degrees(float(position[0]))
    latitude = float(position[1])
    distance_au = float(position[2])
    speed_longitude = float(position[3])
    speed_latitude = float(position[4])
    speed_distance = float(position[5])
    motion_name = outer_motion_name(speed_longitude)

    house = None

    if rashi_placidus.get("status") == "Pass":
        house = house_number_for_longitude(
            longitude,
            rashi_placidus["cusps"],
        )

    distances = {}
    qualifying_contacts = []

    if rashi_placidus.get("status") == "Pass":
        for cusp_name in SENSITIVE_CUSP_DETAILS:
            cusp_longitude = (
                rashi_placidus["cusps"][cusp_name]["sidereal_longitude"]
            )
            distance = angular_distance(longitude, cusp_longitude)

            distances[cusp_name] = round(distance, 8)

            if distance <= OUTER_CUSP_ORB_DEGREES + 1e-9:
                qualifying_contacts.append({
                    "body": body_name,
                    "cusp": cusp_name,
                    "axis": SENSITIVE_CUSP_DETAILS[cusp_name]["axis"],
                    "side": SENSITIVE_CUSP_DETAILS[cusp_name]["side"],
                    "body_longitude": round(longitude, 8),
                    "cusp_longitude": round(cusp_longitude, 8),
                    "angular_distance": round(distance, 8),
                    "orb_limit": OUTER_CUSP_ORB_DEGREES,
                    "within_orb": True,
                    "orb_margin": round(
                        OUTER_CUSP_ORB_DEGREES - distance,
                        8,
                    ),
                    "book_effect": outer_contact_effect(
                        body_name,
                        cusp_name,
                        motion_name,
                    ),
                })

    nearest_cusp = (
        min(distances, key=distances.get)
        if distances
        else None
    )

    d9_position = exact_navamsha_position(longitude)

    return {
        "status": "Pass",
        "body": body_name,
        "engine": "Swiss Ephemeris via pyswisseph",
        "ayanamsa": "Lahiri",
        "lahiri_ayanamsa_degree": round(
            float(ayanamsa_degree),
            8,
        ),
        "julian_day_ut": round(julian_day_ut, 8),
        "sidereal_longitude": round(longitude, 8),
        **sign_details_from_longitude(longitude),
        "ecliptic_latitude": round(latitude, 8),
        "distance_au": round(distance_au, 8),
        "daily_motion_longitude": round(speed_longitude, 10),
        "daily_motion_latitude": round(speed_latitude, 10),
        "daily_motion_distance": round(speed_distance, 10),
        "motion": motion_name,
        "retrograde": speed_longitude < 0,
        "stationary_threshold_applied": False,
        "stationary_note": (
            "Raw speed returned; no arbitrary stationary threshold "
            "is applied in Step 5A."
        ),
        "placidus_house": house,
        "sensitive_cusp_distances": distances,
        "nearest_sensitive_cusp": nearest_cusp,
        "nearest_sensitive_cusp_distance": (
            distances.get(nearest_cusp)
            if nearest_cusp
            else None
        ),
        "qualifying_contacts": qualifying_contacts,
        "d9_position_raw": d9_position,
        "d9_contact_interpretation_applied": False,
        "ephemeris_mode": swisseph_ephemeris_mode(return_flags),
        "return_flags": int(return_flags),
        "book_motion_rule": OUTER_BODY_BOOK_RULES[body_name],
        "points_applied": False,
    }


def calculate_outer_planets(
    std_time: str,
    rashi_placidus: dict[str, Any],
) -> dict[str, Any]:
    """
    Calculate Uranus, Neptune, Pluto, Ceres and Chiron.

    This is a non-essential advanced layer. Missing asteroid files do not
    invalidate the standard event chart.
    """

    if not SWISSEPH_AVAILABLE:
        return {
            "status": "Unavailable",
            "method": "LocalSwissEphemerisOuterPlanets",
            "engine": "pyswisseph",
            "ayanamsa": "Lahiri",
            "bodies": {},
            "available_bodies": [],
            "unavailable_bodies": list(OUTER_BODY_ORDER),
            "error": (
                "pyswisseph is not installed. Deploy the supplied "
                "requirements.txt together with main.py."
            ),
        }

    try:
        time_data = parse_std_time_to_utc(std_time)
        julian_day_ut = time_data["julian_day_ut"]
    except Exception as error:
        return {
            "status": "Fail",
            "method": "LocalSwissEphemerisOuterPlanets",
            "engine": "pyswisseph",
            "ayanamsa": "Lahiri",
            "bodies": {},
            "available_bodies": [],
            "unavailable_bodies": list(OUTER_BODY_ORDER),
            "error": str(error),
        }

    bodies = {}

    for body_name in OUTER_BODY_ORDER:
        bodies[body_name] = calculate_one_outer_body(
            body_name,
            OUTER_BODY_IDS[body_name],
            julian_day_ut,
            rashi_placidus,
        )

    available_bodies = [
        body_name
        for body_name, result in bodies.items()
        if result.get("status") == "Pass"
    ]
    unavailable_bodies = [
        body_name
        for body_name, result in bodies.items()
        if result.get("status") != "Pass"
    ]

    qualifying_contacts = []

    for body_name in OUTER_BODY_ORDER:
        body_result = bodies[body_name]

        if body_result.get("status") != "Pass":
            continue

        qualifying_contacts.extend(
            body_result.get("qualifying_contacts", [])
        )

    closest_body_by_cusp = {}

    for cusp_name in SENSITIVE_CUSP_DETAILS:
        candidates = []

        for body_name in available_bodies:
            body = bodies[body_name]
            distance = body["sensitive_cusp_distances"].get(cusp_name)

            if distance is None:
                continue

            candidates.append({
                "body": body_name,
                "cusp": cusp_name,
                "angular_distance": distance,
                "orb_limit": OUTER_CUSP_ORB_DEGREES,
                "within_orb": (
                    distance <= OUTER_CUSP_ORB_DEGREES + 1e-9
                ),
                "motion": body["motion"],
            })

        if candidates:
            closest_body_by_cusp[cusp_name] = min(
                candidates,
                key=lambda item: (
                    item["angular_distance"],
                    OUTER_BODY_ORDER.index(item["body"]),
                ),
            )

    if len(available_bodies) == len(OUTER_BODY_ORDER):
        status = "Pass"
        error = None
    elif available_bodies:
        status = "Partial"
        error = (
            "Some outer bodies were calculated, while others require "
            "additional Swiss Ephemeris asteroid files."
        )
    else:
        status = "Unavailable"
        error = "No outer-body position could be calculated."

    return {
        "status": status,
        "method": "LocalSwissEphemerisOuterPlanets",
        "book_chapter": 4,
        "book_layer": "Tier 2 invisible-graha cusp geometry",
        "engine": "Swiss Ephemeris via pyswisseph",
        "engine_version": getattr(swe, "version", None),
        "python_binding_version": getattr(swe, "__version__", None),
        "ayanamsa": "Lahiri",
        "event_time": time_data,
        "orb_policy": {
            "outer_planets_degrees": OUTER_CUSP_ORB_DEGREES,
            "applies_to": list(OUTER_BODY_ORDER),
        },
        "interpretation_applied": "Qualitative cusp effect only",
        "points_applied": False,
        "bodies": bodies,
        "available_bodies": available_bodies,
        "unavailable_bodies": unavailable_bodies,
        "qualifying_contacts": sorted(
            qualifying_contacts,
            key=lambda item: (
                list(SENSITIVE_CUSP_DETAILS).index(item["cusp"]),
                item["angular_distance"],
                OUTER_BODY_ORDER.index(item["body"]),
            ),
        ),
        "closest_body_by_cusp": closest_body_by_cusp,
        "ephemeris_path_configured": bool(SWISSEPH_EPHE_PATH),
        "ephemeris_path": (
            SWISSEPH_EPHE_PATH
            if SWISSEPH_EPHE_PATH
            else None
        ),
        "error": error,
    }


def extract_planet_name_from_result(
    result: dict[str, Any],
) -> str | None:
    """Extract one supported planet name from a VedAstro result."""

    value = find_named_value(
        unwrap_data(result),
        ("Name", "PlanetName", "name"),
    )

    if value is None:
        data = unwrap_data(result)

        if isinstance(data, str):
            value = data

    if not value:
        return None

    normalised = str(value).lower()

    for planet_name in PLANETS:
        if planet_name.lower() in normalised:
            return planet_name

    return None


def special_point_rashi_effect(
    point_name: str,
    cusp_name: str,
) -> dict[str, Any]:
    """Return only the qualitative book rule for one qualifying D1 contact."""

    metadata = SENSITIVE_CUSP_DETAILS[cusp_name]
    side = metadata["side"]
    axis = metadata["axis"]
    opposing_side = (
        "Underdog"
        if side == "Favourite"
        else "Favourite"
    )

    if point_name == "Upaketu":
        direction = "Harms cusp side"
        supports = opposing_side
        rule = (
            "Upaketu acts like Ketu and is negative on every "
            "contacted rashi cusp."
        )
        status = "Book-defined"
    elif axis == "1/7":
        direction = "Harms cusp side"
        supports = opposing_side
        rule = (
            "Gulika is negative on the first and seventh cusps."
        )
        status = "Book-defined"
    elif axis == "10/4":
        direction = "Supports cusp side"
        supports = side
        rule = (
            "Gulika behaves like Saturn on the fourth and tenth "
            "cusps and helps the represented team."
        )
        status = "Book-defined"
    else:
        direction = "Undefined"
        supports = None
        rule = (
            "The book does not explicitly define Gulika's effect "
            "on the sixth/twelfth axis."
        )
        status = "Not defined by book"

    return {
        "point": point_name,
        "cusp": cusp_name,
        "axis": axis,
        "represented_side": side,
        "direction": direction,
        "supports": supports,
        "rule": rule,
        "rule_status": status,
        "points_applied": False,
    }


def special_point_d9_effect(
    point_name: str,
    cusp_name: str,
) -> dict[str, Any]:
    """Return the book's qualitative D9 1/7 rule."""

    side = (
        "Favourite"
        if cusp_name == "D9Lagna"
        else "Underdog"
    )
    opposing_side = (
        "Underdog"
        if side == "Favourite"
        else "Favourite"
    )

    if point_name == "Gulika":
        rule = (
            "Gulika on the D9 Lagna or D9 seventh cusp indicates "
            "defeat for the represented side."
        )
    else:
        rule = (
            "Upaketu acts like Ketu in D9 and is negative for the "
            "represented side."
        )

    return {
        "point": point_name,
        "cusp": cusp_name,
        "represented_side": side,
        "direction": "Harms cusp side",
        "supports": opposing_side,
        "rule": rule,
        "points_applied": False,
    }


def calculate_one_special_point(
    point_name: str,
    event_time: Time,
    rashi_placidus: dict[str, Any],
    navamsha_cusps: dict[str, Any],
) -> dict[str, Any]:
    """Calculate one exact Lahiri upagraha through VedAstro's official API."""

    upstream = direct_vedastro_time_endpoint_call(
        SPECIAL_POINT_ENDPOINTS[point_name],
        event_time,
        ayanamsa_name="LAHIRI",
    )
    longitude = extract_total_degrees(upstream)

    if upstream.get("status") != "Pass" or longitude is None:
        return {
            "status": "Unavailable",
            "point": point_name,
            "engine": "Official VedAstro server calculator",
            "ayanamsa": "Lahiri",
            "upstream": upstream,
            "error": (
                f"Could not obtain exact {point_name} longitude "
                "from the official VedAstro calculator."
            ),
        }

    longitude = normalise_degrees(longitude)
    house = None

    if rashi_placidus.get("status") == "Pass":
        house = house_number_for_longitude(
            longitude,
            rashi_placidus["cusps"],
        )

    d1_distances: dict[str, float] = {}
    d1_contacts: list[dict[str, Any]] = []

    if rashi_placidus.get("status") == "Pass":
        for cusp_name, metadata in SENSITIVE_CUSP_DETAILS.items():
            cusp_longitude = rashi_placidus[
                "cusps"
            ][cusp_name]["sidereal_longitude"]
            distance = angular_distance(
                longitude,
                cusp_longitude,
            )
            d1_distances[cusp_name] = round(distance, 8)

            if distance <= SPECIAL_POINT_CUSP_ORB_DEGREES + 1e-9:
                d1_contacts.append({
                    "point": point_name,
                    "cusp": cusp_name,
                    "axis": metadata["axis"],
                    "side": metadata["side"],
                    "point_longitude": round(longitude, 8),
                    "cusp_longitude": round(cusp_longitude, 8),
                    "angular_distance": round(distance, 8),
                    "orb_limit": SPECIAL_POINT_CUSP_ORB_DEGREES,
                    "within_orb": True,
                    "orb_margin": round(
                        SPECIAL_POINT_CUSP_ORB_DEGREES - distance,
                        8,
                    ),
                    "book_effect": special_point_rashi_effect(
                        point_name,
                        cusp_name,
                    ),
                })

    nearest_d1_cusp = (
        min(d1_distances, key=d1_distances.get)
        if d1_distances
        else None
    )

    d9_position = exact_navamsha_position(longitude)
    d9_distances: dict[str, float] = {}
    d9_contacts: list[dict[str, Any]] = []

    if navamsha_cusps.get("status") in {"Pass", "Partial"}:
        for cusp_name, cusp_data in (
            ("D9Lagna", navamsha_cusps.get("lagna")),
            ("D9House7", navamsha_cusps.get("seventh_cusp")),
        ):
            if not isinstance(cusp_data, dict):
                continue

            cusp_longitude = cusp_data.get(
                "d9_sidereal_longitude"
            )

            if not isinstance(cusp_longitude, (int, float)):
                continue

            distance = angular_distance(
                d9_position["d9_sidereal_longitude"],
                cusp_longitude,
            )
            d9_distances[cusp_name] = round(distance, 8)

            if distance <= SPECIAL_POINT_CUSP_ORB_DEGREES + 1e-9:
                d9_contacts.append({
                    "point": point_name,
                    "cusp": cusp_name,
                    "point_d9_longitude": d9_position[
                        "d9_sidereal_longitude"
                    ],
                    "cusp_d9_longitude": round(
                        float(cusp_longitude),
                        8,
                    ),
                    "angular_distance": round(distance, 8),
                    "orb_limit": SPECIAL_POINT_CUSP_ORB_DEGREES,
                    "within_orb": True,
                    "orb_margin": round(
                        SPECIAL_POINT_CUSP_ORB_DEGREES - distance,
                        8,
                    ),
                    "book_effect": special_point_d9_effect(
                        point_name,
                        cusp_name,
                    ),
                })

    nearest_d9_cusp = (
        min(d9_distances, key=d9_distances.get)
        if d9_distances
        else None
    )

    return {
        "status": "Pass",
        "point": point_name,
        "engine": "Official VedAstro server calculator",
        "ayanamsa": "Lahiri",
        "upstream_method": upstream.get("method"),
        "upstream_parameter_name": upstream.get(
            "parameter_name"
        ),
        "sidereal_longitude": round(longitude, 8),
        **sign_details_from_longitude(longitude),
        "placidus_house": house,
        "rashi": {
            "orb_limit": SPECIAL_POINT_CUSP_ORB_DEGREES,
            "sensitive_cusp_distances": d1_distances,
            "nearest_sensitive_cusp": nearest_d1_cusp,
            "nearest_distance": (
                d1_distances.get(nearest_d1_cusp)
                if nearest_d1_cusp
                else None
            ),
            "qualifying_contacts": d1_contacts,
        },
        "d9": {
            "position": d9_position,
            "orb_limit": SPECIAL_POINT_CUSP_ORB_DEGREES,
            "cusp_distances": d9_distances,
            "nearest_cusp": nearest_d9_cusp,
            "nearest_distance": (
                d9_distances.get(nearest_d9_cusp)
                if nearest_d9_cusp
                else None
            ),
            "qualifying_contacts": d9_contacts,
        },
        "points_applied": False,
        "upstream": {
            key: value
            for key, value in upstream.items()
            if key not in {"data", "failures"}
        },
    }


def calculate_gulika_house_lord_contacts(
    gulika_result: dict[str, Any],
    houses: dict[str, dict[str, Any]],
    planets: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    """
    Test the book's special one-degree Gulika conjunction with the lords of
    House 1 and House 7.
    """

    if gulika_result.get("status") != "Pass":
        return {
            "status": "Unavailable",
            "orb_limit": GULIKA_HOUSE_LORD_ORB_DEGREES,
            "contacts": [],
            "error": "Exact Gulika longitude is unavailable.",
        }

    gulika_longitude = gulika_result["sidereal_longitude"]
    contacts: list[dict[str, Any]] = []
    unavailable: list[dict[str, Any]] = []

    for house_name, represented_side in (
        ("House1", "Favourite"),
        ("House7", "Underdog"),
    ):
        house_result = houses.get(house_name, {})
        lord_name = extract_planet_name_from_result(
            house_result.get("lord", {})
        )

        if not lord_name:
            unavailable.append({
                "house": house_name,
                "reason": "Could not parse the house lord.",
            })
            continue

        planet_result = planets.get(lord_name)
        planet_longitude = (
            extract_total_degrees(
                planet_result.get("sidereal_longitude", {})
            )
            if planet_result
            else None
        )

        if planet_longitude is None:
            unavailable.append({
                "house": house_name,
                "lord": lord_name,
                "reason": (
                    "The house-lord planet longitude was not returned "
                    "in the Action request."
                ),
            })
            continue

        distance = angular_distance(
            gulika_longitude,
            planet_longitude,
        )
        within_orb = (
            distance <= GULIKA_HOUSE_LORD_ORB_DEGREES + 1e-9
        )

        contacts.append({
            "house": house_name,
            "represented_side": represented_side,
            "house_lord": lord_name,
            "gulika_longitude": round(gulika_longitude, 8),
            "lord_longitude": round(
                normalise_degrees(planet_longitude),
                8,
            ),
            "angular_distance": round(distance, 8),
            "orb_limit": GULIKA_HOUSE_LORD_ORB_DEGREES,
            "within_orb": within_orb,
            "severe_detriment": within_orb,
            "book_effect": (
                "The represented side is highly detrimented."
                if within_orb
                else "No special one-degree Gulika/lord testimony."
            ),
            "points_applied": False,
        })

    if unavailable and not contacts:
        status = "Unavailable"
    elif unavailable:
        status = "Partial"
    else:
        status = "Pass"

    return {
        "status": status,
        "orb_limit": GULIKA_HOUSE_LORD_ORB_DEGREES,
        "contacts": contacts,
        "qualifying_contacts": [
            contact
            for contact in contacts
            if contact["within_orb"]
        ],
        "unavailable": unavailable,
        "points_applied": False,
    }


def calculate_special_points(
    event_time: Time,
    rashi_placidus: dict[str, Any],
    navamsha_cusps: dict[str, Any],
    houses: dict[str, dict[str, Any]],
    planets: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    """Calculate and validate Gulika and Upaketu as a non-essential layer."""

    points = {
        point_name: calculate_one_special_point(
            point_name,
            event_time,
            rashi_placidus,
            navamsha_cusps,
        )
        for point_name in SPECIAL_POINT_NAMES
    }

    available_points = [
        name
        for name, result in points.items()
        if result.get("status") == "Pass"
    ]
    unavailable_points = [
        name
        for name, result in points.items()
        if result.get("status") != "Pass"
    ]

    gulika_lord_contacts = calculate_gulika_house_lord_contacts(
        points["Gulika"],
        houses,
        planets,
    )

    qualifying_rashi_contacts = []
    qualifying_d9_contacts = []

    for point_name in SPECIAL_POINT_NAMES:
        point_result = points[point_name]

        if point_result.get("status") != "Pass":
            continue

        qualifying_rashi_contacts.extend(
            point_result["rashi"]["qualifying_contacts"]
        )
        qualifying_d9_contacts.extend(
            point_result["d9"]["qualifying_contacts"]
        )

    if len(available_points) == len(SPECIAL_POINT_NAMES):
        status = "Pass"
        error = None
    elif available_points:
        status = "Partial"
        error = (
            "One special point passed while the other official "
            "VedAstro calculation was unavailable."
        )
    else:
        status = "Unavailable"
        error = (
            "Neither official VedAstro special-point calculator "
            "returned an exact longitude."
        )

    return {
        "status": status,
        "method": "OfficialVedAstroGulikaUpaketuLongitudes",
        "book_chapters": [4, 5],
        "book_layer": "Invisible upagraha cusp geometry",
        "engine": "Official VedAstro server calculators",
        "ayanamsa": "Lahiri",
        "house_system": "Placidus",
        "interpretation_applied": "Qualitative book rules only",
        "points_applied": False,
        "orb_policy": {
            "rashi_and_d9_cusp_degrees": (
                SPECIAL_POINT_CUSP_ORB_DEGREES
            ),
            "gulika_house_lord_degrees": (
                GULIKA_HOUSE_LORD_ORB_DEGREES
            ),
        },
        "points": points,
        "gulika_house_lord_contacts": gulika_lord_contacts,
        "qualifying_rashi_contacts": qualifying_rashi_contacts,
        "qualifying_d9_contacts": qualifying_d9_contacts,
        "available_points": available_points,
        "unavailable_points": unavailable_points,
        "error": error,
    }


def opposite_contest_side(side: str) -> str:
    return "Underdog" if side == "Favourite" else "Favourite"


def distance_to_circular_range(
    longitude: float,
    range_start: float,
    range_end: float,
) -> tuple[float, float]:
    """
    Return minimum angular distance to an inclusive circular longitude range
    and the nearest longitude on that range.
    """

    value = normalise_degrees(longitude)
    start = normalise_degrees(range_start)
    end = normalise_degrees(range_end)

    if start <= end:
        inside = start <= value <= end
    else:
        inside = value >= start or value <= end

    if inside:
        return 0.0, value

    start_distance = angular_distance(value, start)
    end_distance = angular_distance(value, end)

    if start_distance <= end_distance:
        return start_distance, start

    return end_distance, end


def distance_to_tara_marker(
    longitude: float,
    tara: dict[str, Any],
) -> dict[str, Any] | None:
    """Find the closest book-defined marker position for one tara."""

    candidates: list[dict[str, Any]] = []

    for marker_longitude in tara.get("positions", ()):
        candidates.append({
            "distance": angular_distance(
                longitude,
                marker_longitude,
            ),
            "nearest_marker_longitude": normalise_degrees(
                marker_longitude
            ),
            "marker_type": "Point",
        })

    marker_range = tara.get("range")

    if marker_range:
        distance, nearest = distance_to_circular_range(
            longitude,
            marker_range[0],
            marker_range[1],
        )
        candidates.append({
            "distance": distance,
            "nearest_marker_longitude": nearest,
            "marker_type": "Range",
            "marker_range": [
                normalise_degrees(marker_range[0]),
                normalise_degrees(marker_range[1]),
            ],
        })

    if not candidates:
        return None

    return min(
        candidates,
        key=lambda item: (
            item["distance"],
            item["nearest_marker_longitude"],
        ),
    )


def tara_effect_for_target(
    tara: dict[str, Any],
    target: dict[str, Any],
) -> dict[str, Any]:
    """Apply only the explicit Chapter 8 qualitative direction."""

    effect_class = tara["effect_class"]
    represented_side = target["side"]
    opposing_side = opposite_contest_side(represented_side)
    target_house = target["house"]

    direction = "No winner direction"
    supports = None
    harms = None
    status = "Context or research only"

    if effect_class in {"positive", "mildly_positive"}:
        direction = "Supports represented side"
        supports = represented_side
        harms = opposing_side
        status = "Book-defined"
    elif effect_class in {"negative", "mildly_negative"}:
        direction = "Harms represented side"
        supports = opposing_side
        harms = represented_side
        status = "Book-defined"
    elif effect_class == "axis_dependent":
        if target["target_type"] != "Cusp":
            status = "Not applied to house lords"
        elif target_house in {"House10", "House4"}:
            direction = "Supports represented side"
            supports = represented_side
            harms = opposing_side
            status = "Book-defined Wasat/Saturn cusp rule"
        elif target_house in {"House1", "House7"}:
            direction = "Harms represented side"
            supports = opposing_side
            harms = represented_side
            status = "Book-defined Wasat/Saturn cusp rule"

    return {
        "effect_class": effect_class,
        "direction": direction,
        "supports": supports,
        "harms": harms,
        "rule_status": status,
        "description": tara["effect"],
        "tier_hint": tara["tier_hint"],
        "points_applied": False,
    }


def build_tara_targets(
    rashi_placidus: dict[str, Any],
    houses: dict[str, dict[str, Any]],
    planets: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    """
    Build exact H1/H10/H7/H4 cusp targets and their house-lord targets.

    A repeated planet is kept only in its highest-priority role. This applies
    the book's instruction that a Lagna lord outranks an opposing honour lord.
    """

    cusp_targets: list[dict[str, Any]] = []
    lord_candidates: list[dict[str, Any]] = []
    unavailable: list[dict[str, Any]] = []

    if rashi_placidus.get("status") != "Pass":
        return {
            "status": "Unavailable",
            "cusp_targets": [],
            "lord_targets": [],
            "suppressed_lord_roles": [],
            "unavailable": [{
                "reason": "Exact Lahiri Placidus cusps are unavailable."
            }],
        }

    cusp_map = rashi_placidus.get("cusps", {})

    for house_name, metadata in TARA_TARGETS.items():
        cusp_longitude = cusp_map.get(
            house_name,
            {},
        ).get("sidereal_longitude")

        if not isinstance(cusp_longitude, (int, float)):
            unavailable.append({
                "target_type": "Cusp",
                "house": house_name,
                "reason": "Exact cusp longitude is unavailable.",
            })
        else:
            cusp_targets.append({
                "target_id": f"{house_name}_cusp",
                "target_type": "Cusp",
                "house": house_name,
                "side": metadata["side"],
                "role": metadata["role"],
                "priority": metadata["priority"],
                "priority_rank": metadata["priority_rank"],
                "longitude": round(
                    normalise_degrees(cusp_longitude),
                    8,
                ),
            })

        house_result = houses.get(house_name, {})
        lord_name = extract_planet_name_from_result(
            house_result.get("lord", {})
        )

        if not lord_name:
            unavailable.append({
                "target_type": "House lord",
                "house": house_name,
                "reason": "Could not parse the house lord.",
            })
            continue

        planet_result = planets.get(lord_name)
        lord_longitude = (
            extract_total_degrees(
                planet_result.get("sidereal_longitude", {})
            )
            if planet_result
            else None
        )

        if lord_longitude is None:
            unavailable.append({
                "target_type": "House lord",
                "house": house_name,
                "planet": lord_name,
                "reason": (
                    "The exact house-lord longitude was not returned."
                ),
            })
            continue

        lord_candidates.append({
            "target_id": f"{house_name}_lord_{lord_name}",
            "target_type": "House lord",
            "house": house_name,
            "planet": lord_name,
            "side": metadata["side"],
            "role": metadata["role"],
            "priority": metadata["priority"],
            "priority_rank": metadata["priority_rank"],
            "longitude": round(
                normalise_degrees(lord_longitude),
                8,
            ),
        })

    lord_targets: list[dict[str, Any]] = []
    suppressed: list[dict[str, Any]] = []

    for planet_name in PLANETS:
        roles = [
            item
            for item in lord_candidates
            if item["planet"] == planet_name
        ]

        if not roles:
            continue

        roles = sorted(
            roles,
            key=lambda item: (
                item["priority_rank"],
                0 if item["house"] == "House1" else
                1 if item["house"] == "House7" else
                2 if item["house"] == "House10" else 3,
            ),
        )

        winner = roles[0]
        lord_targets.append(winner)

        for suppressed_role in roles[1:]:
            suppressed.append({
                **suppressed_role,
                "suppressed_by": winner["target_id"],
                "reason": (
                    "The same planet already represents a higher-priority "
                    "Lagna role; the book gives Lagna lords priority over "
                    "honour-house lords."
                ),
            })

    status = "Pass" if not unavailable else "Partial"

    return {
        "status": status,
        "cusp_targets": cusp_targets,
        "lord_targets": lord_targets,
        "suppressed_lord_roles": suppressed,
        "unavailable": unavailable,
    }


def calculate_tara_contacts_for_targets(
    targets: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    """Calculate all strict one-degree contacts and each target's nearest marker."""

    qualifying: list[dict[str, Any]] = []
    closest_by_target: dict[str, dict[str, Any]] = {}

    for target in targets:
        closest_candidate = None

        for tara_index, tara in enumerate(TARA_CATALOG):
            applies = (
                tara.get("applies_to_cusps", False)
                if target["target_type"] == "Cusp"
                else tara.get("applies_to_lords", False)
            )

            if not applies:
                continue

            marker = distance_to_tara_marker(
                target["longitude"],
                tara,
            )

            if marker is None:
                continue

            contact = {
                "catalog_index": tara_index,
                "nakshatra": tara["nakshatra"],
                "marker": tara["marker"],
                "book_position": tara["book_position"],
                "book_position_precision": (
                    "Book-stated sidereal degree; some table values "
                    "are explicitly rounded."
                ),
                "target_id": target["target_id"],
                "target_type": target["target_type"],
                "house": target["house"],
                "planet": target.get("planet"),
                "side": target["side"],
                "role": target["role"],
                "priority": target["priority"],
                "priority_rank": target["priority_rank"],
                "target_longitude": target["longitude"],
                "nearest_marker_longitude": round(
                    marker["nearest_marker_longitude"],
                    8,
                ),
                "marker_type": marker["marker_type"],
                "marker_range": marker.get("marker_range"),
                "angular_distance": round(
                    marker["distance"],
                    8,
                ),
                "orb_limit": TARA_ORB_DEGREES,
                "within_orb": (
                    marker["distance"] <= TARA_ORB_DEGREES + 1e-9
                ),
                "orb_margin": round(
                    TARA_ORB_DEGREES - marker["distance"],
                    8,
                ),
                "book_effect": tara_effect_for_target(
                    tara,
                    target,
                ),
                "pdf_pages": tara["pdf_pages"],
                "points_applied": False,
            }

            if (
                closest_candidate is None
                or contact["angular_distance"]
                < closest_candidate["angular_distance"]
            ):
                closest_candidate = contact

            if contact["within_orb"]:
                qualifying.append(contact)

        if closest_candidate is not None:
            closest_by_target[target["target_id"]] = {
                **closest_candidate,
                "qualifies": closest_candidate["within_orb"],
            }

    qualifying.sort(
        key=lambda item: (
            item["priority_rank"],
            0 if item["target_type"] == "House lord" else 1,
            item["angular_distance"],
            item["catalog_index"],
        )
    )

    return qualifying, closest_by_target


def compare_same_tara_sides(
    qualifying_contacts: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """
    Compare opposing-side contacts to the same tara.

    Primary house-lord testimony outranks secondary house-lord testimony.
    Where priority is equal, the closer conjunction wins. Cusp and house-lord
    contacts are not mechanically ranked against each other.
    """

    comparisons: list[dict[str, Any]] = []

    star_keys = sorted({
        (
            contact["nakshatra"],
            contact["marker"],
            contact["target_type"],
        )
        for contact in qualifying_contacts
        if contact["book_effect"]["supports"] is not None
    })

    for nakshatra, marker, target_type in star_keys:
        relevant = [
            contact
            for contact in qualifying_contacts
            if contact["nakshatra"] == nakshatra
            and contact["marker"] == marker
            and contact["target_type"] == target_type
            and contact["book_effect"]["supports"] is not None
        ]

        favourite = [
            item for item in relevant
            if item["side"] == "Favourite"
        ]
        underdog = [
            item for item in relevant
            if item["side"] == "Underdog"
        ]

        if not favourite or not underdog:
            continue

        best_favourite = min(
            favourite,
            key=lambda item: (
                item["priority_rank"],
                item["angular_distance"],
            ),
        )
        best_underdog = min(
            underdog,
            key=lambda item: (
                item["priority_rank"],
                item["angular_distance"],
            ),
        )

        if (
            target_type == "House lord"
            and best_favourite["priority_rank"]
            != best_underdog["priority_rank"]
        ):
            winning_contact = min(
                (best_favourite, best_underdog),
                key=lambda item: item["priority_rank"],
            )
            comparison_rule = (
                "Primary Lagna lord outranks secondary honour-house lord."
            )
            balanced = False
        else:
            difference = abs(
                best_favourite["angular_distance"]
                - best_underdog["angular_distance"]
            )

            if difference <= 1e-8:
                winning_contact = None
                comparison_rule = "Equal-priority contacts are equally close."
                balanced = True
            else:
                winning_contact = min(
                    (best_favourite, best_underdog),
                    key=lambda item: item["angular_distance"],
                )
                comparison_rule = (
                    "Equal-priority significators: the closer conjunction "
                    "normally prevails."
                )
                balanced = False

        comparisons.append({
            "nakshatra": nakshatra,
            "marker": marker,
            "target_type": target_type,
            "favourite_contact": best_favourite,
            "underdog_contact": best_underdog,
            "comparison_rule": comparison_rule,
            "dominant_represented_side": (
                winning_contact["side"]
                if winning_contact
                else None
            ),
            "dominant_supported_side": (
                winning_contact["book_effect"]["supports"]
                if winning_contact
                else None
            ),
            "balanced": balanced,
            "points_applied": False,
        })

    return comparisons


def detect_aldebaran_antares_cancellation(
    qualifying_contacts: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Flag the book's specific Aldebaran/Antares cusp cancellation."""

    aldebaran = [
        item for item in qualifying_contacts
        if item["marker"] == "Aldebaran"
        and item["target_type"] == "Cusp"
    ]
    antares = [
        item for item in qualifying_contacts
        if item["marker"] == "Antares"
        and item["target_type"] == "Cusp"
    ]

    cancellations = []

    for first in aldebaran:
        for second in antares:
            cancellations.append({
                "markers": ["Aldebaran", "Antares"],
                "contacts": [first, second],
                "book_rule": (
                    "When Aldebaran and Antares both sit on cusps, "
                    "the book says they more or less nullify one another."
                ),
                "status": "Cancellation testimony",
                "points_applied": False,
            })

    return cancellations


def calculate_nakshatra_taras(
    rashi_placidus: dict[str, Any],
    houses: dict[str, dict[str, Any]],
    planets: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    """
    Calculate the Chapter 8 rashi marker-star layer.

    This intentionally does not inspect D9, because the book says not to use
    taras in Navamsha. Appendix 3 Tara Balam is also kept unavailable unless
    a verified natal Moon nakshatra is supplied outside the event-chart API.
    """

    targets = build_tara_targets(
        rashi_placidus,
        houses,
        planets,
    )

    if targets["status"] == "Unavailable":
        return {
            "status": "Unavailable",
            "method": "BookLockedChapter8NakshatraTaras",
            "ayanamsa": "Lahiri",
            "chart_layer": "Rashi only",
            "targets": targets,
            "qualifying_contacts": [],
            "error": "Exact rashi targets were unavailable.",
        }

    combined_targets = (
        targets["cusp_targets"] + targets["lord_targets"]
    )
    qualifying, closest = calculate_tara_contacts_for_targets(
        combined_targets
    )

    same_tara_comparisons = compare_same_tara_sides(
        qualifying
    )
    cancellations = detect_aldebaran_antares_cancellation(
        qualifying
    )

    decision_contacts = [
        contact
        for contact in qualifying
        if contact["book_effect"]["rule_status"].startswith(
            "Book-defined"
        )
        and contact["book_effect"]["supports"] is not None
    ]
    contextual_contacts = [
        contact
        for contact in qualifying
        if contact not in decision_contacts
    ]

    catalog_summary = [
        {
            "nakshatra": tara["nakshatra"],
            "marker": tara["marker"],
            "book_position": tara["book_position"],
            "effect_class": tara["effect_class"],
            "effect": tara["effect"],
            "tier_hint": tara["tier_hint"],
            "tested_on_cusps": tara["applies_to_cusps"],
            "tested_on_house_lords": tara["applies_to_lords"],
            "pdf_pages": tara["pdf_pages"],
        }
        for tara in TARA_CATALOG
    ]

    return {
        "status": targets["status"],
        "method": "BookLockedChapter8NakshatraTaras",
        "book_chapter": 8,
        "book_layer": "Nakshatra marker stars / yogataras",
        "ayanamsa": "Lahiri",
        "chart_layer": "Rashi only",
        "navamsha_checked": False,
        "navamsha_exclusion_reason": (
            "Chapter 8 explicitly says not to use this tara technique "
            "in Navamsha."
        ),
        "orb_policy": {
            "sports_event_chart_degrees": TARA_ORB_DEGREES,
            "strict_maximum": True,
            "closer_is_stronger": True,
        },
        "position_policy": {
            "source": "Gambler's Dharma Table 8.1 and Chapter 8 prose",
            "precision": (
                "Book-stated sidereal sign-degrees; the book notes "
                "that some values are rounded."
            ),
            "dynamic_precession_applied": False,
        },
        "assignment_policy": {
            "favourite": ["House1", "House10"],
            "underdog": ["House7", "House4"],
            "primary_lords": ["House1", "House7"],
            "secondary_lords": ["House10", "House4"],
            "same_planet_priority": (
                "Lagna-lord role retained; secondary honour-lord "
                "role suppressed."
            ),
            "same_star_equal_priority": "Closer conjunction normally prevails.",
        },
        "targets": targets,
        "qualifying_contacts": qualifying,
        "decision_contacts": decision_contacts,
        "contextual_or_research_contacts": contextual_contacts,
        "closest_marker_by_target": closest,
        "same_tara_side_comparisons": same_tara_comparisons,
        "cancellations": cancellations,
        "catalog_summary": catalog_summary,
        "appendix_3_tara_balam": {
            "status": "Unavailable",
            "reason": (
                "Tara Balam requires a verified natal Moon nakshatra. "
                "An event chart alone is insufficient."
            ),
            "fabricated": False,
        },
        "interpretation_applied": "Qualitative book direction only",
        "points_applied": False,
        "points_note": (
            "No automatic signed points are assigned. Chapter 8 strength "
            "depends on star quality, exact orb and competing testimony."
        ),
        "error": (
            None
            if targets["status"] == "Pass"
            else (
                "The tara layer ran, but one or more requested target "
                "lords or cusps were unavailable."
            )
        ),
    }


def nama_pada_for_longitude(
    sidereal_longitude: float,
) -> dict[str, Any]:
    """Return the exact Table 7.1 syllable for one Lahiri D1 longitude."""

    longitude = normalise_degrees(float(sidereal_longitude))
    sign_index = int(longitude // 30.0)
    sign_name = ZODIAC_SIGNS[sign_index]
    degree_in_sign = longitude - (sign_index * 30.0)

    pada_index = int(
        degree_in_sign // NAMA_PADA_SECTION_DEGREES
    )
    pada_index = min(max(pada_index, 0), 8)

    start_degree = pada_index * NAMA_PADA_SECTION_DEGREES
    end_degree = start_degree + NAMA_PADA_SECTION_DEGREES
    syllable = NAMA_PADA_TABLE[sign_name][pada_index]
    syllable_options = [
        item.strip()
        for item in syllable.split("/")
        if item.strip()
    ]

    return {
        "sidereal_longitude": round(longitude, 8),
        "sign": sign_name,
        "degree_in_sign": round(degree_in_sign, 8),
        "navamsha_number_in_sign": pada_index + 1,
        "segment_start_degree": round(start_degree, 8),
        "segment_end_degree": round(end_degree, 8),
        "segment_width_degrees": round(
            NAMA_PADA_SECTION_DEGREES,
            8,
        ),
        "table_7_1_syllable": syllable,
        "syllable_options_iast": syllable_options,
    }


def normalize_confirmed_name_sound(value: str) -> str:
    """
    Normalize a caller-confirmed sound without deriving pronunciation from a
    raw name.
    """

    raw = str(value).strip().lower()

    # Preserve the phonetic meaning of IAST characters before stripping
    # combining marks.
    replacements = {
        "ś": "sh",
        "ṣ": "sh",
        "ṅ": "ng",
        "ñ": "ny",
        "ṭ": "t",
        "ḍ": "d",
        "ṇ": "n",
        "ṛ": "r",
        "ḷ": "l",
    }

    for source, target in replacements.items():
        raw = raw.replace(source, target)

    decomposed = unicodedata.normalize("NFKD", raw)
    without_marks = "".join(
        character
        for character in decomposed
        if not unicodedata.combining(character)
    )

    return re.sub(r"[^a-z]", "", without_marks)


def nama_pada_sound_aliases(
    syllable_value: str,
) -> list[str]:
    """
    Build only book-supported comparison aliases for a Table 7.1 syllable.

    The closure includes v/w, pa-or-pha/f, jha/z, e/ai and o/au. It also
    includes the standard English approximation of Sanskrit c as ch.
    """

    seeds = {
        normalize_confirmed_name_sound(item)
        for item in str(syllable_value).split("/")
        if normalize_confirmed_name_sound(item)
    }
    aliases = set(seeds)

    changed = True

    while changed:
        changed = False

        for sound in list(aliases):
            additions = set()

            if sound.startswith("v"):
                additions.add("w" + sound[1:])
            if sound.startswith("w"):
                additions.add("v" + sound[1:])

            if sound.startswith("ph"):
                additions.add("p" + sound[2:])
                additions.add("f" + sound[2:])
            elif sound.startswith("p"):
                additions.add("ph" + sound[1:])
                additions.add("f" + sound[1:])
            elif sound.startswith("f"):
                additions.add("p" + sound[1:])
                additions.add("ph" + sound[1:])

            if sound.startswith("jh"):
                additions.add("z" + sound[2:])
            elif sound.startswith("z"):
                additions.add("jh" + sound[1:])

            if sound.startswith("c"):
                additions.add("ch" + sound[1:])
            elif sound.startswith("ch"):
                additions.add("c" + sound[2:])

            if sound.endswith("e"):
                additions.add(sound[:-1] + "ai")
            elif sound.endswith("ai"):
                additions.add(sound[:-2] + "e")

            if sound.endswith("o"):
                additions.add(sound[:-1] + "au")
            elif sound.endswith("au"):
                additions.add(sound[:-2] + "o")

            additions = {
                item
                for item in additions
                if item and len(item) <= 12
            }

            new_items = additions - aliases

            if new_items:
                aliases.update(new_items)
                changed = True

    return sorted(aliases)


def raw_name_word_audit(name: str) -> list[dict[str, Any]]:
    """
    Return raw name words for human review.

    No word is converted into a decision-grade Sanskrit sound here.
    """

    words = re.findall(
        r"[A-Za-zÀ-ÖØ-öø-ÿĀ-ž]+",
        str(name),
    )

    return [
        {
            "word": word,
            "orthographic_normalization": (
                normalize_confirmed_name_sound(word)
            ),
            "used_as_confirmed_sound": False,
        }
        for word in words
    ]


def participant_sound_record(
    side: str,
    participant: ParticipantNameInput | None,
) -> dict[str, Any]:
    """Prepare one participant's explicit sound evidence."""

    if participant is None:
        return {
            "status": "Unavailable",
            "side": side,
            "name": None,
            "raw_name_words": [],
            "confirmed_opening_sounds": [],
            "normalized_confirmed_sounds": [],
            "error": "Participant input was not supplied.",
        }

    confirmed = [
        {
            "index": index,
            "supplied_sound": sound,
            "normalized_sound": normalize_confirmed_name_sound(
                sound
            ),
        }
        for index, sound in enumerate(
            participant.confirmed_opening_sounds
        )
        if normalize_confirmed_name_sound(sound)
    ]

    return {
        "status": "Pass" if confirmed else "NeedsSoundReview",
        "side": side,
        "name": participant.name,
        "raw_name_words": raw_name_word_audit(
            participant.name
        ),
        "confirmed_opening_sounds": list(
            participant.confirmed_opening_sounds
        ),
        "normalized_confirmed_sounds": confirmed,
        "raw_name_used_for_matching": False,
        "error": (
            None
            if confirmed
            else (
                "No caller-confirmed opening sounds were supplied. "
                "Raw spelling is retained for review but is not scored."
            )
        ),
    }


def match_participant_to_nama_pada(
    participant_record: dict[str, Any],
    pada: dict[str, Any],
) -> dict[str, Any]:
    """Compare explicit sound evidence with one exact Table 7.1 pada."""

    aliases = nama_pada_sound_aliases(
        pada["table_7_1_syllable"]
    )
    matches = []

    for sound in participant_record.get(
        "normalized_confirmed_sounds",
        [],
    ):
        if sound["normalized_sound"] in aliases:
            matches.append({
                **sound,
                "matched_alias": sound["normalized_sound"],
                "table_syllable": pada[
                    "table_7_1_syllable"
                ],
            })

    return {
        "participant_status": participant_record["status"],
        "side": participant_record["side"],
        "name": participant_record["name"],
        "table_syllable": pada["table_7_1_syllable"],
        "book_supported_aliases": aliases,
        "matches": matches,
        "match_count": len(matches),
        "matched": bool(matches),
        "multiple_name_part_exposure": len(matches) >= 2,
        "decision_grade": (
            participant_record["status"] == "Pass"
            and bool(matches)
        ),
    }


def calculate_navamsha_name_sounds(
    participants: ParticipantsInput | None,
    rashi_placidus: dict[str, Any],
    planets: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    """
    Calculate Chapter 7 nama-pada syllables and participant-name matches.

    The principal sports rule uses the exact D1 House 10 cusp. Planetary
    syllables are returned as secondary resonance evidence. Raw participant
    spelling is never silently treated as a confirmed pronunciation.
    """

    if rashi_placidus.get("status") != "Pass":
        return {
            "status": "Unavailable",
            "method": "BookLockedChapter7NamaPada",
            "ayanamsa": "Lahiri",
            "error": "Exact Lahiri Placidus cusps are unavailable.",
        }

    house10_longitude = rashi_placidus.get(
        "cusps",
        {},
    ).get(
        "House10",
        {},
    ).get("sidereal_longitude")

    if not isinstance(house10_longitude, (int, float)):
        return {
            "status": "Unavailable",
            "method": "BookLockedChapter7NamaPada",
            "ayanamsa": "Lahiri",
            "error": "Exact House 10 cusp longitude is unavailable.",
        }

    favourite_input = (
        participants.favourite
        if participants
        else None
    )
    underdog_input = (
        participants.underdog
        if participants
        else None
    )

    participant_records = {
        "Favourite": participant_sound_record(
            "Favourite",
            favourite_input,
        ),
        "Underdog": participant_sound_record(
            "Underdog",
            underdog_input,
        ),
    }

    house10_pada = nama_pada_for_longitude(
        float(house10_longitude)
    )
    house10_matches = {
        side: match_participant_to_nama_pada(
            record,
            house10_pada,
        )
        for side, record in participant_records.items()
    }

    matched_sides = [
        side
        for side, result in house10_matches.items()
        if result["decision_grade"]
    ]

    if matched_sides == ["Favourite"]:
        main_indication = "Favourite"
        main_note = (
            "The exact House 10 nama-pada matches the favourite's "
            "caller-confirmed opening sound."
        )
    elif matched_sides == ["Underdog"]:
        main_indication = "Underdog"
        main_note = (
            "The exact House 10 nama-pada matches the underdog's "
            "caller-confirmed opening sound."
        )
    elif len(matched_sides) == 2:
        main_indication = "Both / cancellation"
        main_note = (
            "Both participants have caller-confirmed sounds matching "
            "the House 10 nama-pada. No automatic winner is assigned."
        )
    else:
        main_indication = "None"
        main_note = (
            "No decision-grade participant sound matches the exact "
            "House 10 nama-pada."
        )

    maximum_exposure_sides = [
        side
        for side, result in house10_matches.items()
        if result["decision_grade"]
        and result["multiple_name_part_exposure"]
    ]

    planet_syllables: dict[str, dict[str, Any]] = {}
    planet_matches = []

    for planet_name, planet_result in planets.items():
        longitude = extract_total_degrees(
            planet_result.get("sidereal_longitude", {})
        )

        if longitude is None:
            planet_syllables[planet_name] = {
                "status": "Unavailable",
                "planet": planet_name,
                "error": "Exact sidereal longitude is unavailable.",
            }
            continue

        pada = nama_pada_for_longitude(longitude)
        planet_house = house_number_for_longitude(
            longitude,
            rashi_placidus["cusps"],
        )
        side_matches = {
            side: match_participant_to_nama_pada(
                record,
                pada,
            )
            for side, record in participant_records.items()
        }

        planet_syllables[planet_name] = {
            "status": "Pass",
            "planet": planet_name,
            "placidus_house": planet_house,
            **pada,
            "participant_matches": side_matches,
        }

        for side, match_result in side_matches.items():
            if not match_result["decision_grade"]:
                continue

            planet_matches.append({
                "planet": planet_name,
                "placidus_house": planet_house,
                "participant_side": side,
                "participant_name": match_result["name"],
                "table_syllable": pada[
                    "table_7_1_syllable"
                ],
                "matched_sounds": match_result["matches"],
                "book_strength_guidance": (
                    "Sun syllable resonance may be first- or "
                    "second-tier, subject to the whole chart."
                    if planet_name == "Sun"
                    else (
                        "Planetary name resonance is qualitative "
                        "'buzz'; Chapter 7 does not assign a fixed "
                        "automatic point value here."
                    )
                ),
                "points_applied": False,
            })

    participant_readiness = {
        side: record["status"]
        for side, record in participant_records.items()
    }
    both_sides_confirmed = all(
        status == "Pass"
        for status in participant_readiness.values()
    )

    if both_sides_confirmed:
        status = "Pass"
        error = None
    else:
        status = "Partial"
        error = (
            "Exact Table 7.1 syllables were calculated, but both "
            "participants do not yet have caller-confirmed opening sounds."
        )

    return {
        "status": status,
        "method": "BookLockedChapter7NamaPada",
        "book_chapter": 7,
        "book_layer": "Navamsha syllables / nama pada",
        "ayanamsa": "Lahiri",
        "source_chart": "Exact D1 rashi longitude divided into 3°20' sections",
        "interpretation_applied": (
            "Only caller-confirmed sounds are used for participant matching."
        ),
        "raw_name_pronunciation_inferred": False,
        "table_7_1": {
            "section_width_degrees": round(
                NAMA_PADA_SECTION_DEGREES,
                8,
            ),
            "total_sections": 108,
            "pdf_pages": NAMA_PADA_PDF_PAGES,
        },
        "book_sound_rules": {
            **NAMA_PADA_BOOK_SUBSTITUTIONS,
            "raw_spelling_alone_is_decision_grade": False,
        },
        "participants": participant_records,
        "house10_main_test": {
            "status": (
                "Pass"
                if both_sides_confirmed
                else "Partial"
            ),
            "cusp": "House10",
            "tier": 3,
            "book_point_range_if_manually_applied": [
                14,
                18,
            ],
            "exact_pada": house10_pada,
            "participant_matches": house10_matches,
            "matched_sides": matched_sides,
            "indication": main_indication,
            "note": main_note,
            "maximum_exposure_sides": (
                maximum_exposure_sides
            ),
            "maximum_exposure_note": (
                "Matching more than one initial name part may justify "
                "the upper end of the third-tier range, but this layer "
                "does not assign an automatic exact score."
                if maximum_exposure_sides
                else None
            ),
            "points_applied": False,
        },
        "planet_syllables": planet_syllables,
        "planet_resonance_matches": planet_matches,
        "participant_readiness": participant_readiness,
        "name_comparison_allowed": both_sides_confirmed,
        "points_applied": False,
        "error": error,
    }


def compact_scalar_text(value: Any, limit: int = 240) -> Any:
    """Keep scalar values readable without carrying oversized error text."""

    if isinstance(value, str) and len(value) > limit:
        return value[:limit] + "…"

    return value


def compact_recursive(
    value: Any,
    *,
    depth: int = 0,
    list_limit: int = 20,
    string_limit: int = 240,
) -> Any:
    """Bound arbitrary nested evidence without changing scalar meaning."""

    if depth >= 7:
        return limit_data(value, 180)

    if isinstance(value, dict):
        output: dict[str, Any] = {}

        for key, item in value.items():
            if key in {
                "raw_data",
                "raw_calculation",
                "longitude_result",
                "upstream",
                "catalog_summary",
                "significator_matrix",
            }:
                continue

            if key == "data":
                output[key] = limit_data(item, 180)
                continue

            output[key] = compact_recursive(
                item,
                depth=depth + 1,
                list_limit=list_limit,
                string_limit=string_limit,
            )

        return output

    if isinstance(value, (list, tuple)):
        items = list(value)
        compacted = [
            compact_recursive(
                item,
                depth=depth + 1,
                list_limit=list_limit,
                string_limit=string_limit,
            )
            for item in items[:list_limit]
        ]

        if len(items) > list_limit:
            compacted.append({
                "items_omitted": len(items) - list_limit,
            })

        return compacted

    if isinstance(value, str):
        return compact_scalar_text(value, string_limit)

    return json_safe(value)


def compact_calculation_result(
    result: Any,
    data_limit: int = 160,
) -> Any:
    """Compact one VedAstro CalculationResult while retaining audit fields."""

    if not isinstance(result, dict):
        return compact_recursive(result)

    output: dict[str, Any] = {}

    priority_keys = (
        "status",
        "required",
        "method",
        "attempt",
        "attempts",
        "ayanamsa_requested",
        "error",
    )

    for key in priority_keys:
        if key in result and result[key] is not None:
            output[key] = compact_scalar_text(result[key], 220)

    for key, value in result.items():
        if key in output or key in {
            "data",
            "details",
            "raw_data",
            "raw_calculation",
            "failures",
        }:
            continue

        if isinstance(value, (str, int, float, bool)) or value is None:
            output[key] = compact_scalar_text(value, 180)
        elif (
            isinstance(value, list)
            and len(value) <= 12
            and all(
                isinstance(item, (str, int, float, bool))
                or item is None
                for item in value
            )
        ):
            output[key] = value

    if "data" in result:
        output["data"] = limit_data(
            result["data"],
            data_limit,
        )

    if result.get("status") != "Pass" and "details" in result:
        output["details"] = compact_recursive(
            result["details"],
            list_limit=8,
            string_limit=180,
        )

    return output


def compact_value_result(
    result: Any,
    data_limit: int = 90,
) -> Any:
    """
    Keep the returned value and validation status without repeating the
    upstream method wrapper for every successful field.
    """

    if not isinstance(result, dict):
        return compact_recursive(
            result,
            list_limit=8,
            string_limit=140,
        )

    output: dict[str, Any] = {
        "status": result.get("status"),
    }

    if "data" in result:
        output["value"] = limit_data(
            result["data"],
            data_limit,
        )

    # Preserve scalar validation facts that are not stored in data.
    for key, value in result.items():
        if key in {
            "status",
            "data",
            "required",
            "attempt",
            "attempts",
            "method",
            "details",
            "raw_data",
            "raw_calculation",
            "failures",
        }:
            continue

        if isinstance(value, (str, int, float, bool)) or value is None:
            output[key] = compact_scalar_text(value, 140)
        elif (
            isinstance(value, list)
            and len(value) <= 8
            and all(
                isinstance(item, (str, int, float, bool))
                or item is None
                for item in value
            )
        ):
            output[key] = value

    if result.get("status") != "Pass":
        output["method"] = result.get("method")
        output["error"] = compact_scalar_text(
            result.get("error"),
            180,
        )

    return output


def compact_core_results(
    core: dict[str, Any],
) -> dict[str, Any]:
    return {
        key: compact_value_result(value, 110)
        for key, value in core.items()
    }


def compact_house_results(
    houses: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    output: dict[str, Any] = {}

    for house_name, result in houses.items():
        output[house_name] = {
            "status": result.get("status"),
            "sign": compact_value_result(
                result.get("sign", {}),
                70,
            ),
            "lord": compact_value_result(
                result.get("lord", {}),
                70,
            ),
            "constellation": compact_value_result(
                result.get("constellation", {}),
                70,
            ),
            "constellation_lord": compact_value_result(
                result.get("constellation_lord", {}),
                70,
            ),
            "aspects": compact_value_result(
                result.get("aspects", {}),
                90,
            ),
        }

    return output


def compact_planet_results(
    planets: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    output: dict[str, Any] = {}

    for planet_name, result in planets.items():
        compacted: dict[str, Any] = {
            "status": result.get("status"),
            "planet": result.get("planet", planet_name),
        }

        for key in (
            "d1_sign",
            "d9_sign",
            "sidereal_longitude",
            "motion",
            "retrograde",
            "combust",
            "exalted",
            "debilitated",
            "own_sign",
            "moolatrikona",
            "shadbala",
            "sign_longitude_consistency",
        ):
            if key in result:
                compacted[key] = compact_value_result(
                    result[key],
                    80,
                )

        output[planet_name] = compacted

    return output


def compact_rashi_placidus(
    layer: dict[str, Any],
) -> dict[str, Any]:
    return compact_recursive({
        "status": layer.get("status"),
        "method": layer.get("method"),
        "ayanamsa": layer.get("ayanamsa"),
        "house_system": layer.get("house_system"),
        "source_array_shape": layer.get("source_array_shape"),
        "cusps": layer.get("cusps", {}),
        "axis_validation": layer.get("axis_validation", []),
        "error": layer.get("error"),
    })


def compact_navamsha_layer(
    layer: dict[str, Any],
) -> dict[str, Any]:
    planets = {}

    for planet_name, result in layer.get("planets", {}).items():
        planets[planet_name] = {
            key: compact_recursive(value)
            for key, value in result.items()
            if key in {
                "planet",
                "d1_sidereal_longitude",
                "d1_sign",
                "d1_degree_in_sign",
                "navamsha_number_in_d1_sign",
                "d9_sidereal_longitude",
                "d9_sign",
                "d9_degree_in_sign",
                "vedastro_d9_sign",
                "vedastro_sign_match",
                "distances",
                "nearest_d9_cusp",
                "nearest_distance",
                "orb_limit",
                "nearest_within_orb",
            }
        }

    return compact_recursive({
        "status": layer.get("status"),
        "method": layer.get("method"),
        "ayanamsa": layer.get("ayanamsa"),
        "book_layer": layer.get("book_layer"),
        "interpretation_applied": layer.get(
            "interpretation_applied"
        ),
        "degree_method": layer.get("degree_method"),
        "orb_policy": layer.get("orb_policy"),
        "lagna": layer.get("lagna"),
        "seventh_cusp": layer.get("seventh_cusp"),
        "axis_validation": layer.get("axis_validation"),
        "planets": planets,
        "qualifying_contacts": layer.get(
            "qualifying_contacts",
            [],
        ),
        "closest_planet_by_cusp": layer.get(
            "closest_planet_by_cusp",
            {},
        ),
        "unavailable_planets": layer.get(
            "unavailable_planets",
            [],
        ),
        "failed_validations": layer.get(
            "failed_validations",
            [],
        ),
        "optional_house_sign_cross_check": layer.get(
            "optional_house_sign_cross_check"
        ),
        "error": layer.get("error"),
    })


def compact_kp_layer(
    layer: dict[str, Any],
) -> dict[str, Any]:
    return compact_recursive({
        "status": layer.get("status"),
        "method": layer.get("method"),
        "book_chapter": layer.get("book_chapter"),
        "book_tier": layer.get("book_tier"),
        "ayanamsa": layer.get("ayanamsa"),
        "requested_payload_ayanamsa": layer.get(
            "requested_payload_ayanamsa"
        ),
        "standard_chart_ayanamsa_unchanged": layer.get(
            "standard_chart_ayanamsa_unchanged"
        ),
        "scoring_policy": layer.get("scoring_policy"),
        "cusp_sublords": layer.get("cusp_sublords", {}),
        "main_sublord_comparison": layer.get(
            "main_sublord_comparison",
            {},
        ),
        "sublord_array": layer.get("sublord_array", {}),
        "failed_planets": layer.get("failed_planets", []),
        "missing_planet_houses": layer.get(
            "missing_planet_houses",
            [],
        ),
        "missing_requested_planets": layer.get(
            "missing_requested_planets",
            [],
        ),
        "error": layer.get("error"),
        "omitted_from_action_response": [
            "full significator_matrix",
            "duplicate KP planet_positions",
            "duplicate full KP cusp table",
        ],
    }, list_limit=24)


def compact_outer_planets_layer(
    layer: dict[str, Any],
) -> dict[str, Any]:
    bodies: dict[str, Any] = {}

    for body_name, body in layer.get("bodies", {}).items():
        bodies[body_name] = compact_recursive({
            "status": body.get("status"),
            "body": body.get("body", body_name),
            "engine": body.get("engine"),
            "ayanamsa": body.get("ayanamsa"),
            "sidereal_longitude": body.get(
                "sidereal_longitude"
            ),
            "sign": body.get("sign"),
            "degree_in_sign": body.get("degree_in_sign"),
            "daily_motion_longitude": body.get(
                "daily_motion_longitude"
            ),
            "motion": body.get("motion"),
            "retrograde": body.get("retrograde"),
            "placidus_house": body.get("placidus_house"),
            "nearest_sensitive_cusp": body.get(
                "nearest_sensitive_cusp"
            ),
            "nearest_sensitive_cusp_distance": body.get(
                "nearest_sensitive_cusp_distance"
            ),
            "qualifying_contacts": body.get(
                "qualifying_contacts",
                [],
            ),
            "d9_position_raw": body.get("d9_position_raw"),
            "ephemeris_mode": body.get("ephemeris_mode"),
            "missing_ephemeris_file": body.get(
                "missing_ephemeris_file"
            ),
            "error": body.get("error"),
        })

    return compact_recursive({
        "status": layer.get("status"),
        "method": layer.get("method"),
        "book_chapter": layer.get("book_chapter"),
        "book_layer": layer.get("book_layer"),
        "engine": layer.get("engine"),
        "ayanamsa": layer.get("ayanamsa"),
        "orb_policy": layer.get("orb_policy"),
        "points_applied": layer.get("points_applied"),
        "bodies": bodies,
        "available_bodies": layer.get(
            "available_bodies",
            [],
        ),
        "unavailable_bodies": layer.get(
            "unavailable_bodies",
            [],
        ),
        "qualifying_contacts": layer.get(
            "qualifying_contacts",
            [],
        ),
        "closest_body_by_cusp": layer.get(
            "closest_body_by_cusp",
            {},
        ),
        "error": layer.get("error"),
    })


def compact_special_points_layer(
    layer: dict[str, Any],
) -> dict[str, Any]:
    points: dict[str, Any] = {}

    for point_name, point in layer.get("points", {}).items():
        points[point_name] = compact_recursive({
            "status": point.get("status"),
            "point": point.get("point", point_name),
            "engine": point.get("engine"),
            "ayanamsa": point.get("ayanamsa"),
            "upstream_method": point.get("upstream_method"),
            "sidereal_longitude": point.get(
                "sidereal_longitude"
            ),
            "sign": point.get("sign"),
            "degree_in_sign": point.get("degree_in_sign"),
            "placidus_house": point.get("placidus_house"),
            "rashi": {
                "orb_limit": point.get("rashi", {}).get(
                    "orb_limit"
                ),
                "nearest_sensitive_cusp": point.get(
                    "rashi",
                    {},
                ).get("nearest_sensitive_cusp"),
                "nearest_distance": point.get(
                    "rashi",
                    {},
                ).get("nearest_distance"),
                "qualifying_contacts": point.get(
                    "rashi",
                    {},
                ).get("qualifying_contacts", []),
            },
            "d9": {
                "position": point.get("d9", {}).get(
                    "position"
                ),
                "orb_limit": point.get("d9", {}).get(
                    "orb_limit"
                ),
                "nearest_cusp": point.get("d9", {}).get(
                    "nearest_cusp"
                ),
                "nearest_distance": point.get("d9", {}).get(
                    "nearest_distance"
                ),
                "qualifying_contacts": point.get(
                    "d9",
                    {},
                ).get("qualifying_contacts", []),
            },
            "error": point.get("error"),
        })

    return compact_recursive({
        "status": layer.get("status"),
        "method": layer.get("method"),
        "book_chapters": layer.get("book_chapters"),
        "ayanamsa": layer.get("ayanamsa"),
        "house_system": layer.get("house_system"),
        "orb_policy": layer.get("orb_policy"),
        "points": points,
        "gulika_house_lord_contacts": layer.get(
            "gulika_house_lord_contacts",
            {},
        ),
        "qualifying_rashi_contacts": layer.get(
            "qualifying_rashi_contacts",
            [],
        ),
        "qualifying_d9_contacts": layer.get(
            "qualifying_d9_contacts",
            [],
        ),
        "available_points": layer.get(
            "available_points",
            [],
        ),
        "unavailable_points": layer.get(
            "unavailable_points",
            [],
        ),
        "points_applied": layer.get("points_applied"),
        "error": layer.get("error"),
    })


def compact_tier1_combinations_layer(
    layer: dict[str, Any],
) -> dict[str, Any]:
    """Preserve all decision-bearing Chapter 3 testimony compactly."""

    snapshots = {}

    for planet_name, record in layer.get(
        "planet_snapshots",
        {},
    ).items():
        snapshots[planet_name] = {
            "status": record.get("status"),
            "house": record.get("whole_sign_house"),
            "side": record.get("victory_side"),
            "retrograde": record.get("retrograde"),
            "combust": record.get("combust"),
            "exalted": record.get("exalted"),
            "debilitated": record.get("debilitated"),
            "own_sign": record.get("own_sign"),
            "dig_bala": record.get("dig_bala"),
            "own_nakshatra": record.get(
                "own_nakshatra"
            ),
            "error": record.get("error"),
        }

    victory = layer.get("victory_houses", {})
    sky_pky = layer.get("sky_pky", {})
    parivartana = layer.get("parivartana", {})
    war = layer.get("planetary_war", {})

    return compact_recursive({
        "status": layer.get("status"),
        "method": layer.get("method"),
        "book_chapter": layer.get("book_chapter"),
        "ayanamsa": layer.get("ayanamsa"),
        "house_system": layer.get("house_system"),
        "assignment": layer.get("assignment"),
        "ascendant": layer.get("ascendant"),
        "planet_snapshots": snapshots,
        "victory_houses": {
            "status": victory.get("status"),
            "ledger": victory.get("ledger", []),
            "manual_candidates": victory.get(
                "manual_candidates",
                [],
            ),
            "unavailable_planets": victory.get(
                "unavailable_planets",
                [],
            ),
            "favourite_points": victory.get(
                "favourite_points"
            ),
            "underdog_points": victory.get(
                "underdog_points"
            ),
            "signed_favourite_total": victory.get(
                "signed_favourite_total"
            ),
            "automatic_point_scope": victory.get(
                "automatic_point_scope"
            ),
        },
        "sky_pky": {
            "status": sky_pky.get("status"),
            "book_tier": sky_pky.get("book_tier"),
            "sides": sky_pky.get("sides", {}),
            "points_applied": sky_pky.get(
                "points_applied"
            ),
        },
        "parivartana": {
            "status": parivartana.get("status"),
            "detected": parivartana.get("detected"),
            "pairs": parivartana.get("pairs", []),
            "eligible_benefics": parivartana.get(
                "eligible_benefics",
                [],
            ),
            "points_applied": parivartana.get(
                "points_applied"
            ),
        },
        "planetary_war": {
            "status": war.get("status"),
            "orb_degrees": war.get("orb_degrees"),
            "relevant_house_lords": war.get(
                "relevant_house_lords",
                {},
            ),
            "wars": war.get("wars", []),
            "detected": war.get("detected"),
            "winner_standard": war.get(
                "winner_standard"
            ),
            "lesser_longitude_fallback_used": (
                war.get(
                    "lesser_longitude_fallback_used"
                )
            ),
            "points_applied": war.get(
                "points_applied"
            ),
            "time_error": war.get("time_error"),
        },
        "automatic_signed_total": layer.get(
            "automatic_signed_total"
        ),
        "automatic_signed_total_scope": layer.get(
            "automatic_signed_total_scope"
        ),
        "missing_required_planets": layer.get(
            "missing_required_planets",
            [],
        ),
        "manual_review_items": layer.get(
            "manual_review_items"
        ),
        "pdf_pages": layer.get("pdf_pages"),
        "points_applied": layer.get("points_applied"),
        "error": layer.get("error"),
    }, list_limit=24, string_limit=220)


def compact_navamsha_interpretation_layer(
    layer: dict[str, Any],
) -> dict[str, Any]:
    """Preserve decision-bearing Chapter 5 results and v1.19 audits."""

    concise_d9_contacts = []

    for contact in (layer.get("d9_cusp_contacts") or []):
        effect = contact.get("book_effect") or {}
        concise_d9_contacts.append({
            "body": contact.get("body"),
            "category": contact.get("category"),
            "cusp": contact.get("cusp"),
            "angular_distance": contact.get(
                "angular_distance"
            ),
            "orb_limit": contact.get("orb_limit"),
            "motion": contact.get("motion"),
            "direction": (
                effect.get("direction")
                or contact.get("direction")
            ),
            "supports": (
                effect.get("supports")
                or contact.get("supports")
            ),
            "decision_eligible": effect.get(
                "decision_eligible"
            ),
            "research_only": effect.get(
                "research_only"
            ),
            "automatic_decision_use": effect.get(
                "automatic_decision_use"
            ),
            "decision_reason": effect.get(
                "decision_reason"
            ),
            "orb_strength": effect.get(
                "orb_strength"
            ),
            "reliability": (
                effect.get("reliability")
                or contact.get("reliability")
            ),
            "book_point_range": (
                effect.get("book_point_range")
                or contact.get("book_point_range")
            ),
            "signed_interval": effect.get(
                "signed_interval"
            ),
            "exact_points_applied": False,
        })

    concise_d1_contacts = []

    for contact in (layer.get("d1_cusp_contacts") or []):
        effect = contact.get("book_effect") or {}
        concise_d1_contacts.append({
            "body": contact.get("body"),
            "category": contact.get("category"),
            "cusp": contact.get("cusp"),
            "effective_cusp": contact.get(
                "effective_cusp"
            ),
            "axis": contact.get("axis"),
            "angular_distance": contact.get(
                "angular_distance"
            ),
            "orb_limit": contact.get("orb_limit"),
            "direction": (
                effect.get("direction")
                or contact.get("direction")
            ),
            "supports": (
                effect.get("supports")
                or contact.get("supports")
            ),
            "decision_eligible": effect.get(
                "decision_eligible"
            ),
            "research_only": effect.get(
                "research_only"
            ),
            "automatic_decision_use": effect.get(
                "automatic_decision_use"
            ),
            "decision_reason": effect.get(
                "decision_reason"
            ),
            "book_point_range": effect.get(
                "book_point_range"
            ),
            "signed_interval": effect.get(
                "signed_interval"
            ),
            "orb_strength": effect.get(
                "orb_strength"
            ),
            "stolen_type": (
                effect.get("stolen_type")
                or contact.get("stolen_type")
            ),
            "contact_strength": (
                effect.get("contact_strength")
                or contact.get("contact_strength")
            ),
            "node_axis_duplicate": effect.get(
                "node_axis_duplicate"
            ),
            "node_axis_group": effect.get(
                "node_axis_group"
            ),
            "duplicate_of": effect.get(
                "duplicate_of"
            ),
        })

    combos = layer.get("navamsha_combinations") or {}
    combo_items = combos.get("combinations") or []
    concise_combos = [
        {
            key: item.get(key)
            for key in (
                "planets",
                "d9_house",
                "represented_side",
                "effect_for_represented_side",
                "supports",
                "rule_grade",
                "book_points",
                "raw_signed_favourite_points",
                "signed_favourite_points",
                "points_candidate",
                "points_applied",
                "overlap_cluster_id",
                "overlap_suppressed",
                "suppressed_reason",
                "manual_review_required",
                "pdf_pages",
            )
            if key in item
        }
        for item in combo_items
        if isinstance(item, dict)
    ]

    return compact_recursive({
        "status": layer.get("status"),
        "method": layer.get("method"),
        "assignment": layer.get("assignment"),
        "tier_hierarchy": layer.get("tier_hierarchy"),
        "decision_grade_policy": layer.get(
            "decision_grade_policy"
        ),
        "d9_cusp_contacts": concise_d9_contacts,
        "d9_cusp_summary": layer.get(
            "d9_cusp_summary"
        ),
        "navamsha_combinations": {
            "status": combos.get("status"),
            "houses": combos.get("houses"),
            "combinations": concise_combos,
            "raw_signed_favourite_total": combos.get(
                "raw_signed_favourite_total"
            ),
            "signed_favourite_total": combos.get(
                "signed_favourite_total"
            ),
            "indication": combos.get("indication"),
            "overlap_clusters": combos.get(
                "overlap_clusters",
                [],
            ),
            "overlapping_pair_policy": combos.get(
                "overlapping_pair_policy"
            ),
            "unavailable_planets": combos.get(
                "unavailable_planets",
                [],
            ),
            "error": combos.get("error"),
        },
        "d1_cusp_contacts": concise_d1_contacts,
        "d1_cusp_summary": layer.get(
            "d1_cusp_summary"
        ),
        "d1_summary": layer.get("d1_summary"),
        "d9_summary": layer.get("d9_summary"),
        "d1_d9_relationship": layer.get(
            "d1_d9_relationship"
        ),
        "double_whammy": layer.get("double_whammy"),
        "node_axis_deduplication": layer.get(
            "node_axis_deduplication"
        ),
        "signed_points": layer.get("signed_points"),
        "unavailable_d9_bodies": layer.get(
            "unavailable_d9_bodies",
            [],
        ),
        "optional_body_coverage_status": layer.get(
            "optional_body_coverage_status"
        ),
        "research_or_undefined_d1_contacts": [
            {
                "body": item.get("body"),
                "cusp": item.get("cusp"),
                "reason": (
                    (item.get("book_effect") or {}).get(
                        "decision_reason"
                    )
                    or item.get("reason")
                ),
            }
            for item in (
                layer.get(
                    "research_or_undefined_d1_contacts",
                    [],
                )
                or []
            )
            if isinstance(item, dict)
        ],
        "research_or_undefined_d9_contacts": [
            {
                "body": item.get("body"),
                "cusp": item.get("cusp"),
                "reason": (
                    (item.get("book_effect") or {}).get(
                        "decision_reason"
                    )
                    or (item.get("book_effect") or {}).get("note")
                    or item.get("reason")
                ),
            }
            for item in (
                layer.get(
                    "research_or_undefined_d9_contacts",
                    [],
                )
                or []
            )
            if isinstance(item, dict)
        ],
        "completeness": layer.get("completeness"),
        "pdf_pages": layer.get("pdf_pages"),
        "points_applied": layer.get("points_applied"),
        "error": layer.get("error"),
    }, list_limit=24, string_limit=180)


def compact_reliability_audit_layer(
    layer: dict[str, Any],
) -> dict[str, Any]:
    """
    Preserve the reliability decision across any number of compact passes.

    Raw and already-compacted key names are both accepted. The original
    station/evidence totals survive later projections.
    """

    stationary = layer.get("stationary_kutila") or {}
    station_source_rows = (
        stationary.get("swiss_station_search")
        or stationary.get("station_search")
        or []
    )
    hard_veto_planets = set(
        stationary.get("hard_veto_planets") or []
    )
    warning_planets = set(
        stationary.get("warning_planets") or []
    )

    original_station_total = stationary.get(
        "station_search_total_count"
    )

    if not isinstance(original_station_total, int):
        previous_omitted = stationary.get(
            "station_rows_omitted_from_transport",
            0,
        )
        previous_omitted = (
            previous_omitted
            if isinstance(previous_omitted, int)
            else 0
        )
        original_station_total = (
            len([
                row
                for row in station_source_rows
                if (
                    isinstance(row, dict)
                    and row.get("body")
                )
            ])
            + previous_omitted
        )

    def first_value(
        record: dict[str, Any],
        *keys: str,
    ) -> Any:
        for key in keys:
            if record.get(key) is not None:
                return record.get(key)

        return None

    def station_distance(
        row: dict[str, Any],
    ) -> float:
        nearest = row.get("nearest_station") or {}
        value = first_value(
            nearest,
            "absolute_days_from_event",
            "absolute_days",
        )

        if isinstance(value, (int, float)):
            return float(value)

        return 999.0

    valid_station_rows = [
        row
        for row in station_source_rows
        if (
            isinstance(row, dict)
            and row.get("body")
        )
    ]
    ordered_stations = sorted(
        valid_station_rows,
        key=lambda row: (
            0
            if (
                row.get("automatic_veto")
                or row.get("body") in hard_veto_planets
            )
            else 1
            if (
                row.get("within_seven_days")
                or row.get("body") in warning_planets
            )
            else 2,
            station_distance(row),
            str(row.get("body") or ""),
        ),
    )

    retained_stations: list[dict[str, Any]] = []
    retained_bodies: set[str] = set()

    for row in ordered_stations:
        body = str(row.get("body") or "")

        if (
            row.get("automatic_veto")
            or body in hard_veto_planets
        ):
            retained_stations.append(row)
            retained_bodies.add(body)

    for row in ordered_stations:
        body = str(row.get("body") or "")

        if body in retained_bodies:
            continue

        if len(retained_stations) >= 6:
            break

        retained_stations.append(row)
        retained_bodies.add(body)

    concise_stations: list[dict[str, Any]] = []

    for row in retained_stations:
        nearest = row.get("nearest_station") or {}
        concise_nearest = None

        if nearest:
            concise_nearest = {
                "local": nearest.get("local"),
                "signed_days": first_value(
                    nearest,
                    "signed_days_from_event",
                    "signed_days",
                ),
                "absolute_days": first_value(
                    nearest,
                    "absolute_days_from_event",
                    "absolute_days",
                ),
            }

        concise_stations.append({
            "body": row.get("body"),
            "status": row.get("status"),
            "event_speed": first_value(
                row,
                "event_speed_degrees_per_day",
                "event_speed",
            ),
            "event_motion": row.get("event_motion"),
            "nearest_station": concise_nearest,
            "same_local_date": first_value(
                row,
                "same_local_calendar_date",
                "same_local_date",
            ),
            "within_one_day": row.get("within_one_day"),
            "within_seven_days": row.get(
                "within_seven_days"
            ),
            "automatic_veto": row.get(
                "automatic_veto"
            ),
            "error": compact_scalar_text(
                row.get("error"),
                70,
            ),
        })

    omitted_station_count = max(
        0,
        original_station_total - len(
            concise_stations
        ),
    )

    eclipses = layer.get("eclipses") or {}
    concise_eclipses = []

    for event in eclipses.get("events") or []:
        if (
            not isinstance(event, dict)
            or not event.get("kind")
        ):
            continue

        concise_eclipses.append({
            "status": event.get("status"),
            "kind": event.get("kind"),
            "direction": event.get("direction"),
            "type_flags": event.get("type_flags"),
            "major": first_value(
                event,
                "major_for_automatic_gate",
                "major",
            ),
            "maximum_local": event.get(
                "maximum_local"
            ),
            "signed_days": first_value(
                event,
                "signed_days_from_event",
                "signed_days",
            ),
            "absolute_days": first_value(
                event,
                "absolute_days_from_event",
                "absolute_days",
            ),
            "error": compact_scalar_text(
                event.get("error"),
                70,
            ),
        })

    original_eclipse_total = eclipses.get(
        "event_total_count"
    )

    if not isinstance(original_eclipse_total, int):
        original_eclipse_total = (
            len(concise_eclipses)
            + int(
                eclipses.get(
                    "event_rows_omitted_from_transport",
                    0,
                )
                or 0
            )
        )

    nearest_major = eclipses.get(
        "nearest_major_eclipse"
    )
    concise_nearest_major = None

    if isinstance(nearest_major, dict):
        concise_nearest_major = {
            "kind": nearest_major.get("kind"),
            "direction": nearest_major.get(
                "direction"
            ),
            "type_flags": nearest_major.get(
                "type_flags"
            ),
            "maximum_local": nearest_major.get(
                "maximum_local"
            ),
            "signed_days": first_value(
                nearest_major,
                "signed_days_from_event",
                "signed_days",
            ),
            "absolute_days": first_value(
                nearest_major,
                "absolute_days_from_event",
                "absolute_days",
            ),
        }

    sankranti = layer.get("solar_sankranti") or {}
    concise_sankranti = {
        "status": sankranti.get("status"),
        "sign": sankranti.get("sign"),
        "degree_in_sign": sankranti.get(
            "degree_in_sign"
        ),
        "end_of_sign_29_degrees_or_higher": (
            sankranti.get(
                "end_of_sign_29_degrees_or_higher"
            )
        ),
        "beginning_of_sign_under_one_degree": (
            sankranti.get(
                "beginning_of_sign_under_one_degree"
            )
        ),
        "hard_veto": sankranti.get("hard_veto"),
        "hard_veto_reason": compact_scalar_text(
            sankranti.get("hard_veto_reason"),
            100,
        ),
        "beginning_warning": compact_scalar_text(
            sankranti.get("beginning_warning"),
            100,
        ),
        "error": compact_scalar_text(
            sankranti.get("error"),
            70,
        ),
    }

    rise_set = layer.get("sunrise_sunset") or {}

    def concise_solar_event(
        record: Any,
    ) -> dict[str, Any] | None:
        if not isinstance(record, dict):
            return None

        return {
            "local": record.get("local"),
            "signed_minutes": first_value(
                record,
                "signed_minutes_from_event",
                "signed_minutes",
            ),
            "absolute_minutes": first_value(
                record,
                "absolute_minutes_from_event",
                "absolute_minutes",
            ),
        }

    concise_rise_set = {
        "status": rise_set.get("status"),
        "nearest_sunrise": concise_solar_event(
            rise_set.get("nearest_sunrise")
        ),
        "nearest_sunset": concise_solar_event(
            rise_set.get("nearest_sunset")
        ),
        "hard_veto": rise_set.get("hard_veto"),
        "automatic_window_applied": rise_set.get(
            "automatic_window_applied"
        ),
        "manual_review_required": rise_set.get(
            "manual_review_required"
        ),
        "error": compact_scalar_text(
            rise_set.get("error"),
            70,
        ),
    }

    karma = layer.get("karma_fixity") or {}
    karma_source = karma.get("evidence") or []
    original_evidence_total = karma.get(
        "evidence_total_count"
    )

    if not isinstance(original_evidence_total, int):
        original_evidence_total = len(karma_source)

    concise_evidence = []

    for item in karma_source[:8]:
        if not isinstance(item, dict):
            continue

        concise_evidence.append({
            key: item.get(key)
            for key in (
                "source",
                "family",
                "tier",
                "supports",
                "value",
                "condition",
                "body",
                "cusp",
                "planets",
                "independence_key",
                "overlap_cluster_id",
            )
            if key in item
        })

    motion_rows = []

    for row in (
        stationary.get("vedastro_motion_labels")
        or []
    ):
        if not isinstance(row, dict):
            continue

        label = row.get("vedastro_motion_label")
        planet = row.get("planet")

        if (
            row.get("kutila_or_stationary")
            or planet in hard_veto_planets
            or label not in {
                None,
                "Direct",
                "Retrograde",
            }
        ):
            motion_rows.append({
                "planet": planet,
                "vedastro_motion_label": label,
                "classification": row.get(
                    "classification"
                ),
                "strict_book_veto": (
                    row.get("strict_book_veto")
                    if row.get("strict_book_veto") is not None
                    else row.get("kutila_or_stationary")
                ),
                "practical_hard_veto": row.get(
                    "practical_hard_veto"
                ),
                "practical_warning": row.get(
                    "practical_warning"
                ),
            })

    return compact_recursive({
        "status": layer.get("status"),
        "policy_mode": layer.get("policy_mode"),
        "strict_book_hard_veto": layer.get(
            "strict_book_hard_veto"
        ),
        "strict_book_prediction_allowed": layer.get(
            "strict_book_prediction_allowed"
        ),
        "strict_book_hard_veto_reasons": layer.get(
            "strict_book_hard_veto_reasons",
            [],
        ),
        "practical_hard_veto": layer.get(
            "practical_hard_veto"
        ),
        "practical_prediction_allowed": layer.get(
            "practical_prediction_allowed"
        ),
        "practical_hard_veto_reasons": layer.get(
            "practical_hard_veto_reasons",
            [],
        ),
        "hard_veto": layer.get("hard_veto"),
        "strict_prediction_allowed_by_reliability": (
            layer.get(
                "strict_prediction_allowed_by_reliability"
            )
        ),
        "decision": layer.get("decision"),
        "confidence_cap": layer.get("confidence_cap"),
        "performance_fallback_recommended": layer.get(
            "performance_fallback_recommended"
        ),
        "market_assignment_note": compact_scalar_text(
            layer.get("market_assignment_note"),
            150,
        ),
        "hard_veto_reasons": layer.get(
            "hard_veto_reasons",
            [],
        ),
        "warning_reasons": layer.get(
            "warning_reasons",
            [],
        ),
        "stationary_kutila": {
            "status": stationary.get("status"),
            "vedastro_motion_labels": motion_rows,
            "station_search": concise_stations,
            "station_search_total_count": (
                original_station_total
            ),
            "station_rows_omitted_from_transport": (
                omitted_station_count
            ),
            "same_local_date_station_planets": stationary.get(
                "same_local_date_station_planets",
                [],
            ),
            "strict_book_hard_veto_planets": stationary.get(
                "strict_book_hard_veto_planets",
                [],
            ),
            "practical_hard_veto_planets": stationary.get(
                "practical_hard_veto_planets",
                [],
            ),
            "practical_warning_planets": stationary.get(
                "practical_warning_planets",
                [],
            ),
            "hard_veto_planets": sorted(
                hard_veto_planets
            ),
            "hard_veto": stationary.get("hard_veto"),
            "confidence_cap": stationary.get(
                "confidence_cap"
            ),
            "warning_planets": sorted(
                warning_planets
            ),
            "error": compact_scalar_text(
                stationary.get("error"),
                90,
            ),
        },
        "eclipses": {
            "status": eclipses.get("status"),
            "events": concise_eclipses,
            "event_total_count": original_eclipse_total,
            "event_rows_omitted_from_transport": max(
                0,
                original_eclipse_total
                - len(concise_eclipses),
            ),
            "nearest_major_eclipse": (
                concise_nearest_major
            ),
            "avoid_window_days_each_side": (
                eclipses.get(
                    "avoid_window_days_each_side"
                )
            ),
            "hard_veto": eclipses.get("hard_veto"),
            "error": compact_scalar_text(
                eclipses.get("error"),
                80,
            ),
        },
        "solar_sankranti": concise_sankranti,
        "sunrise_sunset": concise_rise_set,
        "karma_fixity": {
            "status": karma.get("status"),
            "evidence": concise_evidence,
            "evidence_total_count": (
                original_evidence_total
            ),
            "counts": karma.get("counts"),
            "family_direction": karma.get(
                "family_direction"
            ),
            "rule_of_three_reached": karma.get(
                "rule_of_three_reached"
            ),
            "rule_of_three_basis": karma.get(
                "rule_of_three_basis"
            ),
            "automatic_karma_classification": None,
            "automatic_classification_allowed": False,
            "manual_classification_required": True,
            "reason": compact_scalar_text(
                karma.get("reason"),
                110,
            ),
        },
        "unavailable_sublayers": layer.get(
            "unavailable_sublayers",
            [],
        ),
        "partial_sublayers": layer.get(
            "partial_sublayers",
            [],
        ),
        "pdf_pages": layer.get("pdf_pages"),
        "points_applied": False,
        "error": compact_scalar_text(
            layer.get("error"),
            90,
        ),
    }, list_limit=12, string_limit=110)

def compact_stolen_cusps_layer(
    layer: dict[str, Any],
) -> dict[str, Any]:
    """Preserve the complete decision-bearing stolen-cusp audit."""

    return compact_recursive({
        "status": layer.get("status"),
        "method": layer.get("method"),
        "book_chapter": layer.get("book_chapter"),
        "book_tier": layer.get("book_tier"),
        "pdf_pages": layer.get("pdf_pages"),
        "ayanamsa": layer.get("ayanamsa"),
        "house_system": layer.get("house_system"),
        "whole_sign_reference": layer.get(
            "whole_sign_reference"
        ),
        "book_rules": layer.get("book_rules"),
        "orb_policy": layer.get("orb_policy"),
        "audit_summary": layer.get("audit_summary"),
        "stolen_cusps": layer.get(
            "stolen_cusps",
            [],
        ),
        "qualifying_contacts": layer.get(
            "qualifying_contacts",
            [],
        ),
        "dormant_stolen_cusps": layer.get(
            "dormant_stolen_cusps",
            [],
        ),
        "available_body_count": layer.get(
            "available_body_count"
        ),
        "unavailable_bodies": layer.get(
            "unavailable_bodies",
            [],
        ),
        "coverage_status": layer.get(
            "coverage_status"
        ),
        "interpretation_applied": layer.get(
            "interpretation_applied"
        ),
        "winner_direction_inferred": layer.get(
            "winner_direction_inferred"
        ),
        "points_applied": layer.get("points_applied"),
        "error": layer.get("error"),
    }, list_limit=24, string_limit=220)


def compact_tara_layer(
    layer: dict[str, Any],
) -> dict[str, Any]:
    return compact_recursive({
        "status": layer.get("status"),
        "method": layer.get("method"),
        "book_chapter": layer.get("book_chapter"),
        "book_layer": layer.get("book_layer"),
        "ayanamsa": layer.get("ayanamsa"),
        "chart_layer": layer.get("chart_layer"),
        "navamsha_checked": layer.get("navamsha_checked"),
        "navamsha_exclusion_reason": layer.get(
            "navamsha_exclusion_reason"
        ),
        "orb_policy": layer.get("orb_policy"),
        "assignment_policy": layer.get(
            "assignment_policy"
        ),
        "targets": layer.get("targets"),
        "qualifying_contacts": layer.get(
            "qualifying_contacts",
            [],
        ),
        "decision_contacts": layer.get(
            "decision_contacts",
            [],
        ),
        "contextual_or_research_contacts": layer.get(
            "contextual_or_research_contacts",
            [],
        ),
        "closest_marker_by_target": layer.get(
            "closest_marker_by_target",
            {},
        ),
        "same_tara_side_comparisons": layer.get(
            "same_tara_side_comparisons",
            [],
        ),
        "cancellations": layer.get("cancellations", []),
        "appendix_3_tara_balam": layer.get(
            "appendix_3_tara_balam"
        ),
        "points_applied": layer.get("points_applied"),
        "error": layer.get("error"),
        "catalog_omitted_from_action_response": True,
    }, list_limit=16)


def compact_name_match(
    match: dict[str, Any],
) -> dict[str, Any]:
    return compact_recursive({
        "participant_status": match.get(
            "participant_status"
        ),
        "side": match.get("side"),
        "name": match.get("name"),
        "table_syllable": match.get(
            "table_syllable"
        ),
        "matches": match.get("matches", []),
        "match_count": match.get("match_count"),
        "matched": match.get("matched"),
        "multiple_name_part_exposure": match.get(
            "multiple_name_part_exposure"
        ),
        "decision_grade": match.get(
            "decision_grade"
        ),
    })


def compact_name_sounds_layer(
    layer: dict[str, Any],
) -> dict[str, Any]:
    participants = {}

    for side, record in layer.get("participants", {}).items():
        participants[side] = compact_recursive({
            "status": record.get("status"),
            "side": record.get("side", side),
            "name": record.get("name"),
            "confirmed_opening_sounds": record.get(
                "confirmed_opening_sounds",
                [],
            ),
            "normalized_confirmed_sounds": record.get(
                "normalized_confirmed_sounds",
                [],
            ),
            "raw_name_used_for_matching": record.get(
                "raw_name_used_for_matching"
            ),
            "error": record.get("error"),
        })

    main_test = layer.get("house10_main_test", {})
    participant_matches = {
        side: compact_name_match(match)
        for side, match in main_test.get(
            "participant_matches",
            {},
        ).items()
    }

    planet_syllables = {}

    for planet_name, result in layer.get(
        "planet_syllables",
        {},
    ).items():
        planet_syllables[planet_name] = compact_recursive({
            "status": result.get("status"),
            "planet": result.get("planet", planet_name),
            "placidus_house": result.get(
                "placidus_house"
            ),
            "sidereal_longitude": result.get(
                "sidereal_longitude"
            ),
            "sign": result.get("sign"),
            "degree_in_sign": result.get(
                "degree_in_sign"
            ),
            "navamsha_number_in_sign": result.get(
                "navamsha_number_in_sign"
            ),
            "table_7_1_syllable": result.get(
                "table_7_1_syllable"
            ),
            "error": result.get("error"),
        })

    return compact_recursive({
        "status": layer.get("status"),
        "method": layer.get("method"),
        "book_chapter": layer.get("book_chapter"),
        "book_layer": layer.get("book_layer"),
        "ayanamsa": layer.get("ayanamsa"),
        "source_chart": layer.get("source_chart"),
        "raw_name_pronunciation_inferred": layer.get(
            "raw_name_pronunciation_inferred"
        ),
        "table_7_1": layer.get("table_7_1"),
        "participants": participants,
        "house10_main_test": {
            "status": main_test.get("status"),
            "cusp": main_test.get("cusp"),
            "tier": main_test.get("tier"),
            "book_point_range_if_manually_applied": (
                main_test.get(
                    "book_point_range_if_manually_applied"
                )
            ),
            "exact_pada": main_test.get("exact_pada"),
            "participant_matches": participant_matches,
            "matched_sides": main_test.get(
                "matched_sides",
                [],
            ),
            "indication": main_test.get("indication"),
            "note": main_test.get("note"),
            "maximum_exposure_sides": main_test.get(
                "maximum_exposure_sides",
                [],
            ),
            "points_applied": main_test.get(
                "points_applied"
            ),
        },
        "planet_syllables": planet_syllables,
        "planet_resonance_matches": layer.get(
            "planet_resonance_matches",
            [],
        ),
        "participant_readiness": layer.get(
            "participant_readiness",
            {},
        ),
        "name_comparison_allowed": layer.get(
            "name_comparison_allowed"
        ),
        "points_applied": layer.get("points_applied"),
        "error": layer.get("error"),
    }, list_limit=20)


def tighten_value_record(
    record: Any,
    value_limit: int = 55,
) -> Any:
    """Compress an already compact successful value record."""

    if not isinstance(record, dict):
        return limit_data(record, value_limit)

    output: dict[str, Any] = {
        "status": record.get("status"),
    }

    if "value" in record:
        value = record["value"]

        if (
            isinstance(value, dict)
            and value.get("response_compacted")
            and isinstance(value.get("preview"), str)
        ):
            output["value_preview"] = value["preview"][
                :value_limit
            ]
        else:
            output["value"] = limit_data(
                value,
                value_limit,
            )

    if record.get("status") != "Pass":
        output["method"] = record.get("method")
        output["error"] = compact_scalar_text(
            record.get("error"),
            120,
        )

    return output


def tighten_house_results(
    houses: dict[str, Any],
) -> dict[str, Any]:
    """Keep the emergency house fields needed for contest analysis."""

    output: dict[str, Any] = {}

    for house_name, result in houses.items():
        output[house_name] = {
            "status": result.get("status"),
            "sign": tighten_value_record(
                result.get("sign", {}),
                45,
            ),
            "lord": tighten_value_record(
                result.get("lord", {}),
                45,
            ),
            "aspects": tighten_value_record(
                result.get("aspects", {}),
                60,
            ),
        }

    return output


def tighten_planet_results(
    planets: dict[str, Any],
) -> dict[str, Any]:
    """Keep every material planet condition in a smaller value envelope."""

    output: dict[str, Any] = {}

    material_keys = (
        "d1_sign",
        "d9_sign",
        "sidereal_longitude",
        "motion",
        "retrograde",
        "combust",
        "exalted",
        "debilitated",
        "own_sign",
        "moolatrikona",
        "shadbala",
    )

    for planet_name, result in planets.items():
        planet_output: dict[str, Any] = {
            "status": result.get("status"),
        }

        for key in material_keys:
            if key in result:
                planet_output[key] = tighten_value_record(
                    result[key],
                    55,
                )

        output[planet_name] = planet_output

    return output


def action_flatten_value(
    value: Any,
    limit: int = 90,
) -> Any:
    """Flatten common VedAstro value objects to their meaningful scalar."""

    if isinstance(value, dict):
        if (
            value.get("response_compacted")
            and isinstance(value.get("preview"), str)
        ):
            return value["preview"][:limit]

        preferred_keys = (
            "Name",
            "name",
            "TotalDegrees",
            "total_degrees",
            "Value",
            "value",
            "IsRetrograde",
            "IsCombust",
            "IsExalted",
            "IsDebilitated",
            "IsOwnSign",
            "IsMoolatrikona",
            "Motion",
            "motion",
        )

        for key in preferred_keys:
            candidate = value.get(key)

            if isinstance(candidate, (str, int, float, bool)) or candidate is None:
                if key in value:
                    return compact_scalar_text(
                        candidate,
                        limit,
                    )

        scalar_items = {
            key: compact_scalar_text(item, limit)
            for key, item in value.items()
            if isinstance(item, (str, int, float, bool))
            or item is None
        }

        if scalar_items and len(scalar_items) <= 4:
            return scalar_items

        encoded = json.dumps(
            json_safe(value),
            ensure_ascii=False,
            default=str,
        )
        return encoded[:limit] + (
            "…" if len(encoded) > limit else ""
        )

    if isinstance(value, (list, tuple)):
        items = list(value)

        if all(
            isinstance(item, (str, int, float, bool))
            or item is None
            for item in items
        ):
            return items[:8]

        encoded = json.dumps(
            json_safe(items),
            ensure_ascii=False,
            default=str,
        )
        return encoded[:limit] + (
            "…" if len(encoded) > limit else ""
        )

    return compact_scalar_text(value, limit)


def action_direct_value(
    record: Any,
    limit: int = 90,
) -> Any:
    """
    Convert a successful calculation wrapper into a direct transport value.

    Validation status is retained at the parent object. Repeated per-field
    method/status wrappers are the largest avoidable source of Action payload
    growth.
    """

    if not isinstance(record, dict):
        return action_flatten_value(record, limit)

    if "value" in record:
        return action_flatten_value(
            record["value"],
            limit,
        )

    if "value_preview" in record:
        return compact_scalar_text(
            record.get("value_preview"),
            limit,
        )

    if "data" in record:
        return action_flatten_value(
            record["data"],
            limit,
        )

    if (
        record.get("response_compacted")
        and isinstance(record.get("preview"), str)
    ):
        return record["preview"][:limit]

    return action_flatten_value(record, limit)


def action_compact_houses(
    houses: dict[str, Any],
) -> dict[str, Any]:
    """Retain required house values without repeated wrappers."""

    output: dict[str, Any] = {}

    for house_name, result in houses.items():
        entry = {
            "status": result.get("status"),
            "sign": action_direct_value(
                result.get("sign"),
                55,
            ),
            "lord": action_direct_value(
                result.get("lord"),
                55,
            ),
            "constellation": action_direct_value(
                result.get("constellation"),
                55,
            ),
            "constellation_lord": action_direct_value(
                result.get("constellation_lord"),
                55,
            ),
            "aspects": action_direct_value(
                result.get("aspects"),
                85,
            ),
        }

        if entry["status"] != "Pass":
            entry["error"] = compact_scalar_text(
                result.get("error"),
                120,
            )

        output[house_name] = entry

    return output


def action_compact_planets(
    planets: dict[str, Any],
) -> dict[str, Any]:
    """Retain all material D1/D9 and dignity fields directly."""

    output: dict[str, Any] = {}
    material_keys = (
        "d1_sign",
        "d9_sign",
        "sidereal_longitude",
        "motion",
        "retrograde",
        "combust",
        "exalted",
        "debilitated",
        "own_sign",
        "moolatrikona",
        "shadbala",
        "sign_longitude_consistency",
    )

    for planet_name, result in planets.items():
        entry: dict[str, Any] = {
            "status": result.get("status"),
        }

        for key in material_keys:
            if key in result:
                entry[key] = action_direct_value(
                    result[key],
                    65 if key != "shadbala" else 85,
                )

        if entry["status"] != "Pass":
            entry["error"] = compact_scalar_text(
                result.get("error"),
                120,
            )

        output[planet_name] = entry

    return output


def action_compact_core(
    core: dict[str, Any],
) -> dict[str, Any]:
    """Retain essential validation calculations as direct values."""

    output: dict[str, Any] = {}

    for key, record in core.items():
        status = (
            record.get("status")
            if isinstance(record, dict)
            else None
        )
        entry = {
            "status": status,
            "value": action_direct_value(record, 90),
        }

        if status not in {None, "Pass"}:
            entry["error"] = compact_scalar_text(
                record.get("error"),
                120,
            )

        output[key] = entry

    return output


def action_compact_rashi(
    layer: dict[str, Any],
) -> dict[str, Any]:
    """Keep exact cusp geometry and axis validation only."""

    cusps: dict[str, Any] = {}

    for cusp_name, cusp in layer.get("cusps", {}).items():
        cusps[cusp_name] = {
            key: cusp.get(key)
            for key in (
                "house",
                "sidereal_longitude",
                "sign",
                "degree_in_sign",
            )
            if key in cusp
        }

    return {
        "status": layer.get("status"),
        "ayanamsa": layer.get("ayanamsa"),
        "house_system": layer.get("house_system"),
        "cusps": cusps,
        "axis_validation": compact_recursive(
            layer.get("axis_validation", []),
            list_limit=8,
            string_limit=100,
        ),
        "error": compact_scalar_text(
            layer.get("error"),
            120,
        ),
    }


def action_compact_tier1(
    layer: dict[str, Any],
) -> dict[str, Any]:
    """Keep the signed ledger and yoga decisions without duplicated prose."""

    victory = layer.get("victory_houses", {})
    concise_ledger = []

    for item in victory.get("ledger", []):
        concise_ledger.append({
            key: item.get(key)
            for key in (
                "planet",
                "house",
                "side",
                "natural_class",
                "strength_sources",
                "debilitated",
                "combust",
                "points",
                "signed_points",
            )
            if key in item
        })
        concise_ledger[-1]["eligibility"] = (
            compact_scalar_text(
                item.get("eligibility"),
                100,
            )
        )

    manual_candidates = []

    for item in victory.get("manual_candidates", []):
        manual_candidates.append({
            "planet": item.get("planet"),
            "side": item.get("side"),
            "house": item.get("house"),
            "reason": compact_scalar_text(
                item.get("reason"),
                120,
            ),
            "strength_sources": item.get(
                "strength_sources",
                [],
            ),
            "automatic_points": item.get(
                "automatic_points"
            ),
        })

    sky_pky = layer.get("sky_pky", {})
    concise_sides: dict[str, Any] = {}

    for side, side_result in sky_pky.get("sides", {}).items():
        sky = side_result.get("sky", {})
        pky = side_result.get("pky", {})

        concise_sides[side] = {
            "target_house": side_result.get("target_house"),
            "flanking_houses": side_result.get(
                "flanking_houses"
            ),
            "flanking_occupancy": side_result.get(
                "flanking_occupancy"
            ),
            "sky": {
                "formed": sky.get("formed"),
                "condition": sky.get("condition"),
                "benefics_previous_side": sky.get(
                    "benefics_previous_side"
                ),
                "benefics_next_side": sky.get(
                    "benefics_next_side"
                ),
                "mild_or_shadow_marring": sky.get(
                    "mild_or_shadow_marring"
                ),
                "heavy_marring": sky.get(
                    "heavy_marring"
                ),
                "debilitated_benefics": sky.get(
                    "debilitated_benefics"
                ),
                "automatic_points_applied": False,
            },
            "pky": {
                "formed": pky.get("formed"),
                "condition": pky.get("condition"),
                "classical_malefics_previous_side": pky.get(
                    "classical_malefics_previous_side"
                ),
                "classical_malefics_next_side": pky.get(
                    "classical_malefics_next_side"
                ),
                "nodes_previous_side": pky.get(
                    "nodes_previous_side"
                ),
                "nodes_next_side": pky.get(
                    "nodes_next_side"
                ),
                "intensified_by_nodes": pky.get(
                    "intensified_by_nodes"
                ),
                "automatic_points_applied": False,
            },
            "mixed_testimony": side_result.get(
                "cancellation_or_mixed_testimony"
            ),
        }

    parivartana = layer.get("parivartana", {})
    concise_pairs = []

    for pair in parivartana.get("pairs", []):
        concise_pairs.append({
            "planets": pair.get("planets"),
            "first": pair.get("first"),
            "second": pair.get("second"),
            "victory_house_relevance": pair.get(
                "victory_house_relevance"
            ),
            "especially_relevant": pair.get(
                "especially_relevant"
            ),
            "fixed_points_defined": False,
        })

    war = layer.get("planetary_war", {})
    concise_wars = []

    for item in war.get("wars", []):
        concise_wars.append({
            key: item.get(key)
            for key in (
                "planets",
                "angular_distance",
                "within_one_degree",
                "first_roles",
                "second_roles",
                "cross_side_war",
                "winner",
                "loser",
                "winner_represented_sides",
                "loser_represented_sides",
                "fixed_points_defined",
                "points_applied",
            )
            if key in item
        })

    return {
        "status": layer.get("status"),
        "assignment": layer.get("assignment"),
        "ascendant": layer.get("ascendant"),
        "victory_houses": {
            "status": victory.get("status"),
            "ledger": concise_ledger,
            "manual_candidates": manual_candidates,
            "unavailable_planets": victory.get(
                "unavailable_planets",
                [],
            ),
            "favourite_points": victory.get(
                "favourite_points"
            ),
            "underdog_points": victory.get(
                "underdog_points"
            ),
            "signed_favourite_total": victory.get(
                "signed_favourite_total"
            ),
        },
        "sky_pky": {
            "status": sky_pky.get("status"),
            "sides": concise_sides,
            "points_applied": False,
        },
        "parivartana": {
            "status": parivartana.get("status"),
            "detected": parivartana.get("detected"),
            "pairs": concise_pairs,
            "eligible_benefics": parivartana.get(
                "eligible_benefics",
                [],
            ),
            "points_applied": False,
        },
        "planetary_war": {
            "status": war.get("status"),
            "detected": war.get("detected"),
            "orb_degrees": war.get("orb_degrees"),
            "wars": concise_wars,
            "winner_standard": war.get(
                "winner_standard"
            ),
            "lesser_longitude_fallback_used": war.get(
                "lesser_longitude_fallback_used"
            ),
            "points_applied": False,
            "time_error": compact_scalar_text(
                war.get("time_error"),
                100,
            ),
        },
        "automatic_signed_total": layer.get(
            "automatic_signed_total"
        ),
        "missing_required_planets": layer.get(
            "missing_required_planets",
            [],
        ),
        "error": compact_scalar_text(
            layer.get("error"),
            120,
        ),
    }


def action_compact_d9(
    layer: dict[str, Any],
) -> dict[str, Any]:
    planets: dict[str, Any] = {}

    for planet_name, record in layer.get("planets", {}).items():
        planets[planet_name] = {
            key: record.get(key)
            for key in (
                "d1_sidereal_longitude",
                "d1_sign",
                "navamsha_number_in_d1_sign",
                "d9_sidereal_longitude",
                "d9_sign",
                "d9_degree_in_sign",
                "vedastro_d9_sign",
                "vedastro_sign_match",
                "nearest_d9_cusp",
                "nearest_distance",
                "orb_limit",
                "nearest_within_orb",
            )
            if key in record
        }

    return {
        "status": layer.get("status"),
        "ayanamsa": layer.get("ayanamsa"),
        "lagna": layer.get("lagna"),
        "seventh_cusp": layer.get("seventh_cusp"),
        "axis_validation": layer.get("axis_validation"),
        "planets": planets,
        "qualifying_contacts": compact_recursive(
            layer.get("qualifying_contacts", []),
            list_limit=16,
            string_limit=110,
        ),
        "unavailable_planets": layer.get(
            "unavailable_planets",
            [],
        ),
        "failed_validations": layer.get(
            "failed_validations",
            [],
        ),
        "error": compact_scalar_text(
            layer.get("error"),
            120,
        ),
    }


def action_compact_kp(
    layer: dict[str, Any],
) -> dict[str, Any]:
    cusp_sublords = layer.get("cusp_sublords", {})
    key_cusps = {
        house: cusp_sublords.get(house)
        for house in (
            "House1",
            "House4",
            "House6",
            "House7",
            "House10",
            "House12",
        )
        if house in cusp_sublords
    }

    sublord_array = layer.get("sublord_array", {})
    concise_array = {
        "status": sublord_array.get("status"),
        "pointers": compact_recursive(
            sublord_array.get("pointers", {}),
            list_limit=12,
            string_limit=100,
        ),
        "signed_favourite_total": sublord_array.get(
            "signed_favourite_total"
        ),
        "error": compact_scalar_text(
            sublord_array.get("error"),
            100,
        ),
    }

    return {
        "status": layer.get("status"),
        "ayanamsa": layer.get("ayanamsa"),
        "book_tier": layer.get("book_tier"),
        "cusp_sublords": key_cusps,
        "main_sublord_comparison": compact_recursive(
            layer.get("main_sublord_comparison", {}),
            list_limit=12,
            string_limit=110,
        ),
        "sublord_array": concise_array,
        "failed_planets": layer.get(
            "failed_planets",
            [],
        ),
        "missing_planet_houses": layer.get(
            "missing_planet_houses",
            [],
        ),
        "error": compact_scalar_text(
            layer.get("error"),
            120,
        ),
    }


def action_compact_outer(
    layer: dict[str, Any],
) -> dict[str, Any]:
    bodies: dict[str, Any] = {}

    for body_name, body in layer.get("bodies", {}).items():
        bodies[body_name] = {
            key: body.get(key)
            for key in (
                "status",
                "sidereal_longitude",
                "sign",
                "degree_in_sign",
                "motion",
                "retrograde",
                "placidus_house",
                "nearest_sensitive_cusp",
                "nearest_sensitive_cusp_distance",
                "missing_ephemeris_file",
                "error",
            )
            if key in body
        }

    return {
        "status": layer.get("status"),
        "ayanamsa": layer.get("ayanamsa"),
        "bodies": bodies,
        "qualifying_contacts": compact_recursive(
            layer.get("qualifying_contacts", []),
            list_limit=12,
            string_limit=100,
        ),
        "available_bodies": layer.get(
            "available_bodies",
            [],
        ),
        "unavailable_bodies": layer.get(
            "unavailable_bodies",
            [],
        ),
        "error": compact_scalar_text(
            layer.get("error"),
            120,
        ),
    }


def action_compact_special(
    layer: dict[str, Any],
) -> dict[str, Any]:
    points: dict[str, Any] = {}

    for point_name, point in layer.get("points", {}).items():
        rashi = point.get("rashi", {})
        d9 = point.get("d9", {})

        points[point_name] = {
            "status": point.get("status"),
            "sidereal_longitude": point.get(
                "sidereal_longitude"
            ),
            "sign": point.get("sign"),
            "degree_in_sign": point.get(
                "degree_in_sign"
            ),
            "placidus_house": point.get(
                "placidus_house"
            ),
            "rashi": {
                key: rashi.get(key)
                for key in (
                    "orb_limit",
                    "nearest_sensitive_cusp",
                    "nearest_distance",
                )
                if key in rashi
            },
            "d9": {
                key: d9.get(key)
                for key in (
                    "position",
                    "orb_limit",
                    "nearest_cusp",
                    "nearest_distance",
                )
                if key in d9
            },
            "error": compact_scalar_text(
                point.get("error"),
                100,
            ),
        }

    return {
        "status": layer.get("status"),
        "points": points,
        "gulika_house_lord_contacts": compact_recursive(
            layer.get("gulika_house_lord_contacts", {}),
            list_limit=5,
            string_limit=80,
        ),
        "qualifying_rashi_contacts": compact_contact_rows(
            layer.get("qualifying_rashi_contacts", []),
            maximum=8,
        ),
        "qualifying_d9_contacts": compact_contact_rows(
            layer.get("qualifying_d9_contacts", []),
            maximum=8,
        ),
        "unavailable_points": layer.get(
            "unavailable_points",
            [],
        ),
        "error": compact_scalar_text(
            layer.get("error"),
            120,
        ),
    }


def action_compact_stolen(
    layer: dict[str, Any],
) -> dict[str, Any]:
    stolen = []

    for cusp in layer.get("stolen_cusps", []):
        classification = cusp.get("classification", {})

        stolen.append({
            "cusp": cusp.get("cusp"),
            "sidereal_longitude": cusp.get(
                "sidereal_longitude"
            ),
            "whole_sign_house": cusp.get(
                "whole_sign_house"
            ),
            "signed_house_shift": cusp.get(
                "signed_house_shift"
            ),
            "stolen_type": classification.get(
                "stolen_type"
            ),
            "rule_status": classification.get(
                "rule_status"
            ),
            "effective_represented_side": (
                classification.get(
                    "effective_represented_side"
                )
            ),
            "transformation": classification.get(
                "transformation"
            ),
        })

    contacts = []

    for item in layer.get("qualifying_contacts", []):
        contacts.append({
            key: item.get(key)
            for key in (
                "body",
                "body_category",
                "cusp",
                "source_house_number",
                "whole_sign_house_number",
                "stolen_type",
                "angular_distance",
                "orb_limit",
                "book_effect",
                "points_applied",
            )
            if key in item
        })

    return {
        "status": layer.get("status"),
        "audit_summary": layer.get("audit_summary"),
        "stolen_cusps": compact_recursive(
            stolen,
            list_limit=12,
            string_limit=100,
        ),
        "qualifying_contacts": compact_recursive(
            contacts,
            list_limit=12,
            string_limit=100,
        ),
        "unavailable_bodies": layer.get(
            "unavailable_bodies",
            [],
        ),
        "coverage_status": layer.get(
            "coverage_status"
        ),
        "points_applied": False,
        "error": compact_scalar_text(
            layer.get("error"),
            120,
        ),
    }


def compact_contact_rows(
    rows: Any,
    maximum: int = 8,
) -> list[Any]:
    """Keep the event-bearing fields of contact rows with an omission count."""

    if not isinstance(rows, list):
        return []

    compacted: list[Any] = []
    preferred_keys = (
        "marker",
        "body",
        "planet",
        "point",
        "target",
        "target_id",
        "target_type",
        "cusp",
        "house",
        "side",
        "supports",
        "afflicts",
        "stolen_type",
        "angular_distance",
        "distance",
        "orb_limit",
        "within_orb",
        "book_effect",
        "decision_grade",
        "status",
        "reason",
    )

    for row in rows[:maximum]:
        if isinstance(row, dict):
            compacted.append({
                key: compact_recursive(
                    row.get(key),
                    list_limit=4,
                    string_limit=80,
                )
                for key in preferred_keys
                if key in row
            })
        else:
            compacted.append(
                compact_scalar_text(row, 80)
            )

    if len(rows) > maximum:
        compacted.append({
            "items_omitted_from_transport": (
                len(rows) - maximum
            ),
            "full_calculation_performed": True,
        })

    return compacted


def action_compact_taras(
    layer: dict[str, Any],
) -> dict[str, Any]:
    return {
        "status": layer.get("status"),
        "qualifying_contacts": compact_contact_rows(
            layer.get("qualifying_contacts", []),
            maximum=8,
        ),
        "decision_contacts": compact_contact_rows(
            layer.get("decision_contacts", []),
            maximum=8,
        ),
        "contextual_or_research_contacts": compact_contact_rows(
            layer.get(
                "contextual_or_research_contacts",
                [],
            ),
            maximum=4,
        ),
        "same_tara_side_comparisons": compact_contact_rows(
            layer.get("same_tara_side_comparisons", []),
            maximum=4,
        ),
        "cancellations": compact_contact_rows(
            layer.get("cancellations", []),
            maximum=4,
        ),
        "error": compact_scalar_text(
            layer.get("error"),
            120,
        ),
    }


def action_compact_names(
    layer: dict[str, Any],
) -> dict[str, Any]:
    syllables: dict[str, Any] = {}

    for planet_name, record in layer.get(
        "planet_syllables",
        {},
    ).items():
        syllables[planet_name] = {
            "house": record.get("placidus_house"),
            "syllable": record.get(
                "table_7_1_syllable"
            ),
            "status": record.get("status"),
        }

    return {
        "status": layer.get("status"),
        "participants": compact_recursive(
            layer.get("participants", {}),
            list_limit=8,
            string_limit=100,
        ),
        "house10_main_test": compact_recursive(
            layer.get("house10_main_test", {}),
            list_limit=10,
            string_limit=110,
        ),
        "planet_syllables": syllables,
        "planet_resonance_matches": compact_recursive(
            layer.get("planet_resonance_matches", []),
            list_limit=12,
            string_limit=100,
        ),
        "name_comparison_allowed": layer.get(
            "name_comparison_allowed"
        ),
        "error": compact_scalar_text(
            layer.get("error"),
            120,
        ),
    }


def action_compact_v2(
    compacted: dict[str, Any],
) -> dict[str, Any]:
    """
    Deterministic prediction-grade payload designed for Custom GPT Actions.

    It keeps every layer and all active testimony while removing repeated
    wrappers, prose and nonqualifying reference tables.
    """

    result = {
        "status": compacted.get("status"),
        "strict_prediction_allowed": compacted.get(
            "strict_prediction_allowed"
        ),
        "essential_failures": compacted.get(
            "essential_failures",
            [],
        ),
        "event": compact_recursive(
            compacted.get("event", {}),
            list_limit=8,
            string_limit=120,
        ),
        "core": action_compact_core(
            compacted.get("core", {})
        ),
        "rashi_placidus": action_compact_rashi(
            compacted.get("rashi_placidus", {})
        ),
        "tier1_combinations": action_compact_tier1(
            compacted.get("tier1_combinations", {})
        ),
        "planet_cusp_contacts": {
            "status": compacted.get(
                "planet_cusp_contacts",
                {},
            ).get("status"),
            "orb_policy": compacted.get(
                "planet_cusp_contacts",
                {},
            ).get("orb_policy"),
            "qualifying_contacts": compact_recursive(
                compacted.get(
                    "planet_cusp_contacts",
                    {},
                ).get("qualifying_contacts", []),
                list_limit=16,
                string_limit=100,
            ),
        },
        "navamsha_cusps": action_compact_d9(
            compacted.get("navamsha_cusps", {})
        ),
        "kp_sublords": action_compact_kp(
            compacted.get("kp_sublords", {})
        ),
        "outer_planets": action_compact_outer(
            compacted.get("outer_planets", {})
        ),
        "special_points": action_compact_special(
            compacted.get("special_points", {})
        ),
        "stolen_cusps": action_compact_stolen(
            compacted.get("stolen_cusps", {})
        ),
        "navamsha_interpretation": (
            compact_navamsha_interpretation_layer(
                compacted.get("navamsha_interpretation", {})
            )
        ),
        "chart_correlation": compact_recursive(
            compacted.get("chart_correlation", {}),
            list_limit=6,
            string_limit=120,
        ),
        "reliability_audit": compact_reliability_audit_layer(
            compacted.get("reliability_audit", {})
        ),
        "nakshatra_taras": action_compact_taras(
            compacted.get("nakshatra_taras", {})
        ),
        "navamsha_name_sounds": action_compact_names(
            compacted.get("navamsha_name_sounds", {})
        ),
        "houses": action_compact_houses(
            compacted.get("houses", {})
        ),
        "planets": action_compact_planets(
            compacted.get("planets", {})
        ),
        "provenance": {
            "engine": compacted.get(
                "provenance",
                {},
            ).get("engine"),
            "proxy_version": PROXY_VERSION,
            "standard_ayanamsa": "Lahiri",
            "kp_ayanamsa": "Krishnamurti",
            "response_profile": "prediction-grade compact v2; v1.19 aggregation",
        },
        "response_compaction": {
            "applied": True,
            "profile": "action-compact-v2",
            "target_characters": (
                ACTION_RESPONSE_TARGET_CHARACTERS
            ),
            "full_calculation_performed": True,
            "all_advanced_layers_retained": True,
            "nonqualifying_reference_tables_omitted": True,
        },
    }

    return result


def first_non_null_numeric(
    record: dict[str, Any],
    *keys: str,
    default: float = 999.0,
) -> float:
    for key in keys:
        value = record.get(key)

        if isinstance(value, (int, float)):
            return float(value)

    return float(default)


def enforce_action_response_limit(
    payload: dict[str, Any],
) -> dict[str, Any]:
    """Apply deterministic low-priority trims until the target is met."""

    def encoded_size() -> int:
        return len(json.dumps(
            payload,
            ensure_ascii=False,
            default=str,
        ))

    if encoded_size() <= ACTION_RESPONSE_PAYLOAD_TARGET_CHARACTERS:
        return payload

    # 1. House aspects are already represented in dedicated geometry layers.
    for house in payload.get("houses", {}).values():
        house.pop("aspects", None)

    payload["response_compaction"][
        "house_aspects_omitted"
    ] = True

    if encoded_size() <= ACTION_RESPONSE_PAYLOAD_TARGET_CHARACTERS:
        return payload

    # 2. Shadbala remains available in the full server calculation but is not
    # assigned a fabricated automatic threshold.
    for planet in payload.get("planets", {}).values():
        planet.pop("shadbala", None)
        planet.pop("sign_longitude_consistency", None)

    payload["response_compaction"][
        "raw_shadbala_omitted"
    ] = True

    if encoded_size() <= ACTION_RESPONSE_PAYLOAD_TARGET_CHARACTERS:
        return payload

    # 3. D9 nearest nonqualifying distances are redundant when the actual
    # qualifying-contact list is retained.
    for planet in payload.get(
        "navamsha_cusps",
        {},
    ).get("planets", {}).values():
        planet.pop("nearest_distance", None)
        planet.pop("nearest_d9_cusp", None)
        planet.pop("orb_limit", None)
        planet.pop("nearest_within_orb", None)

    payload["response_compaction"][
        "nonqualifying_d9_distances_omitted"
    ] = True

    if encoded_size() <= ACTION_RESPONSE_PAYLOAD_TARGET_CHARACTERS:
        return payload

    # 4. D1 cusp rows are already represented by the D1 summary and the
    # original cusp/stolen-cusp layers. Keep only the closest rows if needed.
    nav_layer = payload.get(
        "navamsha_interpretation",
        {},
    )
    d1_rows = nav_layer.get("d1_cusp_contacts", [])

    if isinstance(d1_rows, list) and len(d1_rows) > 6:
        nav_layer["d1_cusp_contacts"] = (
            d1_rows[:6]
            + [{
                "items_omitted_from_transport": (
                    len(d1_rows) - 6
                ),
                "full_calculation_performed": True,
            }]
        )
        payload["response_compaction"][
            "secondary_d1_d9_rows_trimmed"
        ] = True

    if encoded_size() <= ACTION_RESPONSE_PAYLOAD_TARGET_CHARACTERS:
        return payload

    # 5. Reliability veto summaries remain complete; long non-veto station
    # rows may be represented by the closest six bodies when hard trimming.
    reliability = payload.get("reliability_audit", {})
    station_rows = reliability.get(
        "stationary_kutila",
        {},
    ).get("station_search", [])

    if isinstance(station_rows, list) and len(station_rows) > 6:
        station_rows = sorted(
            station_rows,
            key=lambda item: (
                0 if item.get("automatic_veto") else 1,
                first_non_null_numeric(
                    item.get("nearest_station") or {},
                    "absolute_days_from_event",
                    "absolute_days",
                    default=999,
                ),
            ),
        )
        reliability["stationary_kutila"][
            "station_search"
        ] = station_rows[:6] + [{
            "items_omitted_from_transport": (
                len(station_rows) - 6
            ),
            "full_calculation_performed": True,
        }]
        payload["response_compaction"][
            "nonveto_station_rows_trimmed"
        ] = True

    if encoded_size() <= ACTION_RESPONSE_PAYLOAD_TARGET_CHARACTERS:
        return payload

    # 6. Planet syllables are only secondary resonance evidence; the exact
    # House 10 name test and all actual resonance matches stay present.
    payload.get(
        "navamsha_name_sounds",
        {},
    ).pop("planet_syllables", None)

    payload["response_compaction"][
        "unmatched_planet_syllables_omitted"
    ] = True

    if encoded_size() <= ACTION_RESPONSE_PAYLOAD_TARGET_CHARACTERS:
        return payload

    # 7. Preserve the KP main comparison and first/seventh cusp evidence; the
    # full pointer array was calculated but is omitted from transport.
    payload.get(
        "kp_sublords",
        {},
    ).pop("sublord_array", None)

    payload["response_compaction"][
        "kp_pointer_array_omitted_from_transport"
    ] = True

    if encoded_size() <= ACTION_RESPONSE_PAYLOAD_TARGET_CHARACTERS:
        return payload

    # 8. Final reliability projection. Preserve all veto rows and summarize
    # non-veto station/eclipses when the payload still exceeds the safety
    # target.
    reliability = payload.get("reliability_audit", {})
    stationary = reliability.get(
        "stationary_kutila",
        {},
    )
    station_rows = stationary.get(
        "station_search",
        [],
    )

    if isinstance(station_rows, list):
        decisive_rows = [
            row
            for row in station_rows
            if (
                isinstance(row, dict)
                and (
                    row.get("automatic_veto")
                    or row.get("body")
                    in set(
                        stationary.get(
                            "hard_veto_planets",
                            [],
                        )
                    )
                )
            )
        ]
        warning_rows = [
            row
            for row in station_rows
            if (
                isinstance(row, dict)
                and row not in decisive_rows
                and row.get("within_seven_days")
            )
        ]
        ordinary_rows = [
            row
            for row in station_rows
            if (
                isinstance(row, dict)
                and row not in decisive_rows
                and row not in warning_rows
            )
        ]
        retained_rows = (
            decisive_rows
            + warning_rows[:2]
            + ordinary_rows[:1]
        )
        omitted = len(station_rows) - len(
            retained_rows
        )

        if omitted > 0:
            retained_rows.append({
                "nondecisive_station_rows_omitted": (
                    omitted
                ),
                "full_calculation_performed": True,
            })
            stationary["station_search"] = (
                retained_rows
            )
            payload["response_compaction"][
                "reliability_station_rows_decisive_only"
            ] = True

    eclipses = reliability.get("eclipses", {})
    eclipse_rows = eclipses.get("events", [])

    if isinstance(eclipse_rows, list) and len(eclipse_rows) > 2:
        eclipse_rows = sorted(
            [
                row
                for row in eclipse_rows
                if isinstance(row, dict)
            ],
            key=lambda row: (
                0 if row.get("major") else 1,
                first_non_null_numeric(
                    row,
                    "absolute_days",
                    "absolute_days_from_event",
                    default=999,
                ),
            ),
        )
        eclipses["events"] = eclipse_rows[:2] + [{
            "eclipse_rows_omitted": (
                len(eclipse_rows) - 2
            ),
            "full_calculation_performed": True,
        }]
        payload["response_compaction"][
            "nonnearest_eclipse_rows_trimmed"
        ] = True

    if encoded_size() <= ACTION_RESPONSE_PAYLOAD_TARGET_CHARACTERS:
        return payload

    # 9. Final bounded pass.
    bounded = compact_recursive(
        payload,
        list_limit=6,
        string_limit=75,
    )
    bounded["response_compaction"] = {
        **payload.get("response_compaction", {}),
        "profile": "action-compact-v2-bounded",
        "final_bounded_pass": True,
    }

    bounded_size = len(json.dumps(
        bounded,
        ensure_ascii=False,
        default=str,
    ))

    if bounded_size <= ACTION_RESPONSE_PAYLOAD_TARGET_CHARACTERS:
        return bounded

    # 10. Hard transport ceiling. Every layer and every result summary remains,
    # while repeated secondary arrays are represented by counts.
    for key in (
        "contextual_or_research_contacts",
        "same_tara_side_comparisons",
        "cancellations",
    ):
        rows = bounded.get(
            "nakshatra_taras",
            {},
        ).get(key, [])

        bounded.get(
            "nakshatra_taras",
            {},
        )[key] = {
            "count": (
                len(rows) if isinstance(rows, list) else 0
            ),
            "transport_detail_omitted": True,
        }

    for key in (
        "qualifying_rashi_contacts",
        "qualifying_d9_contacts",
    ):
        rows = bounded.get(
            "special_points",
            {},
        ).get(key, [])

        if isinstance(rows, list) and len(rows) > 4:
            bounded["special_points"][key] = (
                rows[:4]
                + [{
                    "items_omitted_from_transport": (
                        len(rows) - 4
                    )
                }]
            )

    bounded = compact_recursive(
        bounded,
        list_limit=4,
        string_limit=60,
    )
    bounded["response_compaction"] = {
        **payload.get("response_compaction", {}),
        "profile": "action-compact-v2-hard-bounded",
        "hard_transport_ceiling_applied": True,
        "full_calculation_performed": True,
    }

    return bounded


def emergency_action_response(
    compacted: dict[str, Any],
) -> dict[str, Any]:
    """
    Last-resort transport profile. It preserves the decision-bearing evidence
    and removes nonqualifying detail tables.
    """

    kp = compacted.get("kp_sublords", {})
    d9 = compacted.get("navamsha_cusps", {})
    tara = compacted.get("nakshatra_taras", {})
    names = compacted.get("navamsha_name_sounds", {})

    cusp_sublords = kp.get("cusp_sublords", {})
    key_cusp_sublords = {
        house: cusp_sublords.get(house)
        for house in ("House1", "House7", "House10")
        if house in cusp_sublords
    }

    return {
        "status": compacted.get("status"),
        "strict_prediction_allowed": compacted.get(
            "strict_prediction_allowed"
        ),
        "essential_failures": compacted.get(
            "essential_failures",
            [],
        ),
        "event": compacted.get("event"),
        "core": compacted.get("core"),
        "rashi_placidus": compacted.get(
            "rashi_placidus"
        ),
        "tier1_combinations": {
            "status": compacted.get(
                "tier1_combinations",
                {},
            ).get("status"),
            "assignment": compacted.get(
                "tier1_combinations",
                {},
            ).get("assignment"),
            "ascendant": compacted.get(
                "tier1_combinations",
                {},
            ).get("ascendant"),
            "victory_houses": compacted.get(
                "tier1_combinations",
                {},
            ).get("victory_houses"),
            "sky_pky": compacted.get(
                "tier1_combinations",
                {},
            ).get("sky_pky"),
            "parivartana": compacted.get(
                "tier1_combinations",
                {},
            ).get("parivartana"),
            "planetary_war": compacted.get(
                "tier1_combinations",
                {},
            ).get("planetary_war"),
            "automatic_signed_total": compacted.get(
                "tier1_combinations",
                {},
            ).get("automatic_signed_total"),
            "missing_required_planets": compacted.get(
                "tier1_combinations",
                {},
            ).get("missing_required_planets", []),
            "error": compacted.get(
                "tier1_combinations",
                {},
            ).get("error"),
        },
        "planet_cusp_contacts": {
            "status": compacted.get(
                "planet_cusp_contacts",
                {},
            ).get("status"),
            "orb_policy": compacted.get(
                "planet_cusp_contacts",
                {},
            ).get("orb_policy"),
            "qualifying_contacts": compacted.get(
                "planet_cusp_contacts",
                {},
            ).get("qualifying_contacts", []),
        },
        "navamsha_cusps": {
            "status": d9.get("status"),
            "method": d9.get("method"),
            "ayanamsa": d9.get("ayanamsa"),
            "orb_policy": d9.get("orb_policy"),
            "lagna": d9.get("lagna"),
            "seventh_cusp": d9.get("seventh_cusp"),
            "axis_validation": d9.get(
                "axis_validation"
            ),
            "planets": d9.get("planets", {}),
            "qualifying_contacts": d9.get(
                "qualifying_contacts",
                [],
            ),
            "unavailable_planets": d9.get(
                "unavailable_planets",
                [],
            ),
            "failed_validations": d9.get(
                "failed_validations",
                [],
            ),
            "error": d9.get("error"),
        },
        "kp_sublords": {
            "status": kp.get("status"),
            "method": kp.get("method"),
            "ayanamsa": kp.get("ayanamsa"),
            "book_tier": kp.get("book_tier"),
            "cusp_sublords": key_cusp_sublords,
            "main_sublord_comparison": kp.get(
                "main_sublord_comparison"
            ),
            "sublord_array": kp.get(
                "sublord_array"
            ),
            "error": kp.get("error"),
        },
        "outer_planets": compacted.get(
            "outer_planets"
        ),
        "special_points": compacted.get(
            "special_points"
        ),
        "stolen_cusps": {
            "status": compacted.get(
                "stolen_cusps",
                {},
            ).get("status"),
            "book_tier": compacted.get(
                "stolen_cusps",
                {},
            ).get("book_tier"),
            "pdf_pages": compacted.get(
                "stolen_cusps",
                {},
            ).get("pdf_pages"),
            "whole_sign_reference": compacted.get(
                "stolen_cusps",
                {},
            ).get("whole_sign_reference"),
            "audit_summary": compacted.get(
                "stolen_cusps",
                {},
            ).get("audit_summary"),
            "stolen_cusps": compacted.get(
                "stolen_cusps",
                {},
            ).get("stolen_cusps", []),
            "qualifying_contacts": compacted.get(
                "stolen_cusps",
                {},
            ).get("qualifying_contacts", []),
            "unavailable_bodies": compacted.get(
                "stolen_cusps",
                {},
            ).get("unavailable_bodies", []),
            "points_applied": False,
            "error": compacted.get(
                "stolen_cusps",
                {},
            ).get("error"),
        },
        "navamsha_interpretation": {
            "status": compacted.get(
                "navamsha_interpretation",
                {},
            ).get("status"),
            "assignment": compacted.get(
                "navamsha_interpretation",
                {},
            ).get("assignment"),
            "tier_hierarchy": compacted.get(
                "navamsha_interpretation",
                {},
            ).get("tier_hierarchy"),
            "d9_cusp_contacts": compacted.get(
                "navamsha_interpretation",
                {},
            ).get("d9_cusp_contacts", []),
            "d9_cusp_summary": compacted.get(
                "navamsha_interpretation",
                {},
            ).get("d9_cusp_summary"),
            "navamsha_combinations": compacted.get(
                "navamsha_interpretation",
                {},
            ).get("navamsha_combinations"),
            "d1_cusp_contacts": compacted.get(
                "navamsha_interpretation",
                {},
            ).get("d1_cusp_contacts", []),
            "d1_summary": compacted.get(
                "navamsha_interpretation",
                {},
            ).get("d1_summary"),
            "d9_summary": compacted.get(
                "navamsha_interpretation",
                {},
            ).get("d9_summary"),
            "d1_d9_relationship": compacted.get(
                "navamsha_interpretation",
                {},
            ).get("d1_d9_relationship"),
            "double_whammy": compacted.get(
                "navamsha_interpretation",
                {},
            ).get("double_whammy"),
            "signed_points": compacted.get(
                "navamsha_interpretation",
                {},
            ).get("signed_points"),
            "unavailable_d9_bodies": compacted.get(
                "navamsha_interpretation",
                {},
            ).get("unavailable_d9_bodies", []),
            "optional_body_coverage_status": compacted.get(
                "navamsha_interpretation",
                {},
            ).get("optional_body_coverage_status"),
            "research_or_undefined_d9_contacts": compacted.get(
                "navamsha_interpretation",
                {},
            ).get("research_or_undefined_d9_contacts", []),
            "completeness": compacted.get(
                "navamsha_interpretation",
                {},
            ).get("completeness"),
            "pdf_pages": compacted.get(
                "navamsha_interpretation",
                {},
            ).get("pdf_pages"),
            "points_applied": compacted.get(
                "navamsha_interpretation",
                {},
            ).get("points_applied"),
            "error": compacted.get(
                "navamsha_interpretation",
                {},
            ).get("error"),
        },
        "reliability_audit": {
            "status": compacted.get(
                "reliability_audit",
                {},
            ).get("status"),
            "hard_veto": compacted.get(
                "reliability_audit",
                {},
            ).get("hard_veto"),
            "strict_prediction_allowed_by_reliability": compacted.get(
                "reliability_audit",
                {},
            ).get("strict_prediction_allowed_by_reliability"),
            "decision": compacted.get(
                "reliability_audit",
                {},
            ).get("decision"),
            "hard_veto_reasons": compacted.get(
                "reliability_audit",
                {},
            ).get("hard_veto_reasons", []),
            "warning_reasons": compacted.get(
                "reliability_audit",
                {},
            ).get("warning_reasons", []),
            "stationary_kutila": compacted.get(
                "reliability_audit",
                {},
            ).get("stationary_kutila"),
            "eclipses": compacted.get(
                "reliability_audit",
                {},
            ).get("eclipses"),
            "solar_sankranti": compacted.get(
                "reliability_audit",
                {},
            ).get("solar_sankranti"),
            "sunrise_sunset": compacted.get(
                "reliability_audit",
                {},
            ).get("sunrise_sunset"),
            "karma_fixity": compacted.get(
                "reliability_audit",
                {},
            ).get("karma_fixity"),
            "pdf_pages": compacted.get(
                "reliability_audit",
                {},
            ).get("pdf_pages"),
            "error": compacted.get(
                "reliability_audit",
                {},
            ).get("error"),
        },
        "nakshatra_taras": {
            "status": tara.get("status"),
            "orb_policy": tara.get("orb_policy"),
            "qualifying_contacts": tara.get(
                "qualifying_contacts",
                [],
            ),
            "decision_contacts": tara.get(
                "decision_contacts",
                [],
            ),
            "contextual_or_research_contacts": tara.get(
                "contextual_or_research_contacts",
                [],
            ),
            "same_tara_side_comparisons": tara.get(
                "same_tara_side_comparisons",
                [],
            ),
            "cancellations": tara.get(
                "cancellations",
                [],
            ),
            "appendix_3_tara_balam": tara.get(
                "appendix_3_tara_balam"
            ),
            "error": tara.get("error"),
        },
        "navamsha_name_sounds": {
            "status": names.get("status"),
            "participants": names.get("participants"),
            "house10_main_test": names.get(
                "house10_main_test"
            ),
            "planet_syllables": names.get(
                "planet_syllables"
            ),
            "planet_resonance_matches": names.get(
                "planet_resonance_matches",
                [],
            ),
            "name_comparison_allowed": names.get(
                "name_comparison_allowed"
            ),
            "error": names.get("error"),
        },
        "houses": tighten_house_results(
            compacted.get("houses", {})
        ),
        "planets": tighten_planet_results(
            compacted.get("planets", {})
        ),
        "provenance": compacted.get("provenance"),
        "response_compaction": {
            "applied": True,
            "profile": "action-compact-v1-emergency",
            "target_characters": (
                ACTION_RESPONSE_TARGET_CHARACTERS
            ),
            "full_calculation_performed": True,
            "omitted_only_from_transport": [
                "nonqualifying distance tables",
                "static reference catalogues",
                "duplicate KP detail tables",
                "repeated upstream wrappers",
            ],
        },
    }


def final_action_response_ceiling(
    payload: dict[str, Any],
) -> dict[str, Any]:
    """
    Last deterministic size guard.

    Every decision layer remains present. Secondary repeated rows are replaced
    by counts, while reliability veto rows and exact veto timing survive.
    """

    reliability = payload.get("reliability_audit", {})
    stationary = reliability.get(
        "stationary_kutila",
        {},
    )
    hard_veto_planets = set(
        stationary.get("hard_veto_planets", [])
    )
    rows = stationary.get("station_search", [])

    if isinstance(rows, list):
        decisive = [
            row
            for row in rows
            if (
                isinstance(row, dict)
                and row.get("body")
                and (
                    row.get("automatic_veto")
                    or row.get("body")
                    in hard_veto_planets
                )
            )
        ]
        warnings = [
            row
            for row in rows
            if (
                isinstance(row, dict)
                and row.get("body")
                and row not in decisive
                and row.get("within_seven_days")
            )
        ]
        stationary["station_search"] = (
            decisive + warnings[:1]
        )
        stationary[
            "station_rows_omitted_from_transport"
        ] = max(
            stationary.get(
                "station_rows_omitted_from_transport",
                0,
            ),
            stationary.get(
                "station_search_total_count",
                len(rows),
            )
            - len(stationary["station_search"]),
        )

    eclipse_layer = reliability.get("eclipses", {})
    eclipse_rows = eclipse_layer.get("events", [])

    if isinstance(eclipse_rows, list):
        valid_eclipses = [
            row
            for row in eclipse_rows
            if (
                isinstance(row, dict)
                and row.get("kind")
            )
        ]
        valid_eclipses.sort(
            key=lambda row: first_non_null_numeric(
                row,
                "absolute_days",
                "absolute_days_from_event",
                default=999,
            )
        )
        eclipse_layer["events"] = valid_eclipses[:2]
        eclipse_layer[
            "event_rows_omitted_from_transport"
        ] = max(
            eclipse_layer.get(
                "event_rows_omitted_from_transport",
                0,
            ),
            eclipse_layer.get(
                "event_total_count",
                len(valid_eclipses),
            )
            - len(eclipse_layer["events"]),
        )

    karma = reliability.get("karma_fixity", {})
    karma_evidence = karma.get("evidence", [])

    if isinstance(karma_evidence, list):
        karma["evidence"] = karma_evidence[:4]

    trims = (
        ("planet_cusp_contacts", "qualifying_contacts", 5),
        ("navamsha_cusps", "qualifying_contacts", 5),
        ("outer_planets", "qualifying_contacts", 4),
        ("special_points", "qualifying_rashi_contacts", 4),
        ("special_points", "qualifying_d9_contacts", 4),
        ("stolen_cusps", "qualifying_contacts", 4),
        ("navamsha_interpretation", "d1_cusp_contacts", 4),
        ("navamsha_interpretation", "d9_cusp_contacts", 4),
        ("nakshatra_taras", "qualifying_contacts", 4),
        ("nakshatra_taras", "decision_contacts", 4),
        ("navamsha_name_sounds", "planet_resonance_matches", 4),
    )

    for layer_name, key, maximum in trims:
        layer = payload.get(layer_name, {})
        values = layer.get(key)

        if isinstance(values, list) and len(values) > maximum:
            layer[key] = values[:maximum] + [{
                "items_omitted_from_transport": (
                    len(values) - maximum
                ),
                "full_calculation_performed": True,
            }]

    for key in (
        "contextual_or_research_contacts",
        "same_tara_side_comparisons",
        "cancellations",
    ):
        layer = payload.get("nakshatra_taras", {})
        values = layer.get(key)

        if isinstance(values, list):
            layer[key] = {
                "count": len(values),
                "transport_detail_omitted": True,
            }

    payload.get(
        "navamsha_name_sounds",
        {},
    ).pop("planet_syllables", None)
    payload.get(
        "kp_sublords",
        {},
    ).pop("sublord_array", None)

    for house in payload.get("houses", {}).values():
        if isinstance(house, dict):
            house.pop("aspects", None)

    for planet in payload.get("planets", {}).values():
        if isinstance(planet, dict):
            planet.pop("shadbala", None)
            planet.pop(
                "sign_longitude_consistency",
                None,
            )

    payload = compact_recursive(
        payload,
        list_limit=5,
        string_limit=70,
    )
    payload.setdefault(
        "response_compaction",
        {},
    ).update({
        "profile": "action-compact-v2-final-ceiling",
        "final_ceiling_applied": True,
        "full_calculation_performed": True,
        "safety_target_characters": (
            ACTION_RESPONSE_SAFETY_TARGET_CHARACTERS
        ),
        "payload_target_characters": (
            ACTION_RESPONSE_PAYLOAD_TARGET_CHARACTERS
        ),
    })

    if (
        len(json.dumps(
            payload,
            ensure_ascii=False,
            default=str,
        ))
        <= ACTION_RESPONSE_SAFETY_TARGET_CHARACTERS
    ):
        return payload

    # Decision-only fallback. All layer decisions remain, but secondary rows
    # are summarized. Required house and planet validation fields stay present.
    nav = payload.get("navamsha_interpretation", {})
    reliability = payload.get("reliability_audit", {})
    compacted = {
        "status": payload.get("status"),
        "strict_prediction_allowed": payload.get(
            "strict_prediction_allowed"
        ),
        "essential_failures": payload.get(
            "essential_failures",
            [],
        ),
        "event": payload.get("event"),
        "core": payload.get("core"),
        "rashi_placidus": payload.get(
            "rashi_placidus"
        ),
        "tier1_combinations": payload.get(
            "tier1_combinations"
        ),
        "planet_cusp_contacts": {
            "status": payload.get(
                "planet_cusp_contacts",
                {},
            ).get("status"),
            "qualifying_contacts": payload.get(
                "planet_cusp_contacts",
                {},
            ).get("qualifying_contacts", []),
        },
        "navamsha_cusps": payload.get(
            "navamsha_cusps"
        ),
        "kp_sublords": payload.get("kp_sublords"),
        "outer_planets": payload.get(
            "outer_planets"
        ),
        "special_points": payload.get(
            "special_points"
        ),
        "stolen_cusps": payload.get(
            "stolen_cusps"
        ),
        "navamsha_interpretation": {
            "status": nav.get("status"),
            "assignment": nav.get("assignment"),
            "tier_hierarchy": nav.get(
                "tier_hierarchy"
            ),
            "decision_grade_policy": nav.get(
                "decision_grade_policy"
            ),
            "d1_cusp_summary": nav.get(
                "d1_cusp_summary"
            ),
            "d9_cusp_summary": nav.get(
                "d9_cusp_summary"
            ),
            "navamsha_combinations": nav.get(
                "navamsha_combinations"
            ),
            "d1_summary": nav.get("d1_summary"),
            "d9_summary": nav.get("d9_summary"),
            "d1_d9_relationship": nav.get(
                "d1_d9_relationship"
            ),
            "double_whammy": nav.get(
                "double_whammy"
            ),
            "node_axis_deduplication": nav.get(
                "node_axis_deduplication"
            ),
            "research_or_undefined_d1_contact_count": len(
                nav.get(
                    "research_or_undefined_d1_contacts",
                    [],
                )
            ),
            "research_or_undefined_d9_contact_count": len(
                nav.get(
                    "research_or_undefined_d9_contacts",
                    [],
                )
            ),
            "signed_points": nav.get(
                "signed_points"
            ),
            "completeness": nav.get(
                "completeness"
            ),
            "error": nav.get("error"),
        },
        "chart_correlation": payload.get(
            "chart_correlation"
        ),
        "reliability_audit": reliability,
        "nakshatra_taras": payload.get(
            "nakshatra_taras"
        ),
        "navamsha_name_sounds": payload.get(
            "navamsha_name_sounds"
        ),
        "houses": payload.get("houses"),
        "planets": payload.get("planets"),
        "provenance": payload.get("provenance"),
        "response_compaction": {
            "applied": True,
            "profile": (
                "action-compact-v2-decision-ceiling"
            ),
            "final_ceiling_applied": True,
            "full_calculation_performed": True,
            "safety_target_characters": (
                ACTION_RESPONSE_SAFETY_TARGET_CHARACTERS
            ),
            "payload_target_characters": (
                ACTION_RESPONSE_PAYLOAD_TARGET_CHARACTERS
            ),
        },
    }

    final_compacted = compact_recursive(
        compacted,
        list_limit=2,
        string_limit=50,
    )

    if (
        len(json.dumps(
            final_compacted,
            ensure_ascii=False,
            default=str,
        ))
        <= ACTION_RESPONSE_SAFETY_TARGET_CHARACTERS
    ):
        return final_compacted

    # Absolute decision kernel. This branch is intentionally small and keeps
    # validation, hierarchy, reliability, de-duplication and correlation
    # decisions while replacing repeated contact rows with counts.
    nav = final_compacted.get(
        "navamsha_interpretation",
        {},
    )
    reliability = final_compacted.get(
        "reliability_audit",
        {},
    )

    kernel = {
        "status": final_compacted.get("status"),
        "strict_prediction_allowed": final_compacted.get(
            "strict_prediction_allowed"
        ),
        "essential_failures": final_compacted.get(
            "essential_failures",
            [],
        ),
        "event": final_compacted.get("event"),
        "core": final_compacted.get("core"),
        "rashi_placidus": final_compacted.get(
            "rashi_placidus"
        ),
        "tier1_combinations": final_compacted.get(
            "tier1_combinations"
        ),
        "planet_cusp_contacts": {
            "status": final_compacted.get(
                "planet_cusp_contacts",
                {},
            ).get("status"),
            "qualifying_contact_count": len(
                final_compacted.get(
                    "planet_cusp_contacts",
                    {},
                ).get("qualifying_contacts", [])
            ),
            "qualifying_contacts": final_compacted.get(
                "planet_cusp_contacts",
                {},
            ).get("qualifying_contacts", [])[:1],
        },
        "navamsha_cusps": {
            "status": final_compacted.get(
                "navamsha_cusps",
                {},
            ).get("status"),
            "lagna": final_compacted.get(
                "navamsha_cusps",
                {},
            ).get("lagna"),
            "seventh_cusp": final_compacted.get(
                "navamsha_cusps",
                {},
            ).get("seventh_cusp"),
            "axis_validation": final_compacted.get(
                "navamsha_cusps",
                {},
            ).get("axis_validation"),
            "qualifying_contacts": final_compacted.get(
                "navamsha_cusps",
                {},
            ).get("qualifying_contacts", [])[:1],
        },
        "kp_sublords": final_compacted.get(
            "kp_sublords"
        ),
        "outer_planets": {
            "status": final_compacted.get(
                "outer_planets",
                {},
            ).get("status"),
            "qualifying_contacts": final_compacted.get(
                "outer_planets",
                {},
            ).get("qualifying_contacts", [])[:1],
            "unavailable_bodies": final_compacted.get(
                "outer_planets",
                {},
            ).get("unavailable_bodies", []),
        },
        "special_points": {
            "status": final_compacted.get(
                "special_points",
                {},
            ).get("status"),
            "qualifying_rashi_contacts": final_compacted.get(
                "special_points",
                {},
            ).get("qualifying_rashi_contacts", [])[:1],
            "qualifying_d9_contacts": final_compacted.get(
                "special_points",
                {},
            ).get("qualifying_d9_contacts", [])[:1],
            "unavailable_points": final_compacted.get(
                "special_points",
                {},
            ).get("unavailable_points", []),
        },
        "stolen_cusps": {
            "status": final_compacted.get(
                "stolen_cusps",
                {},
            ).get("status"),
            "audit_summary": final_compacted.get(
                "stolen_cusps",
                {},
            ).get("audit_summary"),
            "stolen_cusps": final_compacted.get(
                "stolen_cusps",
                {},
            ).get("stolen_cusps", [])[:1],
            "qualifying_contacts": final_compacted.get(
                "stolen_cusps",
                {},
            ).get("qualifying_contacts", [])[:1],
        },
        "navamsha_interpretation": {
            "status": nav.get("status"),
            "assignment": nav.get("assignment"),
            "tier_hierarchy": nav.get(
                "tier_hierarchy"
            ),
            "decision_grade_policy": nav.get(
                "decision_grade_policy"
            ),
            "d1_cusp_summary": nav.get(
                "d1_cusp_summary"
            ),
            "d9_cusp_summary": nav.get(
                "d9_cusp_summary"
            ),
            "navamsha_combinations": {
                key: (
                    nav.get(
                        "navamsha_combinations",
                        {},
                    ).get(key)
                )
                for key in (
                    "status",
                    "raw_signed_favourite_total",
                    "signed_favourite_total",
                    "indication",
                    "overlapping_pair_policy",
                    "overlap_clusters",
                )
            },
            "d1_summary": nav.get("d1_summary"),
            "d9_summary": nav.get("d9_summary"),
            "d1_d9_relationship": nav.get(
                "d1_d9_relationship"
            ),
            "double_whammy": nav.get(
                "double_whammy"
            ),
            "node_axis_deduplication": nav.get(
                "node_axis_deduplication"
            ),
            "signed_points": nav.get(
                "signed_points"
            ),
            "research_or_undefined_d1_contacts": nav.get(
                "research_or_undefined_d1_contacts",
                [],
            )[:1],
            "research_or_undefined_d9_contacts": nav.get(
                "research_or_undefined_d9_contacts",
                [],
            )[:1],
            "completeness": nav.get(
                "completeness"
            ),
            "error": nav.get("error"),
        },
        "chart_correlation": final_compacted.get(
            "chart_correlation"
        ),
        "reliability_audit": {
            key: reliability.get(key)
            for key in (
                "status",
                "policy_mode",
                "strict_book_hard_veto",
                "strict_book_prediction_allowed",
                "strict_book_hard_veto_reasons",
                "practical_hard_veto",
                "practical_prediction_allowed",
                "practical_hard_veto_reasons",
                "hard_veto",
                "strict_prediction_allowed_by_reliability",
                "decision",
                "confidence_cap",
                "hard_veto_reasons",
                "warning_reasons",
                "stationary_kutila",
                "eclipses",
                "solar_sankranti",
                "sunrise_sunset",
                "karma_fixity",
                "error",
            )
        },
        "nakshatra_taras": final_compacted.get(
            "nakshatra_taras"
        ),
        "navamsha_name_sounds": final_compacted.get(
            "navamsha_name_sounds"
        ),
        "houses": final_compacted.get("houses"),
        "planets": final_compacted.get("planets"),
        "provenance": final_compacted.get(
            "provenance"
        ),
        "response_compaction": {
            "applied": True,
            "profile": (
                "action-compact-v2-absolute-decision-kernel"
            ),
            "final_ceiling_applied": True,
            "full_calculation_performed": True,
            "safety_target_characters": (
                ACTION_RESPONSE_SAFETY_TARGET_CHARACTERS
            ),
            "payload_target_characters": (
                ACTION_RESPONSE_PAYLOAD_TARGET_CHARACTERS
            ),
        },
    }

    return compact_recursive(
        kernel,
        list_limit=1,
        string_limit=45,
    )


def compact_action_response(
    response: dict[str, Any],
) -> dict[str, Any]:
    """
    Build a prediction-grade Action response below the transport-size limit.

    All calculations and validation run before this function. Only duplicate
    raw payloads and non-event reference catalogues are omitted.
    """

    compacted: dict[str, Any] = {
        "status": response.get("status"),
        "strict_prediction_allowed": response.get(
            "strict_prediction_allowed"
        ),
        "essential_failures": compact_recursive(
            response.get("essential_failures", []),
            list_limit=12,
            string_limit=200,
        ),
        "event": compact_recursive(response.get("event", {})),
        "core": compact_core_results(
            response.get("core", {})
        ),
        "rashi_placidus": compact_rashi_placidus(
            response.get("rashi_placidus", {})
        ),
        "tier1_combinations": compact_tier1_combinations_layer(
            response.get("tier1_combinations", {})
        ),
        "planet_cusp_contacts": compact_recursive(
            response.get("planet_cusp_contacts", {}),
            list_limit=20,
            string_limit=200,
        ),
        "navamsha_cusps": compact_navamsha_layer(
            response.get("navamsha_cusps", {})
        ),
        "kp_sublords": compact_kp_layer(
            response.get("kp_sublords", {})
        ),
        "outer_planets": compact_outer_planets_layer(
            response.get("outer_planets", {})
        ),
        "special_points": compact_special_points_layer(
            response.get("special_points", {})
        ),
        "stolen_cusps": compact_stolen_cusps_layer(
            response.get("stolen_cusps", {})
        ),
        "navamsha_interpretation": (
            compact_navamsha_interpretation_layer(
                response.get("navamsha_interpretation", {})
            )
        ),
        "chart_correlation": compact_recursive(
            response.get("chart_correlation", {}),
            list_limit=8,
            string_limit=160,
        ),
        "reliability_audit": compact_reliability_audit_layer(
            response.get("reliability_audit", {})
        ),
        "nakshatra_taras": compact_tara_layer(
            response.get("nakshatra_taras", {})
        ),
        "navamsha_name_sounds": compact_name_sounds_layer(
            response.get("navamsha_name_sounds", {})
        ),
        "houses": compact_house_results(
            response.get("houses", {})
        ),
        "planets": compact_planet_results(
            response.get("planets", {})
        ),
        "provenance": {
            "engine": response.get(
                "provenance",
                {},
            ).get("engine"),
            "proxy_version": PROXY_VERSION,
            "standard_ayanamsa": "Lahiri",
            "kp_ayanamsa": "Krishnamurti",
            "response_profile": (
                "prediction-grade compact v2; v1.19 decision-grade aggregation"
            ),
        },
        "response_compaction": {
            "applied": True,
            "profile": "action-compact-v1",
            "target_characters": (
                ACTION_RESPONSE_TARGET_CHARACTERS
            ),
            "full_calculation_performed": True,
            "omitted_only_from_transport": [
                "Chapter 8 full static tara catalogue",
                "KP full significator matrix",
                "duplicate KP planet/cusp tables",
                "repeated upstream raw VedAstro payloads",
                "repeated Chapter 7 sound-alias lists",
            ],
        },
    }

    encoded = json.dumps(
        compacted,
        ensure_ascii=False,
        default=str,
    )

    if len(encoded) > ACTION_RESPONSE_PAYLOAD_TARGET_CHARACTERS:
        # Tier 2 transport trim: retain actual contacts/results, remove only
        # nearest non-qualifying reference material.
        compacted.get(
            "nakshatra_taras",
            {},
        ).pop("closest_marker_by_target", None)
        compacted.get(
            "nakshatra_taras",
            {},
        ).pop("targets", None)
        compacted.get(
            "planet_cusp_contacts",
            {},
        ).pop("distances_by_planet", None)
        compacted.get(
            "planet_cusp_contacts",
            {},
        ).pop("closest_planet_by_cusp", None)
        compacted.get(
            "outer_planets",
            {},
        ).pop("closest_body_by_cusp", None)

        compacted["response_compaction"][
            "secondary_trim_applied"
        ] = True

    encoded = json.dumps(
        compacted,
        ensure_ascii=False,
        default=str,
    )

    if len(encoded) > ACTION_RESPONSE_PAYLOAD_TARGET_CHARACTERS:
        # Compact-v2 must receive the complete first-pass projection.
        # Running the older emergency profile first can discard newer-layer
        # metadata before compact-v2 has a chance to retain it.
        compacted = action_compact_v2(compacted)
        compacted = enforce_action_response_limit(
            compacted
        )

    encoded = json.dumps(
        compacted,
        ensure_ascii=False,
        default=str,
    )

    compacted.setdefault(
        "response_compaction",
        {},
    ).update({
        "safety_target_characters": (
            ACTION_RESPONSE_SAFETY_TARGET_CHARACTERS
        ),
        "payload_target_characters": (
            ACTION_RESPONSE_PAYLOAD_TARGET_CHARACTERS
        ),
    })

    # Metadata is added before the last limiter pass, so the actual serialized
    # response—not only the pre-metadata payload—is kept below the ceiling.
    compacted = enforce_action_response_limit(
        compacted
    )
    compacted.setdefault(
        "response_compaction",
        {},
    ).update({
        "safety_target_characters": (
            ACTION_RESPONSE_SAFETY_TARGET_CHARACTERS
        ),
        "payload_target_characters": (
            ACTION_RESPONSE_PAYLOAD_TARGET_CHARACTERS
        ),
        "final_character_count": 0,
    })

    for _ in range(4):
        final_count = len(json.dumps(
            compacted,
            ensure_ascii=False,
            default=str,
        ))
        compacted["response_compaction"][
            "final_character_count"
        ] = final_count

    # Absolute final guard. This is only reached after every targeted trim.
    if (
        len(json.dumps(
            compacted,
            ensure_ascii=False,
            default=str,
        ))
        > ACTION_RESPONSE_SAFETY_TARGET_CHARACTERS
    ):
        compacted = final_action_response_ceiling(
            compacted
        )

    for _ in range(4):
        final_count = len(json.dumps(
            compacted,
            ensure_ascii=False,
            default=str,
        ))
        compacted.setdefault(
            "response_compaction",
            {},
        )["final_character_count"] = final_count

    return compacted


def whole_sign_house_for_longitude(
    sidereal_longitude: float,
    ascendant_longitude: float,
) -> int:
    """Place one longitude in a whole-sign house relative to the Ascendant."""

    longitude_sign_index = int(
        normalise_degrees(float(sidereal_longitude)) // 30.0
    )
    ascendant_sign_index = int(
        normalise_degrees(float(ascendant_longitude)) // 30.0
    )

    return (
        (longitude_sign_index - ascendant_sign_index) % 12
    ) + 1


def stolen_cusp_house_class(house_number: int) -> str:
    if house_number in STOLEN_CUSP_POWER_HOUSES:
        return "Power"

    if house_number in STOLEN_CUSP_NEUTRAL_HOUSES:
        return "Neutral"

    return "Outside explicit Chapter 4 contest classes"


def signed_house_shift(
    source_house: int,
    target_house: int,
) -> int:
    """Return the shortest signed whole-sign displacement."""

    shift = (target_house - source_house) % 12

    if shift > 6:
        shift -= 12

    return shift


def classify_stolen_cusp(
    source_house: int,
    whole_sign_house: int,
) -> dict[str, Any]:
    """
    Classify the exact Chapter 4 stolen-cusp transformation.

    Winner direction is deliberately not guessed. A transferred contact must
    still use the body's book-defined effect on the effective power cusp.
    """

    source_class = stolen_cusp_house_class(source_house)
    target_class = stolen_cusp_house_class(whole_sign_house)
    is_stolen = source_house != whole_sign_house
    source_side = STOLEN_CUSP_SIDE_BY_POWER_HOUSE.get(
        source_house
    )
    effective_side = STOLEN_CUSP_SIDE_BY_POWER_HOUSE.get(
        whole_sign_house
    )
    source_axis = STOLEN_CUSP_AXIS_BY_POWER_HOUSE.get(
        source_house
    )
    effective_axis = STOLEN_CUSP_AXIS_BY_POWER_HOUSE.get(
        whole_sign_house
    )

    if not is_stolen:
        stolen_type = "Not stolen"
        rule_status = "Not applicable"
        transformation = "No transfer"
        contact_strength = "Normal cusp rule"
        interpretation = (
            "The Placidus cusp remains in its same-numbered whole-sign "
            "house."
        )
    elif source_class == "Power" and target_class == "Neutral":
        stolen_type = "Power-to-neutral"
        rule_status = "Book-defined"
        transformation = "Weakened"
        contact_strength = "Significantly reduced"
        interpretation = (
            "A planet conjoined with this power cusp loses effectiveness. "
            "Both helpful and harmful cusp effects are reduced."
        )
    elif source_class == "Neutral" and target_class == "Power":
        stolen_type = "Neutral-to-power"
        rule_status = "Book-defined"
        transformation = "Activated and transferred"
        contact_strength = "Effective through target power house"
        interpretation = (
            "A planet conjoined with this normally neutral cusp becomes "
            "eligible to affect the match through the whole-sign power "
            f"house, House{whole_sign_house}."
        )
    elif source_class == "Power" and target_class == "Power":
        stolen_type = "Power-to-power"
        rule_status = "Book-defined"
        transformation = "Redirected"
        contact_strength = "Judge through target power house"
        interpretation = (
            "A planet conjoined with this cusp is judged through the "
            f"whole-sign House{whole_sign_house}, not mechanically through "
            f"the original House{source_house} cusp."
        )
    else:
        stolen_type = "Other shifted cusp"
        rule_status = "Not explicitly defined by book"
        transformation = "No automatic contest rule"
        contact_strength = "Unscored"
        interpretation = (
            "The cusp changed whole-sign house, but Chapter 4 does not "
            "define this source/target class as one of its three contest "
            "stolen-cusp types."
        )

    return {
        "is_stolen": is_stolen,
        "stolen_type": stolen_type,
        "rule_status": rule_status,
        "source_house_class": source_class,
        "target_house_class": target_class,
        "source_axis": source_axis,
        "effective_axis": effective_axis,
        "source_represented_side": source_side,
        "effective_represented_side": effective_side,
        "transformation": transformation,
        "contact_strength": contact_strength,
        "interpretation": interpretation,
        "winner_direction_inferred": False,
        "points_applied": False,
    }


def collect_stolen_cusp_bodies(
    planets: dict[str, dict[str, Any]],
    outer_planets: dict[str, Any],
    special_points: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Collect every exact body currently available to the proxy."""

    bodies: list[dict[str, Any]] = []
    unavailable: list[dict[str, Any]] = []

    for planet_name, result in planets.items():
        longitude = extract_total_degrees(
            result.get("sidereal_longitude", {})
        )

        if longitude is None:
            unavailable.append({
                "body": planet_name,
                "category": "Classical planet",
                "reason": "Exact sidereal longitude is unavailable.",
            })
            continue

        visibility_class, orb_limit = cusp_orb_policy(
            planet_name
        )
        bodies.append({
            "body": planet_name,
            "category": "Classical planet",
            "sidereal_longitude": round(
                normalise_degrees(longitude),
                8,
            ),
            "visibility_class": visibility_class,
            "orb_limit": orb_limit,
            "motion": None,
        })

    for body_name, result in outer_planets.get(
        "bodies",
        {},
    ).items():
        longitude = result.get("sidereal_longitude")

        if (
            result.get("status") != "Pass"
            or not isinstance(longitude, (int, float))
        ):
            unavailable.append({
                "body": body_name,
                "category": "Outer planet",
                "reason": result.get(
                    "error",
                    "Exact sidereal longitude is unavailable.",
                ),
            })
            continue

        bodies.append({
            "body": body_name,
            "category": "Outer planet",
            "sidereal_longitude": round(
                normalise_degrees(float(longitude)),
                8,
            ),
            "visibility_class": "invisible",
            "orb_limit": OUTER_CUSP_ORB_DEGREES,
            "motion": result.get("motion"),
        })

    for point_name, result in special_points.get(
        "points",
        {},
    ).items():
        longitude = result.get("sidereal_longitude")

        if (
            result.get("status") != "Pass"
            or not isinstance(longitude, (int, float))
        ):
            unavailable.append({
                "body": point_name,
                "category": "Special point",
                "reason": result.get(
                    "error",
                    "Exact sidereal longitude is unavailable.",
                ),
            })
            continue

        bodies.append({
            "body": point_name,
            "category": "Special point",
            "sidereal_longitude": round(
                normalise_degrees(float(longitude)),
                8,
            ),
            "visibility_class": "invisible",
            "orb_limit": SPECIAL_POINT_CUSP_ORB_DEGREES,
            "motion": None,
        })

    return bodies, unavailable


def stolen_cusp_contact_effect(
    cusp: dict[str, Any],
) -> dict[str, Any]:
    """Return the book-locked transfer instruction for one active contact."""

    classification = cusp["classification"]
    stolen_type = classification["stolen_type"]

    if stolen_type == "Power-to-neutral":
        instruction = (
            "Reduce the body's normal positive or negative cusp influence. "
            "Do not transfer it to a team."
        )
    elif stolen_type in {
        "Neutral-to-power",
        "Power-to-power",
    }:
        instruction = (
            "Apply the body's separate Chapter 4 cusp rule as though it "
            f"contacted House{cusp['whole_sign_house_number']}. The stolen-"
            "cusp layer itself does not decide whether that body helps or "
            "harms the represented side."
        )
    else:
        instruction = (
            "No automatic Chapter 4 contest transformation is applied."
        )

    return {
        "stolen_type": stolen_type,
        "transformation": classification["transformation"],
        "contact_strength": classification["contact_strength"],
        "source_cusp": cusp["cusp"],
        "source_represented_side": classification[
            "source_represented_side"
        ],
        "effective_power_house": (
            f"House{cusp['whole_sign_house_number']}"
            if classification["target_house_class"] == "Power"
            else None
        ),
        "effective_axis": classification["effective_axis"],
        "effective_represented_side": classification[
            "effective_represented_side"
        ],
        "interpretation_instruction": instruction,
        "winner_direction_inferred": False,
        "points_applied": False,
    }


def calculate_stolen_cusps(
    rashi_placidus: dict[str, Any],
    planets: dict[str, dict[str, Any]],
    outer_planets: dict[str, Any],
    special_points: dict[str, Any],
) -> dict[str, Any]:
    """
    Calculate all exact Chapter 4 stolen cusps and active body contacts.

    The geometry uses exact Lahiri Placidus cusps and the rashi whole-sign
    house counted from the sign containing the Ascendant.
    """

    if rashi_placidus.get("status") != "Pass":
        return {
            "status": "Unavailable",
            "method": "BookLockedChapter4StolenCusps",
            "book_chapter": 4,
            "book_tier": 2,
            "pdf_pages": STOLEN_CUSP_PDF_PAGES,
            "ayanamsa": "Lahiri",
            "house_system": "Placidus compared with whole-sign rashi",
            "error": "Exact Lahiri Placidus cusps are unavailable.",
            "cusp_audit": [],
            "stolen_cusps": [],
            "qualifying_contacts": [],
        }

    cusps = rashi_placidus.get("cusps", {})
    ascendant_longitude = cusps.get(
        "House1",
        {},
    ).get("sidereal_longitude")

    if not isinstance(ascendant_longitude, (int, float)):
        return {
            "status": "Unavailable",
            "method": "BookLockedChapter4StolenCusps",
            "book_chapter": 4,
            "book_tier": 2,
            "pdf_pages": STOLEN_CUSP_PDF_PAGES,
            "ayanamsa": "Lahiri",
            "house_system": "Placidus compared with whole-sign rashi",
            "error": "Exact Ascendant cusp longitude is unavailable.",
            "cusp_audit": [],
            "stolen_cusps": [],
            "qualifying_contacts": [],
        }

    ascendant_longitude = normalise_degrees(
        float(ascendant_longitude)
    )
    cusp_audit: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []

    for house_number in range(1, 13):
        cusp_name = f"House{house_number}"
        cusp = cusps.get(cusp_name, {})
        cusp_longitude = cusp.get("sidereal_longitude")

        if not isinstance(cusp_longitude, (int, float)):
            failures.append({
                "cusp": cusp_name,
                "reason": "Exact cusp longitude is unavailable.",
            })
            continue

        cusp_longitude = normalise_degrees(
            float(cusp_longitude)
        )
        whole_sign_house = whole_sign_house_for_longitude(
            cusp_longitude,
            ascendant_longitude,
        )
        classification = classify_stolen_cusp(
            house_number,
            whole_sign_house,
        )

        cusp_audit.append({
            "cusp": cusp_name,
            "source_house_number": house_number,
            "sidereal_longitude": round(cusp_longitude, 8),
            **sign_details_from_longitude(cusp_longitude),
            "whole_sign_house_number": whole_sign_house,
            "whole_sign_house": f"House{whole_sign_house}",
            "signed_house_shift": signed_house_shift(
                house_number,
                whole_sign_house,
            ),
            "classification": classification,
        })

    if failures:
        return {
            "status": "Fail",
            "method": "BookLockedChapter4StolenCusps",
            "book_chapter": 4,
            "book_tier": 2,
            "pdf_pages": STOLEN_CUSP_PDF_PAGES,
            "ayanamsa": "Lahiri",
            "house_system": "Placidus compared with whole-sign rashi",
            "error": "One or more exact cusp longitudes are missing.",
            "failed_cusps": failures,
            "cusp_audit": cusp_audit,
            "stolen_cusps": [],
            "qualifying_contacts": [],
        }

    stolen_cusps = [
        cusp
        for cusp in cusp_audit
        if cusp["classification"]["is_stolen"]
    ]
    book_defined_stolen_cusps = [
        cusp
        for cusp in stolen_cusps
        if cusp["classification"]["rule_status"] == "Book-defined"
    ]

    bodies, unavailable_bodies = collect_stolen_cusp_bodies(
        planets,
        outer_planets,
        special_points,
    )
    qualifying_contacts: list[dict[str, Any]] = []

    for cusp in book_defined_stolen_cusps:
        for body in bodies:
            distance = angular_distance(
                body["sidereal_longitude"],
                cusp["sidereal_longitude"],
            )

            if distance > body["orb_limit"] + 1e-9:
                continue

            qualifying_contacts.append({
                "body": body["body"],
                "body_category": body["category"],
                "body_longitude": body["sidereal_longitude"],
                "body_sign": sign_details_from_longitude(
                    body["sidereal_longitude"]
                )["sign"],
                "body_motion": body.get("motion"),
                "visibility_class": body[
                    "visibility_class"
                ],
                "cusp": cusp["cusp"],
                "cusp_longitude": cusp[
                    "sidereal_longitude"
                ],
                "source_house_number": cusp[
                    "source_house_number"
                ],
                "whole_sign_house_number": cusp[
                    "whole_sign_house_number"
                ],
                "stolen_type": cusp[
                    "classification"
                ]["stolen_type"],
                "angular_distance": round(distance, 8),
                "orb_limit": body["orb_limit"],
                "within_orb": True,
                "orb_margin": round(
                    body["orb_limit"] - distance,
                    8,
                ),
                "book_effect": stolen_cusp_contact_effect(
                    cusp
                ),
                "pdf_pages": STOLEN_CUSP_PDF_PAGES,
                "points_applied": False,
            })

    type_order = {
        "Power-to-power": 0,
        "Neutral-to-power": 1,
        "Power-to-neutral": 2,
    }
    qualifying_contacts.sort(
        key=lambda item: (
            type_order.get(item["stolen_type"], 9),
            item["angular_distance"],
            item["cusp"],
            item["body"],
        )
    )

    active_cusps = {
        contact["cusp"]
        for contact in qualifying_contacts
    }
    dormant_stolen_cusps = [
        cusp["cusp"]
        for cusp in book_defined_stolen_cusps
        if cusp["cusp"] not in active_cusps
    ]

    type_counts = {
        stolen_type: sum(
            1
            for cusp in book_defined_stolen_cusps
            if cusp["classification"]["stolen_type"]
            == stolen_type
        )
        for stolen_type in (
            "Power-to-neutral",
            "Neutral-to-power",
            "Power-to-power",
        )
    }

    return {
        "status": "Pass",
        "method": "BookLockedChapter4StolenCusps",
        "book_chapter": 4,
        "book_tier": 2,
        "pdf_pages": STOLEN_CUSP_PDF_PAGES,
        "ayanamsa": "Lahiri",
        "house_system": "Placidus compared with whole-sign rashi",
        "whole_sign_reference": {
            "ascendant_longitude": round(
                ascendant_longitude,
                8,
            ),
            **sign_details_from_longitude(
                ascendant_longitude
            ),
            "whole_sign_house1": sign_details_from_longitude(
                ascendant_longitude
            )["sign"],
        },
        "book_rules": {
            "power_houses": sorted(
                STOLEN_CUSP_POWER_HOUSES
            ),
            "neutral_houses": sorted(
                STOLEN_CUSP_NEUTRAL_HOUSES
            ),
            "power_to_neutral": (
                "Reduces the effect of a body conjoined with the cusp."
            ),
            "neutral_to_power": (
                "Activates the neutral cusp through the target whole-sign "
                "power house."
            ),
            "power_to_power": (
                "Redirects the body contact to the target whole-sign "
                "power house."
            ),
            "body_effect_required_separately": True,
        },
        "orb_policy": {
            "visible_planets_degrees": VISIBLE_CUSP_ORB_DEGREES,
            "invisible_planets_and_points_degrees": (
                INVISIBLE_CUSP_ORB_DEGREES
            ),
            "outer_planets_degrees": OUTER_CUSP_ORB_DEGREES,
            "special_points_degrees": (
                SPECIAL_POINT_CUSP_ORB_DEGREES
            ),
            "exalted_or_retrograde_extra_orb_applied": False,
            "extra_orb_note": (
                "The book mentions a little extra orb for exalted or "
                "retrograde bodies but does not provide one universal "
                "mechanical quantity here, so no value is invented."
            ),
        },
        "audit_summary": {
            "all_12_cusps_checked": len(cusp_audit) == 12,
            "stolen_cusp_count": len(stolen_cusps),
            "book_defined_stolen_cusp_count": len(
                book_defined_stolen_cusps
            ),
            "type_counts": type_counts,
            "qualifying_contact_count": len(
                qualifying_contacts
            ),
            "active_stolen_cusps": sorted(
                active_cusps
            ),
            "dormant_stolen_cusps": sorted(
                dormant_stolen_cusps
            ),
        },
        "cusp_audit": cusp_audit,
        "stolen_cusps": stolen_cusps,
        "qualifying_contacts": qualifying_contacts,
        "dormant_stolen_cusps": dormant_stolen_cusps,
        "available_body_count": len(bodies),
        "unavailable_bodies": unavailable_bodies,
        "coverage_status": (
            "Pass"
            if not unavailable_bodies
            else "Partial"
        ),
        "interpretation_applied": (
            "Book-defined transfer/weakening only; body winner effect "
            "must be interpreted separately."
        ),
        "winner_direction_inferred": False,
        "points_applied": False,
        "error": None,
    }


def extract_boolean_result(
    result: dict[str, Any] | None,
) -> bool | None:
    """Extract a genuine boolean from a compact VedAstro result."""

    if not isinstance(result, dict):
        return None

    value = result.get("data")

    def search(candidate: Any) -> bool | None:
        if isinstance(candidate, bool):
            return candidate

        if isinstance(candidate, (int, float)):
            if candidate == 1:
                return True
            if candidate == 0:
                return False
            return None

        if isinstance(candidate, str):
            normalised = candidate.strip().lower()

            if normalised in {"true", "yes", "1"}:
                return True
            if normalised in {"false", "no", "0"}:
                return False

            return None

        if isinstance(candidate, dict):
            for child in candidate.values():
                found = search(child)

                if found is not None:
                    return found

        if isinstance(candidate, list):
            for child in candidate:
                found = search(child)

                if found is not None:
                    return found

        return None

    return search(value)


def tier1_nakshatra_details(
    sidereal_longitude: float,
) -> dict[str, Any]:
    """Derive the exact Lahiri nakshatra and its Vimshottari lord."""

    normalised = normalise_degrees(sidereal_longitude)
    span = 360.0 / 27.0
    index = min(int(normalised // span), 26)

    return {
        "nakshatra": NAKSHATRA_NAMES[index],
        "nakshatra_lord": NAKSHATRA_LORDS[index],
        "own_nakshatra": (
            NAKSHATRA_LORDS[index]
        ),
    }


def tier1_side_for_house(
    house_number: int,
) -> str | None:
    if house_number in TIER1_FAVOURITE_VICTORY_HOUSES:
        return "Favourite"

    if house_number in TIER1_UNDERDOG_VICTORY_HOUSES:
        return "Underdog"

    return None


def tier1_signed_value(
    side: str,
    points: float,
) -> float:
    return points if side == "Favourite" else -points


def tier1_planet_snapshot(
    planet_name: str,
    result: dict[str, Any],
    ascendant_longitude: float,
) -> dict[str, Any]:
    """Build one exact D1 whole-sign planet snapshot."""

    longitude = extract_total_degrees(
        result.get("sidereal_longitude", {})
    )

    if longitude is None:
        return {
            "status": "Unavailable",
            "planet": planet_name,
            "error": "Exact sidereal longitude is unavailable.",
        }

    longitude = normalise_degrees(longitude)
    sign_details = sign_details_from_longitude(longitude)
    house_number = whole_sign_house_for_longitude(
        longitude,
        ascendant_longitude,
    )
    nakshatra = tier1_nakshatra_details(longitude)

    return {
        "status": "Pass",
        "planet": planet_name,
        "sidereal_longitude": round(longitude, 8),
        **sign_details,
        "whole_sign_house_number": house_number,
        "whole_sign_house": f"House{house_number}",
        "victory_side": tier1_side_for_house(house_number),
        "natural_class": (
            "Malefic"
            if planet_name in TIER1_NATURAL_MALEFICS
            else (
                "Benefic"
                if planet_name in TIER1_NATURAL_BENEFICS
                else "Moon/manual"
            )
        ),
        "retrograde": extract_boolean_result(
            result.get("retrograde")
        ),
        "combust": extract_boolean_result(
            result.get("combust")
        ),
        "exalted": extract_boolean_result(
            result.get("exalted")
        ),
        "debilitated": extract_boolean_result(
            result.get("debilitated")
        ),
        "own_sign": extract_boolean_result(
            result.get("own_sign")
        ),
        "moolatrikona": extract_boolean_result(
            result.get("moolatrikona")
        ),
        "dig_bala": (
            TIER1_DIG_BALA_HOUSES.get(planet_name)
            == house_number
        ),
        "nakshatra": nakshatra["nakshatra"],
        "nakshatra_lord": nakshatra["nakshatra_lord"],
        "own_nakshatra": (
            nakshatra["nakshatra_lord"] == planet_name
        ),
        "shadbala": compact_calculation_result(
            result.get("shadbala", {}),
            90,
        ),
    }


def calculate_tier1_parivartana(
    snapshots: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    """Detect exact classical mutual sign reception."""

    classical = (
        "Sun",
        "Moon",
        "Mars",
        "Mercury",
        "Jupiter",
        "Venus",
        "Saturn",
    )
    pairs: list[dict[str, Any]] = []
    eligible_benefics: set[str] = set()

    for first_index, first_name in enumerate(classical):
        first = snapshots.get(first_name, {})

        if first.get("status") != "Pass":
            continue

        first_sign_lord = SIGN_LORDS.get(first.get("sign"))

        for second_name in classical[first_index + 1:]:
            second = snapshots.get(second_name, {})

            if second.get("status") != "Pass":
                continue

            second_sign_lord = SIGN_LORDS.get(
                second.get("sign")
            )

            if (
                first_sign_lord != second_name
                or second_sign_lord != first_name
            ):
                continue

            first_side = first.get("victory_side")
            second_side = second.get("victory_side")
            victory_house_relevance = [
                side
                for side in (first_side, second_side)
                if side
            ]

            pair = {
                "planets": [first_name, second_name],
                "first": {
                    "planet": first_name,
                    "sign": first.get("sign"),
                    "sign_lord": first_sign_lord,
                    "house": first.get("whole_sign_house"),
                    "victory_side": first_side,
                },
                "second": {
                    "planet": second_name,
                    "sign": second.get("sign"),
                    "sign_lord": second_sign_lord,
                    "house": second.get("whole_sign_house"),
                    "victory_side": second_side,
                },
                "victory_house_relevance": (
                    victory_house_relevance
                ),
                "especially_relevant": bool(
                    victory_house_relevance
                ),
                "book_effect": (
                    "Mutual reception strengthens both planets. A "
                    "benefic in the pair becomes eligible for the "
                    "victory-house technique."
                ),
                "tier": "Supplemental to Tier 1",
                "fixed_points_defined": False,
                "pdf_pages": TIER1_PDF_PAGES[
                    "parivartana"
                ],
            }
            pairs.append(pair)

            for planet_name in (first_name, second_name):
                if planet_name in TIER1_NATURAL_BENEFICS:
                    eligible_benefics.add(planet_name)

    return {
        "status": "Pass",
        "method": "Chapter3Parivartana",
        "detected": bool(pairs),
        "pairs": pairs,
        "eligible_benefics": sorted(eligible_benefics),
        "fixed_points_defined": False,
        "points_applied": False,
        "pdf_pages": TIER1_PDF_PAGES["parivartana"],
    }


def tier1_independent_strength_sources(
    snapshot: dict[str, Any],
    parivartana_eligible: bool,
) -> list[str]:
    """
    Return independent strength sources without double-counting Mercury's
    exaltation and own-sign condition in Virgo as two separate dignities.
    """

    sources: list[str] = []

    if snapshot.get("exalted") is True:
        sources.append("exaltation")
    elif snapshot.get("own_sign") is True:
        sources.append("own sign")
    elif snapshot.get("moolatrikona") is True:
        sources.append("moolatrikona")

    if snapshot.get("retrograde") is True:
        sources.append("retrogression")

    if snapshot.get("dig_bala") is True:
        sources.append("dig bala")

    if snapshot.get("own_nakshatra") is True:
        sources.append("own nakshatra")

    if parivartana_eligible:
        sources.append("parivartana")

    return sources


def calculate_tier1_victory_houses(
    snapshots: dict[str, dict[str, Any]],
    parivartana: dict[str, Any],
) -> dict[str, Any]:
    """Apply the book's conservative signed victory-house point method."""

    parivartana_benefics = set(
        parivartana.get("eligible_benefics", [])
    )
    ledger: list[dict[str, Any]] = []
    manual_candidates: list[dict[str, Any]] = []
    unavailable: list[dict[str, Any]] = []
    favourite_points = 0.0
    underdog_points = 0.0

    for planet_name, snapshot in snapshots.items():
        if snapshot.get("status") != "Pass":
            unavailable.append({
                "planet": planet_name,
                "reason": snapshot.get("error"),
            })
            continue

        side = snapshot.get("victory_side")

        if not side:
            continue

        natural_malefic = (
            planet_name in TIER1_NATURAL_MALEFICS
        )
        natural_benefic = (
            planet_name in TIER1_NATURAL_BENEFICS
        )
        parivartana_eligible = (
            planet_name in parivartana_benefics
        )
        strength_sources = (
            tier1_independent_strength_sources(
                snapshot,
                parivartana_eligible,
            )
        )

        auto_eligible = natural_malefic
        eligibility_reason = (
            "Natural malefic in a victory house."
            if natural_malefic
            else None
        )

        if natural_benefic:
            exaggerated_sources = [
                source
                for source in strength_sources
                if source in {
                    "exaltation",
                    "retrogression",
                    "parivartana",
                }
            ]

            if exaggerated_sources:
                auto_eligible = True
                eligibility_reason = (
                    "Benefic made eligible by "
                    + ", ".join(exaggerated_sources)
                    + "."
                )
            elif (
                snapshot.get("own_sign") is True
                or snapshot.get("own_nakshatra") is True
                or snapshot.get("dig_bala") is True
            ):
                manual_candidates.append({
                    "planet": planet_name,
                    "side": side,
                    "house": snapshot.get(
                        "whole_sign_house"
                    ),
                    "reason": (
                        "Stable/strong benefic without the stricter "
                        "automatic exaggerated condition. The book "
                        "allows judgment but later prefers exaltation "
                        "or retrogression."
                    ),
                    "strength_sources": strength_sources,
                    "automatic_points": 0.0,
                })

        if planet_name == "Moon":
            manual_candidates.append({
                "planet": "Moon",
                "side": side,
                "house": snapshot.get(
                    "whole_sign_house"
                ),
                "reason": (
                    "The author normally excludes the Moon from "
                    "automatic victory-house scoring; exceptional "
                    "phase/strength requires manual judgment."
                ),
                "strength_sources": strength_sources,
                "automatic_points": 0.0,
            })
            continue

        if not auto_eligible:
            continue

        if snapshot.get("debilitated") is True:
            points = 2.0
            point_reason = (
                "Book example value for a debilitated qualifying "
                "victory-house planet."
            )
            bonus_sources: list[str] = []
        else:
            points = 2.5
            point_reason = (
                "Book base value for an eligible victory-house planet."
            )

            if natural_benefic:
                # One exaggerated source makes the benefic eligible.
                # Additional independent sources add 0.5, capped at the
                # book's general Tier 1 upper range.
                eligibility_sources = [
                    source
                    for source in strength_sources
                    if source in {
                        "exaltation",
                        "retrogression",
                        "parivartana",
                    }
                ]
                consumed = (
                    eligibility_sources[0]
                    if eligibility_sources
                    else None
                )
                bonus_sources = [
                    source
                    for source in strength_sources
                    if source != consumed
                ]
            else:
                bonus_sources = list(strength_sources)

            points += min(
                len(bonus_sources) * 0.5,
                1.5,
            )
            points = min(points, 4.0)

            if bonus_sources:
                point_reason += (
                    " Additional half-point strength sources: "
                    + ", ".join(bonus_sources)
                    + "."
                )

        if side == "Favourite":
            favourite_points += points
        else:
            underdog_points += points

        ledger.append({
            "rule": (
                f"{planet_name} in "
                f"{snapshot.get('whole_sign_house')}"
            ),
            "planet": planet_name,
            "house": snapshot.get(
                "whole_sign_house"
            ),
            "side": side,
            "natural_class": snapshot.get(
                "natural_class"
            ),
            "eligibility": eligibility_reason,
            "strength_sources": strength_sources,
            "debilitated": snapshot.get(
                "debilitated"
            ),
            "combust": snapshot.get("combust"),
            "points": round(points, 2),
            "signed_points": round(
                tier1_signed_value(side, points),
                2,
            ),
            "point_reason": point_reason,
            "combustion_adjustment_applied": False,
            "combustion_note": (
                "Combustion is reported as instability/affliction. "
                "No fixed numerical reduction is invented."
                if snapshot.get("combust") is True
                else None
            ),
            "shadbala": snapshot.get("shadbala"),
            "pdf_pages": TIER1_PDF_PAGES[
                "victory_houses"
            ],
        })

    favourite_points = round(favourite_points, 2)
    underdog_points = round(underdog_points, 2)

    return {
        "status": (
            "Pass" if not unavailable else "Partial"
        ),
        "method": "Chapter3VictoryHouseLedger",
        "book_tier": 1,
        "favourite_houses": sorted(
            TIER1_FAVOURITE_VICTORY_HOUSES
        ),
        "underdog_houses": sorted(
            TIER1_UNDERDOG_VICTORY_HOUSES
        ),
        "ledger": ledger,
        "manual_candidates": manual_candidates,
        "unavailable_planets": unavailable,
        "favourite_points": favourite_points,
        "underdog_points": underdog_points,
        "signed_favourite_total": round(
            favourite_points - underdog_points,
            2,
        ),
        "automatic_point_scope": (
            "Eligible victory-house planets only."
        ),
        "sky_pky_included_in_total": False,
        "supplemental_yogas_included_in_total": False,
        "pdf_pages": TIER1_PDF_PAGES[
            "victory_houses"
        ],
    }


def calculate_tier1_sky_pky(
    snapshots: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    """Detect book-locked SKY and PKY around Houses 1 and 7."""

    occupancy: dict[int, list[str]] = {
        house: [] for house in range(1, 13)
    }

    for planet_name, snapshot in snapshots.items():
        if snapshot.get("status") != "Pass":
            continue

        house = snapshot.get("whole_sign_house_number")

        if isinstance(house, int):
            occupancy[house].append(planet_name)

    for planets_in_house in occupancy.values():
        planets_in_house.sort(
            key=lambda item: PLANET_ORDER.get(item, 99)
        )

    results: dict[str, Any] = {}

    for target_house, side in (
        (1, "Favourite"),
        (7, "Underdog"),
    ):
        previous_house = (
            12 if target_house == 1 else target_house - 1
        )
        next_house = (
            1 if target_house == 12 else target_house + 1
        )

        previous_planets = occupancy[previous_house]
        next_planets = occupancy[next_house]

        previous_benefics = [
            planet
            for planet in previous_planets
            if planet in TIER1_NATURAL_BENEFICS
        ]
        next_benefics = [
            planet
            for planet in next_planets
            if planet in TIER1_NATURAL_BENEFICS
        ]

        sky_formed = bool(
            previous_benefics and next_benefics
        )
        sky_mild_marring = sorted({
            planet
            for planet in previous_planets + next_planets
            if planet in {"Sun", "Rahu", "Ketu"}
        })
        sky_heavy_marring = sorted({
            planet
            for planet in previous_planets + next_planets
            if planet in {"Mars", "Saturn"}
        })
        debilitated_sky_benefics = sorted({
            planet
            for planet in previous_benefics + next_benefics
            if snapshots.get(
                planet,
                {},
            ).get("debilitated") is True
        })

        if not sky_formed:
            sky_condition = "Absent"
        elif sky_heavy_marring:
            sky_condition = "Heavily afflicted"
        elif sky_mild_marring or debilitated_sky_benefics:
            sky_condition = "Diminished"
        else:
            sky_condition = "Full"

        previous_classical_malefics = [
            planet
            for planet in previous_planets
            if planet in TIER1_CLASSICAL_PKY_MALEFICS
        ]
        next_classical_malefics = [
            planet
            for planet in next_planets
            if planet in TIER1_CLASSICAL_PKY_MALEFICS
        ]
        previous_nodes = [
            planet
            for planet in previous_planets
            if planet in TIER1_NODE_MALEFICS
        ]
        next_nodes = [
            planet
            for planet in next_planets
            if planet in TIER1_NODE_MALEFICS
        ]

        pky_formed = bool(
            previous_classical_malefics
            and next_classical_malefics
        )
        any_malefic_each_side = bool(
            (
                previous_classical_malefics
                or previous_nodes
            )
            and (
                next_classical_malefics
                or next_nodes
            )
        )
        node_only_partial = bool(
            any_malefic_each_side and not pky_formed
        )
        pky_intensified_by_nodes = bool(
            pky_formed
            and (previous_nodes or next_nodes)
        )

        if pky_formed and pky_intensified_by_nodes:
            pky_condition = "Full and node-intensified"
        elif pky_formed:
            pky_condition = "Full"
        elif node_only_partial:
            pky_condition = "Partial node pattern"
        else:
            pky_condition = "Absent"

        results[side] = {
            "side": side,
            "target_house": f"House{target_house}",
            "flanking_houses": [
                f"House{previous_house}",
                f"House{next_house}",
            ],
            "flanking_occupancy": {
                f"House{previous_house}": (
                    previous_planets
                ),
                f"House{next_house}": next_planets,
            },
            "sky": {
                "formed": sky_formed,
                "condition": sky_condition,
                "benefics_previous_side": (
                    previous_benefics
                ),
                "benefics_next_side": next_benefics,
                "mild_or_shadow_marring": (
                    sky_mild_marring
                ),
                "heavy_marring": sky_heavy_marring,
                "debilitated_benefics": (
                    debilitated_sky_benefics
                ),
                "book_effect": (
                    "Protects the represented team and often "
                    "improves performance beyond expectation."
                    if sky_formed
                    else "No full SKY."
                ),
                "book_point_guidance": (
                    "Full SKY/PKY tier is generally 7-9; a "
                    "heavily afflicted SKY may be only 3-4 or "
                    "less. Exact value requires chart judgment."
                ),
                "automatic_points_applied": False,
            },
            "pky": {
                "formed": pky_formed,
                "condition": pky_condition,
                "classical_malefics_previous_side": (
                    previous_classical_malefics
                ),
                "classical_malefics_next_side": (
                    next_classical_malefics
                ),
                "nodes_previous_side": previous_nodes,
                "nodes_next_side": next_nodes,
                "node_only_side_is_full_pky": False,
                "intensified_by_nodes": (
                    pky_intensified_by_nodes
                ),
                "book_effect": (
                    "Makes the represented team vulnerable; "
                    "PKY is generally less potent for harm than "
                    "SKY is for protection."
                    if pky_formed
                    else (
                        "A node-only flank is not treated as a "
                        "full PKY."
                        if node_only_partial
                        else "No full PKY."
                    )
                ),
                "book_point_guidance": (
                    "Tier 2 range is generally 7-9, but the "
                    "book does not make PKY mechanically equal "
                    "to a clean SKY."
                ),
                "automatic_points_applied": False,
            },
            "cancellation_or_mixed_testimony": (
                "Both SKY and PKY are present; retain both as "
                "cumulative contradictory testimony."
                if sky_formed and pky_formed
                else None
            ),
            "pdf_pages": TIER1_PDF_PAGES["sky_pky"],
        }

    return {
        "status": "Pass",
        "method": "Chapter3SkyPky",
        "book_tier": 2,
        "natural_benefics_used": sorted(
            TIER1_NATURAL_BENEFICS
        ),
        "moon_automatically_used_as_sky_benefic": False,
        "pky_full_formation_policy": (
            "Each flank must contain Sun, Mars or Saturn. "
            "Rahu/Ketu can mar or intensify but cannot alone "
            "supply a full flank."
        ),
        "sides": results,
        "points_applied": False,
        "pdf_pages": TIER1_PDF_PAGES["sky_pky"],
    }


def tier1_apparent_magnitude(
    julian_day_ut: float | None,
    planet_name: str,
) -> dict[str, Any]:
    """Calculate apparent magnitude for a planetary-war pair."""

    if (
        not SWISSEPH_AVAILABLE
        or julian_day_ut is None
        or planet_name not in TIER1_SWISSEPH_BODY_IDS
    ):
        return {
            "status": "Unavailable",
            "planet": planet_name,
            "magnitude": None,
            "error": (
                "Swiss Ephemeris apparent magnitude is unavailable."
            ),
        }

    try:
        attributes = swe.pheno_ut(
            julian_day_ut,
            TIER1_SWISSEPH_BODY_IDS[planet_name],
        )
        magnitude = float(attributes[4])

        return {
            "status": "Pass",
            "planet": planet_name,
            "magnitude": round(magnitude, 8),
            "brighter_when": "Lower magnitude",
            "error": None,
        }
    except Exception as error:
        return {
            "status": "Unavailable",
            "planet": planet_name,
            "magnitude": None,
            "error": str(error),
        }


def calculate_tier1_planetary_war(
    std_time: str,
    ascendant_longitude: float,
    snapshots: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    """Detect relevant <1 degree wars and determine the brighter body."""

    ascendant_sign_index = int(
        normalise_degrees(ascendant_longitude) // 30.0
    )
    relevant_house_sides = {
        1: "Favourite",
        10: "Favourite",
        7: "Underdog",
        4: "Underdog",
    }
    lord_roles: dict[str, list[dict[str, Any]]] = {}

    for house_number, side in relevant_house_sides.items():
        sign_index = (
            ascendant_sign_index + house_number - 1
        ) % 12
        sign_name = ZODIAC_SIGNS[sign_index]
        lord = SIGN_LORDS[sign_name]
        lord_roles.setdefault(lord, []).append({
            "house": f"House{house_number}",
            "sign": sign_name,
            "side": side,
        })

    try:
        parsed_time = parse_std_time_to_utc(std_time)
        julian_day_ut = parsed_time.get("julian_day_ut")
        time_error = None
    except Exception as error:
        julian_day_ut = None
        time_error = str(error)

    wars: list[dict[str, Any]] = []
    unavailable_planets: list[str] = []

    for index, first_name in enumerate(
        TIER1_PLANETARY_WAR_PLANETS
    ):
        first = snapshots.get(first_name, {})

        if first.get("status") != "Pass":
            unavailable_planets.append(first_name)
            continue

        for second_name in (
            TIER1_PLANETARY_WAR_PLANETS[index + 1:]
        ):
            second = snapshots.get(second_name, {})

            if second.get("status") != "Pass":
                unavailable_planets.append(second_name)
                continue

            first_relevant = first_name in lord_roles
            second_relevant = second_name in lord_roles

            if not (first_relevant or second_relevant):
                continue

            distance = angular_distance(
                first["sidereal_longitude"],
                second["sidereal_longitude"],
            )

            if distance > 1.0 + 1e-9:
                continue

            first_magnitude = tier1_apparent_magnitude(
                julian_day_ut,
                first_name,
            )
            second_magnitude = tier1_apparent_magnitude(
                julian_day_ut,
                second_name,
            )
            winner = None
            loser = None
            winner_method = None

            if (
                first_magnitude.get("status") == "Pass"
                and second_magnitude.get("status") == "Pass"
            ):
                first_value = first_magnitude["magnitude"]
                second_value = second_magnitude["magnitude"]

                if abs(first_value - second_value) > 1e-9:
                    winner = (
                        first_name
                        if first_value < second_value
                        else second_name
                    )
                    loser = (
                        second_name
                        if winner == first_name
                        else first_name
                    )
                    winner_method = (
                        "Swiss Ephemeris apparent magnitude; "
                        "lower magnitude is brighter."
                    )

            first_sides = sorted({
                role["side"]
                for role in lord_roles.get(first_name, [])
            })
            second_sides = sorted({
                role["side"]
                for role in lord_roles.get(second_name, [])
            })
            cross_side = bool(
                first_sides
                and second_sides
                and set(first_sides) != set(second_sides)
            )
            winner_sides = sorted({
                role["side"]
                for role in lord_roles.get(winner, [])
            }) if winner else []
            loser_sides = sorted({
                role["side"]
                for role in lord_roles.get(loser, [])
            }) if loser else []

            wars.append({
                "planets": [first_name, second_name],
                "angular_distance": round(distance, 8),
                "within_one_degree": True,
                "first_roles": lord_roles.get(
                    first_name,
                    [],
                ),
                "second_roles": lord_roles.get(
                    second_name,
                    [],
                ),
                "cross_side_war": cross_side,
                "first_magnitude": first_magnitude,
                "second_magnitude": second_magnitude,
                "winner": winner,
                "loser": loser,
                "winner_method": winner_method,
                "winner_represented_sides": winner_sides,
                "loser_represented_sides": loser_sides,
                "book_effect": (
                    "Every relevant ruler in war is destabilized. "
                    "For a cross-side war, the brighter planet's "
                    "side receives an edge while the loser's side "
                    "is tarnished, not automatically defeated."
                ),
                "fixed_points_defined": False,
                "points_applied": False,
                "pdf_pages": TIER1_PDF_PAGES[
                    "planetary_war"
                ],
            })

    return {
        "status": (
            "Pass"
            if time_error is None
            else "Partial"
        ),
        "method": "Chapter3PlanetaryWar",
        "orb_degrees": 1.0,
        "relevant_house_lords": lord_roles,
        "wars": wars,
        "detected": bool(wars),
        "winner_standard": (
            "Brighter planet, measured by lower apparent magnitude."
        ),
        "lesser_longitude_fallback_used": False,
        "unavailable_planets": sorted(
            set(unavailable_planets)
        ),
        "time_error": time_error,
        "fixed_points_defined": False,
        "points_applied": False,
        "pdf_pages": TIER1_PDF_PAGES[
            "planetary_war"
        ],
    }


def calculate_tier1_combinations(
    std_time: str,
    rashi_placidus: dict[str, Any],
    planets: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    """
    Calculate Chapter 3 victory houses, SKY/PKY, parivartana and
    relevant planetary war using exact Lahiri D1 longitudes.
    """

    if rashi_placidus.get("status") != "Pass":
        return {
            "status": "Unavailable",
            "method": "BookLockedChapter3Tier1Engine",
            "book_chapter": 3,
            "ayanamsa": "Lahiri",
            "error": "Exact Lahiri Ascendant longitude is unavailable.",
            "victory_houses": {},
            "sky_pky": {},
            "parivartana": {},
            "planetary_war": {},
        }

    ascendant_longitude = rashi_placidus.get(
        "cusps",
        {},
    ).get(
        "House1",
        {},
    ).get("sidereal_longitude")

    if not isinstance(ascendant_longitude, (int, float)):
        return {
            "status": "Unavailable",
            "method": "BookLockedChapter3Tier1Engine",
            "book_chapter": 3,
            "ayanamsa": "Lahiri",
            "error": "Exact Lahiri Ascendant longitude is unavailable.",
            "victory_houses": {},
            "sky_pky": {},
            "parivartana": {},
            "planetary_war": {},
        }

    ascendant_longitude = normalise_degrees(
        float(ascendant_longitude)
    )
    snapshots = {
        planet_name: tier1_planet_snapshot(
            planet_name,
            result,
            ascendant_longitude,
        )
        for planet_name, result in planets.items()
    }
    missing_required_planets = [
        planet_name
        for planet_name in PLANETS
        if planet_name not in snapshots
        or snapshots[planet_name].get("status") != "Pass"
    ]

    parivartana = calculate_tier1_parivartana(
        snapshots
    )
    victory_houses = calculate_tier1_victory_houses(
        snapshots,
        parivartana,
    )
    sky_pky = calculate_tier1_sky_pky(snapshots)
    planetary_war = calculate_tier1_planetary_war(
        std_time,
        ascendant_longitude,
        snapshots,
    )

    layer_status = (
        "Pass"
        if not missing_required_planets
        and planetary_war.get("status") == "Pass"
        else "Partial"
    )

    return {
        "status": layer_status,
        "method": "BookLockedChapter3Tier1Engine",
        "book_chapter": 3,
        "ayanamsa": "Lahiri",
        "house_system": "Whole-sign rashi from exact Lahiri Ascendant",
        "assignment": {
            "House1": "Favourite",
            "House7": "Underdog",
        },
        "ascendant": {
            "sidereal_longitude": round(
                ascendant_longitude,
                8,
            ),
            **sign_details_from_longitude(
                ascendant_longitude
            ),
        },
        "planet_snapshots": snapshots,
        "victory_houses": victory_houses,
        "sky_pky": sky_pky,
        "parivartana": parivartana,
        "planetary_war": planetary_war,
        "automatic_signed_total": (
            victory_houses.get(
                "signed_favourite_total"
            )
        ),
        "automatic_signed_total_scope": (
            "Victory-house planets only. SKY/PKY, parivartana "
            "and planetary war remain separate because their "
            "exact numerical adjustment is contextual."
        ),
        "missing_required_planets": (
            missing_required_planets
        ),
        "manual_review_items": {
            "moon_victory_house": (
                "Reported but not automatically scored."
            ),
            "combustion": (
                "Reported without an invented fixed reduction."
            ),
            "shadbala": (
                "Returned as evidence without an invented threshold."
            ),
            "own_sign_or_own_nakshatra_only_benefic": (
                "Reported as a manual candidate unless another "
                "exaggerated condition is present."
            ),
        },
        "pdf_pages": sorted({
            page
            for pages in TIER1_PDF_PAGES.values()
            for page in pages
        }),
        "points_applied": True,
        "error": None,
    }


def opposite_contest_side(side: str | None) -> str | None:
    if side == "Favourite":
        return "Underdog"

    if side == "Underdog":
        return "Favourite"

    return None


def d9_house_for_longitude(
    d9_longitude: float,
    d9_lagna_longitude: float,
) -> int:
    """Place a D9 longitude in a whole-sign Navamsha house."""

    return whole_sign_house_for_longitude(
        d9_longitude,
        d9_lagna_longitude,
    )



def contact_orb_strength(
    angular_distance: Any,
) -> str:
    """
    Classify only the exact book-stated cusp thresholds.

    Printed pages 72-73:
    - within 0°30' is extra-special
    - under 1° is very strong
    - otherwise the contact remains within the normal book orb

    No invented outer-edge percentage or automatic point adjustment is used.
    """

    if not isinstance(angular_distance, (int, float)):
        return "Unknown"

    distance = abs(float(angular_distance))

    if distance <= 0.5 + 1e-9:
        return "Exceptional (within 0°30')"

    if distance < 1.0 - 1e-9:
        return "Very strong (under 1°)"

    return "Within book orb"


def signed_interval_for_effect(
    effect: dict[str, Any],
) -> list[float] | None:
    """
    Convert one book range into favourite-signed interval form.

    Positive supports the favourite; negative supports the underdog.
    Research-only, weakened or otherwise non-decision-grade testimony is not
    assigned a numerical interval.
    """

    if (
        effect.get("decision_eligible") is not True
        or effect.get("automatic_decision_use") is False
        or effect.get("research_only") is True
    ):
        return None

    supports = effect.get("supports")
    point_range = effect.get("book_point_range")

    if (
        supports not in DECISION_SIDES
        or not isinstance(point_range, (list, tuple))
        or len(point_range) != 2
        or not all(
            isinstance(value, (int, float))
            for value in point_range
        )
    ):
        return None

    low = float(min(point_range))
    high = float(max(point_range))

    if supports == "Favourite":
        return [round(low, 4), round(high, 4)]

    return [round(-high, 4), round(-low, 4)]


def finalise_contact_effect(
    effect: dict[str, Any],
    *,
    body: str,
    cusp: str,
    family: str,
    angular_distance: Any,
    orb_limit: Any,
) -> dict[str, Any]:
    """Add common decision-grade and audit metadata to one cusp effect."""

    result = dict(effect)
    supports = result.get("supports")

    eligible = bool(
        result.get(
            "decision_eligible",
            supports in DECISION_SIDES,
        )
    )

    if (
        result.get("automatic_decision_use") is False
        or result.get("research_only") is True
        or supports not in DECISION_SIDES
    ):
        eligible = False

    result["decision_eligible"] = eligible
    result.setdefault("research_only", not eligible)
    result.setdefault(
        "automatic_decision_use",
        eligible,
    )
    result["orb_strength"] = contact_orb_strength(
        angular_distance
    )
    result["independence_family"] = family
    result["independence_key"] = (
        f"{family}:{body}:{cusp}"
    )
    result["angular_distance"] = angular_distance
    result["orb_limit"] = orb_limit
    result["signed_interval"] = signed_interval_for_effect(
        result
    )

    return result


def deduplicate_d1_node_axis_contacts(
    contacts: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """
    Count Rahu/Ketu once per rashi axis and supported side.

    Printed page 67 says Rahu and Ketu are always opposite and, in practice,
    their opposite cusp effects are taken as one. Geometry is retained for
    both nodes, but only the tighter same-direction testimony is eligible for
    automatic aggregation. If stolen-cusp redirection makes the nodes support
    opposite sides, both directions remain visible.
    """

    groups: dict[
        tuple[str, str],
        list[tuple[int, dict[str, Any]]],
    ] = {}

    for index, contact in enumerate(contacts):
        body = contact.get("body")
        effect = contact.get("book_effect") or {}
        axis = contact.get("axis") or effect.get("axis")
        supports = effect.get("supports")

        if (
            body not in {"Rahu", "Ketu"}
            or axis not in {"1/7", "6/12", "10/4"}
            or supports not in DECISION_SIDES
            or effect.get("decision_eligible") is not True
        ):
            continue

        groups.setdefault(
            (str(axis), str(supports)),
            [],
        ).append((index, contact))

    for (axis, supports), grouped in groups.items():
        if len(grouped) <= 1:
            _, only = grouped[0]
            effect = dict(only.get("book_effect") or {})
            effect["node_axis_counted_once"] = True
            effect["node_axis_group"] = (
                f"{axis}:{supports}"
            )
            only["book_effect"] = effect
            continue

        grouped.sort(
            key=lambda item: (
                float(
                    item[1].get("angular_distance")
                    if isinstance(
                        item[1].get("angular_distance"),
                        (int, float),
                    )
                    else 999
                ),
                0 if item[1].get("body") == "Rahu" else 1,
            )
        )
        retained_index, retained = grouped[0]
        retained_effect = dict(
            retained.get("book_effect") or {}
        )
        retained_effect.update({
            "node_axis_counted_once": True,
            "node_axis_group": f"{axis}:{supports}",
            "node_axis_duplicate_count": len(grouped) - 1,
        })
        retained["book_effect"] = retained_effect

        retained_label = (
            f"{retained.get('body')}@"
            f"{retained.get('cusp')}"
        )

        for duplicate_index, duplicate in grouped[1:]:
            duplicate_effect = dict(
                duplicate.get("book_effect") or {}
            )
            duplicate_effect.update({
                "decision_eligible_before_node_dedup": (
                    duplicate_effect.get(
                        "decision_eligible"
                    )
                ),
                "decision_eligible": False,
                "automatic_decision_use": False,
                "node_axis_counted_once": False,
                "node_axis_duplicate": True,
                "node_axis_group": f"{axis}:{supports}",
                "duplicate_of": retained_label,
                "signed_interval": None,
                "decision_reason": (
                    "Rahu/Ketu opposite-axis testimony is "
                    "counted once under printed page 67."
                ),
            })
            duplicate["book_effect"] = duplicate_effect

    return contacts


def contact_to_indicator(
    contact: dict[str, Any],
    *,
    source: str,
    tier: int,
) -> dict[str, Any]:
    """Project one contact into the tier-aware aggregation format."""

    effect = contact.get("book_effect") or {}

    return {
        "source": source,
        "body": contact.get("body"),
        "cusp": contact.get("cusp"),
        "effective_cusp": contact.get(
            "effective_cusp"
        ),
        "tier": tier,
        "supports": effect.get("supports"),
        "decision_eligible": effect.get(
            "decision_eligible"
        ),
        "research_only": effect.get(
            "research_only",
            False,
        ),
        "automatic_decision_use": effect.get(
            "automatic_decision_use",
            True,
        ),
        "book_point_range": effect.get(
            "book_point_range"
        ),
        "signed_interval": effect.get(
            "signed_interval"
        ),
        "orb_strength": effect.get(
            "orb_strength"
        ),
        "angular_distance": contact.get(
            "angular_distance"
        ),
        "contact_strength": effect.get(
            "contact_strength"
        ),
        "independence_family": effect.get(
            "independence_family"
        ),
        "independence_key": effect.get(
            "independence_key"
        ),
        "decision_reason": effect.get(
            "decision_reason"
        ),
    }

def d9_cusp_effect(
    body_name: str,
    cusp_name: str,
    *,
    motion: str | None = None,
) -> dict[str, Any]:
    """
    Apply Table 5.3 and adjacent Chapter 5 text to one D9 1/7 contact.

    Research-caution or undefined bodies remain visible but are excluded from
    automatic direction and point intervals. Exact point values inside the
    book's 14-18 and 12-15 ranges are never invented.
    """

    represented_side = (
        "Favourite"
        if cusp_name == "D9Lagna"
        else "Underdog"
    )
    opposing_side = opposite_contest_side(
        represented_side
    )
    normalised_motion = (
        motion.strip().lower()
        if isinstance(motion, str)
        else None
    )

    direction = "Uncertain"
    effect = "Research/undefined"
    supports = None
    rule_status = "Not defined by book"
    reliability = "Unavailable"
    decision_eligible = False
    research_only = True
    decision_reason = (
        "No decision-grade standalone D9 rule is available."
    )
    note = None

    if body_name == "Sun":
        direction = "Harms cusp side"
        effect = "Burns the team; cautious and often low-scoring."
        supports = opposing_side
        rule_status = "Book-defined"
        reliability = "Moderate"
        decision_eligible = True
        research_only = False
        decision_reason = "Explicit Table 5.3 rule."
    elif body_name == "Moon":
        direction = "Harms cusp side"
        effect = "Lazy or unstable influence."
        supports = opposing_side
        rule_status = "Book-defined with research caution"
        reliability = "Research caution"
        decision_reason = (
            "Printed page 113 says more research is needed; "
            "report only, do not auto-score."
        )
    elif body_name == "Mars":
        direction = "Harms cusp side"
        effect = "Frustration, anger and self-undoing."
        supports = opposing_side
        rule_status = "Book-defined"
        reliability = "Strong"
        decision_eligible = True
        research_only = False
        decision_reason = "Explicit Table 5.3 rule."
    elif body_name == "Rahu":
        direction = "Supports cusp side"
        effect = "Ambition and desire to win."
        supports = represented_side
        rule_status = "Book-defined"
        reliability = "Reduced shadow-graha force"
        decision_eligible = True
        research_only = False
        decision_reason = "Explicit Table 5.3 rule."
    elif body_name == "Jupiter":
        direction = "Supports cusp side"
        effect = "Grace, luck and a positive attitude."
        supports = represented_side
        rule_status = "Book-defined"
        reliability = "Strong"
        decision_eligible = True
        research_only = False
        decision_reason = "Explicit Table 5.3 rule."
    elif body_name == "Saturn":
        direction = "Harms cusp side"
        effect = "Restricts, slows and depresses the team."
        supports = opposing_side
        rule_status = "Book-defined"
        reliability = "Strong"
        decision_eligible = True
        research_only = False
        decision_reason = "Explicit Table 5.3 rule."
    elif body_name == "Mercury":
        direction = "Supports cusp side"
        effect = "Skill, speed and cleverness."
        supports = represented_side
        rule_status = "Book-defined"
        reliability = "Strong"
        decision_eligible = True
        research_only = False
        decision_reason = "Explicit Table 5.3 rule."
    elif body_name == "Ketu":
        direction = "Harms cusp side"
        effect = "Confusion and unusual circumstances leading to defeat."
        supports = opposing_side
        rule_status = "Book-defined"
        reliability = "Reduced shadow-graha force"
        decision_eligible = True
        research_only = False
        decision_reason = "Explicit Table 5.3 rule."
    elif body_name == "Venus":
        direction = "Harms cusp side"
        effect = "Laziness, complacency and inattention."
        supports = opposing_side
        rule_status = "Book-defined"
        reliability = "Milder negative"
        decision_eligible = True
        research_only = False
        decision_reason = "Explicit Table 5.3 rule."
    elif body_name == "Uranus":
        if normalised_motion == "direct":
            direction = "Supports cusp side"
            effect = "Galvanizing positive current."
            supports = represented_side
            rule_status = "Book-defined"
            reliability = "Research-sensitive outer planet"
            decision_eligible = True
            research_only = False
            decision_reason = "Explicit direct-motion Chapter 5 rule."
        elif normalised_motion == "retrograde":
            direction = "Harms cusp side"
            effect = "Retrograde current reverses the normal boost."
            supports = opposing_side
            rule_status = "Book-defined"
            reliability = "Research-sensitive outer planet"
            decision_eligible = True
            research_only = False
            decision_reason = "Explicit retrograde Chapter 5 rule."
        else:
            direction = "Uncertain"
            effect = "Stationary or unknown Uranus is not a clean signal."
            supports = None
            rule_status = "Book caution"
            reliability = "Uncertain/kutila"
            decision_reason = "Stationary or unknown motion is not decision-grade."
    elif body_name == "Neptune":
        if normalised_motion == "retrograde":
            direction = "Supports cusp side"
            effect = "Retrograde Neptune can inspire and push ahead."
            supports = represented_side
            rule_status = "Book-defined"
            reliability = "Research-sensitive outer planet"
            decision_eligible = True
            research_only = False
            decision_reason = "Explicit retrograde Chapter 5 rule."
        elif normalised_motion == "direct":
            direction = "Harms cusp side"
            effect = "Sleep, smoke and confusion."
            supports = opposing_side
            rule_status = "Book-defined"
            reliability = "Research-sensitive outer planet"
            decision_eligible = True
            research_only = False
            decision_reason = "Explicit direct-motion Chapter 5 rule."
        else:
            direction = "Uncertain"
            effect = "Stationary or unknown Neptune is not a clean signal."
            supports = None
            rule_status = "Book caution"
            reliability = "Uncertain/kutila"
            decision_reason = "Stationary or unknown motion is not decision-grade."
    elif body_name == "Pluto":
        direction = "Harms cusp side"
        effect = "Heaviness, intensity and misfortune."
        supports = opposing_side
        rule_status = "Book-defined with research caution"
        reliability = "Research-sensitive outer planet"
        decision_reason = (
            "Printed page 114 says more research is needed; "
            "report only, do not auto-score."
        )
    elif body_name == "Upaketu":
        direction = "Harms cusp side"
        effect = "Acts like Ketu and spoils the represented team's luck."
        supports = opposing_side
        rule_status = "Book-defined"
        reliability = "Invisible upagraha"
        decision_eligible = True
        research_only = False
        decision_reason = "Explicit Chapter 5 rule."
    elif body_name == "Gulika":
        direction = "Harms cusp side"
        effect = "Indicates defeat for the represented side."
        supports = opposing_side
        rule_status = "Book-defined"
        reliability = "Invisible upagraha"
        decision_eligible = True
        research_only = False
        decision_reason = "Explicit Chapter 5 rule."
    elif body_name == "Chiron":
        if normalised_motion == "retrograde":
            direction = "Harms cusp side"
            effect = (
                "Retrograde Chiron is negative only when a very tight D1 "
                "contact transfers into D9."
            )
            supports = opposing_side
            rule_status = "Book-defined double-whammy example"
            reliability = "Transfer-only rule"
            decision_reason = (
                "Not a standalone D9 rule; eligible only through an "
                "independently detected double whammy."
            )
        else:
            note = (
                "Chapter 5 does not give a standalone direct-Chiron D9 "
                "cusp rule."
            )
    elif body_name == "Ceres":
        note = (
            "Chapter 5 does not define a standalone Ceres D9 cusp effect."
        )

    invisible = body_name in D9_INVISIBLE_CUSP_BODIES
    point_range = (
        [12.0, 15.0]
        if invisible and decision_eligible
        else [14.0, 18.0]
        if decision_eligible
        else None
    )

    return {
        "body": body_name,
        "cusp": cusp_name,
        "represented_side": represented_side,
        "direction": direction,
        "supports": supports,
        "effect": effect,
        "rule_status": rule_status,
        "reliability": reliability,
        "motion": motion,
        "tier": 3,
        "book_point_range": point_range,
        "decision_eligible": decision_eligible,
        "research_only": research_only,
        "automatic_decision_use": decision_eligible,
        "decision_reason": decision_reason,
        "exact_points_applied": False,
        "exact_point_reason": (
            "The book gives a range and says orb tightness and planetary "
            "quality require judgment; no exact value is invented."
            if decision_eligible
            else "Research-only or undefined testimony is not scored."
        ),
        "pdf_pages": [112, 113, 114, 115, 116, 158, 159],
        "note": note,
    }


def d1_classical_cusp_effect(
    body_name: str,
    cusp_name: str,
) -> dict[str, Any]:
    """
    Apply explicit Chapter 4 classical-planet cusp rules.

    Mercury, Moon and Mars on 4/10 are retained as research testimony but
    excluded from automatic scoring. Rahu and Ketu use the book's reduced
    invisible-graha rashi value of 7 rather than the visible 7-9 range.
    """

    metadata = SENSITIVE_CUSP_DETAILS.get(cusp_name)

    if not metadata:
        return {
            "body": body_name,
            "cusp": cusp_name,
            "direction": "Undefined",
            "supports": None,
            "rule_status": "Cusp is outside the six primary axes.",
            "decision_eligible": False,
            "research_only": True,
            "automatic_decision_use": False,
            "book_point_range": None,
        }

    side = metadata["side"]
    axis = metadata["axis"]
    opposing_side = opposite_contest_side(side)
    direction = "Uncertain"
    supports = None
    rule_status = "Not defined by book"
    effect = "No automatic interpretation."
    decision_eligible = False
    research_only = True
    decision_reason = "No decision-grade rule."
    point_range: list[float] | None = None

    if body_name == "Sun":
        direction = "Harms cusp side"
        supports = opposing_side
        rule_status = "Book-defined"
        effect = "Burns every contacted cusp."
        decision_eligible = True
        research_only = False
        decision_reason = "Explicit Chapter 4 rule."
        point_range = [7.0, 9.0]
    elif body_name == "Moon":
        direction = "Harms cusp side"
        supports = opposing_side
        rule_status = "Book-defined with research caution"
        effect = "Lazy and lacklustre influence."
        decision_reason = (
            "The book says Moon is weaker and research-sensitive; "
            "report only, do not auto-score."
        )
    elif body_name == "Mars":
        if axis in {"1/7", "6/12"}:
            direction = "Supports cusp side"
            supports = side
            rule_status = "Book-defined"
            effect = "Galvanizes the team."
            decision_eligible = True
            research_only = False
            decision_reason = "Explicit Chapter 4 rule."
            point_range = [7.0, 9.0]
        else:
            direction = "Harms cusp side"
            supports = opposing_side
            rule_status = "Book-defined with research caution"
            effect = "Mars appears negative on the 4/10 axis."
            decision_reason = (
                "Printed page 67 says more research is needed; "
                "report only, do not auto-score."
            )
    elif body_name == "Rahu":
        direction = "Supports cusp side"
        supports = side
        rule_status = "Book-defined"
        effect = "Force and ambition, but weaker than visible planets."
        decision_eligible = True
        research_only = False
        decision_reason = "Explicit Chapter 4 rule."
        point_range = [7.0, 7.0]
    elif body_name == "Jupiter":
        direction = "Supports cusp side"
        supports = side
        rule_status = "Book-defined"
        effect = "Grants favour and victory."
        decision_eligible = True
        research_only = False
        decision_reason = "Explicit Chapter 4 rule."
        point_range = [7.0, 9.0]
    elif body_name == "Saturn":
        if axis == "1/7":
            direction = "Harms cusp side"
            supports = opposing_side
            effect = "Slows and handicaps the represented team."
        else:
            direction = "Supports cusp side"
            supports = side
            effect = "Supports the 6/12 and 4/10 axes."
        rule_status = "Book-defined"
        decision_eligible = True
        research_only = False
        decision_reason = "Explicit Chapter 4 rule."
        point_range = [7.0, 9.0]
    elif body_name == "Mercury":
        if axis == "6/12":
            direction = "Supports cusp side"
            supports = side
            rule_status = "Research-only; rulership required"
            effect = (
                "May be positive on 6/12, but the book says Mercury "
                "requires more research and should be judged by rulership."
            )
        else:
            direction = "Uncertain"
            supports = None
            rule_status = "Book says further research is needed"
            effect = "Judge Mercury through house rulership; not automatic."
        decision_reason = (
            "No automatic Mercury score without explicit relevant "
            "house-rulership confirmation."
        )
    elif body_name == "Ketu":
        direction = "Harms cusp side"
        supports = opposing_side
        rule_status = "Book-defined"
        effect = "Unilaterally negative on a cusp."
        decision_eligible = True
        research_only = False
        decision_reason = "Explicit Chapter 4 rule."
        point_range = [7.0, 7.0]
    elif body_name == "Venus":
        direction = "Supports cusp side"
        supports = side
        rule_status = "Book-defined"
        effect = "Positive but mild influence."
        decision_eligible = True
        research_only = False
        decision_reason = "Explicit Chapter 4 rule."
        point_range = [7.0, 9.0]

    return {
        "body": body_name,
        "cusp": cusp_name,
        "axis": axis,
        "represented_side": side,
        "direction": direction,
        "supports": supports,
        "effect": effect,
        "rule_status": rule_status,
        "tier": 2,
        "book_point_range": point_range,
        "decision_eligible": decision_eligible,
        "research_only": research_only,
        "automatic_decision_use": decision_eligible,
        "decision_reason": decision_reason,
        "exact_points_applied": False,
        "pdf_pages": [63, 64, 66, 67, 68, 72, 73, 158, 159],
    }


def d1_contact_effect(
    body_name: str,
    cusp_name: str,
    *,
    motion: str | None = None,
    category: str = "Classical planet",
) -> dict[str, Any]:
    """Unify classical, outer and special-point Chapter 4 cusp effects."""

    if category == "Outer planet":
        effect = outer_contact_effect(
            body_name,
            cusp_name,
            motion or "Unknown",
        )
        supports = None

        if effect.get("direction") == "Supports":
            supports = effect.get("represented_side")
        elif effect.get("direction") == "Harms":
            supports = opposite_contest_side(
                effect.get("represented_side")
            )

        motion_key = (
            str(motion or "Unknown")
            .strip()
            .lower()
            .replace("-", " ")
        )
        motion_is_clean = motion_key in {
            "direct",
            "retrograde",
        }
        decision_eligible = bool(
            supports in DECISION_SIDES
            and motion_is_clean
        )

        return {
            **effect,
            "supports": supports,
            "tier": 2,
            "book_point_range": (
                [7.0, 7.0]
                if decision_eligible
                else None
            ),
            "decision_eligible": decision_eligible,
            "research_only": not decision_eligible,
            "automatic_decision_use": decision_eligible,
            "decision_reason": (
                "Explicit Chapter 4 outer-body rule with "
                "clean direct/retrograde motion."
                if decision_eligible
                else (
                    "Undefined axis or stationary/unknown outer-body "
                    "motion is not decision-grade."
                )
            ),
            "reliability": "Research-sensitive outer planet",
            "exact_points_applied": False,
            "pdf_pages": [68, 69, 70, 72, 73, 158, 159],
        }

    if category == "Special point":
        effect = special_point_rashi_effect(
            body_name,
            cusp_name,
        )
        decision_eligible = (
            effect.get("supports") in DECISION_SIDES
            and effect.get("rule_status") == "Book-defined"
        )

        return {
            **effect,
            "body": body_name,
            "tier": 2,
            "book_point_range": (
                [7.0, 7.0]
                if decision_eligible
                else None
            ),
            "decision_eligible": decision_eligible,
            "research_only": not decision_eligible,
            "automatic_decision_use": decision_eligible,
            "decision_reason": (
                "Explicit Chapter 4 special-point rule."
                if decision_eligible
                else "The book does not define this special-point axis."
            ),
            "exact_points_applied": False,
            "pdf_pages": [68, 72, 73, 158, 159],
        }

    return d1_classical_cusp_effect(
        body_name,
        cusp_name,
    )


def collect_d9_axis_contacts(
    navamsha_cusps: dict[str, Any],
    outer_planets: dict[str, Any],
    special_points: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Collect exact D9 1/7 contacts from every available body."""

    if navamsha_cusps.get("status") not in {"Pass", "Partial"}:
        return [], [{
            "body": "All",
            "reason": "Exact D9 geometry is unavailable.",
        }]

    lagna = navamsha_cusps.get("lagna", {})
    seventh = navamsha_cusps.get("seventh_cusp", {})
    lagna_longitude = lagna.get("d9_sidereal_longitude")
    seventh_longitude = seventh.get("d9_sidereal_longitude")

    if not isinstance(lagna_longitude, (int, float)):
        return [], [{
            "body": "All",
            "reason": "D9 Lagna longitude is unavailable.",
        }]

    if not isinstance(seventh_longitude, (int, float)):
        return [], [{
            "body": "All",
            "reason": "D9 seventh-cusp longitude is unavailable.",
        }]

    contacts: list[dict[str, Any]] = []
    unavailable: list[dict[str, Any]] = []

    for raw in navamsha_cusps.get("qualifying_contacts", []):
        planet = raw.get("planet")
        cusp = raw.get("cusp")

        if not planet or cusp not in {"D9Lagna", "D9House7"}:
            continue

        distance = raw.get("angular_distance")
        orb_limit = raw.get("orb_limit")
        effect = finalise_contact_effect(
            d9_cusp_effect(
                planet,
                cusp,
            ),
            body=planet,
            cusp=cusp,
            family="D9_cusp",
            angular_distance=distance,
            orb_limit=orb_limit,
        )
        contacts.append({
            "body": planet,
            "category": "Classical planet",
            "cusp": cusp,
            "side": raw.get("side"),
            "d9_longitude": raw.get(
                "planet_d9_longitude"
            ),
            "cusp_d9_longitude": raw.get(
                "cusp_d9_longitude"
            ),
            "angular_distance": distance,
            "orb_limit": orb_limit,
            "rank_on_cusp": raw.get(
                "rank_on_cusp"
            ),
            "book_effect": effect,
        })

    for body_name, body in outer_planets.get(
        "bodies",
        {},
    ).items():
        if body.get("status") != "Pass":
            unavailable.append({
                "body": body_name,
                "category": "Outer planet",
                "reason": body.get(
                    "error",
                    "Exact outer-body position is unavailable.",
                ),
            })
            continue

        d9_position = body.get("d9_position_raw", {})
        d9_longitude = d9_position.get(
            "d9_sidereal_longitude"
        )

        if not isinstance(d9_longitude, (int, float)):
            unavailable.append({
                "body": body_name,
                "category": "Outer planet",
                "reason": "Exact D9 longitude is unavailable.",
            })
            continue

        for cusp_name, cusp_longitude in (
            ("D9Lagna", lagna_longitude),
            ("D9House7", seventh_longitude),
        ):
            distance = angular_distance(
                float(d9_longitude),
                float(cusp_longitude),
            )

            if distance > OUTER_CUSP_ORB_DEGREES + 1e-9:
                continue

            effect = finalise_contact_effect(
                d9_cusp_effect(
                    body_name,
                    cusp_name,
                    motion=body.get("motion"),
                ),
                body=body_name,
                cusp=cusp_name,
                family="D9_cusp",
                angular_distance=round(distance, 8),
                orb_limit=OUTER_CUSP_ORB_DEGREES,
            )
            contacts.append({
                "body": body_name,
                "category": "Outer planet",
                "cusp": cusp_name,
                "side": (
                    "Favourite"
                    if cusp_name == "D9Lagna"
                    else "Underdog"
                ),
                "d9_longitude": round(
                    float(d9_longitude),
                    8,
                ),
                "cusp_d9_longitude": round(
                    float(cusp_longitude),
                    8,
                ),
                "angular_distance": round(distance, 8),
                "orb_limit": OUTER_CUSP_ORB_DEGREES,
                "motion": body.get("motion"),
                "book_effect": effect,
            })

    for raw in special_points.get(
        "qualifying_d9_contacts",
        [],
    ):
        point = raw.get("point")
        cusp = raw.get("cusp")

        if not point or cusp not in {"D9Lagna", "D9House7"}:
            continue

        distance = raw.get("angular_distance")
        orb_limit = raw.get("orb_limit")
        effect = finalise_contact_effect(
            d9_cusp_effect(
                point,
                cusp,
            ),
            body=point,
            cusp=cusp,
            family="D9_cusp",
            angular_distance=distance,
            orb_limit=orb_limit,
        )
        contacts.append({
            "body": point,
            "category": "Special point",
            "cusp": cusp,
            "side": (
                "Favourite"
                if cusp == "D9Lagna"
                else "Underdog"
            ),
            "d9_longitude": raw.get(
                "point_d9_longitude"
            ),
            "cusp_d9_longitude": raw.get(
                "cusp_d9_longitude"
            ),
            "angular_distance": distance,
            "orb_limit": orb_limit,
            "book_effect": effect,
        })

    contacts.sort(
        key=lambda item: (
            0 if item["cusp"] == "D9Lagna" else 1,
            float(
                item.get("angular_distance")
                if isinstance(
                    item.get("angular_distance"),
                    (int, float),
                )
                else 999
            ),
            PLANET_ORDER.get(item["body"], 999),
            item["body"],
        )
    )

    return contacts, unavailable


def calculate_d9_combinations(
    navamsha_cusps: dict[str, Any],
) -> dict[str, Any]:
    """
    Detect Chapter 5 D9 House 1/7 combinations without pairwise over-counting.

    When multiple listed pairs share a planet, they form one overlap cluster.
    - same-direction cluster: count one five-point testimony
    - conflicting cluster: score none; mark unresolved
    - disjoint pairs: each may count independently
    """

    if navamsha_cusps.get("status") not in {"Pass", "Partial"}:
        return {
            "status": "Unavailable",
            "method": "Chapter5NavamshaCombinations",
            "book_tier": 1,
            "houses": {},
            "combinations": [],
            "raw_signed_favourite_total": 0.0,
            "signed_favourite_total": 0.0,
            "overlap_clusters": [],
            "error": "Exact D9 geometry is unavailable.",
        }

    lagna = navamsha_cusps.get("lagna", {})
    lagna_longitude = lagna.get("d9_sidereal_longitude")

    if not isinstance(lagna_longitude, (int, float)):
        return {
            "status": "Unavailable",
            "method": "Chapter5NavamshaCombinations",
            "book_tier": 1,
            "houses": {},
            "combinations": [],
            "raw_signed_favourite_total": 0.0,
            "signed_favourite_total": 0.0,
            "overlap_clusters": [],
            "error": "D9 Lagna longitude is unavailable.",
        }

    occupancy: dict[int, list[str]] = {
        1: [],
        7: [],
    }
    unavailable_planets: list[str] = []

    for planet_name in D9_COMBINATION_ALLOWED_PLANETS:
        position = navamsha_cusps.get(
            "planets",
            {},
        ).get(planet_name)

        if not isinstance(position, dict):
            unavailable_planets.append(planet_name)
            continue

        longitude = position.get("d9_sidereal_longitude")

        if not isinstance(longitude, (int, float)):
            unavailable_planets.append(planet_name)
            continue

        house = d9_house_for_longitude(
            float(longitude),
            float(lagna_longitude),
        )

        if house in occupancy:
            occupancy[house].append(planet_name)

    for planet_list in occupancy.values():
        planet_list.sort(
            key=lambda name: PLANET_ORDER.get(name, 999)
        )

    combinations: list[dict[str, Any]] = []

    for house_number, planet_list in occupancy.items():
        side = (
            "Favourite"
            if house_number == 1
            else "Underdog"
        )
        opposing_side = opposite_contest_side(side)

        for first_index, first_name in enumerate(planet_list):
            for second_name in planet_list[first_index + 1:]:
                pair = frozenset((first_name, second_name))
                rule = D9_COMBINATION_TABLE.get(pair)
                research_rule = D9_RESEARCH_COMBINATIONS.get(pair)
                selected_rule = rule or research_rule

                if selected_rule:
                    effect = selected_rule["effect"]
                    supports = (
                        side
                        if effect == "Win"
                        else opposing_side
                    )
                    points = float(
                        selected_rule["automatic_points"]
                    )
                    signed_points = (
                        points
                        if supports == "Favourite"
                        else -points
                    )

                    combinations.append({
                        "planets": sorted(
                            pair,
                            key=lambda name: (
                                PLANET_ORDER.get(name, 999),
                                name,
                            ),
                        ),
                        "d9_house": f"House{house_number}",
                        "represented_side": side,
                        "effect_for_represented_side": effect,
                        "supports": supports,
                        "rule_grade": selected_rule[
                            "rule_grade"
                        ],
                        "book_points": points,
                        "raw_signed_favourite_points": round(
                            signed_points,
                            2,
                        ),
                        "signed_favourite_points": 0.0,
                        "points_candidate": points > 0,
                        "points_applied": False,
                        "overlap_suppressed": False,
                        "manual_review_required": points <= 0,
                        "pdf_pages": selected_rule[
                            "pdf_pages"
                        ],
                    })
                    continue

                # General textual tendencies are reported but not scored.
                if "Sun" in pair:
                    effect = "Loss tendency"
                    supports = opposing_side
                    tendency = "General Sun-combination tendency"
                elif "Moon" in pair:
                    effect = "Win tendency"
                    supports = side
                    tendency = "General Moon-combination tendency"
                else:
                    continue

                combinations.append({
                    "planets": sorted(
                        pair,
                        key=lambda name: (
                            PLANET_ORDER.get(name, 999),
                            name,
                        ),
                    ),
                    "d9_house": f"House{house_number}",
                    "represented_side": side,
                    "effect_for_represented_side": effect,
                    "supports": supports,
                    "rule_grade": tendency,
                    "book_points": 0.0,
                    "raw_signed_favourite_points": 0.0,
                    "signed_favourite_points": 0.0,
                    "points_candidate": False,
                    "points_applied": False,
                    "manual_review_required": True,
                    "pdf_pages": [124],
                })

    raw_signed_total = round(sum(
        float(item.get("raw_signed_favourite_points") or 0.0)
        for item in combinations
        if item.get("points_candidate")
    ), 2)

    overlap_clusters: list[dict[str, Any]] = []
    signed_total = 0.0
    cluster_counter = 0

    for house_number in (1, 7):
        house_name = f"House{house_number}"
        candidate_indexes = [
            index
            for index, item in enumerate(combinations)
            if (
                item.get("d9_house") == house_name
                and item.get("points_candidate")
            )
        ]

        # Connected components by shared planet.
        unvisited = set(candidate_indexes)

        while unvisited:
            seed = min(unvisited)
            component = {seed}
            frontier = [seed]
            unvisited.remove(seed)

            while frontier:
                current = frontier.pop()
                current_planets = set(
                    combinations[current].get(
                        "planets",
                        [],
                    )
                )

                linked = [
                    index
                    for index in list(unvisited)
                    if current_planets.intersection(
                        combinations[index].get(
                            "planets",
                            [],
                        )
                    )
                ]

                for index in linked:
                    unvisited.remove(index)
                    component.add(index)
                    frontier.append(index)

            cluster_counter += 1
            cluster_id = f"D9C{cluster_counter}"
            members = sorted(component)
            supports_set = {
                combinations[index].get("supports")
                for index in members
            }
            planets_set = sorted({
                planet
                for index in members
                for planet in combinations[index].get(
                    "planets",
                    []
                )
            }, key=lambda name: (
                PLANET_ORDER.get(name, 999),
                name,
            ))

            if len(members) == 1:
                chosen = members[0]
                status = "Independent pair scored"
                cluster_supports = combinations[
                    chosen
                ].get("supports")
                combinations[chosen][
                    "points_applied"
                ] = True
                combinations[chosen][
                    "signed_favourite_points"
                ] = combinations[chosen].get(
                    "raw_signed_favourite_points",
                    0.0,
                )
                signed_total += float(
                    combinations[chosen].get(
                        "signed_favourite_points"
                    ) or 0.0
                )
            elif len(supports_set) == 1:
                # Same-direction overlapping pairs are one testimony, not
                # multiple independent five-point scores.
                chosen = min(
                    members,
                    key=lambda index: tuple(
                        combinations[index].get(
                            "planets",
                            [],
                        )
                    ),
                )
                cluster_supports = next(iter(supports_set))
                status = (
                    "Overlapping same-direction pairs collapsed "
                    "to one five-point testimony"
                )
                combinations[chosen][
                    "points_applied"
                ] = True
                combinations[chosen][
                    "signed_favourite_points"
                ] = combinations[chosen].get(
                    "raw_signed_favourite_points",
                    0.0,
                )
                signed_total += float(
                    combinations[chosen].get(
                        "signed_favourite_points"
                    ) or 0.0
                )

                for index in members:
                    if index == chosen:
                        continue
                    combinations[index].update({
                        "overlap_suppressed": True,
                        "manual_review_required": True,
                        "suppressed_reason": (
                            "Shares a planet with another same-direction "
                            "combination; counted once."
                        ),
                    })
            else:
                chosen = None
                cluster_supports = "Conflicting"
                status = (
                    "Overlapping contradictory pairs unresolved; "
                    "no automatic points"
                )

                for index in members:
                    combinations[index].update({
                        "overlap_suppressed": True,
                        "manual_review_required": True,
                        "suppressed_reason": (
                            "Shares planets with an opposing listed "
                            "combination; automatic stacking disabled."
                        ),
                    })

            for index in members:
                combinations[index][
                    "overlap_cluster_id"
                ] = cluster_id

            overlap_clusters.append({
                "cluster_id": cluster_id,
                "d9_house": house_name,
                "planets": planets_set,
                "member_pairs": [
                    combinations[index].get("planets")
                    for index in members
                ],
                "supports": cluster_supports,
                "status": status,
                "chosen_pair": (
                    combinations[chosen].get("planets")
                    if chosen is not None
                    else None
                ),
                "automatic_points_applied": (
                    5.0 if chosen is not None else 0.0
                ),
            })

    signed_total = round(signed_total, 2)

    side_summaries: dict[str, Any] = {}

    for side, house_number in (
        ("Favourite", 1),
        ("Underdog", 7),
    ):
        relevant = [
            item
            for item in combinations
            if item["d9_house"] == f"House{house_number}"
        ]
        applied = [
            item
            for item in relevant
            if item.get("points_applied")
        ]

        side_summaries[side] = {
            "house": f"House{house_number}",
            "occupants": occupancy[house_number],
            "combination_count": len(relevant),
            "raw_scored_candidate_count": sum(
                1
                for item in relevant
                if item.get("points_candidate")
            ),
            "independent_scored_combination_count": len(applied),
            "supports_favourite_count": sum(
                1
                for item in relevant
                if item.get("supports") == "Favourite"
            ),
            "supports_underdog_count": sum(
                1
                for item in relevant
                if item.get("supports") == "Underdog"
            ),
        }

    if signed_total > 0:
        indication = "Favourite"
    elif signed_total < 0:
        indication = "Underdog"
    elif combinations:
        indication = "Balanced, unresolved or research-only combinations"
    else:
        indication = "No listed combination"

    return {
        "status": (
            "Pass"
            if not unavailable_planets
            else "Partial"
        ),
        "method": "Chapter5NavamshaCombinations",
        "book_chapter": 5,
        "book_tier": 1,
        "house_system": "D9 whole-sign House1 and House7",
        "invisible_body_policy": (
            "Rahu and Ketu are included. Other invisible bodies "
            "are excluded from the combination technique."
        ),
        "houses": side_summaries,
        "combinations": combinations,
        "raw_signed_favourite_total": raw_signed_total,
        "signed_favourite_total": signed_total,
        "indication": indication,
        "overlap_clusters": overlap_clusters,
        "overlapping_pair_policy": (
            "Pairs sharing a planet are one correlated cluster. "
            "Same-direction clusters count once; contradictory clusters "
            "remain unresolved."
        ),
        "unavailable_planets": sorted(
            set(unavailable_planets),
            key=lambda name: PLANET_ORDER.get(name, 999),
        ),
        "pdf_pages": [124, 126, 127, 158, 159],
        "error": (
            None
            if not unavailable_planets
            else "One or more requested D9 planet positions were unavailable."
        ),
    }


def stolen_contact_lookup(
    stolen_cusps: dict[str, Any],
) -> dict[tuple[str, str], dict[str, Any]]:
    lookup: dict[tuple[str, str], dict[str, Any]] = {}

    for item in stolen_cusps.get(
        "qualifying_contacts",
        [],
    ):
        body = item.get("body")
        cusp = item.get("cusp")

        if body and cusp:
            lookup[(body, cusp)] = item

    return lookup


def interpret_stolen_contact(
    effect: dict[str, Any],
    stolen: dict[str, Any] | None,
) -> dict[str, Any]:
    """Apply the Chapter 4 stolen-cusp transformation to one body effect."""

    if not stolen:
        return {
            **effect,
            "stolen_cusp_applied": False,
            "contact_strength": "Normal",
        }

    stolen_effect = stolen.get("book_effect", {})
    stolen_type = stolen.get("stolen_type")
    effective_side = stolen_effect.get(
        "effective_represented_side"
    )

    if stolen_type == "Power-to-neutral":
        return {
            **effect,
            "supports_before_stolen_cusp": effect.get("supports"),
            "supports": None,
            "stolen_cusp_applied": True,
            "stolen_type": stolen_type,
            "contact_strength": "Significantly reduced",
            "transformation": "Weakened",
            "automatic_decision_use": False,
        }

    if stolen_type in {
        "Power-to-power",
        "Neutral-to-power",
    } and effective_side:
        direction = effect.get("direction")

        if direction in {
            "Supports cusp side",
            "Supports",
        }:
            supports = effective_side
        elif direction in {
            "Harms cusp side",
            "Harms",
        }:
            supports = opposite_contest_side(
                effective_side
            )
        else:
            supports = None

        return {
            **effect,
            "supports_before_stolen_cusp": effect.get("supports"),
            "supports": supports,
            "stolen_cusp_applied": True,
            "stolen_type": stolen_type,
            "contact_strength": "Transferred",
            "effective_represented_side": effective_side,
            "effective_power_house": stolen_effect.get(
                "effective_power_house"
            ),
            "transformation": stolen_effect.get(
                "transformation"
            ),
        }

    return {
        **effect,
        "stolen_cusp_applied": True,
        "stolen_type": stolen_type,
        "contact_strength": "Unscored",
    }


def collect_d1_directional_contacts(
    planet_cusp_contacts: dict[str, Any],
    outer_planets: dict[str, Any],
    special_points: dict[str, Any],
    stolen_cusps: dict[str, Any],
) -> list[dict[str, Any]]:
    """Interpret all available Chapter 4 D1 cusp contacts."""

    lookup = stolen_contact_lookup(stolen_cusps)
    contacts: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()

    for raw in planet_cusp_contacts.get(
        "qualifying_contacts",
        [],
    ):
        body = raw.get("planet")
        cusp = raw.get("cusp")

        if not body or cusp not in SENSITIVE_CUSP_DETAILS:
            continue

        distance = raw.get("angular_distance")
        orb_limit = raw.get("orb_limit")
        effect = d1_contact_effect(
            body,
            cusp,
            category="Classical planet",
        )
        effect = interpret_stolen_contact(
            effect,
            lookup.get((body, cusp)),
        )
        effect = finalise_contact_effect(
            effect,
            body=body,
            cusp=cusp,
            family="D1_cusp",
            angular_distance=distance,
            orb_limit=orb_limit,
        )
        contacts.append({
            "body": body,
            "category": "Classical planet",
            "cusp": cusp,
            "axis": raw.get("axis"),
            "represented_side": raw.get("side"),
            "angular_distance": distance,
            "orb_limit": orb_limit,
            "book_effect": effect,
        })
        seen.add((body, cusp))

    for raw in outer_planets.get(
        "qualifying_contacts",
        [],
    ):
        body = raw.get("body")
        cusp = raw.get("cusp")

        if not body or cusp not in SENSITIVE_CUSP_DETAILS:
            continue

        motion = raw.get("motion")
        distance = raw.get("angular_distance")
        orb_limit = raw.get("orb_limit")
        effect = d1_contact_effect(
            body,
            cusp,
            motion=motion,
            category="Outer planet",
        )
        effect = interpret_stolen_contact(
            effect,
            lookup.get((body, cusp)),
        )
        effect = finalise_contact_effect(
            effect,
            body=body,
            cusp=cusp,
            family="D1_cusp",
            angular_distance=distance,
            orb_limit=orb_limit,
        )
        contacts.append({
            "body": body,
            "category": "Outer planet",
            "cusp": cusp,
            "axis": raw.get("axis"),
            "represented_side": raw.get("side"),
            "angular_distance": distance,
            "orb_limit": orb_limit,
            "motion": motion,
            "book_effect": effect,
        })
        seen.add((body, cusp))

    for raw in special_points.get(
        "qualifying_rashi_contacts",
        [],
    ):
        body = raw.get("point")
        cusp = raw.get("cusp")

        if not body or cusp not in SENSITIVE_CUSP_DETAILS:
            continue

        distance = raw.get("angular_distance")
        orb_limit = raw.get("orb_limit")
        effect = d1_contact_effect(
            body,
            cusp,
            category="Special point",
        )
        effect = interpret_stolen_contact(
            effect,
            lookup.get((body, cusp)),
        )
        effect = finalise_contact_effect(
            effect,
            body=body,
            cusp=cusp,
            family="D1_cusp",
            angular_distance=distance,
            orb_limit=orb_limit,
        )
        contacts.append({
            "body": body,
            "category": "Special point",
            "cusp": cusp,
            "axis": raw.get("axis"),
            "represented_side": raw.get("side"),
            "angular_distance": distance,
            "orb_limit": orb_limit,
            "book_effect": effect,
        })
        seen.add((body, cusp))

    # Add neutral-to-power contacts that are absent from the six-cusp raw
    # geometry layer. The stolen-cusp result already verified their exact orb.
    for raw in stolen_cusps.get(
        "qualifying_contacts",
        [],
    ):
        body = raw.get("body")
        source_cusp = raw.get("cusp")
        stolen_type = raw.get("stolen_type")

        if (
            not body
            or not source_cusp
            or (body, source_cusp) in seen
            or stolen_type != "Neutral-to-power"
        ):
            continue

        effective_house = raw.get(
            "whole_sign_house_number"
        )
        effective_cusp = (
            f"House{effective_house}"
            if isinstance(effective_house, int)
            else None
        )

        if effective_cusp not in SENSITIVE_CUSP_DETAILS:
            continue

        category = raw.get(
            "body_category",
            "Classical planet",
        )
        motion = raw.get("body_motion")
        distance = raw.get("angular_distance")
        orb_limit = raw.get("orb_limit")
        effect = d1_contact_effect(
            body,
            effective_cusp,
            motion=motion,
            category=category,
        )
        effect = interpret_stolen_contact(
            effect,
            raw,
        )
        effect = finalise_contact_effect(
            effect,
            body=body,
            cusp=source_cusp,
            family="D1_cusp",
            angular_distance=distance,
            orb_limit=orb_limit,
        )

        contacts.append({
            "body": body,
            "category": category,
            "cusp": source_cusp,
            "effective_cusp": effective_cusp,
            "axis": SENSITIVE_CUSP_DETAILS[
                effective_cusp
            ]["axis"],
            "represented_side": SENSITIVE_CUSP_DETAILS[
                effective_cusp
            ]["side"],
            "angular_distance": distance,
            "orb_limit": orb_limit,
            "motion": motion,
            "book_effect": effect,
        })

    contacts = deduplicate_d1_node_axis_contacts(
        contacts
    )

    contacts.sort(
        key=lambda item: (
            float(
                item.get("angular_distance")
                if isinstance(
                    item.get("angular_distance"),
                    (int, float),
                )
                else 999
            ),
            item.get("cusp") or "",
            item.get("body") or "",
        )
    )

    return contacts


def directional_summary(
    indicators: list[dict[str, Any]],
) -> dict[str, Any]:
    """
    Summarize only decision-grade testimony at the highest available tier.

    The previous implementation counted every indicator equally. This version:
    - excludes research-only, weakened and de-duplicated testimony
    - honours Tier 3 > Tier 2 > Tier 1
    - preserves the book's point ranges as intervals
    - returns Mixed when intervals overlap zero or unscored opposition remains
    """

    eligible: list[dict[str, Any]] = []
    excluded: list[dict[str, Any]] = []

    for item in indicators:
        supports = item.get("supports")
        decision_eligible = item.get(
            "decision_eligible",
            supports in DECISION_SIDES,
        )

        reason = None

        if supports not in DECISION_SIDES:
            reason = "No directional side"
        elif decision_eligible is not True:
            reason = "Not decision-eligible"
        elif item.get("research_only") is True:
            reason = "Research-only"
        elif item.get("automatic_decision_use") is False:
            reason = "Automatic decision use disabled"

        if reason:
            excluded.append({
                "source": item.get("source"),
                "body": item.get("body"),
                "cusp": item.get("cusp"),
                "supports": supports,
                "reason": reason,
            })
            continue

        eligible.append(item)

    if eligible:
        highest_tier = max(
            int(item.get("tier") or 0)
            for item in eligible
        )
        active = [
            item
            for item in eligible
            if int(item.get("tier") or 0)
            == highest_tier
        ]
    else:
        highest_tier = None
        active = []

    intervals: list[list[float]] = []
    unscored_active: list[dict[str, Any]] = []

    for item in active:
        interval = item.get("signed_interval")

        if (
            not isinstance(interval, (list, tuple))
            or len(interval) != 2
            or not all(
                isinstance(value, (int, float))
                for value in interval
            )
        ):
            point_range = item.get(
                "book_point_range"
            )
            supports = item.get("supports")

            if (
                isinstance(point_range, (list, tuple))
                and len(point_range) == 2
                and all(
                    isinstance(value, (int, float))
                    for value in point_range
                )
            ):
                low = float(min(point_range))
                high = float(max(point_range))
                interval = (
                    [low, high]
                    if supports == "Favourite"
                    else [-high, -low]
                )

        if (
            isinstance(interval, (list, tuple))
            and len(interval) == 2
            and all(
                isinstance(value, (int, float))
                for value in interval
            )
        ):
            intervals.append([
                float(interval[0]),
                float(interval[1]),
            ])
        else:
            unscored_active.append(item)

    signed_interval = None

    if intervals:
        signed_interval = [
            round(sum(item[0] for item in intervals), 4),
            round(sum(item[1] for item in intervals), 4),
        ]

    active_supports = {
        item.get("supports")
        for item in active
        if item.get("supports") in DECISION_SIDES
    }

    if signed_interval is not None:
        low, high = signed_interval

        if low > 0:
            direction = "Favourite"
        elif high < 0:
            direction = "Underdog"
        else:
            direction = "Mixed"

        # A same-tier unscored testimony opposing the interval prevents a
        # false clean direction.
        unscored_supports = {
            item.get("supports")
            for item in unscored_active
            if item.get("supports") in DECISION_SIDES
        }

        if (
            direction == "Favourite"
            and "Underdog" in unscored_supports
        ) or (
            direction == "Underdog"
            and "Favourite" in unscored_supports
        ):
            direction = "Mixed"
    elif len(active_supports) == 1:
        direction = next(iter(active_supports))
    elif len(active_supports) > 1:
        direction = "Mixed"
    else:
        direction = "None"

    closest = None
    contacts_with_distance = [
        item
        for item in active
        if isinstance(
            item.get("angular_distance"),
            (int, float),
        )
    ]

    if contacts_with_distance:
        closest_item = min(
            contacts_with_distance,
            key=lambda item: float(
                item["angular_distance"]
            ),
        )
        closest = {
            "source": closest_item.get("source"),
            "body": closest_item.get("body"),
            "cusp": closest_item.get("cusp"),
            "supports": closest_item.get("supports"),
            "angular_distance": closest_item.get(
                "angular_distance"
            ),
            "orb_strength": closest_item.get(
                "orb_strength"
            ),
        }

    return {
        "direction": direction,
        "highest_decisive_tier": highest_tier,
        "signed_interval": signed_interval,
        "decision_eligible_count": len(eligible),
        "active_highest_tier_count": len(active),
        "favourite_indicator_count": sum(
            1
            for item in active
            if item.get("supports") == "Favourite"
        ),
        "underdog_indicator_count": sum(
            1
            for item in active
            if item.get("supports") == "Underdog"
        ),
        "unscored_active_count": len(
            unscored_active
        ),
        "excluded_count": len(excluded),
        "excluded": excluded,
        "closest_decision_contact": closest,
        "indicator_count": len(indicators),
        "aggregation_policy": (
            "Highest tier first; book ranges retained as intervals; "
            "research-only and de-duplicated testimony excluded."
        ),
    }


def tier1_yoga_indicators(
    tier1_combinations: dict[str, Any],
) -> list[dict[str, Any]]:
    indicators: list[dict[str, Any]] = []
    sides = tier1_combinations.get(
        "sky_pky",
        {},
    ).get("sides", {})

    for side, result in sides.items():
        sky = result.get("sky", {})
        pky = result.get("pky", {})

        if sky.get("formed"):
            condition = sky.get("condition")
            point_range = (
                [7.0, 9.0]
                if condition == "Full"
                else None
            )
            indicators.append({
                "source": "SKY",
                "tier": 2,
                "represented_side": side,
                "supports": side,
                "condition": condition,
                "decision_eligible": True,
                "research_only": False,
                "automatic_decision_use": True,
                "book_point_range": point_range,
                "signed_interval": (
                    point_range
                    if side == "Favourite"
                    and point_range
                    else (
                        [-point_range[1], -point_range[0]]
                        if point_range
                        else None
                    )
                ),
                "independence_family": "SKY_PKY",
                "independence_key": f"SKY:{side}",
                "decision_reason": (
                    "Full SKY uses the general Tier 2 range."
                    if point_range
                    else (
                        "Afflicted/diminished SKY is directional but "
                        "left unscored because the exact reduction "
                        "requires judgment."
                    )
                ),
            })

        if pky.get("formed"):
            condition = pky.get("condition")
            point_range = [7.0, 9.0]
            supports = opposite_contest_side(side)
            indicators.append({
                "source": "PKY",
                "tier": 2,
                "represented_side": side,
                "supports": supports,
                "condition": condition,
                "decision_eligible": True,
                "research_only": False,
                "automatic_decision_use": True,
                "book_point_range": point_range,
                "signed_interval": (
                    point_range
                    if supports == "Favourite"
                    else [-point_range[1], -point_range[0]]
                ),
                "independence_family": "SKY_PKY",
                "independence_key": f"PKY:{side}",
                "decision_reason": (
                    "The book gives SKY/PKY a general Tier 2 range; "
                    "exact strength remains contextual."
                ),
            })

    return indicators


def find_double_whammy_contacts(
    d1_contacts: list[dict[str, Any]],
    d9_contacts: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Find the same body on corresponding D1 and D9 1/7 cusps."""

    results: list[dict[str, Any]] = []

    for d1 in d1_contacts:
        d1_cusp = d1.get("cusp")

        if d1_cusp not in {"House1", "House7"}:
            continue

        expected_d9 = (
            "D9Lagna"
            if d1_cusp == "House1"
            else "D9House7"
        )

        for d9 in d9_contacts:
            if (
                d1.get("body") != d9.get("body")
                or d9.get("cusp") != expected_d9
            ):
                continue

            d1_supports = d1.get(
                "book_effect",
                {},
            ).get("supports")
            d9_supports = d9.get(
                "book_effect",
                {},
            ).get("supports")

            if (
                d1_supports
                and d9_supports
                and d1_supports == d9_supports
            ):
                relationship = "Reinforcing double whammy"
                supports = d1_supports
            elif d1_supports and d9_supports:
                relationship = (
                    "Same-body D1/D9 contradiction"
                )
                supports = None
            else:
                relationship = (
                    "Geometric transfer; effect requires manual review"
                )
                supports = d1_supports or d9_supports

            results.append({
                "body": d1.get("body"),
                "d1_cusp": d1_cusp,
                "d1_distance": d1.get(
                    "angular_distance"
                ),
                "d9_cusp": expected_d9,
                "d9_distance": d9.get(
                    "angular_distance"
                ),
                "d1_supports": d1_supports,
                "d9_supports": d9_supports,
                "relationship": relationship,
                "supports": supports,
                "pdf_pages": [131, 132],
                "points_applied": False,
            })

    return results


def strongest_d1_direction(
    tier1_combinations: dict[str, Any],
    d1_contacts: list[dict[str, Any]],
) -> dict[str, Any]:
    cusp_indicators = [
        contact_to_indicator(
            contact,
            source="Rashi cusp",
            tier=2,
        )
        for contact in d1_contacts
    ]

    yoga_indicators = tier1_yoga_indicators(
        tier1_combinations
    )
    tier2_indicators = cusp_indicators + yoga_indicators
    tier2_summary = directional_summary(
        tier2_indicators
    )

    if tier2_summary["direction"] in {
        "Favourite",
        "Underdog",
        "Mixed",
    }:
        return {
            "direction": tier2_summary["direction"],
            "deciding_tier": 2,
            "source": "Rashi cusp and SKY/PKY testimony",
            "tier2_summary": tier2_summary,
            "tier1_signed_total": tier1_combinations.get(
                "automatic_signed_total"
            ),
        }

    signed_total = tier1_combinations.get(
        "automatic_signed_total"
    )

    if isinstance(signed_total, (int, float)):
        if signed_total > 0:
            direction = "Favourite"
        elif signed_total < 0:
            direction = "Underdog"
        else:
            direction = "Balanced"
    else:
        direction = "None"

    return {
        "direction": direction,
        "deciding_tier": 1 if direction != "None" else None,
        "source": "Victory-house signed total",
        "tier2_summary": tier2_summary,
        "tier1_signed_total": signed_total,
    }


def strongest_d9_direction(
    d9_contacts: list[dict[str, Any]],
    combinations: dict[str, Any],
) -> dict[str, Any]:
    cusp_indicators = [
        contact_to_indicator(
            contact,
            source="D9 cusp",
            tier=3,
        )
        for contact in d9_contacts
    ]
    cusp_summary = directional_summary(
        cusp_indicators
    )

    if cusp_summary["direction"] in {
        "Favourite",
        "Underdog",
        "Mixed",
    }:
        return {
            "direction": cusp_summary["direction"],
            "deciding_tier": 3,
            "source": "D9 1/7 cusp testimony",
            "cusp_summary": cusp_summary,
            "combination_signed_total": combinations.get(
                "signed_favourite_total"
            ),
        }

    combo_total = combinations.get(
        "signed_favourite_total"
    )

    if isinstance(combo_total, (int, float)):
        if combo_total > 0:
            direction = "Favourite"
        elif combo_total < 0:
            direction = "Underdog"
        elif combinations.get("combinations"):
            direction = "Balanced"
        else:
            direction = "None"
    else:
        direction = "None"

    return {
        "direction": direction,
        "deciding_tier": 1 if direction != "None" else None,
        "source": "D9 combinations",
        "cusp_summary": cusp_summary,
        "combination_signed_total": combo_total,
    }


def compare_d1_d9_hierarchy(
    d1_summary: dict[str, Any],
    d9_summary: dict[str, Any],
) -> dict[str, Any]:
    """Apply the book's tier hierarchy without flattening all testimony."""

    d1_direction = d1_summary.get("direction")
    d9_direction = d9_summary.get("direction")
    d1_tier = d1_summary.get("deciding_tier")
    d9_tier = d9_summary.get("deciding_tier")
    sides = {"Favourite", "Underdog"}

    if d9_direction in sides and d1_direction in sides:
        if d9_direction == d1_direction:
            relationship = "Reinforcement"
            hierarchy_direction = d9_direction
            rule = "D9 confirms the D1 direction."
        elif d9_tier == 3:
            relationship = "Tier 3 reversal"
            hierarchy_direction = d9_direction
            rule = (
                "D9 cusp testimony opposes and outranks the lower D1 "
                "direction."
            )
        elif (
            d9_tier == 1
            and isinstance(d1_tier, int)
            and d1_tier > d9_tier
        ):
            relationship = (
                "Contradiction; higher-tier D1 testimony retains priority"
            )
            hierarchy_direction = d1_direction
            rule = (
                "A Tier 1 D9 combination does not overrule Tier 2 "
                "rashi testimony."
            )
        else:
            relationship = "Same-tier contradiction"
            hierarchy_direction = "Balanced"
            rule = "Neither side receives a hierarchy advantage."
    elif d9_direction in sides:
        relationship = "D9 establishes direction"
        hierarchy_direction = d9_direction
        rule = "D1 is balanced, absent or mixed."
    elif d9_direction == "Mixed":
        relationship = "D9 cancellation"
        hierarchy_direction = (
            d1_direction
            if d1_direction in sides
            else "Balanced"
        )
        rule = "Opposing Tier 3 contacts cancel at the D9 level."
    elif d1_direction in sides:
        relationship = "D1 unconfirmed by D9"
        hierarchy_direction = d1_direction
        rule = "No decisive D9 testimony is present."
    elif d1_direction == "Mixed":
        relationship = "D1 mixed; D9 not decisive"
        hierarchy_direction = "Balanced"
        rule = "The chart remains contradictory."
    else:
        relationship = "No directional relationship"
        hierarchy_direction = "Balanced"
        rule = "Neither chart supplies decisive directional testimony."

    return {
        "relationship": relationship,
        "d1_direction": d1_direction,
        "d1_deciding_tier": d1_tier,
        "d9_direction": d9_direction,
        "d9_deciding_tier": d9_tier,
        "hierarchy_direction": hierarchy_direction,
        "rule": rule,
        "pdf_pages": [108, 109, 136, 137, 140],
        "points_applied": False,
    }


def calculate_navamsha_interpretation(
    navamsha_cusps: dict[str, Any],
    tier1_combinations: dict[str, Any],
    planet_cusp_contacts: dict[str, Any],
    outer_planets: dict[str, Any],
    special_points: dict[str, Any],
    stolen_cusps: dict[str, Any],
) -> dict[str, Any]:
    """
    Complete the Chapter 5 contest layer with decision-grade filtering.

    Research-only contacts remain visible, but only explicit book-defined
    testimony enters tier direction or point intervals.
    """

    combinations = calculate_d9_combinations(
        navamsha_cusps
    )
    d9_contacts, unavailable_d9_bodies = (
        collect_d9_axis_contacts(
            navamsha_cusps,
            outer_planets,
            special_points,
        )
    )
    d1_contacts = collect_d1_directional_contacts(
        planet_cusp_contacts,
        outer_planets,
        special_points,
        stolen_cusps,
    )
    double_whammy = find_double_whammy_contacts(
        d1_contacts,
        d9_contacts,
    )
    d1_summary = strongest_d1_direction(
        tier1_combinations,
        d1_contacts,
    )
    d9_summary = strongest_d9_direction(
        d9_contacts,
        combinations,
    )
    hierarchy = compare_d1_d9_hierarchy(
        d1_summary,
        d9_summary,
    )

    d1_indicators = [
        contact_to_indicator(
            contact,
            source="Rashi cusp",
            tier=2,
        )
        for contact in d1_contacts
    ]
    d9_indicators = [
        contact_to_indicator(
            contact,
            source="D9 cusp",
            tier=3,
        )
        for contact in d9_contacts
    ]
    d1_cusp_summary = directional_summary(
        d1_indicators
    )
    d9_cusp_summary = directional_summary(
        d9_indicators
    )

    research_d1_effects = [
        contact
        for contact in d1_contacts
        if (
            (contact.get("book_effect") or {}).get(
                "decision_eligible"
            ) is not True
            or (contact.get("book_effect") or {}).get(
                "research_only"
            ) is True
            or (contact.get("book_effect") or {}).get(
                "automatic_decision_use"
            ) is False
        )
    ]
    research_d9_effects = [
        contact
        for contact in d9_contacts
        if (
            (contact.get("book_effect") or {}).get(
                "decision_eligible"
            ) is not True
            or (contact.get("book_effect") or {}).get(
                "research_only"
            ) is True
            or (contact.get("book_effect") or {}).get(
                "automatic_decision_use"
            ) is False
        )
    ]

    node_rows = [
        contact
        for contact in d1_contacts
        if contact.get("body") in {"Rahu", "Ketu"}
    ]
    node_duplicates = [
        contact
        for contact in node_rows
        if (contact.get("book_effect") or {}).get(
            "node_axis_duplicate"
        )
    ]

    if navamsha_cusps.get("status") == "Fail":
        status = "Fail"
        error = "Exact D9 geometry failed validation."
    elif navamsha_cusps.get("status") not in {
        "Pass",
        "Partial",
    }:
        status = "Unavailable"
        error = "Exact D9 geometry is unavailable."
    elif combinations.get("status") == "Partial":
        status = "Partial"
        error = (
            "One or more requested classical D9 planet positions were "
            "unavailable."
        )
    else:
        status = "Pass"
        error = None

    return {
        "status": status,
        "method": "BookLockedChapter5NavamshaInterpretationV119",
        "book_chapter": 5,
        "ayanamsa": "Lahiri",
        "assignment": {
            "D9Lagna": "Favourite",
            "D9House7": "Underdog",
        },
        "tier_hierarchy": {
            "Tier3": "D9 1/7 cusp strength",
            "Tier2": "Rashi cusp strength and SKY/PKY",
            "Tier1": "Victory houses and D9 combinations",
        },
        "decision_grade_policy": {
            "research_contacts_auto_scored": False,
            "ranges_preserved_as_intervals": True,
            "rahu_ketu_rashi_axis_counted_once": True,
            "overlapping_d9_pairs_stacked": False,
            "highest_tier_controls_direction": True,
        },
        "d9_cusp_contacts": d9_contacts,
        "d9_cusp_summary": d9_cusp_summary,
        "navamsha_combinations": combinations,
        "d1_cusp_contacts": d1_contacts,
        "d1_cusp_summary": d1_cusp_summary,
        "d1_summary": d1_summary,
        "d9_summary": d9_summary,
        "d1_d9_relationship": hierarchy,
        "double_whammy": {
            "detected": bool(double_whammy),
            "contacts": double_whammy,
            "definition": (
                "The same body is within orb of the corresponding D1 "
                "and D9 1/7 cusps."
            ),
            "points_applied": False,
            "pdf_pages": [131, 132],
        },
        "node_axis_deduplication": {
            "rashi_node_contact_count": len(node_rows),
            "duplicates_excluded_from_automatic_aggregation": len(
                node_duplicates
            ),
            "policy": (
                "Rahu/Ketu same-axis, same-direction testimony is counted "
                "once; all geometry remains visible."
            ),
            "pdf_pages": [67],
        },
        "signed_points": {
            "d1_tier2_interval": d1_cusp_summary.get(
                "signed_interval"
            ),
            "d9_tier3_interval": d9_cusp_summary.get(
                "signed_interval"
            ),
            "navamsha_combination_raw_total": combinations.get(
                "raw_signed_favourite_total",
                0.0,
            ),
            "navamsha_combination_deduplicated_total": combinations.get(
                "signed_favourite_total",
                0.0,
            ),
            "d9_cusp_exact_total": None,
            "d9_cusp_exact_total_reason": (
                "The book gives 14-18 for visible and 12-15 for "
                "invisible D9 cusp contacts but leaves the exact value "
                "to orb and contextual judgment."
            ),
        },
        "unavailable_d9_bodies": unavailable_d9_bodies,
        "optional_body_coverage_status": (
            "Pass"
            if not unavailable_d9_bodies
            else "Partial"
        ),
        "research_or_undefined_d1_contacts": research_d1_effects,
        "research_or_undefined_d9_contacts": research_d9_effects,
        "completeness": {
            "d9_axis_geometry_available": (
                navamsha_cusps.get("status")
                in {"Pass", "Partial"}
            ),
            "all_requested_combo_planets_available": (
                not combinations.get(
                    "unavailable_planets",
                    []
                )
            ),
            "d1_d9_hierarchy_completed": True,
            "double_whammy_checked": True,
            "exact_d9_cusp_points_mechanical": False,
            "research_contact_filtering_completed": True,
            "node_axis_deduplication_completed": True,
            "d9_overlap_deduplication_completed": True,
        },
        "pdf_pages": sorted({
            page
            for pages in NAVAMSHA_INTERPRETATION_PDF_PAGES.values()
            for page in pages
        }),
        "points_applied": any(
            item.get("points_applied") is True
            for item in combinations.get(
                "combinations",
                [],
            )
        ),
        "error": error,
    }



def calculate_chart_correlation_signature(
    rashi_placidus: dict[str, Any],
    navamsha_cusps: dict[str, Any],
    kp_sublords: dict[str, Any],
    navamsha_interpretation: dict[str, Any],
) -> dict[str, Any]:
    """
    Return deterministic fine and slate-cluster signatures.

    The proxy is stateless and cannot count other events. A batch caller can
    group matching cluster_signature values so similar same-time charts are
    not mistaken for independent validation.
    """

    def rounded_step(
        value: Any,
        step: float,
    ) -> float | None:
        if not isinstance(value, (int, float)):
            return None

        return round(
            round(float(value) / step) * step,
            4,
        )

    rashi_cusps = rashi_placidus.get(
        "cusps",
        {},
    )
    d9_lagna = navamsha_cusps.get(
        "lagna",
        {},
    ).get("d9_sidereal_longitude")
    d9_seventh = navamsha_cusps.get(
        "seventh_cusp",
        {},
    ).get("d9_sidereal_longitude")
    kp_cusps = kp_sublords.get(
        "cusp_sublords",
        {},
    )

    def build_payload(step: float) -> dict[str, Any]:
        return {
            "rashi_sensitive_cusps": {
                house: rounded_step(
                    (
                        rashi_cusps.get(house, {})
                        or {}
                    ).get("sidereal_longitude"),
                    step,
                )
                for house in (
                    "House1",
                    "House7",
                    "House4",
                    "House10",
                    "House6",
                    "House12",
                )
            },
            "d9_axis": {
                "lagna": rounded_step(
                    d9_lagna,
                    step,
                ),
                "house7": rounded_step(
                    d9_seventh,
                    step,
                ),
            },
            "kp_main_sublords": {
                house: (
                    kp_cusps.get(house, {})
                    or {}
                ).get("sublord")
                for house in (
                    "House1",
                    "House7",
                    "House4",
                    "House10",
                )
            },
            "d1_contacts": sorted([
                {
                    "body": item.get("body"),
                    "cusp": item.get("cusp"),
                    "supports": (
                        item.get("book_effect")
                        or {}
                    ).get("supports"),
                    "eligible": (
                        item.get("book_effect")
                        or {}
                    ).get("decision_eligible"),
                    "distance": rounded_step(
                        item.get("angular_distance"),
                        step,
                    ),
                }
                for item in navamsha_interpretation.get(
                    "d1_cusp_contacts",
                    [],
                )
                if isinstance(item, dict)
            ], key=lambda item: (
                str(item.get("body")),
                str(item.get("cusp")),
                str(item.get("supports")),
            )),
            "d9_contacts": sorted([
                {
                    "body": item.get("body"),
                    "cusp": item.get("cusp"),
                    "supports": (
                        item.get("book_effect")
                        or {}
                    ).get("supports"),
                    "eligible": (
                        item.get("book_effect")
                        or {}
                    ).get("decision_eligible"),
                    "distance": rounded_step(
                        item.get("angular_distance"),
                        step,
                    ),
                }
                for item in navamsha_interpretation.get(
                    "d9_cusp_contacts",
                    [],
                )
                if isinstance(item, dict)
            ], key=lambda item: (
                str(item.get("body")),
                str(item.get("cusp")),
                str(item.get("supports")),
            )),
            "hierarchy_direction": (
                navamsha_interpretation.get(
                    "d1_d9_relationship",
                    {},
                ).get("hierarchy_direction")
            ),
        }

    fine_payload = build_payload(0.05)
    cluster_payload = build_payload(0.5)

    def digest(payload: dict[str, Any]) -> str:
        encoded = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")

        return hashlib.sha256(encoded).hexdigest()[:20]

    return {
        "status": "Pass",
        "method": "Deterministic chart-correlation signatures",
        "fine_signature": digest(fine_payload),
        "cluster_signature": digest(cluster_payload),
        "fine_rounding_degrees": 0.05,
        "cluster_rounding_degrees": 0.5,
        "batch_use": (
            "Group equal cluster_signature values. Repeated outcomes in one "
            "cluster are correlated tests, not independent confirmations."
        ),
        "same_time_event_count": None,
        "same_time_event_count_reason": (
            "The Action is stateless; the batch caller must count matching "
            "signatures across events."
        ),
        "payload_summary": {
            "rashi_sensitive_cusps": cluster_payload[
                "rashi_sensitive_cusps"
            ],
            "d9_axis": cluster_payload["d9_axis"],
            "kp_main_sublords": cluster_payload[
                "kp_main_sublords"
            ],
            "hierarchy_direction": cluster_payload[
                "hierarchy_direction"
            ],
            "d1_contact_count": len(
                cluster_payload["d1_contacts"]
            ),
            "d9_contact_count": len(
                cluster_payload["d9_contacts"]
            ),
        },
        "pdf_pages": [26, 27, 28],
    }

def extract_motion_label(
    planet_result: dict[str, Any] | None,
) -> str | None:
    """Extract VedAstro's exact motion label without guessing."""

    if not isinstance(planet_result, dict):
        return None

    motion_result = planet_result.get("motion")

    if not isinstance(motion_result, dict):
        return None

    data = unwrap_data(motion_result)

    if isinstance(data, str):
        return data.strip() or None

    label = find_named_value(
        data,
        (
            "Name",
            "MotionName",
            "PlanetMotionName",
            "Value",
        ),
    )

    return label.strip() if label else None


def normalise_motion_label(
    label: str | None,
) -> str:
    if not label:
        return ""

    return " ".join(
        label.lower()
        .replace("-", " ")
        .replace("_", " ")
        .split()
    )


def classify_motion_label(
    label: str | None,
) -> str:
    """
    Classify the upstream label without assuming all terminology is identical.

    The Gambler's Dharma glossary defines kutila as stationary. VedAstro may
    also return Vikala as a motion label. Strict mode treats Vikala as a veto;
    practical mode requires same-date Swiss confirmation and otherwise treats
    it as a warning.
    """

    normalised = normalise_motion_label(label)

    if not normalised:
        return "none"

    if normalised in {
        "kutila",
        "stationary",
        "station",
        "exactly stationary",
    }:
        return "explicit_station"

    if "stationary" in normalised:
        return "explicit_station"

    if "kutila" in normalised:
        return "explicit_station"

    if "vikala" in normalised:
        return "vikala"

    return "ordinary"


def motion_label_is_kutila(
    label: str | None,
) -> bool:
    """Backward-compatible strict-book classification."""

    return classify_motion_label(label) in {
        "explicit_station",
        "vikala",
    }


def motion_label_is_practical_hard_veto(
    label: str | None,
) -> bool:
    return classify_motion_label(label) == "explicit_station"


def motion_label_is_practical_warning(
    label: str | None,
) -> bool:
    return classify_motion_label(label) == "vikala"

def julian_day_to_utc_datetime(
    julian_day_ut: float,
) -> datetime:
    year, month, day, decimal_hour = swe.revjul(
        julian_day_ut,
        swe.GREG_CAL,
    )
    hour = int(decimal_hour)
    minute_float = (decimal_hour - hour) * 60.0
    minute = int(minute_float)
    second_float = (minute_float - minute) * 60.0
    second = int(second_float)
    microsecond = int(
        round((second_float - second) * 1_000_000)
    )

    if microsecond >= 1_000_000:
        second += 1
        microsecond -= 1_000_000

    return datetime(
        year,
        month,
        day,
        hour,
        minute,
        second,
        microsecond,
        tzinfo=timezone.utc,
    )


def swiss_speed_longitude(
    julian_day_ut: float,
    body_id: int,
) -> tuple[float, str]:
    """Return exact geocentric apparent longitude speed in degrees/day."""

    if not SWISSEPH_AVAILABLE:
        raise RuntimeError("pyswisseph is unavailable.")

    flags = swe.FLG_SWIEPH | swe.FLG_SPEED

    with SWISSEPH_LOCK:
        if SWISSEPH_EPHE_PATH:
            swe.set_ephe_path(SWISSEPH_EPHE_PATH)

        position, return_flags = swe.calc_ut(
            julian_day_ut,
            body_id,
            flags,
        )

    return (
        float(position[3]),
        swisseph_ephemeris_mode(return_flags),
    )


def bisect_station_time(
    body_id: int,
    left_jd: float,
    right_jd: float,
    left_speed: float,
    right_speed: float,
) -> float:
    """Find a speed-zero crossing inside one bracket."""

    left = left_jd
    right = right_jd
    f_left = left_speed
    f_right = right_speed

    for _ in range(55):
        middle = (left + right) / 2.0
        f_middle, _ = swiss_speed_longitude(
            middle,
            body_id,
        )

        if abs(f_middle) < 1e-11:
            return middle

        if f_left * f_middle <= 0:
            right = middle
            f_right = f_middle
        else:
            left = middle
            f_left = f_middle

    return (left + right) / 2.0


def nearest_station_for_body(
    body_name: str,
    body_id: int,
    event_jd: float,
    local_timezone: Any,
) -> dict[str, Any]:
    """
    Find exact speed-zero crossings within the book's broad one-to-seven-day
    discussion plus one day of search margin.

    The code does not invent a planet-specific kutila window. It reports the
    exact station time, same-local-date status, and one-/seven-day proximity.
    """

    try:
        event_speed, source_mode = swiss_speed_longitude(
            event_jd,
            body_id,
        )
    except Exception as error:
        return {
            "status": "Unavailable",
            "body": body_name,
            "error": str(error),
        }

    start = event_jd - RELIABILITY_STATION_SEARCH_DAYS
    end = event_jd + RELIABILITY_STATION_SEARCH_DAYS
    step = RELIABILITY_STATION_SCAN_STEP_DAYS
    roots: list[float] = []

    try:
        previous_jd = start
        previous_speed, _ = swiss_speed_longitude(
            previous_jd,
            body_id,
        )
        current_jd = start + step

        while current_jd <= end + 1e-9:
            current_speed, _ = swiss_speed_longitude(
                current_jd,
                body_id,
            )

            if abs(previous_speed) < 1e-11:
                roots.append(previous_jd)
            elif abs(current_speed) < 1e-11:
                roots.append(current_jd)
            elif previous_speed * current_speed < 0:
                roots.append(
                    bisect_station_time(
                        body_id,
                        previous_jd,
                        current_jd,
                        previous_speed,
                        current_speed,
                    )
                )

            previous_jd = current_jd
            previous_speed = current_speed
            current_jd += step
    except Exception as error:
        return {
            "status": "Partial",
            "body": body_name,
            "event_speed_degrees_per_day": round(
                event_speed,
                10,
            ),
            "ephemeris_mode": source_mode,
            "error": str(error),
        }

    unique_roots: list[float] = []

    for root in sorted(roots):
        if (
            not unique_roots
            or abs(root - unique_roots[-1]) > 0.01
        ):
            unique_roots.append(root)

    if not unique_roots:
        return {
            "status": "Pass",
            "body": body_name,
            "event_speed_degrees_per_day": round(
                event_speed,
                10,
            ),
            "event_motion": (
                "Retrograde"
                if event_speed < 0
                else "Direct"
                if event_speed > 0
                else "Exactly stationary"
            ),
            "ephemeris_mode": source_mode,
            "nearest_station": None,
            "station_found_within_search_window": False,
            "same_local_calendar_date": False,
            "within_one_day": False,
            "within_seven_days": False,
            "automatic_veto": False,
            "error": None,
        }

    nearest = min(
        unique_roots,
        key=lambda value: abs(value - event_jd),
    )
    delta_days = nearest - event_jd
    station_utc = julian_day_to_utc_datetime(nearest)
    station_local = station_utc.astimezone(local_timezone)
    event_local = julian_day_to_utc_datetime(
        event_jd
    ).astimezone(local_timezone)
    same_local_date = (
        station_local.date() == event_local.date()
    )

    return {
        "status": "Pass",
        "body": body_name,
        "event_speed_degrees_per_day": round(
            event_speed,
            10,
        ),
        "event_motion": (
            "Retrograde"
            if event_speed < 0
            else "Direct"
            if event_speed > 0
            else "Exactly stationary"
        ),
        "ephemeris_mode": source_mode,
        "nearest_station": {
            "julian_day_ut": round(nearest, 8),
            "utc": station_utc.isoformat(),
            "local": station_local.isoformat(),
            "signed_days_from_event": round(
                delta_days,
                8,
            ),
            "absolute_days_from_event": round(
                abs(delta_days),
                8,
            ),
        },
        "station_found_within_search_window": True,
        "same_local_calendar_date": same_local_date,
        "within_one_day": abs(delta_days) <= 1.0,
        "within_seven_days": abs(delta_days) <= 7.0,
        "automatic_veto": same_local_date,
        "automatic_veto_basis": (
            "The exact station falls on the event's local calendar date."
            if same_local_date
            else None
        ),
        "error": None,
    }


def calculate_stationary_audit(
    std_time: str,
    planets: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    parsed = parse_std_time_to_utc(std_time)
    event_jd = parsed.get("julian_day_ut")
    local_datetime = datetime.fromisoformat(
        parsed["local_datetime"]
    )
    source_labels: list[dict[str, Any]] = []
    strict_label_vetoes: list[str] = []
    practical_label_vetoes: list[str] = []
    practical_label_warnings: list[str] = []

    for planet_name, result in planets.items():
        label = extract_motion_label(result)
        classification = classify_motion_label(label)
        strict_veto = motion_label_is_kutila(label)
        practical_veto = (
            motion_label_is_practical_hard_veto(label)
        )
        practical_warning = (
            motion_label_is_practical_warning(label)
        )

        source_labels.append({
            "planet": planet_name,
            "vedastro_motion_label": label,
            "classification": classification,
            "strict_book_veto": strict_veto,
            "practical_hard_veto": practical_veto,
            "practical_warning": practical_warning,
        })

        if strict_veto:
            strict_label_vetoes.append(planet_name)

        if practical_veto:
            practical_label_vetoes.append(planet_name)

        if practical_warning:
            practical_label_warnings.append(planet_name)

    if not SWISSEPH_AVAILABLE or event_jd is None:
        selected_hard_veto_planets = (
            strict_label_vetoes
            if RELIABILITY_POLICY_MODE == "strict_book"
            else practical_label_vetoes
        )

        return {
            "status": "Partial",
            "method": "VedAstro motion labels plus Swiss speed-zero search",
            "policy_mode": RELIABILITY_POLICY_MODE,
            "vedastro_motion_labels": source_labels,
            "swiss_station_search": [],
            "strict_book_hard_veto_planets": sorted(
                set(strict_label_vetoes),
                key=lambda name: PLANET_ORDER.get(name, 999),
            ),
            "practical_hard_veto_planets": sorted(
                set(practical_label_vetoes),
                key=lambda name: PLANET_ORDER.get(name, 999),
            ),
            "practical_warning_planets": sorted(
                set(practical_label_warnings),
                key=lambda name: PLANET_ORDER.get(name, 999),
            ),
            "hard_veto_planets": sorted(
                set(selected_hard_veto_planets),
                key=lambda name: PLANET_ORDER.get(name, 999),
            ),
            "strict_book_hard_veto": bool(strict_label_vetoes),
            "practical_hard_veto": bool(practical_label_vetoes),
            "hard_veto": bool(selected_hard_veto_planets),
            "warning_planets": sorted(
                set(practical_label_warnings),
                key=lambda name: PLANET_ORDER.get(name, 999),
            ),
            "confidence_cap": (
                "LOW"
                if (
                    RELIABILITY_POLICY_MODE == "practical_verified"
                    and practical_label_warnings
                    and not practical_label_vetoes
                )
                else None
            ),
            "error": "Swiss Ephemeris station search is unavailable.",
        }

    station_rows: list[dict[str, Any]] = []

    for body_name, body_id in (
        RELIABILITY_SWISS_BODY_IDS.items()
    ):
        station_rows.append(
            nearest_station_for_body(
                body_name,
                body_id,
                float(event_jd),
                local_datetime.tzinfo,
            )
        )

    same_date_planets = [
        row["body"]
        for row in station_rows
        if row.get("same_local_calendar_date")
    ]
    seven_day_planets = [
        row["body"]
        for row in station_rows
        if row.get("within_seven_days")
    ]

    strict_hard_veto_planets = sorted(
        set(strict_label_vetoes + same_date_planets),
        key=lambda name: (
            PLANET_ORDER.get(
                name,
                100 + OUTER_BODY_ORDER.index(name)
                if name in OUTER_BODY_ORDER
                else 999,
            )
        ),
    )
    practical_hard_veto_planets = sorted(
        set(practical_label_vetoes + same_date_planets),
        key=lambda name: (
            PLANET_ORDER.get(
                name,
                100 + OUTER_BODY_ORDER.index(name)
                if name in OUTER_BODY_ORDER
                else 999,
            )
        ),
    )
    practical_warning_planets = sorted(
        set(
            practical_label_warnings
            + [
                name
                for name in seven_day_planets
                if name not in practical_hard_veto_planets
            ]
        ),
        key=lambda name: (
            PLANET_ORDER.get(
                name,
                100 + OUTER_BODY_ORDER.index(name)
                if name in OUTER_BODY_ORDER
                else 999,
            )
        ),
    )

    if RELIABILITY_POLICY_MODE == "strict_book":
        selected_hard_veto_planets = strict_hard_veto_planets
        selected_warning_planets = [
            name
            for name in seven_day_planets
            if name not in selected_hard_veto_planets
        ]
    else:
        selected_hard_veto_planets = practical_hard_veto_planets
        selected_warning_planets = practical_warning_planets

    unavailable = [
        row
        for row in station_rows
        if row.get("status") not in {"Pass"}
    ]

    return {
        "status": "Pass" if not unavailable else "Partial",
        "method": "VedAstro motion labels plus Swiss speed-zero search",
        "policy_mode": RELIABILITY_POLICY_MODE,
        "book_rule": (
            "Do not wager when planets are kutila/stationary. The exact "
            "station day is especially to be avoided."
        ),
        "vedastro_motion_labels": source_labels,
        "swiss_station_search": station_rows,
        "same_local_date_station_planets": same_date_planets,
        "strict_book_hard_veto_planets": strict_hard_veto_planets,
        "practical_hard_veto_planets": practical_hard_veto_planets,
        "practical_warning_planets": practical_warning_planets,
        "hard_veto_planets": selected_hard_veto_planets,
        "strict_book_hard_veto": bool(
            strict_hard_veto_planets
        ),
        "practical_hard_veto": bool(
            practical_hard_veto_planets
        ),
        "hard_veto": bool(selected_hard_veto_planets),
        "warning_planets": selected_warning_planets,
        "confidence_cap": (
            "LOW"
            if (
                RELIABILITY_POLICY_MODE == "practical_verified"
                and selected_warning_planets
                and not selected_hard_veto_planets
            )
            else None
        ),
        "policy_explanation": (
            "strict_book treats an upstream Vikala label as a hard veto."
            if RELIABILITY_POLICY_MODE == "strict_book"
            else (
                "practical_verified keeps same-local-date or explicit "
                "Kutila/stationary conditions as hard vetoes; a non-same-date "
                "Vikala/near-station condition is a LOW-confidence warning."
            )
        ),
        "one_to_seven_day_window_note": (
            "The book gives a broad one-to-seven-day range but no exact "
            "planet-by-planet numerical threshold. Both policy decisions "
            "and exact station distances are returned transparently."
        ),
        "unavailable_bodies": unavailable,
        "pdf_pages": [38, 232, 233],
        "error": None if not unavailable else (
            "One or more optional body station searches were unavailable."
        ),
    }

def eclipse_flag_labels(
    flag: int,
    *,
    solar: bool,
) -> list[str]:
    labels: list[str] = []
    candidates = [
        ("Total", getattr(swe, "ECL_TOTAL", 0)),
        ("Partial", getattr(swe, "ECL_PARTIAL", 0)),
        ("Annular", getattr(swe, "ECL_ANNULAR", 0)),
        (
            "Hybrid",
            getattr(
                swe,
                "ECL_ANNULAR_TOTAL",
                getattr(swe, "ECL_HYBRID", 0),
            ),
        ),
        ("Central", getattr(swe, "ECL_CENTRAL", 0)),
        (
            "Noncentral",
            getattr(swe, "ECL_NONCENTRAL", 0),
        ),
    ]

    if not solar:
        candidates.append(
            (
                "Penumbral",
                getattr(swe, "ECL_PENUMBRAL", 0),
            )
        )

    for label, bit in candidates:
        if bit and flag & bit:
            labels.append(label)

    return labels or ["Unspecified"]


def calculate_one_eclipse(
    event_jd: float,
    local_timezone: Any,
    *,
    solar: bool,
    backwards: bool,
) -> dict[str, Any]:
    try:
        with SWISSEPH_LOCK:
            if SWISSEPH_EPHE_PATH:
                swe.set_ephe_path(SWISSEPH_EPHE_PATH)

            if solar:
                flag, times = swe.sol_eclipse_when_glob(
                    event_jd,
                    swe.FLG_SWIEPH,
                    0,
                    backwards,
                )
            else:
                flag, times = swe.lun_eclipse_when(
                    event_jd,
                    swe.FLG_SWIEPH,
                    0,
                    backwards,
                )

        maximum_jd = float(times[0])
        maximum_utc = julian_day_to_utc_datetime(
            maximum_jd
        )
        maximum_local = maximum_utc.astimezone(
            local_timezone
        )
        labels = eclipse_flag_labels(
            int(flag),
            solar=solar,
        )
        major = solar or "Penumbral" not in labels

        return {
            "status": "Pass",
            "kind": "Solar" if solar else "Lunar",
            "direction": (
                "Previous" if backwards else "Next"
            ),
            "type_flags": labels,
            "major_for_automatic_gate": major,
            "maximum_julian_day_ut": round(
                maximum_jd,
                8,
            ),
            "maximum_utc": maximum_utc.isoformat(),
            "maximum_local": maximum_local.isoformat(),
            "signed_days_from_event": round(
                maximum_jd - event_jd,
                8,
            ),
            "absolute_days_from_event": round(
                abs(maximum_jd - event_jd),
                8,
            ),
            "error": None,
        }
    except Exception as error:
        return {
            "status": "Unavailable",
            "kind": "Solar" if solar else "Lunar",
            "direction": (
                "Previous" if backwards else "Next"
            ),
            "error": str(error),
        }


def calculate_eclipse_audit(
    std_time: str,
) -> dict[str, Any]:
    parsed = parse_std_time_to_utc(std_time)
    event_jd = parsed.get("julian_day_ut")
    local_datetime = datetime.fromisoformat(
        parsed["local_datetime"]
    )

    if not SWISSEPH_AVAILABLE or event_jd is None:
        return {
            "status": "Unavailable",
            "events": [],
            "hard_veto": False,
            "error": "Swiss Ephemeris eclipse calculations are unavailable.",
        }

    events = [
        calculate_one_eclipse(
            float(event_jd),
            local_datetime.tzinfo,
            solar=True,
            backwards=True,
        ),
        calculate_one_eclipse(
            float(event_jd),
            local_datetime.tzinfo,
            solar=True,
            backwards=False,
        ),
        calculate_one_eclipse(
            float(event_jd),
            local_datetime.tzinfo,
            solar=False,
            backwards=True,
        ),
        calculate_one_eclipse(
            float(event_jd),
            local_datetime.tzinfo,
            solar=False,
            backwards=False,
        ),
    ]
    available = [
        event
        for event in events
        if event.get("status") == "Pass"
    ]
    major = [
        event
        for event in available
        if event.get("major_for_automatic_gate")
    ]
    nearest_major = (
        min(
            major,
            key=lambda item: item[
                "absolute_days_from_event"
            ],
        )
        if major
        else None
    )
    hard_veto = bool(
        nearest_major
        and nearest_major["absolute_days_from_event"]
        <= RELIABILITY_ECLIPSE_AVOID_DAYS
    )

    return {
        "status": (
            "Pass"
            if len(available) == 4
            else "Partial"
        ),
        "method": "Swiss Ephemeris global eclipse maxima",
        "events": events,
        "nearest_major_eclipse": nearest_major,
        "avoid_window_days_each_side": (
            RELIABILITY_ECLIPSE_AVOID_DAYS
        ),
        "hard_veto": hard_veto,
        "hard_veto_reason": (
            "The event is within three days of a major eclipse."
            if hard_veto
            else None
        ),
        "penumbral_lunar_policy": (
            "Penumbral-only lunar eclipses are reported but not "
            "automatically treated as the book's 'major eclipse'."
        ),
        "pdf_pages": [232, 233, 234],
        "error": (
            None
            if len(available) == 4
            else "One or more eclipse searches were unavailable."
        ),
    }


def calculate_solar_sankranti_audit(
    planets: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    sun = planets.get("Sun", {})
    longitude = extract_total_degrees(
        sun.get("sidereal_longitude", {})
    )

    if longitude is None:
        return {
            "status": "Unavailable",
            "hard_veto": False,
            "error": "Exact sidereal Sun longitude is unavailable.",
        }

    longitude = normalise_degrees(longitude)
    details = sign_details_from_longitude(longitude)
    degree = float(details["degree_in_sign"])
    end_of_sign = degree >= 29.0
    beginning_candidate = degree < 1.0

    return {
        "status": "Pass",
        "sidereal_longitude": round(longitude, 8),
        "sign": details["sign"],
        "degree_in_sign": round(degree, 8),
        "end_of_sign_29_degrees_or_higher": end_of_sign,
        "beginning_of_sign_under_one_degree": beginning_candidate,
        "hard_veto": end_of_sign,
        "hard_veto_reason": (
            "The sidereal Sun is at 29 degrees or higher, the book's "
            "explicit solar-sankranti rule of thumb."
            if end_of_sign
            else None
        ),
        "beginning_warning": (
            "The book also warns about the beginning of a sign but gives "
            "no exact beginning-side cutoff. This is reported for manual "
            "review and is not automatically vetoed."
            if beginning_candidate
            else None
        ),
        "pdf_pages": [232],
        "error": None,
    }


def collect_rise_set_events(
    event_jd: float,
    longitude: float,
    latitude: float,
    *,
    rise: bool,
) -> list[float]:
    events: list[float] = []
    start = event_jd - 1.5
    rsmi = swe.CALC_RISE if rise else swe.CALC_SET

    for _ in range(5):
        with SWISSEPH_LOCK:
            if SWISSEPH_EPHE_PATH:
                swe.set_ephe_path(SWISSEPH_EPHE_PATH)

            result, times = swe.rise_trans(
                start,
                swe.SUN,
                rsmi,
                (longitude, latitude, 0.0),
                0.0,
                0.0,
                swe.FLG_SWIEPH,
            )

        if result == -2:
            break

        found = float(times[0])

        if found > event_jd + 1.5:
            break

        events.append(found)
        start = found + 0.01

    return events


def nearest_rise_set_record(
    event_jd: float,
    local_timezone: Any,
    events: list[float],
    label: str,
) -> dict[str, Any] | None:
    if not events:
        return None

    nearest = min(
        events,
        key=lambda value: abs(value - event_jd),
    )
    utc = julian_day_to_utc_datetime(nearest)
    local = utc.astimezone(local_timezone)
    signed_minutes = (
        nearest - event_jd
    ) * 24.0 * 60.0

    return {
        "event": label,
        "julian_day_ut": round(nearest, 8),
        "utc": utc.isoformat(),
        "local": local.isoformat(),
        "signed_minutes_from_event": round(
            signed_minutes,
            4,
        ),
        "absolute_minutes_from_event": round(
            abs(signed_minutes),
            4,
        ),
    }


def calculate_sunrise_sunset_audit(
    std_time: str,
    location: Any,
) -> dict[str, Any]:
    parsed = parse_std_time_to_utc(std_time)
    event_jd = parsed.get("julian_day_ut")
    local_datetime = datetime.fromisoformat(
        parsed["local_datetime"]
    )

    if not SWISSEPH_AVAILABLE or event_jd is None:
        return {
            "status": "Unavailable",
            "hard_veto": False,
            "error": "Swiss Ephemeris rise/set calculations are unavailable.",
        }

    try:
        rises = collect_rise_set_events(
            float(event_jd),
            float(location.longitude),
            float(location.latitude),
            rise=True,
        )
        sets = collect_rise_set_events(
            float(event_jd),
            float(location.longitude),
            float(location.latitude),
            rise=False,
        )
        nearest_rise = nearest_rise_set_record(
            float(event_jd),
            local_datetime.tzinfo,
            rises,
            "Sunrise",
        )
        nearest_set = nearest_rise_set_record(
            float(event_jd),
            local_datetime.tzinfo,
            sets,
            "Sunset",
        )
    except Exception as error:
        return {
            "status": "Unavailable",
            "hard_veto": False,
            "error": str(error),
        }

    return {
        "status": (
            "Pass"
            if nearest_rise and nearest_set
            else "Partial"
        ),
        "nearest_sunrise": nearest_rise,
        "nearest_sunset": nearest_set,
        "hard_veto": False,
        "automatic_window_applied": False,
        "manual_review_required": True,
        "manual_review_reason": (
            "The book says not to predict at sunrise or sunset but does "
            "not provide a numerical time window. Exact distances are "
            "returned without inventing one."
        ),
        "pdf_pages": [232],
        "error": (
            None
            if nearest_rise and nearest_set
            else "A rise or set event was unavailable at this latitude."
        ),
    }


def calculate_karma_fixity_evidence(
    tier1_combinations: dict[str, Any],
    navamsha_interpretation: dict[str, Any],
    kp_sublords: dict[str, Any],
) -> dict[str, Any]:
    """
    Build raw and independent-family Rule-of-Three ledgers.

    Printed page 28 asks for repeated indications. It does not say that every
    derived output from one planet or one geometry is independent. Therefore
    the automatic Rule of Three uses distinct technique families, while every
    raw testimony remains visible for audit.
    """

    raw_evidence: list[dict[str, Any]] = []

    def add_evidence(
        *,
        source: str,
        family: str,
        tier: int,
        supports: str | None,
        **extra: Any,
    ) -> None:
        if supports not in DECISION_SIDES:
            return

        raw_evidence.append({
            "source": source,
            "family": family,
            "tier": tier,
            "supports": supports,
            **extra,
        })

    tier1_total = tier1_combinations.get(
        "automatic_signed_total"
    )

    if isinstance(tier1_total, (int, float)):
        if tier1_total > 0:
            add_evidence(
                source="Victory-house signed total",
                family="Victory houses",
                tier=1,
                supports="Favourite",
                value=tier1_total,
            )
        elif tier1_total < 0:
            add_evidence(
                source="Victory-house signed total",
                family="Victory houses",
                tier=1,
                supports="Underdog",
                value=tier1_total,
            )

    for side, result in tier1_combinations.get(
        "sky_pky",
        {},
    ).get("sides", {}).items():
        sky = result.get("sky", {})
        pky = result.get("pky", {})

        if sky.get("formed"):
            add_evidence(
                source=f"{side} SKY",
                family="SKY/PKY",
                tier=2,
                supports=side,
                condition=sky.get("condition"),
            )

        if pky.get("formed"):
            add_evidence(
                source=f"{side} PKY",
                family="SKY/PKY",
                tier=2,
                supports=opposite_contest_side(side),
                condition=pky.get("condition"),
            )

    for contact in navamsha_interpretation.get(
        "d1_cusp_contacts",
        [],
    ):
        effect = contact.get("book_effect", {})

        if (
            effect.get("decision_eligible") is True
            and effect.get("automatic_decision_use") is not False
            and effect.get("research_only") is not True
        ):
            add_evidence(
                source="D1 cusp contact",
                family="D1 cusps",
                tier=2,
                supports=effect.get("supports"),
                body=contact.get("body"),
                cusp=contact.get("cusp"),
                independence_key=effect.get(
                    "independence_key"
                ),
            )

    for contact in navamsha_interpretation.get(
        "d9_cusp_contacts",
        [],
    ):
        effect = contact.get("book_effect", {})

        if (
            effect.get("decision_eligible") is True
            and effect.get("automatic_decision_use") is not False
            and effect.get("research_only") is not True
        ):
            add_evidence(
                source="D9 cusp contact",
                family="D9 cusps",
                tier=3,
                supports=effect.get("supports"),
                body=contact.get("body"),
                cusp=contact.get("cusp"),
                independence_key=effect.get(
                    "independence_key"
                ),
            )

    combos = navamsha_interpretation.get(
        "navamsha_combinations",
        {},
    )

    for combo in combos.get("combinations", []) or []:
        if (
            combo.get("points_applied")
            and combo.get("supports") in DECISION_SIDES
        ):
            add_evidence(
                source="D9 combination",
                family="D9 combinations",
                tier=1,
                supports=combo.get("supports"),
                planets=combo.get("planets"),
                overlap_cluster_id=combo.get(
                    "overlap_cluster_id"
                ),
            )

    kp = kp_sublords.get(
        "main_sublord_comparison",
        {},
    )
    kp_indication = kp.get("indication")

    if kp_indication in DECISION_SIDES:
        add_evidence(
            source="KP main sublord comparison",
            family="KP sublords",
            tier=2,
            supports=kp_indication,
            value=kp.get(
                "signed_favourite_differential"
            ),
        )

    # One family can support both sides and therefore remain contradictory.
    family_direction: dict[str, dict[str, Any]] = {}

    for family in sorted({
        item["family"]
        for item in raw_evidence
    }):
        family_items = [
            item
            for item in raw_evidence
            if item["family"] == family
        ]
        sides = sorted({
            item["supports"]
            for item in family_items
        })

        if sides == ["Favourite"]:
            direction = "Favourite"
        elif sides == ["Underdog"]:
            direction = "Underdog"
        else:
            direction = "Mixed"

        family_direction[family] = {
            "direction": direction,
            "raw_testimony_count": len(family_items),
            "favourite_raw_count": sum(
                1
                for item in family_items
                if item["supports"] == "Favourite"
            ),
            "underdog_raw_count": sum(
                1
                for item in family_items
                if item["supports"] == "Underdog"
            ),
            "highest_tier": max(
                item.get("tier", 0)
                for item in family_items
            ),
        }

    counts: dict[str, dict[str, Any]] = {}

    for side in ("Favourite", "Underdog"):
        raw_side = [
            item
            for item in raw_evidence
            if item["supports"] == side
        ]
        supporting_families = [
            family
            for family, result in family_direction.items()
            if result["direction"] == side
        ]
        mixed_families = [
            family
            for family, result in family_direction.items()
            if result["direction"] == "Mixed"
        ]

        counts[side] = {
            "raw_testimony_count": len(raw_side),
            "independent_family_count": len(
                supporting_families
            ),
            "supporting_families": supporting_families,
            "mixed_families_not_counted": mixed_families,
            "tier3_raw_count": sum(
                1
                for item in raw_side
                if item.get("tier") == 3
            ),
            "tier2_raw_count": sum(
                1
                for item in raw_side
                if item.get("tier") == 2
            ),
            "tier1_raw_count": sum(
                1
                for item in raw_side
                if item.get("tier") == 1
            ),
        }

    return {
        "status": "Pass",
        "method": "Book rule-of-three independent-family ledger",
        "raw_evidence": raw_evidence,
        # Backward-compatible key used by compactors and older prompts.
        "evidence": raw_evidence,
        "family_direction": family_direction,
        "counts": counts,
        "rule_of_three_reached": {
            side: (
                counts[side][
                    "independent_family_count"
                ] >= 3
            )
            for side in ("Favourite", "Underdog")
        },
        "rule_of_three_basis": (
            "Distinct technique families, not raw derived rows."
        ),
        "automatic_karma_classification": None,
        "automatic_classification_allowed": False,
        "manual_classification_required": True,
        "reason": (
            "The book defines fixed, mixed and nonfixed karma and recommends "
            "repeated testimony, but it does not provide a complete numerical "
            "classifier. Correlated outputs from one technique family are "
            "not counted as independent."
        ),
        "pdf_pages": [26, 27, 28],
    }


def calculate_reliability_audit(
    std_time: str,
    location: Any,
    planets: dict[str, dict[str, Any]],
    tier1_combinations: dict[str, Any],
    navamsha_interpretation: dict[str, Any],
    kp_sublords: dict[str, Any],
) -> dict[str, Any]:
    """Calculate strict-book and practical-verified reliability decisions."""

    stationary = calculate_stationary_audit(
        std_time,
        planets,
    )
    eclipses = calculate_eclipse_audit(std_time)
    sankranti = calculate_solar_sankranti_audit(
        planets
    )
    sunrise_sunset = calculate_sunrise_sunset_audit(
        std_time,
        location,
    )
    karma = calculate_karma_fixity_evidence(
        tier1_combinations,
        navamsha_interpretation,
        kp_sublords,
    )

    strict_reasons: list[str] = []
    practical_reasons: list[str] = []

    strict_station_planets = stationary.get(
        "strict_book_hard_veto_planets",
        [],
    )
    practical_station_planets = stationary.get(
        "practical_hard_veto_planets",
        [],
    )

    if strict_station_planets:
        strict_reasons.append(
            "Kutila/stationary planet veto: "
            + ", ".join(strict_station_planets)
        )

    if practical_station_planets:
        practical_reasons.append(
            "Exact or same-local-date stationary veto: "
            + ", ".join(practical_station_planets)
        )

    if eclipses.get("hard_veto"):
        reason = "Within three days of a major eclipse"
        strict_reasons.append(reason)
        practical_reasons.append(reason)

    if sankranti.get("hard_veto"):
        reason = "Sidereal Sun at 29 degrees or higher"
        strict_reasons.append(reason)
        practical_reasons.append(reason)

    warning_reasons: list[str] = []

    if stationary.get("warning_planets"):
        warning_reasons.append(
            "Manual station caution: "
            + ", ".join(stationary["warning_planets"])
        )

    if sankranti.get(
        "beginning_of_sign_under_one_degree"
    ):
        warning_reasons.append(
            "Sun is near the beginning of a sidereal sign; the book "
            "gives no exact beginning-side cutoff."
        )

    if sunrise_sunset.get("manual_review_required"):
        warning_reasons.append(
            "Sunrise/sunset distance requires manual review because "
            "the book gives no numerical window."
        )

    strict_book_hard_veto = bool(strict_reasons)
    practical_hard_veto = bool(practical_reasons)

    if RELIABILITY_POLICY_MODE == "strict_book":
        selected_hard_veto = strict_book_hard_veto
        selected_reasons = strict_reasons
    else:
        selected_hard_veto = practical_hard_veto
        selected_reasons = practical_reasons

    sublayers = {
        "stationary_kutila": stationary,
        "eclipses": eclipses,
        "solar_sankranti": sankranti,
        "sunrise_sunset": sunrise_sunset,
        "karma_fixity": karma,
    }
    unavailable = [
        name
        for name, layer in sublayers.items()
        if layer.get("status") == "Unavailable"
    ]
    partial = [
        name
        for name, layer in sublayers.items()
        if layer.get("status") == "Partial"
    ]

    confidence_cap = stationary.get("confidence_cap")

    return {
        "status": (
            "Pass"
            if not unavailable and not partial
            else "Partial"
        ),
        "method": "DualPolicyBookReliabilityAndSandhiAudit",
        "policy_mode": RELIABILITY_POLICY_MODE,
        "book_chapters": [2, 9],
        "strict_book_hard_veto": strict_book_hard_veto,
        "strict_book_prediction_allowed": (
            not strict_book_hard_veto
        ),
        "strict_book_hard_veto_reasons": strict_reasons,
        "practical_hard_veto": practical_hard_veto,
        "practical_prediction_allowed": (
            not practical_hard_veto
        ),
        "practical_hard_veto_reasons": practical_reasons,
        "hard_veto": selected_hard_veto,
        "strict_prediction_allowed_by_reliability": (
            not selected_hard_veto
        ),
        "decision": (
            "Avoid"
            if selected_hard_veto
            else "Proceed with LOW confidence"
            if confidence_cap == "LOW"
            else "Proceed with manual sandhi review"
            if warning_reasons
            else "No automatic reliability veto"
        ),
        "confidence_cap": confidence_cap,
        "hard_veto_reasons": selected_reasons,
        "warning_reasons": warning_reasons,
        "stationary_kutila": stationary,
        "eclipses": eclipses,
        "solar_sankranti": sankranti,
        "sunrise_sunset": sunrise_sunset,
        "karma_fixity": karma,
        "performance_fallback_recommended": (
            selected_hard_veto
        ),
        "market_assignment_note": (
            "The astronomical chart does not change when the market "
            "favourite changes. Remap participant labels using the frozen "
            "pre-call consensus; recalculate only if event time/location "
            "changes or participant-dependent name sounds were used."
        ),
        "unavailable_sublayers": unavailable,
        "partial_sublayers": partial,
        "pdf_pages": RELIABILITY_AUDIT_PDF_PAGES,
        "points_applied": False,
        "error": (
            None
            if not unavailable
            else "One or more essential reliability sublayers are unavailable."
        ),
    }


# ============================================================
# CONSISTENCY VALIDATION
# ============================================================

NAKSHATRA_ALLOWED_SIGNS = {
    "Ashwini": {"Aries"},
    "Bharani": {"Aries"},
    "Krittika": {"Aries", "Taurus"},
    "Rohini": {"Taurus"},
    "Mrigashirsha": {"Taurus", "Gemini"},
    "Ardra": {"Gemini"},
    "Punarvasu": {"Gemini", "Cancer"},
    "Pushya": {"Cancer"},
    "Ashlesha": {"Cancer"},
    "Magha": {"Leo"},
    "Purva Phalguni": {"Leo"},
    "Uttara Phalguni": {"Leo", "Virgo"},
    "Hasta": {"Virgo"},
    "Chitra": {"Virgo", "Libra"},
    "Swati": {"Libra"},
    "Vishakha": {"Libra", "Scorpio"},
    "Anuradha": {"Scorpio"},
    "Jyeshtha": {"Scorpio"},
    "Mula": {"Sagittarius"},
    "Purva Ashadha": {"Sagittarius"},
    "Uttara Ashadha": {"Sagittarius", "Capricorn"},
    "Shravana": {"Capricorn"},
    "Dhanishta": {"Capricorn", "Aquarius"},
    "Shatabhisha": {"Aquarius"},
    "Purva Bhadrapada": {"Aquarius", "Pisces"},
    "Uttara Bhadrapada": {"Pisces"},
    "Revati": {"Pisces"},
}

SIGN_INDEX = {
    "Aries": 0,
    "Taurus": 1,
    "Gemini": 2,
    "Cancer": 3,
    "Leo": 4,
    "Virgo": 5,
    "Libra": 6,
    "Scorpio": 7,
    "Sagittarius": 8,
    "Capricorn": 9,
    "Aquarius": 10,
    "Pisces": 11,
}


def validate_moon_consistency(
    moon_sign_result: dict[str, Any],
    moon_nakshatra_result: dict[str, Any],
) -> dict[str, Any]:
    moon_sign = extract_sign_name(moon_sign_result)
    nakshatra = extract_nakshatra_name(moon_nakshatra_result)

    if not moon_sign or not nakshatra:
        return {
            "status": "Fail",
            "required": True,
            "method": "MoonSignNakshatraConsistency",
            "error": "Could not parse Moon sign or Moon nakshatra.",
            "moon_sign": moon_sign,
            "nakshatra": nakshatra,
        }

    allowed_signs = NAKSHATRA_ALLOWED_SIGNS.get(nakshatra, set())
    is_consistent = moon_sign in allowed_signs

    return {
        "status": "Pass" if is_consistent else "Fail",
        "required": True,
        "method": "MoonSignNakshatraConsistency",
        "moon_sign": moon_sign,
        "nakshatra": nakshatra,
        "allowed_signs": sorted(allowed_signs),
        "error": (
            None
            if is_consistent
            else (
                f"Moon sign {moon_sign} is inconsistent with "
                f"nakshatra {nakshatra}."
            )
        ),
    }


def validate_sign_longitude(
    label: str,
    sign_result: dict[str, Any],
    longitude_result: dict[str, Any],
    required: bool,
) -> dict[str, Any]:
    sign = extract_sign_name(sign_result)
    longitude = extract_total_degrees(longitude_result)

    if sign is None or longitude is None:
        return {
            "status": "Fail" if required else "Unavailable",
            "required": required,
            "method": "PlanetSignLongitudeConsistency",
            "planet": label,
            "sign": sign,
            "total_degrees": longitude,
            "error": "Could not parse sign or total sidereal longitude.",
        }

    normalised_longitude = longitude % 360.0
    longitude_sign_index = int(normalised_longitude // 30.0)
    expected_index = SIGN_INDEX[sign]
    is_consistent = longitude_sign_index == expected_index

    return {
        "status": "Pass" if is_consistent else "Fail",
        "required": required,
        "method": "PlanetSignLongitudeConsistency",
        "planet": label,
        "sign": sign,
        "total_degrees": longitude,
        "longitude_sign_index": longitude_sign_index,
        "expected_sign_index": expected_index,
        "error": (
            None
            if is_consistent
            else (
                f"{label} sign {sign} does not match sidereal "
                f"longitude {longitude}."
            )
        ),
    }


def validate_sun_moon_distinction(
    planets: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    sun = planets.get("Sun")
    moon = planets.get("Moon")

    if not sun or not moon:
        return {
            "status": "Fail",
            "required": True,
            "method": "SunMoonDistinction",
            "error": "Sun or Moon planetary data is missing.",
        }

    sun_sign = extract_sign_name(sun["d1_sign"])
    moon_sign = extract_sign_name(moon["d1_sign"])
    sun_longitude = extract_total_degrees(sun["sidereal_longitude"])
    moon_longitude = extract_total_degrees(moon["sidereal_longitude"])

    if sun_longitude is None or moon_longitude is None:
        return {
            "status": "Fail",
            "required": True,
            "method": "SunMoonDistinction",
            "sun_sign": sun_sign,
            "moon_sign": moon_sign,
            "sun_longitude": sun_longitude,
            "moon_longitude": moon_longitude,
            "error": "Could not parse Sun or Moon longitude.",
        }

    distinct = abs((sun_longitude % 360) - (moon_longitude % 360)) > 0.0001

    return {
        "status": "Pass" if distinct else "Fail",
        "required": True,
        "method": "SunMoonDistinction",
        "sun_sign": sun_sign,
        "moon_sign": moon_sign,
        "sun_longitude": sun_longitude,
        "moon_longitude": moon_longitude,
        "error": None if distinct else "Sun and Moon returned identical data.",
    }


# ============================================================
# CHART COMPONENTS
# ============================================================

def calculate_lagna(event_time: Time) -> dict[str, Any]:
    if hasattr(Calculate, "LagnaSignName"):
        return vedastro_call(
            "LagnaSignName",
            event_time,
            required=True,
        )

    return vedastro_call(
        "HouseSignName",
        HouseName.House1,
        event_time,
        required=True,
    )


def calculate_moon_sign(event_time: Time) -> dict[str, Any]:
    return vedastro_call(
        ["PlanetRasiD1Sign", "PlanetSignName"],
        PlanetName.Moon,
        event_time,
        required=True,
    )


def calculate_yoga(event_time: Time) -> dict[str, Any]:
    return vedastro_call(
        ["NithyaYoga", "Yoga"],
        event_time,
    )


def calculate_house(
    house_name: str,
    event_time: Time,
) -> dict[str, Any]:
    house = HOUSES[house_name]
    is_essential = house_name in {"House1", "House7"}

    sign = vedastro_call(
        "HouseSignName",
        house,
        event_time,
        required=is_essential,
    )

    lord = vedastro_call(
        "LordOfHouse",
        house,
        event_time,
        required=is_essential,
    )

    constellation = vedastro_call(
        "HouseConstellation",
        house,
        event_time,
    )

    constellation_lord = vedastro_call(
        "HouseConstellationLord",
        house,
        event_time,
    )

    aspects = vedastro_call(
        "PlanetsAspectingHouse",
        house,
        event_time,
    )

    passed = sign["status"] == "Pass" and lord["status"] == "Pass"

    return {
        "status": "Pass" if passed else "Fail",
        "house": house_name,
        "sign": sign,
        "lord": lord,
        "constellation": constellation,
        "constellation_lord": constellation_lord,
        "aspects": aspects,
    }


def calculate_planet(
    planet_name: str,
    event_time: Time,
    precomputed_moon_sign: dict[str, Any] | None = None,
) -> dict[str, Any]:
    planet = PLANETS[planet_name]
    is_essential = planet_name in {"Sun", "Moon"}

    if planet_name == "Moon" and precomputed_moon_sign is not None:
        d1_sign = precomputed_moon_sign
    else:
        d1_sign = vedastro_call(
            ["PlanetRasiD1Sign", "PlanetSignName"],
            planet,
            event_time,
            required=is_essential,
        )

    d9_sign = vedastro_call(
        ["PlanetNavamshaD9Sign", "PlanetNavamshaSign"],
        planet,
        event_time,
        required=is_essential,
    )

    longitude = vedastro_call(
        "PlanetNirayanaLongitude",
        planet,
        event_time,
        required=is_essential,
    )

    motion = vedastro_call(
        "PlanetMotionName",
        planet,
        event_time,
    )

    retrograde = vedastro_call(
        "IsPlanetRetrograde",
        planet,
        event_time,
    )

    combust = vedastro_call(
        "IsPlanetCombust",
        planet,
        event_time,
    )

    exalted = vedastro_call(
        "IsPlanetExaltedSign",
        planet,
        event_time,
    )

    debilitated = vedastro_call(
        "IsPlanetDebilitated",
        planet,
        event_time,
    )

    own_sign = vedastro_call(
        "IsPlanetInOwnSign",
        planet,
        event_time,
    )

    moolatrikona = vedastro_call(
        "IsPlanetInMoolatrikona",
        planet,
        event_time,
    )

    shadbala = vedastro_call(
        "PlanetShadbalaPinda",
        planet,
        event_time,
    )

    sign_longitude_consistency = validate_sign_longitude(
        planet_name,
        d1_sign,
        longitude,
        required=is_essential,
    )

    passed = all(
        result["status"] == "Pass"
        for result in (
            d1_sign,
            d9_sign,
            longitude,
            sign_longitude_consistency,
        )
    )

    return {
        "status": "Pass" if passed else "Fail",
        "requested_planet": planet_name,
        "request_shape": {
            "PlanetName": {"Name": planet_name}
        },
        "d1_sign": d1_sign,
        "d9_sign": d9_sign,
        "sidereal_longitude": longitude,
        "sign_longitude_consistency": sign_longitude_consistency,
        "motion": motion,
        "retrograde": retrograde,
        "combust": combust,
        "exalted": exalted,
        "debilitated": debilitated,
        "own_sign": own_sign,
        "moolatrikona": moolatrikona,
        "shadbala": shadbala,
    }


# ============================================================
# DATABASE — CHECKPOINT DB2
# ============================================================

DATABASE_SCHEMA_STARTUP_STATUS: dict[str, Any] = {
    "status": "not_run",
    "schema_version": DATABASE_SCHEMA_VERSION,
}


def _database_connect():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not configured.")
    if psycopg is None:
        raise RuntimeError(
            "The psycopg driver is unavailable. "
            "Add psycopg[binary]>=3.2,<4 to requirements.txt."
        )
    return psycopg.connect(
        DATABASE_URL,
        connect_timeout=DATABASE_CONNECT_TIMEOUT_SECONDS,
    )


def database_connection_status() -> dict[str, Any]:
    """
    Verify that Render can reach PostgreSQL using DATABASE_URL.

    This check performs SELECT 1 only and never exposes credentials.
    """
    if not DATABASE_URL:
        return {
            "status": "not_configured",
            "connected": False,
            "proxy_version": PROXY_VERSION,
            "message": "DATABASE_URL is not configured.",
        }

    if psycopg is None:
        return {
            "status": "driver_missing",
            "connected": False,
            "proxy_version": PROXY_VERSION,
            "message": (
                "The psycopg driver is unavailable. Add "
                "psycopg[binary]>=3.2,<4 to requirements.txt."
            ),
        }

    try:
        with psycopg.connect(
            DATABASE_URL,
            connect_timeout=DATABASE_CONNECT_TIMEOUT_SECONDS,
            autocommit=True,
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT 1, current_setting('server_version')"
                )
                row = cursor.fetchone()

        return {
            "status": "ok",
            "connected": bool(row and row[0] == 1),
            "proxy_version": PROXY_VERSION,
            "postgres_version": row[1] if row else None,
            "operation": "SELECT 1 only",
        }
    except Exception as exc:
        return {
            "status": "error",
            "connected": False,
            "proxy_version": PROXY_VERSION,
            "error_type": type(exc).__name__,
            "message": "Database connection failed.",
        }


def database_schema_statements() -> list[str]:
    """
    Return the idempotent PostgreSQL schema used by the automated platform.

    The schema stores immutable pre-match predictions separately from results
    and audits so a completed event can never rewrite the original forecast.
    """
    return [
        """
        CREATE TABLE IF NOT EXISTS app_metadata (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS fixtures (
            id BIGSERIAL PRIMARY KEY,
            provider TEXT NOT NULL,
            provider_fixture_id TEXT NOT NULL,
            sport TEXT NOT NULL DEFAULT 'soccer',
            competition_name TEXT NOT NULL,
            competition_country TEXT,
            season TEXT,
            home_team TEXT NOT NULL,
            away_team TEXT NOT NULL,
            kickoff_utc TIMESTAMPTZ NOT NULL,
            venue_name TEXT,
            venue_city TEXT,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            timezone_name TEXT,
            fixture_status TEXT NOT NULL DEFAULT 'scheduled',
            neutral_venue BOOLEAN NOT NULL DEFAULT FALSE,
            raw_fixture_json JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (provider, provider_fixture_id)
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_fixtures_kickoff_utc
            ON fixtures (kickoff_utc)
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_fixtures_status_kickoff
            ON fixtures (fixture_status, kickoff_utc)
        """,
        """
        ALTER TABLE fixtures
            ADD COLUMN IF NOT EXISTS location_source TEXT
        """,
        """
        ALTER TABLE fixtures
            ADD COLUMN IF NOT EXISTS location_confidence NUMERIC(6,3)
        """,
        """
        ALTER TABLE fixtures
            ADD COLUMN IF NOT EXISTS location_verified_at TIMESTAMPTZ
        """,
        """
        CREATE TABLE IF NOT EXISTS venue_geocodes (
            id BIGSERIAL PRIMARY KEY,
            query_hash TEXT NOT NULL UNIQUE,
            provider TEXT NOT NULL,
            query_text TEXT NOT NULL,
            venue_name TEXT,
            expected_city TEXT,
            expected_country TEXT,
            provider_place_id TEXT,
            display_name TEXT,
            latitude DOUBLE PRECISION NOT NULL,
            longitude DOUBLE PRECISION NOT NULL,
            timezone_name TEXT,
            candidate_country TEXT,
            candidate_country_code TEXT,
            candidate_city TEXT,
            venue_token_overlap NUMERIC(6,3),
            confidence_score NUMERIC(6,3) NOT NULL,
            country_match BOOLEAN NOT NULL DEFAULT FALSE,
            city_match BOOLEAN NOT NULL DEFAULT FALSE,
            sports_place_match BOOLEAN NOT NULL DEFAULT FALSE,
            decision_status TEXT NOT NULL CHECK (
                decision_status IN (
                    'PREVIEW',
                    'AUTO_APPROVED',
                    'REJECTED',
                    'MANUALLY_APPROVED'
                )
            ),
            raw_response_json JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_venue_geocodes_status
            ON venue_geocodes (decision_status, confidence_score DESC)
        """,
        """
        CREATE TABLE IF NOT EXISTS location_contexts (
            id BIGSERIAL PRIMARY KEY,
            context_hash TEXT NOT NULL UNIQUE,
            provider TEXT NOT NULL,
            city_name TEXT NOT NULL,
            country_name TEXT NOT NULL,
            provider_place_id TEXT,
            display_name TEXT,
            latitude DOUBLE PRECISION NOT NULL,
            longitude DOUBLE PRECISION NOT NULL,
            country_code TEXT,
            candidate_country TEXT,
            candidate_city TEXT,
            country_match BOOLEAN NOT NULL DEFAULT FALSE,
            city_match BOOLEAN NOT NULL DEFAULT FALSE,
            raw_response_json JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_location_contexts_city_country
            ON location_contexts (city_name, country_name)
        """,
        """
        CREATE TABLE IF NOT EXISTS location_attempts (
            id BIGSERIAL PRIMARY KEY,
            query_hash TEXT NOT NULL UNIQUE,
            fixture_id BIGINT REFERENCES fixtures(id) ON DELETE SET NULL,
            strategy_version TEXT NOT NULL,
            query_text TEXT NOT NULL,
            attempt_status TEXT NOT NULL,
            provider_call_count INTEGER NOT NULL DEFAULT 0,
            message TEXT,
            raw_response_json JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_location_attempts_status
            ON location_attempts (attempt_status, created_at DESC)
        """,
        """
        CREATE TABLE IF NOT EXISTS location_reviews (
            id BIGSERIAL PRIMARY KEY,
            review_key TEXT NOT NULL UNIQUE,
            venue_name TEXT NOT NULL,
            venue_city TEXT NOT NULL,
            venue_country TEXT NOT NULL,
            provider TEXT NOT NULL,
            provider_place_id TEXT,
            provider_latitude DOUBLE PRECISION NOT NULL,
            provider_longitude DOUBLE PRECISION NOT NULL,
            provider_timezone TEXT NOT NULL,
            external_source_name TEXT NOT NULL,
            external_source_reference TEXT NOT NULL,
            external_latitude DOUBLE PRECISION NOT NULL,
            external_longitude DOUBLE PRECISION NOT NULL,
            separation_meters DOUBLE PRECISION NOT NULL,
            review_status TEXT NOT NULL CHECK (
                review_status IN (
                    'APPROVED',
                    'REJECTED',
                    'COMMITTED'
                )
            ),
            fixtures_updated INTEGER NOT NULL DEFAULT 0,
            review_notes TEXT,
            reviewed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            committed_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_location_reviews_status
            ON location_reviews (review_status, reviewed_at DESC)
        """,
        """
        CREATE TABLE IF NOT EXISTS odds_snapshots (
            id BIGSERIAL PRIMARY KEY,
            fixture_id BIGINT NOT NULL REFERENCES fixtures(id)
                ON DELETE CASCADE,
            provider TEXT NOT NULL,
            captured_at TIMESTAMPTZ NOT NULL,
            home_odds NUMERIC(10,4),
            draw_odds NUMERIC(10,4),
            away_odds NUMERIC(10,4),
            no_margin_home NUMERIC(10,6),
            no_margin_draw NUMERIC(10,6),
            no_margin_away NUMERIC(10,6),
            consensus_favourite TEXT CHECK (
                consensus_favourite IN ('HOME', 'DRAW', 'AWAY', 'PICKEM')
            ),
            bookmaker_count INTEGER,
            raw_odds_json JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (fixture_id, provider, captured_at)
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_odds_fixture_captured
            ON odds_snapshots (fixture_id, captured_at DESC)
        """,
        """
        CREATE TABLE IF NOT EXISTS performance_snapshots (
            id BIGSERIAL PRIMARY KEY,
            fixture_id BIGINT NOT NULL REFERENCES fixtures(id)
                ON DELETE CASCADE,
            provider TEXT NOT NULL,
            captured_at TIMESTAMPTZ NOT NULL,
            starting_xi_confirmed BOOLEAN NOT NULL DEFAULT FALSE,
            injuries_confirmed BOOLEAN NOT NULL DEFAULT FALSE,
            home_features JSONB NOT NULL DEFAULT '{}'::jsonb,
            away_features JSONB NOT NULL DEFAULT '{}'::jsonb,
            draw_features JSONB NOT NULL DEFAULT '{}'::jsonb,
            raw_performance_json JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (fixture_id, provider, captured_at)
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_performance_fixture_captured
            ON performance_snapshots (fixture_id, captured_at DESC)
        """,
        """
        CREATE TABLE IF NOT EXISTS chart_runs (
            id BIGSERIAL PRIMARY KEY,
            fixture_id BIGINT NOT NULL REFERENCES fixtures(id)
                ON DELETE CASCADE,
            event_id TEXT NOT NULL UNIQUE,
            chart_version INTEGER NOT NULL DEFAULT 1,
            venue_local_std_time TEXT NOT NULL,
            venue_name TEXT NOT NULL,
            latitude DOUBLE PRECISION NOT NULL,
            longitude DOUBLE PRECISION NOT NULL,
            favourite_participant TEXT NOT NULL,
            opponent_participant TEXT NOT NULL,
            action_input JSONB NOT NULL,
            action_output JSONB NOT NULL,
            validation_status TEXT NOT NULL,
            practical_hard_veto BOOLEAN,
            confidence_cap TEXT,
            cluster_signature TEXT,
            backend_version TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_chart_runs_fixture
            ON chart_runs (fixture_id, created_at DESC)
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_chart_runs_cluster_signature
            ON chart_runs (cluster_signature)
        """,
        """
        CREATE TABLE IF NOT EXISTS prediction_runs (
            id BIGSERIAL PRIMARY KEY,
            fixture_id BIGINT NOT NULL REFERENCES fixtures(id)
                ON DELETE CASCADE,
            chart_run_id BIGINT REFERENCES chart_runs(id)
                ON DELETE RESTRICT,
            odds_snapshot_id BIGINT REFERENCES odds_snapshots(id)
                ON DELETE RESTRICT,
            performance_snapshot_id BIGINT REFERENCES performance_snapshots(id)
                ON DELETE RESTRICT,
            event_id TEXT NOT NULL,
            prediction_version INTEGER NOT NULL DEFAULT 1,
            prediction_horizon TEXT NOT NULL CHECK (
                prediction_horizon IN (
                    'PRELIMINARY', 'FINAL_PREMATCH', 'PERFORMANCE_ONLY'
                )
            ),
            predicted_outcome TEXT NOT NULL CHECK (
                predicted_outcome IN ('HOME', 'DRAW', 'AWAY')
            ),
            market_baseline TEXT CHECK (
                market_baseline IN ('HOME', 'DRAW', 'AWAY')
            ),
            house1_participant TEXT,
            house7_participant TEXT,
            confidence TEXT CHECK (
                confidence IN ('HIGH', 'MEDIUM', 'LOW')
            ),
            eligibility TEXT CHECK (
                eligibility IN ('CONDITIONAL', 'NO', 'RESEARCH_ONLY')
            ),
            astrology_reliability TEXT,
            evidence_strength TEXT,
            signed_interval_low NUMERIC(12,4),
            signed_interval_high NUMERIC(12,4),
            model_version TEXT NOT NULL,
            instruction_version TEXT NOT NULL,
            backend_version TEXT NOT NULL,
            prediction_payload JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            frozen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (event_id, prediction_version)
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_predictions_fixture_created
            ON prediction_runs (fixture_id, created_at DESC)
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_predictions_outcome
            ON prediction_runs (predicted_outcome)
        """,
        """
        CREATE TABLE IF NOT EXISTS official_results (
            id BIGSERIAL PRIMARY KEY,
            fixture_id BIGINT NOT NULL UNIQUE REFERENCES fixtures(id)
                ON DELETE CASCADE,
            provider TEXT NOT NULL,
            provider_status TEXT NOT NULL,
            home_score_90 INTEGER,
            away_score_90 INTEGER,
            actual_result_90 TEXT CHECK (
                actual_result_90 IN ('HOME', 'DRAW', 'AWAY')
            ),
            extra_time_played BOOLEAN NOT NULL DEFAULT FALSE,
            penalties_played BOOLEAN NOT NULL DEFAULT FALSE,
            verified_at TIMESTAMPTZ NOT NULL,
            raw_result_json JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS post_match_audits (
            id BIGSERIAL PRIMARY KEY,
            prediction_id BIGINT NOT NULL UNIQUE REFERENCES prediction_runs(id)
                ON DELETE RESTRICT,
            result_id BIGINT NOT NULL REFERENCES official_results(id)
                ON DELETE RESTRICT,
            prediction_correct BOOLEAN NOT NULL,
            market_baseline_correct BOOLEAN,
            error_labels TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
            primary_error_label TEXT,
            knowable_before_kickoff JSONB NOT NULL DEFAULT '[]'::jsonb,
            not_knowable_before_kickoff JSONB NOT NULL DEFAULT '[]'::jsonb,
            strongest_failure_reason TEXT,
            audit_summary TEXT NOT NULL,
            rule_change_recommended BOOLEAN NOT NULL DEFAULT FALSE,
            audit_version TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_audits_primary_error
            ON post_match_audits (primary_error_label)
        """,
        """
        CREATE TABLE IF NOT EXISTS model_versions (
            id BIGSERIAL PRIMARY KEY,
            model_name TEXT NOT NULL,
            model_version TEXT NOT NULL UNIQUE,
            status TEXT NOT NULL CHECK (
                status IN ('CHAMPION', 'CHALLENGER', 'REJECTED', 'ARCHIVED')
            ),
            trained_until TIMESTAMPTZ,
            training_match_count INTEGER NOT NULL DEFAULT 0,
            feature_schema_version TEXT,
            dataset_hash TEXT,
            metrics_json JSONB NOT NULL DEFAULT '{}'::jsonb,
            model_artifact_uri TEXT,
            promoted_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS training_runs (
            id BIGSERIAL PRIMARY KEY,
            challenger_model_version TEXT NOT NULL,
            champion_model_version TEXT,
            training_started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            training_completed_at TIMESTAMPTZ,
            training_match_count INTEGER,
            validation_match_count INTEGER,
            test_match_count INTEGER,
            metrics_json JSONB NOT NULL DEFAULT '{}'::jsonb,
            promotion_decision TEXT CHECK (
                promotion_decision IN ('PENDING', 'PROMOTE', 'REJECT')
            ) DEFAULT 'PENDING',
            notes TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        """
        CREATE OR REPLACE FUNCTION touch_updated_at()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.updated_at = NOW();
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql
        """,
        """
        DROP TRIGGER IF EXISTS fixtures_touch_updated_at ON fixtures
        """,
        """
        CREATE TRIGGER fixtures_touch_updated_at
        BEFORE UPDATE ON fixtures
        FOR EACH ROW EXECUTE FUNCTION touch_updated_at()
        """,
        """
        DROP TRIGGER IF EXISTS venue_geocodes_touch_updated_at
            ON venue_geocodes
        """,
        """
        CREATE TRIGGER venue_geocodes_touch_updated_at
        BEFORE UPDATE ON venue_geocodes
        FOR EACH ROW EXECUTE FUNCTION touch_updated_at()
        """,
        """
        DROP TRIGGER IF EXISTS location_contexts_touch_updated_at
            ON location_contexts
        """,
        """
        CREATE TRIGGER location_contexts_touch_updated_at
        BEFORE UPDATE ON location_contexts
        FOR EACH ROW EXECUTE FUNCTION touch_updated_at()
        """,
        """
        DROP TRIGGER IF EXISTS location_attempts_touch_updated_at
            ON location_attempts
        """,
        """
        CREATE TRIGGER location_attempts_touch_updated_at
        BEFORE UPDATE ON location_attempts
        FOR EACH ROW EXECUTE FUNCTION touch_updated_at()
        """,
        """
        DROP TRIGGER IF EXISTS location_reviews_touch_updated_at
            ON location_reviews
        """,
        """
        CREATE TRIGGER location_reviews_touch_updated_at
        BEFORE UPDATE ON location_reviews
        FOR EACH ROW EXECUTE FUNCTION touch_updated_at()
        """,
        """
        DROP TRIGGER IF EXISTS results_touch_updated_at ON official_results
        """,
        """
        CREATE TRIGGER results_touch_updated_at
        BEFORE UPDATE ON official_results
        FOR EACH ROW EXECUTE FUNCTION touch_updated_at()
        """,
        """
        CREATE OR REPLACE FUNCTION prevent_frozen_prediction_mutation()
        RETURNS TRIGGER AS $$
        BEGIN
            IF OLD.frozen_at IS NOT NULL THEN
                RAISE EXCEPTION
                    'Frozen prediction records are immutable; insert a new version.';
            END IF;
            IF TG_OP = 'DELETE' THEN
                RETURN OLD;
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql
        """,
        """
        DROP TRIGGER IF EXISTS prediction_runs_immutable
            ON prediction_runs
        """,
        """
        CREATE TRIGGER prediction_runs_immutable
        BEFORE UPDATE OR DELETE ON prediction_runs
        FOR EACH ROW EXECUTE FUNCTION prevent_frozen_prediction_mutation()
        """,
        """
        INSERT INTO app_metadata (key, value)
        VALUES ('schema_version', %s)
        ON CONFLICT (key) DO UPDATE
        SET value = EXCLUDED.value, updated_at = NOW()
        """,
    ]


def ensure_database_schema() -> dict[str, Any]:
    """
    Create or update the database schema idempotently.

    An advisory transaction lock prevents concurrent Render workers from
    attempting the same migration at the same time.
    """
    statements = database_schema_statements()
    try:
        with _database_connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT pg_advisory_xact_lock(%s)", (1200002,))
                for statement in statements[:-1]:
                    cursor.execute(statement)
                cursor.execute(
                    statements[-1],
                    (DATABASE_SCHEMA_VERSION,),
                )
            connection.commit()

        return {
            "status": "ok",
            "initialized": True,
            "schema_version": DATABASE_SCHEMA_VERSION,
            "expected_table_count": len(DATABASE_EXPECTED_TABLES),
        }
    except Exception as exc:
        return {
            "status": "error",
            "initialized": False,
            "schema_version": DATABASE_SCHEMA_VERSION,
            "error_type": type(exc).__name__,
            "message": "Database schema initialization failed.",
        }


def database_schema_status() -> dict[str, Any]:
    """
    Verify schema version, expected tables and current row counts.
    """
    if not DATABASE_URL or psycopg is None:
        return {
            "status": "unavailable",
            "ready": False,
            "schema_version": None,
            "missing_tables": list(DATABASE_EXPECTED_TABLES),
        }

    try:
        with psycopg.connect(
            DATABASE_URL,
            connect_timeout=DATABASE_CONNECT_TIMEOUT_SECONDS,
            autocommit=True,
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                      AND table_name = ANY(%s)
                    ORDER BY table_name
                    """,
                    (list(DATABASE_EXPECTED_TABLES),),
                )
                present = [row[0] for row in cursor.fetchall()]
                missing = sorted(set(DATABASE_EXPECTED_TABLES) - set(present))

                cursor.execute(
                    """
                    SELECT value
                    FROM app_metadata
                    WHERE key = 'schema_version'
                    """
                )
                version_row = cursor.fetchone()
                schema_version = version_row[0] if version_row else None

                row_counts: dict[str, int] = {}
                for table_name in DATABASE_EXPECTED_TABLES:
                    if table_name in present:
                        cursor.execute(
                            f'SELECT COUNT(*) FROM "{table_name}"'
                        )
                        count_row = cursor.fetchone()
                        row_counts[table_name] = (
                            int(count_row[0]) if count_row else 0
                        )

        return {
            "status": "ok" if not missing else "incomplete",
            "ready": (
                not missing
                and schema_version == DATABASE_SCHEMA_VERSION
            ),
            "schema_version": schema_version,
            "expected_schema_version": DATABASE_SCHEMA_VERSION,
            "present_tables": present,
            "missing_tables": missing,
            "row_counts": row_counts,
            "prediction_records_immutable": (
                "prediction_runs" in present
            ),
        }
    except Exception as exc:
        return {
            "status": "error",
            "ready": False,
            "schema_version": None,
            "error_type": type(exc).__name__,
            "message": "Database schema verification failed.",
        }


@app.on_event("startup")
def initialize_database_on_startup() -> None:
    """
    Create the DB2 schema automatically when Render starts this service.

    Failure is recorded but does not stop the astrology API from starting.
    """
    global DATABASE_SCHEMA_STARTUP_STATUS
    if not DATABASE_URL:
        DATABASE_SCHEMA_STARTUP_STATUS = {
            "status": "not_configured",
            "initialized": False,
            "schema_version": DATABASE_SCHEMA_VERSION,
        }
        return
    DATABASE_SCHEMA_STARTUP_STATUS = ensure_database_schema()


FIXTURE_SYNC_STARTUP_STATUS: dict[str, Any] = {
    "status": "not_started",
    "synced": False,
}

FIXTURE_TIMEZONE_MIGRATION_STATUS: dict[str, Any] = {
    "status": "not_started",
    "updated_rows": 0,
}


@app.on_event("startup")
def import_today_fixtures_on_startup() -> None:
    """
    Import today's fixtures once per configured interval after deployment.

    Failures do not stop the astrology service.
    """
    global FIXTURE_SYNC_STARTUP_STATUS
    global FIXTURE_TIMEZONE_MIGRATION_STATUS

    FIXTURE_TIMEZONE_MIGRATION_STATUS = (
        clear_unverified_fixture_timezones_once()
    )

    if not DATABASE_URL or not API_FOOTBALL_KEY:
        FIXTURE_SYNC_STARTUP_STATUS = {
            "status": "not_configured",
            "synced": False,
        }
        return
    FIXTURE_SYNC_STARTUP_STATUS = sync_today_fixtures(force=False)


# ============================================================
# API-FOOTBALL CONNECTIVITY — CHECKPOINT DB3
# ============================================================

_API_FOOTBALL_STATUS_CACHE: dict[str, Any] = {
    "checked_at_monotonic": 0.0,
    "result": None,
}
_API_FOOTBALL_STATUS_LOCK = threading.Lock()


def _safe_api_football_status_payload(payload: Any) -> dict[str, Any]:
    """
    Return only non-secret account-health fields.

    The upstream /status response can include account identity details.
    Those fields are intentionally excluded.
    """
    if not isinstance(payload, dict):
        return {
            "status": "error",
            "connected": False,
            "message": "API-Football returned an unexpected response.",
        }

    errors = payload.get("errors")
    if errors:
        return {
            "status": "error",
            "connected": False,
            "message": "API-Football rejected the request.",
            "provider_errors": errors,
        }

    response = payload.get("response")
    if not isinstance(response, dict):
        return {
            "status": "error",
            "connected": False,
            "message": "API-Football status data was missing.",
        }

    subscription = response.get("subscription")
    requests_data = response.get("requests")

    if not isinstance(subscription, dict):
        subscription = {}
    if not isinstance(requests_data, dict):
        requests_data = {}

    return {
        "status": "ok",
        "connected": True,
        "proxy_version": PROXY_VERSION,
        "provider": "API-Football",
        "endpoint_tested": "/status",
        "subscription": {
            "plan": subscription.get("plan"),
            "active": subscription.get("active"),
            "end": subscription.get("end"),
        },
        "requests": {
            "current": requests_data.get("current"),
            "limit_day": requests_data.get("limit_day"),
        },
    }


def api_football_connection_status(
    *,
    force_refresh: bool = False,
) -> dict[str, Any]:
    """
    Verify the API-Football key with GET /status.

    Results are cached to protect a low daily request allowance. The API key
    and account identity are never returned.
    """
    if not API_FOOTBALL_KEY:
        return {
            "status": "not_configured",
            "connected": False,
            "proxy_version": PROXY_VERSION,
            "message": "API_FOOTBALL_KEY is not configured.",
        }

    now = time.monotonic()

    with _API_FOOTBALL_STATUS_LOCK:
        cached_result = _API_FOOTBALL_STATUS_CACHE.get("result")
        checked_at = float(
            _API_FOOTBALL_STATUS_CACHE.get("checked_at_monotonic") or 0.0
        )

        cache_is_fresh = (
            cached_result is not None
            and now - checked_at < API_FOOTBALL_HEALTH_CACHE_SECONDS
        )

        if cache_is_fresh and not force_refresh:
            result = dict(cached_result)
            result["cached"] = True
            result["cache_seconds"] = API_FOOTBALL_HEALTH_CACHE_SECONDS
            return result

        try:
            upstream = requests.get(
                f"{API_FOOTBALL_BASE_URL}/status",
                headers={
                    "x-apisports-key": API_FOOTBALL_KEY,
                    "Accept": "application/json",
                },
                timeout=API_FOOTBALL_TIMEOUT_SECONDS,
            )
        except requests.RequestException as exc:
            result = {
                "status": "error",
                "connected": False,
                "proxy_version": PROXY_VERSION,
                "error_type": type(exc).__name__,
                "message": "Could not reach API-Football.",
            }
        else:
            if upstream.status_code != 200:
                result = {
                    "status": "error",
                    "connected": False,
                    "proxy_version": PROXY_VERSION,
                    "http_status": upstream.status_code,
                    "message": "API-Football returned a non-200 response.",
                }
            else:
                try:
                    payload = upstream.json()
                except ValueError:
                    result = {
                        "status": "error",
                        "connected": False,
                        "proxy_version": PROXY_VERSION,
                        "message": "API-Football returned invalid JSON.",
                    }
                else:
                    result = _safe_api_football_status_payload(payload)

        result["cached"] = False
        result["cache_seconds"] = API_FOOTBALL_HEALTH_CACHE_SECONDS

        _API_FOOTBALL_STATUS_CACHE["checked_at_monotonic"] = now
        _API_FOOTBALL_STATUS_CACHE["result"] = dict(result)
        return result


# ============================================================
# FIXTURE IMPORT — CHECKPOINT DB4
# ============================================================

FIXTURE_STATUS_MAP = {
    "NS": "scheduled",
    "TBD": "scheduled",
    "1H": "live",
    "HT": "live",
    "2H": "live",
    "ET": "live",
    "BT": "live",
    "P": "live",
    "SUSP": "suspended",
    "INT": "interrupted",
    "FT": "completed",
    "AET": "completed",
    "PEN": "completed",
    "PST": "postponed",
    "CANC": "cancelled",
    "ABD": "abandoned",
    "AWD": "awarded",
    "WO": "walkover",
}


def _display_timezone() -> ZoneInfo:
    try:
        return ZoneInfo(SOCCER_DISPLAY_TIMEZONE)
    except ZoneInfoNotFoundError as exc:
        raise RuntimeError(
            f"Invalid SOCCER_DISPLAY_TIMEZONE: {SOCCER_DISPLAY_TIMEZONE}"
        ) from exc


def _local_date_now() -> date:
    return datetime.now(_display_timezone()).date()


def _parse_provider_datetime(value: Any) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("Fixture date is missing.")
    normalized = value.strip().replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        raise ValueError("Fixture date has no UTC offset.")
    return parsed


def _fixture_window_utc(window: str) -> tuple[datetime, datetime]:
    """
    Return a half-open UTC range for the website window.

    Windows are based on SOCCER_DISPLAY_TIMEZONE:
    today, tomorrow, 7d and 30d.
    """
    tz = _display_timezone()
    today_local = datetime.now(tz).date()

    if window == "today":
        start_date = today_local
        days = 1
    elif window == "tomorrow":
        start_date = today_local + timedelta(days=1)
        days = 1
    elif window == "7d":
        start_date = today_local
        days = 7
    elif window == "30d":
        start_date = today_local
        days = 30
    else:
        raise ValueError(
            "window must be one of: today, tomorrow, 7d, 30d"
        )

    local_start = datetime.combine(
        start_date,
        datetime.min.time(),
        tzinfo=tz,
    )
    local_end = local_start + timedelta(days=days)
    return (
        local_start.astimezone(timezone.utc),
        local_end.astimezone(timezone.utc),
    )


def _fixture_sync_metadata() -> dict[str, Any]:
    """
    Read the persisted sync metadata without exposing secrets.
    """
    if not DATABASE_URL or psycopg is None:
        return {
            "last_sync_at": None,
            "last_sync_date": None,
            "last_received": 0,
            "last_stored": 0,
        }

    keys = (
        "fixtures_last_sync_at",
        "fixtures_last_sync_date",
        "fixtures_last_received",
        "fixtures_last_stored",
    )
    output = {
        "last_sync_at": None,
        "last_sync_date": None,
        "last_received": 0,
        "last_stored": 0,
    }

    try:
        with psycopg.connect(
            DATABASE_URL,
            connect_timeout=DATABASE_CONNECT_TIMEOUT_SECONDS,
            autocommit=True,
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT key, value
                    FROM app_metadata
                    WHERE key = ANY(%s)
                    """,
                    (list(keys),),
                )
                values = {row[0]: row[1] for row in cursor.fetchall()}
    except Exception:
        return output

    output["last_sync_at"] = values.get("fixtures_last_sync_at")
    output["last_sync_date"] = values.get("fixtures_last_sync_date")
    try:
        output["last_received"] = int(
            values.get("fixtures_last_received") or 0
        )
    except (TypeError, ValueError):
        pass
    try:
        output["last_stored"] = int(
            values.get("fixtures_last_stored") or 0
        )
    except (TypeError, ValueError):
        pass
    return output


def _fixture_sync_is_fresh(metadata: dict[str, Any]) -> bool:
    value = metadata.get("last_sync_at")
    if not isinstance(value, str) or not value:
        return False
    try:
        last_sync = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    if last_sync.tzinfo is None:
        return False
    age = datetime.now(timezone.utc) - last_sync.astimezone(timezone.utc)
    same_local_date = (
        metadata.get("last_sync_date") == _local_date_now().isoformat()
    )
    return (
        same_local_date
        and age.total_seconds() < FIXTURE_SYNC_MIN_INTERVAL_SECONDS
    )


def _normalise_api_fixture(item: Any) -> dict[str, Any] | None:
    if not isinstance(item, dict):
        return None

    fixture = item.get("fixture")
    league = item.get("league")
    teams = item.get("teams")

    if not isinstance(fixture, dict):
        return None
    if not isinstance(league, dict):
        league = {}
    if not isinstance(teams, dict):
        teams = {}

    home = teams.get("home")
    away = teams.get("away")
    if not isinstance(home, dict):
        home = {}
    if not isinstance(away, dict):
        away = {}

    fixture_id = fixture.get("id")
    home_name = home.get("name")
    away_name = away.get("name")
    competition_name = league.get("name")
    kickoff_value = fixture.get("date")

    if (
        fixture_id is None
        or not home_name
        or not away_name
        or not competition_name
        or not kickoff_value
    ):
        return None

    kickoff = _parse_provider_datetime(kickoff_value)
    venue = fixture.get("venue")
    if not isinstance(venue, dict):
        venue = {}
    status = fixture.get("status")
    if not isinstance(status, dict):
        status = {}

    provider_status = str(status.get("short") or "UNKNOWN").upper()
    canonical_status = FIXTURE_STATUS_MAP.get(
        provider_status,
        provider_status.lower(),
    )

    return {
        "provider": "api-football",
        "provider_fixture_id": str(fixture_id),
        "competition_name": str(competition_name),
        "competition_country": (
            str(league.get("country"))
            if league.get("country") is not None
            else None
        ),
        "season": (
            str(league.get("season"))
            if league.get("season") is not None
            else None
        ),
        "home_team": str(home_name),
        "away_team": str(away_name),
        "kickoff_utc": kickoff.astimezone(timezone.utc),
        "venue_name": (
            str(venue.get("name"))
            if venue.get("name") is not None
            else None
        ),
        "venue_city": (
            str(venue.get("city"))
            if venue.get("city") is not None
            else None
        ),
        # API-Football returns the timezone requested by the caller here.
        # It is a display/conversion timezone, not verified venue-local time.
        # Keep venue timezone empty until coordinate/timezone enrichment.
        "timezone_name": None,
        "fixture_status": canonical_status,
        "provider_status": provider_status,
        "raw_fixture_json": item,
    }


def _api_football_get(
    endpoint: str,
    *,
    params: dict[str, Any],
) -> dict[str, Any]:
    if not API_FOOTBALL_KEY:
        raise RuntimeError("API_FOOTBALL_KEY is not configured.")

    response = requests.get(
        f"{API_FOOTBALL_BASE_URL}{endpoint}",
        headers={
            "x-apisports-key": API_FOOTBALL_KEY,
            "Accept": "application/json",
        },
        params=params,
        timeout=API_FOOTBALL_TIMEOUT_SECONDS,
    )

    if response.status_code != 200:
        raise RuntimeError(
            f"API-Football returned HTTP {response.status_code}."
        )

    payload = response.json()
    if not isinstance(payload, dict):
        raise RuntimeError("API-Football returned unexpected JSON.")

    errors = payload.get("errors")
    if errors:
        raise RuntimeError("API-Football rejected the fixture request.")

    return payload


def sync_today_fixtures(
    *,
    force: bool = False,
) -> dict[str, Any]:
    """
    Import today's API-Football fixtures and upsert them into PostgreSQL.

    The date is interpreted in SOCCER_DISPLAY_TIMEZONE. The provider call is
    skipped when a successful sync exists within the configured interval.
    """
    if not DATABASE_URL or psycopg is None:
        return {
            "status": "error",
            "synced": False,
            "message": "Database is unavailable.",
        }

    metadata = _fixture_sync_metadata()
    if not force and _fixture_sync_is_fresh(metadata):
        return {
            "status": "ok",
            "synced": False,
            "skipped": True,
            "reason": "A fresh fixture sync already exists.",
            "display_timezone": SOCCER_DISPLAY_TIMEZONE,
            **metadata,
        }

    local_date = _local_date_now()
    try:
        payload = _api_football_get(
            "/fixtures",
            params={
                "date": local_date.isoformat(),
                "timezone": SOCCER_DISPLAY_TIMEZONE,
            },
        )
    except Exception as exc:
        return {
            "status": "error",
            "synced": False,
            "error_type": type(exc).__name__,
            "message": "Today's fixture import failed.",
        }

    response_rows = payload.get("response")
    if not isinstance(response_rows, list):
        response_rows = []

    normalised: list[dict[str, Any]] = []
    invalid_count = 0
    for item in response_rows:
        try:
            row = _normalise_api_fixture(item)
        except Exception:
            row = None
        if row is None:
            invalid_count += 1
        else:
            normalised.append(row)

    stored = 0
    sync_at = datetime.now(timezone.utc)

    try:
        with _database_connect() as connection:
            with connection.cursor() as cursor:
                for row in normalised:
                    cursor.execute(
                        """
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
                            venue_city,
                            timezone_name,
                            fixture_status,
                            neutral_venue,
                            raw_fixture_json
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
                            %(venue_city)s,
                            %(timezone_name)s,
                            %(fixture_status)s,
                            FALSE,
                            %(raw_fixture_json)s::jsonb
                        )
                        ON CONFLICT (provider, provider_fixture_id)
                        DO UPDATE SET
                            competition_name = EXCLUDED.competition_name,
                            competition_country = EXCLUDED.competition_country,
                            season = EXCLUDED.season,
                            home_team = EXCLUDED.home_team,
                            away_team = EXCLUDED.away_team,
                            kickoff_utc = EXCLUDED.kickoff_utc,
                            venue_name = EXCLUDED.venue_name,
                            venue_city = EXCLUDED.venue_city,
                            timezone_name = EXCLUDED.timezone_name,
                            fixture_status = EXCLUDED.fixture_status,
                            raw_fixture_json = EXCLUDED.raw_fixture_json
                        """,
                        {
                            **row,
                            "raw_fixture_json": json.dumps(
                                row["raw_fixture_json"],
                                ensure_ascii=False,
                                default=str,
                            ),
                        },
                    )
                    stored += 1

                metadata_values = {
                    "fixtures_last_sync_at": sync_at.isoformat(),
                    "fixtures_last_sync_date": local_date.isoformat(),
                    "fixtures_last_received": str(len(response_rows)),
                    "fixtures_last_stored": str(stored),
                    "fixtures_last_invalid": str(invalid_count),
                    "fixtures_display_timezone": SOCCER_DISPLAY_TIMEZONE,
                }
                for key, value in metadata_values.items():
                    cursor.execute(
                        """
                        INSERT INTO app_metadata (key, value)
                        VALUES (%s, %s)
                        ON CONFLICT (key) DO UPDATE
                        SET value = EXCLUDED.value, updated_at = NOW()
                        """,
                        (key, value),
                    )
            connection.commit()
    except Exception as exc:
        return {
            "status": "error",
            "synced": False,
            "error_type": type(exc).__name__,
            "message": "Fixtures were fetched but could not be stored.",
        }

    return {
        "status": "ok",
        "synced": True,
        "skipped": False,
        "provider": "API-Football",
        "provider_endpoint": "/fixtures",
        "local_date": local_date.isoformat(),
        "display_timezone": SOCCER_DISPLAY_TIMEZONE,
        "received": len(response_rows),
        "stored": stored,
        "invalid_skipped": invalid_count,
        "provider_results_reported": payload.get("results"),
        "synced_at": sync_at.isoformat(),
    }


def clear_unverified_fixture_timezones_once() -> dict[str, Any]:
    """
    Clear DB4 values that represented the website display timezone rather
    than verified venue-local timezone.

    The migration is idempotent and recorded in app_metadata.
    """
    migration_key = "migration_db4a_unverified_fixture_timezone_cleared"

    if not DATABASE_URL or psycopg is None:
        return {
            "status": "not_configured",
            "updated_rows": 0,
        }

    try:
        with _database_connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT value FROM app_metadata WHERE key = %s",
                    (migration_key,),
                )
                existing = cursor.fetchone()
                if existing and existing[0] == "true":
                    return {
                        "status": "ok",
                        "already_applied": True,
                        "updated_rows": 0,
                    }

                cursor.execute(
                    """
                    UPDATE fixtures
                    SET timezone_name = NULL,
                        updated_at = NOW()
                    WHERE provider = 'api-football'
                      AND timezone_name = %s
                    """,
                    (SOCCER_DISPLAY_TIMEZONE,),
                )
                updated_rows = cursor.rowcount

                cursor.execute(
                    """
                    INSERT INTO app_metadata (key, value)
                    VALUES (%s, 'true')
                    ON CONFLICT (key) DO UPDATE
                    SET value = EXCLUDED.value, updated_at = NOW()
                    """,
                    (migration_key,),
                )
            connection.commit()

        return {
            "status": "ok",
            "already_applied": False,
            "updated_rows": updated_rows,
        }
    except Exception as exc:
        return {
            "status": "error",
            "error_type": type(exc).__name__,
            "updated_rows": 0,
        }


def _utc_offset_text(value: datetime) -> str | None:
    offset = value.utcoffset()
    if offset is None:
        return None

    total_minutes = int(offset.total_seconds() // 60)
    sign = "+" if total_minutes >= 0 else "-"
    absolute_minutes = abs(total_minutes)
    hours, minutes = divmod(absolute_minutes, 60)
    return f"{sign}{hours:02d}:{minutes:02d}"


def _fixture_time_views(
    *,
    kickoff_utc: Any,
    venue_timezone_name: Any,
) -> dict[str, Any]:
    """
    Return separate display-local and venue-local time representations.

    `kickoff_local` is kept as a backward-compatible alias for the display
    timezone. Astrology must use `kickoff_venue_local` or `vedastro_std_time`.
    """
    if not isinstance(kickoff_utc, datetime):
        return {
            "kickoff_utc": (
                str(kickoff_utc) if kickoff_utc is not None else None
            ),
            "kickoff_local": None,
            "kickoff_local_semantics": "display_timezone",
            "kickoff_display_local": None,
            "display_timezone": SOCCER_DISPLAY_TIMEZONE,
            "kickoff_venue_local": None,
            "venue_utc_offset": None,
            "vedastro_std_time": None,
            "venue_timezone_conversion_valid": False,
        }

    aware_utc = kickoff_utc
    if aware_utc.tzinfo is None:
        aware_utc = aware_utc.replace(tzinfo=timezone.utc)
    else:
        aware_utc = aware_utc.astimezone(timezone.utc)

    display_local = aware_utc.astimezone(_display_timezone())

    venue_local = None
    venue_offset = None
    vedastro_std_time = None
    venue_conversion_valid = False

    if isinstance(venue_timezone_name, str) and venue_timezone_name.strip():
        try:
            venue_zone = ZoneInfo(venue_timezone_name.strip())
        except ZoneInfoNotFoundError:
            venue_zone = None

        if venue_zone is not None:
            venue_local = aware_utc.astimezone(venue_zone)
            venue_offset = _utc_offset_text(venue_local)
            if venue_offset is not None:
                vedastro_std_time = (
                    f"{venue_local:%H:%M %d/%m/%Y} {venue_offset}"
                )
                venue_conversion_valid = True

    return {
        "kickoff_utc": aware_utc.isoformat(),
        # Backward compatibility: this remains the user's display timezone.
        "kickoff_local": display_local.isoformat(),
        "kickoff_local_semantics": "display_timezone",
        "kickoff_display_local": display_local.isoformat(),
        "display_timezone": SOCCER_DISPLAY_TIMEZONE,
        "kickoff_venue_local": (
            venue_local.isoformat() if venue_local is not None else None
        ),
        "venue_utc_offset": venue_offset,
        "vedastro_std_time": vedastro_std_time,
        "venue_timezone_conversion_valid": venue_conversion_valid,
    }


def list_stored_fixtures(
    *,
    window: str,
    limit: int,
    include_completed: bool = False,
) -> dict[str, Any]:
    start_utc, end_utc = _fixture_window_utc(window)
    safe_limit = max(1, min(int(limit), FIXTURE_LIST_MAX_LIMIT))

    try:
        with psycopg.connect(
            DATABASE_URL,
            connect_timeout=DATABASE_CONNECT_TIMEOUT_SECONDS,
            autocommit=True,
        ) as connection:
            with connection.cursor() as cursor:
                now_utc = datetime.now(timezone.utc)
                effective_start = (
                    start_utc
                    if include_completed
                    else max(start_utc, now_utc)
                )

                cursor.execute(
                    """
                    SELECT
                        id,
                        provider_fixture_id,
                        competition_name,
                        competition_country,
                        season,
                        home_team,
                        away_team,
                        kickoff_utc,
                        venue_name,
                        venue_city,
                        timezone_name,
                        fixture_status,
                        latitude,
                        longitude,
                        location_source,
                        location_confidence,
                        location_verified_at
                    FROM fixtures
                    WHERE sport = 'soccer'
                      AND kickoff_utc >= %s
                      AND kickoff_utc < %s
                      AND (
                          %s
                          OR fixture_status = 'scheduled'
                      )
                    ORDER BY kickoff_utc, competition_name, home_team
                    LIMIT %s
                    """,
                    (
                        effective_start,
                        end_utc,
                        include_completed,
                        safe_limit,
                    ),
                )
                rows = cursor.fetchall()
    except Exception as exc:
        return {
            "status": "error",
            "ready": False,
            "error_type": type(exc).__name__,
            "message": "Stored fixtures could not be read.",
        }

    fixtures = []
    for row in rows:
        time_views = _fixture_time_views(
            kickoff_utc=row[7],
            venue_timezone_name=row[10],
        )
        venue_time_valid = bool(
            time_views["venue_timezone_conversion_valid"]
        )
        location_time_blockers = [
            blocker
            for blocker, missing in (
                ("venue_name_missing", row[8] is None),
                (
                    "venue_timezone_unverified",
                    row[10] is None or not venue_time_valid,
                ),
                ("latitude_missing", row[12] is None),
                ("longitude_missing", row[13] is None),
                ("location_source_missing", row[14] is None),
                ("location_verification_missing", row[16] is None),
                (
                    "venue_local_time_unavailable",
                    not venue_time_valid,
                ),
            )
            if missing
        ]
        location_time_ready = not location_time_blockers
        fixtures.append({
            "database_fixture_id": row[0],
            "provider_fixture_id": row[1],
            "competition": row[2],
            "country": row[3],
            "season": row[4],
            "home_team": row[5],
            "away_team": row[6],
            **time_views,
            "venue_name": row[8],
            "venue_city": row[9],
            "venue_timezone": row[10],
            "venue_timezone_verified": (
                row[10] is not None and venue_time_valid
            ),
            "fixture_status": row[11],
            "latitude": row[12],
            "longitude": row[13],
            "location_source": row[14],
            "location_confidence": (
                float(row[15]) if row[15] is not None else None
            ),
            "location_verified_at": (
                row[16].isoformat()
                if isinstance(row[16], datetime)
                else None
            ),
            "venue_coordinates_available": (
                row[12] is not None and row[13] is not None
            ),
            "location_time_ready": location_time_ready,
            "location_time_blockers": location_time_blockers,
            "market_status_endpoint": f"/fixtures/{row[0]}/market-status",
            "prediction_ready": False,
            "prediction_blockers": (
                list(location_time_blockers)
                + [
                    "market_consensus_not_frozen",
                    "performance_evidence_not_verified",
                    "lineups_not_verified",
                    "injuries_not_verified",
                ]
            ),
            "astrology_action_allowed": False,
        })

    return {
        "status": "ok",
        "ready": True,
        "window": window,
        "include_completed": include_completed,
        "display_timezone": SOCCER_DISPLAY_TIMEZONE,
        "time_semantics": {
            "kickoff_local": "display_timezone",
            "kickoff_display_local": SOCCER_DISPLAY_TIMEZONE,
            "kickoff_venue_local": "verified venue timezone",
            "vedastro_std_time": "HH:MM DD/MM/YYYY +HH:MM",
        },
        "range_utc": {
            "start": start_utc.isoformat(),
            "end_exclusive": end_utc.isoformat(),
        },
        "count": len(fixtures),
        "limit": safe_limit,
        "fixtures": fixtures,
    }


def get_stored_fixture_by_id(
    fixture_id: int,
) -> dict[str, Any]:
    """
    Return one stored fixture without list-window truncation.
    """
    if not DATABASE_URL or psycopg is None:
        return {
            "status": "error",
            "ready": False,
            "message": "Database is unavailable.",
        }

    try:
        with psycopg.connect(
            DATABASE_URL,
            connect_timeout=DATABASE_CONNECT_TIMEOUT_SECONDS,
            autocommit=True,
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        id,
                        provider_fixture_id,
                        competition_name,
                        competition_country,
                        season,
                        home_team,
                        away_team,
                        kickoff_utc,
                        venue_name,
                        venue_city,
                        timezone_name,
                        fixture_status,
                        latitude,
                        longitude,
                        location_source,
                        location_confidence,
                        location_verified_at
                    FROM fixtures
                    WHERE id = %s
                      AND sport = 'soccer'
                    LIMIT 1
                    """,
                    (fixture_id,),
                )
                row = cursor.fetchone()
    except Exception as exc:
        return {
            "status": "error",
            "ready": False,
            "error_type": type(exc).__name__,
            "message": "Stored fixture could not be read.",
        }

    if row is None:
        return {
            "status": "not_found",
            "ready": True,
            "database_fixture_id": fixture_id,
            "message": "No stored soccer fixture has this database ID.",
        }

    time_views = _fixture_time_views(
        kickoff_utc=row[7],
        venue_timezone_name=row[10],
    )
    venue_time_valid = bool(
        time_views["venue_timezone_conversion_valid"]
    )

    location_time_blockers = [
        blocker
        for blocker, missing in (
            ("venue_name_missing", row[8] is None),
            (
                "venue_timezone_unverified",
                row[10] is None or not venue_time_valid,
            ),
            ("latitude_missing", row[12] is None),
            ("longitude_missing", row[13] is None),
            ("location_source_missing", row[14] is None),
            ("location_verification_missing", row[16] is None),
            ("venue_local_time_unavailable", not venue_time_valid),
        )
        if missing
    ]
    location_time_ready = not location_time_blockers
    market_status = latest_market_status(int(row[0]))
    market_ready = bool(market_status.get("market_ready"))

    prediction_blockers = list(location_time_blockers)
    if not market_ready:
        prediction_blockers.append("market_consensus_unavailable")
    prediction_blockers.extend(
        [
            "market_consensus_not_frozen",
            "performance_evidence_not_verified",
            "lineups_not_verified",
            "injuries_not_verified",
        ]
    )

    fixture = {
        "database_fixture_id": row[0],
        "provider_fixture_id": row[1],
        "competition": row[2],
        "country": row[3],
        "season": row[4],
        "home_team": row[5],
        "away_team": row[6],
        **time_views,
        "venue_name": row[8],
        "venue_city": row[9],
        "venue_timezone": row[10],
        "venue_timezone_verified": (
            row[10] is not None and venue_time_valid
        ),
        "fixture_status": row[11],
        "latitude": row[12],
        "longitude": row[13],
        "location_source": row[14],
        "location_confidence": (
            float(row[15]) if row[15] is not None else None
        ),
        "location_verified_at": (
            row[16].isoformat()
            if isinstance(row[16], datetime)
            else None
        ),
        "venue_coordinates_available": (
            row[12] is not None and row[13] is not None
        ),
        "location_time_ready": location_time_ready,
        "location_time_blockers": location_time_blockers,
        "market_ready": market_ready,
        "market_status": market_status,
        "prediction_ready": False,
        "prediction_blockers": prediction_blockers,
        "astrology_action_allowed": False,
    }

    return {
        "status": "ok",
        "ready": True,
        "time_semantics": {
            "kickoff_local": "display_timezone",
            "kickoff_display_local": SOCCER_DISPLAY_TIMEZONE,
            "kickoff_venue_local": "verified venue timezone",
            "vedastro_std_time": "HH:MM DD/MM/YYYY +HH:MM",
        },
        "fixture": fixture,
    }


# ============================================================
# LOCATIONIQ + TIMEZONE CONNECTIVITY — CHECKPOINT DB5
# ============================================================

_LOCATIONIQ_HEALTH_CACHE: dict[str, Any] = {
    "checked_at_monotonic": 0.0,
    "result": None,
}
_LOCATIONIQ_HEALTH_LOCK = threading.Lock()


def _safe_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _safe_locationiq_candidate(candidate: Any) -> dict[str, Any] | None:
    if not isinstance(candidate, dict):
        return None

    latitude = _safe_float(candidate.get("lat"))
    longitude = _safe_float(candidate.get("lon"))

    if latitude is None or longitude is None:
        return None
    if not (-90.0 <= latitude <= 90.0):
        return None
    if not (-180.0 <= longitude <= 180.0):
        return None

    derived_timezone = None
    if timezone_at is not None:
        try:
            derived_timezone = timezone_at(
                lng=longitude,
                lat=latitude,
            )
        except Exception:
            derived_timezone = None

    return {
        "display_name": candidate.get("display_name"),
        "latitude": latitude,
        "longitude": longitude,
        "place_type": candidate.get("type"),
        "category": candidate.get("class"),
        "importance": _safe_float(candidate.get("importance")),
        "derived_timezone": derived_timezone,
        "timezone_lookup_successful": bool(derived_timezone),
    }


def locationiq_connection_status(
    *,
    force_refresh: bool = False,
) -> dict[str, Any]:
    """
    Verify LocationIQ with one known forward-geocoding request.

    The access token is never returned. The result is cached for 24 hours by
    default to avoid unnecessary use of the provider allowance.
    """
    if not LOCATIONIQ_KEY:
        return {
            "status": "not_configured",
            "connected": False,
            "proxy_version": PROXY_VERSION,
            "message": "LOCATIONIQ_KEY is not configured.",
        }

    if timezone_at is None:
        return {
            "status": "timezone_driver_missing",
            "connected": False,
            "proxy_version": PROXY_VERSION,
            "message": (
                "timezonefinder is unavailable. Add "
                "timezonefinder>=8.2,<9 to requirements.txt."
            ),
        }

    now = time.monotonic()

    with _LOCATIONIQ_HEALTH_LOCK:
        cached_result = _LOCATIONIQ_HEALTH_CACHE.get("result")
        checked_at = float(
            _LOCATIONIQ_HEALTH_CACHE.get("checked_at_monotonic") or 0.0
        )
        cache_is_fresh = (
            cached_result is not None
            and now - checked_at < LOCATIONIQ_HEALTH_CACHE_SECONDS
        )

        if cache_is_fresh and not force_refresh:
            result = dict(cached_result)
            result["cached"] = True
            result["cache_seconds"] = LOCATIONIQ_HEALTH_CACHE_SECONDS
            return result

        try:
            response = requests.get(
                f"{LOCATIONIQ_BASE_URL}/search",
                params={
                    "key": LOCATIONIQ_KEY,
                    "q": LOCATIONIQ_HEALTH_TEST_QUERY,
                    "format": "json",
                    "addressdetails": 1,
                    "limit": 1,
                },
                headers={
                    "Accept": "application/json",
                    "User-Agent": (
                        f"VedAstro-GPT-Proxy/{PROXY_VERSION}"
                    ),
                },
                timeout=LOCATIONIQ_TIMEOUT_SECONDS,
            )
        except requests.RequestException as exc:
            result = {
                "status": "error",
                "connected": False,
                "proxy_version": PROXY_VERSION,
                "error_type": type(exc).__name__,
                "message": "Could not reach LocationIQ.",
            }
        else:
            if response.status_code != 200:
                result = {
                    "status": "error",
                    "connected": False,
                    "proxy_version": PROXY_VERSION,
                    "http_status": response.status_code,
                    "message": "LocationIQ returned a non-200 response.",
                }
            else:
                try:
                    payload = response.json()
                except ValueError:
                    result = {
                        "status": "error",
                        "connected": False,
                        "proxy_version": PROXY_VERSION,
                        "message": "LocationIQ returned invalid JSON.",
                    }
                else:
                    candidate = (
                        payload[0]
                        if isinstance(payload, list) and payload
                        else None
                    )
                    safe_candidate = _safe_locationiq_candidate(candidate)

                    if safe_candidate is None:
                        result = {
                            "status": "error",
                            "connected": False,
                            "proxy_version": PROXY_VERSION,
                            "message": (
                                "LocationIQ returned no usable coordinates."
                            ),
                        }
                    elif not safe_candidate[
                        "timezone_lookup_successful"
                    ]:
                        result = {
                            "status": "error",
                            "connected": False,
                            "proxy_version": PROXY_VERSION,
                            "message": (
                                "Coordinates were returned, but the offline "
                                "timezone lookup failed."
                            ),
                            "test_result": safe_candidate,
                        }
                    else:
                        result = {
                            "status": "ok",
                            "connected": True,
                            "proxy_version": PROXY_VERSION,
                            "provider": "LocationIQ",
                            "endpoint_tested": "/search",
                            "test_query": LOCATIONIQ_HEALTH_TEST_QUERY,
                            "test_result": safe_candidate,
                            "attribution_required_for_public_website": True,
                            "attribution_text": (
                                "Search by LocationIQ.com"
                            ),
                        }

        result["cached"] = False
        result["cache_seconds"] = LOCATIONIQ_HEALTH_CACHE_SECONDS
        _LOCATIONIQ_HEALTH_CACHE["checked_at_monotonic"] = now
        _LOCATIONIQ_HEALTH_CACHE["result"] = dict(result)
        return result


# ============================================================
# ONE-FIXTURE LOCATION PREVIEW — CHECKPOINT DB6
# ============================================================

COUNTRY_NAME_ALIASES = {
    "czech republic": {"czech republic", "czechia", "cz"},
    "usa": {"usa", "united states", "united states of america", "us"},
    "uk": {"uk", "united kingdom", "great britain", "gb"},
    "south korea": {"south korea", "republic of korea", "kr"},
    "north korea": {"north korea", "democratic peoples republic of korea", "kp"},
    "russia": {"russia", "russian federation", "ru"},
    "iran": {"iran", "islamic republic of iran", "ir"},
    "bolivia": {"bolivia", "plurinational state of bolivia", "bo"},
    "venezuela": {"venezuela", "bolivarian republic of venezuela", "ve"},
}


def _normalise_lookup_text(value: Any) -> str:
    if value is None:
        return ""
    text_value = unicodedata.normalize("NFKD", str(value))
    text_value = "".join(
        character
        for character in text_value
        if not unicodedata.combining(character)
    )
    text_value = re.sub(r"[^a-z0-9]+", " ", text_value.lower())
    return " ".join(text_value.split())


def _country_aliases(value: Any) -> set[str]:
    normalised = _normalise_lookup_text(value)
    if not normalised:
        return set()
    for canonical, aliases in COUNTRY_NAME_ALIASES.items():
        if normalised == canonical or normalised in aliases:
            return {_normalise_lookup_text(item) for item in aliases | {canonical}}
    return {normalised}


def _significant_tokens(value: Any) -> set[str]:
    stopwords = {
        "stadium", "stade", "arena", "park", "field", "ground",
        "sports", "sport", "centre", "center", "complex", "oval",
        "the", "of", "fc", "club", "football", "artificial", "grass",
    }
    return {
        token
        for token in _normalise_lookup_text(value).split()
        if len(token) >= 3 and token not in stopwords
    }


def _locationiq_address(candidate: dict[str, Any]) -> dict[str, Any]:
    address = candidate.get("address")
    return address if isinstance(address, dict) else {}


def _candidate_city(address: dict[str, Any]) -> str | None:
    for key in (
        "city", "town", "village", "municipality", "county",
        "state_district", "suburb",
    ):
        value = address.get(key)
        if value:
            return str(value)
    return None


def _evaluate_location_candidate(
    *,
    candidate: Any,
    venue_name: str,
    expected_city: str | None,
    expected_country: str | None,
    city_context: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    safe = _safe_locationiq_candidate(candidate)
    if safe is None:
        return None

    candidate_dict = candidate if isinstance(candidate, dict) else {}
    address = _locationiq_address(candidate_dict)

    candidate_country = (
        str(address.get("country"))
        if address.get("country") is not None
        else None
    )
    candidate_country_code = (
        str(address.get("country_code")).lower()
        if address.get("country_code") is not None
        else None
    )
    candidate_city = _candidate_city(address)

    expected_country_aliases = _country_aliases(expected_country)
    candidate_country_aliases = (
        _country_aliases(candidate_country)
        | _country_aliases(candidate_country_code)
    )
    country_match = bool(
        expected_country_aliases
        and candidate_country_aliases
        and expected_country_aliases.intersection(candidate_country_aliases)
    )

    expected_city_norm = _normalise_lookup_text(expected_city)
    candidate_city_norm = _normalise_lookup_text(candidate_city)
    display_norm = _normalise_lookup_text(safe.get("display_name"))
    city_match = bool(
        expected_city_norm
        and (
            expected_city_norm == candidate_city_norm
            or expected_city_norm in display_norm
            or candidate_city_norm in expected_city_norm
        )
    )

    venue_tokens = _significant_tokens(venue_name)
    display_tokens = _significant_tokens(safe.get("display_name"))
    if venue_tokens:
        venue_overlap = len(
            venue_tokens.intersection(display_tokens)
        ) / len(venue_tokens)
    else:
        venue_overlap = 0.0

    place_type = _normalise_lookup_text(safe.get("place_type"))
    category = _normalise_lookup_text(safe.get("category"))
    sports_place_match = (
        place_type in {
            "stadium", "sports centre", "sports_center", "pitch",
            "recreation ground", "recreation_ground",
        }
        or category in {"leisure", "sport"}
        or "stadium" in display_norm
        or "arena" in display_norm
        or "sports centre" in display_norm
        or "sports center" in display_norm
    )

    distance_from_city_center_km = None
    if city_context is not None:
        city_latitude = _safe_float(city_context.get("latitude"))
        city_longitude = _safe_float(city_context.get("longitude"))
        if city_latitude is not None and city_longitude is not None:
            phi1 = math.radians(city_latitude)
            phi2 = math.radians(float(safe["latitude"]))
            delta_phi = math.radians(
                float(safe["latitude"]) - city_latitude
            )
            delta_lambda = math.radians(
                float(safe["longitude"]) - city_longitude
            )
            haversine = (
                math.sin(delta_phi / 2.0) ** 2
                + math.cos(phi1)
                * math.cos(phi2)
                * math.sin(delta_lambda / 2.0) ** 2
            )
            distance_from_city_center_km = (
                6371.0088
                * 2.0
                * math.atan2(
                    math.sqrt(haversine),
                    math.sqrt(max(0.0, 1.0 - haversine)),
                )
            )

    score = 0.0
    if expected_country:
        score += 30.0 if country_match else 0.0
    else:
        score += 15.0

    if expected_city:
        score += 20.0 if city_match else 0.0
    else:
        score += 10.0

    score += min(30.0, venue_overlap * 30.0)
    if sports_place_match:
        score += 15.0

    if distance_from_city_center_km is not None:
        if distance_from_city_center_km <= 25.0:
            score += 5.0
        elif distance_from_city_center_km <= LOCATION_MAX_CITY_DISTANCE_KM:
            score += 2.0

    approval_blockers: list[str] = []

    if expected_country and not country_match:
        approval_blockers.append("country_mismatch")
        score = min(score, 39.0)

    if expected_city and not city_match:
        approval_blockers.append("city_mismatch")

    if not sports_place_match:
        approval_blockers.append("not_a_verified_sports_place")
        # A hotel, restaurant, road or generic landmark must never be
        # auto-approved merely because it is in the right city/country.
        score = min(score, 59.0)

    if venue_overlap < 0.50:
        approval_blockers.append("venue_name_overlap_below_0_50")

    if safe.get("timezone_lookup_successful") is not True:
        approval_blockers.append("timezone_lookup_failed")

    if (
        distance_from_city_center_km is not None
        and distance_from_city_center_km > LOCATION_MAX_CITY_DISTANCE_KM
    ):
        approval_blockers.append("outside_city_distance_limit")

    auto_approved = (
        score >= LOCATION_PREVIEW_AUTO_APPROVE_SCORE
        and (not expected_country or country_match)
        and (not expected_city or city_match)
        and sports_place_match
        and venue_overlap >= 0.50
        and safe.get("timezone_lookup_successful") is True
        and (
            distance_from_city_center_km is None
            or distance_from_city_center_km
            <= LOCATION_MAX_CITY_DISTANCE_KM
        )
    )

    rejected = (
        (expected_country and not country_match)
        or (
            not sports_place_match
            and venue_overlap < 0.75
        )
    )

    if auto_approved:
        decision_status = "AUTO_APPROVED"
    elif rejected:
        decision_status = "REJECTED"
    else:
        decision_status = "PREVIEW"

    return {
        **safe,
        "provider_place_id": candidate_dict.get("place_id"),
        "candidate_country": candidate_country,
        "candidate_country_code": candidate_country_code,
        "candidate_city": candidate_city,
        "venue_token_overlap": round(venue_overlap, 3),
        "country_match": country_match,
        "city_match": city_match,
        "sports_place_match": sports_place_match,
        "distance_from_city_center_km": (
            round(distance_from_city_center_km, 3)
            if distance_from_city_center_km is not None
            else None
        ),
        "confidence_score": round(score, 3),
        "approval_blockers": approval_blockers,
        "decision_status": decision_status,
    }


def _fixture_location_query(fixture: dict[str, Any]) -> str:
    parts = [
        fixture.get("venue_name"),
        fixture.get("venue_city"),
        fixture.get("competition_country"),
    ]
    return ", ".join(str(part).strip() for part in parts if part)


SPORTS_VENUE_HINTS = {
    "stadium", "stadion", "stadio", "stade", "estadio", "arena",
    "oval", "ground", "park", "sport", "sports", "football",
    "futbol", "fútbol", "soccer", "pitch", "campo", "cancha",
    "centre", "center", "complex", "kompleks",
}


def _fixture_row_to_location_dict(row: Any) -> dict[str, Any]:
    return {
        "database_fixture_id": int(row[0]),
        "provider_fixture_id": row[1],
        "competition": row[2],
        "competition_country": row[3],
        "home_team": row[4],
        "away_team": row[5],
        "kickoff_utc": (
            row[6].isoformat()
            if isinstance(row[6], datetime)
            else str(row[6])
        ),
        "venue_name": row[7],
        "venue_city": row[8],
        "latitude": row[9],
        "longitude": row[10],
        "venue_timezone": row[11],
        "fixture_status": row[12],
    }


def _location_query_hash(query_text: str) -> str:
    payload = (
        f"{LOCATION_GEOCODE_STRATEGY_VERSION}|"
        f"{_normalise_lookup_text(query_text)}"
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _location_context_hash(
    city_name: str,
    country_name: str,
) -> str:
    payload = (
        f"city_context_v1|{_normalise_lookup_text(city_name)}|"
        f"{_normalise_lookup_text(country_name)}"
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _fixture_location_priority(
    fixture: dict[str, Any],
) -> tuple[int, int, str]:
    """
    Prefer explicit sports-venue names, then richer names, then kickoff order.

    This affects only which venue is previewed next. It never approves a
    candidate and never writes coordinates.
    """
    venue_name = _normalise_lookup_text(fixture.get("venue_name"))
    venue_tokens = set(venue_name.split())
    sports_hint_count = len(venue_tokens.intersection(SPORTS_VENUE_HINTS))
    significant_count = len(_significant_tokens(venue_name))
    kickoff = str(fixture.get("kickoff_utc") or "")
    return (-sports_hint_count, -significant_count, kickoff)


def _load_fixture_for_location_preview(
    fixture_id: int | None = None,
) -> dict[str, Any] | None:
    """
    Load a fixture for geocoding preview.

    Explicit fixture_id:
      Return that fixture, even if its query was already cached.

    Automatic selection:
      Scan upcoming candidates, skip every query already present in
      venue_geocodes (PREVIEW, AUTO_APPROVED, REJECTED or MANUALLY_APPROVED),
      and return the highest-priority unseen venue.
    """
    if not DATABASE_URL or psycopg is None:
        return None

    with psycopg.connect(
        DATABASE_URL,
        connect_timeout=DATABASE_CONNECT_TIMEOUT_SECONDS,
        autocommit=True,
    ) as connection:
        with connection.cursor() as cursor:
            if fixture_id is not None:
                cursor.execute(
                    """
                    SELECT
                        id, provider_fixture_id, competition_name,
                        competition_country, home_team, away_team,
                        kickoff_utc, venue_name, venue_city,
                        latitude, longitude, timezone_name, fixture_status
                    FROM fixtures
                    WHERE id = %s
                    """,
                    (fixture_id,),
                )
                row = cursor.fetchone()
                return (
                    _fixture_row_to_location_dict(row)
                    if row
                    else None
                )

            cursor.execute(
                """
                SELECT
                    id, provider_fixture_id, competition_name,
                    competition_country, home_team, away_team,
                    kickoff_utc, venue_name, venue_city,
                    latitude, longitude, timezone_name, fixture_status
                FROM fixtures
                WHERE sport = 'soccer'
                  AND fixture_status = 'scheduled'
                  AND kickoff_utc >= NOW()
                  AND kickoff_utc < NOW() + (%s * INTERVAL '1 day')
                  AND venue_name IS NOT NULL
                  AND venue_city IS NOT NULL
                  AND competition_country IS NOT NULL
                  AND latitude IS NULL
                  AND longitude IS NULL
                ORDER BY kickoff_utc, id
                LIMIT %s
                """,
                (
                    LOCATION_PREVIEW_LOOKAHEAD_DAYS,
                    LOCATION_PREVIEW_QUEUE_SCAN_LIMIT,
                ),
            )
            rows = cursor.fetchall()

            fixtures = [
                _fixture_row_to_location_dict(row)
                for row in rows
            ]
            if not fixtures:
                return None

            query_hashes = {
                _location_query_hash(_fixture_location_query(fixture))
                for fixture in fixtures
            }

            cursor.execute(
                """
                SELECT query_hash
                FROM venue_geocodes
                WHERE query_hash = ANY(%s)
                UNION
                SELECT query_hash
                FROM location_attempts
                WHERE query_hash = ANY(%s)
                  AND strategy_version = %s
                """,
                (
                    list(query_hashes),
                    list(query_hashes),
                    LOCATION_GEOCODE_STRATEGY_VERSION,
                ),
            )
            cached_hashes = {str(row[0]) for row in cursor.fetchall()}

    unseen = [
        fixture
        for fixture in fixtures
        if _location_query_hash(
            _fixture_location_query(fixture)
        ) not in cached_hashes
    ]

    if not unseen:
        return None

    unseen.sort(key=_fixture_location_priority)
    selected = dict(unseen[0])
    selected["automatic_queue_selection"] = True
    selected["cached_queries_skipped"] = len(fixtures) - len(unseen)
    selected["queue_candidates_scanned"] = len(fixtures)
    return selected


def downgrade_unsafe_geocode_approvals_once() -> dict[str, Any]:
    """
    Downgrade DB6 false-positive auto-approvals.

    Any cached candidate that is not a sports place and has weak venue-name
    overlap is marked REJECTED. Strong-name but non-sports candidates are
    reduced to PREVIEW for manual review.
    """
    migration_key = "migration_db6a_geocode_safety_applied"

    if not DATABASE_URL or psycopg is None:
        return {
            "status": "not_configured",
            "rejected_rows": 0,
            "preview_rows": 0,
        }

    try:
        with _database_connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT value FROM app_metadata WHERE key = %s",
                    (migration_key,),
                )
                existing = cursor.fetchone()
                if existing and existing[0] == "true":
                    return {
                        "status": "ok",
                        "already_applied": True,
                        "rejected_rows": 0,
                        "preview_rows": 0,
                    }

                cursor.execute(
                    """
                    UPDATE venue_geocodes
                    SET decision_status = 'REJECTED',
                        confidence_score = LEAST(confidence_score, 59),
                        updated_at = NOW()
                    WHERE decision_status = 'AUTO_APPROVED'
                      AND sports_place_match = FALSE
                      AND venue_token_overlap < 0.75
                    """
                )
                rejected_rows = cursor.rowcount

                cursor.execute(
                    """
                    UPDATE venue_geocodes
                    SET decision_status = 'PREVIEW',
                        confidence_score = LEAST(confidence_score, 74),
                        updated_at = NOW()
                    WHERE decision_status = 'AUTO_APPROVED'
                      AND sports_place_match = FALSE
                      AND venue_token_overlap >= 0.75
                    """
                )
                preview_rows = cursor.rowcount

                cursor.execute(
                    """
                    INSERT INTO app_metadata (key, value)
                    VALUES (%s, 'true')
                    ON CONFLICT (key) DO UPDATE
                    SET value = EXCLUDED.value, updated_at = NOW()
                    """,
                    (migration_key,),
                )
            connection.commit()

        return {
            "status": "ok",
            "already_applied": False,
            "rejected_rows": rejected_rows,
            "preview_rows": preview_rows,
        }
    except Exception as exc:
        return {
            "status": "error",
            "error_type": type(exc).__name__,
            "rejected_rows": 0,
            "preview_rows": 0,
        }


GEOCODE_SAFETY_MIGRATION_STATUS: dict[str, Any] = {
    "status": "not_started",
    "rejected_rows": 0,
    "preview_rows": 0,
}


def _cached_geocode_preview(query_hash: str) -> dict[str, Any] | None:
    with psycopg.connect(
        DATABASE_URL,
        connect_timeout=DATABASE_CONNECT_TIMEOUT_SECONDS,
        autocommit=True,
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    provider, query_text, venue_name, expected_city,
                    expected_country, provider_place_id, display_name,
                    latitude, longitude, timezone_name,
                    candidate_country, candidate_country_code,
                    candidate_city, venue_token_overlap,
                    confidence_score, country_match, city_match,
                    sports_place_match, decision_status, created_at
                FROM venue_geocodes
                WHERE query_hash = %s
                """,
                (query_hash,),
            )
            row = cursor.fetchone()

    if not row:
        return None

    approval_blockers = []
    if not bool(row[15]):
        approval_blockers.append("country_mismatch")
    if not bool(row[16]):
        approval_blockers.append("city_mismatch")
    if not bool(row[17]):
        approval_blockers.append("not_a_verified_sports_place")
    if float(row[13]) < 0.50:
        approval_blockers.append("venue_name_overlap_below_0_50")
    if row[9] is None:
        approval_blockers.append("timezone_lookup_failed")

    return {
        "provider": row[0],
        "query_text": row[1],
        "venue_name": row[2],
        "expected_city": row[3],
        "expected_country": row[4],
        "provider_place_id": row[5],
        "display_name": row[6],
        "latitude": float(row[7]),
        "longitude": float(row[8]),
        "timezone_name": row[9],
        "candidate_country": row[10],
        "candidate_country_code": row[11],
        "candidate_city": row[12],
        "venue_token_overlap": float(row[13]),
        "confidence_score": float(row[14]),
        "country_match": bool(row[15]),
        "city_match": bool(row[16]),
        "sports_place_match": bool(row[17]),
        "approval_blockers": approval_blockers,
        "decision_status": row[18],
        "cached_at": (
            row[19].isoformat()
            if isinstance(row[19], datetime)
            else str(row[19])
        ),
        "cached": True,
    }


def _city_context_viewbox(
    context: dict[str, Any],
) -> str:
    latitude = float(context["latitude"])
    longitude = float(context["longitude"])
    lat_delta = LOCATION_CITY_VIEWBOX_LAT_DELTA
    lon_scale = max(abs(math.cos(math.radians(latitude))), 0.25)
    lon_delta = min(1.8, lat_delta / lon_scale)

    min_lon = max(-180.0, longitude - lon_delta)
    max_lon = min(180.0, longitude + lon_delta)
    min_lat = max(-90.0, latitude - lat_delta)
    max_lat = min(90.0, latitude + lat_delta)

    return f"{min_lon},{min_lat},{max_lon},{max_lat}"


def _cached_location_context(
    context_hash: str,
) -> dict[str, Any] | None:
    with psycopg.connect(
        DATABASE_URL,
        connect_timeout=DATABASE_CONNECT_TIMEOUT_SECONDS,
        autocommit=True,
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    provider, city_name, country_name, provider_place_id,
                    display_name, latitude, longitude, country_code,
                    candidate_country, candidate_city,
                    country_match, city_match, created_at
                FROM location_contexts
                WHERE context_hash = %s
                """,
                (context_hash,),
            )
            row = cursor.fetchone()

    if not row:
        return None

    return {
        "provider": row[0],
        "city_name": row[1],
        "country_name": row[2],
        "provider_place_id": row[3],
        "display_name": row[4],
        "latitude": float(row[5]),
        "longitude": float(row[6]),
        "country_code": row[7],
        "candidate_country": row[8],
        "candidate_city": row[9],
        "country_match": bool(row[10]),
        "city_match": bool(row[11]),
        "cached_at": (
            row[12].isoformat()
            if isinstance(row[12], datetime)
            else str(row[12])
        ),
        "cached": True,
    }


def _evaluate_city_context_candidate(
    *,
    candidate: Any,
    expected_city: str,
    expected_country: str,
) -> dict[str, Any] | None:
    safe = _safe_locationiq_candidate(candidate)
    if safe is None:
        return None

    candidate_dict = candidate if isinstance(candidate, dict) else {}
    address = _locationiq_address(candidate_dict)
    candidate_country = (
        str(address.get("country"))
        if address.get("country") is not None
        else None
    )
    candidate_country_code = (
        str(address.get("country_code")).lower()
        if address.get("country_code") is not None
        else None
    )
    candidate_city = _candidate_city(address)

    expected_country_aliases = _country_aliases(expected_country)
    candidate_country_aliases = (
        _country_aliases(candidate_country)
        | _country_aliases(candidate_country_code)
    )
    country_match = bool(
        expected_country_aliases.intersection(candidate_country_aliases)
    )

    expected_city_norm = _normalise_lookup_text(expected_city)
    candidate_city_norm = _normalise_lookup_text(candidate_city)
    display_norm = _normalise_lookup_text(safe.get("display_name"))
    city_match = bool(
        expected_city_norm
        and (
            expected_city_norm == candidate_city_norm
            or expected_city_norm in display_norm
        )
    )

    place_type = _normalise_lookup_text(safe.get("place_type"))
    category = _normalise_lookup_text(safe.get("category"))
    city_place_match = (
        place_type in {
            "city", "town", "municipality", "administrative",
            "state district", "county",
        }
        or category in {"place", "boundary"}
        or expected_city_norm in display_norm
    )

    score = 0.0
    if country_match:
        score += 45.0
    if city_match:
        score += 45.0
    if city_place_match:
        score += 10.0

    return {
        **safe,
        "provider_place_id": candidate_dict.get("place_id"),
        "candidate_country": candidate_country,
        "country_code": candidate_country_code,
        "candidate_city": candidate_city,
        "country_match": country_match,
        "city_match": city_match,
        "city_place_match": city_place_match,
        "context_score": round(score, 3),
    }


def _get_or_create_city_context(
    *,
    city_name: str,
    country_name: str,
) -> dict[str, Any]:
    context_hash = _location_context_hash(city_name, country_name)
    cached = _cached_location_context(context_hash)
    if cached is not None:
        return {
            "status": "ok",
            "provider_call_made": False,
            "context": cached,
        }

    query_text = f"{city_name}, {country_name}"
    try:
        response = requests.get(
            f"{LOCATIONIQ_BASE_URL}/search",
            params={
                "key": LOCATIONIQ_KEY,
                "q": query_text,
                "format": "json",
                "addressdetails": 1,
                "normalizecity": 1,
                "dedupe": 1,
                "limit": 5,
            },
            headers={
                "Accept": "application/json",
                "User-Agent": f"VedAstro-GPT-Proxy/{PROXY_VERSION}",
            },
            timeout=LOCATIONIQ_TIMEOUT_SECONDS,
        )
    except requests.RequestException as exc:
        return {
            "status": "error",
            "provider_call_made": True,
            "error_type": type(exc).__name__,
            "message": "City context lookup could not reach LocationIQ.",
        }

    if response.status_code != 200:
        return {
            "status": "error",
            "provider_call_made": True,
            "http_status": response.status_code,
            "message": "City context lookup returned a non-200 response.",
        }

    try:
        payload = response.json()
    except ValueError:
        return {
            "status": "error",
            "provider_call_made": True,
            "message": "City context lookup returned invalid JSON.",
        }

    candidates = payload if isinstance(payload, list) else []
    evaluated = [
        item
        for item in (
            _evaluate_city_context_candidate(
                candidate=candidate,
                expected_city=city_name,
                expected_country=country_name,
            )
            for candidate in candidates
        )
        if item is not None
    ]

    valid = [
        item
        for item in evaluated
        if item["country_match"] and item["city_match"]
    ]
    if not valid:
        return {
            "status": "no_match",
            "provider_call_made": True,
            "message": "No verified city/country context was found.",
            "candidate_count": len(evaluated),
        }

    best = max(
        valid,
        key=lambda item: (
            float(item.get("context_score") or 0.0),
            float(item.get("importance") or 0.0),
        ),
    )

    with _database_connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO location_contexts (
                    context_hash, provider, city_name, country_name,
                    provider_place_id, display_name, latitude, longitude,
                    country_code, candidate_country, candidate_city,
                    country_match, city_match, raw_response_json
                )
                VALUES (
                    %s, 'locationiq', %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s::jsonb
                )
                ON CONFLICT (context_hash) DO UPDATE SET
                    provider_place_id = EXCLUDED.provider_place_id,
                    display_name = EXCLUDED.display_name,
                    latitude = EXCLUDED.latitude,
                    longitude = EXCLUDED.longitude,
                    country_code = EXCLUDED.country_code,
                    candidate_country = EXCLUDED.candidate_country,
                    candidate_city = EXCLUDED.candidate_city,
                    country_match = EXCLUDED.country_match,
                    city_match = EXCLUDED.city_match,
                    raw_response_json = EXCLUDED.raw_response_json
                """,
                (
                    context_hash,
                    city_name,
                    country_name,
                    (
                        str(best.get("provider_place_id"))
                        if best.get("provider_place_id") is not None
                        else None
                    ),
                    best.get("display_name"),
                    best["latitude"],
                    best["longitude"],
                    best.get("country_code"),
                    best.get("candidate_country"),
                    best.get("candidate_city"),
                    best.get("country_match"),
                    best.get("city_match"),
                    json.dumps(
                        {
                            "query": query_text,
                            "best_candidate": best,
                            "candidate_count": len(evaluated),
                        },
                        ensure_ascii=False,
                        default=str,
                    ),
                ),
            )
        connection.commit()

    return {
        "status": "ok",
        "provider_call_made": True,
        "context": {
            "provider": "locationiq",
            "city_name": city_name,
            "country_name": country_name,
            "provider_place_id": best.get("provider_place_id"),
            "display_name": best.get("display_name"),
            "latitude": best["latitude"],
            "longitude": best["longitude"],
            "country_code": best.get("country_code"),
            "candidate_country": best.get("candidate_country"),
            "candidate_city": best.get("candidate_city"),
            "country_match": best.get("country_match"),
            "city_match": best.get("city_match"),
            "cached": False,
        },
    }


def _record_location_attempt(
    *,
    query_hash: str,
    fixture_id: int | None,
    query_text: str,
    status: str,
    provider_call_count: int,
    message: str,
    raw_response: Any,
) -> None:
    try:
        with _database_connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO location_attempts (
                        query_hash, fixture_id, strategy_version,
                        query_text, attempt_status,
                        provider_call_count, message, raw_response_json
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                    ON CONFLICT (query_hash) DO UPDATE SET
                        fixture_id = EXCLUDED.fixture_id,
                        attempt_status = EXCLUDED.attempt_status,
                        provider_call_count = EXCLUDED.provider_call_count,
                        message = EXCLUDED.message,
                        raw_response_json = EXCLUDED.raw_response_json
                    """,
                    (
                        query_hash,
                        fixture_id,
                        LOCATION_GEOCODE_STRATEGY_VERSION,
                        query_text,
                        status,
                        provider_call_count,
                        message,
                        json.dumps(
                            raw_response,
                            ensure_ascii=False,
                            default=str,
                        ),
                    ),
                )
            connection.commit()
    except Exception:
        # A failed audit-cache write must not crash the astronomy service.
        return


def preview_fixture_location(
    *,
    fixture_id: int | None = None,
) -> dict[str, Any]:
    """
    Preview one venue using a verified city-bounded search.

    DB6C still never updates fixture latitude, longitude or timezone.
    """
    if not LOCATIONIQ_KEY:
        return {
            "status": "error",
            "previewed": False,
            "message": "LOCATIONIQ_KEY is not configured.",
        }
    if timezone_at is None:
        return {
            "status": "error",
            "previewed": False,
            "message": "timezonefinder is unavailable.",
        }
    if not DATABASE_URL or psycopg is None:
        return {
            "status": "error",
            "previewed": False,
            "message": "Database is unavailable.",
        }

    fixture = _load_fixture_for_location_preview(fixture_id)
    if fixture is None:
        return {
            "status": "no_candidate",
            "previewed": False,
            "message": (
                "No unseen suitable fixture with venue, city and country "
                "was found in the preview queue."
            ),
        }

    query_text = _fixture_location_query(fixture)
    query_hash = _location_query_hash(query_text)

    cached = _cached_geocode_preview(query_hash)
    if cached is not None:
        return {
            "status": "ok",
            "previewed": True,
            "provider_call_made": False,
            "provider_call_count": 0,
            "strategy_version": LOCATION_GEOCODE_STRATEGY_VERSION,
            "fixture": fixture,
            "preview": cached,
            "fixture_updated": False,
        }

    city_name = fixture.get("venue_city")
    country_name = fixture.get("competition_country")
    if not city_name or not country_name:
        _record_location_attempt(
            query_hash=query_hash,
            fixture_id=fixture.get("database_fixture_id"),
            query_text=query_text,
            status="MISSING_CITY_CONTEXT",
            provider_call_count=0,
            message="Fixture city or country is missing.",
            raw_response={},
        )
        return {
            "status": "no_match",
            "previewed": False,
            "fixture": fixture,
            "query_text": query_text,
            "message": "Fixture city or country is missing.",
        }

    context_result = _get_or_create_city_context(
        city_name=str(city_name),
        country_name=str(country_name),
    )
    provider_call_count = (
        1 if context_result.get("provider_call_made") else 0
    )

    if context_result.get("status") != "ok":
        _record_location_attempt(
            query_hash=query_hash,
            fixture_id=fixture.get("database_fixture_id"),
            query_text=query_text,
            status="CITY_CONTEXT_NOT_VERIFIED",
            provider_call_count=provider_call_count,
            message=str(context_result.get("message") or ""),
            raw_response=context_result,
        )
        return {
            "status": "no_match",
            "previewed": False,
            "strategy_version": LOCATION_GEOCODE_STRATEGY_VERSION,
            "fixture": fixture,
            "query_text": query_text,
            "provider_call_count": provider_call_count,
            "city_context_result": context_result,
            "message": "Venue search stopped because city context failed.",
        }

    city_context = context_result["context"]
    viewbox = _city_context_viewbox(city_context)

    params = {
        "key": LOCATIONIQ_KEY,
        "q": str(fixture["venue_name"]),
        "format": "json",
        "addressdetails": 1,
        "normalizecity": 1,
        "dedupe": 1,
        "limit": 10,
        "viewbox": viewbox,
        "bounded": 1,
    }
    country_code = city_context.get("country_code")
    if country_code:
        params["countrycodes"] = str(country_code).lower()

    try:
        response = requests.get(
            f"{LOCATIONIQ_BASE_URL}/search",
            params=params,
            headers={
                "Accept": "application/json",
                "User-Agent": f"VedAstro-GPT-Proxy/{PROXY_VERSION}",
            },
            timeout=LOCATIONIQ_TIMEOUT_SECONDS,
        )
        provider_call_count += 1
    except requests.RequestException as exc:
        return {
            "status": "error",
            "previewed": False,
            "error_type": type(exc).__name__,
            "provider_call_count": provider_call_count,
            "message": "Bounded venue search could not reach LocationIQ.",
        }

    if response.status_code == 404:
        # LocationIQ documents HTTP 404 as "no location or places were
        # found", not as an authentication or transport failure.
        _record_location_attempt(
            query_hash=query_hash,
            fixture_id=fixture.get("database_fixture_id"),
            query_text=query_text,
            status="NO_BOUNDED_RESULTS_404",
            provider_call_count=provider_call_count,
            message=(
                "LocationIQ found no venue inside the verified city bounds."
            ),
            raw_response={
                "strategy_version": LOCATION_GEOCODE_STRATEGY_VERSION,
                "city_context": city_context,
                "viewbox": viewbox,
                "bounded": True,
                "country_filter_applied": bool(country_code),
                "http_status": 404,
            },
        )
        return {
            "status": "no_match",
            "previewed": False,
            "http_status": 404,
            "provider_empty_result": True,
            "provider_call_count": provider_call_count,
            "strategy_version": LOCATION_GEOCODE_STRATEGY_VERSION,
            "fixture": fixture,
            "query_text": query_text,
            "city_context": city_context,
            "viewbox": viewbox,
            "bounded_search": True,
            "country_filter_applied": bool(country_code),
            "fixture_updated": False,
            "message": (
                "No venue was found inside the verified city bounds. "
                "The negative result was cached."
            ),
        }

    if response.status_code != 200:
        return {
            "status": "error",
            "previewed": False,
            "http_status": response.status_code,
            "provider_call_count": provider_call_count,
            "message": "Bounded venue search returned a non-200 response.",
        }

    try:
        payload = response.json()
    except ValueError:
        return {
            "status": "error",
            "previewed": False,
            "provider_call_count": provider_call_count,
            "message": "Bounded venue search returned invalid JSON.",
        }

    candidates = payload if isinstance(payload, list) else []
    evaluated = [
        item
        for item in (
            _evaluate_location_candidate(
                candidate=candidate,
                venue_name=str(fixture["venue_name"]),
                expected_city=str(city_name),
                expected_country=str(country_name),
                city_context=city_context,
            )
            for candidate in candidates
        )
        if item is not None
    ]

    if not evaluated:
        _record_location_attempt(
            query_hash=query_hash,
            fixture_id=fixture.get("database_fixture_id"),
            query_text=query_text,
            status="NO_BOUNDED_VENUE_MATCH",
            provider_call_count=provider_call_count,
            message="No usable venue candidate inside the city viewbox.",
            raw_response={
                "city_context": city_context,
                "viewbox": viewbox,
                "candidate_count": 0,
            },
        )
        return {
            "status": "no_match",
            "previewed": False,
            "strategy_version": LOCATION_GEOCODE_STRATEGY_VERSION,
            "fixture": fixture,
            "query_text": query_text,
            "provider_call_count": provider_call_count,
            "city_context": city_context,
            "viewbox": viewbox,
            "message": "No usable venue candidate inside the city viewbox.",
        }

    best = max(
        evaluated,
        key=lambda item: (
            float(item.get("confidence_score") or 0.0),
            -float(item.get("distance_from_city_center_km") or 9999.0),
            float(item.get("importance") or 0.0),
        ),
    )

    with _database_connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO venue_geocodes (
                    query_hash,
                    provider,
                    query_text,
                    venue_name,
                    expected_city,
                    expected_country,
                    provider_place_id,
                    display_name,
                    latitude,
                    longitude,
                    timezone_name,
                    candidate_country,
                    candidate_country_code,
                    candidate_city,
                    venue_token_overlap,
                    confidence_score,
                    country_match,
                    city_match,
                    sports_place_match,
                    decision_status,
                    raw_response_json
                )
                VALUES (
                    %s, 'locationiq', %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s::jsonb
                )
                ON CONFLICT (query_hash) DO UPDATE SET
                    provider_place_id = EXCLUDED.provider_place_id,
                    display_name = EXCLUDED.display_name,
                    latitude = EXCLUDED.latitude,
                    longitude = EXCLUDED.longitude,
                    timezone_name = EXCLUDED.timezone_name,
                    candidate_country = EXCLUDED.candidate_country,
                    candidate_country_code = EXCLUDED.candidate_country_code,
                    candidate_city = EXCLUDED.candidate_city,
                    venue_token_overlap = EXCLUDED.venue_token_overlap,
                    confidence_score = EXCLUDED.confidence_score,
                    country_match = EXCLUDED.country_match,
                    city_match = EXCLUDED.city_match,
                    sports_place_match = EXCLUDED.sports_place_match,
                    decision_status = EXCLUDED.decision_status,
                    raw_response_json = EXCLUDED.raw_response_json
                """,
                (
                    query_hash,
                    query_text,
                    fixture.get("venue_name"),
                    city_name,
                    country_name,
                    (
                        str(best.get("provider_place_id"))
                        if best.get("provider_place_id") is not None
                        else None
                    ),
                    best.get("display_name"),
                    best["latitude"],
                    best["longitude"],
                    best.get("derived_timezone"),
                    best.get("candidate_country"),
                    best.get("candidate_country_code"),
                    best.get("candidate_city"),
                    best.get("venue_token_overlap"),
                    best.get("confidence_score"),
                    best.get("country_match"),
                    best.get("city_match"),
                    best.get("sports_place_match"),
                    best.get("decision_status"),
                    json.dumps(
                        {
                            "strategy_version": (
                                LOCATION_GEOCODE_STRATEGY_VERSION
                            ),
                            "query": query_text,
                            "city_context": city_context,
                            "viewbox": viewbox,
                            "bounded": True,
                            "best_candidate": best,
                            "candidate_count": len(evaluated),
                        },
                        ensure_ascii=False,
                        default=str,
                    ),
                ),
            )
        connection.commit()

    safe_best = dict(best)

    return {
        "status": "ok",
        "previewed": True,
        "provider_call_made": provider_call_count > 0,
        "provider_call_count": provider_call_count,
        "strategy_version": LOCATION_GEOCODE_STRATEGY_VERSION,
        "fixture": fixture,
        "query_text": query_text,
        "city_context": city_context,
        "viewbox": viewbox,
        "bounded_search": True,
        "country_filter_applied": bool(country_code),
        "candidate_count": len(evaluated),
        "preview": {
            **safe_best,
            "cached": False,
        },
        "fixture_updated": False,
        "next_action": (
            "Review this city-bounded candidate. DB6C does not commit "
            "coordinates."
        ),
    }


def _location_preview_attempt_summary(
    result: dict[str, Any],
) -> dict[str, Any]:
    fixture = result.get("fixture")
    fixture_dict = fixture if isinstance(fixture, dict) else {}
    preview = result.get("preview")
    preview_dict = preview if isinstance(preview, dict) else {}

    return {
        "status": result.get("status"),
        "database_fixture_id": fixture_dict.get("database_fixture_id"),
        "fixture": (
            f"{fixture_dict.get('home_team')} vs "
            f"{fixture_dict.get('away_team')}"
            if fixture_dict
            else None
        ),
        "venue_name": fixture_dict.get("venue_name"),
        "venue_city": fixture_dict.get("venue_city"),
        "competition_country": fixture_dict.get(
            "competition_country"
        ),
        "provider_call_count": int(
            result.get("provider_call_count") or 0
        ),
        "http_status": result.get("http_status"),
        "provider_empty_result": bool(
            result.get("provider_empty_result")
        ),
        "decision_status": preview_dict.get("decision_status"),
        "matched_display_name": preview_dict.get("display_name"),
        "confidence_score": preview_dict.get("confidence_score"),
        "message": result.get("message"),
    }


def preview_fixture_location_batch() -> dict[str, Any]:
    """
    Advance through a small number of unseen venues at startup.

    Stop at the first AUTO_APPROVED or PREVIEW candidate that needs review.
    REJECTED and provider-empty results are cached and skipped automatically.
    Provider/authentication/rate-limit errors stop the batch immediately.
    """
    summaries: list[dict[str, Any]] = []
    selected_result: dict[str, Any] | None = None
    provider_call_count = 0
    terminal_error: dict[str, Any] | None = None

    for _ in range(max(1, LOCATION_PREVIEW_STARTUP_MAX_FIXTURES)):
        if (
            provider_call_count
            >= LOCATION_PREVIEW_STARTUP_MAX_PROVIDER_CALLS
        ):
            break

        result = preview_fixture_location()
        call_count = int(result.get("provider_call_count") or 0)
        provider_call_count += call_count
        summaries.append(_location_preview_attempt_summary(result))

        status = result.get("status")
        preview = result.get("preview")
        preview_dict = preview if isinstance(preview, dict) else {}
        decision_status = preview_dict.get("decision_status")

        if status == "ok" and decision_status in {
            "AUTO_APPROVED",
            "PREVIEW",
        }:
            selected_result = result
            break

        if status == "no_candidate":
            break

        if status == "error":
            terminal_error = result
            break

        # REJECTED and no_match results are already cached, so the next
        # loop iteration selects a different unseen venue.

    if selected_result is not None:
        return {
            "status": "ok",
            "previewed": True,
            "batch_mode": True,
            "fixtures_attempted": len(summaries),
            "provider_call_count": provider_call_count,
            "maximum_fixtures": (
                LOCATION_PREVIEW_STARTUP_MAX_FIXTURES
            ),
            "maximum_provider_calls": (
                LOCATION_PREVIEW_STARTUP_MAX_PROVIDER_CALLS
            ),
            "attempts": summaries,
            "selected_result": selected_result,
            "fixture_coordinates_committed": False,
        }

    if terminal_error is not None:
        return {
            "status": "error",
            "previewed": False,
            "batch_mode": True,
            "fixtures_attempted": len(summaries),
            "provider_call_count": provider_call_count,
            "attempts": summaries,
            "terminal_error": {
                "http_status": terminal_error.get("http_status"),
                "message": terminal_error.get("message"),
            },
            "fixture_coordinates_committed": False,
        }

    return {
        "status": "no_review_candidate",
        "previewed": False,
        "batch_mode": True,
        "fixtures_attempted": len(summaries),
        "provider_call_count": provider_call_count,
        "maximum_fixtures": LOCATION_PREVIEW_STARTUP_MAX_FIXTURES,
        "maximum_provider_calls": (
            LOCATION_PREVIEW_STARTUP_MAX_PROVIDER_CALLS
        ),
        "attempts": summaries,
        "message": (
            "No reviewable venue candidate was found in this startup batch."
        ),
        "fixture_coordinates_committed": False,
    }


LOCATION_PREVIEW_STARTUP_STATUS: dict[str, Any] = {
    "status": "not_started",
    "previewed": False,
}


@app.on_event("startup")
def preview_one_fixture_location_on_startup() -> None:
    """
    Preview one future fixture exactly once per unique venue query.

    The geocoding result is cached in PostgreSQL. Fixture coordinates remain
    untouched.
    """
    global LOCATION_PREVIEW_STARTUP_STATUS

    if not LOCATION_PREVIEW_AUTO_RUN:
        LOCATION_PREVIEW_STARTUP_STATUS = {
            "status": "disabled",
            "previewed": False,
        }
        return

    global GEOCODE_SAFETY_MIGRATION_STATUS

    GEOCODE_SAFETY_MIGRATION_STATUS = (
        downgrade_unsafe_geocode_approvals_once()
    )

    if not DATABASE_URL or not LOCATIONIQ_KEY or timezone_at is None:
        LOCATION_PREVIEW_STARTUP_STATUS = {
            "status": "not_configured",
            "previewed": False,
        }
        return

    LOCATION_PREVIEW_STARTUP_STATUS = preview_fixture_location_batch()


# ============================================================
# REVIEWED LOCATION COMMIT — CHECKPOINT DB7
# ============================================================

# Independent reference reviewed on 26 July 2026.
#
# LocationIQ candidate:
#   24.4740081, 118.0264486
# OpenStreetMap-derived reference:
#   way 1251039637, 24.47387, 118.02609
#
# The two points are about 39 metres apart. This manifest is intentionally
# explicit and allows only this one reviewed venue to be committed in DB7.
REVIEWED_LOCATION_MANIFESTS: tuple[dict[str, Any], ...] = (
    {
        "review_key": "haicang-sports-centre-stadium-xiamen-cn-v1",
        "venue_name": "Haicang Sports Centre Stadium",
        "venue_city": "Xiamen",
        "venue_country": "China",
        "provider": "locationiq",
        "provider_place_id": "224538479",
        "provider_latitude": 24.4740081,
        "provider_longitude": 118.0264486,
        "provider_timezone": "Asia/Shanghai",
        "external_source_name": "OpenStreetMap-derived map reference",
        "external_source_reference": "OpenStreetMap way 1251039637",
        "external_latitude": 24.47387,
        "external_longitude": 118.02609,
        "maximum_separation_meters": 100.0,
        "minimum_confidence_score": 100.0,
        "minimum_venue_token_overlap": 1.0,
        "review_notes": (
            "Exact stadium name, Xiamen/China match, sports-place classification, "
            "city-bounded search and independent coordinate corroboration."
        ),
    },
)


def _haversine_distance_meters(
    latitude_a: float,
    longitude_a: float,
    latitude_b: float,
    longitude_b: float,
) -> float:
    phi_a = math.radians(latitude_a)
    phi_b = math.radians(latitude_b)
    delta_phi = math.radians(latitude_b - latitude_a)
    delta_lambda = math.radians(longitude_b - longitude_a)

    haversine = (
        math.sin(delta_phi / 2.0) ** 2
        + math.cos(phi_a)
        * math.cos(phi_b)
        * math.sin(delta_lambda / 2.0) ** 2
    )
    return (
        6371008.8
        * 2.0
        * math.atan2(
            math.sqrt(haversine),
            math.sqrt(max(0.0, 1.0 - haversine)),
        )
    )


def _normalised_sql_match(value: Any) -> str:
    return _normalise_lookup_text(value)


def commit_reviewed_location_manifest(
    manifest: dict[str, Any],
) -> dict[str, Any]:
    """
    Commit one independently reviewed geocode with strict guards.

    The operation is idempotent. It updates all exact matching fixture rows so
    future fixtures at the same venue can reuse the verified coordinates.
    """
    if not DATABASE_URL or psycopg is None:
        return {
            "status": "not_configured",
            "committed": False,
            "review_key": manifest.get("review_key"),
        }

    separation_meters = _haversine_distance_meters(
        float(manifest["provider_latitude"]),
        float(manifest["provider_longitude"]),
        float(manifest["external_latitude"]),
        float(manifest["external_longitude"]),
    )

    if separation_meters > float(manifest["maximum_separation_meters"]):
        return {
            "status": "guard_failed",
            "committed": False,
            "review_key": manifest["review_key"],
            "guard": "independent_coordinate_separation",
            "separation_meters": round(separation_meters, 3),
        }

    try:
        with _database_connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        id,
                        provider,
                        provider_place_id,
                        latitude,
                        longitude,
                        timezone_name,
                        confidence_score,
                        venue_token_overlap,
                        country_match,
                        city_match,
                        sports_place_match,
                        decision_status,
                        venue_name,
                        expected_city,
                        expected_country
                    FROM venue_geocodes
                    WHERE lower(trim(venue_name)) = lower(trim(%s))
                      AND lower(trim(expected_city)) = lower(trim(%s))
                      AND lower(trim(expected_country)) = lower(trim(%s))
                    ORDER BY updated_at DESC, id DESC
                    LIMIT 1
                    """,
                    (
                        manifest["venue_name"],
                        manifest["venue_city"],
                        manifest["venue_country"],
                    ),
                )
                row = cursor.fetchone()

                if not row:
                    connection.rollback()
                    return {
                        "status": "guard_failed",
                        "committed": False,
                        "review_key": manifest["review_key"],
                        "guard": "cached_geocode_missing",
                    }

                geocode = {
                    "id": int(row[0]),
                    "provider": row[1],
                    "provider_place_id": row[2],
                    "latitude": float(row[3]),
                    "longitude": float(row[4]),
                    "timezone_name": row[5],
                    "confidence_score": float(row[6]),
                    "venue_token_overlap": float(row[7]),
                    "country_match": bool(row[8]),
                    "city_match": bool(row[9]),
                    "sports_place_match": bool(row[10]),
                    "decision_status": row[11],
                    "venue_name": row[12],
                    "expected_city": row[13],
                    "expected_country": row[14],
                }

                provider_distance_meters = _haversine_distance_meters(
                    geocode["latitude"],
                    geocode["longitude"],
                    float(manifest["provider_latitude"]),
                    float(manifest["provider_longitude"]),
                )

                guards = {
                    "provider_match": (
                        geocode["provider"] == manifest["provider"]
                    ),
                    "provider_place_id_match": (
                        str(geocode["provider_place_id"])
                        == str(manifest["provider_place_id"])
                    ),
                    "provider_coordinate_match": (
                        provider_distance_meters <= 5.0
                    ),
                    "timezone_match": (
                        geocode["timezone_name"]
                        == manifest["provider_timezone"]
                    ),
                    "confidence_match": (
                        geocode["confidence_score"]
                        >= float(manifest["minimum_confidence_score"])
                    ),
                    "venue_overlap_match": (
                        geocode["venue_token_overlap"]
                        >= float(manifest["minimum_venue_token_overlap"])
                    ),
                    "country_match": geocode["country_match"],
                    "city_match": geocode["city_match"],
                    "sports_place_match": geocode["sports_place_match"],
                    "decision_status_match": (
                        geocode["decision_status"]
                        in {"AUTO_APPROVED", "MANUALLY_APPROVED"}
                    ),
                }

                failed_guards = [
                    name for name, passed in guards.items() if not passed
                ]
                if failed_guards:
                    connection.rollback()
                    return {
                        "status": "guard_failed",
                        "committed": False,
                        "review_key": manifest["review_key"],
                        "failed_guards": failed_guards,
                        "provider_distance_meters": round(
                            provider_distance_meters,
                            3,
                        ),
                    }

                cursor.execute(
                    """
                    INSERT INTO location_reviews (
                        review_key,
                        venue_name,
                        venue_city,
                        venue_country,
                        provider,
                        provider_place_id,
                        provider_latitude,
                        provider_longitude,
                        provider_timezone,
                        external_source_name,
                        external_source_reference,
                        external_latitude,
                        external_longitude,
                        separation_meters,
                        review_status,
                        fixtures_updated,
                        review_notes
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, 'APPROVED', 0, %s
                    )
                    ON CONFLICT (review_key) DO UPDATE SET
                        provider_place_id = EXCLUDED.provider_place_id,
                        provider_latitude = EXCLUDED.provider_latitude,
                        provider_longitude = EXCLUDED.provider_longitude,
                        provider_timezone = EXCLUDED.provider_timezone,
                        external_source_name = EXCLUDED.external_source_name,
                        external_source_reference = EXCLUDED.external_source_reference,
                        external_latitude = EXCLUDED.external_latitude,
                        external_longitude = EXCLUDED.external_longitude,
                        separation_meters = EXCLUDED.separation_meters,
                        review_notes = EXCLUDED.review_notes
                    """,
                    (
                        manifest["review_key"],
                        manifest["venue_name"],
                        manifest["venue_city"],
                        manifest["venue_country"],
                        manifest["provider"],
                        str(manifest["provider_place_id"]),
                        float(manifest["provider_latitude"]),
                        float(manifest["provider_longitude"]),
                        manifest["provider_timezone"],
                        manifest["external_source_name"],
                        manifest["external_source_reference"],
                        float(manifest["external_latitude"]),
                        float(manifest["external_longitude"]),
                        separation_meters,
                        manifest["review_notes"],
                    ),
                )

                cursor.execute(
                    """
                    UPDATE fixtures
                    SET
                        latitude = %s,
                        longitude = %s,
                        timezone_name = %s,
                        location_source = %s,
                        location_confidence = %s,
                        location_verified_at = NOW(),
                        updated_at = NOW()
                    WHERE lower(trim(venue_name)) = lower(trim(%s))
                      AND lower(trim(venue_city)) = lower(trim(%s))
                      AND lower(trim(competition_country)) = lower(trim(%s))
                      AND (
                          latitude IS NULL
                          OR longitude IS NULL
                          OR timezone_name IS NULL
                          OR location_verified_at IS NULL
                      )
                    """,
                    (
                        geocode["latitude"],
                        geocode["longitude"],
                        geocode["timezone_name"],
                        (
                            "reviewed:locationiq+"
                            "openstreetmap-way-1251039637"
                        ),
                        geocode["confidence_score"],
                        manifest["venue_name"],
                        manifest["venue_city"],
                        manifest["venue_country"],
                    ),
                )
                fixtures_updated = cursor.rowcount

                cursor.execute(
                    """
                    UPDATE venue_geocodes
                    SET decision_status = 'MANUALLY_APPROVED',
                        updated_at = NOW()
                    WHERE id = %s
                    """,
                    (geocode["id"],),
                )

                cursor.execute(
                    """
                    UPDATE location_reviews
                    SET review_status = 'COMMITTED',
                        fixtures_updated = %s,
                        committed_at = COALESCE(committed_at, NOW()),
                        updated_at = NOW()
                    WHERE review_key = %s
                    """,
                    (
                        fixtures_updated,
                        manifest["review_key"],
                    ),
                )
            connection.commit()

        return {
            "status": "ok",
            "committed": True,
            "review_key": manifest["review_key"],
            "venue_name": manifest["venue_name"],
            "venue_city": manifest["venue_city"],
            "venue_country": manifest["venue_country"],
            "latitude": geocode["latitude"],
            "longitude": geocode["longitude"],
            "timezone_name": geocode["timezone_name"],
            "confidence_score": geocode["confidence_score"],
            "independent_reference_separation_meters": round(
                separation_meters,
                3,
            ),
            "fixtures_updated": fixtures_updated,
            "location_source": (
                "reviewed:locationiq+openstreetmap-way-1251039637"
            ),
        }
    except Exception as exc:
        return {
            "status": "error",
            "committed": False,
            "review_key": manifest.get("review_key"),
            "error_type": type(exc).__name__,
            "message": "Reviewed location could not be committed.",
        }


def commit_reviewed_locations() -> dict[str, Any]:
    if not REVIEWED_LOCATION_COMMIT_ENABLED:
        return {
            "status": "disabled",
            "committed": False,
            "results": [],
        }

    results = [
        commit_reviewed_location_manifest(manifest)
        for manifest in REVIEWED_LOCATION_MANIFESTS
    ]
    committed_count = sum(
        1 for result in results if result.get("committed")
    )
    return {
        "status": (
            "ok"
            if all(
                result.get("status") in {"ok", "guard_failed"}
                for result in results
            )
            else "error"
        ),
        "committed": committed_count > 0,
        "manifest_count": len(REVIEWED_LOCATION_MANIFESTS),
        "committed_count": committed_count,
        "results": results,
    }


REVIEWED_LOCATION_COMMIT_STATUS: dict[str, Any] = {
    "status": "not_started",
    "committed": False,
    "results": [],
}


@app.on_event("startup")
def commit_reviewed_locations_on_startup() -> None:
    global REVIEWED_LOCATION_COMMIT_STATUS

    if not DATABASE_URL:
        REVIEWED_LOCATION_COMMIT_STATUS = {
            "status": "not_configured",
            "committed": False,
            "results": [],
        }
        return

    REVIEWED_LOCATION_COMMIT_STATUS = commit_reviewed_locations()


# ============================================================
# PRE-MATCH 1X2 MARKET CAPTURE — CHECKPOINT DB8
# ============================================================

MARKET_CAPTURE_STARTUP_STATUS: dict[str, Any] = {
    "status": "not_started",
    "captured": False,
}


def _market_metadata_prefix(fixture_id: int) -> str:
    return f"market_capture_{fixture_id}"


def _read_market_attempt_metadata(
    fixture_id: int,
) -> dict[str, Any]:
    prefix = _market_metadata_prefix(fixture_id)
    keys = [
        f"{prefix}_last_attempt_at",
        f"{prefix}_last_status",
        f"{prefix}_last_bookmaker_count",
    ]
    output = {
        "last_attempt_at": None,
        "last_status": None,
        "last_bookmaker_count": 0,
    }

    if not DATABASE_URL or psycopg is None:
        return output

    try:
        with psycopg.connect(
            DATABASE_URL,
            connect_timeout=DATABASE_CONNECT_TIMEOUT_SECONDS,
            autocommit=True,
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT key, value
                    FROM app_metadata
                    WHERE key = ANY(%s)
                    """,
                    (keys,),
                )
                values = {
                    str(row[0]): str(row[1])
                    for row in cursor.fetchall()
                }
    except Exception:
        return output

    output["last_attempt_at"] = values.get(
        f"{prefix}_last_attempt_at"
    )
    output["last_status"] = values.get(
        f"{prefix}_last_status"
    )
    try:
        output["last_bookmaker_count"] = int(
            values.get(f"{prefix}_last_bookmaker_count") or 0
        )
    except (TypeError, ValueError):
        pass
    return output


def _write_market_attempt_metadata(
    *,
    fixture_id: int,
    status: str,
    bookmaker_count: int,
) -> None:
    if not DATABASE_URL or psycopg is None:
        return

    prefix = _market_metadata_prefix(fixture_id)
    values = {
        f"{prefix}_last_attempt_at": (
            datetime.now(timezone.utc).isoformat()
        ),
        f"{prefix}_last_status": status,
        f"{prefix}_last_bookmaker_count": str(bookmaker_count),
    }

    try:
        with _database_connect() as connection:
            with connection.cursor() as cursor:
                for key, value in values.items():
                    cursor.execute(
                        """
                        INSERT INTO app_metadata (key, value)
                        VALUES (%s, %s)
                        ON CONFLICT (key) DO UPDATE SET
                            value = EXCLUDED.value,
                            updated_at = NOW()
                        """,
                        (key, value),
                    )
            connection.commit()
    except Exception:
        return


def _market_attempt_is_fresh(
    metadata: dict[str, Any],
) -> bool:
    value = metadata.get("last_attempt_at")
    if not isinstance(value, str) or not value:
        return False
    try:
        last_attempt = datetime.fromisoformat(
            value.replace("Z", "+00:00")
        )
    except ValueError:
        return False
    if last_attempt.tzinfo is None:
        return False

    age = (
        datetime.now(timezone.utc)
        - last_attempt.astimezone(timezone.utc)
    )
    return age.total_seconds() < MARKET_CAPTURE_MIN_INTERVAL_SECONDS


def _load_market_capture_fixture(
    fixture_id: int,
) -> dict[str, Any] | None:
    if not DATABASE_URL or psycopg is None:
        return None

    try:
        with psycopg.connect(
            DATABASE_URL,
            connect_timeout=DATABASE_CONNECT_TIMEOUT_SECONDS,
            autocommit=True,
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        id,
                        provider_fixture_id,
                        home_team,
                        away_team,
                        kickoff_utc,
                        fixture_status,
                        venue_name,
                        timezone_name,
                        latitude,
                        longitude,
                        location_source,
                        location_verified_at
                    FROM fixtures
                    WHERE id = %s
                      AND sport = 'soccer'
                    LIMIT 1
                    """,
                    (fixture_id,),
                )
                row = cursor.fetchone()
    except Exception:
        return None

    if not row:
        return None

    location_time_ready = all(
        value is not None
        for value in (
            row[6],
            row[7],
            row[8],
            row[9],
            row[10],
            row[11],
        )
    )

    return {
        "database_fixture_id": int(row[0]),
        "provider_fixture_id": str(row[1]) if row[1] else None,
        "home_team": row[2],
        "away_team": row[3],
        "kickoff_utc": (
            row[4].isoformat()
            if isinstance(row[4], datetime)
            else str(row[4])
        ),
        "fixture_status": row[5],
        "location_time_ready": location_time_ready,
    }


def _normalise_market_text(value: Any) -> str:
    return " ".join(
        re.sub(
            r"[^a-z0-9]+",
            " ",
            str(value or "").lower(),
        ).split()
    )


def _decimal_odd(value: Any) -> float | None:
    try:
        odd = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(odd) or odd <= 1.0:
        return None
    return odd


def _outcome_key(value: Any) -> str | None:
    label = _normalise_market_text(value)
    mapping = {
        "home": "home",
        "1": "home",
        "draw": "draw",
        "x": "draw",
        "away": "away",
        "2": "away",
    }
    return mapping.get(label)


def _extract_prematch_1x2_quotes(
    payload: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[str]]:
    response_rows = payload.get("response")
    if not isinstance(response_rows, list):
        return [], []

    quotes_by_bookmaker: dict[str, dict[str, Any]] = {}
    provider_updates: list[str] = []

    for response_item in response_rows:
        if not isinstance(response_item, dict):
            continue

        update_value = response_item.get("update")
        if update_value:
            provider_updates.append(str(update_value))

        bookmakers = response_item.get("bookmakers")
        if not isinstance(bookmakers, list):
            continue

        for bookmaker in bookmakers:
            if not isinstance(bookmaker, dict):
                continue

            bookmaker_id = bookmaker.get("id")
            bookmaker_name = str(
                bookmaker.get("name") or f"bookmaker-{bookmaker_id}"
            )
            bookmaker_key = str(
                bookmaker_id
                if bookmaker_id is not None
                else bookmaker_name
            )

            bets = bookmaker.get("bets")
            if not isinstance(bets, list):
                continue

            complete_quote = None
            for bet in bets:
                if not isinstance(bet, dict):
                    continue

                bet_name = _normalise_market_text(bet.get("name"))
                is_match_winner = bet_name in {
                    "match winner",
                    "fulltime result",
                    "full time result",
                    "1x2",
                }
                if not is_match_winner:
                    continue

                values = bet.get("values")
                if not isinstance(values, list):
                    continue

                odds: dict[str, float] = {}
                for item in values:
                    if not isinstance(item, dict):
                        continue
                    key = _outcome_key(item.get("value"))
                    odd = _decimal_odd(item.get("odd"))
                    if key and odd is not None:
                        odds[key] = odd

                if set(odds) != {"home", "draw", "away"}:
                    continue

                raw_probabilities = {
                    outcome: 1.0 / odd
                    for outcome, odd in odds.items()
                }
                overround = sum(raw_probabilities.values())
                if overround <= 0:
                    continue

                no_margin = {
                    outcome: probability / overround
                    for outcome, probability
                    in raw_probabilities.items()
                }

                complete_quote = {
                    "bookmaker_id": bookmaker_id,
                    "bookmaker_name": bookmaker_name,
                    "bet_id": bet.get("id"),
                    "bet_name": bet.get("name"),
                    "odds": odds,
                    "overround": overround,
                    "no_margin": no_margin,
                }
                break

            if complete_quote is not None:
                quotes_by_bookmaker[bookmaker_key] = complete_quote

    quotes = sorted(
        quotes_by_bookmaker.values(),
        key=lambda item: (
            str(item.get("bookmaker_name") or ""),
            str(item.get("bookmaker_id") or ""),
        ),
    )
    return quotes, sorted(set(provider_updates))


def _build_market_consensus(
    *,
    quotes: list[dict[str, Any]],
    home_team: str,
    away_team: str,
) -> dict[str, Any] | None:
    if not quotes:
        return None

    outcomes = ("home", "draw", "away")
    median_odds = {
        outcome: statistics.median(
            float(quote["odds"][outcome])
            for quote in quotes
        )
        for outcome in outcomes
    }
    median_no_margin_raw = {
        outcome: statistics.median(
            float(quote["no_margin"][outcome])
            for quote in quotes
        )
        for outcome in outcomes
    }

    probability_total = sum(median_no_margin_raw.values())
    if probability_total <= 0:
        return None

    consensus_probability = {
        outcome: median_no_margin_raw[outcome] / probability_total
        for outcome in outcomes
    }

    home_probability = consensus_probability["home"]
    away_probability = consensus_probability["away"]
    if math.isclose(
        home_probability,
        away_probability,
        rel_tol=0.0,
        abs_tol=1e-12,
    ):
        team_favourite = "PICKEM"
        team_favourite_name = None
    elif home_probability > away_probability:
        team_favourite = "HOME"
        team_favourite_name = home_team
    else:
        team_favourite = "AWAY"
        team_favourite_name = away_team

    market_outcome_leader = max(
        outcomes,
        key=lambda outcome: consensus_probability[outcome],
    ).upper()

    bookmaker_count = len(quotes)
    market_ready = bookmaker_count >= MARKET_MIN_BOOKMAKER_COUNT

    return {
        "method": (
            "median bookmaker no-margin probabilities, then renormalised"
        ),
        "market": "90-minute Match Winner 1X2",
        "bookmaker_count": bookmaker_count,
        "minimum_bookmaker_count": MARKET_MIN_BOOKMAKER_COUNT,
        "market_ready": market_ready,
        "pre_match_capture_valid": pre_match_capture_valid,
        "historical_research_only": not pre_match_capture_valid,
        "temporal_validation": temporal_validation,
        "median_decimal_odds": {
            outcome: round(value, 6)
            for outcome, value in median_odds.items()
        },
        "consensus_no_margin_probability": {
            outcome: round(value, 8)
            for outcome, value in consensus_probability.items()
        },
        "market_outcome_leader": market_outcome_leader,
        "team_favourite": team_favourite,
        "team_favourite_name": team_favourite_name,
        "home_away_probability_gap": round(
            abs(home_probability - away_probability),
            8,
        ),
        "draw_is_market_leader": (
            market_outcome_leader == "DRAW"
        ),
        "favourite_frozen": False,
        "astrology_action_allowed": False,
    }


def _parse_market_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(
                value.strip().replace("Z", "+00:00")
            )
        except ValueError:
            return None
    else:
        return None

    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(timezone.utc)


def _market_temporal_validation(
    *,
    kickoff_utc: Any,
    captured_at: Any,
    provider_updates: list[str] | None = None,
) -> dict[str, Any]:
    kickoff = _parse_market_datetime(kickoff_utc)
    captured = _parse_market_datetime(captured_at)

    parsed_updates = [
        parsed
        for parsed in (
            _parse_market_datetime(value)
            for value in (provider_updates or [])
        )
        if parsed is not None
    ]
    latest_provider_update = (
        max(parsed_updates) if parsed_updates else None
    )

    capture_before_kickoff = bool(
        kickoff is not None
        and captured is not None
        and captured < kickoff
    )
    provider_update_before_kickoff = bool(
        kickoff is not None
        and (
            latest_provider_update is None
            or latest_provider_update < kickoff
        )
    )
    pre_match_capture_valid = bool(
        capture_before_kickoff
        and provider_update_before_kickoff
    )

    reason = None
    if kickoff is None:
        reason = "kickoff_time_invalid"
    elif captured is None:
        reason = "capture_time_invalid"
    elif captured >= kickoff:
        reason = "captured_at_or_after_kickoff"
    elif (
        latest_provider_update is not None
        and latest_provider_update >= kickoff
    ):
        reason = "provider_update_at_or_after_kickoff"

    seconds_before_kickoff = None
    if kickoff is not None and captured is not None:
        seconds_before_kickoff = (
            kickoff - captured
        ).total_seconds()

    return {
        "kickoff_utc": (
            kickoff.isoformat() if kickoff is not None else None
        ),
        "captured_at": (
            captured.isoformat() if captured is not None else None
        ),
        "latest_provider_update": (
            latest_provider_update.isoformat()
            if latest_provider_update is not None
            else None
        ),
        "capture_before_kickoff": capture_before_kickoff,
        "provider_update_before_kickoff": (
            provider_update_before_kickoff
        ),
        "pre_match_capture_valid": pre_match_capture_valid,
        "seconds_before_kickoff": seconds_before_kickoff,
        "temporal_rejection_reason": reason,
    }


def _store_market_snapshot(
    *,
    fixture_id: int,
    consensus: dict[str, Any],
    quotes: list[dict[str, Any]],
    provider_updates: list[str],
    provider_paging: Any,
    captured_at: datetime,
    temporal_validation: dict[str, Any],
) -> dict[str, Any]:
    probabilities = consensus["consensus_no_margin_probability"]
    median_odds = consensus["median_decimal_odds"]

    raw_payload = {
        "market": "90-minute Match Winner 1X2",
        "method": consensus["method"],
        "quotes": quotes,
        "provider_updates": provider_updates,
        "provider_paging": provider_paging,
        "consensus": consensus,
        "temporal_validation": temporal_validation,
    }

    try:
        with _database_connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO odds_snapshots (
                        fixture_id,
                        provider,
                        captured_at,
                        home_odds,
                        draw_odds,
                        away_odds,
                        no_margin_home,
                        no_margin_draw,
                        no_margin_away,
                        consensus_favourite,
                        bookmaker_count,
                        raw_odds_json
                    )
                    VALUES (
                        %s,
                        'api-football:prematch-1x2',
                        %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s::jsonb
                    )
                    RETURNING id
                    """,
                    (
                        fixture_id,
                        captured_at,
                        median_odds["home"],
                        median_odds["draw"],
                        median_odds["away"],
                        probabilities["home"],
                        probabilities["draw"],
                        probabilities["away"],
                        consensus["team_favourite"],
                        consensus["bookmaker_count"],
                        json.dumps(
                            raw_payload,
                            ensure_ascii=False,
                            default=str,
                        ),
                    ),
                )
                snapshot_id = int(cursor.fetchone()[0])
            connection.commit()
    except Exception as exc:
        return {
            "status": "error",
            "stored": False,
            "error_type": type(exc).__name__,
            "message": "Market snapshot could not be stored.",
        }

    return {
        "status": "ok",
        "stored": True,
        "snapshot_id": snapshot_id,
        "captured_at": captured_at.isoformat(),
    }


def latest_market_status(
    fixture_id: int,
) -> dict[str, Any]:
    if not DATABASE_URL or psycopg is None:
        return {
            "status": "database_unavailable",
            "market_ready": False,
            "fixture_id": fixture_id,
        }

    try:
        with psycopg.connect(
            DATABASE_URL,
            connect_timeout=DATABASE_CONNECT_TIMEOUT_SECONDS,
            autocommit=True,
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        odds_snapshots.id,
                        odds_snapshots.provider,
                        odds_snapshots.captured_at,
                        odds_snapshots.home_odds,
                        odds_snapshots.draw_odds,
                        odds_snapshots.away_odds,
                        odds_snapshots.no_margin_home,
                        odds_snapshots.no_margin_draw,
                        odds_snapshots.no_margin_away,
                        odds_snapshots.consensus_favourite,
                        odds_snapshots.bookmaker_count,
                        odds_snapshots.raw_odds_json,
                        fixtures.kickoff_utc
                    FROM odds_snapshots
                    INNER JOIN fixtures
                        ON fixtures.id = odds_snapshots.fixture_id
                    WHERE odds_snapshots.fixture_id = %s
                    ORDER BY
                        odds_snapshots.captured_at DESC,
                        odds_snapshots.id DESC
                    LIMIT 1
                    """,
                    (fixture_id,),
                )
                row = cursor.fetchone()
    except Exception as exc:
        return {
            "status": "error",
            "market_ready": False,
            "fixture_id": fixture_id,
            "error_type": type(exc).__name__,
        }

    if not row:
        metadata = _read_market_attempt_metadata(fixture_id)
        return {
            "status": "missing",
            "market_ready": False,
            "fixture_id": fixture_id,
            "bookmaker_count": 0,
            "minimum_bookmaker_count": MARKET_MIN_BOOKMAKER_COUNT,
            "last_attempt": metadata,
            "favourite_frozen": False,
            "astrology_action_allowed": False,
        }

    raw_json = row[11] if isinstance(row[11], dict) else {}
    consensus = (
        raw_json.get("consensus")
        if isinstance(raw_json, dict)
        else None
    )
    consensus = consensus if isinstance(consensus, dict) else {}

    bookmaker_count = int(row[10] or 0)
    provider_updates = (
        raw_json.get("provider_updates", [])
        if isinstance(raw_json, dict)
        and isinstance(raw_json.get("provider_updates"), list)
        else []
    )
    temporal_validation = _market_temporal_validation(
        kickoff_utc=row[12],
        captured_at=row[2],
        provider_updates=provider_updates,
    )
    pre_match_capture_valid = bool(
        temporal_validation["pre_match_capture_valid"]
    )
    market_ready = (
        bool(consensus.get("market_ready"))
        and bookmaker_count >= MARKET_MIN_BOOKMAKER_COUNT
        and pre_match_capture_valid
    )

    return {
        "status": (
            "ok"
            if pre_match_capture_valid
            else "historical_research_only"
        ),
        "fixture_id": fixture_id,
        "snapshot_id": int(row[0]),
        "provider": row[1],
        "captured_at": (
            row[2].isoformat()
            if isinstance(row[2], datetime)
            else str(row[2])
        ),
        "market": "90-minute Match Winner 1X2",
        "bookmaker_count": bookmaker_count,
        "minimum_bookmaker_count": MARKET_MIN_BOOKMAKER_COUNT,
        "market_ready": market_ready,
        "median_decimal_odds": {
            "home": float(row[3]) if row[3] is not None else None,
            "draw": float(row[4]) if row[4] is not None else None,
            "away": float(row[5]) if row[5] is not None else None,
        },
        "consensus_no_margin_probability": {
            "home": float(row[6]) if row[6] is not None else None,
            "draw": float(row[7]) if row[7] is not None else None,
            "away": float(row[8]) if row[8] is not None else None,
        },
        "team_favourite": row[9],
        "team_favourite_name": consensus.get(
            "team_favourite_name"
        ),
        "market_outcome_leader": consensus.get(
            "market_outcome_leader"
        ),
        "home_away_probability_gap": consensus.get(
            "home_away_probability_gap"
        ),
        "draw_is_market_leader": bool(
            consensus.get("draw_is_market_leader")
        ),
        "favourite_frozen": False,
        "astrology_action_allowed": False,
        "quote_count": len(
            raw_json.get("quotes", [])
            if isinstance(raw_json, dict)
            and isinstance(raw_json.get("quotes"), list)
            else []
        ),
    }


def capture_target_market_odds(
    *,
    force: bool = False,
) -> dict[str, Any]:
    fixture_id = MARKET_CAPTURE_TARGET_FIXTURE_ID
    fixture = _load_market_capture_fixture(fixture_id)
    if fixture is None:
        return {
            "status": "no_candidate",
            "captured": False,
            "fixture_id": fixture_id,
            "message": "The target stored fixture was not found.",
        }

    if not fixture.get("location_time_ready"):
        return {
            "status": "blocked",
            "captured": False,
            "fixture": fixture,
            "message": "Location and venue-local time are not ready.",
        }

    provider_fixture_id = fixture.get("provider_fixture_id")
    if not provider_fixture_id:
        return {
            "status": "blocked",
            "captured": False,
            "fixture": fixture,
            "message": "Provider fixture ID is missing.",
        }

    kickoff_utc = _parse_market_datetime(
        fixture.get("kickoff_utc")
    )
    capture_started_at = datetime.now(timezone.utc)
    if kickoff_utc is None:
        _write_market_attempt_metadata(
            fixture_id=fixture_id,
            status="kickoff_time_invalid",
            bookmaker_count=0,
        )
        return {
            "status": "blocked",
            "captured": False,
            "fixture": fixture,
            "market_ready": False,
            "favourite_frozen": False,
            "astrology_action_allowed": False,
            "message": "Kickoff time is invalid.",
        }

    if capture_started_at >= kickoff_utc:
        _write_market_attempt_metadata(
            fixture_id=fixture_id,
            status="kickoff_passed_no_capture",
            bookmaker_count=0,
        )
        return {
            "status": "blocked_post_kickoff",
            "captured": False,
            "fixture": fixture,
            "market_ready": False,
            "pre_match_capture_valid": False,
            "historical_research_only": True,
            "favourite_frozen": False,
            "astrology_action_allowed": False,
            "seconds_after_kickoff": round(
                (capture_started_at - kickoff_utc).total_seconds(),
                3,
            ),
            "latest_market_status": latest_market_status(
                fixture_id
            ),
            "message": (
                "Kickoff has passed. No new odds request was made."
            ),
        }

    metadata = _read_market_attempt_metadata(fixture_id)
    if not force and _market_attempt_is_fresh(metadata):
        return {
            "status": "ok",
            "captured": False,
            "skipped": True,
            "reason": "A recent market capture attempt already exists.",
            "fixture": fixture,
            "last_attempt": metadata,
            "latest_market_status": latest_market_status(fixture_id),
        }

    try:
        payload = _api_football_get(
            "/odds",
            params={
                "fixture": provider_fixture_id,
                "page": 1,
            },
        )
    except Exception as exc:
        _write_market_attempt_metadata(
            fixture_id=fixture_id,
            status="provider_error",
            bookmaker_count=0,
        )
        return {
            "status": "error",
            "captured": False,
            "fixture": fixture,
            "error_type": type(exc).__name__,
            "message": "Pre-match odds capture failed.",
        }

    quotes, provider_updates = _extract_prematch_1x2_quotes(payload)
    bookmaker_count = len(quotes)

    if not quotes:
        _write_market_attempt_metadata(
            fixture_id=fixture_id,
            status="no_complete_1x2_odds",
            bookmaker_count=0,
        )
        return {
            "status": "no_odds",
            "captured": False,
            "fixture": fixture,
            "bookmaker_count": 0,
            "market_ready": False,
            "favourite_frozen": False,
            "astrology_action_allowed": False,
            "message": (
                "API-Football returned no complete pre-match 1X2 quotes."
            ),
            "provider_paging": payload.get("paging"),
        }

    consensus = _build_market_consensus(
        quotes=quotes,
        home_team=str(fixture["home_team"]),
        away_team=str(fixture["away_team"]),
    )
    if consensus is None:
        _write_market_attempt_metadata(
            fixture_id=fixture_id,
            status="consensus_failed",
            bookmaker_count=bookmaker_count,
        )
        return {
            "status": "error",
            "captured": False,
            "fixture": fixture,
            "bookmaker_count": bookmaker_count,
            "message": "The market consensus could not be calculated.",
        }

    captured_at = datetime.now(timezone.utc)
    temporal_validation = _market_temporal_validation(
        kickoff_utc=kickoff_utc,
        captured_at=captured_at,
        provider_updates=provider_updates,
    )
    if not temporal_validation["pre_match_capture_valid"]:
        _write_market_attempt_metadata(
            fixture_id=fixture_id,
            status=(
                temporal_validation["temporal_rejection_reason"]
                or "temporal_validation_failed"
            ),
            bookmaker_count=bookmaker_count,
        )
        return {
            "status": "blocked_temporal_validation",
            "captured": False,
            "fixture": fixture,
            "bookmaker_count": bookmaker_count,
            "market_ready": False,
            "pre_match_capture_valid": False,
            "historical_research_only": True,
            "temporal_validation": temporal_validation,
            "favourite_frozen": False,
            "astrology_action_allowed": False,
            "message": (
                "The odds response was not stored as a live "
                "pre-match market snapshot."
            ),
        }

    stored = _store_market_snapshot(
        fixture_id=fixture_id,
        consensus=consensus,
        quotes=quotes,
        provider_updates=provider_updates,
        provider_paging=payload.get("paging"),
        captured_at=captured_at,
        temporal_validation=temporal_validation,
    )
    if not stored.get("stored"):
        _write_market_attempt_metadata(
            fixture_id=fixture_id,
            status="storage_failed",
            bookmaker_count=bookmaker_count,
        )
        return {
            **stored,
            "captured": False,
            "fixture": fixture,
        }

    attempt_status = (
        "market_ready"
        if consensus["market_ready"]
        else "insufficient_bookmakers"
    )
    _write_market_attempt_metadata(
        fixture_id=fixture_id,
        status=attempt_status,
        bookmaker_count=bookmaker_count,
    )

    return {
        "status": "ok",
        "captured": True,
        "fixture": fixture,
        "snapshot": stored,
        "consensus": consensus,
        "provider_updates": provider_updates,
        "provider_paging": payload.get("paging"),
        "temporal_validation": temporal_validation,
    }


@app.on_event("startup")
def capture_market_odds_on_startup() -> None:
    global MARKET_CAPTURE_STARTUP_STATUS

    if not MARKET_CAPTURE_AUTO_RUN:
        MARKET_CAPTURE_STARTUP_STATUS = {
            "status": "disabled",
            "captured": False,
        }
        return

    if not DATABASE_URL or not API_FOOTBALL_KEY:
        MARKET_CAPTURE_STARTUP_STATUS = {
            "status": "not_configured",
            "captured": False,
        }
        return

    MARKET_CAPTURE_STARTUP_STATUS = capture_target_market_odds()


# ============================================================
# AUTHENTICATION
# ============================================================

def verify_proxy_key(supplied_key: str | None) -> None:
    if supplied_key != PROXY_API_KEY:
        raise HTTPException(
            status_code=401,
            detail="Invalid proxy API key.",
        )


# ============================================================
# PUBLIC ROUTES
# ============================================================

@app.get("/")
def root() -> dict[str, Any]:
    return {
        "status": "ok",
        "service": "VedAstro GPT Proxy",
        "version": PROXY_VERSION,
        "health": "/health",
    }


@app.get("/health")
def health() -> dict[str, Any]:
    return {
        "status": "ok",
        "service": "VedAstro GPT Proxy",
        "version": PROXY_VERSION,
        "vedastro_key_configured": bool(VEDASTRO_API_KEY),
        "proxy_key_configured": bool(PROXY_API_KEY),
        "database_url_configured": bool(DATABASE_URL),
        "database_driver_available": psycopg is not None,
        "database_checkpoint": "DB8A enforce capture-before-kickoff market validity",
        "database_schema_version": DATABASE_SCHEMA_VERSION,
        "database_schema_startup_status": DATABASE_SCHEMA_STARTUP_STATUS,
        "api_football_key_configured": bool(API_FOOTBALL_KEY),
        "api_football_checkpoint": (
            "DB4 imports today's fixtures with a persisted cooldown"
        ),
        "api_football_health_cache_seconds": (
            API_FOOTBALL_HEALTH_CACHE_SECONDS
        ),
        "soccer_display_timezone": SOCCER_DISPLAY_TIMEZONE,
        "fixture_sync_min_interval_seconds": (
            FIXTURE_SYNC_MIN_INTERVAL_SECONDS
        ),
        "fixture_sync_startup_status": FIXTURE_SYNC_STARTUP_STATUS,
        "fixture_timezone_migration_status": (
            FIXTURE_TIMEZONE_MIGRATION_STATUS
        ),
        "fixture_import_checkpoint": (
            "today imported; default listing now shows upcoming only"
        ),
        "locationiq_key_configured": bool(LOCATIONIQ_KEY),
        "timezonefinder_available": timezone_at is not None,
        "location_checkpoint": (
            "DB7 commits only independently reviewed venue manifests"
        ),
        "location_health_cache_seconds": (
            LOCATIONIQ_HEALTH_CACHE_SECONDS
        ),
        "location_preview_auto_run": LOCATION_PREVIEW_AUTO_RUN,
        "location_preview_startup_status": (
            LOCATION_PREVIEW_STARTUP_STATUS
        ),
        "geocode_safety_migration_status": (
            GEOCODE_SAFETY_MIGRATION_STATUS
        ),
        "geocode_auto_approval_policy": {
            "minimum_score": LOCATION_PREVIEW_AUTO_APPROVE_SCORE,
            "country_match_required": True,
            "city_match_required_when_known": True,
            "sports_place_match_required": True,
            "minimum_venue_token_overlap": 0.50,
            "timezone_required": True,
        },
        "location_preview_queue_policy": {
            "scan_limit": LOCATION_PREVIEW_QUEUE_SCAN_LIMIT,
            "skip_all_cached_query_statuses": True,
            "prefer_sports_venue_names": True,
            "coordinates_committed": False,
        },
        "location_geocode_strategy": {
            "version": LOCATION_GEOCODE_STRATEGY_VERSION,
            "city_context_required": True,
            "bounded_viewbox_required": True,
            "country_code_filter_used_when_available": True,
            "maximum_city_distance_km": LOCATION_MAX_CITY_DISTANCE_KM,
            "http_404_means_no_match": True,
            "negative_attempts_cached": True,
            "coordinates_committed": False,
        },
        "location_preview_batch_policy": {
            "maximum_fixtures": (
                LOCATION_PREVIEW_STARTUP_MAX_FIXTURES
            ),
            "maximum_provider_calls": (
                LOCATION_PREVIEW_STARTUP_MAX_PROVIDER_CALLS
            ),
            "continue_after_rejected_or_no_match": True,
            "stop_for_preview_or_auto_approved": True,
            "coordinates_committed": False,
        },
        "reviewed_location_commit_enabled": (
            REVIEWED_LOCATION_COMMIT_ENABLED
        ),
        "reviewed_location_commit_status": (
            REVIEWED_LOCATION_COMMIT_STATUS
        ),
        "location_commit_policy": {
            "automatic_general_commit": False,
            "review_manifest_required": True,
            "independent_coordinate_reference_required": True,
            "maximum_reference_separation_meters": 100.0,
            "provenance_required_for_prediction_ready": True,
        },
        "fixture_detail_endpoint": {
            "enabled": True,
            "path_template": "/fixtures/{database_fixture_id}",
            "avoids_list_limit_truncation": True,
        },
        "fixture_time_semantics": {
            "kickoff_local": "backward-compatible display timezone",
            "kickoff_display_local": SOCCER_DISPLAY_TIMEZONE,
            "kickoff_venue_local": "verified venue timezone",
            "venue_utc_offset_included": True,
            "vedastro_std_time_enabled": True,
            "vedastro_std_time_format": "HH:MM DD/MM/YYYY +HH:MM",
            "venue_local_time_required_for_prediction_ready": True,
        },
        "market_capture_checkpoint": {
            "provider": "API-Football",
            "endpoint": "/odds",
            "market": "90-minute Match Winner 1X2",
            "target_database_fixture_id": (
                MARKET_CAPTURE_TARGET_FIXTURE_ID
            ),
            "minimum_bookmaker_count": MARKET_MIN_BOOKMAKER_COUNT,
            "capture_interval_seconds": (
                MARKET_CAPTURE_MIN_INTERVAL_SECONDS
            ),
            "favourite_frozen": False,
            "astrology_action_allowed": False,
        },
        "market_temporal_guard": {
            "capture_must_precede_kickoff": True,
            "provider_update_must_precede_kickoff": True,
            "post_kickoff_provider_call_allowed": False,
            "post_kickoff_snapshot_market_ready": False,
            "post_kickoff_snapshot_use": "historical_research_only",
        },
        "market_capture_startup_status": (
            MARKET_CAPTURE_STARTUP_STATUS
        ),
        "prediction_readiness_semantics": {
            "location_time_ready_is_separate": True,
            "market_ready_is_separate": True,
            "prediction_ready_requires_future_performance_layers": True,
            "astrology_action_remains_blocked": True,
        },
        "locationiq_public_attribution_required": True,
        "ayanamsa": "Lahiri",
        "engine": "VedAstro.Python",
        "planet_parameter_shape": "nested PlanetName object",
        "vedastro_authentication": (
            "x-api-key header with APIKey body fallback"
        ),
        "response_mode": "prediction-grade compact v2 + pre-match time guard DB8A",
        "action_response_target_characters": (
            ACTION_RESPONSE_TARGET_CHARACTERS
        ),
        "action_response_safety_target_characters": (
            ACTION_RESPONSE_SAFETY_TARGET_CHARACTERS
        ),
        "action_response_payload_target_characters": (
            ACTION_RESPONSE_PAYLOAD_TARGET_CHARACTERS
        ),
        "reliability_policy_mode": RELIABILITY_POLICY_MODE,
        "advanced_layers": {
            "exact_lahiri_placidus_cusps": True,
            "planet_cusp_contacts": True,
            "navamsha_cusps": True,
            "kp_sublords": True,
            "outer_planets": SWISSEPH_AVAILABLE,
            "gulika_upaketu": True,
            "nakshatra_taras": True,
            "navamsha_name_sounds": True,
            "stolen_cusps": True,
            "tier1_combinations": True,
            "navamsha_interpretation": True,
            "decision_grade_contact_filtering": True,
            "rahu_ketu_axis_deduplication": True,
            "d9_overlap_deduplication": True,
            "independent_rule_of_three": True,
            "chart_correlation_signature": True,
            "printed_page_reference_correction": True,
            "reliability_audit": True,
        },
        "outer_planet_engine": {
            "name": "pyswisseph",
            "available": SWISSEPH_AVAILABLE,
            "version": (
                getattr(swe, "version", None)
                if SWISSEPH_AVAILABLE
                else None
            ),
            "ephemeris_path_configured": bool(SWISSEPH_EPHE_PATH),
        },
        "minimum_call_interval_seconds": VEDASTRO_MIN_INTERVAL_SECONDS,
        "maximum_attempts_per_method": VEDASTRO_MAX_RETRIES,
        "parallel_workers": VEDASTRO_MAX_WORKERS,
    }




@app.get("/database-health")
def database_health() -> dict[str, Any]:
    """
    Public, non-secret connectivity check for the setup process.
    """
    result = database_connection_status()
    if not result.get("connected"):
        raise HTTPException(status_code=503, detail=result)
    return result


@app.get("/database-schema")
def database_schema() -> dict[str, Any]:
    """
    Public, non-secret schema verification for the setup process.
    """
    result = database_schema_status()
    if not result.get("ready"):
        raise HTTPException(status_code=503, detail=result)
    return result


@app.get("/football-health")
def football_health() -> dict[str, Any]:
    """
    Public, cached, non-secret API-Football connectivity check.

    The one-hour cache prevents repeated browser refreshes from rapidly
    consuming a limited daily provider allowance.
    """
    result = api_football_connection_status()
    if not result.get("connected"):
        raise HTTPException(status_code=503, detail=result)
    return result


@app.get("/location-health")
def location_health() -> dict[str, Any]:
    """
    Public, cached and non-secret LocationIQ/timezone connectivity check.
    """
    result = locationiq_connection_status()
    if not result.get("connected"):
        raise HTTPException(status_code=503, detail=result)
    return result


@app.get("/location-commit-status")
def location_commit_status() -> dict[str, Any]:
    """
    Public non-secret audit status for reviewed coordinate commits.
    """
    return {
        "status": "ok",
        "proxy_version": PROXY_VERSION,
        "automatic_general_commit": False,
        "review_manifest_required": True,
        "startup_status": REVIEWED_LOCATION_COMMIT_STATUS,
    }


@app.get("/location-preview-status")
def location_preview_status() -> dict[str, Any]:
    """
    Show the latest one-fixture preview. This does not expose API keys.
    """
    return {
        "status": "ok",
        "proxy_version": PROXY_VERSION,
        "fixture_coordinates_committed": False,
        "geocode_safety_migration_status": (
            GEOCODE_SAFETY_MIGRATION_STATUS
        ),
        "auto_approval_policy": {
            "minimum_score": LOCATION_PREVIEW_AUTO_APPROVE_SCORE,
            "sports_place_match_required": True,
            "minimum_venue_token_overlap": 0.50,
        },
        "queue_policy": {
            "scan_limit": LOCATION_PREVIEW_QUEUE_SCAN_LIMIT,
            "skip_cached_queries": True,
            "prefer_sports_venue_names": True,
        },
        "geocode_strategy": {
            "version": LOCATION_GEOCODE_STRATEGY_VERSION,
            "city_context_required": True,
            "bounded_viewbox_required": True,
            "country_code_filter_used_when_available": True,
            "maximum_city_distance_km": LOCATION_MAX_CITY_DISTANCE_KM,
            "http_404_means_no_match": True,
            "negative_attempts_cached": True,
        },
        "batch_policy": {
            "maximum_fixtures": (
                LOCATION_PREVIEW_STARTUP_MAX_FIXTURES
            ),
            "maximum_provider_calls": (
                LOCATION_PREVIEW_STARTUP_MAX_PROVIDER_CALLS
            ),
            "continue_after_rejected_or_no_match": True,
        },
        "startup_status": LOCATION_PREVIEW_STARTUP_STATUS,
    }


@app.post("/fixtures/{fixture_id}/location-preview")
def fixture_location_preview(
    fixture_id: int,
    x_proxy_key: str | None = Header(
        default=None,
        alias="x-proxy-key",
    ),
) -> dict[str, Any]:
    """
    Protected manual preview for one stored fixture.

    Results are cached. This endpoint never updates fixture coordinates.
    """
    verify_proxy_key(x_proxy_key)
    result = preview_fixture_location(fixture_id=fixture_id)
    if result.get("status") not in {"ok", "no_candidate", "no_match"}:
        raise HTTPException(status_code=503, detail=result)
    return result


@app.get("/fixtures/{fixture_id}/market-status")
def fixture_market_status(
    fixture_id: int,
) -> dict[str, Any]:
    """
    Return the latest stored pre-match 1X2 consensus.

    This route never calls the provider and never freezes the favourite.
    """
    return latest_market_status(fixture_id)


@app.get("/fixtures/{fixture_id}")
def fixture_detail(
    fixture_id: int,
) -> dict[str, Any]:
    """
    Retrieve one stored fixture by its database ID.
    """
    result = get_stored_fixture_by_id(fixture_id)
    if result.get("status") == "not_found":
        raise HTTPException(status_code=404, detail=result)
    if not result.get("ready"):
        raise HTTPException(status_code=503, detail=result)
    return result


@app.get("/fixtures")
def fixtures(
    window: str = Query(
        default="today",
        pattern="^(today|tomorrow|7d|30d)$",
    ),
    limit: int = Query(
        default=FIXTURE_LIST_DEFAULT_LIMIT,
        ge=1,
        le=FIXTURE_LIST_MAX_LIMIT,
    ),
    include_completed: bool = Query(default=False),
) -> dict[str, Any]:
    """
    List stored fixtures.

    Default behaviour is future scheduled fixtures only. Historical/completed
    rows remain available with include_completed=true for later auditing.
    """
    result = list_stored_fixtures(
        window=window,
        limit=limit,
        include_completed=include_completed,
    )
    if not result.get("ready"):
        raise HTTPException(status_code=503, detail=result)
    return result


@app.get("/fixtures-sync-status")
def fixtures_sync_status() -> dict[str, Any]:
    metadata = _fixture_sync_metadata()
    return {
        "status": "ok",
        "proxy_version": PROXY_VERSION,
        "display_timezone": SOCCER_DISPLAY_TIMEZONE,
        "minimum_sync_interval_seconds": (
            FIXTURE_SYNC_MIN_INTERVAL_SECONDS
        ),
        "startup_status": FIXTURE_SYNC_STARTUP_STATUS,
        "timezone_migration_status": FIXTURE_TIMEZONE_MIGRATION_STATUS,
        "metadata": metadata,
    }


@app.post("/fixtures/sync-today")
def fixtures_sync_today(
    x_proxy_key: str | None = Header(
        default=None,
        alias="x-proxy-key",
    ),
) -> dict[str, Any]:
    """
    Protected manual sync. It still honours the persisted cooldown.
    """
    verify_proxy_key(x_proxy_key)
    result = sync_today_fixtures(force=False)
    if result.get("status") != "ok":
        raise HTTPException(status_code=503, detail=result)
    return result


# ============================================================
# EVENT CHART
# ============================================================

def calculate_event_chart(request: EventChartInput) -> dict[str, Any]:
    invalid_houses = [
        house for house in request.houses if house not in HOUSES
    ]

    if invalid_houses:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid houses: {invalid_houses}",
        )

    invalid_planets = [
        planet for planet in request.planets if planet not in PLANETS
    ]

    if invalid_planets:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid planets: {invalid_planets}",
        )

    try:
        location = GeoLocation(
            request.location.name,
            request.location.longitude,
            request.location.latitude,
        )
        event_time = Time(request.std_time, location)
    except Exception as error:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid time or location: {error}",
        ) from error

    # Core calculations are kept explicit because they are essential and
    # their individual method names are useful in validation failures.
    core = {
        "ayanamsa_degree": vedastro_call(
            "AyanamsaDegree",
            event_time,
            required=True,
        ),
        "lagna_sign": calculate_lagna(event_time),
        "moon_sign": calculate_moon_sign(event_time),
        "moon_nakshatra": vedastro_call(
            "MoonConstellation",
            event_time,
            required=True,
        ),
        "tithi": vedastro_call("LunarDay", event_time),
        "yoga": calculate_yoga(event_time),
        "karana": vedastro_call("Karana", event_time),
        "weekday": vedastro_call("DayOfWeek", event_time),
        "hora_lord": vedastro_call("LordOfHoraFromTime", event_time),
    }

    moon_consistency = validate_moon_consistency(
        core["moon_sign"],
        core["moon_nakshatra"],
    )
    core["moon_consistency"] = moon_consistency

    # Exact Lahiri sidereal Placidus cusp degrees for Gambler's Dharma Tier 2.
    # This is intentionally non-essential for the existing standard-chart gate:
    # a temporary upstream cusp failure must not break the working v1 response.
    rashi_placidus = calculate_rashi_placidus(event_time)

    requested_houses = list(dict.fromkeys(request.houses))
    requested_planets = list(dict.fromkeys(request.planets))

    houses: dict[str, dict[str, Any]] = {}
    planets: dict[str, dict[str, Any]] = {}

    # House groups and planet groups run concurrently. Upstream call starts
    # are still throttled centrally by wait_for_call_slot().
    with ThreadPoolExecutor(max_workers=VEDASTRO_MAX_WORKERS) as executor:
        future_map = {}

        for house_name in requested_houses:
            future = executor.submit(
                calculate_house,
                house_name,
                event_time,
            )
            future_map[future] = ("house", house_name)

        for planet_name in requested_planets:
            future = executor.submit(
                calculate_planet,
                planet_name,
                event_time,
                core["moon_sign"] if planet_name == "Moon" else None,
            )
            future_map[future] = ("planet", planet_name)

        for future in as_completed(future_map):
            result_type, name = future_map[future]

            try:
                result = future.result()
            except Exception as error:
                result = {
                    "status": "Fail",
                    "required": name in {"House1", "House7", "Sun", "Moon"},
                    "method": f"{result_type}:{name}",
                    "error": str(error),
                }

            if result_type == "house":
                houses[name] = result
            else:
                planets[name] = result

    # Restore the exact requested order in the response.
    houses = {
        name: houses[name]
        for name in requested_houses
        if name in houses
    }
    planets = {
        name: planets[name]
        for name in requested_planets
        if name in planets
    }

    # Chapter 3 victory houses, SKY/PKY, parivartana and relevant
    # planetary war. This uses whole-sign rashi houses from the exact
    # Lahiri Ascendant and remains non-essential to the standard chart gate.
    tier1_combinations = calculate_tier1_combinations(
        request.std_time,
        rashi_placidus,
        planets,
    )

    # Exact Tier 2 raw geometry: requested planets against the six sensitive
    # Placidus cusps. This remains non-essential for the standard chart gate.
    planet_cusp_contacts = calculate_planet_cusp_contacts(
        planets,
        rashi_placidus,
    )

    # Exact Chapter 5 D9 geometry. This is kept separate from the standard
    # essential chart gate until its live output has been verified.
    navamsha_cusps = calculate_exact_navamsha_cusps(
        event_time,
        planets,
        rashi_placidus,
    )

    # Chapter 6 is calculated independently under Krishnamurti ayanamsha.
    # It does not alter or replace the standard Lahiri chart.
    kp_sublords = calculate_kp_sublords(
        event_time,
        requested_planets,
    )

    # Chapter 4 outer planets are calculated locally because the official
    # VedAstro PlanetName enum contains only the nine classical bodies.
    outer_planets = calculate_outer_planets(
        request.std_time,
        rashi_placidus,
    )

    # Official VedAstro Gulika and Upaketu calculators. This remains a
    # non-essential advanced layer and cannot invalidate the standard chart.
    special_points = calculate_special_points(
        event_time,
        rashi_placidus,
        navamsha_cusps,
        houses,
        planets,
    )

    # Chapter 4 exact stolen-cusp audit. This combines the exact Placidus
    # geometry with all currently available classical, outer and special bodies.
    stolen_cusps = calculate_stolen_cusps(
        rashi_placidus,
        planets,
        outer_planets,
        special_points,
    )

    # Chapter 5 interpretation: D9 cusp effects, D9 combinations,
    # D1/D9 reinforcement or reversal, and double-whammy transfer.
    navamsha_interpretation = calculate_navamsha_interpretation(
        navamsha_cusps,
        tier1_combinations,
        planet_cusp_contacts,
        outer_planets,
        special_points,
        stolen_cusps,
    )

    # Correlation signature for batch slates. The Action remains stateless;
    # the caller groups matching cluster signatures across events.
    chart_correlation = calculate_chart_correlation_signature(
        rashi_placidus,
        navamsha_cusps,
        kp_sublords,
        navamsha_interpretation,
    )

    # Chapters 2 and 9: kutila/stationary veto, eclipse sandhi,
    # solar ingress, sunrise/sunset timing and karma-fixity evidence.
    reliability_audit = calculate_reliability_audit(
        request.std_time,
        request.location,
        planets,
        tier1_combinations,
        navamsha_interpretation,
        kp_sublords,
    )

    # Chapter 8 fixed marker stars. Book-stated rashi degrees only;
    # intentionally excluded from Navamsha.
    nakshatra_taras = calculate_nakshatra_taras(
        rashi_placidus,
        houses,
        planets,
    )

    # Chapter 7 exact nama-pada syllables. Participant matching is enabled
    # only when caller-confirmed opening sounds are supplied.
    navamsha_name_sounds = calculate_navamsha_name_sounds(
        request.participants,
        rashi_placidus,
        planets,
    )

    essential_results: list[dict[str, Any]] = [
        core["ayanamsa_degree"],
        core["lagna_sign"],
        core["moon_sign"],
        core["moon_nakshatra"],
        moon_consistency,
    ]

    for essential_house in ("House1", "House7"):
        house_result = houses.get(essential_house)

        if not house_result:
            essential_results.append({
                "status": "Fail",
                "required": True,
                "method": essential_house,
                "error": f"{essential_house} was not requested.",
            })
        elif house_result.get("status") != "Pass":
            essential_results.append({
                "status": "Fail",
                "required": True,
                "method": essential_house,
                "error": f"{essential_house} essential data failed.",
                "details": house_result,
            })

    for essential_planet in ("Sun", "Moon"):
        planet_result = planets.get(essential_planet)

        if not planet_result:
            essential_results.append({
                "status": "Fail",
                "required": True,
                "method": f"{essential_planet}PlanetData",
                "error": (
                    f"{essential_planet} was not requested in the planets list."
                ),
            })
        elif planet_result.get("status") != "Pass":
            essential_results.append({
                "status": "Fail",
                "required": True,
                "method": f"{essential_planet}PlanetData",
                "error": (
                    f"Essential {essential_planet} planetary data failed."
                ),
                "details": planet_result,
            })

    sun_moon_distinction = validate_sun_moon_distinction(planets)
    essential_results.append(sun_moon_distinction)
    core["sun_moon_distinction"] = sun_moon_distinction

    house1_sign = (
        extract_sign_name(houses["House1"]["sign"])
        if "House1" in houses and "sign" in houses["House1"]
        else None
    )
    house7_sign = (
        extract_sign_name(houses["House7"]["sign"])
        if "House7" in houses and "sign" in houses["House7"]
        else None
    )

    house_axis_distinction = {
        "status": (
            "Pass"
            if house1_sign and house7_sign and house1_sign != house7_sign
            else "Fail"
        ),
        "required": True,
        "method": "House1House7Distinction",
        "house1_sign": house1_sign,
        "house7_sign": house7_sign,
        "error": (
            None
            if house1_sign and house7_sign and house1_sign != house7_sign
            else "House1 and House7 signs are missing or duplicated."
        ),
    }
    essential_results.append(house_axis_distinction)
    core["house_axis_distinction"] = house_axis_distinction

    essential_failures = [
        result
        for result in essential_results
        if result.get("status") != "Pass"
    ]

    chart_validation_passed = not essential_failures
    strict_prediction_allowed = (
        chart_validation_passed
        and reliability_audit.get(
            "strict_prediction_allowed_by_reliability",
            False,
        )
    )

    response: dict[str, Any] = {
        "status": "Pass" if chart_validation_passed else "Fail",
        "strict_prediction_allowed": strict_prediction_allowed,
        "essential_failures": essential_failures,
        "event": {
            "event_id": request.event_id,
            "std_time": request.std_time,
            "location": request.location.model_dump(),
            "ayanamsa": "Lahiri",
            "participants": (
                request.participants.model_dump()
                if request.participants
                else None
            ),
        },
        "core": core,
        "rashi_placidus": rashi_placidus,
        "tier1_combinations": tier1_combinations,
        "planet_cusp_contacts": planet_cusp_contacts,
        "navamsha_cusps": navamsha_cusps,
        "kp_sublords": kp_sublords,
        "outer_planets": outer_planets,
        "special_points": special_points,
        "stolen_cusps": stolen_cusps,
        "navamsha_interpretation": navamsha_interpretation,
        "chart_correlation": chart_correlation,
        "reliability_audit": reliability_audit,
        "nakshatra_taras": nakshatra_taras,
        "navamsha_name_sounds": navamsha_name_sounds,
        "houses": houses,
        "planets": planets,
        "provenance": {
            "engine": "official VedAstro.Python",
            "proxy_version": PROXY_VERSION,
            "authentication": (
                "paid x-api-key header with APIKey body fallback"
            ),
            "planet_parameter_fix": (
                "Every planet request is sent as a nested Name object."
            ),
            "tier1_combinations": (
                "Chapter 3 victory houses, SKY/PKY, parivartana and "
                "relevant planetary war calculated from exact Lahiri D1 "
                "longitudes. Apparent magnitude for planetary war uses "
                "Swiss Ephemeris when available."
            ),
            "planet_cusp_contacts": (
                "Internally calculated from exact Lahiri Placidus cusps and "
                "requested VedAstro sidereal planet longitudes."
            ),
            "navamsha_cusps": (
                "Exact Chapter 5 D9 degrees derived from Lahiri D1 "
                "longitudes and cross-checked against available "
                "VedAstro D9 planet signs. Whole-house D9 sign "
                "cross-check is optional."
            ),
            "kp_sublords": (
                "Chapter 6 KP cusps and planet longitudes calculated "
                "with an isolated KRISHNAMURTI payload. Standard chart "
                "calls remain LAHIRI."
            ),
            "outer_planets": (
                "Uranus, Neptune, Pluto, Ceres and Chiron calculated "
                "locally with Swiss Ephemeris under Lahiri. This layer "
                "is non-essential and does not replace VedAstro."
            ),
            "special_points": (
                "Gulika and Upaketu exact Lahiri longitudes requested "
                "from official VedAstro server calculators. Geometry "
                "and book-supported contact labels are calculated locally."
            ),
            "stolen_cusps": (
                "Chapter 4 exact Placidus cusps compared with whole-sign "
                "rashi houses counted from the Lahiri Ascendant. Contacts "
                "use the proxy's existing visible/invisible body orbs."
            ),
            "navamsha_interpretation": (
                "Chapter 5 D9 1/7 cusp effects, House1/House7 combinations, "
                "D1/D9 hierarchy and double-whammy transfer calculated from "
                "the already verified Lahiri D1 and exact D9 geometry. "
                "Research-only contacts are excluded from automatic scoring; "
                "node-axis and overlapping-pair testimony is de-duplicated."
            ),
            "chart_correlation": (
                "Deterministic fine and slate-cluster signatures built from "
                "rashi cusps, D9 axis, KP sublords and active contacts. The "
                "batch caller counts matching signatures."
            ),
            "reliability_audit": (
                "Chapters 2 and 9 reliability gate using VedAstro motion "
                "labels plus Swiss Ephemeris speed-zero, eclipse and "
                "rise/set calculations. Karma fixity remains a transparent "
                "manual evidence audit because the book gives no complete "
                "mechanical classifier."
            ),
            "nakshatra_taras": (
                "Chapter 8 marker-star contacts calculated against exact "
                "Lahiri rashi cusps and house-lord longitudes using the "
                "book's stated sidereal degrees and strict one-degree orb."
            ),
            "navamsha_name_sounds": (
                "Chapter 7 Table 7.1 syllables calculated from exact "
                "Lahiri D1 longitudes. Participant matching uses only "
                "caller-confirmed opening sounds; raw names are not "
                "silently converted into Sanskrit pronunciation."
            ),
            "vedastro_api_key": "stored only on Render",
            "minimum_call_interval_seconds": VEDASTRO_MIN_INTERVAL_SECONDS,
            "maximum_attempts_per_method": VEDASTRO_MAX_RETRIES,
            "parallel_workers": VEDASTRO_MAX_WORKERS,
        },
    }

    # Calculations and strict validation are complete. Build a bounded
    # prediction-grade transport response for the Custom GPT Action.
    return compact_action_response(response)


# ============================================================
# CUSTOM GPT ACTION ROUTES
# ============================================================

@app.post("/event-chart")
def event_chart(
    request: EventChartInput,
    x_proxy_key: str | None = Header(
        default=None,
        alias="x-proxy-key",
    ),
) -> dict[str, Any]:
    verify_proxy_key(x_proxy_key)
    return calculate_event_chart(request)


@app.post("/v1/event-chart")
def event_chart_v1(
    request: EventChartInput,
    x_proxy_key: str | None = Header(
        default=None,
        alias="x-proxy-key",
    ),
) -> dict[str, Any]:
    verify_proxy_key(x_proxy_key)
    return calculate_event_chart(request)
