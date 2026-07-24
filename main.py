from __future__ import annotations

import json
import os
import threading
import time
import re
import unicodedata
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from enum import Enum
from typing import Any, Callable

try:
    import swisseph as swe
except ImportError:
    swe = None

import requests
from fastapi import FastAPI, Header, HTTPException
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

PROXY_VERSION = "1.13.1"


# ============================================================
# ENVIRONMENT SETTINGS
# ============================================================

VEDASTRO_API_KEY = os.getenv("VEDASTRO_API_KEY", "").strip()
PROXY_API_KEY = os.getenv("PROXY_API_KEY", "").strip()

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
    "chapter_opening": 182,
    "table_7_1": 184,
    "main_house10_rule": 185,
    "third_tier_points_example": 187,
    "planet_resonance": 189,
    "sun_research_rule": 190,
    "compound_name_rule": 191,
    "diphthong_rule": 192,
    "nasal_guidance": 194,
    "summary": 198,
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
                "prediction-grade compact; calculations unchanged"
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

    if len(encoded) > ACTION_RESPONSE_TARGET_CHARACTERS:
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

    if len(encoded) > ACTION_RESPONSE_TARGET_CHARACTERS:
        compacted = emergency_action_response(
            compacted
        )

    encoded = json.dumps(
        compacted,
        ensure_ascii=False,
        default=str,
    )

    if len(encoded) > ACTION_RESPONSE_TARGET_CHARACTERS:
        # Bound text and list lengths after the decision-bearing projection.
        compacted = compact_recursive(
            compacted,
            list_limit=10,
            string_limit=140,
        )
        compacted["response_compaction"] = {
            "applied": True,
            "profile": "action-compact-v1-emergency-bounded",
            "target_characters": (
                ACTION_RESPONSE_TARGET_CHARACTERS
            ),
            "full_calculation_performed": True,
            "emergency_bounded_pass": True,
        }

    final_encoded = json.dumps(
        compacted,
        ensure_ascii=False,
        default=str,
    )
    compacted["response_compaction"][
        "final_character_count"
    ] = len(final_encoded)

    return compacted


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
        "ayanamsa": "Lahiri",
        "engine": "VedAstro.Python",
        "planet_parameter_shape": "nested PlanetName object",
        "vedastro_authentication": (
            "x-api-key header with APIKey body fallback"
        ),
        "response_mode": "prediction-grade compact v1",
        "action_response_target_characters": (
            ACTION_RESPONSE_TARGET_CHARACTERS
        ),
        "advanced_layers": {
            "exact_lahiri_placidus_cusps": True,
            "planet_cusp_contacts": True,
            "navamsha_cusps": True,
            "kp_sublords": True,
            "outer_planets": SWISSEPH_AVAILABLE,
            "gulika_upaketu": True,
            "nakshatra_taras": True,
            "navamsha_name_sounds": True,
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

    strict_prediction_allowed = not essential_failures

    response: dict[str, Any] = {
        "status": "Pass" if strict_prediction_allowed else "Fail",
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
        "planet_cusp_contacts": planet_cusp_contacts,
        "navamsha_cusps": navamsha_cusps,
        "kp_sublords": kp_sublords,
        "outer_planets": outer_planets,
        "special_points": special_points,
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
