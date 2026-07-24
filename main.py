from __future__ import annotations

import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from enum import Enum
from typing import Any, Callable

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

PROXY_VERSION = "1.9.0"


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
        "Krishnamurti KP sublords and strict validation."
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


# ============================================================
# REQUEST MODELS
# ============================================================

class LocationInput(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    longitude: float = Field(ge=-180, le=180)
    latitude: float = Field(ge=-90, le=90)


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
        "response_mode": "compact direct calculations",
        "advanced_layers": {
            "exact_lahiri_placidus_cusps": True,
            "planet_cusp_contacts": True,
            "navamsha_cusps": True,
            "kp_sublords": True,
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
        },
        "core": core,
        "rashi_placidus": rashi_placidus,
        "planet_cusp_contacts": planet_cusp_contacts,
        "navamsha_cusps": navamsha_cusps,
        "kp_sublords": kp_sublords,
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
            "vedastro_api_key": "stored only on Render",
            "minimum_call_interval_seconds": VEDASTRO_MIN_INTERVAL_SECONDS,
            "maximum_attempts_per_method": VEDASTRO_MAX_RETRIES,
            "parallel_workers": VEDASTRO_MAX_WORKERS,
        },
    }

    encoded = json.dumps(
        response,
        ensure_ascii=False,
        default=str,
    )

    if len(encoded) > MAX_RESPONSE_CHARACTERS:
        for planet_result in response["planets"].values():
            for item in planet_result.values():
                if isinstance(item, dict) and "data" in item:
                    item["data"] = limit_data(item["data"], 250)

        for house_result in response["houses"].values():
            for item in house_result.values():
                if isinstance(item, dict) and "data" in item:
                    item["data"] = limit_data(item["data"], 250)

        response["response_was_further_compacted"] = True

    return response


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
