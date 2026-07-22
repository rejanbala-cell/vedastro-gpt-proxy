from __future__ import annotations

import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
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

PROXY_VERSION = "1.5.1"


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
    - Lahiri is forced on every calculation;
    - planet parameters use the nested {"Name": "Moon"} shape.
    """

    payload = {
        str(key): fix_api_value(str(key), value)
        for key, value in dict(params).items()
    }

    payload["Ayanamsa"] = "LAHIRI"

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
        "nested planet parameters and strict validation."
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
