from __future__ import annotations

import json
import os
import threading
import time
from enum import Enum
from typing import Any, Callable

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
# ENVIRONMENT SETTINGS
# ============================================================

VEDASTRO_API_KEY = os.getenv(
    "VEDASTRO_API_KEY",
    "",
).strip()

PROXY_API_KEY = os.getenv(
    "PROXY_API_KEY",
    "",
).strip()

VEDASTRO_MIN_INTERVAL_SECONDS = float(
    os.getenv(
        "VEDASTRO_MIN_INTERVAL_SECONDS",
        "0.5",
    )
)

VEDASTRO_MAX_RETRIES = int(
    os.getenv(
        "VEDASTRO_MAX_RETRIES",
        "4",
    )
)

MAX_RESULT_CHARACTERS = int(
    os.getenv(
        "MAX_RESULT_CHARACTERS",
        "700",
    )
)


if not VEDASTRO_API_KEY:
    raise RuntimeError(
        "VEDASTRO_API_KEY is missing from Render."
    )

if not PROXY_API_KEY:
    raise RuntimeError(
        "PROXY_API_KEY is missing from Render."
    )


# ============================================================
# CONFIGURE AND PATCH OFFICIAL VEDASTRO CLIENT
# ============================================================

Calculate.SetAPIKey(
    VEDASTRO_API_KEY
)


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


_original_make_request = (
    Calculate._make_request.__func__
)


def fix_api_value(
    key: str,
    value: Any,
) -> Any:
    """
    Fixes VedAstro.Python planet values.

    Generated client may send:

        "planetName": "Moon"

    Live VedAstro REST expects:

        "planetName": {
            "Name": "Moon"
        }
    """

    if isinstance(value, Enum):
        value = value.value

    if isinstance(value, dict):
        return {
            str(child_key): fix_api_value(
                str(child_key),
                child_value,
            )
            for child_key, child_value
            in value.items()
        }

    if isinstance(
        value,
        (list, tuple, set),
    ):
        return [
            fix_api_value(
                key,
                item,
            )
            for item in value
        ]

    if (
        isinstance(value, str)
        and "planet" in key.lower()
        and value in PLANET_LITERAL_NAMES
    ):
        return {
            "Name": value
        }

    return value


def make_request_fixed(
    cls,
    endpoint: str,
    params: dict[str, Any],
):
    payload = {
        str(key): fix_api_value(
            str(key),
            value,
        )
        for key, value
        in dict(params).items()
    }

    # Force Lahiri for every request.
    payload["Ayanamsa"] = "LAHIRI"

    # Original method adds APIKey into the JSON body.
    return _original_make_request(
        cls,
        endpoint,
        payload,
    )


Calculate._make_request = classmethod(
    make_request_fixed
)


# Support package versions that still have SetAyanamsa.
if hasattr(
    Calculate,
    "SetAyanamsa",
):
    try:
        Calculate.SetAyanamsa(
            Ayanamsa.Lahiri
        )
    except Exception:
        pass


# ============================================================
# FASTAPI
# ============================================================

app = FastAPI(
    title="VedAstro GPT Proxy",
    version="1.4.0",
    description=(
        "Compact Lahiri event-chart proxy using "
        "the official VedAstro.Python client with "
        "correct nested planet parameters."
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
    f"House{i}": getattr(
        HouseName,
        f"House{i}",
    )
    for i in range(1, 13)
}


DEFAULT_HOUSES = [
    "House1",
    "House7",
]


DEFAULT_PLANETS = [
    "Sun",
    "Moon",
]


# ============================================================
# REQUEST MODELS
# ============================================================

class LocationInput(BaseModel):

    name: str = Field(
        min_length=1,
        max_length=200,
    )

    longitude: float = Field(
        ge=-180,
        le=180,
    )

    latitude: float = Field(
        ge=-90,
        le=90,
    )


class EventChartInput(BaseModel):

    event_id: str | None = None

    std_time: str = Field(
        description=(
            "Exact local event time: "
            "HH:MM DD/MM/YYYY +HH:MM"
        ),
        examples=[
            "20:00 22/07/2026 +10:00"
        ],
    )

    location: LocationInput

    houses: list[str] = Field(
        default_factory=lambda: (
            DEFAULT_HOUSES.copy()
        )
    )

    planets: list[str] = Field(
        default_factory=lambda: (
            DEFAULT_PLANETS.copy()
        )
    )


# ============================================================
# JSON AND RESPONSE SIZE HELPERS
# ============================================================

def json_safe(
    value: Any,
    depth: int = 0,
) -> Any:

    if depth > 10:
        return str(value)[:500]

    if value is None:
        return None

    if isinstance(
        value,
        (int, float, bool),
    ):
        return value

    if isinstance(value, str):
        return value[:2000]

    if isinstance(value, Enum):
        return value.value

    if isinstance(value, dict):
        return {
            str(key): json_safe(
                item,
                depth + 1,
            )
            for key, item
            in value.items()
        }

    if isinstance(
        value,
        (list, tuple, set),
    ):
        return [
            json_safe(
                item,
                depth + 1,
            )
            for item in list(value)[:50]
        ]

    if hasattr(
        value,
        "to_json",
    ):
        try:
            return json_safe(
                value.to_json(),
                depth + 1,
            )
        except Exception:
            pass

    if hasattr(
        value,
        "__dict__",
    ):
        return {
            str(key): json_safe(
                item,
                depth + 1,
            )
            for key, item
            in vars(value).items()
            if not str(key).startswith("_")
        }

    return str(value)[:2000]


def limit_data(
    value: Any,
    maximum_characters: int | None = None,
) -> Any:

    limit = (
        maximum_characters
        or MAX_RESULT_CHARACTERS
    )

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
# RATE LIMITING AND RETRIES
# ============================================================

call_lock = threading.Lock()
last_call_time = 0.0


def wait_for_call_slot() -> None:

    global last_call_time

    with call_lock:

        elapsed = (
            time.monotonic()
            - last_call_time
        )

        remaining = (
            VEDASTRO_MIN_INTERVAL_SECONDS
            - elapsed
        )

        if remaining > 0:
            time.sleep(remaining)

        last_call_time = (
            time.monotonic()
        )


def is_retryable_error(
    message: str,
) -> bool:

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
            "429",
            "502",
            "503",
            "504",
        )
    )


def find_method(
    method_names: str | list[str],
) -> tuple[
    str | None,
    Callable[..., Any] | None,
]:

    names = (
        [method_names]
        if isinstance(
            method_names,
            str,
        )
        else method_names
    )

    for name in names:

        if hasattr(
            Calculate,
            name,
        ):
            return (
                name,
                getattr(
                    Calculate,
                    name,
                ),
            )

    return None, None


def vedastro_call(
    method_names: str | list[str],
    *args: Any,
    required: bool = False,
) -> dict[str, Any]:

    names = (
        [method_names]
        if isinstance(
            method_names,
            str,
        )
        else method_names
    )

    selected_name, method = find_method(
        names
    )

    if method is None:

        return {
            "status": "Fail",
            "required": required,
            "method": names[0],
            "error": (
                "Method unavailable. "
                f"Tried: {names}"
            ),
        }

    final_error = ""

    for attempt in range(
        1,
        VEDASTRO_MAX_RETRIES + 1,
    ):

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

            if (
                attempt
                >= VEDASTRO_MAX_RETRIES
            ):
                break

            if not is_retryable_error(
                final_error
            ):
                break

            time.sleep(
                min(
                    2 ** attempt,
                    10,
                )
            )

    return {
        "status": "Fail",
        "required": required,
        "method": selected_name,
        "attempts": (
            VEDASTRO_MAX_RETRIES
        ),
        "error": (
            final_error
            or "Unknown VedAstro error"
        ),
    }


# ============================================================
# RESULT PARSING
# ============================================================

def unwrap_data(
    result: dict[str, Any],
) -> Any:

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

                if (
                    actual_key.lower()
                    == key.lower()
                ):

                    if isinstance(
                        child,
                        (str, int, float),
                    ):
                        return str(child)

        for child in value.values():

            found = find_named_value(
                child,
                preferred_keys,
            )

            if found:
                return found

    if isinstance(value, list):

        for child in value:

            found = find_named_value(
                child,
                preferred_keys,
            )

            if found:
                return found

    return None


def extract_sign_name(
    result: dict[str, Any],
) -> str | None:

    data = unwrap_data(result)

    value = find_named_value(
        data,
        (
            "Name",
            "SignName",
            "ZodiacName",
        ),
    )

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

        if (
            sign.lower()
            in value.lower()
        ):
            return sign

    return None


def extract_nakshatra_name(
    result: dict[str, Any],
) -> str | None:

    data = unwrap_data(result)

    if isinstance(data, str):
        raw = data

    else:
        raw = find_named_value(
            data,
            (
                "Name",
                "ConstellationName",
                "NakshatraName",
            ),
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
        "visakha": "Vishakha",
        "anuradha": "Anuradha",
        "jyeshtha": "Jyeshtha",
        "jyeshta": "Jyeshtha",
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
# NAKSHATRA AND SIGN CONSISTENCY
# ============================================================

NAKSHATRA_ALLOWED_SIGNS = {
    "Ashwini": {
        "Aries"
    },
    "Bharani": {
        "Aries"
    },
    "Krittika": {
        "Aries",
        "Taurus",
    },
    "Rohini": {
        "Taurus"
    },
    "Mrigashirsha": {
        "Taurus",
        "Gemini",
    },
    "Ardra": {
        "Gemini"
    },
    "Punarvasu": {
        "Gemini",
        "Cancer",
    },
    "Pushya": {
        "Cancer"
    },
    "Ashlesha": {
        "Cancer"
    },
    "Magha": {
        "Leo"
    },
    "Purva Phalguni": {
        "Leo"
    },
    "Uttara Phalguni": {
        "Leo",
        "Virgo",
    },
    "Hasta": {
        "Virgo"
    },
    "Chitra": {
        "Virgo",
        "Libra",
    },
    "Swati": {
        "Libra"
    },
    "Vishakha": {
        "Libra",
        "Scorpio",
    },
    "Anuradha": {
        "Scorpio"
    },
    "Jyeshtha": {
        "Scorpio"
    },
    "Mula": {
        "Sagittarius"
    },
    "Purva Ashadha": {
        "Sagittarius"
    },
    "Uttara Ashadha": {
        "Sagittarius",
        "Capricorn",
    },
    "Shravana": {
        "Capricorn"
    },
    "Dhanishta": {
        "Capricorn",
        "Aquarius",
    },
    "Shatabhisha": {
        "Aquarius"
    },
    "Purva Bhadrapada": {
        "Aquarius",
        "Pisces",
    },
    "Uttara Bhadrapada": {
        "Pisces"
    },
    "Revati": {
        "Pisces"
    },
}


def validate_moon_consistency(
    moon_sign_result: dict[str, Any],
    moon_nakshatra_result: dict[str, Any],
) -> dict[str, Any]:

    moon_sign = extract_sign_name(
        moon_sign_result
    )

    nakshatra = extract_nakshatra_name(
        moon_nakshatra_result
    )

    if not moon_sign or not nakshatra:

        return {
            "status": "Fail",
            "required": True,
            "method": (
                "MoonSignNakshatraConsistency"
            ),
            "error": (
                "Could not parse Moon sign or "
                "Moon nakshatra."
            ),
            "moon_sign": moon_sign,
            "nakshatra": nakshatra,
        }

    allowed_signs = (
        NAKSHATRA_ALLOWED_SIGNS.get(
            nakshatra,
            set(),
        )
    )

    is_consistent = (
        moon_sign in allowed_signs
    )

    return {
        "status": (
            "Pass"
            if is_consistent
            else "Fail"
        ),
        "required": True,
        "method": (
            "MoonSignNakshatraConsistency"
        ),
        "moon_sign": moon_sign,
        "nakshatra": nakshatra,
        "allowed_signs": sorted(
            allowed_signs
        ),
        "error": (
            None
            if is_consistent
            else (
                f"Moon sign {moon_sign} is "
                f"inconsistent with nakshatra "
                f"{nakshatra}."
            )
        ),
    }


# ============================================================
# CHART COMPONENTS
# ============================================================

def calculate_lagna(
    event_time: Time,
) -> dict[str, Any]:

    if hasattr(
        Calculate,
        "LagnaSignName",
    ):

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


def calculate_moon_sign(
    event_time: Time,
) -> dict[str, Any]:

    return vedastro_call(
        [
            "PlanetRasiD1Sign",
            "PlanetSignName",
        ],
        PlanetName.Moon,
        event_time,
        required=True,
    )


def calculate_yoga(
    event_time: Time,
) -> dict[str, Any]:

    return vedastro_call(
        [
            "NithyaYoga",
            "Yoga",
        ],
        event_time,
    )


def calculate_house(
    house_name: str,
    event_time: Time,
) -> dict[str, Any]:

    house = HOUSES[
        house_name
    ]

    is_essential = (
        house_name
        in {
            "House1",
            "House7",
        }
    )

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

    required_results = [
        sign,
        lord,
    ]

    passed = all(
        result["status"] == "Pass"
        for result in required_results
    )

    return {
        "status": (
            "Pass"
            if passed
            else "Fail"
        ),
        "house": house_name,
        "sign": sign,
        "lord": lord,
        "constellation": constellation,
        "constellation_lord": (
            constellation_lord
        ),
        "aspects": aspects,
    }


def calculate_planet(
    planet_name: str,
    event_time: Time,
) -> dict[str, Any]:

    planet = PLANETS[
        planet_name
    ]

    is_moon = (
        planet_name == "Moon"
    )

    d1_sign = vedastro_call(
        [
            "PlanetRasiD1Sign",
            "PlanetSignName",
        ],
        planet,
        event_time,
        required=is_moon,
    )

    d9_sign = vedastro_call(
        [
            "PlanetNavamshaD9Sign",
            "PlanetNavamshaSign",
        ],
        planet,
        event_time,
        required=is_moon,
    )

    longitude = vedastro_call(
        "PlanetNirayanaLongitude",
        planet,
        event_time,
        required=is_moon,
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

    required_results = [
        d1_sign,
        d9_sign,
        longitude,
    ]

    passed = all(
        result["status"] == "Pass"
        for result in required_results
    )

    return {
        "status": (
            "Pass"
            if passed
            else "Fail"
        ),
        "requested_planet": planet_name,
        "request_shape": {
            "PlanetName": {
                "Name": planet_name
            }
        },
        "d1_sign": d1_sign,
        "d9_sign": d9_sign,
        "sidereal_longitude": longitude,
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

def verify_proxy_key(
    supplied_key: str | None,
) -> None:

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
        "version": "1.4.0",
        "health": "/health",
    }


@app.get("/health")
def health() -> dict[str, Any]:

    return {
        "status": "ok",
        "service": "VedAstro GPT Proxy",
        "version": "1.4.0",
        "vedastro_key_configured": bool(
            VEDASTRO_API_KEY
        ),
        "proxy_key_configured": bool(
            PROXY_API_KEY
        ),
        "ayanamsa": "Lahiri",
        "engine": "VedAstro.Python",
        "planet_parameter_shape": (
            "nested PlanetName object"
        ),
        "response_mode": (
            "compact direct calculations"
        ),
    }


# ============================================================
# EVENT CHART
# ============================================================

def calculate_event_chart(
    request: EventChartInput,
) -> dict[str, Any]:

    invalid_houses = [
        house
        for house in request.houses
        if house not in HOUSES
    ]

    if invalid_houses:

        raise HTTPException(
            status_code=422,
            detail=(
                f"Invalid houses: "
                f"{invalid_houses}"
            ),
        )

    invalid_planets = [
        planet
        for planet in request.planets
        if planet not in PLANETS
    ]

    if invalid_planets:

        raise HTTPException(
            status_code=422,
            detail=(
                f"Invalid planets: "
                f"{invalid_planets}"
            ),
        )

    try:

        location = GeoLocation(
            request.location.name,
            request.location.longitude,
            request.location.latitude,
        )

        event_time = Time(
            request.std_time,
            location,
        )

    except Exception as error:

        raise HTTPException(
            status_code=422,
            detail=(
                "Invalid time or location: "
                f"{error}"
            ),
        ) from error


    # --------------------------------------------------------
    # CORE DATA
    # --------------------------------------------------------

    core = {

        "ayanamsa_degree": vedastro_call(
            "AyanamsaDegree",
            event_time,
            required=True,
        ),

        "lagna_sign": calculate_lagna(
            event_time
        ),

        "moon_sign": calculate_moon_sign(
            event_time
        ),

        "moon_nakshatra": vedastro_call(
            "MoonConstellation",
            event_time,
            required=True,
        ),

        "tithi": vedastro_call(
            "LunarDay",
            event_time,
        ),

        "yoga": calculate_yoga(
            event_time
        ),

        "karana": vedastro_call(
            "Karana",
            event_time,
        ),

        "weekday": vedastro_call(
            "DayOfWeek",
            event_time,
        ),

        "hora_lord": vedastro_call(
            "LordOfHoraFromTime",
            event_time,
        ),
    }


    # --------------------------------------------------------
    # MOON CONSISTENCY
    # --------------------------------------------------------

    moon_consistency = (
        validate_moon_consistency(
            core["moon_sign"],
            core["moon_nakshatra"],
        )
    )

    core["moon_consistency"] = (
        moon_consistency
    )


    # --------------------------------------------------------
    # HOUSE DATA
    # --------------------------------------------------------

    houses = {
        house_name: calculate_house(
            house_name,
            event_time,
        )
        for house_name in dict.fromkeys(
            request.houses
        )
    }


    # --------------------------------------------------------
    # PLANET DATA
    # --------------------------------------------------------

    planets = {
        planet_name: calculate_planet(
            planet_name,
            event_time,
        )
        for planet_name in dict.fromkeys(
            request.planets
        )
    }


    # --------------------------------------------------------
    # ESSENTIAL VALIDATION
    # --------------------------------------------------------

    essential_results: list[
        dict[str, Any]
    ] = [

        core["ayanamsa_degree"],

        core["lagna_sign"],

        core["moon_sign"],

        core["moon_nakshatra"],

        moon_consistency,
    ]


    for essential_house in (
        "House1",
        "House7",
    ):

        house_result = houses.get(
            essential_house
        )

        if not house_result:

            essential_results.append({
                "status": "Fail",
                "required": True,
                "method": essential_house,
                "error": (
                    f"{essential_house} "
                    "was not requested."
                ),
            })

        elif (
            house_result["status"]
            != "Pass"
        ):

            essential_results.append({
                "status": "Fail",
                "required": True,
                "method": essential_house,
                "error": (
                    f"{essential_house} "
                    "essential data failed."
                ),
                "details": house_result,
            })


    moon_planet_result = planets.get(
        "Moon"
    )


    if not moon_planet_result:

        essential_results.append({
            "status": "Fail",
            "required": True,
            "method": "MoonPlanetData",
            "error": (
                "Moon was not requested "
                "in the planets list."
            ),
        })

    elif (
        moon_planet_result["status"]
        != "Pass"
    ):

        essential_results.append({
            "status": "Fail",
            "required": True,
            "method": "MoonPlanetData",
            "error": (
                "Essential Moon planetary "
                "data failed."
            ),
            "details": moon_planet_result,
        })


    essential_failures = [
        result
        for result in essential_results
        if result.get("status") != "Pass"
    ]


    strict_prediction_allowed = (
        not essential_failures
    )


    response: dict[str, Any] = {

        "status": (
            "Pass"
            if strict_prediction_allowed
            else "Fail"
        ),

        "strict_prediction_allowed": (
            strict_prediction_allowed
        ),

        "essential_failures": (
            essential_failures
        ),

        "event": {

            "event_id": request.event_id,

            "std_time": request.std_time,

            "location": (
                request.location.model_dump()
            ),

            "ayanamsa": "Lahiri",
        },

        "core": core,

        "houses": houses,

        "planets": planets,

        "provenance": {

            "engine": (
                "official VedAstro.Python"
            ),

            "proxy_version": "1.4.0",

            "planet_parameter_fix": (
                "Every planet request is sent "
                "as a nested Name object."
            ),

            "vedastro_api_key": (
                "stored only on Render"
            ),
        },
    }


    # --------------------------------------------------------
    # EMERGENCY RESPONSE SIZE PROTECTION
    # --------------------------------------------------------

    encoded = json.dumps(
        response,
        ensure_ascii=False,
        default=str,
    )


    if len(encoded) > 70000:

        for planet_result in response[
            "planets"
        ].values():

            for key, item in (
                planet_result.items()
            ):

                if (
                    isinstance(item, dict)
                    and "data" in item
                ):

                    item["data"] = (
                        limit_data(
                            item["data"],
                            250,
                        )
                    )


        for house_result in response[
            "houses"
        ].values():

            for key, item in (
                house_result.items()
            ):

                if (
                    isinstance(item, dict)
                    and "data" in item
                ):

                    item["data"] = (
                        limit_data(
                            item["data"],
                            250,
                        )
                    )


        response[
            "response_was_further_compacted"
        ] = True


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

    verify_proxy_key(
        x_proxy_key
    )

    return calculate_event_chart(
        request
    )


@app.post("/v1/event-chart")
def event_chart_v1(
    request: EventChartInput,

    x_proxy_key: str | None = Header(
        default=None,
        alias="x-proxy-key",
    ),

) -> dict[str, Any]:

    verify_proxy_key(
        x_proxy_key
    )

    return calculate_event_chart(
        request
    )
