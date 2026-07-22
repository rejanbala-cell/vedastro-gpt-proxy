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
# SETTINGS
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
        "1800",
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
# CONFIGURE OFFICIAL VEDASTRO CLIENT
# ============================================================

Calculate.SetAPIKey(
    VEDASTRO_API_KEY
)


# Some VedAstro.Python versions contain SetAyanamsa.
# The current generated client may not contain it.
if hasattr(
    Calculate,
    "SetAyanamsa",
):

    Calculate.SetAyanamsa(
        Ayanamsa.Lahiri
    )

else:

    original_make_request = (
        Calculate._make_request.__func__
    )

    def make_request_with_lahiri(
        cls,
        endpoint,
        params,
    ):
        payload = dict(params)

        # Force Lahiri for every calculation.
        payload["Ayanamsa"] = "LAHIRI"

        return original_make_request(
            cls,
            endpoint,
            payload,
        )

    Calculate._make_request = classmethod(
        make_request_with_lahiri
    )


# ============================================================
# FASTAPI
# ============================================================

app = FastAPI(
    title="VedAstro GPT Proxy",
    version="1.3.0",
    description=(
        "Compact VedAstro event-chart proxy "
        "using the official VedAstro.Python client."
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
# INPUT MODELS
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
            "Exact stadium-local time in "
            "HH:MM DD/MM/YYYY +HH:MM format"
        ),
        examples=[
            "12:00 22/07/2026 +10:00"
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
# SERIALISE VEDASTRO RESULTS
# ============================================================

def json_safe(
    value: Any,
    depth: int = 0,
) -> Any:

    if depth > 12:
        return str(value)[:500]

    if value is None:
        return None

    if isinstance(
        value,
        (str, int, float, bool),
    ):
        if isinstance(value, str):
            return value[:1000]

        return value

    if isinstance(value, Enum):
        return value.value

    if isinstance(value, dict):
        return {
            str(key): json_safe(
                item,
                depth + 1,
            )
            for key, item in value.items()
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
            for item in list(value)
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
            for key, item in vars(value).items()
            if not str(key).startswith("_")
        }

    return str(value)[:1000]


IMPORTANT_WORDS = (
    "name",
    "planet",
    "house",
    "sign",
    "rasi",
    "navamsha",
    "d1",
    "d9",
    "lord",
    "longitude",
    "degree",
    "motion",
    "retro",
    "combust",
    "exalt",
    "debil",
    "own",
    "moola",
    "shadbala",
    "strength",
    "aspect",
    "constellation",
    "nakshatra",
    "pada",
    "tithi",
    "yoga",
    "karana",
    "hora",
    "weekday",
    "ayanamsa",
    "benefic",
    "malefic",
)


def compact_data(
    value: Any,
    depth: int = 0,
) -> Any:

    value = json_safe(value)

    if depth > 8:
        return None

    if value is None:
        return None

    if isinstance(
        value,
        (str, int, float, bool),
    ):
        return value

    if isinstance(value, list):

        compacted_list = []

        for item in value[:40]:

            compacted_item = compact_data(
                item,
                depth + 1,
            )

            if compacted_item not in (
                None,
                {},
                [],
            ):
                compacted_list.append(
                    compacted_item
                )

            if len(compacted_list) >= 20:
                break

        return compacted_list

    if isinstance(value, dict):

        result = {}

        for key, child in value.items():

            key_text = str(key).lower()

            keep_key = (
                depth <= 1
                or any(
                    word in key_text
                    for word in IMPORTANT_WORDS
                )
            )

            if not keep_key:

                try:
                    child_text = json.dumps(
                        child,
                        ensure_ascii=False,
                        default=str,
                    ).lower()

                    keep_key = any(
                        word in child_text
                        for word in IMPORTANT_WORDS
                    )

                except Exception:
                    keep_key = False

            if not keep_key:
                continue

            compacted_child = compact_data(
                child,
                depth + 1,
            )

            if compacted_child not in (
                None,
                {},
                [],
            ):
                result[str(key)] = (
                    compacted_child
                )

            if len(result) >= 35:
                result["_limited"] = True
                break

        if not result:

            for key, child in list(
                value.items()
            )[:5]:

                result[str(key)] = compact_data(
                    child,
                    depth + 1,
                )

        return result

    return str(value)[:1000]


def limit_result_size(
    value: Any,
    character_limit: int,
) -> Any:

    try:
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            default=str,
        )
    except Exception:
        encoded = str(value)

    if len(encoded) <= character_limit:
        return value

    return {
        "response_compacted": True,
        "preview": encoded[:character_limit],
    }


# ============================================================
# API RATE LIMITING
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


def is_retryable(
    message: str,
) -> bool:

    message = message.lower()

    retryable_messages = (
        "access denied",
        "too many",
        "rate limit",
        "timeout",
        "timed out",
        "temporarily",
        "connection",
        "429",
        "502",
        "503",
        "504",
    )

    return any(
        text in message
        for text in retryable_messages
    )


# ============================================================
# VEDASTRO CALL HELPERS
# ============================================================

def find_method(
    method_names: str | list[str],
) -> tuple[str | None, Callable[..., Any] | None]:

    if isinstance(
        method_names,
        str,
    ):
        possible_names = [
            method_names
        ]
    else:
        possible_names = method_names

    for method_name in possible_names:

        if hasattr(
            Calculate,
            method_name,
        ):
            return (
                method_name,
                getattr(
                    Calculate,
                    method_name,
                ),
            )

    return None, None


def vedastro_call(
    method_names: str | list[str],
    *args: Any,
) -> dict[str, Any]:

    selected_name, method = find_method(
        method_names
    )

    if isinstance(
        method_names,
        str,
    ):
        possible_names = [
            method_names
        ]
    else:
        possible_names = method_names

    if method is None:

        return {
            "status": "Fail",
            "method": possible_names[0],
            "error": (
                "Method unavailable. "
                f"Tried: {possible_names}"
            ),
        }

    final_error = ""

    for attempt in range(
        1,
        VEDASTRO_MAX_RETRIES + 1,
    ):

        try:

            wait_for_call_slot()

            raw_result = method(*args)

            compact_result = compact_data(
                raw_result
            )

            compact_result = limit_result_size(
                compact_result,
                MAX_RESULT_CHARACTERS,
            )

            return {
                "status": "Pass",
                "method": selected_name,
                "attempt": attempt,
                "data": compact_result,
            }

        except Exception as error:

            final_error = str(error)

            if (
                attempt
                >= VEDASTRO_MAX_RETRIES
            ):
                break

            if not is_retryable(
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
        "method": selected_name,
        "attempts": (
            VEDASTRO_MAX_RETRIES
        ),
        "error": (
            final_error
            or "Unknown VedAstro error"
        ),
    }


def calculate_lagna(
    event_time: Time,
) -> dict[str, Any]:

    # Some versions expose LagnaSignName(time).
    if hasattr(
        Calculate,
        "LagnaSignName",
    ):
        return vedastro_call(
            "LagnaSignName",
            event_time,
        )

    # Current fallback:
    # House1 sign is the Lagna sign.
    return vedastro_call(
        "HouseSignName",
        HouseName.House1,
        event_time,
    )


def calculate_moon_sign(
    event_time: Time,
) -> dict[str, Any]:

    # Older or alternate package versions.
    if hasattr(
        Calculate,
        "PlanetSignName",
    ):
        return vedastro_call(
            "PlanetSignName",
            PlanetName.Moon,
            event_time,
        )

    # Current generated package method.
    return vedastro_call(
        "PlanetRasiD1Sign",
        PlanetName.Moon,
        event_time,
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
        "version": "1.3.0",
        "health": "/health",
    }


@app.get("/health")
def health() -> dict[str, Any]:

    return {
        "status": "ok",
        "service": "VedAstro GPT Proxy",
        "version": "1.3.0",
        "vedastro_key_configured": bool(
            VEDASTRO_API_KEY
        ),
        "proxy_key_configured": bool(
            PROXY_API_KEY
        ),
        "ayanamsa": "Lahiri",
        "engine": "VedAstro.Python",
        "response_mode": "compact",
        "planet_sign_method": (
            "PlanetSignName"
            if hasattr(
                Calculate,
                "PlanetSignName",
            )
            else "PlanetRasiD1Sign"
        ),
    }


# ============================================================
# EVENT CHART CALCULATION
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
    # HOUSE DATA
    # --------------------------------------------------------

    houses: dict[str, Any] = {}

    for house_name in request.houses:

        house = HOUSES[
            house_name
        ]

        if hasattr(
            Calculate,
            "AllHouseData",
        ):

            houses[house_name] = (
                vedastro_call(
                    "AllHouseData",
                    house,
                    event_time,
                )
            )

        else:

            houses[house_name] = {

                "sign": vedastro_call(
                    "HouseSignName",
                    house,
                    event_time,
                ),

                "lord": vedastro_call(
                    "LordOfHouse",
                    house,
                    event_time,
                ),

                "constellation": (
                    vedastro_call(
                        "HouseConstellation",
                        house,
                        event_time,
                    )
                ),

                "aspects": vedastro_call(
                    "PlanetsAspectingHouse",
                    house,
                    event_time,
                ),
            }


    # --------------------------------------------------------
    # PLANET DATA
    # --------------------------------------------------------

    planets: dict[str, Any] = {}

    for planet_name in request.planets:

        planet = PLANETS[
            planet_name
        ]

        if hasattr(
            Calculate,
            "AllPlanetData",
        ):

            planets[planet_name] = (
                vedastro_call(
                    "AllPlanetData",
                    planet,
                    event_time,
                )
            )

        else:

            planets[planet_name] = {

                "d1_sign": vedastro_call(
                    "PlanetRasiD1Sign",
                    planet,
                    event_time,
                ),

                "d9_sign": vedastro_call(
                    "PlanetNavamshaD9Sign",
                    planet,
                    event_time,
                ),

                "sidereal_longitude": (
                    vedastro_call(
                        "PlanetNirayanaLongitude",
                        planet,
                        event_time,
                    )
                ),

                "motion": vedastro_call(
                    "PlanetMotionName",
                    planet,
                    event_time,
                ),

                "retrograde": vedastro_call(
                    "IsPlanetRetrograde",
                    planet,
                    event_time,
                ),

                "combust": vedastro_call(
                    "IsPlanetCombust",
                    planet,
                    event_time,
                ),
            }


    # --------------------------------------------------------
    # ESSENTIAL VALIDATION
    # --------------------------------------------------------

    essential_results = [

        core["ayanamsa_degree"],

        core["lagna_sign"],

        core["moon_sign"],

        core["moon_nakshatra"],

        houses.get(
            "House1",
            {"status": "Fail"},
        ),

        houses.get(
            "House7",
            {"status": "Fail"},
        ),

        planets.get(
            "Moon",
            {"status": "Fail"},
        ),
    ]


    essential_failures = [
        result
        for result in essential_results
        if result.get("status") != "Pass"
    ]


    strict_prediction_allowed = (
        len(essential_failures) == 0
    )


    response = {

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

            "proxy_version": "1.3.0",

            "response_mode": "compact",

            "vedastro_api_key": (
                "stored only on Render"
            ),
        },
    }


    # --------------------------------------------------------
    # FINAL RESPONSE SIZE PROTECTION
    # --------------------------------------------------------

    encoded_response = json.dumps(
        response,
        ensure_ascii=False,
        default=str,
    )

    if len(encoded_response) > 60000:

        for section_name in (
            "houses",
            "planets",
        ):

            for item in response[
                section_name
            ].values():

                if (
                    isinstance(item, dict)
                    and "data" in item
                ):

                    item["data"] = (
                        limit_result_size(
                            item["data"],
                            700,
                        )
                    )

        response[
            "response_was_further_compacted"
        ] = True


    return response


# ============================================================
# CUSTOM GPT ACTION ENDPOINTS
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
