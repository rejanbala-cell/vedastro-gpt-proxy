from __future__ import annotations

import os
import threading
import time
from enum import Enum
from typing import Any

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
# ENVIRONMENT VARIABLES
# ============================================================

VEDASTRO_API_KEY = os.getenv("VEDASTRO_API_KEY", "").strip()
PROXY_API_KEY = os.getenv("PROXY_API_KEY", "").strip()

# Premium key: 0.5
# Free key: approximately 12.5
MIN_INTERVAL_SECONDS = float(
    os.getenv("VEDASTRO_MIN_INTERVAL_SECONDS", "0.5")
)

MAX_RETRIES = int(
    os.getenv("VEDASTRO_MAX_RETRIES", "4")
)


# ============================================================
# CONFIGURE VEDASTRO
# ============================================================

if VEDASTRO_API_KEY:
    Calculate.SetAPIKey(VEDASTRO_API_KEY)


# Some VedAstro.Python versions contain SetAyanamsa().
# The current generated Calculate class may not contain it.
# This supports both versions.
if hasattr(Calculate, "SetAyanamsa"):

    Calculate.SetAyanamsa(Ayanamsa.Lahiri)

else:

    original_make_request = Calculate._make_request.__func__

    def make_request_with_lahiri(cls, endpoint, params):
        payload = dict(params)

        # Force Lahiri on every VedAstro request.
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
    version="1.1.0",
    description=(
        "Private VedAstro proxy using the official "
        "VedAstro.Python client and Lahiri ayanamsa."
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
    "House3",
    "House4",
    "House5",
    "House6",
    "House7",
    "House9",
    "House10",
    "House11",
    "House12",
]


DEFAULT_PLANETS = [
    "Sun",
    "Moon",
    "Mars",
    "Mercury",
    "Jupiter",
    "Venus",
    "Saturn",
    "Rahu",
    "Ketu",
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

    # Required format:
    # HH:MM DD/MM/YYYY +HH:MM
    std_time: str = Field(
        examples=[
            "19:30 22/07/2026 +10:00"
        ]
    )

    location: LocationInput

    houses: list[str] = Field(
        default_factory=lambda: DEFAULT_HOUSES.copy()
    )

    planets: list[str] = Field(
        default_factory=lambda: DEFAULT_PLANETS.copy()
    )


# ============================================================
# JSON SERIALISATION
# ============================================================

def serialise(
    value: Any,
    depth: int = 0,
) -> Any:

    if depth > 12:
        return str(value)

    if value is None:
        return None

    if isinstance(
        value,
        (str, int, float, bool),
    ):
        return value

    if isinstance(value, Enum):
        return value.value

    if isinstance(value, dict):
        return {
            str(key): serialise(
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
            serialise(
                item,
                depth + 1,
            )
            for item in value
        ]

    if hasattr(value, "to_json"):
        try:
            return serialise(
                value.to_json(),
                depth + 1,
            )
        except Exception:
            pass

    if hasattr(value, "__dict__"):
        return {
            str(key): serialise(
                item,
                depth + 1,
            )
            for key, item in vars(value).items()
            if not str(key).startswith("_")
        }

    return str(value)


# ============================================================
# RATE LIMITING AND RETRIES
# ============================================================

call_lock = threading.Lock()
last_call_time = 0.0


def wait_before_call() -> None:

    global last_call_time

    with call_lock:

        elapsed = (
            time.monotonic()
            - last_call_time
        )

        remaining = (
            MIN_INTERVAL_SECONDS
            - elapsed
        )

        if remaining > 0:
            time.sleep(remaining)

        last_call_time = time.monotonic()


def retryable_error(
    error_message: str,
) -> bool:

    text = error_message.lower()

    retryable_terms = [
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
    ]

    return any(
        term in text
        for term in retryable_terms
    )


def vedastro_call(
    method_names: str | list[str],
    *args: Any,
) -> dict[str, Any]:

    if isinstance(method_names, str):
        possible_names = [method_names]
    else:
        possible_names = method_names

    selected_method_name = None

    for method_name in possible_names:

        if hasattr(
            Calculate,
            method_name,
        ):
            selected_method_name = method_name
            break

    if selected_method_name is None:

        return {
            "status": "Fail",
            "method": possible_names[0],
            "error": (
                "Method is unavailable. "
                f"Tried: {possible_names}"
            ),
        }

    method = getattr(
        Calculate,
        selected_method_name,
    )

    final_error = ""

    for attempt in range(
        1,
        MAX_RETRIES + 1,
    ):

        try:

            wait_before_call()

            result = method(*args)

            return {
                "status": "Pass",
                "method": selected_method_name,
                "attempt": attempt,
                "data": serialise(result),
            }

        except Exception as error:

            final_error = str(error)

            if attempt >= MAX_RETRIES:
                break

            if not retryable_error(
                final_error
            ):
                break

            wait_seconds = min(
                2 ** attempt,
                10,
            )

            time.sleep(wait_seconds)

    return {
        "status": "Fail",
        "method": selected_method_name,
        "attempts": MAX_RETRIES,
        "error": (
            final_error
            or "Unknown VedAstro error"
        ),
    }


# ============================================================
# PROXY AUTHENTICATION
# ============================================================

def verify_proxy_key(
    supplied_key: str | None,
) -> None:

    if not PROXY_API_KEY:

        raise HTTPException(
            status_code=500,
            detail=(
                "PROXY_API_KEY is missing "
                "from Render environment variables."
            ),
        )

    if supplied_key != PROXY_API_KEY:

        raise HTTPException(
            status_code=401,
            detail="Invalid proxy API key.",
        )


# ============================================================
# BASIC ROUTES
# ============================================================

@app.get("/")
def root() -> dict[str, Any]:

    return {
        "status": "ok",
        "service": "VedAstro GPT Proxy",
        "version": "1.1.0",
        "health": "/health",
    }


@app.get("/health")
def health() -> dict[str, Any]:

    return {
        "status": "ok",
        "service": "VedAstro GPT Proxy",
        "version": "1.1.0",
        "vedastro_key_configured": bool(
            VEDASTRO_API_KEY
        ),
        "proxy_key_configured": bool(
            PROXY_API_KEY
        ),
        "ayanamsa": "Lahiri",
        "engine": "VedAstro.Python",
    }


# ============================================================
# EVENT CHART CALCULATION
# ============================================================

def calculate_event_chart(
    request: EventChartInput,
) -> dict[str, Any]:

    if not VEDASTRO_API_KEY:

        raise HTTPException(
            status_code=500,
            detail=(
                "VEDASTRO_API_KEY is missing "
                "from Render environment variables."
            ),
        )

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
    # CORE EVENT DATA
    # --------------------------------------------------------

    core = {

        "ayanamsa_degree": vedastro_call(
            "AyanamsaDegree",
            event_time,
        ),

        "lagna_sign": vedastro_call(
            "HouseSignName",
            HouseName.House1,
            event_time,
        ),

        "moon_sign": vedastro_call(
            "PlanetSignName",
            PlanetName.Moon,
            event_time,
        ),

        "moon_nakshatra": vedastro_call(
            "MoonConstellation",
            event_time,
        ),

        "tithi": vedastro_call(
            "LunarDay",
            event_time,
        ),

        "yoga": vedastro_call(
            [
                "Yoga",
                "NithyaYoga",
            ],
            event_time,
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

        houses[house_name] = vedastro_call(
            "AllHouseData",
            HOUSES[house_name],
            event_time,
        )


    # --------------------------------------------------------
    # PLANET DATA
    # --------------------------------------------------------

    planets: dict[str, Any] = {}

    for planet_name in request.planets:

        planets[planet_name] = vedastro_call(
            "AllPlanetData",
            PLANETS[planet_name],
            event_time,
        )


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


    return {

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

            "proxy_version": "1.1.0",

            "vedastro_api_key": (
                "stored only on Render"
            ),
        },
    }


# ============================================================
# CUSTOM GPT ENDPOINTS
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


# Also supports /v1/event-chart.
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
