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

# These values will be stored securely on Render.
VEDASTRO_API_KEY = os.environ["VEDASTRO_API_KEY"]
PROXY_API_KEY = os.environ["PROXY_API_KEY"]

MIN_INTERVAL_SECONDS = float(
    os.getenv("VEDASTRO_MIN_INTERVAL_SECONDS", "0.75")
)
MAX_RETRIES = int(os.getenv("VEDASTRO_MAX_RETRIES", "4"))

# Use the official VedAstro Python client.
Calculate.SetAPIKey(VEDASTRO_API_KEY)
Calculate.SetAyanamsa(Ayanamsa.Lahiri)

app = FastAPI(
    title="VedAstro GPT Proxy",
    version="1.0.0",
)

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

DEFAULT_HOUSES = [
    "House1",
    "House3",
    "House6",
    "House10",
    "House11",
    "House4",
    "House5",
    "House7",
    "House9",
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

_call_lock = threading.Lock()
_last_call_time = 0.0


class LocationInput(BaseModel):
    name: str
    longitude: float = Field(ge=-180, le=180)
    latitude: float = Field(ge=-90, le=90)


class EventChartInput(BaseModel):
    event_id: str | None = None
    std_time: str = Field(
        description="Exact local time: HH:MM DD/MM/YYYY +HH:MM"
    )
    location: LocationInput

    houses: list[str] = Field(
        default_factory=lambda: list(DEFAULT_HOUSES)
    )

    planets: list[str] = Field(
        default_factory=lambda: list(DEFAULT_PLANETS)
    )


def serialise(value: Any) -> Any:
    """Convert VedAstro objects into JSON-safe values."""

    if value is None or isinstance(
        value,
        (str, int, float, bool),
    ):
        return value

    if isinstance(value, Enum):
        return value.value

    if isinstance(value, dict):
        return {
            str(key): serialise(item)
            for key, item in value.items()
        }

    if isinstance(value, (list, tuple, set)):
        return [serialise(item) for item in value]

    if hasattr(value, "to_json"):
        try:
            return serialise(value.to_json())
        except Exception:
            pass

    if hasattr(value, "__dict__"):
        return {
            str(key): serialise(item)
            for key, item in vars(value).items()
            if not str(key).startswith("_")
        }

    return str(value)


IMPORTANT_KEYS = (
    "name",
    "sign",
    "house",
    "lord",
    "constellation",
    "nakshatra",
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
    "planet",
    "cusp",
    "pada",
    "tithi",
    "yoga",
    "karana",
    "hora",
    "weekday",
    "ayanamsa",
)


def compact(value: Any, depth: int = 0) -> Any:
    """
    Keep the astrology fields needed by ChatGPT while preventing an
    oversized Action response.
    """

    value = serialise(value)

    if depth > 8:
        if isinstance(value, (str, int, float, bool)):
            return value
        return None

    if isinstance(value, dict):
        output = {}

        for key, child in value.items():
            compacted = compact(child, depth + 1)

            if compacted in (None, {}, []):
                continue

            normalised_key = key.lower().replace("_", "")

            if (
                depth <= 1
                or any(
                    token in normalised_key
                    for token in IMPORTANT_KEYS
                )
            ):
                output[key] = compacted

        return output

    if isinstance(value, list):
        output = [
            compact(item, depth + 1)
            for item in value[:100]
        ]

        return [
            item
            for item in output
            if item not in (None, {}, [])
        ]

    return value


def is_retryable(message: str) -> bool:
    message = message.lower()

    return any(
        text in message
        for text in [
            "access denied",
            "rate limit",
            "too many",
            "timeout",
            "timed out",
            "429",
            "502",
            "503",
            "504",
            "temporarily",
            "connection",
        ]
    )


def vedastro_call(
    method_name: str,
    *arguments: Any,
) -> dict[str, Any]:
    """
    Call one official VedAstro.Python method with spacing and retries.
    """

    global _last_call_time

    if not hasattr(Calculate, method_name):
        return {
            "status": "Fail",
            "method": method_name,
            "error": "Method unavailable in installed package",
        }

    method = getattr(Calculate, method_name)
    last_error = ""

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with _call_lock:
                elapsed = time.monotonic() - _last_call_time

                if elapsed < MIN_INTERVAL_SECONDS:
                    time.sleep(
                        MIN_INTERVAL_SECONDS - elapsed
                    )

                result = method(*arguments)
                _last_call_time = time.monotonic()

            return {
                "status": "Pass",
                "method": method_name,
                "attempt": attempt,
                "data": compact(result),
            }

        except Exception as error:
            last_error = str(error)

            if (
                attempt == MAX_RETRIES
                or not is_retryable(last_error)
            ):
                break

            time.sleep(min(2**attempt, 10))

    return {
        "status": "Fail",
        "method": method_name,
        "attempts": MAX_RETRIES,
        "error": last_error or "Unknown VedAstro error",
    }


def first_available_call(
    method_names: list[str],
    *arguments: Any,
) -> dict[str, Any]:
    for method_name in method_names:
        if hasattr(Calculate, method_name):
            return vedastro_call(
                method_name,
                *arguments,
            )

    return {
        "status": "Fail",
        "method": method_names[0],
        "error": f"No available method from {method_names}",
    }


def verify_proxy_key(
    x_proxy_key: str | None,
) -> None:
    if x_proxy_key != PROXY_API_KEY:
        raise HTTPException(
            status_code=401,
            detail="Invalid proxy API key",
        )


@app.get("/health")
def health():
    return {
        "status": "ok",
        "engine": "official VedAstro.Python",
        "ayanamsa": "Lahiri",
    }


@app.post("/event-chart")
def event_chart(
    request: EventChartInput,
    x_proxy_key: str | None = Header(
        default=None,
        alias="x-proxy-key",
    ),
):
    verify_proxy_key(x_proxy_key)

    unknown_houses = [
        house
        for house in request.houses
        if house not in HOUSES
    ]

    unknown_planets = [
        planet
        for planet in request.planets
        if planet not in PLANETS
    ]

    if unknown_houses or unknown_planets:
        raise HTTPException(
            status_code=422,
            detail={
                "unknown_houses": unknown_houses,
                "unknown_planets": unknown_planets,
            },
        )

    location = GeoLocation(
        request.location.name,
        request.location.longitude,
        request.location.latitude,
    )

    event_time = Time(
        request.std_time,
        location,
    )

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
        "yoga": first_available_call(
            ["Yoga", "NithyaYoga"],
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

    houses = {}

    for house_name in request.houses:
        houses[house_name] = vedastro_call(
            "AllHouseData",
            HOUSES[house_name],
            event_time,
        )

    planets = {}

    for planet_name in request.planets:
        planets[planet_name] = vedastro_call(
            "AllPlanetData",
            PLANETS[planet_name],
            event_time,
        )

    all_results = (
        list(core.values())
        + list(houses.values())
        + list(planets.values())
    )

    failures = [
        result
        for result in all_results
        if result.get("status") != "Pass"
    ]

    return {
        "status": (
            "Pass"
            if not failures
            else "Fail"
        ),
        "strict_prediction_allowed": not failures,
        "essential_failures": failures,
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
            "vedastro_key": "stored server-side",
        },
    }
