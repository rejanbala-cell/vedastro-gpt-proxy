from __future__ import annotations

import traceback
from typing import Any


REQUIRED_HOUSES = [
    "House1", "House3", "House4", "House5", "House6",
    "House7", "House9", "House10", "House11", "House12",
]
REQUIRED_PLANETS = [
    "Sun", "Moon", "Mars", "Mercury", "Jupiter",
    "Venus", "Saturn", "Rahu", "Ketu",
]


def _validation(result: dict[str, Any]) -> dict[str, Any]:
    status = str(result.get("status") or "")
    failures = result.get("essential_failures")
    failures = failures if isinstance(failures, list) else []
    standard = str(
        result.get("standard_chart_ayanamsha")
        or result.get("standard_ayanamsha")
        or result.get("ayanamsha")
        or ""
    ).lower()
    kp = str(
        result.get("kp_ayanamsha")
        or result.get("chapter_6_ayanamsha")
        or ""
    ).lower()
    return {
        "status_pass": status.lower() == "pass",
        "essential_failures_empty": not failures,
        "lahiri_standard": "lahiri" in standard or not standard,
        "krishnamurti_kp": "krishnamurti" in kp or not kp,
        "valid": (
            status.lower() == "pass"
            and not failures
            and ("lahiri" in standard or not standard)
            and ("krishnamurti" in kp or not kp)
        ),
        "essential_failures": failures,
    }


def calculate_chart(
    *,
    event_id: str,
    std_time: str,
    venue_name: str,
    latitude: float,
    longitude: float,
    favourite_team: str,
    underdog_team: str,
) -> dict[str, Any]:
    try:
        from .gambler_dharma_chart_engine import (
            EventChartInput,
            LocationInput,
            ParticipantInput,
            ParticipantsInput,
            calculate_event_chart,
        )
        request = EventChartInput(
            event_id=event_id,
            std_time=std_time,
            location=LocationInput(
                name=venue_name,
                latitude=latitude,
                longitude=longitude,
            ),
            houses=REQUIRED_HOUSES,
            planets=REQUIRED_PLANETS,
            participants=ParticipantsInput(
                favourite=ParticipantInput(
                    name=favourite_team,
                    confirmed_opening_sounds=[],
                ),
                underdog=ParticipantInput(
                    name=underdog_team,
                    confirmed_opening_sounds=[],
                ),
            ),
        )
        result = calculate_event_chart(request)
        if not isinstance(result, dict):
            result = {"status": "Fail", "raw": str(result)}
        return {
            "status": "ok",
            "chart": result,
            "validation": _validation(result),
            "one_high_level_call": True,
        }
    except Exception as exc:
        return {
            "status": "error",
            "error_type": type(exc).__name__,
            "message": str(exc) or repr(exc),
            "traceback_tail": traceback.format_exc().splitlines()[-12:],
            "chart": None,
            "validation": {"valid": False},
            "one_high_level_call": True,
        }


def practical_reliability(chart: dict[str, Any]) -> dict[str, Any]:
    audit = chart.get("reliability_audit")
    audit = audit if isinstance(audit, dict) else {}
    mode = str(audit.get("policy_mode") or "practical_verified")
    practical_hard = bool(
        chart.get("practical_hard_veto")
        or audit.get("practical_hard_veto")
        or audit.get("hard_veto")
    )
    practical_allowed = bool(
        chart.get("practical_prediction_allowed", True)
        and audit.get("practical_prediction_allowed", True)
    )
    strict_allowed = bool(
        chart.get("strict_prediction_allowed", True)
        and chart.get("strict_prediction_allowed_by_reliability", True)
    )
    return {
        "policy_mode": mode,
        "hard_veto": practical_hard,
        "prediction_allowed": (
            not practical_hard
            and practical_allowed
            and strict_allowed
        ),
        "confidence_cap": (
            chart.get("confidence_cap")
            or audit.get("confidence_cap")
        ),
        "strict_book_hard_veto": bool(
            chart.get("strict_book_hard_veto")
            or audit.get("strict_book_hard_veto")
        ),
    }


def _numbers(value: Any, path: str = "") -> list[tuple[str, float]]:
    output: list[tuple[str, float]] = []
    if isinstance(value, dict):
        for key, item in value.items():
            next_path = f"{path}.{key}" if path else str(key)
            key_lower = str(key).lower()
            if (
                isinstance(item, (int, float))
                and any(
                    token in key_lower
                    for token in (
                        "signed_favourite_total",
                        "signed_interval",
                        "favourite_total",
                    )
                )
                and "raw_" not in key_lower
            ):
                output.append((next_path, float(item)))
            else:
                output.extend(_numbers(item, next_path))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            output.extend(_numbers(item, f"{path}[{index}]"))
    return output


def astrology_signal(chart: dict[str, Any]) -> dict[str, Any]:
    reliability = practical_reliability(chart)
    if not reliability["prediction_allowed"]:
        return {
            "status": "research_only",
            "direction": "balanced",
            "strength": 0,
            "reliability": reliability,
            "signals": [],
        }

    signals = _numbers(chart)
    positives = [value for _, value in signals if value > 0]
    negatives = [value for _, value in signals if value < 0]
    total = sum(value for _, value in signals)

    if not signals or abs(total) < 0.5:
        direction = "balanced"
    elif total > 0:
        direction = "favourite"
    else:
        direction = "opponent"

    strength = min(3, int(abs(total) >= 0.5) + int(abs(total) >= 1.5) + int(abs(total) >= 3))
    return {
        "status": "active",
        "direction": direction,
        "strength": strength,
        "signed_total": total,
        "signals": [
            {"path": path, "value": value}
            for path, value in signals[:40]
        ],
        "reliability": reliability,
    }
