from __future__ import annotations

import hmac
from typing import Any

from fastapi import APIRouter, Header, HTTPException, Query
from pydantic import BaseModel, Field

from .config import settings
from .gambler_dharma_chart_engine import (
    EventChartInput,
    LocationInput,
    ParticipantNameInput,
    ParticipantsInput,
    calculate_event_chart,
)
from .name_sound_resolver import name_sound_resolver_health


router = APIRouter(tags=["Custom GPT Action"])


class FootballDrawAuditInput(BaseModel):
    """Verified pre-match football evidence for one deterministic draw audit."""

    home_team: str = Field(min_length=1, max_length=200)
    away_team: str = Field(min_length=1, max_length=200)

    home_odds: float = Field(gt=1.0, le=1000.0)
    draw_odds: float = Field(gt=1.0, le=1000.0)
    away_odds: float = Field(gt=1.0, le=1000.0)

    combined_recent_draw_rate: float | None = Field(
        default=None, ge=0.0, le=1.0
    )
    h2h_draw_rate: float | None = Field(
        default=None, ge=0.0, le=1.0
    )
    h2h_sample_size: int = Field(default=0, ge=0, le=100)

    combined_average_total_goals: float | None = Field(
        default=None, ge=0.0, le=20.0
    )
    combined_expected_goals: float | None = Field(
        default=None, ge=0.0, le=20.0
    )
    points_per_game_gap: float | None = Field(
        default=None, ge=0.0, le=3.0
    )
    expected_goals_gap: float | None = Field(
        default=None, ge=0.0, le=10.0
    )
    expected_goals_against_gap: float | None = Field(
        default=None, ge=0.0, le=10.0
    )

    strong_defence_or_goalkeeping: bool = False
    attacking_absences: bool = False
    rotation_or_fatigue: bool = False
    starting_xi_confirmed: bool = False


def _extract_bearer_token(authorization: str | None) -> str | None:
    if not authorization:
        return None
    scheme, separator, token = authorization.partition(" ")
    if separator and scheme.lower() == "bearer" and token.strip():
        return token.strip()
    return None


def _verify_proxy_key(
    x_proxy_key: str | None = None,
    authorization: str | None = None,
) -> None:
    """Accept either the legacy custom header or standard Bearer auth."""
    expected = settings.proxy_api_key

    if not expected:
        raise HTTPException(
            status_code=503,
            detail={
                "status": "error",
                "message": "PROXY_API_KEY is not configured on Render.",
            },
        )

    supplied = x_proxy_key or _extract_bearer_token(authorization)
    if not supplied or not hmac.compare_digest(supplied, expected):
        raise HTTPException(
            status_code=401,
            detail={
                "status": "unauthorized",
                "message": (
                    "Invalid or missing API key. Use Authorization: "
                    "Bearer <PROXY_API_KEY> or x-proxy-key."
                ),
            },
        )


def calculate_football_draw_audit(
    request: FootballDrawAuditInput,
) -> dict[str, Any]:
    """Return a transparent deterministic 1X2 draw audit.

    The market gates and evidence-family thresholds match the Custom GPT
    policy. Correlated inputs are grouped so they cannot be double-counted.
    """
    inverse = {
        "home": 1.0 / request.home_odds,
        "draw": 1.0 / request.draw_odds,
        "away": 1.0 / request.away_odds,
    }
    overround = sum(inverse.values())
    probabilities = {
        key: value / overround
        for key, value in inverse.items()
    }

    home_probability = probabilities["home"]
    draw_probability = probabilities["draw"]
    away_probability = probabilities["away"]
    home_away_gap = abs(home_probability - away_probability)
    team_leader_probability = max(home_probability, away_probability)
    draw_to_team_leader_gap = team_leader_probability - draw_probability
    draw_is_market_leader = (
        draw_probability >= home_probability
        and draw_probability >= away_probability
    )

    probability_gate = draw_probability >= 0.24
    parity_gate = home_away_gap <= 0.15
    near_leader_gate = draw_to_team_leader_gap <= 0.07

    signal_families: dict[str, list[str]] = {}

    if (
        request.combined_recent_draw_rate is not None
        and request.combined_recent_draw_rate >= 0.28
    ):
        signal_families["recent_draw_rate"] = [
            "combined_recent_draw_rate"
        ]

    if (
        request.h2h_sample_size >= 3
        and request.h2h_draw_rate is not None
        and request.h2h_draw_rate >= 0.30
    ):
        signal_families["head_to_head_draw_rate"] = [
            "h2h_draw_rate"
        ]

    low_scoring_sources: list[str] = []
    if (
        request.combined_average_total_goals is not None
        and request.combined_average_total_goals <= 2.50
    ):
        low_scoring_sources.append("combined_average_total_goals")
    if (
        request.combined_expected_goals is not None
        and request.combined_expected_goals <= 2.50
    ):
        low_scoring_sources.append("combined_expected_goals")
    if low_scoring_sources:
        signal_families["low_scoring_environment"] = low_scoring_sources

    parity_sources: list[str] = []
    if (
        request.points_per_game_gap is not None
        and request.points_per_game_gap <= 0.35
    ):
        parity_sources.append("points_per_game_gap")
    if (
        request.expected_goals_gap is not None
        and request.expected_goals_gap <= 0.35
    ):
        parity_sources.append("expected_goals_gap")
    if (
        request.expected_goals_against_gap is not None
        and request.expected_goals_against_gap <= 0.35
    ):
        parity_sources.append("expected_goals_against_gap")
    if parity_sources:
        signal_families["performance_parity"] = parity_sources

    if request.strong_defence_or_goalkeeping:
        signal_families["defence_goalkeeping"] = [
            "strong_defence_or_goalkeeping"
        ]
    if request.attacking_absences:
        signal_families["attacking_absences"] = [
            "attacking_absences"
        ]
    if request.rotation_or_fatigue:
        signal_families["rotation_fatigue"] = [
            "rotation_or_fatigue"
        ]
    if draw_is_market_leader:
        signal_families["market_draw_leader"] = [
            "draw_is_market_leader"
        ]

    independent_signal_count = len(signal_families)
    draw_candidate = bool(
        probability_gate
        and (parity_gate or near_leader_gate)
        and independent_signal_count >= 2
    )

    rejection_reasons: list[str] = []
    if not probability_gate:
        rejection_reasons.append(
            "No-margin draw probability is below 24%."
        )
    if not (parity_gate or near_leader_gate):
        rejection_reasons.append(
            "Neither the 15-point home/away parity gate nor the "
            "7-point draw-to-team-leader gate passed."
        )
    if independent_signal_count < 2:
        rejection_reasons.append(
            "Fewer than two independent football draw-evidence families passed."
        )

    missing_evidence: list[str] = []
    optional_values = {
        "combined_recent_draw_rate": request.combined_recent_draw_rate,
        "h2h_draw_rate": (
            request.h2h_draw_rate
            if request.h2h_sample_size > 0
            else None
        ),
        "combined_average_total_goals": (
            request.combined_average_total_goals
        ),
        "combined_expected_goals": request.combined_expected_goals,
        "points_per_game_gap": request.points_per_game_gap,
        "expected_goals_gap": request.expected_goals_gap,
        "expected_goals_against_gap": (
            request.expected_goals_against_gap
        ),
    }
    for field_name, value in optional_values.items():
        if value is None:
            missing_evidence.append(field_name)

    market_leader = max(
        probabilities,
        key=probabilities.get,
    ).upper()

    return {
        "status": "Pass",
        "market": "90-minute Match Winner 1X2",
        "home_team": request.home_team,
        "away_team": request.away_team,
        "decimal_odds": {
            "home": request.home_odds,
            "draw": request.draw_odds,
            "away": request.away_odds,
        },
        "bookmaker_overround": round(overround, 8),
        "no_margin_probability": {
            key: round(value, 8)
            for key, value in probabilities.items()
        },
        "market_outcome_leader": market_leader,
        "draw_is_market_leader": draw_is_market_leader,
        "home_away_probability_gap": round(home_away_gap, 8),
        "draw_to_team_leader_gap": round(
            draw_to_team_leader_gap,
            8,
        ),
        "gates": {
            "draw_probability_at_least_24_percent": probability_gate,
            "home_away_gap_at_most_15_points": parity_gate,
            "draw_within_7_points_of_team_leader": near_leader_gate,
            "at_least_two_independent_signal_families": (
                independent_signal_count >= 2
            ),
        },
        "draw_signal_families": signal_families,
        "draw_signal_count": independent_signal_count,
        "draw_candidate": draw_candidate,
        "draw_rejection_reasons": rejection_reasons,
        "missing_evidence": missing_evidence,
        "starting_xi_confirmed": request.starting_xi_confirmed,
        "policy": {
            "draw_probability_threshold": 0.24,
            "home_away_gap_threshold": 0.15,
            "draw_to_team_leader_threshold": 0.07,
            "minimum_independent_signal_families": 2,
            "correlated_metrics_count_once": True,
            "draw_candidate_is_not_a_final_prediction": True,
        },
    }


@router.get("/event-chart-health")
def event_chart_health() -> dict[str, Any]:
    return {
        "status": "ok",
        "route_available": True,
        "canonical_path": "/event-chart",
        "operation_id": "calculateVerifiedVedAstroEventChart",
        "draw_audit_path": "/draw-audit",
        "draw_audit_operation_id": "calculateFootballDrawAudit",
        "name_sound_resolver": name_sound_resolver_health(),
        "proxy_key_configured": bool(settings.proxy_api_key),
    }


@router.post(
    "/event-chart",
    operation_id="calculateVerifiedVedAstroEventChart",
    summary="Calculate one verified Gambler's Dharma event chart",
)
def event_chart(
    request: EventChartInput,
    x_proxy_key: str | None = Header(
        default=None,
        alias="x-proxy-key",
    ),
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    _verify_proxy_key(x_proxy_key, authorization)
    return calculate_event_chart(request)


@router.post(
    "/draw-audit",
    operation_id="calculateFootballDrawAudit",
    summary="Calculate a deterministic football 1X2 draw audit",
)
def football_draw_audit(
    request: FootballDrawAuditInput,
    x_proxy_key: str | None = Header(
        default=None,
        alias="x-proxy-key",
    ),
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    _verify_proxy_key(x_proxy_key, authorization)
    return calculate_football_draw_audit(request)


FULL_HOUSES = [
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

FULL_PLANETS = [
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


def _split_confirmed_sounds(value: str | None) -> list[str]:
    if not value:
        return []
    return [
        item.strip()
        for item in value.split(",")
        if item.strip()
    ][:20]


@router.get(
    "/action/ping",
    operation_id="actionReadOnlyPing",
    summary="Check the read-only Custom GPT Action route",
)
def action_read_only_ping() -> dict[str, Any]:
    return {
        "status": "ok",
        "mode": "read_only",
        "authentication": "not_required",
        "message": "The Custom GPT can reach the Render service.",
    }


@router.get(
    "/action/draw-audit",
    operation_id="calculateFootballDrawAuditReadOnly",
    summary="Calculate one read-only football 1X2 draw audit",
)
def football_draw_audit_read_only(
    home_team: str = Query(min_length=1, max_length=200),
    away_team: str = Query(min_length=1, max_length=200),
    home_odds: float = Query(gt=1.0, le=1000.0),
    draw_odds: float = Query(gt=1.0, le=1000.0),
    away_odds: float = Query(gt=1.0, le=1000.0),
    combined_recent_draw_rate: float | None = Query(
        default=None, ge=0.0, le=1.0
    ),
    h2h_draw_rate: float | None = Query(
        default=None, ge=0.0, le=1.0
    ),
    h2h_sample_size: int = Query(default=0, ge=0, le=100),
    combined_average_total_goals: float | None = Query(
        default=None, ge=0.0, le=20.0
    ),
    combined_expected_goals: float | None = Query(
        default=None, ge=0.0, le=20.0
    ),
    points_per_game_gap: float | None = Query(
        default=None, ge=0.0, le=3.0
    ),
    expected_goals_gap: float | None = Query(
        default=None, ge=0.0, le=10.0
    ),
    expected_goals_against_gap: float | None = Query(
        default=None, ge=0.0, le=10.0
    ),
    strong_defence_or_goalkeeping: bool = Query(default=False),
    attacking_absences: bool = Query(default=False),
    rotation_or_fatigue: bool = Query(default=False),
    starting_xi_confirmed: bool = Query(default=False),
    authorization: str | None = Header(default=None),
    x_proxy_key: str | None = Header(
        default=None,
        alias="x-proxy-key",
    ),
) -> dict[str, Any]:
    _verify_proxy_key(x_proxy_key, authorization)
    request = FootballDrawAuditInput(
        home_team=home_team,
        away_team=away_team,
        home_odds=home_odds,
        draw_odds=draw_odds,
        away_odds=away_odds,
        combined_recent_draw_rate=combined_recent_draw_rate,
        h2h_draw_rate=h2h_draw_rate,
        h2h_sample_size=h2h_sample_size,
        combined_average_total_goals=combined_average_total_goals,
        combined_expected_goals=combined_expected_goals,
        points_per_game_gap=points_per_game_gap,
        expected_goals_gap=expected_goals_gap,
        expected_goals_against_gap=expected_goals_against_gap,
        strong_defence_or_goalkeeping=strong_defence_or_goalkeeping,
        attacking_absences=attacking_absences,
        rotation_or_fatigue=rotation_or_fatigue,
        starting_xi_confirmed=starting_xi_confirmed,
    )
    return calculate_football_draw_audit(request)


@router.get(
    "/action/event-chart",
    operation_id="calculateVerifiedVedAstroEventChartReadOnly",
    summary="Calculate one read-only verified Gambler's Dharma chart",
)
def event_chart_read_only(
    event_id: str = Query(min_length=1, max_length=200),
    std_time: str = Query(
        min_length=20,
        max_length=40,
        description="HH:MM DD/MM/YYYY +HH:MM",
    ),
    location_name: str = Query(min_length=1, max_length=200),
    longitude: float = Query(ge=-180.0, le=180.0),
    latitude: float = Query(ge=-90.0, le=90.0),
    favourite_name: str = Query(min_length=1, max_length=200),
    underdog_name: str = Query(min_length=1, max_length=200),
    favourite_confirmed_sounds: str | None = Query(
        default=None,
        max_length=300,
        description="Optional comma-separated human-verified sounds.",
    ),
    underdog_confirmed_sounds: str | None = Query(
        default=None,
        max_length=300,
        description="Optional comma-separated human-verified sounds.",
    ),
    authorization: str | None = Header(default=None),
    x_proxy_key: str | None = Header(
        default=None,
        alias="x-proxy-key",
    ),
) -> dict[str, Any]:
    _verify_proxy_key(x_proxy_key, authorization)
    request = EventChartInput(
        event_id=event_id,
        std_time=std_time,
        location=LocationInput(
            name=location_name,
            longitude=longitude,
            latitude=latitude,
        ),
        houses=FULL_HOUSES,
        planets=FULL_PLANETS,
        participants=ParticipantsInput(
            favourite=ParticipantNameInput(
                name=favourite_name,
                confirmed_opening_sounds=_split_confirmed_sounds(
                    favourite_confirmed_sounds
                ),
            ),
            underdog=ParticipantNameInput(
                name=underdog_name,
                confirmed_opening_sounds=_split_confirmed_sounds(
                    underdog_confirmed_sounds
                ),
            ),
        ),
    )
    return calculate_event_chart(request)


@router.post(
    "/v1/event-chart",
    include_in_schema=False,
)
def event_chart_v1_alias(
    request: EventChartInput,
    x_proxy_key: str | None = Header(
        default=None,
        alias="x-proxy-key",
    ),
) -> dict[str, Any]:
    _verify_proxy_key(x_proxy_key)
    return calculate_event_chart(request)


@router.post(
    "/actions/calculate-verified-vedastro-event-chart",
    include_in_schema=False,
)
def event_chart_named_alias(
    request: EventChartInput,
    x_proxy_key: str | None = Header(
        default=None,
        alias="x-proxy-key",
    ),
) -> dict[str, Any]:
    _verify_proxy_key(x_proxy_key)
    return calculate_event_chart(request)
