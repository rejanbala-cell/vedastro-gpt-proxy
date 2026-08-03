from __future__ import annotations

import hmac
from typing import Any

from fastapi import APIRouter, Header, HTTPException

from .config import settings
from .gambler_dharma_chart_engine import (
    EventChartInput,
    calculate_event_chart,
)


router = APIRouter(tags=["Custom GPT Action"])


def _verify_proxy_key(x_proxy_key: str | None) -> None:
    expected = settings.proxy_api_key

    if not expected:
        raise HTTPException(
            status_code=503,
            detail={
                "status": "error",
                "message": "PROXY_API_KEY is not configured on Render.",
            },
        )

    if (
        not x_proxy_key
        or not hmac.compare_digest(x_proxy_key, expected)
    ):
        raise HTTPException(
            status_code=401,
            detail={
                "status": "unauthorized",
                "message": "Invalid or missing x-proxy-key.",
            },
        )


@router.get("/event-chart-health")
def event_chart_health() -> dict[str, Any]:
    return {
        "status": "ok",
        "route_available": True,
        "canonical_path": "/event-chart",
        "operation_id": "calculateVerifiedVedAstroEventChart",
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
) -> dict[str, Any]:
    _verify_proxy_key(x_proxy_key)
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
