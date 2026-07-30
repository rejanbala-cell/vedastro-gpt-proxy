from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any

import requests

from .config import settings


class FootballDataError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        provider_message: str | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.provider_message = provider_message


@dataclass(frozen=True)
class FootballDataResponse:
    matches: list[dict[str, Any]]
    result_set: dict[str, Any]
    filters: dict[str, Any]
    response_headers: dict[str, str]


class FootballDataClient:
    def __init__(self) -> None:
        self.base_url = settings.football_data_base_url
        self.timeout = settings.football_data_timeout_seconds
        self.token = settings.football_data_api_key

    def _headers(self) -> dict[str, str]:
        if not self.token:
            raise FootballDataError(
                "FOOTBALL_DATA_API_KEY is not configured."
            )
        return {
            "X-Auth-Token": self.token,
            "Accept": "application/json",
            "User-Agent": (
                f"VedAstro-Private-Predictor/{settings.version}"
            ),
        }

    def matches(
        self,
        *,
        date_from: date,
        date_to: date,
    ) -> FootballDataResponse:
        if not settings.football_data_enabled:
            raise FootballDataError(
                "Football-data.org integration is disabled."
            )

        response = requests.get(
            f"{self.base_url}/matches",
            headers=self._headers(),
            params={
                "dateFrom": date_from.isoformat(),
                "dateTo": date_to.isoformat(),
            },
            timeout=self.timeout,
        )

        try:
            payload = response.json()
        except ValueError as exc:
            raise FootballDataError(
                "Football-data.org returned invalid JSON.",
                status_code=response.status_code,
            ) from exc

        if response.status_code != 200:
            provider_message = None
            if isinstance(payload, dict):
                provider_message = str(
                    payload.get("message")
                    or payload.get("errorCode")
                    or ""
                ).strip() or None
            raise FootballDataError(
                "Football-data.org rejected the request.",
                status_code=response.status_code,
                provider_message=provider_message,
            )

        rows = payload.get("matches")
        if not isinstance(rows, list):
            rows = []

        return FootballDataResponse(
            matches=[
                row for row in rows if isinstance(row, dict)
            ],
            result_set=(
                payload.get("resultSet")
                if isinstance(payload.get("resultSet"), dict)
                else {}
            ),
            filters=(
                payload.get("filters")
                if isinstance(payload.get("filters"), dict)
                else {}
            ),
            response_headers={
                key: value
                for key, value in response.headers.items()
                if key.lower().startswith("x-request")
            },
        )

    def health(self) -> dict[str, Any]:
        today = datetime.now(timezone.utc).date()
        response = self.matches(
            date_from=today,
            date_to=today,
        )
        return {
            "status": "ok",
            "connected": True,
            "matches_returned": len(response.matches),
            "result_set": response.result_set,
            "request_headers": response.response_headers,
        }


client = FootballDataClient()
