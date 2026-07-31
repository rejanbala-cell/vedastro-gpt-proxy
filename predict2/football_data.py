from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from datetime import date
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
        retry_after_seconds: int | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.provider_message = provider_message
        self.retry_after_seconds = retry_after_seconds


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
        self._request_lock = threading.Lock()
        self._last_request_monotonic = 0.0

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

    @staticmethod
    def _provider_message(payload: Any) -> str | None:
        if not isinstance(payload, dict):
            return None
        value = (
            payload.get("message")
            or payload.get("error")
            or payload.get("errorCode")
        )
        text = str(value or "").strip()
        return text or None

    def _wait_for_request_slot(self) -> None:
        elapsed = time.monotonic() - self._last_request_monotonic
        delay = (
            settings.football_data_min_interval_seconds - elapsed
        )
        if delay > 0:
            time.sleep(delay)

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
        if date_to < date_from:
            raise FootballDataError(
                "The football-data.org date window is invalid."
            )

        with self._request_lock:
            self._wait_for_request_slot()
            try:
                response = requests.get(
                    f"{self.base_url}/matches",
                    headers=self._headers(),
                    params={
                        "dateFrom": date_from.isoformat(),
                        "dateTo": date_to.isoformat(),
                    },
                    timeout=self.timeout,
                )
            except requests.RequestException as exc:
                self._last_request_monotonic = time.monotonic()
                raise FootballDataError(
                    "Football-data.org network request failed.",
                    provider_message=type(exc).__name__,
                ) from exc
            self._last_request_monotonic = time.monotonic()

        try:
            payload = response.json()
        except ValueError as exc:
            raise FootballDataError(
                "Football-data.org returned invalid JSON.",
                status_code=response.status_code,
            ) from exc

        if response.status_code != 200:
            retry_after = response.headers.get("Retry-After")
            retry_seconds = None
            if retry_after:
                try:
                    retry_seconds = int(retry_after)
                except ValueError:
                    retry_seconds = None
            raise FootballDataError(
                "Football-data.org rejected the request.",
                status_code=response.status_code,
                provider_message=self._provider_message(payload),
                retry_after_seconds=retry_seconds,
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
        from datetime import datetime, timezone

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


    def match(self, match_id: str) -> dict[str, Any]:
        if not settings.football_data_enabled:
            raise FootballDataError(
                "Football-data.org integration is disabled."
            )
        with self._request_lock:
            self._wait_for_request_slot()
            try:
                response = requests.get(
                    f"{self.base_url}/matches/{match_id}",
                    headers=self._headers(),
                    timeout=self.timeout,
                )
            except requests.RequestException as exc:
                self._last_request_monotonic = time.monotonic()
                raise FootballDataError(
                    "Football-data.org match request failed.",
                    provider_message=type(exc).__name__,
                ) from exc
            self._last_request_monotonic = time.monotonic()
        try:
            payload = response.json()
        except ValueError as exc:
            raise FootballDataError(
                "Football-data.org returned invalid match JSON.",
                status_code=response.status_code,
            ) from exc
        if response.status_code != 200:
            raise FootballDataError(
                "Football-data.org rejected the match request.",
                status_code=response.status_code,
                provider_message=self._provider_message(payload),
            )
        return payload if isinstance(payload, dict) else {}

    def standings(self, competition_code: str) -> dict[str, Any]:
        code = str(competition_code or "").strip()
        if not code:
            raise FootballDataError("Competition code is unavailable.")
        with self._request_lock:
            self._wait_for_request_slot()
            try:
                response = requests.get(
                    f"{self.base_url}/competitions/{code}/standings",
                    headers=self._headers(),
                    timeout=self.timeout,
                )
            except requests.RequestException as exc:
                self._last_request_monotonic = time.monotonic()
                raise FootballDataError(
                    "Football-data.org standings request failed.",
                    provider_message=type(exc).__name__,
                ) from exc
            self._last_request_monotonic = time.monotonic()
        try:
            payload = response.json()
        except ValueError as exc:
            raise FootballDataError(
                "Football-data.org returned invalid standings JSON.",
                status_code=response.status_code,
            ) from exc
        if response.status_code != 200:
            raise FootballDataError(
                "Football-data.org rejected the standings request.",
                status_code=response.status_code,
                provider_message=self._provider_message(payload),
            )
        return payload if isinstance(payload, dict) else {}


client = FootballDataClient()
