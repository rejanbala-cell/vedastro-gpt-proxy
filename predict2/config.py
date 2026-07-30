from __future__ import annotations

import os
from dataclasses import dataclass


def _bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _int(name: str, default: int, minimum: int, maximum: int) -> int:
    raw = os.getenv(name, str(default)).strip()
    try:
        value = int(raw)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer.") from exc
    return max(minimum, min(value, maximum))


@dataclass(frozen=True)
class Settings:
    version: str
    database_url: str
    football_data_api_key: str
    football_data_enabled: bool
    football_data_base_url: str
    football_data_timeout_seconds: int
    football_data_sync_days: int
    football_data_min_interval_seconds: int
    football_data_sync_interval_seconds: int
    private_ui_password: str
    proxy_api_key: str
    private_ui_cookie_secure: bool
    private_ui_cookie_samesite: str
    private_ui_session_hours: int

    @property
    def login_secret(self) -> str:
        return self.private_ui_password or self.proxy_api_key

    @classmethod
    def load(cls) -> "Settings":
        same_site = os.getenv(
            "PRIVATE_UI_COOKIE_SAMESITE", "lax"
        ).strip().lower()
        if same_site not in {"lax", "strict", "none"}:
            same_site = "lax"

        secure = _bool("PRIVATE_UI_COOKIE_SECURE", True)
        if same_site == "none" and not secure:
            same_site = "lax"

        return cls(
            version="2.0.3-metadata",
            database_url=os.getenv("DATABASE_URL", "").strip(),
            football_data_api_key=os.getenv(
                "FOOTBALL_DATA_API_KEY", ""
            ).strip(),
            football_data_enabled=_bool(
                "FOOTBALL_DATA_ENABLED", True
            ),
            football_data_base_url=os.getenv(
                "FOOTBALL_DATA_BASE_URL",
                "https://api.football-data.org/v4",
            ).rstrip("/"),
            football_data_timeout_seconds=_int(
                "FOOTBALL_DATA_TIMEOUT_SECONDS", 20, 5, 60
            ),
            football_data_sync_days=_int(
                "FOOTBALL_DATA_SYNC_DAYS", 90, 1, 120
            ),
            football_data_min_interval_seconds=_int(
                "FOOTBALL_DATA_MIN_INTERVAL_SECONDS", 6, 1, 60
            ),
            football_data_sync_interval_seconds=_int(
                "FOOTBALL_DATA_SYNC_INTERVAL_SECONDS",
                86400,
                3600,
                604800,
            ),
            private_ui_password=os.getenv(
                "PRIVATE_UI_PASSWORD", ""
            ).strip(),
            proxy_api_key=os.getenv("PROXY_API_KEY", "").strip(),
            private_ui_cookie_secure=secure,
            private_ui_cookie_samesite=same_site,
            private_ui_session_hours=_int(
                "PRIVATE_UI_SESSION_HOURS", 12, 1, 168
            ),
        )


settings = Settings.load()
