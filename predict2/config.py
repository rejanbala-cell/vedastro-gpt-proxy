from __future__ import annotations

import os
from dataclasses import dataclass


def _bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _float(
    name: str,
    default: float,
    minimum: float,
    maximum: float,
) -> float:
    raw = os.getenv(name, str(default)).strip()
    try:
        value = float(raw)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be a number.") from exc
    return max(minimum, min(value, maximum))


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
    tavily_api_key: str
    tavily_enabled: bool
    tavily_base_url: str
    tavily_search_depth: str
    tavily_max_results: int
    tavily_extract_enabled: bool
    tavily_extract_depth: str
    tavily_extract_max_urls: int
    tavily_min_distinct_domains: int
    tavily_require_official_source: bool
    tavily_min_interval_seconds: float
    locationiq_key: str
    locationiq_base_url: str
    locationiq_timeout_seconds: int
    nominatim_enabled: bool
    nominatim_base_url: str
    nominatim_timeout_seconds: int
    nominatim_user_agent: str
    nominatim_referer: str
    venue_enrichment_window_days: int
    venue_enrichment_max_per_job: int
    venue_enrichment_retry_hours: int
    venue_similarity_minimum: float
    geocode_confidence_minimum: float
    tavily_timeout_seconds: int
    venue_stage_warning_seconds: int
    venue_job_stale_minutes: int
    prediction_model_version: str
    prediction_tavily_timeout_seconds: int
    prediction_evidence_max_results: int
    prediction_market_min_domains: int
    prediction_draw_margin: float
    prediction_home_advantage: float

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
            version="3.0.0-final",
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
            tavily_api_key=os.getenv(
                "TAVILY_API_KEY", ""
            ).strip(),
            tavily_enabled=_bool(
                "TAVILY_SEARCH_ENABLED", True
            ),
            tavily_base_url=os.getenv(
                "TAVILY_BASE_URL",
                "https://api.tavily.com",
            ).rstrip("/"),
            tavily_search_depth=os.getenv(
                "TAVILY_SEARCH_DEPTH", "basic"
            ).strip().lower(),
            tavily_max_results=_int(
                "TAVILY_MAX_RESULTS", 8, 3, 10
            ),
            tavily_extract_enabled=_bool(
                "TAVILY_EXTRACT_ENABLED", True
            ),
            tavily_extract_depth=os.getenv(
                "TAVILY_EXTRACT_DEPTH", "basic"
            ).strip().lower(),
            tavily_extract_max_urls=_int(
                "TAVILY_EXTRACT_MAX_URLS", 5, 1, 5
            ),
            tavily_min_distinct_domains=_int(
                "TAVILY_MIN_DISTINCT_DOMAINS", 2, 1, 3
            ),
            tavily_require_official_source=_bool(
                "TAVILY_REQUIRE_OFFICIAL_SOURCE", True
            ),
            tavily_min_interval_seconds=_float(
                "TAVILY_MIN_INTERVAL_SECONDS", 1.0, 0.5, 30.0
            ),
            locationiq_key=os.getenv(
                "LOCATIONIQ_KEY", ""
            ).strip(),
            locationiq_base_url=os.getenv(
                "LOCATIONIQ_BASE_URL",
                "https://us1.locationiq.com/v1",
            ).rstrip("/"),
            locationiq_timeout_seconds=_int(
                "LOCATIONIQ_TIMEOUT_SECONDS", 20, 5, 60
            ),
            nominatim_enabled=_bool(
                "NOMINATIM_FALLBACK_ENABLED", True
            ),
            nominatim_base_url=os.getenv(
                "NOMINATIM_BASE_URL",
                "https://nominatim.openstreetmap.org",
            ).rstrip("/"),
            nominatim_timeout_seconds=_int(
                "NOMINATIM_TIMEOUT_SECONDS", 20, 5, 60
            ),
            nominatim_user_agent=os.getenv(
                "NOMINATIM_USER_AGENT",
                "VedAstroPrivatePredictor/2.1 "
                "(https://vedastro-gpt-proxy.onrender.com)",
            ).strip(),
            nominatim_referer=os.getenv(
                "NOMINATIM_REFERER",
                "https://vedastro-gpt-proxy.onrender.com/private",
            ).strip(),
            venue_enrichment_window_days=_int(
                "VENUE_ENRICHMENT_WINDOW_DAYS", 90, 1, 90
            ),
            venue_enrichment_max_per_job=_int(
                "VENUE_ENRICHMENT_MAX_PER_JOB", 12, 1, 30
            ),
            venue_enrichment_retry_hours=_int(
                "VENUE_ENRICHMENT_RETRY_HOURS", 24, 1, 168
            ),
            venue_similarity_minimum=_float(
                "VENUE_SIMILARITY_MINIMUM", 0.64, 0.50, 0.95
            ),
            geocode_confidence_minimum=_float(
                "GEOCODE_CONFIDENCE_MINIMUM", 72.0, 55.0, 95.0
            ),
            tavily_timeout_seconds=_int(
                "TAVILY_TIMEOUT_SECONDS", 20, 8, 45
            ),
            venue_stage_warning_seconds=_int(
                "VENUE_STAGE_WARNING_SECONDS", 45, 15, 180
            ),
            venue_job_stale_minutes=_int(
                "VENUE_JOB_STALE_MINUTES", 5, 2, 60
            ),
            prediction_model_version=os.getenv(
                "PREDICTION_MODEL_VERSION",
                "gambler-dharma-final-v1",
            ).strip(),
            prediction_tavily_timeout_seconds=_int(
                "PREDICTION_TAVILY_TIMEOUT_SECONDS", 20, 8, 45
            ),
            prediction_evidence_max_results=_int(
                "PREDICTION_EVIDENCE_MAX_RESULTS", 8, 3, 10
            ),
            prediction_market_min_domains=_int(
                "PREDICTION_MARKET_MIN_DOMAINS", 2, 1, 4
            ),
            prediction_draw_margin=_float(
                "PREDICTION_DRAW_MARGIN", 0.08, 0.03, 0.20
            ),
            prediction_home_advantage=_float(
                "PREDICTION_HOME_ADVANTAGE", 0.10, 0.00, 0.25
            ),
        )


settings = Settings.load()
