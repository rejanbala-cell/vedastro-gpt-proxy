from __future__ import annotations

import json
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Query, Response
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from .auth import (
    COOKIE_NAME,
    create_token,
    password_matches,
    require_session,
)
from .config import settings
from .db import ensure_schema
from .football_data import FootballDataError, client
from .fixtures import list_fixtures
from .metadata import get, get_datetime
from .sync import sync_if_stale_async, sync_now
from .ui import PRIVATE_HTML


@asynccontextmanager
async def lifespan(app: FastAPI):
    ensure_schema()
    sync_if_stale_async()
    yield


app = FastAPI(
    title="VedAstro Private Predictor — PREDICT2",
    version=settings.version,
    lifespan=lifespan,
)


class LoginRequest(BaseModel):
    password: str = Field(min_length=1, max_length=500)


@app.get("/health")
def health() -> dict[str, Any]:
    last_sync = None
    try:
        value = get_datetime(
            "predict2_football_data_last_sync_at"
        )
        last_sync = value.isoformat() if value else None
    except Exception:
        pass

    return {
        "status": "ok",
        "version": settings.version,
        "architecture": "clean_modular_foundation",
        "football_data": {
            "enabled": settings.football_data_enabled,
            "api_key_configured": bool(
                settings.football_data_api_key
            ),
            "base_url": settings.football_data_base_url,
            "sync_days": settings.football_data_sync_days,
            "sync_interval_seconds": (
                settings.football_data_sync_interval_seconds
            ),
            "last_sync_at": last_sync,
        },
        "database_configured": bool(settings.database_url),
        "private_login_configured": bool(
            settings.login_secret
        ),
        "prediction_engine": {
            "status": "not_migrated",
            "reason": (
                "Foundation release validates the new fixture "
                "catalogue before chart orchestration is migrated."
            ),
        },
    }


@app.get("/football-data-health")
def football_data_health() -> dict[str, Any]:
    try:
        return client.health()
    except FootballDataError as exc:
        raise HTTPException(
            status_code=503,
            detail={
                "status": "error",
                "connected": False,
                "message": str(exc),
                "http_status": exc.status_code,
                "provider_message": exc.provider_message,
            },
        ) from exc


@app.get("/private", response_class=HTMLResponse)
def private_page() -> str:
    return PRIVATE_HTML


@app.post("/private/api/login")
def private_login(
    request: LoginRequest,
    response: Response,
) -> dict[str, Any]:
    if not settings.login_secret:
        raise HTTPException(
            status_code=503,
            detail=(
                "PRIVATE_UI_PASSWORD or PROXY_API_KEY "
                "is not configured."
            ),
        )
    if not password_matches(request.password):
        raise HTTPException(
            status_code=401,
            detail="Private password is incorrect.",
        )

    response.set_cookie(
        key=COOKIE_NAME,
        value=create_token(),
        max_age=settings.private_ui_session_hours * 3600,
        httponly=True,
        secure=settings.private_ui_cookie_secure,
        samesite=settings.private_ui_cookie_samesite,
        path="/",
    )
    return {"status": "ok", "authenticated": True}


@app.post("/private/api/logout")
def private_logout(response: Response) -> dict[str, str]:
    response.delete_cookie(
        COOKIE_NAME,
        path="/",
    )
    return {"status": "ok"}


@app.get(
    "/private/api/session",
    dependencies=[Depends(require_session)],
)
def private_session() -> dict[str, Any]:
    return {
        "status": "ok",
        "authenticated": True,
        "version": settings.version,
    }


@app.get(
    "/private/api/fixtures",
    dependencies=[Depends(require_session)],
)
def private_fixtures(
    window: str = Query(
        default="today",
        pattern="^(today|tomorrow|3days|3months)$",
    ),
    search: str = Query(default="", max_length=120),
) -> dict[str, Any]:
    fixtures = list_fixtures(
        window=window,
        search=search,
    )
    return {
        "status": "ok",
        "window": window,
        "count": len(fixtures),
        "fixtures": fixtures,
        "provider_call_made": False,
    }


@app.post(
    "/private/api/sync-football-data",
    dependencies=[Depends(require_session)],
)
def private_sync() -> dict[str, Any]:
    result = sync_now(reason="private_ui_button")
    if result.get("status") == "error":
        raise HTTPException(status_code=500, detail=result)
    if result.get("status") == "provider_error":
        raise HTTPException(status_code=503, detail=result)
    return result


@app.get(
    "/private/api/sync-status",
    dependencies=[Depends(require_session)],
)
def private_sync_status() -> dict[str, Any]:
    raw = get("predict2_football_data_last_sync_audit")
    audit = None
    if raw:
        try:
            audit = json.loads(raw)
        except ValueError:
            audit = {"raw": raw[:1000]}

    last = get_datetime(
        "predict2_football_data_last_sync_at"
    )
    return {
        "status": "ok",
        "last_sync_at": last.isoformat() if last else None,
        "last_audit": audit,
        "checked_at": datetime.now(
            timezone.utc
        ).isoformat(),
    }
