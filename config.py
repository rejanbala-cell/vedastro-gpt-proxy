from __future__ import annotations

import base64
import hashlib
import hmac
import time

from fastapi import Cookie, HTTPException

from .config import settings


COOKIE_NAME = "predict2_private_session"


def _secret() -> bytes:
    value = settings.login_secret
    if not value:
        raise RuntimeError(
            "PRIVATE_UI_PASSWORD or PROXY_API_KEY is required."
        )
    return value.encode("utf-8")


def create_token() -> str:
    expires = int(
        time.time() + settings.private_ui_session_hours * 3600
    )
    payload = f"v1.{expires}"
    signature = hmac.new(
        _secret(),
        payload.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    raw = f"{payload}.{signature}".encode("utf-8")
    return base64.urlsafe_b64encode(
        raw
    ).decode("ascii").rstrip("=")


def verify_token(token: str | None) -> bool:
    if not token:
        return False
    try:
        padded = token + "=" * (-len(token) % 4)
        decoded = base64.urlsafe_b64decode(
            padded.encode("ascii")
        ).decode("utf-8")
        version, expires_text, signature = decoded.split(".", 2)
        if version != "v1":
            return False
        if int(expires_text) <= int(time.time()):
            return False
        payload = f"{version}.{expires_text}"
        expected = hmac.new(
            _secret(),
            payload.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        return hmac.compare_digest(signature, expected)
    except Exception:
        return False


def require_session(
    predict2_private_session: str | None = Cookie(
        default=None,
        alias=COOKIE_NAME,
    ),
) -> None:
    if not verify_token(predict2_private_session):
        raise HTTPException(
            status_code=401,
            detail="Private session is missing or expired.",
        )


def password_matches(value: str) -> bool:
    supplied = value.encode("utf-8")
    candidates = [
        candidate
        for candidate in (
            settings.private_ui_password,
            settings.proxy_api_key,
        )
        if candidate
    ]
    return any(
        hmac.compare_digest(
            supplied,
            candidate.encode("utf-8"),
        )
        for candidate in candidates
    )
