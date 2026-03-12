"""
Authentication utilities for Stock Fitness Agent.

Approach: JWT tokens stored in HTTP-only cookies.
  - Login  → issue JWT → set cookie → redirect /dashboard
  - Access → read cookie → decode JWT → get user
  - Logout → clear cookie → redirect /login

Passwords are hashed with bcrypt directly (avoids passlib/bcrypt 5.x compat issues).
JWT signed with SECRET_KEY env var.
"""
import os
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

import bcrypt
from jose import JWTError, jwt
from fastapi import Request
from fastapi.responses import RedirectResponse

log = logging.getLogger(__name__)

SECRET_KEY = os.environ.get("SECRET_KEY", "dev-insecure-key-change-before-deploy")
ALGORITHM = "HS256"
TOKEN_EXPIRE_DAYS = 7
COOKIE_NAME = "sfa_token"


def hash_password(plain: str) -> str:
    return bcrypt.hashpw(plain.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def verify_password(plain: str, hashed: str) -> bool:
    try:
        return bcrypt.checkpw(plain.encode("utf-8"), hashed.encode("utf-8"))
    except Exception:
        return False


def create_access_token(user_id: int, username: str) -> str:
    expire = datetime.now(timezone.utc) + timedelta(days=TOKEN_EXPIRE_DAYS)
    return jwt.encode(
        {"sub": str(user_id), "username": username, "exp": expire},
        SECRET_KEY,
        algorithm=ALGORITHM,
    )


def decode_token(token: str) -> Optional[dict]:
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return {"id": int(payload["sub"]), "username": payload["username"]}
    except JWTError:
        return None


def get_current_user(request: Request) -> Optional[dict]:
    """Return user dict if authenticated, else None."""
    token = request.cookies.get(COOKIE_NAME)
    if not token:
        return None
    return decode_token(token)


def login_required(request: Request) -> dict:
    """
    Use as a FastAPI dependency on page routes.
    Redirects to /login if not authenticated.
    Raises exception that FastAPI turns into a redirect response.
    """
    user = get_current_user(request)
    if not user:
        # FastAPI will catch HTTPException; for redirects from dependencies
        # we raise a special redirect exception
        raise _RedirectToLogin()
    return user


def set_auth_cookie(response, token: str, is_prod: bool = False) -> None:
    # No max_age = session cookie — expires when the browser is closed.
    # User must sign in again each new browser session.
    response.set_cookie(
        key=COOKIE_NAME,
        value=token,
        httponly=True,
        secure=is_prod,   # HTTPS only in production
        samesite="lax",
        path="/",
        # max_age intentionally omitted — session-only cookie
    )


def clear_auth_cookie(response) -> None:
    response.delete_cookie(key=COOKIE_NAME, path="/")


def is_production() -> bool:
    return os.environ.get("ENVIRONMENT", "development").lower() == "production"


class _RedirectToLogin(Exception):
    """Internal signal: user is not authenticated, redirect to /login."""
    pass
