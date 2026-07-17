"""Login + JWT verification.

postgrest mode: credentials go to the postgrest-auth service
(AUTH_URL /token with schema=book_bot), which checks book_bot.users and
mints an HS256 JWT that PostgREST trusts. We verify the same secret here
before doing any work on a request.

dev mode: same check against the local SQLite users table, token minted
locally with the dev secret.

Every token must carry the user's uuid as a "user_id" (or "sub") claim —
library membership and read states hang off it, both here and in the
row-level-security policies PostgREST enforces (deploy/04_user_libraries.sql).
"""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import bcrypt
import jwt
import requests
from fastapi import HTTPException, Request

from . import config, security
from .store import get_store


@dataclass
class AuthContext:
    token: str  # raw JWT, forwarded to PostgREST
    user_id: str


def login(username: str, password: str, client_ip: str | None = None) -> str:
    if config.MODE == "postgrest":
        # forward the browser's IP: the auth service keeps its own per-IP
        # lockout, and without this every book-bot user would share this
        # container's IP in that limiter
        headers = {"X-Forwarded-For": client_ip} if client_ip else {}
        resp = requests.post(
            f"{config.AUTH_URL}/token",
            json={"schema": config.APP_SCHEMA, "username": username, "password": password},
            headers=headers,
            timeout=config.HTTP_TIMEOUT,
        )
        if resp.status_code == 401:
            raise HTTPException(401, "invalid username or password")
        if resp.status_code == 429:
            try:
                detail = resp.json().get("detail", "")
            except ValueError:
                detail = ""
            raise HTTPException(429, detail or "too many attempts — try again later")
        if resp.status_code >= 400:
            raise HTTPException(502, f"auth service error ({resp.status_code})")
        return resp.json()["token"]

    user = get_store().get_user(username)
    # unknown users still pay the bcrypt cost (dummy hash) so the reject
    # path can't be timed to enumerate usernames
    hashed = user["password_hash"] if user else security.DUMMY_HASH
    if not bcrypt.checkpw(password.encode(), hashed.encode()) or not user:
        raise HTTPException(401, "invalid username or password")
    payload = {
        "role": f"{config.APP_SCHEMA}_user",
        "user_id": str(user["id"]),
        "exp": datetime.now(timezone.utc) + timedelta(hours=config.JWT_TTL_HOURS),
    }
    return jwt.encode(payload, config.JWT_SECRET, algorithm="HS256")


def _user_id_from_payload(payload: dict) -> str:
    user_id = payload.get("user_id") or payload.get("sub")
    if not user_id:
        raise HTTPException(
            401,
            "token carries no user_id claim — the auth service must mint one "
            "(see deploy/README.md); log in again",
        )
    return str(user_id)


def decode_token(token: str) -> AuthContext:
    try:
        payload = jwt.decode(token, config.JWT_SECRET, algorithms=["HS256"])
    except jwt.ExpiredSignatureError:
        raise HTTPException(401, "session expired — log in again")
    except jwt.InvalidTokenError:
        raise HTTPException(401, "invalid session — log in again")
    return AuthContext(token=token, user_id=_user_id_from_payload(payload))


def require_auth(request: Request) -> AuthContext:
    """FastAPI dependency: validates the Bearer token, returns the raw token
    (so stores can forward it to PostgREST) plus the caller's user id."""
    header = request.headers.get("Authorization", "")
    if not header.startswith("Bearer "):
        raise HTTPException(401, "not logged in")
    return decode_token(header[7:])
