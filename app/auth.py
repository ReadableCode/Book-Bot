"""Login + JWT verification.

postgrest mode: credentials go to the postgrest-auth service
(AUTH_URL /token with schema=book_bot), which checks book_bot.users and
mints an HS256 JWT that PostgREST trusts. We verify the same secret here
before doing any work on a request.

dev mode: same check against the local SQLite users table, token minted
locally with the dev secret.
"""

from datetime import datetime, timedelta, timezone

import bcrypt
import jwt
import requests
from fastapi import HTTPException, Request

from . import config
from .store import get_store


def login(username: str, password: str) -> str:
    if config.MODE == "postgrest":
        resp = requests.post(
            f"{config.AUTH_URL}/token",
            json={"schema": config.APP_SCHEMA, "username": username, "password": password},
            timeout=config.HTTP_TIMEOUT,
        )
        if resp.status_code == 401:
            raise HTTPException(401, "invalid username or password")
        if resp.status_code >= 400:
            raise HTTPException(502, f"auth service error ({resp.status_code})")
        return resp.json()["token"]

    user = get_store().get_user(username)
    if not user or not bcrypt.checkpw(password.encode(), user["password_hash"].encode()):
        raise HTTPException(401, "invalid username or password")
    payload = {
        "role": f"{config.APP_SCHEMA}_user",
        "user_id": str(user["id"]),
        "exp": datetime.now(timezone.utc) + timedelta(hours=config.JWT_TTL_HOURS),
    }
    return jwt.encode(payload, config.JWT_SECRET, algorithm="HS256")


def require_token(request: Request) -> str:
    """FastAPI dependency: validates the Bearer token, returns it raw
    (so stores can forward it to PostgREST)."""
    header = request.headers.get("Authorization", "")
    if not header.startswith("Bearer "):
        raise HTTPException(401, "not logged in")
    token = header[7:]
    try:
        jwt.decode(token, config.JWT_SECRET, algorithms=["HS256"])
    except jwt.ExpiredSignatureError:
        raise HTTPException(401, "session expired — log in again")
    except jwt.InvalidTokenError:
        raise HTTPException(401, "invalid session — log in again")
    return token
