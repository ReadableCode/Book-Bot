"""Login + JWT verification.

Credentials go to the postgrest-auth service (AUTH_URL /token with
schema=book_bot), which checks book_bot.users (argon2id standard, legacy
bcrypt rehashed on login) and mints an HS256 JWT that PostgREST trusts.
We verify the same secret here before doing any work on a request, and
compare the token's iat against the user row's password_changed_at (30 s
cache) so password change and disable revoke existing sessions statelessly.

This is the only login path. There is no local verification fallback.

Every token must carry the user's uuid as a "user_id" (or "sub") claim —
library membership and read states hang off it, both here and in the
row-level-security policies PostgREST enforces (deploy/04_user_libraries.sql).
"""

import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import jwt
import psycopg2
import requests
from fastapi import HTTPException, Request

from . import config
from .db import superuser_conn


@dataclass
class AuthContext:
    token: str  # raw JWT, forwarded to PostgREST
    user_id: str


def login(username: str, password: str, client_ip: str | None = None) -> str:
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


def _user_id_from_payload(payload: dict) -> str:
    user_id = payload.get("user_id") or payload.get("sub")
    if not user_id:
        raise HTTPException(
            401,
            "token carries no user_id claim — the auth service must mint one "
            "(see deploy/README.md); log in again",
        )
    return str(user_id)


_REVOKE_CACHE_TTL = 30.0
_revoke_cache: dict[str, tuple[float, tuple | None]] = {}


def _auth_row(username: str) -> tuple | None:
    """(disabled, password_changed_at) from book_bot.users, cached 30 s.
    Same superuser POSTGRES_* path as signup — the table is deliberately
    unreachable through PostgREST."""
    now = time.monotonic()
    hit = _revoke_cache.get(username)
    if hit and now - hit[0] < _REVOKE_CACHE_TTL:
        return hit[1]
    conn = superuser_conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                f"SELECT disabled, password_changed_at FROM {config.APP_SCHEMA}.users "
                "WHERE username = %s",
                (username,),
            )
            row = cur.fetchone()
    finally:
        conn.close()
    _revoke_cache[username] = (now, row)
    return row


def _check_revocation(payload: dict) -> None:
    """Reject sessions issued before the user's password_changed_at, and
    sessions for disabled or deleted users. Tokens from before the auth
    service minted iat/username force one re-login."""
    username = payload.get("username")
    issued_ts = payload.get("iat")
    if not username or issued_ts is None:
        raise HTTPException(401, "session predates the current auth service — log in again")
    try:
        row = _auth_row(username)
    except (psycopg2.Error, KeyError):
        raise HTTPException(503, "session validation unavailable — try again")
    if row is None or row[0]:
        raise HTTPException(401, "account unavailable — log in again")
    issued = datetime.fromtimestamp(float(issued_ts), tz=timezone.utc)
    # Small grace: iat is second-granular, password_changed_at is not.
    if issued + timedelta(seconds=1) < row[1]:
        raise HTTPException(401, "session expired — log in again")


def decode_token(token: str) -> AuthContext:
    try:
        payload = jwt.decode(token, config.JWT_SECRET, algorithms=["HS256"])
    except jwt.ExpiredSignatureError:
        raise HTTPException(401, "session expired — log in again")
    except jwt.InvalidTokenError:
        raise HTTPException(401, "invalid session — log in again")
    _check_revocation(payload)
    return AuthContext(token=token, user_id=_user_id_from_payload(payload))


def require_auth(request: Request) -> AuthContext:
    """FastAPI dependency: validates the Bearer token, returns the raw token
    (so stores can forward it to PostgREST) plus the caller's user id."""
    header = request.headers.get("Authorization", "")
    if not header.startswith("Bearer "):
        raise HTTPException(401, "not logged in")
    return decode_token(header[7:])
