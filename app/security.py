"""Login hardening the app owns, now that it no longer sits behind Authelia.

Password hashing, verification and the per-username/per-IP lockout come
from the postgrest-auth library (argon2id, legacy bcrypt verified by prefix
and rehashed on login, 5 failures inside 15 minutes locks a key for 15
minutes). This module adds what is specific to book-bot: the signup
username/password policy and reading the client IP off a FastAPI request.

TLS still terminates at the SWAG proxy in front.
"""

import re

import postgrest_auth
from fastapi import Request

MIN_PASSWORD_LENGTH = 10

_USERNAME_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{0,31}$")


def validate_username(username: str) -> str:
    username = username.strip().lower()
    if not _USERNAME_RE.match(username):
        raise ValueError(
            "username must be 1-32 chars: lowercase letters, digits, '.', '_' or '-', "
            "starting with a letter or digit"
        )
    return username


def validate_password(password: str) -> None:
    if len(password) < MIN_PASSWORD_LENGTH:
        raise ValueError(f"password must be at least {MIN_PASSWORD_LENGTH} characters")


def client_ip(request: Request) -> str:
    """First X-Forwarded-For hop when behind the proxy, else the socket
    peer. Spoofable on direct hits, but the username key of the rate
    limiter doesn't depend on it."""
    return postgrest_auth.client_ip(
        request.headers.get("X-Forwarded-For"),
        request.client.host if request.client else None,
    )
