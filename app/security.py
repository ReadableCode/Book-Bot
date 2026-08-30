"""Login hardening — the same posture Sync_Plex ships, adapted to
stateless-JWT FastAPI. Book-bot no longer sits behind Authelia, so the
edge protections it provided move into the app:

  - LoginRateLimiter: in-memory failure tracking keyed per-username AND
    per-client-IP; 5 failures inside 15 minutes locks that key for 15
    minutes (mirrors Authelia's regulation block). In-memory is fine for
    a single uvicorn process; it resets on restart.
  - username/password policy for self-signup.
  - a dummy argon2id hash so dev-mode login costs the same for unknown
    users as for wrong passwords (no user enumeration by timing).

Password hashing: argon2id is the standard (matching the shared
postgrest-auth service). Stored hashes are self-identifying by prefix,
so legacy bcrypt hashes still verify — the auth service rehashes them
to argon2id on the next successful production login.

TLS still terminates at the SWAG proxy in front.
"""

import re
import threading
import time

import bcrypt
from argon2 import PasswordHasher
from argon2.exceptions import InvalidHashError, VerificationError
from fastapi import Request

MAX_FAILURES = 5
WINDOW_SECONDS = 15 * 60
LOCKOUT_SECONDS = 15 * 60

MIN_PASSWORD_LENGTH = 10

_USERNAME_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{0,31}$")

_hasher = PasswordHasher()

# verified for unknown usernames so the reject path always pays the
# KDF cost — never compare against it with a real password expecting
# a match.
DUMMY_HASH = _hasher.hash("book-bot-dummy-password")


def hash_password(password: str) -> str:
    return _hasher.hash(password)


def verify_password(password: str, stored_hash: str) -> bool:
    """Prefix-dispatched verify: $2* → bcrypt (legacy), else argon2id."""
    if stored_hash.startswith("$2"):
        try:
            return bcrypt.checkpw(password.encode(), stored_hash.encode())
        except ValueError:
            return False
    try:
        _hasher.verify(stored_hash, password)
        return True
    except (VerificationError, InvalidHashError):
        return False


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
    forwarded = request.headers.get("X-Forwarded-For", "")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


class LoginRateLimiter:
    """Track failures per key; too many inside the window locks the key."""

    def __init__(self, max_failures: int = MAX_FAILURES,
                 window: float = WINDOW_SECONDS, lockout: float = LOCKOUT_SECONDS):
        self.max_failures = max_failures
        self.window = window
        self.lockout = lockout
        self._failures: dict[str, list[float]] = {}
        self._locked_until: dict[str, float] = {}
        self._lock = threading.Lock()

    def locked_for(self, *keys: str) -> float:
        """Seconds until the most-locked of the keys unlocks (0 = open)."""
        now = time.monotonic()
        with self._lock:
            remaining = 0.0
            for key in keys:
                until = self._locked_until.get(key, 0.0)
                if until > now:
                    remaining = max(remaining, until - now)
                elif key in self._locked_until:
                    del self._locked_until[key]
            return remaining

    def record_failure(self, *keys: str) -> None:
        now = time.monotonic()
        with self._lock:
            for key in keys:
                hits = [t for t in self._failures.get(key, []) if now - t < self.window]
                hits.append(now)
                self._failures[key] = hits
                if len(hits) >= self.max_failures:
                    self._locked_until[key] = now + self.lockout
                    self._failures[key] = []

    def record_success(self, *keys: str) -> None:
        with self._lock:
            for key in keys:
                self._failures.pop(key, None)
                self._locked_until.pop(key, None)
