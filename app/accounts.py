"""Account management.

book_bot.users is deliberately unreachable through PostgREST
(03_secure_users.sql revokes it), so everything here goes straight to
Postgres with the superuser POSTGRES_* env — the I2 exception for
credential management. Signup calls create_user() from the API;
scripts/create_user.py and scripts/manage_user.py call the same functions
from the CLI.

password_changed_at is what makes revocation work: app/auth.py rejects any
session whose iat predates it. So every operation that should end a live
session bumps it — password change, disable, and re-enable alike (a
re-enabled account must not resurrect the sessions it had before).

User-facing problems (name taken, no such user) raise ValueError, which
app/main.py turns into a 4xx. A missing superuser env raises RuntimeError
and surfaces as a 500 — that is a deployment fault, not a user's mistake.
"""

import psycopg2
import psycopg2.errors

from . import config, security
from .db import superuser_conn

USERS = f"{config.APP_SCHEMA}.users"


def hash_password(password: str) -> str:
    return security.hash_password(password)


def _connect():
    try:
        return superuser_conn()
    except KeyError as exc:
        raise RuntimeError(f"account management unavailable: {exc.args[0]} is not configured")


def _execute(sql: str, params: tuple):
    conn = _connect()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.rowcount
    finally:
        conn.close()


def create_user(username: str, password: str) -> None:
    """Insert a new user. Raises ValueError if the username is taken."""
    try:
        _execute(
            f"INSERT INTO {USERS} (username, password_hash) VALUES (%s, %s)",
            (username, hash_password(password)),
        )
    except psycopg2.errors.UniqueViolation:
        raise ValueError("that username is taken")


def get_user(username: str) -> dict | None:
    conn = _connect()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                f"SELECT id, username, password_hash, role, display_name, disabled, "
                f"created_at, password_changed_at FROM {USERS} WHERE username = %s",
                (username,),
            )
            row = cur.fetchone()
            if row is None:
                return None
            cols = ("id", "username", "password_hash", "role", "display_name",
                    "disabled", "created_at", "password_changed_at")
            return dict(zip(cols, row))
    finally:
        conn.close()


def set_password(username: str, password: str) -> None:
    """Change a password and revoke every session issued before now."""
    changed = _execute(
        f"UPDATE {USERS} SET password_hash = %s, password_changed_at = now() "
        "WHERE username = %s",
        (hash_password(password), username),
    )
    if not changed:
        raise ValueError(f"no user named {username!r}")


def set_disabled(username: str, disabled: bool) -> None:
    """Disable or re-enable an account. Both directions bump
    password_changed_at: disabling must kill live sessions immediately,
    and re-enabling must not bring the old ones back."""
    changed = _execute(
        f"UPDATE {USERS} SET disabled = %s, password_changed_at = now() "
        "WHERE username = %s",
        (disabled, username),
    )
    if not changed:
        raise ValueError(f"no user named {username!r}")


def remove_user(username: str) -> None:
    """Delete an account. library_members and read_states cascade (the
    reading history goes with them), but libraries and their shelved books
    do not — a library the user solely owned is left orphaned and can be
    reattached with scripts/manage_library.py add-member."""
    changed = _execute(f"DELETE FROM {USERS} WHERE username = %s", (username,))
    if not changed:
        raise ValueError(f"no user named {username!r}")
