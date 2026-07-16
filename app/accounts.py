"""Self-signup user creation.

dev mode: insert into the local SQLite users table.
postgrest mode: book_bot.users is deliberately unreachable through
PostgREST (03_secure_users.sql revokes it), so signup inserts directly
into Postgres with the superuser POSTGRES_* env — the exact path
scripts/create_user.py uses, just triggered by the API instead of the
CLI. The container already carries these vars for init_db.
"""

import os

import bcrypt
from fastapi import HTTPException

from . import config
from .store import get_store


def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()


def create_user(username: str, password: str) -> None:
    """Insert a new user; raises 409 if the username is taken."""
    password_hash = hash_password(password)

    if config.MODE == "dev":
        store = get_store()
        if store.get_user(username):
            raise HTTPException(409, "that username is taken")
        store.create_user(username, password_hash)
        return

    import psycopg2
    import psycopg2.errors

    try:
        conn = psycopg2.connect(
            host=os.environ["POSTGRES_URL"],
            port=os.environ.get("POSTGRES_PORT", "5432"),
            dbname=os.environ.get("POSTGRES_DB", "apps"),
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
            connect_timeout=config.HTTP_TIMEOUT,
        )
    except KeyError as exc:
        raise HTTPException(500, f"signup unavailable: {exc.args[0]} is not configured")
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO {config.APP_SCHEMA}.users (username, password_hash) VALUES (%s, %s)",
                (username, password_hash),
            )
    except psycopg2.errors.UniqueViolation:
        raise HTTPException(409, "that username is taken")
    finally:
        conn.close()
