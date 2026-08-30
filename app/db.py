"""The superuser Postgres connection.

Conventions I2: application data goes through PostgREST, never here. The
only permitted direct connections are startup bootstrap, credential reads
at login, and account management — all three of which need
book_bot.users, which 03_secure_users.sql deliberately puts out of
PostgREST's reach.
"""

import os

import psycopg2

from . import config

REQUIRED = ("POSTGRES_URL", "POSTGRES_USER", "POSTGRES_PASSWORD")


def superuser_env() -> bool:
    return all(os.environ.get(k) for k in REQUIRED)


def superuser_conn():
    return psycopg2.connect(
        host=os.environ["POSTGRES_URL"],
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ.get("POSTGRES_DB", "apps"),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        connect_timeout=config.HTTP_TIMEOUT,
    )


def db_reachable() -> tuple[bool, str]:
    """(ok, detail) for the superuser connection. Tests assert on this so
    an unreachable database is a red test, never a skipped one."""
    missing = [k for k in REQUIRED if not os.environ.get(k)]
    if missing:
        return False, f"missing env: {', '.join(missing)}"
    try:
        conn = superuser_conn()
    except psycopg2.Error as exc:
        return False, str(exc).strip()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        return True, "ok"
    finally:
        conn.close()
