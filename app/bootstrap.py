"""Startup self-bootstrap: converge the database, wherever the app runs.

Schema setup is database state, not deployment plumbing — so it runs from
FastAPI startup (app/main.py lifespan), identically for a local
`uv run uvicorn` and for the container. Docker has no special role; its
entrypoint just execs uvicorn.

  apply_schema()  with the superuser POSTGRES_* env: idempotently applies
                  deploy/*.sql (book-bot's `alembic upgrade head`) and
                  reloads PostgREST's schema cache. Skipped without creds.

It is best-effort at startup: failures are logged, the app still serves,
and the next startup retries. scripts/init_db.py is a thin CLI wrapper
over the same function for running it by hand.
"""

import logging
import os

from . import config
from .db import superuser_conn, superuser_env

log = logging.getLogger("bookbot.bootstrap")

SCHEMA_FILES = ("02_schema.sql", "03_secure_users.sql", "04_user_libraries.sql",
                "06_drop_sample_library.sql")

# Bump when deploy/*.sql changes. Startup compares this against
# book_bot.deploy_meta and skips ALL DDL (and the PostgREST cache reload)
# when they match — the SQL files are idempotent but not free: they take
# ACCESS EXCLUSIVE locks (DROP/ADD CONSTRAINT, DROP/CREATE POLICY) and
# reload PostgREST's schema cache, which disrupts live traffic. Schema
# work must happen once per schema change, not once per process start.
SCHEMA_VERSION = 7

ROLE_SQL = """
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'book_bot_user') THEN
        CREATE ROLE book_bot_user NOLOGIN;
    END IF;
END
$$;
GRANT book_bot_user TO postgrest_authenticator;
"""

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _applied_version(cur) -> int | None:
    import psycopg2
    try:
        cur.execute(f"SELECT version FROM {config.APP_SCHEMA}.deploy_meta LIMIT 1")
        row = cur.fetchone()
        return row[0] if row else None
    except psycopg2.errors.UndefinedTable:
        return None  # pre-versioning database (or brand new)


def apply_schema(force: bool = False) -> bool:
    """book_bot role/schema setup against the shared apps database
    (postgrest mode; requires the superuser POSTGRES_* env). Version-gated:
    when book_bot.deploy_meta already records SCHEMA_VERSION this is a
    single cheap SELECT — no DDL, no locks, no PostgREST reload. Returns
    True when the schema files were actually applied."""
    conn = superuser_conn()
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            if not force and _applied_version(cur) == SCHEMA_VERSION:
                log.info("schema already at version %s — nothing to apply", SCHEMA_VERSION)
                return False
            cur.execute(
                "SELECT rolname FROM pg_roles WHERE rolname IN ('postgrest_authenticator', 'web_anon')")
            found = {row[0] for row in cur.fetchall()}
            missing = {"postgrest_authenticator", "web_anon"} - found
            if missing:
                raise RuntimeError(
                    f"missing cluster roles: {', '.join(sorted(missing))} — "
                    "run load-log's deploy/01_create_roles.sql first (shared PostgREST setup)")
            cur.execute(ROLE_SQL)
            for name in SCHEMA_FILES:
                log.info("applying deploy/%s", name)
                with open(os.path.join(_REPO_ROOT, "deploy", name)) as f:
                    cur.execute(f.read())
            # marker table is deliberately not granted to any PostgREST role
            cur.execute(
                f"CREATE TABLE IF NOT EXISTS {config.APP_SCHEMA}.deploy_meta (version int NOT NULL)")
            cur.execute(f"DELETE FROM {config.APP_SCHEMA}.deploy_meta")
            cur.execute(
                f"INSERT INTO {config.APP_SCHEMA}.deploy_meta (version) VALUES (%s)",
                (SCHEMA_VERSION,))
            # PostgREST caches the schema; reload only when it changed
            cur.execute("NOTIFY pgrst, 'reload schema'")
    finally:
        conn.close()
    log.info("book_bot schema updated to version %s", SCHEMA_VERSION)
    return True


def run() -> None:
    """Called from the FastAPI lifespan. Synchronous — the app shouldn't
    serve against a stale schema. Never raises: the app must come up
    regardless."""
    # uvicorn only configures its own loggers; make bootstrap progress
    # visible in the server output without reconfiguring anything global
    if not logging.getLogger().handlers and not log.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(levelname)s:     [bootstrap] %(message)s"))
        log.addHandler(handler)
        log.setLevel(logging.INFO)
    if not superuser_env():
        log.info("no superuser POSTGRES_* env — skipping schema bootstrap")
        return
    try:
        apply_schema()
    except Exception as exc:
        log.error("schema bootstrap failed (will retry next startup): %s", exc)
