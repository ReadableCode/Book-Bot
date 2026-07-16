"""Startup self-bootstrap: converge the database, wherever the app runs.

Schema setup and sample-library stocking are database state, not
deployment plumbing — so they run from FastAPI startup (app/main.py
lifespan), identically for a local `uv run uvicorn` and for the
container. Docker has no special role; its entrypoint just execs uvicorn.

  apply_schema()          postgrest mode, superuser POSTGRES_* env:
                          idempotently applies deploy/*.sql (book-bot's
                          `alembic upgrade head`) and reloads PostgREST's
                          schema cache. Skipped without creds.
  stock_sample_library()  fills the shared, view-only Sample Library from
                          app/sample_books.json; stocks only what the
                          shelf is missing (by ISBN), so a stocked shelf
                          is an immediate no-op and an interrupted run
                          resumes on the next startup. Runs in a
                          background thread so startup isn't blocked by
                          300 inserts.

Both are best-effort at startup: failures are logged, the app still
serves, and the next startup retries. scripts/init_db.py and
scripts/seed_sample_library.py are thin CLI wrappers over the same
functions for running them by hand.
"""

import json
import logging
import os
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone

from . import config

log = logging.getLogger("bookbot.bootstrap")

SCHEMA_FILES = ("02_schema.sql", "03_secure_users.sql", "04_user_libraries.sql",
                "05_sample_library.sql")

# Bump when deploy/*.sql changes. Startup compares this against
# book_bot.deploy_meta and skips ALL DDL (and the PostgREST cache reload)
# when they match — the SQL files are idempotent but not free: they take
# ACCESS EXCLUSIVE locks (DROP/ADD CONSTRAINT, DROP/CREATE POLICY) and
# reload PostgREST's schema cache, which disrupts live traffic. Schema
# work must happen once per schema change, not once per process start.
SCHEMA_VERSION = 5

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


def _superuser_env() -> bool:
    return all(os.environ.get(k) for k in ("POSTGRES_URL", "POSTGRES_USER", "POSTGRES_PASSWORD"))


def _superuser_conn():
    import psycopg2
    return psycopg2.connect(
        host=os.environ["POSTGRES_URL"],
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ.get("POSTGRES_DB", "apps"),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        connect_timeout=config.HTTP_TIMEOUT,
    )


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
    conn = _superuser_conn()
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


def _service_token() -> str:
    """A short-lived book_bot_user token for catalog reads/writes (the
    shared works/editions tables have no RLS; sample holdings don't go
    through PostgREST at all)."""
    import jwt
    return jwt.encode(
        {
            "role": f"{config.APP_SCHEMA}_user",
            "user_id": str(uuid.uuid4()),
            "exp": datetime.now(timezone.utc) + timedelta(hours=1),
        },
        config.JWT_SECRET,
        algorithm="HS256",
    )


def stock_sample_library(manifest_path: str | None = None, force: bool = False) -> dict:
    """Fill the Sample Library from the manifest. Books already on the
    shelf are skipped by ISBN up front, so a fully stocked shelf is an
    immediate no-op and an interrupted stocking resumes where it left off
    (the stocking thread dies with the process). force re-walks every
    manifest entry regardless. Returns counters for the CLI/tests."""
    from .auth import AuthContext
    from .main import _resolve_edition  # deferred: main imports this module
    from .store import get_store, new_id, now_iso

    store = get_store()
    auth = AuthContext(token=_service_token(), user_id="sample-stocker")

    shelf = store.list_library_books(auth.token, [config.SAMPLE_LIBRARY_ID])
    have_isbns = {(h.get("edition") or {}).get("isbn13") for h in shelf}

    with open(manifest_path or config.SAMPLE_BOOKS_PATH) as f:
        books = json.load(f)
    if not force:
        books = [m for m in books if m.get("isbn13") not in have_isbns]
        if not books:
            return {"added": 0, "existing": len(shelf), "failed": 0, "already_stocked": True}

    # holdings bypass PostgREST: the sample library has no members, so RLS
    # correctly refuses API writes — dev inserts into SQLite, prod as superuser
    conn = _superuser_conn() if config.MODE == "postgrest" else None
    if conn is not None:
        conn.autocommit = True

    added = existing = failed = 0
    for meta in books:
        try:
            edition = _resolve_edition(auth, meta, meta.get("format"))
            if conn is not None:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""INSERT INTO {config.APP_SCHEMA}.library_books
                            (id, library_id, edition_id, status, copies, added_at, status_changed_at)
                            VALUES (gen_random_uuid(), %s, %s, 'library', 1, now(), now())
                            ON CONFLICT (library_id, edition_id) DO NOTHING""",
                        (config.SAMPLE_LIBRARY_ID, str(edition["id"])),
                    )
                    inserted = cur.rowcount > 0
            elif store.find_library_book(auth.token, config.SAMPLE_LIBRARY_ID, edition["id"]):
                inserted = False
            else:
                store.insert_library_book(auth.token, {
                    "id": new_id(),
                    "library_id": config.SAMPLE_LIBRARY_ID,
                    "edition_id": edition["id"],
                    "status": "library",
                    "notes": None,
                    "copies": 1,
                    "added_at": now_iso(),
                    "status_changed_at": now_iso(),
                })
                inserted = True
            added += 1 if inserted else 0
            existing += 0 if inserted else 1
        except Exception as exc:
            failed += 1
            log.warning("sample stocking: %s (%s): %s",
                        meta.get("title"), meta.get("isbn13"), exc)
        if conn is not None:
            time.sleep(0.05)  # be gentle with the proxy chain in front of PostgREST
    if conn is not None:
        conn.close()
    return {"added": added, "existing": existing + len(shelf), "failed": failed,
            "already_stocked": False}


def run() -> None:
    """Called from the FastAPI lifespan. Schema first (synchronous — the
    app shouldn't serve against a stale schema), then stocking in the
    background. Never raises: the app must come up regardless."""
    # uvicorn only configures its own loggers; make bootstrap progress
    # visible in the server output without reconfiguring anything global
    if not logging.getLogger().handlers and not log.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(levelname)s:     [bootstrap] %(message)s"))
        log.addHandler(handler)
        log.setLevel(logging.INFO)
    if config.MODE == "postgrest":
        if _superuser_env():
            try:
                apply_schema()
            except Exception as exc:
                log.error("schema bootstrap failed (will retry next startup): %s", exc)
        else:
            log.info("no superuser POSTGRES_* env — skipping schema bootstrap")

    if not config.SAMPLE_AUTOSTOCK:
        return
    if config.MODE == "postgrest" and not _superuser_env():
        log.info("no superuser POSTGRES_* env — skipping sample stocking")
        return

    def _stock():
        try:
            result = stock_sample_library()
            if result["already_stocked"]:
                log.info("sample library already stocked (%s books)", result["existing"])
            else:
                log.info("sample library stocked: %(added)s added, %(existing)s existing, "
                         "%(failed)s failed", result)
        except Exception as exc:
            log.error("sample stocking failed (will retry next startup): %s", exc)

    threading.Thread(target=_stock, name="sample-stocker", daemon=True).start()
