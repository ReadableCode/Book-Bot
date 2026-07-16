"""Runtime configuration.

Two modes, decided by whether POSTGREST_URL is set:
  - "postgrest": production. Data via the shared PostgREST deployment,
    logins via the postgrest-auth service (same pattern as load-log).
  - "dev": local demo. Data in a SQLite file, logins against a local
    users table. No Postgres/Docker needed.
"""

import os

from dotenv import load_dotenv

load_dotenv()

POSTGREST_URL = os.environ.get("POSTGREST_URL", "").rstrip("/")
AUTH_URL = os.environ.get("AUTH_URL", "https://auth.tinkernet.me").rstrip("/")
APP_SCHEMA = os.environ.get("APP_SCHEMA", "book_bot")

MODE = "postgrest" if POSTGREST_URL else "dev"

# Must match the auth service / PostgREST secret in postgrest mode so the
# backend can verify the tokens it forwards. POSTGREST_JWT_SECRET is the
# name that secret has in personal_credentials/personal.env (the repo's
# .env symlink), so accept it as a fallback.
JWT_SECRET = os.environ.get("JWT_SECRET") or os.environ.get("POSTGREST_JWT_SECRET", "")
if not JWT_SECRET:
    if MODE == "postgrest":
        raise RuntimeError("JWT_SECRET must be set when POSTGREST_URL is configured")
    JWT_SECRET = "book-bot-dev-secret-do-not-use-in-prod"

JWT_TTL_HOURS = int(os.environ.get("JWT_TTL_HOURS", "720"))  # dev-mode logins

# self-signup (open by default now that the app owns its own login
# hardening; set SIGNUP_ENABLED=false to go invite-only via create_user.py)
SIGNUP_ENABLED = os.environ.get("SIGNUP_ENABLED", "true").strip().lower() not in ("0", "false", "no")

# The shared, view-only Sample Library: one library row with a fixed uuid
# that every logged-in user can browse and nobody can edit (RLS read
# policies in deploy/05_sample_library.sql key on this exact id, so it is
# not configurable per-env). scripts/seed_sample_library.py fills it from
# the SAMPLE_BOOKS_PATH manifest.
SAMPLE_LIBRARY_ID = "11111111-1111-1111-1111-111111111111"
SAMPLE_LIBRARY_NAME = "Sample Library"
SAMPLE_BOOKS_PATH = os.environ.get(
    "SAMPLE_BOOKS_PATH", os.path.join(os.path.dirname(__file__), "sample_books.json"))

# stock the sample library at startup when it's empty (app/bootstrap.py);
# false = only ever stock via scripts/seed_sample_library.py
SAMPLE_AUTOSTOCK = os.environ.get("SAMPLE_AUTOSTOCK", "true").strip().lower() not in ("0", "false", "no")

_DEFAULT_SQLITE = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "book_bot.db")
SQLITE_PATH = os.environ.get("SQLITE_PATH", _DEFAULT_SQLITE)

GOOGLE_BOOKS_API_KEY = os.environ.get("GOOGLE_BOOKS_API_KEY") or None

# Service account with the Books API enabled on its project. Anonymous
# Books API calls are quota-blocked (429) these days, so authenticated
# requests are the only reliable path. Same GOOGLE_SERVICE_ACCOUNT JSON
# blob the other repos' google_tools.py uses from personal.env.
import json as _json

_sa_raw = os.environ.get("GOOGLE_SERVICE_ACCOUNT")
try:
    GOOGLE_SERVICE_ACCOUNT_INFO = _json.loads(_sa_raw) if _sa_raw else None
except ValueError:
    GOOGLE_SERVICE_ACCOUNT_INFO = None

HTTP_TIMEOUT = 10
