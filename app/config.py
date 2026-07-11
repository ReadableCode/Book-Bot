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

_DEFAULT_SQLITE = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "book_bot.db")
SQLITE_PATH = os.environ.get("SQLITE_PATH", _DEFAULT_SQLITE)

GOOGLE_BOOKS_API_KEY = os.environ.get("GOOGLE_BOOKS_API_KEY") or None

HTTP_TIMEOUT = 10
