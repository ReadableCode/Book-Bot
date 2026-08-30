"""Runtime configuration.

One backend, no modes. Data goes through the shared PostgREST deployment
and logins through the postgrest-auth service (same pattern as load-log).

POSTGREST_URL and JWT_SECRET are required: a missing or misspelled one
raises here, at import, rather than degrading to something that accepts
writes nobody will ever see. Local development points at the real
deployment, which is the documented approach for the rest of the fleet.
"""

import os

from dotenv import load_dotenv

load_dotenv()

POSTGREST_URL = os.environ.get("POSTGREST_URL", "").rstrip("/")
if not POSTGREST_URL:
    raise RuntimeError(
        "POSTGREST_URL must be set — Book-Bot has no local fallback store. "
        "Point it at the shared deployment (https://pgrest.tinkernet.me).")

AUTH_URL = os.environ.get("AUTH_URL", "https://auth.tinkernet.me").rstrip("/")
APP_SCHEMA = os.environ.get("APP_SCHEMA", "book_bot")

# Must match the auth service / PostgREST secret so the backend can verify
# the tokens it forwards. POSTGREST_JWT_SECRET is the name that secret has
# in personal_credentials/personal.env (the repo's .env symlink), so accept
# it as a fallback.
JWT_SECRET = os.environ.get("JWT_SECRET") or os.environ.get("POSTGREST_JWT_SECRET", "")
if not JWT_SECRET:
    raise RuntimeError("JWT_SECRET (or POSTGREST_JWT_SECRET) must be set")

# self-signup (open by default now that the app owns its own login
# hardening; set SIGNUP_ENABLED=false to go invite-only via create_user.py)
SIGNUP_ENABLED = os.environ.get("SIGNUP_ENABLED", "true").strip().lower() not in ("0", "false", "no")

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
