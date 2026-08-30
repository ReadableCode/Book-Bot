"""Storage layer.

PostgrestStore is the only implementation. It talks to the shared PostgREST
deployment (same one load-log uses), forwarding the caller's JWT; the
book_bot schema is selected with Accept-Profile / Content-Profile headers.
Row-level security in Postgres (deploy/04_user_libraries.sql) enforces
library membership even for clients that bypass the app.

There is deliberately no second backend. A missing POSTGRES/PostgREST
config fails at import rather than falling back to a local file — a
fallback that silently accepts writes is how you end up running against
an empty database and not noticing.

Data model:
  works          — one row per *book* (the story itself), keyed by the Open
                   Library work key when known, else a normalized
                   title+author key. Shared catalog, owned by nobody.
  editions       — one row per ISBN: catalog metadata only. Also shared.
  libraries      — a shelf; several users can own the same one.
  library_members— which users own which libraries.
  library_books  — a library's copy/copies of an edition, with status
                   ('library' or 'wishlist'), notes and a copies count.
  read_states    — per-user reading history keyed to the work: status
                   ('want_to_read' / 'reading' / 'read'), rating, notes,
                   started/finished dates. Independent of ownership, so
                   "read but don't own" is a plain query. This is where a
                   future Goodreads sync lands shelves/read-dates/ratings
                   (editions match by isbn13, works by norm_key).
  users          — credentials (argon2id; legacy bcrypt); in postgres this table is only
                   readable by the auth service, never through PostgREST —
                   the user_directory view exposes usernames only.
"""

import json
import uuid
from datetime import datetime, timezone

import requests

from . import config

EDITION_FIELDS = [
    "id", "work_id", "isbn13", "isbn10", "title", "subtitle", "authors",
    "publisher", "published_date", "description", "format", "cover_url",
    "google_volume_id", "ol_edition_key", "page_count", "language", "genre",
    "added_at",
]

WORK_FIELDS = ["id", "ol_work_key", "norm_key", "title", "authors", "cover_url", "created_at"]

LIBRARY_BOOK_FIELDS = [
    "id", "library_id", "edition_id", "status", "notes", "copies",
    "added_at", "status_changed_at",
]

READ_STATE_FIELDS = [
    "id", "user_id", "work_id", "edition_id", "status", "rating", "notes",
    "started_at", "finished_at", "created_at", "updated_at",
]

READ_STATUSES = ("want_to_read", "reading", "read")

# 'library' = physical copy on the shelf; 'digital' = owned as
# ebook/audiobook/file; 'wishlist' = wanted.
HOLDING_STATUSES = ("library", "digital", "wishlist")


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def new_id() -> str:
    return str(uuid.uuid4())


class StoreError(Exception):
    def __init__(self, message: str, status_code: int = 500):
        super().__init__(message)
        self.status_code = status_code


# --------------------------------------------------------------------------
# PostgREST implementation
# --------------------------------------------------------------------------

class PostgrestStore:
    def __init__(self):
        self.base = config.POSTGREST_URL

    def _headers(self, token: str, write: bool = False, minimal: bool = False) -> dict:
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept-Profile": config.APP_SCHEMA,
        }
        if write:
            headers["Content-Profile"] = config.APP_SCHEMA
            headers["Content-Type"] = "application/json"
            headers["Prefer"] = "return=minimal" if minimal else "return=representation"
        return headers

    def _request(self, method: str, table: str, token: str, params=None, body=None, minimal=False):
        resp = requests.request(
            method,
            f"{self.base}/{table}",
            headers=self._headers(token, write=method in ("POST", "PATCH", "DELETE"), minimal=minimal),
            params=params,
            data=json.dumps(body) if body is not None else None,
            timeout=config.HTTP_TIMEOUT,
        )
        if resp.status_code == 401:
            raise StoreError("session expired — log in again", 401)
        if resp.status_code == 400 and "22P02" in resp.text:
            # malformed uuid in an eq. filter — the row can't exist, so
            # answer 404 instead of surfacing a 502
            raise StoreError("not found", 404)
        if resp.status_code >= 400:
            snippet = resp.text[:300]
            # a proxy in the chain (Cloudflare/SWAG) answered with an HTML
            # error page — don't dump markup into the UI
            if snippet.lstrip().startswith("<"):
                snippet = "upstream proxy error — try again in a moment"
            raise StoreError(f"database error ({resp.status_code}): {snippet}", 502)
        if resp.text:
            return resp.json()
        return []

    @staticmethod
    def _in(values):
        return "in.(" + ",".join(f'"{v}"' for v in values) + ")"

    # The proxy chain in front of PostgREST (Cloudflare -> SWAG/nginx)
    # rejects request lines beyond ~4KB — about 90 quoted uuids in one
    # in.() filter — killing the connection (surfaces as 502/520). Any
    # query filtering on an unbounded id list must go out in chunks.
    CHUNK_IDS = 50

    @classmethod
    def _chunks(cls, values):
        values = list(values)
        for i in range(0, len(values), cls.CHUNK_IDS):
            yield values[i:i + cls.CHUNK_IDS]

    # -- works ------------------------------------------------------------

    def get_work(self, token, work_id):
        rows = self._request("GET", "works", token, {"id": f"eq.{work_id}", "limit": 1})
        return rows[0] if rows else None

    def find_work(self, token, ol_work_key, nkey):
        if ol_work_key:
            rows = self._request("GET", "works", token, {"ol_work_key": f"eq.{ol_work_key}", "limit": 1})
            if rows:
                return rows[0]
        if nkey:
            rows = self._request("GET", "works", token, {"norm_key": f"eq.{nkey}", "limit": 1})
            if rows:
                return rows[0]
        return None

    def create_work(self, token, work):
        rows = self._request("POST", "works", token, body=work)
        return rows[0]

    def update_work(self, token, work_id, fields):
        self._request("PATCH", "works", token, {"id": f"eq.{work_id}"}, body=fields)

    def works_by_norm_keys(self, token, nkeys, ol_keys):
        clauses = []
        if nkeys:
            vals = ",".join(f'"{k}"' for k in nkeys)
            clauses.append(f"norm_key.in.({vals})")
        if ol_keys:
            vals = ",".join(f'"{k}"' for k in ol_keys)
            clauses.append(f"ol_work_key.in.({vals})")
        if not clauses:
            return []
        return self._request("GET", "works", token, {"or": f"({','.join(clauses)})"})

    # -- editions (shared catalog) -----------------------------------------

    def get_edition_by_isbn(self, token, isbn13):
        rows = self._request("GET", "editions", token, {"isbn13": f"eq.{isbn13}", "limit": 1})
        return rows[0] if rows else None

    def get_edition(self, token, edition_id):
        rows = self._request("GET", "editions", token, {"id": f"eq.{edition_id}", "limit": 1})
        return rows[0] if rows else None

    def editions_for_work(self, token, work_id):
        return self._request("GET", "editions", token, {"work_id": f"eq.{work_id}", "order": "added_at.asc"})

    def editions_for_works(self, token, work_ids):
        if not work_ids:
            return []
        rows = []
        for chunk in self._chunks(work_ids):
            rows += self._request("GET", "editions", token, {"work_id": self._in(chunk)})
        return rows

    def insert_edition(self, token, edition):
        rows = self._request("POST", "editions", token, body=edition)
        return rows[0]

    def update_edition(self, token, edition_id, fields):
        rows = self._request("PATCH", "editions", token, {"id": f"eq.{edition_id}"}, body=fields)
        return rows[0] if rows else None

    # -- libraries + members ------------------------------------------------

    def libraries_for_user(self, token, user_id):
        rows = self._request("GET", "library_members", token, {
            "user_id": f"eq.{user_id}",
            "select": "role,added_at,library:libraries(*)",
            "order": "added_at.asc",
        })
        return [{**r["library"], "role": r["role"]} for r in rows if r.get("library")]

    def get_library(self, token, library_id):
        rows = self._request("GET", "libraries", token, {"id": f"eq.{library_id}", "limit": 1})
        return rows[0] if rows else None

    def create_library(self, token, library):
        # return=minimal: the caller isn't a member yet, so the row would be
        # invisible to RLS in a RETURNING clause.
        self._request("POST", "libraries", token, body=library, minimal=True)
        return library

    def update_library(self, token, library_id, fields):
        self._request("PATCH", "libraries", token, {"id": f"eq.{library_id}"}, body=fields)

    def add_member(self, token, member):
        self._request("POST", "library_members", token, body=member, minimal=True)
        return member

    def members_for_libraries(self, token, library_ids):
        if not library_ids:
            return []
        members = self._request("GET", "library_members", token, {
            "library_id": self._in(library_ids), "order": "added_at.asc",
        })
        names = self.usernames_for_ids(token, [m["user_id"] for m in members])
        for m in members:
            m["username"] = names.get(str(m["user_id"]))
        return members

    # -- user directory (usernames only; hashes stay superuser-only) --------

    def find_user_by_username(self, token, username):
        rows = self._request("GET", "user_directory", token, {"username": f"eq.{username}", "limit": 1})
        return rows[0] if rows else None

    def usernames_for_ids(self, token, user_ids):
        if not user_ids:
            return {}
        names = {}
        for chunk in self._chunks(set(user_ids)):
            rows = self._request("GET", "user_directory", token, {"id": self._in(chunk)})
            names.update({str(r["id"]): r["username"] for r in rows})
        return names

    # -- library books (holdings) -------------------------------------------

    def list_library_books(self, token, library_ids, status=None):
        if not library_ids:
            return []
        params = {
            "library_id": self._in(library_ids),
            "select": "*,edition:editions(*)",
            "order": "added_at.desc",
        }
        if status:
            params["status"] = f"eq.{status}"
        return self._request("GET", "library_books", token, params)

    def get_library_book(self, token, book_id):
        rows = self._request("GET", "library_books", token, {
            "id": f"eq.{book_id}", "select": "*,edition:editions(*)", "limit": 1,
        })
        return rows[0] if rows else None

    def find_library_book(self, token, library_id, edition_id):
        rows = self._request("GET", "library_books", token, {
            "library_id": f"eq.{library_id}", "edition_id": f"eq.{edition_id}", "limit": 1,
        })
        return rows[0] if rows else None

    def holdings_for_editions(self, token, library_ids, edition_ids):
        if not library_ids or not edition_ids:
            return []
        rows = []
        for chunk in self._chunks(edition_ids):
            rows += self._request("GET", "library_books", token, {
                "library_id": self._in(library_ids), "edition_id": self._in(chunk),
            })
        return rows

    def insert_library_book(self, token, book):
        rows = self._request("POST", "library_books", token, body=book)
        return rows[0]

    def update_library_book(self, token, book_id, fields):
        rows = self._request("PATCH", "library_books", token, {"id": f"eq.{book_id}"}, body=fields)
        return rows[0] if rows else None

    def delete_library_book(self, token, book_id):
        self._request("DELETE", "library_books", token, {"id": f"eq.{book_id}"})

    # -- read states ----------------------------------------------------------

    def read_states_for_user(self, token, user_id):
        return self._request("GET", "read_states", token, {
            "user_id": f"eq.{user_id}",
            "select": "*,work:works(*)",
            "order": "updated_at.desc",
        })

    def get_read_state(self, token, user_id, work_id):
        rows = self._request("GET", "read_states", token, {
            "user_id": f"eq.{user_id}", "work_id": f"eq.{work_id}", "limit": 1,
        })
        return rows[0] if rows else None

    def read_states_for_works(self, token, user_id, work_ids):
        if not work_ids:
            return []
        rows = []
        for chunk in self._chunks(work_ids):
            rows += self._request("GET", "read_states", token, {
                "user_id": f"eq.{user_id}", "work_id": self._in(chunk),
            })
        return rows

    def insert_read_state(self, token, state):
        rows = self._request("POST", "read_states", token, body=state)
        return rows[0]

    def update_read_state(self, token, user_id, work_id, fields):
        rows = self._request("PATCH", "read_states", token,
                             {"user_id": f"eq.{user_id}", "work_id": f"eq.{work_id}"}, body=fields)
        return rows[0] if rows else None

    def delete_read_state(self, token, user_id, work_id):
        self._request("DELETE", "read_states", token,
                      {"user_id": f"eq.{user_id}", "work_id": f"eq.{work_id}"})


def postgrest_reachable() -> tuple[bool, str]:
    """(ok, detail) for the PostgREST deployment. Tests assert on this so
    an unreachable dependency is a red test, never a skipped one."""
    try:
        resp = requests.get(f"{config.POSTGREST_URL}/",
                            headers={"Accept-Profile": config.APP_SCHEMA},
                            timeout=config.HTTP_TIMEOUT)
    except requests.RequestException as exc:
        return False, str(exc)
    if resp.status_code >= 400:
        return False, f"HTTP {resp.status_code}: {resp.text[:200]}"
    return True, "ok"


_store = None


def get_store():
    global _store
    if _store is None:
        _store = PostgrestStore()
    return _store
