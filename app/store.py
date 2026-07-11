"""Storage layer.

Two implementations of the same interface:

  PostgrestStore — production. Talks to the shared PostgREST deployment
      (same one load-log uses), forwarding the caller's JWT. The book_bot
      schema is selected with Accept-Profile / Content-Profile headers.

  SqliteStore — local dev/demo. Same tables in a SQLite file so the whole
      app runs with nothing but `uv run`.

Data model:
  works    — one row per *book* (the story itself), keyed by the Open
             Library work key when known, else a normalized title+author
             key. This is what makes "do I already have this in hardback?"
             answerable when a scanned ISBN isn't in the library.
  editions — one row per physical/each ISBN. Carries status
             ('library' or 'wishlist'), format, notes, copies.
  users    — credentials (bcrypt); in postgres this table is only readable
             by the auth service, never through PostgREST.
"""

import json
import sqlite3
import threading
import uuid
from datetime import datetime, timezone

import requests

from . import config

EDITION_FIELDS = [
    "id", "work_id", "isbn13", "isbn10", "title", "subtitle", "authors",
    "publisher", "published_date", "description", "format", "cover_url",
    "google_volume_id", "ol_edition_key", "page_count", "language",
    "genre", "status", "notes", "copies", "added_at", "status_changed_at",
]

WORK_FIELDS = ["id", "ol_work_key", "norm_key", "title", "authors", "created_at"]


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

    def _headers(self, token: str, write: bool = False) -> dict:
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept-Profile": config.APP_SCHEMA,
        }
        if write:
            headers["Content-Profile"] = config.APP_SCHEMA
            headers["Content-Type"] = "application/json"
            headers["Prefer"] = "return=representation"
        return headers

    def _request(self, method: str, table: str, token: str, params=None, body=None):
        resp = requests.request(
            method,
            f"{self.base}/{table}",
            headers=self._headers(token, write=method in ("POST", "PATCH", "DELETE")),
            params=params,
            data=json.dumps(body) if body is not None else None,
            timeout=config.HTTP_TIMEOUT,
        )
        if resp.status_code == 401:
            raise StoreError("session expired — log in again", 401)
        if resp.status_code >= 400:
            raise StoreError(f"database error ({resp.status_code}): {resp.text[:300]}", 502)
        if resp.text:
            return resp.json()
        return []

    # -- works ------------------------------------------------------------

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

    # -- editions ---------------------------------------------------------

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
        vals = ",".join(f'"{w}"' for w in work_ids)
        return self._request("GET", "editions", token, {"work_id": f"in.({vals})"})

    def insert_edition(self, token, edition):
        rows = self._request("POST", "editions", token, body=edition)
        return rows[0]

    def update_edition(self, token, edition_id, fields):
        rows = self._request("PATCH", "editions", token, {"id": f"eq.{edition_id}"}, body=fields)
        return rows[0] if rows else None

    def delete_edition(self, token, edition_id):
        self._request("DELETE", "editions", token, {"id": f"eq.{edition_id}"})

    def list_editions(self, token, status=None, q=None):
        params = {"order": "added_at.desc"}
        if status:
            params["status"] = f"eq.{status}"
        if q:
            terms = [t for t in q.split() if t]
            if len(terms) == 1 and q.isdigit():
                params["or"] = f"(isbn13.eq.{q},title.ilike.*{q}*)"
            else:
                # every term must appear in title or authors
                clauses = ",".join(
                    f"or(title.ilike.*{t}*,authors.ilike.*{t}*)" for t in terms
                )
                params["and"] = f"({clauses})"
        return self._request("GET", "editions", token, params)

    def stats(self, token):
        rows = self._request("GET", "editions", token, {"select": "status,work_id"})
        return _stats_from_rows(rows)

    # dev login only — never used in postgrest mode
    def get_user(self, username):
        raise StoreError("direct user lookup is not available in postgrest mode", 500)


# --------------------------------------------------------------------------
# SQLite implementation (dev mode)
# --------------------------------------------------------------------------

SQLITE_SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    username TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS works (
    id TEXT PRIMARY KEY,
    ol_work_key TEXT UNIQUE,
    norm_key TEXT UNIQUE NOT NULL,
    title TEXT NOT NULL,
    authors TEXT,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS editions (
    id TEXT PRIMARY KEY,
    work_id TEXT NOT NULL REFERENCES works(id),
    isbn13 TEXT UNIQUE,
    isbn10 TEXT,
    title TEXT NOT NULL,
    subtitle TEXT,
    authors TEXT,
    publisher TEXT,
    published_date TEXT,
    description TEXT,
    format TEXT,
    cover_url TEXT,
    google_volume_id TEXT,
    ol_edition_key TEXT,
    page_count INTEGER,
    language TEXT,
    genre TEXT,
    status TEXT NOT NULL CHECK (status IN ('library', 'wishlist')),
    notes TEXT,
    copies INTEGER NOT NULL DEFAULT 1,
    added_at TEXT NOT NULL,
    status_changed_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_editions_work ON editions(work_id);
CREATE INDEX IF NOT EXISTS idx_editions_status ON editions(status);
"""


class SqliteStore:
    def __init__(self, path=None):
        import os
        self.path = path or config.SQLITE_PATH
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(self.path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(SQLITE_SCHEMA)
        self._conn.commit()
        self._migrate()

    def _migrate(self):
        """Bring pre-existing dev databases up to the current schema."""
        cols = {r["name"] for r in self._rows("PRAGMA table_info(editions)")}
        if "genre" not in cols:
            self._exec("ALTER TABLE editions ADD COLUMN genre TEXT")

    def _rows(self, query, params=()):
        with self._lock:
            cur = self._conn.execute(query, params)
            return [dict(r) for r in cur.fetchall()]

    def _exec(self, query, params=()):
        with self._lock:
            self._conn.execute(query, params)
            self._conn.commit()

    # -- works ------------------------------------------------------------

    def find_work(self, token, ol_work_key, nkey):
        if ol_work_key:
            rows = self._rows("SELECT * FROM works WHERE ol_work_key = ?", (ol_work_key,))
            if rows:
                return rows[0]
        if nkey:
            rows = self._rows("SELECT * FROM works WHERE norm_key = ?", (nkey,))
            if rows:
                return rows[0]
        return None

    def create_work(self, token, work):
        cols = ", ".join(work)
        marks = ", ".join("?" for _ in work)
        self._exec(f"INSERT INTO works ({cols}) VALUES ({marks})", tuple(work.values()))
        return work

    def update_work(self, token, work_id, fields):
        sets = ", ".join(f"{k} = ?" for k in fields)
        self._exec(f"UPDATE works SET {sets} WHERE id = ?", (*fields.values(), work_id))

    def works_by_norm_keys(self, token, nkeys, ol_keys):
        rows = []
        if nkeys:
            marks = ",".join("?" for _ in nkeys)
            rows += self._rows(f"SELECT * FROM works WHERE norm_key IN ({marks})", tuple(nkeys))
        if ol_keys:
            marks = ",".join("?" for _ in ol_keys)
            rows += self._rows(f"SELECT * FROM works WHERE ol_work_key IN ({marks})", tuple(ol_keys))
        seen, out = set(), []
        for r in rows:
            if r["id"] not in seen:
                seen.add(r["id"])
                out.append(r)
        return out

    # -- editions ---------------------------------------------------------

    def get_edition_by_isbn(self, token, isbn13):
        rows = self._rows("SELECT * FROM editions WHERE isbn13 = ?", (isbn13,))
        return rows[0] if rows else None

    def get_edition(self, token, edition_id):
        rows = self._rows("SELECT * FROM editions WHERE id = ?", (edition_id,))
        return rows[0] if rows else None

    def editions_for_work(self, token, work_id):
        return self._rows("SELECT * FROM editions WHERE work_id = ? ORDER BY added_at ASC", (work_id,))

    def editions_for_works(self, token, work_ids):
        if not work_ids:
            return []
        marks = ",".join("?" for _ in work_ids)
        return self._rows(f"SELECT * FROM editions WHERE work_id IN ({marks})", tuple(work_ids))

    def insert_edition(self, token, edition):
        cols = ", ".join(edition)
        marks = ", ".join("?" for _ in edition)
        self._exec(f"INSERT INTO editions ({cols}) VALUES ({marks})", tuple(edition.values()))
        return edition

    def update_edition(self, token, edition_id, fields):
        sets = ", ".join(f"{k} = ?" for k in fields)
        self._exec(f"UPDATE editions SET {sets} WHERE id = ?", (*fields.values(), edition_id))
        return self.get_edition(token, edition_id)

    def delete_edition(self, token, edition_id):
        self._exec("DELETE FROM editions WHERE id = ?", (edition_id,))

    def list_editions(self, token, status=None, q=None):
        query = "SELECT * FROM editions WHERE 1=1"
        params = []
        if status:
            query += " AND status = ?"
            params.append(status)
        if q:
            if q.isdigit():
                query += " AND (isbn13 = ? OR title LIKE ?)"
                params += [q, f"%{q}%"]
            else:
                for term in q.split():
                    query += " AND (title LIKE ? COLLATE NOCASE OR authors LIKE ? COLLATE NOCASE)"
                    params += [f"%{term}%", f"%{term}%"]
        query += " ORDER BY added_at DESC"
        return self._rows(query, tuple(params))

    def stats(self, token):
        return _stats_from_rows(self._rows("SELECT status, work_id FROM editions"))

    # -- users (dev login) --------------------------------------------------

    def get_user(self, username):
        rows = self._rows("SELECT * FROM users WHERE username = ?", (username,))
        return rows[0] if rows else None

    def create_user(self, username, password_hash):
        self._exec(
            "INSERT INTO users (id, username, password_hash, created_at) VALUES (?, ?, ?, ?)",
            (new_id(), username, password_hash, now_iso()),
        )


def _stats_from_rows(rows):
    library = [r for r in rows if r["status"] == "library"]
    wishlist = [r for r in rows if r["status"] == "wishlist"]
    return {
        "library": len(library),
        "wishlist": len(wishlist),
        "works": len({r["work_id"] for r in rows}),
    }


_store = None


def get_store():
    global _store
    if _store is None:
        _store = PostgrestStore() if config.MODE == "postgrest" else SqliteStore()
    return _store
