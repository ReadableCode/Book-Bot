"""Storage layer.

Two implementations of the same interface:

  PostgrestStore — production. Talks to the shared PostgREST deployment
      (same one load-log uses), forwarding the caller's JWT. The book_bot
      schema is selected with Accept-Profile / Content-Profile headers.
      Row-level security in Postgres (deploy/04_user_libraries.sql)
      enforces library membership even for clients that bypass the app.

  SqliteStore — local dev/demo. Same tables in a SQLite file so the whole
      app runs with nothing but `uv run`. Auto-migrates databases created
      before multi-user libraries existed.

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
  users          — credentials (bcrypt); in postgres this table is only
                   readable by the auth service, never through PostgREST —
                   the user_directory view exposes usernames only.
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
    "google_volume_id", "ol_edition_key", "page_count", "language", "added_at",
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
            # malformed uuid in an eq. filter — the row can't exist; match
            # SqliteStore's not-found behavior instead of surfacing a 502
            raise StoreError("not found", 404)
        if resp.status_code >= 400:
            raise StoreError(f"database error ({resp.status_code}): {resp.text[:300]}", 502)
        if resp.text:
            return resp.json()
        return []

    @staticmethod
    def _in(values):
        return "in.(" + ",".join(f'"{v}"' for v in values) + ")"

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
        return self._request("GET", "editions", token, {"work_id": self._in(work_ids)})

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
        rows = self._request("GET", "user_directory", token, {"id": self._in(set(user_ids))})
        return {str(r["id"]): r["username"] for r in rows}

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
        return self._request("GET", "library_books", token, {
            "library_id": self._in(library_ids), "edition_id": self._in(edition_ids),
        })

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
        return self._request("GET", "read_states", token, {
            "user_id": f"eq.{user_id}", "work_id": self._in(work_ids),
        })

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
    cover_url TEXT,
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
    added_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_editions_work ON editions(work_id);
CREATE TABLE IF NOT EXISTS libraries (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS library_members (
    library_id TEXT NOT NULL REFERENCES libraries(id),
    user_id TEXT NOT NULL REFERENCES users(id),
    role TEXT NOT NULL DEFAULT 'owner',
    added_at TEXT NOT NULL,
    PRIMARY KEY (library_id, user_id)
);
CREATE TABLE IF NOT EXISTS library_books (
    id TEXT PRIMARY KEY,
    library_id TEXT NOT NULL REFERENCES libraries(id),
    edition_id TEXT NOT NULL REFERENCES editions(id),
    status TEXT NOT NULL CHECK (status IN ('library', 'wishlist')),
    notes TEXT,
    copies INTEGER NOT NULL DEFAULT 1,
    added_at TEXT NOT NULL,
    status_changed_at TEXT NOT NULL,
    UNIQUE (library_id, edition_id)
);
CREATE INDEX IF NOT EXISTS idx_library_books_library ON library_books(library_id);
CREATE INDEX IF NOT EXISTS idx_library_books_edition ON library_books(edition_id);
CREATE TABLE IF NOT EXISTS read_states (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL REFERENCES users(id),
    work_id TEXT NOT NULL REFERENCES works(id),
    edition_id TEXT REFERENCES editions(id),
    status TEXT NOT NULL CHECK (status IN ('want_to_read', 'reading', 'read')),
    rating INTEGER CHECK (rating BETWEEN 1 AND 5),
    notes TEXT,
    started_at TEXT,
    finished_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (user_id, work_id)
);
CREATE INDEX IF NOT EXISTS idx_read_states_user ON read_states(user_id);
"""

# editions rebuilt without the per-library ownership columns (SQLite can't
# DROP a column referenced by a table CHECK constraint).
SQLITE_EDITIONS_REBUILD = """
CREATE TABLE editions_new (
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
    added_at TEXT NOT NULL
);
"""


class SqliteStore:
    def __init__(self, path=None):
        import os
        self.path = path or config.SQLITE_PATH
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(self.path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._prepare_schema()
        self._migrate_legacy()

    def _prepare_schema(self):
        # pre-multi-user works tables lack cover_url; CREATE IF NOT EXISTS
        # won't add it, so patch it in before applying the schema.
        cur = self._conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'works'")
        if cur.fetchone():
            columns = {r["name"] for r in self._conn.execute("PRAGMA table_info(works)")}
            if "cover_url" not in columns:
                self._conn.execute("ALTER TABLE works ADD COLUMN cover_url TEXT")
        self._conn.executescript(SQLITE_SCHEMA)
        self._conn.commit()

    def _migrate_legacy(self):
        """Move ownership off editions into library_books for dev databases
        created before multi-user libraries, mirroring 04_user_libraries.sql:
        one shared 'Family Library' that every existing user owns."""
        columns = {r["name"] for r in self._conn.execute("PRAGMA table_info(editions)")}
        if "status" not in columns:
            return
        try:
            library_id = new_id()
            self._conn.execute(
                "INSERT INTO libraries (id, name, created_at) VALUES (?, ?, ?)",
                (library_id, "Family Library", now_iso()))
            self._conn.execute(
                "INSERT INTO library_members (library_id, user_id, role, added_at) "
                "SELECT ?, id, 'owner', ? FROM users", (library_id, now_iso()))
            for ed in self._conn.execute("SELECT * FROM editions").fetchall():
                self._conn.execute(
                    "INSERT INTO library_books (id, library_id, edition_id, status, notes, "
                    "copies, added_at, status_changed_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (new_id(), library_id, ed["id"], ed["status"], ed["notes"],
                     ed["copies"], ed["added_at"], ed["status_changed_at"]))
            self._conn.execute(SQLITE_EDITIONS_REBUILD)
            cols = ", ".join(EDITION_FIELDS)
            self._conn.execute(f"INSERT INTO editions_new ({cols}) SELECT {cols} FROM editions")
            self._conn.execute("DROP TABLE editions")
            self._conn.execute("ALTER TABLE editions_new RENAME TO editions")
            self._conn.execute("CREATE INDEX IF NOT EXISTS idx_editions_work ON editions(work_id)")
            self._conn.execute(
                "UPDATE works SET cover_url = (SELECT e.cover_url FROM editions e "
                "WHERE e.work_id = works.id AND e.cover_url IS NOT NULL "
                "ORDER BY e.added_at LIMIT 1) WHERE cover_url IS NULL")
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise

    def _rows(self, query, params=()):
        with self._lock:
            cur = self._conn.execute(query, params)
            return [dict(r) for r in cur.fetchall()]

    def _exec(self, query, params=()):
        with self._lock:
            self._conn.execute(query, params)
            self._conn.commit()

    def _insert(self, table, row):
        cols = ", ".join(row)
        marks = ", ".join("?" for _ in row)
        self._exec(f"INSERT INTO {table} ({cols}) VALUES ({marks})", tuple(row.values()))
        return row

    @staticmethod
    def _marks(values):
        return ",".join("?" for _ in values)

    def _attach_editions(self, holdings):
        edition_ids = {h["edition_id"] for h in holdings}
        editions = {}
        if edition_ids:
            rows = self._rows(
                f"SELECT * FROM editions WHERE id IN ({self._marks(edition_ids)})", tuple(edition_ids))
            editions = {r["id"]: r for r in rows}
        for h in holdings:
            h["edition"] = editions.get(h["edition_id"])
        return holdings

    # -- works ------------------------------------------------------------

    def get_work(self, token, work_id):
        rows = self._rows("SELECT * FROM works WHERE id = ?", (work_id,))
        return rows[0] if rows else None

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
        return self._insert("works", work)

    def update_work(self, token, work_id, fields):
        sets = ", ".join(f"{k} = ?" for k in fields)
        self._exec(f"UPDATE works SET {sets} WHERE id = ?", (*fields.values(), work_id))

    def works_by_norm_keys(self, token, nkeys, ol_keys):
        rows = []
        if nkeys:
            rows += self._rows(
                f"SELECT * FROM works WHERE norm_key IN ({self._marks(nkeys)})", tuple(nkeys))
        if ol_keys:
            rows += self._rows(
                f"SELECT * FROM works WHERE ol_work_key IN ({self._marks(ol_keys)})", tuple(ol_keys))
        seen, out = set(), []
        for r in rows:
            if r["id"] not in seen:
                seen.add(r["id"])
                out.append(r)
        return out

    # -- editions (shared catalog) -----------------------------------------

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
        return self._rows(
            f"SELECT * FROM editions WHERE work_id IN ({self._marks(work_ids)})", tuple(work_ids))

    def insert_edition(self, token, edition):
        return self._insert("editions", edition)

    def update_edition(self, token, edition_id, fields):
        sets = ", ".join(f"{k} = ?" for k in fields)
        self._exec(f"UPDATE editions SET {sets} WHERE id = ?", (*fields.values(), edition_id))
        return self.get_edition(token, edition_id)

    # -- libraries + members ------------------------------------------------

    def libraries_for_user(self, token, user_id):
        return self._rows(
            "SELECT l.*, m.role FROM library_members m JOIN libraries l ON l.id = m.library_id "
            "WHERE m.user_id = ? ORDER BY m.added_at ASC", (user_id,))

    def create_library(self, token, library):
        return self._insert("libraries", library)

    def update_library(self, token, library_id, fields):
        sets = ", ".join(f"{k} = ?" for k in fields)
        self._exec(f"UPDATE libraries SET {sets} WHERE id = ?", (*fields.values(), library_id))

    def add_member(self, token, member):
        return self._insert("library_members", member)

    def members_for_libraries(self, token, library_ids):
        if not library_ids:
            return []
        return self._rows(
            "SELECT m.*, u.username FROM library_members m JOIN users u ON u.id = m.user_id "
            f"WHERE m.library_id IN ({self._marks(library_ids)}) ORDER BY m.added_at ASC",
            tuple(library_ids))

    # -- user directory -------------------------------------------------------

    def find_user_by_username(self, token, username):
        rows = self._rows("SELECT id, username, created_at FROM users WHERE username = ?", (username,))
        return rows[0] if rows else None

    def usernames_for_ids(self, token, user_ids):
        if not user_ids:
            return {}
        ids = set(user_ids)
        rows = self._rows(
            f"SELECT id, username FROM users WHERE id IN ({self._marks(ids)})", tuple(ids))
        return {str(r["id"]): r["username"] for r in rows}

    # -- library books (holdings) ---------------------------------------------

    def list_library_books(self, token, library_ids, status=None):
        if not library_ids:
            return []
        query = f"SELECT * FROM library_books WHERE library_id IN ({self._marks(library_ids)})"
        params = list(library_ids)
        if status:
            query += " AND status = ?"
            params.append(status)
        query += " ORDER BY added_at DESC"
        return self._attach_editions(self._rows(query, tuple(params)))

    def get_library_book(self, token, book_id):
        rows = self._rows("SELECT * FROM library_books WHERE id = ?", (book_id,))
        return self._attach_editions(rows)[0] if rows else None

    def find_library_book(self, token, library_id, edition_id):
        rows = self._rows(
            "SELECT * FROM library_books WHERE library_id = ? AND edition_id = ?",
            (library_id, edition_id))
        return rows[0] if rows else None

    def holdings_for_editions(self, token, library_ids, edition_ids):
        if not library_ids or not edition_ids:
            return []
        return self._rows(
            f"SELECT * FROM library_books WHERE library_id IN ({self._marks(library_ids)}) "
            f"AND edition_id IN ({self._marks(edition_ids)})",
            (*library_ids, *edition_ids))

    def insert_library_book(self, token, book):
        return self._insert("library_books", book)

    def update_library_book(self, token, book_id, fields):
        sets = ", ".join(f"{k} = ?" for k in fields)
        self._exec(f"UPDATE library_books SET {sets} WHERE id = ?", (*fields.values(), book_id))
        rows = self._rows("SELECT * FROM library_books WHERE id = ?", (book_id,))
        return rows[0] if rows else None

    def delete_library_book(self, token, book_id):
        self._exec("DELETE FROM library_books WHERE id = ?", (book_id,))

    # -- read states -----------------------------------------------------------

    def _attach_works(self, states):
        work_ids = {s["work_id"] for s in states}
        works = {}
        if work_ids:
            rows = self._rows(
                f"SELECT * FROM works WHERE id IN ({self._marks(work_ids)})", tuple(work_ids))
            works = {r["id"]: r for r in rows}
        for s in states:
            s["work"] = works.get(s["work_id"])
        return states

    def read_states_for_user(self, token, user_id):
        rows = self._rows(
            "SELECT * FROM read_states WHERE user_id = ? ORDER BY updated_at DESC", (user_id,))
        return self._attach_works(rows)

    def get_read_state(self, token, user_id, work_id):
        rows = self._rows(
            "SELECT * FROM read_states WHERE user_id = ? AND work_id = ?", (user_id, work_id))
        return rows[0] if rows else None

    def read_states_for_works(self, token, user_id, work_ids):
        if not work_ids:
            return []
        return self._rows(
            f"SELECT * FROM read_states WHERE user_id = ? AND work_id IN ({self._marks(work_ids)})",
            (user_id, *work_ids))

    def insert_read_state(self, token, state):
        return self._insert("read_states", state)

    def update_read_state(self, token, user_id, work_id, fields):
        sets = ", ".join(f"{k} = ?" for k in fields)
        self._exec(
            f"UPDATE read_states SET {sets} WHERE user_id = ? AND work_id = ?",
            (*fields.values(), user_id, work_id))
        return self.get_read_state(token, user_id, work_id)

    def delete_read_state(self, token, user_id, work_id):
        self._exec("DELETE FROM read_states WHERE user_id = ? AND work_id = ?", (user_id, work_id))

    # -- users (dev login) --------------------------------------------------

    def get_user(self, username):
        rows = self._rows("SELECT * FROM users WHERE username = ?", (username,))
        return rows[0] if rows else None

    def create_user(self, username, password_hash):
        self._exec(
            "INSERT INTO users (id, username, password_hash, created_at) VALUES (?, ?, ?, ?)",
            (new_id(), username, password_hash, now_iso()),
        )


_store = None


def get_store():
    global _store
    if _store is None:
        _store = PostgrestStore() if config.MODE == "postgrest" else SqliteStore()
    return _store
