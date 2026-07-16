"""Legacy dev databases (pre-multi-user) migrate in place on first open:
per-edition ownership moves into one shared 'Family Library' that every
already-existing user owns — matching deploy/04_user_libraries.sql."""

import sqlite3

import bcrypt

from app import config
from app import store as store_module

LEGACY_SCHEMA = """
CREATE TABLE users (
    id TEXT PRIMARY KEY,
    username TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE works (
    id TEXT PRIMARY KEY,
    ol_work_key TEXT UNIQUE,
    norm_key TEXT UNIQUE NOT NULL,
    title TEXT NOT NULL,
    authors TEXT,
    created_at TEXT NOT NULL
);
CREATE TABLE editions (
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
    status TEXT NOT NULL CHECK (status IN ('library', 'wishlist')),
    notes TEXT,
    copies INTEGER NOT NULL DEFAULT 1,
    added_at TEXT NOT NULL,
    status_changed_at TEXT NOT NULL
);
"""


def build_legacy_db(path):
    conn = sqlite3.connect(path)
    conn.executescript(LEGACY_SCHEMA)
    pw = bcrypt.hashpw(b"pw", bcrypt.gensalt()).decode()
    for uid, name in (("u-jason", "jason"), ("u-beca", "beca")):
        conn.execute("INSERT INTO users VALUES (?, ?, ?, ?)", (uid, name, pw, "2025-01-01"))
    conn.execute(
        "INSERT INTO works (id, norm_key, title, authors, created_at) VALUES "
        "('w1', 'way of kings|sanderson', 'The Way of Kings', 'Brandon Sanderson', '2025-01-01')")
    conn.execute(
        "INSERT INTO editions (id, work_id, isbn13, title, authors, format, cover_url, "
        "status, notes, copies, added_at, status_changed_at) VALUES "
        "('e1', 'w1', '9780000000012', 'The Way of Kings', 'Brandon Sanderson', 'hardcover', "
        "'http://x/cover.jpg', 'library', 'signed', 1, '2025-02-01', '2025-02-01')")
    conn.execute(
        "INSERT INTO editions (id, work_id, isbn13, title, authors, format, "
        "status, notes, copies, added_at, status_changed_at) VALUES "
        "('e2', 'w1', '9780000000029', 'The Way of Kings', 'Brandon Sanderson', 'paperback', "
        "'wishlist', NULL, 2, '2025-03-01', '2025-03-01')")
    conn.execute(
        "INSERT INTO editions (id, work_id, isbn13, title, authors, format, "
        "status, notes, copies, added_at, status_changed_at) VALUES "
        "('e3', 'w1', '9780000000036', 'The Way of Kings', 'Brandon Sanderson', 'ebook', "
        "'library', NULL, 1, '2025-04-01', '2025-04-01')")
    conn.commit()
    conn.close()


def test_legacy_sqlite_migrates_to_shared_family_library(tmp_path):
    path = str(tmp_path / "legacy.db")
    build_legacy_db(path)

    store = store_module.SqliteStore(path)

    # the migrated Family Library plus the always-present Sample Library
    libraries = store._rows(
        "SELECT * FROM libraries WHERE id != ?", (config.SAMPLE_LIBRARY_ID,))
    assert len(libraries) == 1
    assert libraries[0]["name"] == "Family Library"

    # every pre-existing user co-owns the migrated library
    members = store.members_for_libraries(None, [libraries[0]["id"]])
    assert sorted(m["username"] for m in members) == ["beca", "jason"]

    # ownership moved onto library_books, values intact
    books = store.list_library_books(None, [libraries[0]["id"]])
    by_isbn = {b["edition"]["isbn13"]: b for b in books}
    assert by_isbn["9780000000012"]["status"] == "library"
    assert by_isbn["9780000000012"]["notes"] == "signed"
    assert by_isbn["9780000000029"]["status"] == "wishlist"
    assert by_isbn["9780000000029"]["copies"] == 2
    # an owned ebook becomes a digital holding
    assert by_isbn["9780000000036"]["status"] == "digital"

    # editions are pure catalog rows now
    columns = {r["name"] for r in store._conn.execute("PRAGMA table_info(editions)")}
    assert "status" not in columns and "copies" not in columns
    assert store.get_edition_by_isbn(None, "9780000000012")["format"] == "hardcover"

    # work cover backfilled from the edition that had one
    assert store.get_work(None, "w1")["cover_url"] == "http://x/cover.jpg"

    # opening the same file again is a no-op
    again = store_module.SqliteStore(path)
    assert len(again._rows(
        "SELECT * FROM libraries WHERE id != ?", (config.SAMPLE_LIBRARY_ID,))) == 1
    assert len(again.list_library_books(None, [libraries[0]["id"]])) == 3


def test_two_state_status_check_is_widened(tmp_path):
    """Dev databases created before the 'digital' status get their
    library_books table rebuilt with the wider CHECK, keeping data."""
    path = str(tmp_path / "twostate.db")
    old_schema = store_module.SQLITE_SCHEMA.replace(
        "IN ('library', 'wishlist', 'digital')", "IN ('library', 'wishlist')")
    assert old_schema != store_module.SQLITE_SCHEMA
    conn = sqlite3.connect(path)
    conn.executescript(old_schema)
    conn.execute("INSERT INTO libraries VALUES ('l1', 'lib', '2026-01-01')")
    conn.execute(
        "INSERT INTO works (id, norm_key, title, created_at) VALUES ('w1', 'k', 'T', '2026-01-01')")
    conn.execute(
        "INSERT INTO editions (id, work_id, title, added_at) VALUES ('e1', 'w1', 'T', '2026-01-01')")
    conn.execute(
        "INSERT INTO editions (id, work_id, title, added_at) VALUES ('e2', 'w1', 'T', '2026-01-01')")
    conn.execute(
        "INSERT INTO library_books (id, library_id, edition_id, status, copies, added_at, "
        "status_changed_at) VALUES ('b1', 'l1', 'e1', 'library', 1, '2026-01-01', '2026-01-01')")
    conn.commit()
    conn.close()

    store = store_module.SqliteStore(path)
    # existing data survived the rebuild
    assert store.get_library_book(None, "b1")["status"] == "library"
    # and 'digital' is accepted now
    store.insert_library_book(None, {
        "id": "b2", "library_id": "l1", "edition_id": "e2", "status": "digital",
        "notes": None, "copies": 1, "added_at": "2026-01-02", "status_changed_at": "2026-01-02"})
    assert store.get_library_book(None, "b2")["status"] == "digital"


def test_migrated_users_share_but_new_users_do_not(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient

    from app import config, metadata
    from app.main import app

    path = str(tmp_path / "legacy.db")
    build_legacy_db(path)

    monkeypatch.setattr(config, "MODE", "dev")
    monkeypatch.setattr(config, "SQLITE_PATH", path)
    monkeypatch.setattr(config, "JWT_SECRET", "test-secret")
    monkeypatch.setattr(config, "SAMPLE_AUTOSTOCK", False)
    monkeypatch.setattr(store_module, "_store", None)
    monkeypatch.setattr(metadata, "lookup_isbn", lambda isbn13: None)
    monkeypatch.setattr(metadata, "search_external", lambda q, limit=12: [])

    with TestClient(app) as client:
        def login(username):
            resp = client.post("/api/login", json={"username": username, "password": "pw"})
            assert resp.status_code == 200, resp.text
            return {"Authorization": f"Bearer {resp.json()['token']}"}

        jason, beca = login("jason"), login("beca")
        # both existing users land in the same migrated library — no
        # personal library is auto-created for them
        for headers in (jason, beca):
            me = client.get("/api/me", headers=headers).json()
            assert [lib["name"] for lib in me["libraries"]
                    if lib["role"] != "viewer"] == ["Family Library"]
            titles = [i["title"] for i in client.get("/api/books", headers=headers).json()["items"]]
            assert titles == ["The Way of Kings"] * 3

        # a user created after the migration starts from scratch
        store_module.get_store().create_user(
            "newuser", bcrypt.hashpw(b"pw", bcrypt.gensalt()).decode())
        new = login("newuser")
        me = client.get("/api/me", headers=new).json()
        assert [lib["name"] for lib in me["libraries"]
                if lib["role"] != "viewer"] == ["newuser's library"]
        assert client.get("/api/books", headers=new).json()["items"] == []

    store_module._store = None
