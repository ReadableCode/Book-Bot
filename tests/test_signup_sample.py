"""Self-signup, login hardening and the shared view-only Sample Library."""

import pytest

from app import config
from app import store as store_module
from app.store import new_id, now_iso
from conftest import make_meta, valid_isbn13

GOOD_PW = "long-enough-password"


def stock_sample_library(titles=("Sample One", "Sample Two", "Sample Three")):
    """Put books straight onto the sample shelf (dev store, no RLS —
    production uses scripts/seed_sample_library.py)."""
    store = store_module.get_store()
    ids = []
    for i, title in enumerate(titles, 1):
        work = store.create_work(None, {
            "id": new_id(), "ol_work_key": None, "norm_key": f"sample-{i}",
            "title": title, "authors": f"Author {i}", "cover_url": None,
            "created_at": now_iso(),
        })
        edition = store.insert_edition(None, {
            "id": new_id(), "work_id": work["id"],
            "isbn13": valid_isbn13(f"9780000000{i:02d}"),
            "isbn10": None, "title": title, "subtitle": None,
            "authors": f"Author {i}", "publisher": None, "published_date": None,
            "description": None, "format": "hardcover", "cover_url": None,
            "google_volume_id": None, "ol_edition_key": None, "page_count": None,
            "language": "en", "genre": "fiction", "added_at": now_iso(),
        })
        book = store.insert_library_book(None, {
            "id": new_id(), "library_id": config.SAMPLE_LIBRARY_ID,
            "edition_id": edition["id"], "status": "library", "notes": None,
            "copies": 1, "added_at": now_iso(), "status_changed_at": now_iso(),
        })
        ids.append({"book": book, "edition": edition, "work": work})
    return ids


@pytest.fixture()
def sample_books(client):
    return stock_sample_library()


def signup(client, username="newbie", password=GOOD_PW):
    return client.post("/api/signup", json={"username": username, "password": password})


def auth_header(resp):
    return {"Authorization": f"Bearer {resp.json()['token']}"}


# --------------------------------------------------------------------------
# signup
# --------------------------------------------------------------------------

def test_signup_creates_account_with_empty_library(client):
    resp = signup(client)
    assert resp.status_code == 200, resp.text
    headers = auth_header(resp)
    me = client.get("/api/me", headers=headers).json()
    assert me["username"] == "newbie"
    assert client.get("/api/books", headers=headers).json()["items"] == []


def test_signup_duplicate_username(client):
    assert signup(client).status_code == 200
    assert signup(client).status_code == 409


def test_signup_rejects_weak_password(client):
    resp = signup(client, password="short")
    assert resp.status_code == 400
    assert "10 characters" in resp.json()["detail"]


def test_signup_rejects_bad_username(client):
    for bad in ("sp ace", "-lead", "a" * 40, "we!rd"):
        assert signup(client, username=bad).status_code == 400, bad


def test_signup_normalizes_username_to_lowercase(client):
    resp = signup(client, username="NewBie")
    assert resp.status_code == 200
    me = client.get("/api/me", headers=auth_header(resp)).json()
    assert me["username"] == "newbie"


def test_signup_can_be_disabled(client, monkeypatch):
    monkeypatch.setattr(config, "SIGNUP_ENABLED", False)
    assert signup(client).status_code == 403


def test_signup_rate_limited_per_ip(client):
    for i in range(5):
        assert signup(client, username=f"user{i}").status_code == 200
    assert signup(client, username="user5").status_code == 429


# --------------------------------------------------------------------------
# login hardening
# --------------------------------------------------------------------------

def test_login_lockout_after_failures(client, users):
    for _ in range(5):
        resp = client.post("/api/login", json={"username": "jason", "password": "wrong"})
        assert resp.status_code == 401
    # even the correct password is refused while locked
    resp = client.post("/api/login", json={"username": "jason", "password": "pw"})
    assert resp.status_code == 429


def test_login_failures_below_threshold_do_not_lock(client, users):
    for _ in range(4):
        client.post("/api/login", json={"username": "jason", "password": "wrong"})
    resp = client.post("/api/login", json={"username": "jason", "password": "pw"})
    assert resp.status_code == 200


def test_security_headers_present(client):
    resp = client.get("/api/health")
    assert resp.headers["X-Frame-Options"] == "DENY"
    assert resp.headers["X-Content-Type-Options"] == "nosniff"
    assert "default-src 'self'" in resp.headers["Content-Security-Policy"]


# --------------------------------------------------------------------------
# the shared sample library
# --------------------------------------------------------------------------

def test_bootstrap_stocks_the_shelf_once(client, tmp_path, monkeypatch):
    """First stocking run fills the shelf from the manifest; every later
    run sees it's stocked and no-ops (this is what app startup calls)."""
    import json

    from app import bootstrap

    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps([
        make_meta("Boot One", isbn13=valid_isbn13("978000000101")),
        make_meta("Boot Two", isbn13=valid_isbn13("978000000102")),
    ]))
    monkeypatch.setattr(config, "SAMPLE_BOOKS_PATH", str(manifest))

    first = bootstrap.stock_sample_library()
    assert first == {"added": 2, "existing": 0, "failed": 0, "already_stocked": False}
    second = bootstrap.stock_sample_library()
    assert second["already_stocked"] is True and second["existing"] == 2

    # an interrupted/partial stocking resumes: only the missing book is added
    manifest.write_text(json.dumps([
        make_meta("Boot One", isbn13=valid_isbn13("978000000101")),
        make_meta("Boot Two", isbn13=valid_isbn13("978000000102")),
        make_meta("Boot Three", isbn13=valid_isbn13("978000000103")),
    ]))
    third = bootstrap.stock_sample_library()
    assert third == {"added": 1, "existing": 2, "failed": 0, "already_stocked": False}

def test_sample_library_visible_to_everyone(client, users, sample_books):
    for who in ("jason", "beca"):
        me = client.get("/api/me", headers=users(who)).json()
        sample = [l for l in me["libraries"] if l["id"] == config.SAMPLE_LIBRARY_ID]
        assert len(sample) == 1
        assert sample[0]["role"] == "viewer"
        # last in the list — the user's own library stays the default
        assert me["libraries"][-1]["id"] == config.SAMPLE_LIBRARY_ID


def test_sample_library_browsable(client, users, sample_books):
    items = client.get(f"/api/books?library_id={config.SAMPLE_LIBRARY_ID}",
                       headers=users("jason")).json()["items"]
    assert {i["title"] for i in items} == {"Sample One", "Sample Two", "Sample Three"}


def test_sample_books_stay_out_of_own_shelves_and_stats(client, users, sample_books):
    headers = users("jason")
    assert client.get("/api/books", headers=headers).json()["items"] == []
    assert client.get("/api/stats", headers=headers).json()["library"] == 0


def test_sample_library_is_read_only(client, users, sample_books):
    headers = users("jason")
    book_id = sample_books[0]["book"]["id"]

    # no adding into it
    resp = client.post("/api/books", json={
        "status": "library", "metadata": make_meta("Intruder"),
        "library_id": config.SAMPLE_LIBRARY_ID,
    }, headers=headers)
    assert resp.status_code == 404

    # no editing or deleting its holdings
    assert client.patch(f"/api/books/{book_id}", json={"notes": "x"},
                        headers=headers).status_code == 404
    assert client.delete(f"/api/books/{book_id}", headers=headers).status_code == 404

    # no renaming, no member games
    assert client.patch(f"/api/libraries/{config.SAMPLE_LIBRARY_ID}",
                        json={"name": "mine now"}, headers=headers).status_code == 404
    assert client.post(f"/api/libraries/{config.SAMPLE_LIBRARY_ID}/members",
                       json={"username": "jason"}, headers=headers).status_code == 404


def test_read_state_on_a_sample_book_is_personal(client, users, sample_books):
    headers = users("jason")
    work = sample_books[1]["work"]
    resp = client.post("/api/reads", json={"work_id": work["id"], "status": "read"},
                       headers=headers)
    assert resp.status_code == 200
    reads = client.get("/api/reads", headers=headers).json()["items"]
    assert reads[0]["title"] == work["title"]
    # sample copy doesn't count as owning it
    assert reads[0]["owned"] is False
