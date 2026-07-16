"""Test fixtures: the whole API against a throwaway SQLite database.

External metadata lookups (Google Books / Open Library) are stubbed out;
tests feed metadata dicts straight into the API the way the frontend does.
"""

import os
import sys

import bcrypt
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import config, metadata  # noqa: E402
from app import store as store_module  # noqa: E402


@pytest.fixture()
def client(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient

    from app import main as main_module
    from app.main import app
    from app.security import LoginRateLimiter

    monkeypatch.setattr(config, "MODE", "dev")
    monkeypatch.setattr(config, "SQLITE_PATH", str(tmp_path / "book_bot.db"))
    monkeypatch.setattr(config, "JWT_SECRET", "test-secret")
    monkeypatch.setattr(store_module, "_store", None)
    # the rate limiters are process-global; tests must not lock each other out
    monkeypatch.setattr(main_module, "login_limiter", LoginRateLimiter())
    monkeypatch.setattr(main_module, "signup_limiter", LoginRateLimiter())
    # no startup auto-stocking: tests stock the sample shelf explicitly
    monkeypatch.setattr(config, "SAMPLE_AUTOSTOCK", False)
    # no network in tests
    monkeypatch.setattr(metadata, "lookup_isbn", lambda isbn13: None)
    monkeypatch.setattr(metadata, "search_external", lambda q, limit=12: [])

    with TestClient(app) as tc:
        yield tc
    store_module._store = None


@pytest.fixture()
def users(client):
    """Create jason + beca + newuser and return login-header factories."""
    store = store_module.get_store()
    for name in ("jason", "beca", "newuser"):
        store.create_user(name, bcrypt.hashpw(b"pw", bcrypt.gensalt()).decode())

    def login(username):
        resp = client.post("/api/login", json={"username": username, "password": "pw"})
        assert resp.status_code == 200, resp.text
        return {"Authorization": f"Bearer {resp.json()['token']}"}

    return login


def valid_isbn13(prefix12: str) -> str:
    assert len(prefix12) == 12
    return prefix12 + metadata._ean13_check_digit(prefix12)


def make_meta(title, isbn13=None, authors=None, fmt=None, cover=None, **extra):
    return {
        "title": title,
        "authors": authors or ["Test Author"],
        "isbn13": isbn13,
        "format": fmt,
        "cover_url": cover,
        **extra,
    }
