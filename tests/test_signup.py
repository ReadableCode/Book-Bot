"""Self-signup and login hardening, against the real auth service.

Every request carries a random never-routed X-Forwarded-For (see conftest): the
service's lockout is keyed per-username AND per-IP, and the deliberate
failures below would otherwise lock the machine running the tests.
"""

import pytest

from app import config
from conftest import PASSWORD, next_test_ip


@pytest.fixture()
def ip():
    return next_test_ip()


def signup(client, ip, username, password=PASSWORD):
    return client.post("/api/signup", json={"username": username, "password": password},
                       headers={"X-Forwarded-For": ip})


def auth_header(resp):
    return {"Authorization": f"Bearer {resp.json()['token']}"}


# --------------------------------------------------------------------------
# signup
# --------------------------------------------------------------------------

def test_signup_creates_account_with_empty_library(client, throwaway, ip, auth_service):
    username = throwaway("newbie")
    resp = signup(client, ip, username)
    assert resp.status_code == 200, resp.text
    headers = auth_header(resp)
    me = client.get("/api/me", headers=headers).json()
    assert me["username"] == username
    assert client.get("/api/books", headers=headers).json()["items"] == []


def test_signup_duplicate_username(client, throwaway, ip, auth_service):
    username = throwaway("newbie")
    assert signup(client, ip, username).status_code == 200
    assert signup(client, ip, username).status_code == 409


def test_signup_rejects_weak_password(client, throwaway, ip):
    resp = signup(client, ip, throwaway("weak"), password="short")
    assert resp.status_code == 400
    assert "10 characters" in resp.json()["detail"]


def test_signup_rejects_bad_username(client, ip):
    for bad in ("sp ace", "-lead", "a" * 40, "we!rd"):
        assert signup(client, ip, bad).status_code == 400, bad


def test_signup_normalizes_username_to_lowercase(client, throwaway, ip, auth_service):
    username = throwaway("newbie")
    resp = signup(client, ip, username.upper())
    assert resp.status_code == 200
    me = client.get("/api/me", headers=auth_header(resp)).json()
    assert me["username"] == username


def test_signup_can_be_disabled(client, throwaway, ip, monkeypatch):
    monkeypatch.setattr(config, "SIGNUP_ENABLED", False)
    assert signup(client, ip, throwaway("nope")).status_code == 403


def test_signup_rate_limited_per_ip(client, throwaway, ip, auth_service):
    for _ in range(5):
        assert signup(client, ip, throwaway("burst")).status_code == 200
    assert signup(client, ip, throwaway("burst")).status_code == 429


# --------------------------------------------------------------------------
# login hardening
# --------------------------------------------------------------------------

def test_login_lockout_after_failures(client, users):
    """The app's own limiter locks the account before the correct password
    is even tried. The real service is locking the same key in parallel —
    that is the point, and why this runs on a throwaway IP."""
    users("jason")  # create the account and confirm a good login works
    username = users.name("jason")
    headers = {"X-Forwarded-For": users.ip}
    for _ in range(5):
        resp = client.post("/api/login", json={"username": username, "password": "wrong"},
                           headers=headers)
        assert resp.status_code == 401, resp.text
    # even the correct password is refused while locked
    resp = client.post("/api/login", json={"username": username, "password": PASSWORD},
                       headers=headers)
    assert resp.status_code == 429


def test_login_failures_below_threshold_do_not_lock(client, users):
    users("jason")
    username = users.name("jason")
    headers = {"X-Forwarded-For": users.ip}
    for _ in range(4):
        client.post("/api/login", json={"username": username, "password": "wrong"},
                    headers=headers)
    resp = client.post("/api/login", json={"username": username, "password": PASSWORD},
                       headers=headers)
    assert resp.status_code == 200, resp.text


def test_security_headers_present(client):
    resp = client.get("/api/health")
    assert resp.headers["X-Frame-Options"] == "DENY"
    assert resp.headers["X-Content-Type-Options"] == "nosniff"
    assert "default-src 'self'" in resp.headers["Content-Security-Policy"]
