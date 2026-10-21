"""Fixtures for the real stack — no local backend, ever (I8/I10).

Every test runs against the deployed PostgREST and the real database,
using throwaway accounts that log in through app.auth.login (the
postgrest-auth library, in-process). An unreachable dependency is a red
test, never a skip.

Isolation without a scratch database:
  - accounts are `ztest<hex>` and are created and deleted per test, so
    every test starts with an empty library;
  - every login forwards a random never-routed address as X-Forwarded-For.
    The login limiter locks per-username AND per-client-IP, so the
    deliberate failures in the lockout tests can't lock another test's
    logins out;
  - the shared works/editions catalog has no per-user scoping, so every
    book a test creates carries RUN_MARKER as its author. That lands in
    works.norm_key, which is what the teardown sweep keys on. Nothing
    outside a run's own marker is ever touched.
"""

import os
import random
import sys
import uuid

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import accounts, auth, bootstrap, config, metadata  # noqa: E402
from app.db import db_reachable, superuser_conn  # noqa: E402
from app.store import postgrest_reachable  # noqa: E402

# one marker per pytest session: usernames, and the author on every book a
# test creates, so the teardown sweep can find exactly this run's rows.
RUN_MARKER = f"ztest{uuid.uuid4().hex[:8]}"
PASSWORD = "ztest-password-1"


# Reserved class E space (RFC 1112, 240/4): never routed, like TEST-NET.
# Each test gets its own address so the per-IP lockout can't leak between
# tests: app.auth.login without a limiter uses the library's process-wide
# one, which lives for the whole pytest session.
def next_test_ip() -> str:
    return f"240.{random.randrange(256)}.{random.randrange(256)}.{random.randrange(1, 255)}"


@pytest.fixture(scope="session")
def live():
    """The deployed stack, converged. Red — not skipped — if unreachable."""
    ok, detail = db_reachable()
    assert ok, f"database unreachable — this test must be red, not skipped: {detail}"
    ok, detail = postgrest_reachable()
    assert ok, f"postgrest unreachable — this test must be red, not skipped: {detail}"
    bootstrap.apply_schema()  # version-gated: a no-op once converged
    yield
    _sweep_catalog()


def _sweep_catalog():
    """Delete the works and editions this run created. Keyed on the run
    marker in norm_key, so a concurrent real user's books are untouched."""
    conn = superuser_conn()
    conn.autocommit = True
    schema = config.APP_SCHEMA
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT id FROM {schema}.works WHERE norm_key LIKE %s",
                        (f"%|{RUN_MARKER}",))
            work_ids = [row[0] for row in cur.fetchall()]
            if not work_ids:
                return
            # psycopg2 returns uuid columns as str, so the array adapts as
            # text[] — cast back or every comparison is uuid = text
            cur.execute(
                f"DELETE FROM {schema}.library_books WHERE edition_id IN "
                f"(SELECT id FROM {schema}.editions WHERE work_id = ANY(%s::uuid[]))", (work_ids,))
            cur.execute(f"DELETE FROM {schema}.read_states WHERE work_id = ANY(%s::uuid[])",
                        (work_ids,))
            cur.execute(f"DELETE FROM {schema}.editions WHERE work_id = ANY(%s::uuid[])",
                        (work_ids,))
            cur.execute(f"DELETE FROM {schema}.works WHERE id = ANY(%s::uuid[])", (work_ids,))
    finally:
        conn.close()


def _drop_account(username: str):
    """Remove a throwaway account and the libraries only it belonged to.
    Memberships and read states cascade off the user row; libraries and
    their shelved books do not, so collect them first."""
    conn = superuser_conn()
    conn.autocommit = True
    schema = config.APP_SCHEMA
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT library_id FROM {schema}.library_members WHERE user_id = "
                f"(SELECT id FROM {schema}.users WHERE username = %s)", (username,))
            library_ids = [row[0] for row in cur.fetchall()]
            cur.execute(f"DELETE FROM {schema}.users WHERE username = %s", (username,))
            for library_id in library_ids:
                # only if the account was its last member — a shared library
                # outlives the test user who was invited into it
                cur.execute(
                    f"SELECT 1 FROM {schema}.library_members WHERE library_id = %s LIMIT 1",
                    (library_id,))
                if cur.fetchone():
                    continue
                cur.execute(f"DELETE FROM {schema}.library_books WHERE library_id = %s",
                            (library_id,))
                cur.execute(f"DELETE FROM {schema}.libraries WHERE id = %s", (library_id,))
    finally:
        conn.close()


@pytest.fixture()
def client(live, monkeypatch):
    from fastapi.testclient import TestClient

    from app import main as main_module
    from app.main import app
    from postgrest_auth import LoginRateLimiter

    # the rate limiters are process-global; tests must not lock each other out
    monkeypatch.setattr(main_module, "login_limiter", LoginRateLimiter())
    monkeypatch.setattr(main_module, "signup_limiter", LoginRateLimiter())
    # no network for external metadata: tests feed metadata dicts straight
    # into the API the way the frontend does
    monkeypatch.setattr(metadata, "lookup_isbn", lambda isbn13: None)
    monkeypatch.setattr(metadata, "search_external", lambda q, limit=12: [])

    with TestClient(app) as tc:
        yield tc


@pytest.fixture()
def throwaway():
    """Names for accounts this test will create, however they get created
    (accounts.create_user or the /api/signup route). Every name handed out
    is dropped at teardown, whether or not it was ever used."""
    handed_out: list[str] = []

    def name(alias: str = "user") -> str:
        username = f"{RUN_MARKER}{alias}{uuid.uuid4().hex[:6]}"
        handed_out.append(username)
        return username

    yield name
    for username in handed_out:
        _drop_account(username)
    auth._revoke_cache.clear()


@pytest.fixture()
def users(client, throwaway):
    """Factory: users("jason") creates a real throwaway account on first
    use, logs it in through the real login path (app.auth.login against the
    real users table), and returns the auth header. users.name("jason")
    gives the account's actual username."""
    created: dict[str, str] = {}
    ip = next_test_ip()

    def login(alias: str) -> dict:
        if alias not in created:
            username = throwaway(alias)
            accounts.create_user(username, PASSWORD)
            created[alias] = username
        token = auth.login(created[alias], PASSWORD, client_ip=ip)
        return {"Authorization": f"Bearer {token}"}

    login.name = created.__getitem__
    login.created = created
    login.ip = ip
    return login


@pytest.fixture()
def isbns():
    """Three valid ISBN-13s, fresh per test: the works/editions catalog is
    shared and permanent, so reusing a number across tests would make the
    second test find the first one's edition."""
    base = f"978{random.randrange(10 ** 8):08d}"

    class _Isbns:
        a = valid_isbn13(base + "1")
        b = valid_isbn13(base + "2")
        c = valid_isbn13(base + "3")

    return _Isbns


def valid_isbn13(prefix12: str) -> str:
    assert len(prefix12) == 12
    return prefix12 + metadata._ean13_check_digit(prefix12)


def make_meta(title, isbn13=None, fmt=None, cover=None, **extra):
    """Book metadata for a test. The author is always the run marker: it
    is what puts RUN_MARKER into works.norm_key so the teardown sweep can
    find this run's catalog rows and nothing else."""
    return {
        "title": title,
        "authors": [RUN_MARKER],
        "isbn13": isbn13,
        "format": fmt,
        "cover_url": cover,
        **extra,
    }


def make_norm_key(title):
    return metadata.norm_key(title, [RUN_MARKER])
