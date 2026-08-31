"""Real-PostgREST round trip — the full production path, end to end.

Throwaway accounts log in through the REAL postgrest-auth service, so this
exercises argon2id verify -> JWT claims -> app-side session validation ->
RLS on the book_bot tables. Red if any hop is unreachable, never skipped.
"""

import time

import jwt
import pytest

from app import auth, config
from app import accounts
from app.store import StoreError, get_store, postgrest_reachable
from conftest import PASSWORD, make_meta


def test_postgrest_is_reachable(live):
    ok, detail = postgrest_reachable()
    assert ok, detail


def test_service_token_carries_the_claims_the_app_needs(users):
    """user_id drives RLS; iat and username drive revocation. A token
    missing any of them cannot be validated (conventions §4)."""
    header = users("jason")
    token = header["Authorization"].split()[1]
    payload = jwt.decode(token, config.JWT_SECRET, algorithms=["HS256"])
    assert payload["role"] == f"{config.APP_SCHEMA}_user"
    assert payload["user_id"]
    assert payload["username"] == users.name("jason")
    assert payload["iat"] and payload["exp"] > payload["iat"]


def test_a_service_token_validates_in_app(users):
    token = users("jason")["Authorization"].split()[1]
    context = auth.decode_token(token)
    assert context.user_id


def test_wrong_password_is_rejected_by_the_service(client, users):
    users("jason")
    with pytest.raises(Exception) as exc:
        auth.login(users.name("jason"), "definitely-not-the-password", client_ip=users.ip)
    assert getattr(exc.value, "status_code", None) == 401


# --------------------------------------------------------------------------
# revocation — the whole point of Gap 2
# --------------------------------------------------------------------------

def _bypass_revocation_cache():
    """The revocation read is cached 30 s; tests must see the new row now."""
    auth._revoke_cache.clear()


def test_disabling_an_account_revokes_its_live_session(client, users):
    """Definition of done: disabling an account kills sessions already
    issued, without any server-side session store."""
    header = users("jason")
    username = users.name("jason")
    assert client.get("/api/me", headers=header).status_code == 200

    accounts.set_disabled(username, True)
    _bypass_revocation_cache()

    resp = client.get("/api/me", headers=header)
    assert resp.status_code == 401, resp.text
    assert "account unavailable" in resp.json()["detail"]

    # and the service refuses a fresh login too
    with pytest.raises(Exception) as exc:
        auth.login(username, PASSWORD, client_ip=users.ip)
    assert getattr(exc.value, "status_code", None) == 401


def test_reenabling_does_not_resurrect_the_old_session(client, users):
    header = users("jason")
    username = users.name("jason")
    time.sleep(1.1)  # iat is second-granular; auth.py allows a 1 s grace
    accounts.set_disabled(username, True)
    accounts.set_disabled(username, False)
    _bypass_revocation_cache()

    resp = client.get("/api/me", headers=header)
    assert resp.status_code == 401, "the pre-disable token must stay dead"

    # a new login works and is accepted
    fresh = users("jason")
    assert client.get("/api/me", headers=fresh).status_code == 200


def test_changing_the_password_revokes_the_old_session(client, users):
    header = users("jason")
    username = users.name("jason")
    assert client.get("/api/me", headers=header).status_code == 200

    time.sleep(1.1)  # iat is second-granular; auth.py allows a 1 s grace
    accounts.set_password(username, "a-brand-new-password")
    _bypass_revocation_cache()

    resp = client.get("/api/me", headers=header)
    assert resp.status_code == 401, resp.text
    assert "session expired" in resp.json()["detail"]


def test_a_token_without_iat_is_refused(client, users):
    """Tokens minted before the service carried iat/username cannot be
    revocation-checked, so they must force one re-login rather than pass."""
    users("jason")
    payload = {
        "role": f"{config.APP_SCHEMA}_user",
        "user_id": "00000000-0000-0000-0000-000000000000",
        "exp": int(time.time()) + 3600,
    }
    legacy = jwt.encode(payload, config.JWT_SECRET, algorithm="HS256")
    resp = client.get("/api/me", headers={"Authorization": f"Bearer {legacy}"})
    assert resp.status_code == 401
    assert "predates" in resp.json()["detail"]


# --------------------------------------------------------------------------
# RLS through the real PostgREST
# --------------------------------------------------------------------------

def test_rls_hides_another_users_holdings(client, users, isbns):
    jason, beca = users("jason"), users("beca")
    client.post("/api/books", headers=jason,
                json={"status": "library", "metadata": make_meta("RLS Book", isbns.a)})

    store = get_store()
    jason_token = jason["Authorization"].split()[1]
    beca_token = beca["Authorization"].split()[1]
    jason_libs = [lib["id"] for lib in client.get("/api/me", headers=jason).json()["libraries"]]

    assert store.list_library_books(jason_token, jason_libs)
    # beca asking PostgREST directly for jason's library id gets nothing:
    # the policy is enforced in the database, not in the app
    assert store.list_library_books(beca_token, jason_libs) == []


def test_rls_refuses_a_write_into_someone_elses_library(client, users, isbns):
    jason, beca = users("jason"), users("beca")
    jason_lib = client.get("/api/me", headers=jason).json()["libraries"][0]["id"]
    beca_token = beca["Authorization"].split()[1]

    from app.store import new_id, now_iso

    edition = client.post("/api/books", headers=jason, json={
        "status": "library", "metadata": make_meta("Write Guard", isbns.b)}).json()["book"]

    with pytest.raises(StoreError):
        store = get_store()
        store.insert_library_book(beca_token, {
            "id": new_id(),
            "library_id": jason_lib,
            "edition_id": edition["edition_id"],
            "status": "library",
            "notes": None,
            "copies": 1,
            "added_at": now_iso(),
            "status_changed_at": now_iso(),
        })


def test_the_users_table_is_unreachable_through_postgrest(users):
    """I4 from the outside: the credential table must 404/permission-deny
    for an app token, not return rows."""
    import requests

    token = users("jason")["Authorization"].split()[1]
    resp = requests.get(
        f"{config.POSTGREST_URL}/users",
        headers={
            "Accept-Profile": config.APP_SCHEMA,
            "Authorization": f"Bearer {token}",
        },
        timeout=config.HTTP_TIMEOUT,
    )
    assert resp.status_code >= 400, resp.text
    assert "password_hash" not in resp.text
