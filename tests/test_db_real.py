"""Real-database tests — hit the actual shared Postgres, red if unreachable.

Covers the credential table's shape and the account operations that make
session revocation possible. Uses throwaway accounts and removes them.
"""

import psycopg2
import pytest
from argon2 import PasswordHasher

from app import accounts, config
from app.db import superuser_conn

CANONICAL_COLUMNS = {
    "id", "username", "password_hash", "role", "display_name",
    "disabled", "created_at", "password_changed_at",
}


@pytest.fixture()
def account(live, throwaway):
    username = throwaway("dbreal")
    accounts.create_user(username, "correct-horse-battery")
    return username


def test_users_table_matches_the_canonical_shape(live):
    """Conventions §3. Missing password_changed_at or disabled means
    sessions cannot be revoked at all."""
    with superuser_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = 'users'",
            (config.APP_SCHEMA,),
        )
        columns = {row[0] for row in cur.fetchall()}
    assert CANONICAL_COLUMNS <= columns, f"missing: {CANONICAL_COLUMNS - columns}"


def test_users_table_hidden_from_postgrest_roles(live):
    """I4: there must be no PostgREST path to a password hash."""
    with superuser_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """SELECT grantee, privilege_type FROM information_schema.role_table_grants
               WHERE table_schema = %s AND table_name = 'users'
                 AND grantee IN ('book_bot_user', 'web_anon')""",
            (config.APP_SCHEMA,),
        )
        assert cur.fetchall() == []


def test_new_accounts_are_argon2id(account):
    user = accounts.get_user(account)
    assert user["password_hash"].startswith("$argon2id$")
    PasswordHasher().verify(user["password_hash"], "correct-horse-battery")
    assert user["role"] == "user"
    assert user["disabled"] is False


def test_duplicate_username_is_rejected(account):
    with pytest.raises(ValueError):
        accounts.create_user(account, "another-good-password")


def test_password_change_bumps_password_changed_at(account):
    before = accounts.get_user(account)["password_changed_at"]
    accounts.set_password(account, "another-good-password")
    after = accounts.get_user(account)
    assert after["password_changed_at"] > before, "password change must revoke sessions"
    PasswordHasher().verify(after["password_hash"], "another-good-password")


def test_disable_and_reenable_both_bump_password_changed_at(account):
    start = accounts.get_user(account)["password_changed_at"]

    accounts.set_disabled(account, True)
    disabled = accounts.get_user(account)
    assert disabled["disabled"] is True
    assert disabled["password_changed_at"] > start, "disable must revoke live sessions"

    accounts.set_disabled(account, False)
    reenabled = accounts.get_user(account)
    assert reenabled["disabled"] is False
    assert reenabled["password_changed_at"] > disabled["password_changed_at"], \
        "re-enable must not resurrect the sessions the account had before"


def test_operations_on_a_missing_user_raise(live):
    for operation in (
        lambda: accounts.set_password("ztest-no-such-user", "long-enough-password"),
        lambda: accounts.set_disabled("ztest-no-such-user", True),
        lambda: accounts.remove_user("ztest-no-such-user"),
    ):
        with pytest.raises(ValueError):
            operation()


def test_remove_user_cascades_memberships(account):
    user_id = accounts.get_user(account)["id"]
    accounts.remove_user(account)
    assert accounts.get_user(account) is None
    with superuser_conn() as conn, conn.cursor() as cur:
        cur.execute(
            f"SELECT count(*) FROM {config.APP_SCHEMA}.library_members WHERE user_id = %s",
            (user_id,))
        assert cur.fetchone()[0] == 0


def test_the_sample_library_is_gone(live):
    """06_drop_sample_library.sql: users see only their own books. A row
    with the fixed sample uuid must no longer exist, and neither must the
    designator function the world-readable policies keyed on."""
    with superuser_conn() as conn, conn.cursor() as cur:
        cur.execute(
            f"SELECT count(*) FROM {config.APP_SCHEMA}.libraries WHERE id = %s",
            ("11111111-1111-1111-1111-111111111111",))
        assert cur.fetchone()[0] == 0
        cur.execute(
            "SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace "
            "WHERE n.nspname = %s AND p.proname = 'is_sample_library'",
            (config.APP_SCHEMA,))
        assert cur.fetchone()[0] == 0
        cur.execute(
            "SELECT policyname FROM pg_policies WHERE schemaname = %s "
            "AND policyname IN ('libraries_sample_read', 'library_books_sample_read')",
            (config.APP_SCHEMA,))
        assert cur.fetchall() == []


def test_there_is_no_sqlite_fallback():
    """I8. A missing POSTGREST_URL must raise, not swap backends."""
    import app.store as store_module

    assert not hasattr(store_module, "SqliteStore")
    assert store_module.get_store().__class__.__name__ == "PostgrestStore"

    import subprocess
    import sys

    result = subprocess.run(
        [sys.executable, "-c",
         "import dotenv; dotenv.load_dotenv = lambda *a, **k: None; import app.config"],
        capture_output=True, text=True,
        env={"PATH": "/usr/bin:/bin", "PYTHONPATH": str(__import__("pathlib").Path(
            __file__).resolve().parent.parent)},
    )
    assert result.returncode != 0, "app.config imported with no POSTGREST_URL"
    assert "POSTGREST_URL must be set" in result.stderr


def test_schema_version_is_recorded(live):
    from app import bootstrap

    with superuser_conn() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT version FROM {config.APP_SCHEMA}.deploy_meta")
        assert cur.fetchone()[0] == bootstrap.SCHEMA_VERSION


def test_psycopg_errors_are_not_swallowed(live):
    """The revocation read must fail loudly rather than default to allow."""
    with pytest.raises(psycopg2.Error):
        with superuser_conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT * FROM book_bot.no_such_table")
