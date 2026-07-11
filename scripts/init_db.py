"""Idempotent book_bot schema/role setup for the shared apps database.

Run as the postgres superuser (POSTGRES_* env vars). Equivalent to applying
deploy/01_create_role.sql + 02_schema.sql + 03_secure_users.sql, but safe to
re-run: the container entrypoint calls this on every start, the way load-log
runs `alembic upgrade head`.

Expects the cluster-global PostgREST roles (postgrest_authenticator,
web_anon) to already exist — they're created once by load-log's
deploy/01_create_roles.sql.
"""

import os
import sys

import psycopg2

ROLE_SQL = """
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'book_bot_user') THEN
        CREATE ROLE book_bot_user NOLOGIN;
    END IF;
END
$$;
GRANT book_bot_user TO postgrest_authenticator;
"""


def main():
    deploy_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "deploy")
    conn = psycopg2.connect(
        host=os.environ["POSTGRES_URL"],
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ.get("POSTGRES_DB", "apps"),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("SELECT rolname FROM pg_roles WHERE rolname IN ('postgrest_authenticator', 'web_anon')")
        found = {row[0] for row in cur.fetchall()}
        missing = {"postgrest_authenticator", "web_anon"} - found
        if missing:
            sys.exit(
                f"missing cluster roles: {', '.join(sorted(missing))} — "
                "run load-log's deploy/01_create_roles.sql first (shared PostgREST setup)"
            )
        print("[init_db] ensuring book_bot_user role")
        cur.execute(ROLE_SQL)
        for name in ("02_schema.sql", "03_secure_users.sql"):
            print(f"[init_db] applying deploy/{name}")
            with open(os.path.join(deploy_dir, name)) as f:
                cur.execute(f.read())
        # PostgREST caches the schema; without a reload, columns added above
        # 400 until the container restarts. It listens on the "pgrst" channel
        # by default.
        print("[init_db] reloading PostgREST schema cache")
        cur.execute("NOTIFY pgrst, 'reload schema'")
    conn.close()
    print("[init_db] book_bot schema up to date")


if __name__ == "__main__":
    main()
