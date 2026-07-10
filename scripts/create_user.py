"""Create a book-bot login.

dev mode (no POSTGREST_URL set): writes to the local SQLite database.
postgrest mode: inserts into book_bot.users in the shared apps database
using the superuser POSTGRES_* env vars (same as load-log's create_user).

    uv run python scripts/create_user.py --username beca --password '...'
"""

import argparse
import os
import sys

import bcrypt

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import config  # noqa: E402


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--username", required=True)
    parser.add_argument("--password", required=True)
    args = parser.parse_args()

    password_hash = bcrypt.hashpw(args.password.encode(), bcrypt.gensalt()).decode()

    if config.MODE == "dev":
        from app.store import SqliteStore
        store = SqliteStore()
        if store.get_user(args.username):
            sys.exit(f"user {args.username!r} already exists in {store.path}")
        store.create_user(args.username, password_hash)
        print(f"created dev user {args.username!r} in {store.path}")
        return

    import psycopg2
    conn = psycopg2.connect(
        host=os.environ["POSTGRES_URL"],
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ.get("POSTGRES_DB", "apps"),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )
    with conn, conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO {config.APP_SCHEMA}.users (username, password_hash) VALUES (%s, %s)",
            (args.username, password_hash),
        )
    conn.close()
    print(f"created user {args.username!r} in {config.APP_SCHEMA}.users")


if __name__ == "__main__":
    main()
