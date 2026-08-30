"""Create a book-bot login.

Inserts into book_bot.users in the shared apps database using the
superuser POSTGRES_* env vars (same as load-log's create_user). The hash
is argon2id, matching what the auth service mints.

    uv run python scripts/create_user.py --username beca --password '...'
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import accounts, config  # noqa: E402


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--username", required=True)
    parser.add_argument("--password", required=True)
    args = parser.parse_args()

    try:
        accounts.create_user(args.username, args.password)
    except ValueError as exc:
        sys.exit(str(exc))
    print(f"created user {args.username!r} in {config.APP_SCHEMA}.users")


if __name__ == "__main__":
    main()
