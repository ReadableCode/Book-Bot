"""Disable, re-enable, change the password on, or delete a book-bot login.

Creation lives in scripts/create_user.py; this is everything after that.
Connects straight to Postgres with the superuser POSTGRES_* env vars — run
it inside the book-bot container on the server, where those are set (see
deploy/README.md).

Disable, enable and set-password all revoke the account's live sessions:
they bump password_changed_at, and app/auth.py rejects any token issued
before it (within 30 s, the revocation cache TTL).

    uv run python scripts/manage_user.py show --username beca
    uv run python scripts/manage_user.py disable --username beca
    uv run python scripts/manage_user.py enable --username beca
    uv run python scripts/manage_user.py set-password --username beca --password '...'
    uv run python scripts/manage_user.py delete --username beca
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import accounts  # noqa: E402


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("show", "disable", "enable", "delete"):
        p = sub.add_parser(name)
        p.add_argument("--username", required=True)
    p = sub.add_parser("set-password")
    p.add_argument("--username", required=True)
    p.add_argument("--password", required=True)
    args = parser.parse_args()

    try:
        if args.command == "show":
            user = accounts.get_user(args.username)
            if not user:
                sys.exit(f"no user named {args.username!r}")
            for key in ("id", "username", "role", "display_name", "disabled",
                        "created_at", "password_changed_at"):
                print(f"{key:20} {user[key]}")
            print(f"{'hash algorithm':20} "
                  f"{'bcrypt (legacy)' if user['password_hash'].startswith('$2') else 'argon2id'}")
            return

        if args.command == "disable":
            accounts.set_disabled(args.username, True)
            print(f"disabled {args.username!r} — live sessions revoked")
        elif args.command == "enable":
            accounts.set_disabled(args.username, False)
            print(f"enabled {args.username!r} — they must log in again")
        elif args.command == "set-password":
            accounts.set_password(args.username, args.password)
            print(f"password changed for {args.username!r} — live sessions revoked")
        elif args.command == "delete":
            accounts.remove_user(args.username)
            print(f"deleted {args.username!r} (memberships and read states cascaded)")
    except ValueError as exc:
        sys.exit(str(exc))


if __name__ == "__main__":
    main()
