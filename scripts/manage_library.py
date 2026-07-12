"""Manage libraries and their members from the command line.

Same dual-mode pattern as create_user.py:
  dev mode (no POSTGREST_URL set): operates on the local SQLite database.
  postgrest mode: connects straight to Postgres with the superuser
      POSTGRES_* env vars, bypassing the API and row-level security —
      run it inside the book-bot container on the server, where those
      are set (see deploy/README.md).

    uv run python scripts/manage_library.py list
    uv run python scripts/manage_library.py create --name "Cabin Books" \
        --member jason --member beca
    uv run python scripts/manage_library.py add-member \
        --library "Family Library" --username beca

`--library` takes a library name or uuid; names must be unambiguous.
"""

import argparse
import os
import sys
import uuid

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import config  # noqa: E402
from app.store import now_iso  # noqa: E402


# --------------------------------------------------------------------------
# backends — same operations against SQLite (dev) or Postgres (prod)
# --------------------------------------------------------------------------

class SqliteBackend:
    def __init__(self):
        from app.store import SqliteStore
        self.store = SqliteStore()
        print(f"dev mode — {self.store.path}")

    def libraries(self):
        libs = self.store._rows("SELECT * FROM libraries ORDER BY created_at")
        members = self.store._rows(
            "SELECT m.library_id, u.username FROM library_members m "
            "JOIN users u ON u.id = m.user_id ORDER BY m.added_at")
        counts = self.store._rows(
            "SELECT library_id, count(*) AS n FROM library_books GROUP BY library_id")
        by_lib = {}
        for m in members:
            by_lib.setdefault(str(m["library_id"]), []).append(m["username"])
        n_by_lib = {str(c["library_id"]): c["n"] for c in counts}
        return [{**lib, "members": by_lib.get(str(lib["id"]), []),
                 "books": n_by_lib.get(str(lib["id"]), 0)} for lib in libs]

    def user_by_name(self, username):
        return self.store.find_user_by_username(None, username)

    def create_library(self, name):
        library = {"id": str(uuid.uuid4()), "name": name, "created_at": now_iso()}
        self.store.create_library(None, library)
        return library["id"]

    def add_member(self, library_id, user_id):
        self.store.add_member(None, {
            "library_id": library_id, "user_id": user_id,
            "role": "owner", "added_at": now_iso(),
        })


class PostgresBackend:
    def __init__(self):
        import psycopg2
        import psycopg2.extras
        self.conn = psycopg2.connect(
            host=os.environ["POSTGRES_URL"],
            port=os.environ.get("POSTGRES_PORT", "5432"),
            dbname=os.environ.get("POSTGRES_DB", "apps"),
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
        )
        self.conn.autocommit = True
        self.cursor_factory = psycopg2.extras.RealDictCursor
        print(f"postgrest mode — {os.environ['POSTGRES_URL']}/{os.environ.get('POSTGRES_DB', 'apps')}")

    def _rows(self, query, params=()):
        with self.conn.cursor(cursor_factory=self.cursor_factory) as cur:
            cur.execute(query, params)
            return [dict(r) for r in cur.fetchall()] if cur.description else []

    def libraries(self):
        libs = self._rows(f"SELECT * FROM {config.APP_SCHEMA}.libraries ORDER BY created_at")
        members = self._rows(
            f"SELECT m.library_id, u.username FROM {config.APP_SCHEMA}.library_members m "
            f"JOIN {config.APP_SCHEMA}.users u ON u.id = m.user_id ORDER BY m.added_at")
        counts = self._rows(
            f"SELECT library_id, count(*) AS n FROM {config.APP_SCHEMA}.library_books "
            "GROUP BY library_id")
        by_lib = {}
        for m in members:
            by_lib.setdefault(str(m["library_id"]), []).append(m["username"])
        n_by_lib = {str(c["library_id"]): c["n"] for c in counts}
        return [{**lib, "members": by_lib.get(str(lib["id"]), []),
                 "books": n_by_lib.get(str(lib["id"]), 0)} for lib in libs]

    def user_by_name(self, username):
        rows = self._rows(
            f"SELECT id, username FROM {config.APP_SCHEMA}.users WHERE username = %s", (username,))
        return rows[0] if rows else None

    def create_library(self, name):
        library_id = str(uuid.uuid4())
        self._rows(f"INSERT INTO {config.APP_SCHEMA}.libraries (id, name) VALUES (%s, %s)",
                   (library_id, name))
        return library_id

    def add_member(self, library_id, user_id):
        self._rows(
            f"INSERT INTO {config.APP_SCHEMA}.library_members (library_id, user_id, role) "
            "VALUES (%s, %s, 'owner')", (library_id, str(user_id)))


def get_backend():
    return SqliteBackend() if config.MODE == "dev" else PostgresBackend()


# --------------------------------------------------------------------------
# commands
# --------------------------------------------------------------------------

def find_library(backend, selector):
    libs = backend.libraries()
    exact = [lib for lib in libs if str(lib["id"]) == selector]
    if not exact:
        exact = [lib for lib in libs if lib["name"] == selector]
    if not exact:
        exact = [lib for lib in libs if lib["name"].lower() == selector.lower()]
    if not exact:
        sys.exit(f"no library named {selector!r} — try `manage_library.py list`")
    if len(exact) > 1:
        ids = ", ".join(str(lib["id"]) for lib in exact)
        sys.exit(f"{selector!r} is ambiguous ({len(exact)} libraries: {ids}) — use the uuid")
    return exact[0]


def resolve_user(backend, username):
    user = backend.user_by_name(username)
    if not user:
        sys.exit(f"no user named {username!r} — create one with scripts/create_user.py first")
    return user


def cmd_list(backend, _args):
    libs = backend.libraries()
    if not libs:
        print("no libraries yet")
        return
    for lib in libs:
        members = ", ".join(lib["members"]) or "(no members)"
        print(f"{lib['id']}  {lib['name']!r}  {lib['books']} book(s)  members: {members}")


def cmd_create(backend, args):
    name = args.name.strip()
    if not name:
        sys.exit("--name must not be empty")
    if any(lib["name"] == name for lib in backend.libraries()):
        sys.exit(f"a library named {name!r} already exists")
    users = [resolve_user(backend, u) for u in (args.member or [])]
    library_id = backend.create_library(name)
    for user in users:
        backend.add_member(library_id, user["id"])
    who = ", ".join(u["username"] for u in users) or "nobody yet — add with add-member"
    print(f"created library {name!r} ({library_id}); members: {who}")


def cmd_add_member(backend, args):
    library = find_library(backend, args.library)
    user = resolve_user(backend, args.username)
    if user["username"] in library["members"]:
        sys.exit(f"{user['username']} is already a member of {library['name']!r}")
    backend.add_member(library["id"], user["id"])
    print(f"added {user['username']} to {library['name']!r} "
          f"(now: {', '.join(library['members'] + [user['username']])})")


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("list", help="show every library with members and book counts")

    p_create = sub.add_parser("create", help="create a library")
    p_create.add_argument("--name", required=True)
    p_create.add_argument("--member", action="append",
                          help="username to add as an owner (repeatable)")

    p_add = sub.add_parser("add-member", help="add a user to an existing library")
    p_add.add_argument("--library", required=True, help="library name or uuid")
    p_add.add_argument("--username", required=True)

    args = parser.parse_args()
    backend = get_backend()
    {"list": cmd_list, "create": cmd_create, "add-member": cmd_add_member}[args.command](backend, args)


if __name__ == "__main__":
    main()
