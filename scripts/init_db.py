"""Manually apply the book_bot schema (postgrest mode, superuser env).

The app does this itself on every startup (app/bootstrap.py, called from
the FastAPI lifespan), so this wrapper exists only for running the same
idempotent setup by hand — e.g. against a fresh database before any app
process has pointed at it.

    uv run python scripts/init_db.py
"""

import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import bootstrap  # noqa: E402

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true",
                        help="apply the SQL files even if deploy_meta already records the current version")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="[init_db] %(message)s")
    try:
        bootstrap.apply_schema(force=args.force)
    except Exception as exc:
        sys.exit(str(exc))
