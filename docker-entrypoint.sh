#!/bin/sh
# Apply the book_bot schema/roles before starting whatever command was passed
# (mirrors load-log's entrypoint, which runs `alembic upgrade head`).
# scripts/init_db.py is idempotent: CREATE ... IF NOT EXISTS everywhere, so
# restarts are no-ops. Skipped when no superuser credentials are provided
# (e.g. running the image in SQLite dev mode).
set -e

if [ -n "$POSTGRES_PASSWORD" ]; then
    echo "[entrypoint] applying book_bot schema (scripts/init_db.py)..."
    uv run python scripts/init_db.py
else
    echo "[entrypoint] POSTGRES_PASSWORD not set - skipping schema setup"
fi

echo "[entrypoint] starting: $*"
exec "$@"
