#!/bin/sh
# Nothing docker-specific happens here: schema setup and sample-library
# stocking are the app's own job at startup (app/bootstrap.py, run from
# the FastAPI lifespan), identical for a container and a bare uvicorn.
set -e

echo "[entrypoint] starting: $*"
exec "$@"
