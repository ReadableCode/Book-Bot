FROM python:3.13-slim

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

COPY pyproject.toml uv.lock ./

RUN uv sync --frozen --no-dev

COPY app/ ./app/
COPY scripts/ ./scripts/

# deploy/ ships in the image because the app applies its schema/role setup
# itself at startup (app/bootstrap.py, from the FastAPI lifespan).
COPY deploy/ ./deploy/
COPY docker-entrypoint.sh ./docker-entrypoint.sh
RUN chmod +x ./docker-entrypoint.sh

EXPOSE 8010

ENTRYPOINT ["./docker-entrypoint.sh"]

CMD ["uv", "run", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8010"]
