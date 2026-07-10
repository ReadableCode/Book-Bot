# deploying book-bot against the shared postgrest stack

book-bot follows the load-log pattern: no database code of its own in the
request path — a `book_bot` schema in the shared `apps` Postgres, CRUD via
the shared PostgREST, logins via the postgrest-auth service. In production
it runs as a container in the elitedesk stack at
**https://bookbot.tinkernet.me**.

## how the pieces fit (all prepared, awaiting merge)

- `deploy/compose.elitedesk.yaml` (this repo) — the `book-bot` service:
  builds the repo's `Dockerfile`, serves on port **8010**, talks to
  `http://postgrest:3000` / `http://auth:8000` inside the compose network.
- `Docker/docker_compose_projects.yaml` — pulls the fragment in via
  `include:`, and its `postgrest` service exposes the schema:
  `PGRST_DB_SCHEMAS: load_log, book_bot` (book-bot's client selects its
  schema with `Accept-Profile` / `Content-Profile` headers; `load_log`
  stays the default, so load-log is unaffected).
- `server_configs/application_configs/swag/elitedesk/proxy-confs/`
  `bookbot.subdomain.conf` — SWAG reverse proxy for `bookbot.*` →
  `book_bot:8010`, behind Authelia like loadlog.
- `dotfiles/deploy_manifest.yaml` (`book_bot_env` entry) — symlinks this
  repo's `.env` → `personal_credentials/personal.env` on any machine with
  both repos cloned. On elitedesk that feeds the fragment's
  `${POSTGRES_USER}` / `${POSTGRES_PASSWORD}` / `${POSTGREST_JWT_SECRET}`
  interpolation; on dev machines it supplies the JWT secret and superuser
  creds for scripts while the app stays in SQLite dev mode (personal.env
  sets no `POSTGREST_URL`).

## schema setup is automatic

The container entrypoint runs `scripts/init_db.py` before uvicorn starts
(book-bot's `alembic upgrade head`): it idempotently creates the
`book_bot_user` role and applies `02_schema.sql` + `03_secure_users.sql`.
The numbered SQL files remain runnable by hand (`psql -U postgres -d apps
-f ...`) if you prefer; `01_create_role.sql` is the manual, non-idempotent
equivalent of the role block in init_db.py. The cluster-global roles
(`postgrest_authenticator`, `web_anon`) must already exist — they do,
from load-log's setup.

## rollout on elitedesk (when ready)

```sh
# after pulling Book-Bot, Docker, server_configs, dotfiles + running the
# dotfiles deploy (creates Book-Bot/.env):
cd ~/GitHub/Docker
docker compose -f docker_compose_projects.yaml up -d --build book-bot
docker compose -f docker_compose_projects.yaml up -d postgrest   # picks up new PGRST_DB_SCHEMAS
docker compose -f docker_compose_projects.yaml restart swag      # loads bookbot.subdomain.conf

# create the login (from the Book-Bot repo; uses the .env superuser creds)
uv run python scripts/create_user.py --username beca --password '...'
```

DNS: `bookbot.tinkernet.me` is covered by the existing `*.tinkernet.me`
wildcard (SWAG SUBDOMAINS=wildcard), so no DNS change is needed.

If PostgREST 404s the new tables right after first boot, restart it once
more to refresh its schema cache.

## running the app outside docker (any machine)

```sh
POSTGREST_URL=https://pgrest.tinkernet.me \
AUTH_URL=https://auth.tinkernet.me \
uv run uvicorn app.main:app --host 0.0.0.0 --port 8010
```

(`JWT_SECRET` comes from the `.env` symlink's `POSTGREST_JWT_SECRET`.)
Camera scanning needs HTTPS, so phones should use the SWAG domain.
