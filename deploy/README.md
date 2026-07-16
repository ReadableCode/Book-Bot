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
  `book_bot:8010`. **No Authelia anymore** (the forward-auth includes are
  removed, same as syncplex): book-bot owns its login hardening — see
  "login hardening & signup" below. SWAG still terminates TLS.
- `dotfiles/deploy_manifest.yaml` (`book_bot_env` entry) — symlinks this
  repo's `.env` → `personal_credentials/personal.env` on any machine with
  both repos cloned. On elitedesk that feeds the fragment's
  `${POSTGRES_USER}` / `${POSTGRES_PASSWORD}` / `${POSTGREST_JWT_SECRET}`
  interpolation; on dev machines it supplies the JWT secret and superuser
  creds for scripts while the app stays in SQLite dev mode (personal.env
  sets no `POSTGREST_URL`).

## schema setup is automatic

The app converges the database itself on every startup
(`app/bootstrap.py`, run from the FastAPI lifespan — book-bot's
`alembic upgrade head`), **wherever it runs**: a bare `uv run uvicorn`
on a dev machine in postgrest mode does exactly what the container
does; docker has no special role. When the superuser `POSTGRES_*` env
is present it checks `book_bot.deploy_meta` against
`bootstrap.SCHEMA_VERSION` (one SELECT) and only on a mismatch — i.e.
once per actual schema change, never per process start — creates the
`book_bot_user` role, applies `02_schema.sql` + `03_secure_users.sql` +
`04_user_libraries.sql` + `05_sample_library.sql`, stamps the new
version and reloads PostgREST's schema cache. (The SQL is idempotent
but not free: it takes exclusive locks and the cache reload disrupts
in-flight requests, so it must not run on every boot. Bump
`SCHEMA_VERSION` whenever the SQL files change.) Without superuser
creds it logs a skip and serves anyway. `scripts/init_db.py [--force]`
runs the same thing by hand, and the numbered SQL files remain runnable
directly (`psql -U postgres -d apps -f ...`); `01_create_role.sql` is
the manual, non-idempotent equivalent of the role block. The
cluster-global roles (`postgrest_authenticator`, `web_anon`) must
already exist — they do, from load-log's setup.

### the multi-user migration (04_user_libraries.sql)

`04_user_libraries.sql` upgrades a pre-multi-user database in one shot,
the first time it runs: ownership columns (`status`, `notes`, `copies`,
`status_changed_at`) move off `book_bot.editions` into
`book_bot.library_books`, inside a single **Family Library** that every
user existing *at migration time* co-owns. Users created afterwards get
their own empty library at first login and cannot see the Family Library
until a member shares it with them (`▤` button in the app's library
view). The file also installs row-level-security policies keyed on the
JWT's `user_id` claim, so library membership is enforced by Postgres
itself even for clients that talk to PostgREST directly.

Two operational notes:

- **JWTs must carry the user's uuid.** The postgrest-auth service must
  mint the `book_bot.users.id` value as a `user_id` (or `sub`) claim —
  the dev-mode login already does, and load-log-style tokens do too. A
  token without it is rejected by the API (clear 401) and matches no RLS
  policy, so nothing leaks; users just need to log in again after the
  auth service is updated. Verify with:
  `psql -c "SET ROLE book_bot_user; SET request.jwt.claims='{\"user_id\":\"<uuid>\"}'; SELECT count(*) FROM book_bot.libraries;"`
- **Restart PostgREST once after the first deploy** so its schema cache
  picks up the new tables and the `library_books → editions` /
  `read_states → works` embeddings.

## login hardening & signup (why Authelia isn't needed)

Book-bot fronts its own auth now, the same posture Sync_Plex ships:

- **bcrypt password hashes**, checked by the postgrest-auth service in
  prod (dev mode checks locally and pays the same bcrypt cost for
  unknown usernames — no enumeration by timing).
- **Rate limiting / lockout** on `/api/login`: 5 failures inside 15
  minutes locks that username *and* that client IP for 15 minutes
  (mirrors Authelia's regulation block). In-memory, per-process.
- **Security headers** (CSP, `X-Frame-Options: DENY`, nosniff,
  referrer-policy) on every response; TLS/HSTS stay at SWAG.
- **Self-signup** at `POST /api/signup` ("create an account" on the
  login screen): username policy `[a-z0-9][a-z0-9._-]{0,31}`, minimum
  10-char passwords, max 5 signups per 15 minutes per IP. Set
  `SIGNUP_ENABLED=false` to go invite-only (create_user.py still works).
  In prod the signup insert goes straight to Postgres with the
  superuser `POSTGRES_*` env (book_bot.users is unreachable through
  PostgREST by design) — the same path scripts/create_user.py uses.

### the sample library (05_sample_library.sql)

One shared, **view-only** library that every logged-in user can browse:
a `libraries` row with a fixed uuid
(`11111111-1111-1111-1111-111111111111`, see `app/config.py`). No
table/column changes — the migration only inserts that row, adds two
SELECT-only RLS policies keyed on the fixed id (never on the name, so a
user naming their own library "Sample Library" gains nothing), and
excludes it from the claim-a-memberless-library path. It has no members,
so every write policy already refuses it; in the app it shows up last in
`/api/me` with `role: "viewer"` and the frontend is strictly
browse-only for it.

Stocking is automatic and app-owned: every startup checks the shelf
(`app/bootstrap.py`, in a background thread so boot isn't blocked) and
fills it with the 300 well-known books from `app/sample_books.json` the
first time, no-op forever after. Set `SAMPLE_AUTOSTOCK=false` to opt a
process out. Rebuild the manifest with
`scripts/build_sample_library.py`, and top up an already-stocked shelf
with `scripts/seed_sample_library.py --force`.

## rollout on elitedesk (when ready)

```sh
# after pulling Book-Bot, Docker, server_configs, dotfiles + running the
# dotfiles deploy (creates Book-Bot/.env):
cd ~/GitHub/Docker
docker compose -f docker_compose_projects.yaml up -d --build book-bot
docker compose -f docker_compose_projects.yaml up -d postgrest   # picks up new PGRST_DB_SCHEMAS
docker compose -f docker_compose_projects.yaml restart swag      # loads bookbot.subdomain.conf

# create the login — run it INSIDE the container, which has the
# postgrest-mode env (POSTGREST_URL etc.) set by the compose fragment.
# Running the script from a host shell silently falls back to dev mode
# and writes to a local SQLite file instead of book_bot.users.
docker compose -f docker_compose_projects.yaml exec book-bot uv run python scripts/create_user.py --username beca --password '...'

# libraries + membership are managed the same way (also container-side —
# it uses the superuser POSTGRES_* env, bypassing the API and RLS):
docker compose -f docker_compose_projects.yaml exec book-bot uv run python scripts/manage_library.py list
docker compose -f docker_compose_projects.yaml exec book-bot uv run python scripts/manage_library.py create --name 'Cabin Books' --member jason --member beca
docker compose -f docker_compose_projects.yaml exec book-bot uv run python scripts/manage_library.py add-member --library 'Family Library' --username beca
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
