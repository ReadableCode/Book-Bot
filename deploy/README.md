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
  interpolation; on dev machines it supplies the same `POSTGREST_URL`,
  JWT secret and superuser creds. There is no dev mode: local
  development points at the real deployment, which is what the rest of
  the fleet does (conventions **I8**).

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
`04_user_libraries.sql` + `06_drop_sample_library.sql`, stamps the new
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

- **argon2id password hashes**, checked by the postgrest-auth service in
  prod (legacy bcrypt hashes verify by prefix and get rehashed to
  argon2id on the next successful login; the reject path pays the same
  KDF cost for unknown usernames — no enumeration by timing). Sessions
  die when `password_changed_at` moves past their issue time — password
  change, disable, and re-enable all revoke, enforced per request by
  `app/auth.py` against a 30 s cached read.
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

### the sample library is gone (06_drop_sample_library.sql)

Book-bot used to ship a shared, view-only "Sample Library" that every
logged-in account saw alongside its own shelves. It is retired: users see
only their own books. `06_drop_sample_library.sql` drops the two
world-readable SELECT policies, deletes the fixed-uuid library row and
its holdings, and drops the `is_sample_library()` designator. It is
idempotent and safe on a database that never had it.

The 300 sample books stay in the shared `works`/`editions` catalog —
that catalog is owned by nobody and doubles as a metadata cache, and
other users' libraries reference the same rows. Read states on sample
books are per-user reading history and are deliberately left alone.

`04_user_libraries.sql` runs before this file on every apply and
recreates the `members_insert` policy that `05_sample_library.sql` used
to override, so there is nothing to restore by hand.

## deploy order (this one matters)

Three steps, and Book-Bot's app code goes **last**:

1. **Converge the schema** — *already done, 2026-08-28.*
   `apply_schema()` was run from the dev machine (it is the same
   version-gated function the app runs at startup); `book_bot.deploy_meta`
   is at version 7, `book_bot.users` has all four new columns, and the
   sample library is gone. The change is additive, so the
   currently-running auth service — which selects none of those columns —
   was unaffected, and the running book-bot container simply stopped
   showing the sample library.
2. **Rebuild postgrest-auth.** Its `SELECT` needs `disabled`, so it had to
   come after step 1. It starts verifying argon2id and minting `iat` and
   `username`. **This is the outstanding step.**
3. **Deploy book-bot's app code.**

Do not put step 3 before step 2. `app/auth.py` rejects any token that has
no `iat` claim, and the old auth service never mints one — so with the new
app in front of the old service, every user would log in successfully and
then get a 401 on every subsequent request, permanently. That is not the
"one forced re-login" it looks like; there is no token the old service can
issue that the new app will accept.

Once step 2 lands, sessions minted by the old service stop validating and
everyone re-logs in once. That is the expected, one-time cost.

## rollout on elitedesk (when ready)

```sh
# after pulling Book-Bot, Docker, server_configs, dotfiles + running the
# dotfiles deploy (creates Book-Bot/.env):
cd ~/GitHub/Docker
docker compose -f docker_compose_projects.yaml up -d --build book-bot
docker compose -f docker_compose_projects.yaml up -d postgrest   # picks up new PGRST_DB_SCHEMAS
docker compose -f docker_compose_projects.yaml restart swag      # loads bookbot.subdomain.conf

# create the login — run it INSIDE the container, which has the
# superuser POSTGRES_* env set by the compose fragment.
docker compose -f docker_compose_projects.yaml exec book-bot uv run python scripts/create_user.py --username beca --password '...'

# after that: disable, re-enable, change a password, or delete an account.
# disable/enable/set-password all revoke the account's live sessions.
docker compose -f docker_compose_projects.yaml exec book-bot uv run python scripts/manage_user.py show --username beca
docker compose -f docker_compose_projects.yaml exec book-bot uv run python scripts/manage_user.py disable --username beca
docker compose -f docker_compose_projects.yaml exec book-bot uv run python scripts/manage_user.py set-password --username beca --password '...'

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
