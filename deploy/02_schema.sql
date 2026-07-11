-- book-bot: one-time server setup, step 2 of 4.
-- Run as the postgres superuser against the shared "apps" database.
-- Idempotent: safe to re-run. Creates the current (multi-user) schema for
-- fresh installs; 04_user_libraries.sql upgrades pre-multi-user databases
-- and applies grants + row-level security.

CREATE SCHEMA IF NOT EXISTS book_bot;

-- credentials read ONLY by the postgrest-auth service (superuser);
-- step 3 revokes it from the PostgREST-facing roles.
CREATE TABLE IF NOT EXISTS book_bot.users (
    id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    username      text UNIQUE NOT NULL,
    password_hash text NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now()
);

-- one row per *book* (the story), grouping every edition of it.
-- Keyed by the Open Library work key when known, else a normalized
-- title+author key, so "do I have this in hardback?" is answerable
-- from any edition's barcode. Shared catalog: not owned by anyone.
-- cover_url is a denormalized "best cover" so read-history entries
-- for books nobody owns still render.
CREATE TABLE IF NOT EXISTS book_bot.works (
    id          uuid PRIMARY KEY,
    ol_work_key text UNIQUE,
    norm_key    text UNIQUE NOT NULL,
    title       text NOT NULL,
    authors     text,
    cover_url   text,
    created_at  timestamptz NOT NULL DEFAULT now()
);

-- one row per ISBN/edition: shared catalog metadata only. Who owns it,
-- in which library, and in what state lives in library_books. A future
-- Goodreads sync matches its CSV rows to editions by isbn13 and falls
-- back to the work's norm_key.
CREATE TABLE IF NOT EXISTS book_bot.editions (
    id               uuid PRIMARY KEY,
    work_id          uuid NOT NULL REFERENCES book_bot.works(id),
    isbn13           text UNIQUE,
    isbn10           text,
    title            text NOT NULL,
    subtitle         text,
    authors          text,
    publisher        text,
    published_date   text,
    description      text,
    format           text,
    cover_url        text,
    google_volume_id text,
    ol_edition_key   text,
    page_count       integer,
    language         text,
    added_at         timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_book_bot_editions_work ON book_bot.editions(work_id);

-- a library is a shared shelf; several users can own the same one.
CREATE TABLE IF NOT EXISTS book_bot.libraries (
    id         uuid PRIMARY KEY,
    name       text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS book_bot.library_members (
    library_id uuid NOT NULL REFERENCES book_bot.libraries(id) ON DELETE CASCADE,
    user_id    uuid NOT NULL REFERENCES book_bot.users(id) ON DELETE CASCADE,
    role       text NOT NULL DEFAULT 'owner',
    added_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (library_id, user_id)
);

CREATE INDEX IF NOT EXISTS idx_book_bot_members_user ON book_bot.library_members(user_id);

-- a copy (or several identical copies) of an edition on a library's
-- shelf or wishlist. Two different softcover printings are two editions
-- and therefore two rows; two identical softcovers are copies = 2.
CREATE TABLE IF NOT EXISTS book_bot.library_books (
    id                uuid PRIMARY KEY,
    library_id        uuid NOT NULL REFERENCES book_bot.libraries(id) ON DELETE CASCADE,
    edition_id        uuid NOT NULL REFERENCES book_bot.editions(id),
    status            text NOT NULL CHECK (status IN ('library', 'wishlist')),
    notes             text,
    copies            integer NOT NULL DEFAULT 1,
    added_at          timestamptz NOT NULL DEFAULT now(),
    status_changed_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (library_id, edition_id)
);

CREATE INDEX IF NOT EXISTS idx_book_bot_library_books_library ON book_bot.library_books(library_id);
CREATE INDEX IF NOT EXISTS idx_book_bot_library_books_edition ON book_bot.library_books(edition_id);

-- per-user reading history, keyed to the work (the story you read),
-- with an optional pointer to the specific edition. Independent of
-- ownership so "read but don't own" is a plain query. This is the
-- table a Goodreads sync writes shelves/read-dates/ratings into.
CREATE TABLE IF NOT EXISTS book_bot.read_states (
    id          uuid PRIMARY KEY,
    user_id     uuid NOT NULL REFERENCES book_bot.users(id) ON DELETE CASCADE,
    work_id     uuid NOT NULL REFERENCES book_bot.works(id),
    edition_id  uuid REFERENCES book_bot.editions(id),
    status      text NOT NULL CHECK (status IN ('want_to_read', 'reading', 'read')),
    rating      integer CHECK (rating BETWEEN 1 AND 5),
    notes       text,
    started_at  date,
    finished_at date,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now(),
    UNIQUE (user_id, work_id)
);

CREATE INDEX IF NOT EXISTS idx_book_bot_read_states_user ON book_bot.read_states(user_id);
