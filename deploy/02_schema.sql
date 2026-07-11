-- book-bot: one-time server setup, step 2 of 3.
-- Run as the postgres superuser against the shared "apps" database.
-- Idempotent: safe to re-run.

CREATE SCHEMA IF NOT EXISTS book_bot;

-- one row per *book* (the story), grouping every edition of it.
-- Keyed by the Open Library work key when known, else a normalized
-- title+author key, so "do I have this in hardback?" is answerable
-- from any edition's barcode.
CREATE TABLE IF NOT EXISTS book_bot.works (
    id          uuid PRIMARY KEY,
    ol_work_key text UNIQUE,
    norm_key    text UNIQUE NOT NULL,
    title       text NOT NULL,
    authors     text,
    created_at  timestamptz NOT NULL DEFAULT now()
);

-- one row per ISBN/edition owned or wished for.
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
    genre            text,
    status           text NOT NULL CHECK (status IN ('library', 'wishlist')),
    notes            text,
    copies           integer NOT NULL DEFAULT 1,
    added_at         timestamptz NOT NULL DEFAULT now(),
    status_changed_at timestamptz NOT NULL DEFAULT now()
);

-- added after initial deploys — picks the column up on databases created
-- before genre existed (no-op on fresh ones).
ALTER TABLE book_bot.editions ADD COLUMN IF NOT EXISTS genre text;

CREATE INDEX IF NOT EXISTS idx_book_bot_editions_work ON book_bot.editions(work_id);
CREATE INDEX IF NOT EXISTS idx_book_bot_editions_status ON book_bot.editions(status);

-- credentials read ONLY by the postgrest-auth service (superuser);
-- step 3 revokes it from the PostgREST-facing roles.
CREATE TABLE IF NOT EXISTS book_bot.users (
    id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    username      text UNIQUE NOT NULL,
    password_hash text NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now()
);

GRANT USAGE ON SCHEMA book_bot TO book_bot_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON book_bot.works, book_bot.editions TO book_bot_user;
