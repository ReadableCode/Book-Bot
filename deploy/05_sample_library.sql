-- book-bot: server setup, step 5 — the shared, view-only Sample Library.
-- Run as the postgres superuser AFTER 04_user_libraries.sql. Idempotent.
--
-- One library with a fixed uuid that every logged-in user can browse and
-- nobody can edit: it has NO members, so every write policy (all keyed on
-- membership) already refuses it — the additions here are read-only SELECT
-- policies plus a guard so the "claim a member-less library" signup path
-- can't be used to take it over. No table/column changes at all.
-- Fill it with scripts/seed_sample_library.py (inside the container).

INSERT INTO book_bot.libraries (id, name)
VALUES ('11111111-1111-1111-1111-111111111111', 'Sample Library')
ON CONFLICT (id) DO NOTHING;

-- designation by fixed id, not by name — a user naming their own library
-- "Sample Library" must not make it world-readable.
CREATE OR REPLACE FUNCTION book_bot.is_sample_library(lib uuid) RETURNS boolean
LANGUAGE sql IMMUTABLE AS $$
    SELECT lib = '11111111-1111-1111-1111-111111111111'::uuid
$$;

-- any authenticated user may read the sample library and its books
-- (permissive policies OR with the membership ones; SELECT-only, so they
-- grant no writes).
DROP POLICY IF EXISTS libraries_sample_read ON book_bot.libraries;
CREATE POLICY libraries_sample_read ON book_bot.libraries
    FOR SELECT USING (
        book_bot.is_sample_library(id) AND book_bot.jwt_user_id() IS NOT NULL);

DROP POLICY IF EXISTS library_books_sample_read ON book_bot.library_books;
CREATE POLICY library_books_sample_read ON book_bot.library_books
    FOR SELECT USING (
        book_bot.is_sample_library(library_id) AND book_bot.jwt_user_id() IS NOT NULL);

-- 04's members_insert lets a user claim any member-less library (that's how
-- a freshly created library gets its creator). The sample library is
-- member-less forever, so exclude it from that path.
DROP POLICY IF EXISTS members_insert ON book_bot.library_members;
CREATE POLICY members_insert ON book_bot.library_members
    FOR INSERT WITH CHECK (
        NOT book_bot.is_sample_library(library_id)
        AND (
            book_bot.is_library_member(library_id)
            OR (user_id = book_bot.jwt_user_id()
                AND NOT book_bot.library_has_members(library_id))
        )
    );
