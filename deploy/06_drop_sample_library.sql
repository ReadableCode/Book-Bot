-- book-bot: server setup, step 6 — retire the shared Sample Library.
-- Run as the postgres superuser AFTER 04_user_libraries.sql. Idempotent,
-- and safe on a database that never had the sample library at all.
--
-- Users see only their own books now. This reverses 05_sample_library.sql
-- (deleted from the repo): the world-readable SELECT policies go, the
-- fixed-uuid library and its holdings go, and the is_sample_library()
-- designator goes with them.
--
-- 05 also replaced 04's members_insert policy with a variant that
-- excluded the sample library from the "claim a member-less library"
-- path. Nothing to undo here: 04 runs first on every apply and its
-- DROP POLICY IF EXISTS / CREATE POLICY pair restores the original.

-- policies first — they depend on is_sample_library().
DROP POLICY IF EXISTS libraries_sample_read ON book_bot.libraries;
DROP POLICY IF EXISTS library_books_sample_read ON book_bot.library_books;

-- the holdings, then the library row. Editions and works are the shared
-- catalog owned by nobody and are deliberately left alone: other users'
-- libraries reference the same rows.
DELETE FROM book_bot.library_books
WHERE library_id = '11111111-1111-1111-1111-111111111111'::uuid;

-- read_states are per-user reading history keyed to the work, independent
-- of ownership. Someone who marked a sample book as read keeps that;
-- it is their data, not the sample library's.

DELETE FROM book_bot.libraries
WHERE id = '11111111-1111-1111-1111-111111111111'::uuid;

DROP FUNCTION IF EXISTS book_bot.is_sample_library(uuid);
