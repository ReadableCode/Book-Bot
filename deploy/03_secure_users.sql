-- book-bot: one-time server setup, step 3 of 3.
-- The users table must never be reachable through PostgREST
-- (mirrors load-log/deploy/03_post_migrate.sql).

REVOKE ALL ON book_bot.users FROM book_bot_user;
REVOKE ALL ON book_bot.users FROM web_anon;
