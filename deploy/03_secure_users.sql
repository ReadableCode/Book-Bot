-- book-bot users lockdown, applied by app/bootstrap.py at startup (SCHEMA_FILES).
-- Running this file by hand with psql might work, but it is not the intended
-- use: change it here and let the app apply it.
-- The users table must never be reachable through PostgREST
-- (mirrors load-log/deploy/03_secure_users.sql). Usernames alone are
-- exposed through book_bot.user_directory (04_user_libraries.sql).

REVOKE ALL ON book_bot.users FROM book_bot_user;
REVOKE ALL ON book_bot.users FROM web_anon;
