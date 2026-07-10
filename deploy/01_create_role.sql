-- book-bot: one-time server setup, step 1 of 3.
-- Run as the postgres superuser against the shared "apps" database
-- (same cluster load_log lives in). Mirrors load-log/deploy/01_create_roles.sql;
-- postgrest_authenticator and web_anon already exist from that setup.

CREATE ROLE book_bot_user NOLOGIN;
GRANT book_bot_user TO postgrest_authenticator;
