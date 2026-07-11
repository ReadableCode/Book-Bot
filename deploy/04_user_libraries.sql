-- book-bot: server setup, step 4 — multi-user libraries + read states.
-- Run as the postgres superuser AFTER 02_schema.sql. Idempotent.
--
-- Does three things:
--   1. upgrades a pre-multi-user database: ownership columns move off
--      book_bot.editions into book_bot.library_books, inside one shared
--      "Family Library" that every already-existing user owns (new users
--      created later get their own library and can NOT see this one);
--   2. exposes usernames (only) through book_bot.user_directory so
--      members can be invited by name while 03_secure_users.sql keeps
--      password hashes unreachable;
--   3. applies grants and row-level security keyed on the JWT's user_id
--      claim, so PostgREST enforces library membership even for clients
--      that bypass the app.

-- works gains a denormalized cover for read-history rows nobody owns
-- (02_schema.sql includes it on fresh installs).
ALTER TABLE book_bot.works ADD COLUMN IF NOT EXISTS cover_url text;

-- widen the holding-status CHECK to include 'digital' on databases created
-- before it existed (the DROP+ADD pair is idempotent).
ALTER TABLE book_bot.library_books DROP CONSTRAINT IF EXISTS library_books_status_check;
ALTER TABLE book_bot.library_books ADD CONSTRAINT library_books_status_check
    CHECK (status IN ('library', 'wishlist', 'digital'));

-- ---------------------------------------------------------------------
-- 1. data migration from the single-library schema
-- ---------------------------------------------------------------------

DO $$
DECLARE
    shared_library uuid;
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_schema = 'book_bot'
                 AND table_name = 'editions'
                 AND column_name = 'status') THEN
        shared_library := gen_random_uuid();
        INSERT INTO book_bot.libraries (id, name) VALUES (shared_library, 'Family Library');
        INSERT INTO book_bot.library_members (library_id, user_id, role)
            SELECT shared_library, id, 'owner' FROM book_bot.users;
        INSERT INTO book_bot.library_books
            (id, library_id, edition_id, status, notes, copies, added_at, status_changed_at)
            SELECT gen_random_uuid(), shared_library, id,
                   -- an owned ebook/audiobook is by definition owned digitally
                   CASE WHEN status = 'library' AND format IN ('ebook', 'audiobook')
                        THEN 'digital' ELSE status END,
                   notes, copies, added_at, status_changed_at
            FROM book_bot.editions;
        ALTER TABLE book_bot.editions DROP COLUMN status;
        ALTER TABLE book_bot.editions DROP COLUMN notes;
        ALTER TABLE book_bot.editions DROP COLUMN copies;
        ALTER TABLE book_bot.editions DROP COLUMN status_changed_at;
    END IF;
END $$;

-- best-effort work covers from any edition that has one (idempotent).
UPDATE book_bot.works w
SET cover_url = e.cover_url
FROM (SELECT DISTINCT ON (work_id) work_id, cover_url
      FROM book_bot.editions
      WHERE cover_url IS NOT NULL
      ORDER BY work_id, added_at) e
WHERE w.id = e.work_id AND w.cover_url IS NULL;

-- ---------------------------------------------------------------------
-- 2. username directory (no password hashes)
-- ---------------------------------------------------------------------

CREATE OR REPLACE VIEW book_bot.user_directory AS
    SELECT id, username, created_at FROM book_bot.users;

-- ---------------------------------------------------------------------
-- 3. grants + row-level security
-- ---------------------------------------------------------------------

GRANT USAGE ON SCHEMA book_bot TO book_bot_user;
-- shared catalog: readable/writable by any logged-in user, never deletable
-- through the API role (library_books rows reference it).
GRANT SELECT, INSERT, UPDATE ON book_bot.works, book_bot.editions TO book_bot_user;
REVOKE DELETE ON book_bot.works, book_bot.editions FROM book_bot_user;
GRANT SELECT, INSERT, UPDATE, DELETE
    ON book_bot.libraries, book_bot.library_members,
       book_bot.library_books, book_bot.read_states
    TO book_bot_user;
GRANT SELECT ON book_bot.user_directory TO book_bot_user;

-- The JWT must carry the user's uuid: postgrest-auth mints it from
-- book_bot.users.id as the "user_id" claim ("sub" also accepted).
CREATE OR REPLACE FUNCTION book_bot.jwt_user_id() RETURNS uuid
LANGUAGE plpgsql STABLE AS $$
DECLARE
    claims text := NULLIF(current_setting('request.jwt.claims', true), '');
    uid text;
BEGIN
    IF claims IS NOT NULL THEN
        uid := COALESCE(claims::jsonb ->> 'user_id', claims::jsonb ->> 'sub');
    END IF;
    -- pre-v9 PostgREST exposes claims as individual settings
    uid := COALESCE(uid, NULLIF(current_setting('request.jwt.claim.user_id', true), ''));
    RETURN uid::uuid;
EXCEPTION WHEN others THEN
    RETURN NULL;
END $$;

-- SECURITY DEFINER so membership checks inside policies don't recurse
-- into library_members' own policies.
CREATE OR REPLACE FUNCTION book_bot.is_library_member(lib uuid) RETURNS boolean
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = '' AS $$
    SELECT EXISTS (SELECT 1 FROM book_bot.library_members
                   WHERE library_id = lib AND user_id = book_bot.jwt_user_id())
$$;

CREATE OR REPLACE FUNCTION book_bot.library_has_members(lib uuid) RETURNS boolean
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = '' AS $$
    SELECT EXISTS (SELECT 1 FROM book_bot.library_members WHERE library_id = lib)
$$;

ALTER TABLE book_bot.libraries       ENABLE ROW LEVEL SECURITY;
ALTER TABLE book_bot.library_members ENABLE ROW LEVEL SECURITY;
ALTER TABLE book_bot.library_books   ENABLE ROW LEVEL SECURITY;
ALTER TABLE book_bot.read_states     ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS libraries_select ON book_bot.libraries;
CREATE POLICY libraries_select ON book_bot.libraries
    FOR SELECT USING (book_bot.is_library_member(id));
DROP POLICY IF EXISTS libraries_insert ON book_bot.libraries;
CREATE POLICY libraries_insert ON book_bot.libraries
    FOR INSERT WITH CHECK (book_bot.jwt_user_id() IS NOT NULL);
DROP POLICY IF EXISTS libraries_update ON book_bot.libraries;
CREATE POLICY libraries_update ON book_bot.libraries
    FOR UPDATE USING (book_bot.is_library_member(id));
DROP POLICY IF EXISTS libraries_delete ON book_bot.libraries;
CREATE POLICY libraries_delete ON book_bot.libraries
    FOR DELETE USING (book_bot.is_library_member(id));

DROP POLICY IF EXISTS members_select ON book_bot.library_members;
CREATE POLICY members_select ON book_bot.library_members
    FOR SELECT USING (book_bot.is_library_member(library_id));
-- members may invite; a just-created (still member-less) library may only
-- be claimed by its creator adding themself.
DROP POLICY IF EXISTS members_insert ON book_bot.library_members;
CREATE POLICY members_insert ON book_bot.library_members
    FOR INSERT WITH CHECK (
        book_bot.is_library_member(library_id)
        OR (user_id = book_bot.jwt_user_id() AND NOT book_bot.library_has_members(library_id))
    );
-- members may only remove themselves (leave); nobody can strip a shared
-- library's other members and claim its shelves via the member-less path.
DROP POLICY IF EXISTS members_delete ON book_bot.library_members;
CREATE POLICY members_delete ON book_bot.library_members
    FOR DELETE USING (user_id = book_bot.jwt_user_id());

DROP POLICY IF EXISTS library_books_all ON book_bot.library_books;
CREATE POLICY library_books_all ON book_bot.library_books
    FOR ALL USING (book_bot.is_library_member(library_id))
    WITH CHECK (book_bot.is_library_member(library_id));

DROP POLICY IF EXISTS read_states_all ON book_bot.read_states;
CREATE POLICY read_states_all ON book_bot.read_states
    FOR ALL USING (user_id = book_bot.jwt_user_id())
    WITH CHECK (user_id = book_bot.jwt_user_id());
