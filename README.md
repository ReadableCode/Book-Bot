# ❯ book-bot // personal library terminal

A self-hosted, mobile-first website for managing a home library: scan a
book's barcode (or search title/author), and file it on the shelves or the
wishlist. Built for shopping trips — scanning any edition of a book tells
you whether you already own *that story* in another binding.

## how it answers "do I already have this?"

Every ISBN identifies one specific *edition* (hardcover vs. paperback vs.
special edition all have different ISBNs). book-bot stores two levels:

- **works** — the story itself, grouped by the Open Library *work key*
  (which links all editions of a book), with a normalized title+author key
  as fallback when Open Library doesn't know the edition.
- **editions** — each specific ISBN you own or want, with format, notes,
  and status (`library` / `wishlist`).

So when a scan finds an ISBN that isn't in the database, the app still
resolves its work and answers: *"not this edition, but you have this book:
hardcover — in library."*

Book barcodes are Bookland EAN-13 (start 978/979) and *are* the ISBN.
Older mass-market paperbacks sometimes carry a retail UPC that doesn't
encode the ISBN — the app detects that and suggests scanning the barcode
inside the cover or searching by title.

## stack

- **backend** — FastAPI (Python, run with `uv`). Metadata from Google
  Books + Open Library, merged.
- **data** — the shared `apps` Postgres via **PostgREST**, with logins
  through the **postgrest-auth** service (identical pattern to load-log:
  `book_bot` schema, `book_bot_user` role, JWT bearer tokens). A SQLite
  dev mode runs everything locally with no Postgres/Docker.
- **frontend** — vanilla JS PWA in the terminal-navy style
  (style-terminal-navy tokens). Barcode scanning via the native
  BarcodeDetector API where available, vendored ZXing elsewhere
  (iPhone Safari). Installable to the home screen.

## run it locally (dev mode, SQLite)

```sh
uv sync
uv run python scripts/create_user.py --username beca --password 'choose-one'
uv run uvicorn app.main:app --host 0.0.0.0 --port 8010
```

Open http://127.0.0.1:8010. Data lands in `data/book_bot.db` (gitignored).

> Camera scanning needs a secure origin: `http://localhost` works on the
> same machine, but to scan from a phone you need HTTPS (deploy behind the
> reverse proxy, or use manual ISBN entry / title search).

## production

See [`deploy/README.md`](deploy/README.md): three idempotent SQL files add
the `book_bot` schema/role/users to the shared `apps` database, PostgREST
gets `book_bot` appended to `PGRST_DB_SCHEMAS`, and the app runs with
`POSTGREST_URL`/`AUTH_URL`/`JWT_SECRET` set, behind SWAG with HTTPS.

## importing old scans

Earlier barcode scans (JSONL in `~/SyncthingDB/Book-Bot`, states
`Wrapped`/`Wishlist`) can be replayed through the live API with full
metadata enrichment:

```sh
uv run python scripts/import_scans.py --file ~/SyncthingDB/Book-Bot/HoneyCrisp.jsonl \
    --username beca --password '...' --dry-run
```

## repo layout

```
app/            FastAPI backend + static frontend (app/static)
deploy/         one-time SQL + notes for the shared PostgREST stack
scripts/        create_user.py, import_scans.py
archive/        the previous generation of Book-Bot scripts (Open Library
                dump loaders, ebook file renamer, ad-hoc queries) — kept
                for reference, not used by the website
```
