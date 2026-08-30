"""Seed the library with real books pulled from Open Library subject search.

Fetches popular works by subject (with real ISBNs, covers, publishers) and
inserts them through the app's own add-book path, so work grouping and
dedupe-by-ISBN behave exactly as if each book were added in the UI. It
seeds whatever POSTGREST_URL points at, so check that before running.

Auth: mints a JWT with the shared secret (same claims the auth service
issues), so no password is needed — but books live in a user's library,
so pass --username to say whose library gets seeded (their first library,
auto-created if they have none).

    uv run python scripts/seed_books.py --username beca            # 300 books
    uv run python scripts/seed_books.py --username beca --count 50 --dry-run
"""

import argparse
import os
import random
import sys
import time
from datetime import datetime, timedelta, timezone

import jwt
import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import config, metadata  # noqa: E402
from app.auth import AuthContext  # noqa: E402
from app.main import AddBookBody, _ensure_libraries, api_add_book  # noqa: E402
from app.store import get_store  # noqa: E402

SEARCH_URL = "https://openlibrary.org/search.json"
SEARCH_FIELDS = (
    "key,title,subtitle,author_name,first_publish_year,cover_i,"
    "publisher,isbn,number_of_pages_median,language,subject"
)

# genre -> Open Library subject queries, tried in order until the genre's
# quota is filled. Keys are canonical shelf genres (metadata.GENRES) so
# seeded books land on the right shelves. Fiction needs several queries:
# the dominant-genre filter below rejects most of what subject:fiction
# returns (top-rated "fiction" on OL is overwhelmingly fantasy/sci-fi).
SUBJECTS = {
    "science fiction": ["subject_key:science_fiction"],
    "fiction": [
        "subject_key:fiction",
        'subject:"literary fiction"',
        'subject:"historical fiction"',
        'subject:"domestic fiction"',
        "subject:classics",
        'subject:"american fiction"',
    ],
}

# OL subject tagging is noisy (one stray "science fiction" tag on a fantasy
# novel puts it in sci-fi results), so accept a book only when the target
# genre wins a vote across ALL its subject tags. The fiction bucket excludes
# subgenres with their own canonical shelf (fantasy, mystery, ...) — top-rated
# subject:fiction is otherwise wall-to-wall Sanderson.
FICTION_FAMILY = {
    "fiction", "literary fiction", "classics", "historical fiction",
}


def dominant_genre(subjects: list[str]) -> str | None:
    counts: dict[str, int] = {}
    for raw in subjects or []:
        lowered = str(raw).lower()
        for pattern, genre in metadata._GENRE_PATTERNS:
            if pattern.search(lowered):
                counts[genre] = counts.get(genre, 0) + 1
                break  # first matching rule claims the tag, same as the app
    if not counts:
        return metadata.normalize_genre(subjects)
    return max(counts, key=counts.get)


def genre_accepts(genre: str, doc: dict) -> bool:
    dom = dominant_genre(doc.get("subject") or [])
    if genre == "science fiction":
        return dom == "science fiction"
    return dom in FICTION_FAMILY or dom is None


def mint_token(user_id: str) -> str:
    return jwt.encode(
        {
            "role": f"{config.APP_SCHEMA}_user",
            "user_id": user_id,
            "exp": datetime.now(timezone.utc) + timedelta(hours=2),
        },
        config.JWT_SECRET,
        algorithm="HS256",
    )


def resolve_auth(username: str) -> AuthContext:
    """Books belong to a library, libraries to users — resolve whose
    shelves we're filling and mint their token."""
    store = get_store()
    # the user directory is readable by any authenticated token
    bootstrap = mint_token("00000000-0000-0000-0000-000000000000")
    user = store.find_user_by_username(bootstrap, username)
    if not user:
        sys.exit(f"no user named {username!r} — create one with scripts/create_user.py first")
    user_id = str(user["id"])
    return AuthContext(token=mint_token(user_id), user_id=user_id)


def pick_isbn13(candidates: list[str]) -> str | None:
    """Best ISBN-13 from a work's edition ISBNs (the OL list is unordered
    across every translation, so prefer English-language registration
    groups: 978-0, 978-1, 979-8)."""
    valid13 = []
    for raw in candidates or []:
        code = "".join(c for c in raw if c.isdigit())
        if len(code) == 13 and metadata._isbn13_valid(code):
            valid13.append(code)
    for prefix in ("9780", "9781", "9798"):
        for code in valid13:
            if code.startswith(prefix):
                return code
    if valid13:
        return valid13[0]
    for raw in candidates or []:
        code = "".join(c for c in raw.upper() if c.isdigit() or c == "X")
        if len(code) == 10 and metadata._isbn10_valid(code):
            return metadata.isbn10_to_isbn13(code)
    return None


def doc_to_meta(doc: dict, genre: str) -> dict | None:
    title = doc.get("title")
    isbn13 = pick_isbn13(doc.get("isbn"))
    cover_i = doc.get("cover_i")
    if not (title and isbn13 and cover_i):
        return None
    if "/" in title:  # omnibus junk entries like "Works (A / B / C)"
        return None
    if any(ord(c) > 0x2FF for c in title):  # non-Latin canonical title
        return None
    if not genre_accepts(genre, doc):
        return None
    authors = [a for a in doc.get("author_name") or [] if "sparknotes" not in a.lower()]
    lang = (doc.get("language") or [None])[0]
    return {
        "isbn13": isbn13,
        "isbn10": metadata.isbn13_to_isbn10(isbn13),
        "title": title,
        "subtitle": doc.get("subtitle"),
        "authors": authors,
        "publisher": (doc.get("publisher") or [None])[0],
        "published_date": str(doc.get("first_publish_year") or "") or None,
        "description": None,
        "page_count": doc.get("number_of_pages_median"),
        "language": "en" if lang == "eng" else lang,
        "google_volume_id": None,
        "cover_url": f"https://covers.openlibrary.org/b/id/{cover_i}-M.jpg",
        "ol_edition_key": None,
        "ol_work_key": (doc.get("key") or "").replace("/works/", "") or None,
        "format": None,
        "genre": genre,
        "norm_key": metadata.norm_key(title, authors),
    }


def fetch_genre(genre: str, queries: list[str], want: int, seen: set) -> list[dict]:
    """Popular works for one genre, deduped against already-picked books."""
    out: list[dict] = []
    for query in queries:
        out += fetch_subject(genre, query, want - len(out), seen)
        if len(out) >= want:
            break
    return out


def fetch_subject(genre: str, query: str, want: int, seen: set) -> list[dict]:
    out, page = [], 1
    while len(out) < want and page <= 8:
        resp = requests.get(
            SEARCH_URL,
            params={
                "q": query,
                "fields": SEARCH_FIELDS,
                "sort": "rating",  # best-rated on OL ≈ recognizable books
                "limit": 100,
                "page": page,
            },
            timeout=30,
        )
        resp.raise_for_status()
        docs = resp.json().get("docs") or []
        if not docs:
            break
        for doc in docs:
            meta = doc_to_meta(doc, genre)
            if not meta:
                continue
            key = (meta["ol_work_key"] or meta["norm_key"], meta["isbn13"])
            if meta["isbn13"] in seen or key[0] in seen:
                continue
            seen.add(meta["isbn13"])
            seen.add(key[0])
            out.append(meta)
            if len(out) >= want:
                break
        page += 1
        time.sleep(1)  # be polite to open library
    return out


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--username", required=True, help="whose library to seed")
    parser.add_argument("--count", type=int, default=300)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    auth = resolve_auth(args.username)
    print(f"target={config.POSTGREST_URL}")

    # preflight: fail before fetching anything if the token/store is broken.
    # Also provisions the user's library if they don't have one yet.
    library = _ensure_libraries(auth, username=args.username)[0]
    print(f"store preflight ok — seeding into {library['name']!r}")

    per_genre = args.count // len(SUBJECTS)
    seen: set = set()
    books: list[dict] = []
    for genre, queries in SUBJECTS.items():
        got = fetch_genre(genre, queries, per_genre, seen)
        print(f"{genre}: {len(got)} books collected")
        books += got

    # deterministic fake physical formats so the shelves look like a real
    # mixed library rather than 300 identical spines
    rng = random.Random(42)
    formats = ["hardcover"] * 35 + ["paperback"] * 45 + ["mass market"] * 20

    added = existed = failed = 0
    for i, meta in enumerate(books, 1):
        if args.dry_run:
            print(f"would add [{meta['genre']}] {meta['title']} — {', '.join(meta['authors'])} ({meta['isbn13']})")
            continue
        try:
            body = AddBookBody(status="library", metadata=meta, format=rng.choice(formats))
            result = api_add_book(body, auth)
            existed += 1 if result["existed"] else 0
            added += 0 if result["existed"] else 1
        except Exception as exc:
            failed += 1
            print(f"  !! {meta['title']} ({meta['isbn13']}): {exc}")
        if i % 25 == 0:
            print(f"  {i}/{len(books)} ...")
        time.sleep(0.05)

    print(f"done: {added} added, {existed} already present, {failed} failed")


if __name__ == "__main__":
    main()
