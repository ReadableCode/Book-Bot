"""Build app/sample_books.json — the sample library new accounts start with.

Reuses seed_books.py's Open Library fetching (popular, well-rated books with
real ISBNs and covers) but writes a static metadata manifest instead of
touching any database. scripts/seed_sample_library.py loads this file into
the shared, view-only Sample Library that every account can browse.

    uv run python scripts/build_sample_library.py             # 300 books
    uv run python scripts/build_sample_library.py --count 50
"""

import argparse
import json
import os
import random
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.seed_books import SUBJECTS, fetch_genre  # noqa: E402

DEFAULT_OUT = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "app", "sample_books.json")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=300)
    parser.add_argument("--out", default=DEFAULT_OUT)
    args = parser.parse_args()

    per_genre = args.count // len(SUBJECTS)
    seen: set = set()
    books: list[dict] = []
    for genre, queries in SUBJECTS.items():
        got = fetch_genre(genre, queries, per_genre, seen)
        print(f"{genre}: {len(got)} books collected")
        books += got

    # deterministic fake physical formats, same mix seed_books.py uses,
    # so the sample shelves look like a real library
    rng = random.Random(42)
    formats = ["hardcover"] * 35 + ["paperback"] * 45 + ["mass market"] * 20
    for meta in books:
        meta["format"] = rng.choice(formats)

    with open(args.out, "w") as f:
        json.dump(books, f, indent=1)
    print(f"wrote {len(books)} books to {args.out}")


if __name__ == "__main__":
    main()
