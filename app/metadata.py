"""ISBN/barcode normalization and book metadata lookup.

Metadata comes from two sources, merged:
  - Google Books: rich title/author/description/cover data.
  - Open Library: the *work* key, which groups every edition of a book
    (hardcover, paperback, special edition, ...) under one identity.

When Open Library has no work key we fall back to a normalized
title+author key (norm_key) so editions still cluster together.
"""

import re

import requests

from . import config

GOOGLE_VOLUMES_URL = "https://www.googleapis.com/books/v1/volumes"
OPENLIBRARY_ISBN_URL = "https://openlibrary.org/isbn/{isbn}.json"
OPENLIBRARY_SEARCH_URL = "https://openlibrary.org/search.json"
OPENLIBRARY_BASE = "https://openlibrary.org"

FORMATS = ["hardcover", "paperback", "mass market", "special edition", "ebook", "audiobook", "other"]


# --------------------------------------------------------------------------
# barcode / ISBN handling
# --------------------------------------------------------------------------

def _digits(code: str) -> str:
    return re.sub(r"[^0-9Xx]", "", code or "").upper()


def _isbn10_valid(code: str) -> bool:
    if not re.fullmatch(r"\d{9}[\dX]", code):
        return False
    total = sum((10 - i) * (10 if c == "X" else int(c)) for i, c in enumerate(code))
    return total % 11 == 0


def _ean13_check_digit(first12: str) -> str:
    total = sum(int(c) * (1 if i % 2 == 0 else 3) for i, c in enumerate(first12))
    return str((10 - total % 10) % 10)


def _isbn13_valid(code: str) -> bool:
    return bool(re.fullmatch(r"\d{13}", code)) and _ean13_check_digit(code[:12]) == code[12]


def isbn10_to_isbn13(isbn10: str) -> str:
    core = "978" + isbn10[:9]
    return core + _ean13_check_digit(core)


def isbn13_to_isbn10(isbn13: str) -> str | None:
    if not isbn13.startswith("978"):
        return None
    core = isbn13[3:12]
    total = sum((10 - i) * int(c) for i, c in enumerate(core))
    check = (11 - total % 11) % 11
    return core + ("X" if check == 10 else str(check))


def normalize_code(raw: str) -> dict:
    """Turn a scanned barcode or typed code into an ISBN-13, or explain why not.

    Book barcodes are "Bookland" EAN-13 (prefix 978/979) — the ISBN-13
    itself. Older US mass-market paperbacks carry a 12-digit UPC-A that
    does NOT encode the ISBN; those can only be found by title search.
    """
    code = _digits(raw)
    if len(code) == 13 and code.startswith(("978", "979")):
        if _isbn13_valid(code):
            return {"ok": True, "isbn13": code}
        return {"ok": False, "reason": "that looks like an isbn-13 but the check digit is wrong — re-scan or re-type it"}
    if len(code) == 10 and _isbn10_valid(code):
        return {"ok": True, "isbn13": isbn10_to_isbn13(code)}
    if len(code) == 12:
        return {"ok": False, "reason": "that's a upc barcode that doesn't encode an isbn (common on older mass-market paperbacks) — search by title/author instead, or scan the isbn barcode inside the cover"}
    if len(code) == 13:
        return {"ok": False, "reason": "that barcode isn't a book isbn (books start 978 or 979) — try the barcode on the back cover or inside flap"}
    return {"ok": False, "reason": "couldn't read an isbn from that — expected 10 or 13 digits"}


# --------------------------------------------------------------------------
# work grouping key
# --------------------------------------------------------------------------

_ARTICLES = {"the", "a", "an"}


def norm_key(title: str, authors: list[str] | None) -> str:
    t = (title or "").lower().split(":")[0]
    t = re.sub(r"[^a-z0-9 ]", " ", t)
    words = [w for w in t.split() if w not in _ARTICLES]
    a = (authors[0] if authors else "").lower()
    a = re.sub(r"[^a-z0-9 ]", " ", a)
    return " ".join(words) + "|" + " ".join(a.split())


# --------------------------------------------------------------------------
# external lookups
# --------------------------------------------------------------------------

def _google_volume_to_meta(item: dict) -> dict:
    info = item.get("volumeInfo", {})
    isbn13 = isbn10 = None
    for ident in info.get("industryIdentifiers", []):
        if ident.get("type") == "ISBN_13":
            isbn13 = ident.get("identifier")
        elif ident.get("type") == "ISBN_10":
            isbn10 = ident.get("identifier")
    if not isbn13 and isbn10 and _isbn10_valid(_digits(isbn10)):
        isbn13 = isbn10_to_isbn13(_digits(isbn10))
    cover = (info.get("imageLinks") or {}).get("thumbnail")
    if cover:
        cover = cover.replace("http://", "https://")
    elif isbn13:
        cover = f"https://covers.openlibrary.org/b/isbn/{isbn13}-M.jpg"
    authors = info.get("authors") or []
    return {
        "isbn13": isbn13,
        "isbn10": isbn10,
        "title": info.get("title") or "",
        "subtitle": info.get("subtitle"),
        "authors": authors,
        "publisher": info.get("publisher"),
        "published_date": info.get("publishedDate"),
        "description": info.get("description"),
        "page_count": info.get("pageCount"),
        "language": info.get("language"),
        "google_volume_id": item.get("id"),
        "cover_url": cover,
        "ol_edition_key": None,
        "ol_work_key": None,
        "format": None,
        "norm_key": norm_key(info.get("title") or "", authors),
    }


_google_creds = None


def _google_auth_headers() -> dict:
    """Bearer token from the service account (Books API enabled on its
    project). Anonymous calls hit a zero per-IP quota, so this is what
    keeps Google Books usable; failures degrade to anonymous."""
    global _google_creds
    if not config.GOOGLE_SERVICE_ACCOUNT_INFO:
        return {}
    try:
        if _google_creds is None:
            from google.oauth2 import service_account
            _google_creds = service_account.Credentials.from_service_account_info(
                config.GOOGLE_SERVICE_ACCOUNT_INFO,
                scopes=["https://www.googleapis.com/auth/books"],
            )
        if not _google_creds.valid:
            from google.auth.transport.requests import Request
            _google_creds.refresh(Request())
        return {"Authorization": f"Bearer {_google_creds.token}"}
    except Exception:
        return {}


def _google_get(params: dict) -> list[dict]:
    if config.GOOGLE_BOOKS_API_KEY:
        params = {**params, "key": config.GOOGLE_BOOKS_API_KEY}
    try:
        resp = requests.get(GOOGLE_VOLUMES_URL, params=params,
                            headers=_google_auth_headers(), timeout=config.HTTP_TIMEOUT)
        resp.raise_for_status()
        return resp.json().get("items") or []
    except requests.RequestException:
        return []


def guess_format(physical_format: str | None) -> str | None:
    pf = (physical_format or "").lower()
    if not pf:
        return None
    if "mass market" in pf:
        return "mass market"
    if "hard" in pf or "board" in pf:
        return "hardcover"
    if "paper" in pf or "soft" in pf:
        return "paperback"
    if "ebook" in pf or "electronic" in pf or "kindle" in pf:
        return "ebook"
    if "audio" in pf or "cd" in pf or "mp3" in pf:
        return "audiobook"
    return "other"


def fetch_openlibrary(isbn13: str) -> dict:
    """Edition record from Open Library: work key + physical format."""
    try:
        resp = requests.get(OPENLIBRARY_ISBN_URL.format(isbn=isbn13), timeout=config.HTTP_TIMEOUT)
        if resp.status_code != 200:
            return {}
        data = resp.json()
    except (requests.RequestException, ValueError):
        return {}
    works = data.get("works") or []
    return {
        "ol_edition_key": (data.get("key") or "").replace("/books/", "") or None,
        "ol_work_key": (works[0].get("key", "").replace("/works/", "") if works else None) or None,
        "format": guess_format(data.get("physical_format")),
        "ol_title": data.get("title"),
        "publisher": (data.get("publishers") or [None])[0],
        "published_date": data.get("publish_date"),
    }


def _ol_get(url: str, params: dict | None = None) -> dict:
    try:
        resp = requests.get(url, params=params, timeout=config.HTTP_TIMEOUT)
        if resp.status_code != 200:
            return {}
        return resp.json()
    except (requests.RequestException, ValueError):
        return {}


def fetch_ol_work_details(ol_work_key: str) -> dict:
    """Author names + description from an Open Library work record.
    Used when Google Books is unavailable (rate limits are per-IP for
    anonymous callers, so this happens in the wild)."""
    work = _ol_get(f"{OPENLIBRARY_BASE}/works/{ol_work_key}.json")
    if not work:
        return {}
    desc = work.get("description")
    if isinstance(desc, dict):
        desc = desc.get("value")
    authors = []
    for entry in (work.get("authors") or [])[:3]:
        key = ((entry.get("author") or {}).get("key") or "").replace("/authors/", "")
        if key:
            author = _ol_get(f"{OPENLIBRARY_BASE}/authors/{key}.json")
            if author.get("name"):
                authors.append(author["name"])
    return {"authors": authors, "description": desc}


def lookup_isbn(isbn13: str) -> dict | None:
    """Merged Google Books + Open Library metadata for one ISBN-13."""
    items = _google_get({"q": f"isbn:{isbn13}", "maxResults": 5})
    meta = None
    for item in items:
        candidate = _google_volume_to_meta(item)
        if candidate["isbn13"] == isbn13:
            meta = candidate
            break
    if meta is None and items:
        meta = _google_volume_to_meta(items[0])
        meta["isbn13"] = isbn13

    ol = fetch_openlibrary(isbn13)
    if meta is None:
        if not ol.get("ol_title"):
            return None
        meta = {
            "isbn13": isbn13,
            "isbn10": isbn13_to_isbn10(isbn13),
            "title": ol["ol_title"],
            "subtitle": None,
            "authors": [],
            "publisher": ol.get("publisher"),
            "published_date": ol.get("published_date"),
            "description": None,
            "page_count": None,
            "language": None,
            "google_volume_id": None,
            "cover_url": f"https://covers.openlibrary.org/b/isbn/{isbn13}-M.jpg",
            "ol_edition_key": None,
            "ol_work_key": None,
            "format": None,
            "norm_key": norm_key(ol["ol_title"], []),
        }
    if not meta.get("isbn10"):
        meta["isbn10"] = isbn13_to_isbn10(isbn13)
    meta["ol_edition_key"] = ol.get("ol_edition_key")
    meta["ol_work_key"] = ol.get("ol_work_key")
    meta["format"] = ol.get("format")
    # Google gave nothing usable — pull authors/description off the OL work
    if (not meta["authors"] or not meta.get("description")) and ol.get("ol_work_key"):
        details = fetch_ol_work_details(ol["ol_work_key"])
        if not meta["authors"] and details.get("authors"):
            meta["authors"] = details["authors"]
            meta["norm_key"] = norm_key(meta["title"], meta["authors"])
        if not meta.get("description") and details.get("description"):
            meta["description"] = details["description"]
    return meta


def search_openlibrary(query: str, limit: int = 12) -> list[dict]:
    """Work-level search on Open Library — the fallback when Google Books
    is rate-limited. Results have no specific ISBN (they represent the
    story, not one edition), which is fine: work grouping still applies."""
    data = _ol_get(OPENLIBRARY_SEARCH_URL, {
        "q": query,
        "limit": limit,
        "fields": "key,title,author_name,first_publish_year,cover_i,publisher",
    })
    results = []
    for doc in data.get("docs") or []:
        title = doc.get("title")
        if not title:
            continue
        authors = doc.get("author_name") or []
        cover_i = doc.get("cover_i")
        results.append({
            "isbn13": None,
            "isbn10": None,
            "title": title,
            "subtitle": None,
            "authors": authors,
            "publisher": (doc.get("publisher") or [None])[0],
            "published_date": str(doc.get("first_publish_year") or "") or None,
            "description": None,
            "page_count": None,
            "language": None,
            "google_volume_id": None,
            "cover_url": f"https://covers.openlibrary.org/b/id/{cover_i}-M.jpg" if cover_i else None,
            "ol_edition_key": None,
            "ol_work_key": (doc.get("key") or "").replace("/works/", "") or None,
            "format": None,
            "norm_key": norm_key(title, authors),
        })
    return results


def search_external(query: str, limit: int = 12) -> list[dict]:
    """Title/author search via Google Books, normalized to our metadata shape."""
    items = _google_get({"q": query, "maxResults": min(limit, 40), "printType": "books"})
    if not items:
        return search_openlibrary(query, limit)
    results, seen = [], set()
    for item in items:
        meta = _google_volume_to_meta(item)
        if not meta["title"]:
            continue
        key = meta["isbn13"] or meta["google_volume_id"]
        if key in seen:
            continue
        seen.add(key)
        results.append(meta)
    return results[:limit]
