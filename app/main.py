"""book-bot — personal library terminal.

FastAPI backend: serves the mobile-first frontend from app/static and a
small JSON API. All book data flows through the store layer (PostgREST in
production, SQLite in dev); metadata comes from Google Books/Open Library.
"""

import os

from fastapi import Depends, FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from . import config, metadata
from .auth import login, require_token
from .store import StoreError, get_store, new_id, now_iso

app = FastAPI(title="book-bot", docs_url=None, redoc_url=None)


class LoginBody(BaseModel):
    username: str
    password: str


class AddBookBody(BaseModel):
    status: str  # 'library' | 'wishlist'
    metadata: dict
    format: str | None = None
    notes: str | None = None


class UpdateBookBody(BaseModel):
    status: str | None = None
    notes: str | None = None
    format: str | None = None
    copies: int | None = None


@app.exception_handler(StoreError)
async def store_error_handler(request, exc: StoreError):
    from fastapi.responses import JSONResponse
    return JSONResponse(status_code=exc.status_code, content={"detail": str(exc)})


@app.get("/api/health")
def health():
    return {"status": "ok", "mode": config.MODE}


@app.post("/api/login")
def api_login(body: LoginBody):
    return {"token": login(body.username.strip(), body.password)}


# --------------------------------------------------------------------------
# work resolution + ownership
# --------------------------------------------------------------------------

def _resolve_work(token: str, meta: dict) -> dict:
    """Find the work this edition belongs to, or create it.

    Match order: Open Library work key (authoritative), then normalized
    title+author (catches editions OL doesn't link). Backfills the OL key
    onto a norm-key-matched work so future scans match directly.
    """
    store = get_store()
    nkey = meta.get("norm_key") or metadata.norm_key(meta.get("title", ""), meta.get("authors"))
    work = store.find_work(token, meta.get("ol_work_key"), nkey)
    if work:
        if meta.get("ol_work_key") and not work.get("ol_work_key"):
            store.update_work(token, work["id"], {"ol_work_key": meta["ol_work_key"]})
            work["ol_work_key"] = meta["ol_work_key"]
        return work
    return store.create_work(token, {
        "id": new_id(),
        "ol_work_key": meta.get("ol_work_key"),
        "norm_key": nkey,
        "title": (meta.get("title") or "").split(":")[0].strip() or "unknown",
        "authors": ", ".join(meta.get("authors") or []),
        "created_at": now_iso(),
    })


def _edition_public(row: dict) -> dict:
    return row


def _ownership(token: str, meta: dict, exclude_edition_id=None) -> dict:
    """Everything the shopper needs to know: is this exact ISBN in the
    library/wishlist, and which *other* editions of the same work exist."""
    store = get_store()
    exact = store.get_edition_by_isbn(token, meta["isbn13"]) if meta.get("isbn13") else None
    nkey = meta.get("norm_key") or metadata.norm_key(meta.get("title", ""), meta.get("authors"))
    work = store.find_work(token, meta.get("ol_work_key"), nkey)
    related = []
    if work:
        for ed in store.editions_for_work(token, work["id"]):
            if exact and ed["id"] == exact["id"]:
                continue
            if exclude_edition_id and ed["id"] == exclude_edition_id:
                continue
            related.append(ed)
    return {
        "exact": exact,
        "related": related,
        "work": work,
    }


# --------------------------------------------------------------------------
# lookup / search
# --------------------------------------------------------------------------

@app.get("/api/lookup")
def api_lookup(code: str, token: str = Depends(require_token)):
    norm = metadata.normalize_code(code)
    if not norm["ok"]:
        return {"ok": False, "reason": norm["reason"]}
    isbn13 = norm["isbn13"]
    store = get_store()

    existing = store.get_edition_by_isbn(token, isbn13)
    if existing:
        meta = {k: existing.get(k) for k in (
            "isbn13", "isbn10", "title", "subtitle", "publisher", "published_date",
            "description", "format", "cover_url", "google_volume_id",
            "ol_edition_key", "page_count", "language")}
        meta["authors"] = [a.strip() for a in (existing.get("authors") or "").split(",") if a.strip()]
        meta["ol_work_key"] = None
        meta["norm_key"] = None
        related = [e for e in store.editions_for_work(token, existing["work_id"]) if e["id"] != existing["id"]]
        return {"ok": True, "isbn13": isbn13, "found": True, "metadata": meta,
                "ownership": {"exact": existing, "related": related, "work": None}}

    meta = metadata.lookup_isbn(isbn13)
    if meta is None:
        return {"ok": True, "isbn13": isbn13, "found": False, "metadata": None,
                "ownership": {"exact": None, "related": [], "work": None}}
    return {"ok": True, "isbn13": isbn13, "found": True, "metadata": meta,
            "ownership": _ownership(token, meta)}


@app.get("/api/search")
def api_search(q: str, token: str = Depends(require_token)):
    q = q.strip()
    if not q:
        return {"local": [], "external": []}
    store = get_store()

    # An ISBN typed into the search box gets a direct lookup — free-text
    # search misses ISBNs for editions the catalogs haven't cross-indexed.
    norm = metadata.normalize_code(q)
    if norm["ok"]:
        q = norm["isbn13"]
    local = store.list_editions(token, q=q)

    external = []
    if norm["ok"]:
        meta = metadata.lookup_isbn(norm["isbn13"])
        if meta:
            external = [meta]
    if not external:
        external = metadata.search_external(q)
    # annotate external results with what's already on the shelves
    nkeys = [m["norm_key"] for m in external if m.get("norm_key")]
    olkeys = [m["ol_work_key"] for m in external if m.get("ol_work_key")]
    works = store.works_by_norm_keys(token, nkeys, olkeys)
    works_by_nkey = {w["norm_key"]: w for w in works}
    works_by_ol = {w["ol_work_key"]: w for w in works if w.get("ol_work_key")}
    eds = store.editions_for_works(token, [w["id"] for w in works])
    eds_by_work = {}
    for ed in eds:
        eds_by_work.setdefault(ed["work_id"], []).append(ed)
    local_isbns = {ed["isbn13"] for ed in eds if ed.get("isbn13")}
    for m in external:
        work = works_by_ol.get(m.get("ol_work_key")) or works_by_nkey.get(m.get("norm_key"))
        owned = eds_by_work.get(work["id"], []) if work else []
        m["owned_exact"] = bool(m.get("isbn13") and m["isbn13"] in local_isbns)
        m["owned_editions"] = [
            {"id": e["id"], "format": e.get("format"), "status": e["status"], "isbn13": e.get("isbn13")}
            for e in owned
        ]
    return {"local": local, "external": external}


# --------------------------------------------------------------------------
# books CRUD
# --------------------------------------------------------------------------

@app.post("/api/books")
def api_add_book(body: AddBookBody, token: str = Depends(require_token)):
    if body.status not in ("library", "wishlist"):
        raise HTTPException(400, "status must be 'library' or 'wishlist'")
    meta = body.metadata or {}
    if not (meta.get("title") or "").strip():
        raise HTTPException(400, "a title is required")
    store = get_store()

    isbn13 = meta.get("isbn13")
    if isbn13:
        existing = store.get_edition_by_isbn(token, isbn13)
        if existing:
            fields = {"status": body.status, "status_changed_at": now_iso()}
            if body.notes is not None:
                fields["notes"] = body.notes
            updated = store.update_edition(token, existing["id"], fields)
            return {"edition": updated, "existed": True}

    work = _resolve_work(token, meta)
    edition = {
        "id": new_id(),
        "work_id": work["id"],
        "isbn13": isbn13,
        "isbn10": meta.get("isbn10"),
        "title": meta.get("title"),
        "subtitle": meta.get("subtitle"),
        "authors": ", ".join(meta.get("authors") or []),
        "publisher": meta.get("publisher"),
        "published_date": meta.get("published_date"),
        "description": meta.get("description"),
        "format": body.format or meta.get("format"),
        "cover_url": meta.get("cover_url"),
        "google_volume_id": meta.get("google_volume_id"),
        "ol_edition_key": meta.get("ol_edition_key"),
        "page_count": meta.get("page_count"),
        "language": meta.get("language"),
        "status": body.status,
        "notes": body.notes,
        "copies": 1,
        "added_at": now_iso(),
        "status_changed_at": now_iso(),
    }
    created = store.insert_edition(token, edition)
    return {"edition": created, "existed": False}


@app.get("/api/books")
def api_list_books(status: str | None = None, q: str | None = None, token: str = Depends(require_token)):
    if status not in (None, "library", "wishlist"):
        raise HTTPException(400, "status must be 'library' or 'wishlist'")
    return {"items": get_store().list_editions(token, status=status, q=q)}


@app.get("/api/books/{edition_id}")
def api_get_book(edition_id: str, token: str = Depends(require_token)):
    store = get_store()
    edition = store.get_edition(token, edition_id)
    if not edition:
        raise HTTPException(404, "book not found")
    related = [e for e in store.editions_for_work(token, edition["work_id"]) if e["id"] != edition_id]
    return {"edition": edition, "related": related}


@app.patch("/api/books/{edition_id}")
def api_update_book(edition_id: str, body: UpdateBookBody, token: str = Depends(require_token)):
    fields = {}
    if body.status is not None:
        if body.status not in ("library", "wishlist"):
            raise HTTPException(400, "status must be 'library' or 'wishlist'")
        fields["status"] = body.status
        fields["status_changed_at"] = now_iso()
    if body.notes is not None:
        fields["notes"] = body.notes
    if body.format is not None:
        fields["format"] = body.format
    if body.copies is not None:
        fields["copies"] = max(1, body.copies)
    if not fields:
        raise HTTPException(400, "nothing to update")
    updated = get_store().update_edition(token, edition_id, fields)
    if not updated:
        raise HTTPException(404, "book not found")
    return {"edition": updated}


@app.delete("/api/books/{edition_id}")
def api_delete_book(edition_id: str, token: str = Depends(require_token)):
    store = get_store()
    if not store.get_edition(token, edition_id):
        raise HTTPException(404, "book not found")
    store.delete_edition(token, edition_id)
    return {"deleted": True}


@app.get("/api/stats")
def api_stats(token: str = Depends(require_token)):
    return get_store().stats(token)


@app.get("/api/formats")
def api_formats():
    return {"formats": metadata.FORMATS}


static_dir = os.path.join(os.path.dirname(__file__), "static")
app.mount("/", StaticFiles(directory=static_dir, html=True), name="static")
