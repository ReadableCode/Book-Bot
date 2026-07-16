"""book-bot — personal library terminal.

FastAPI backend: serves the mobile-first frontend from app/static and a
small JSON API. All book data flows through the store layer (PostgREST in
production, SQLite in dev); metadata comes from Google Books/Open Library.

Shape of the world: works and editions are a shared catalog; ownership
lives in library_books inside a library that one or more users own
together; each user additionally has private per-work read states
(status / rating / notes / read dates), independent of ownership.
"""

import os
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from . import accounts, bootstrap, config, metadata, security
from .auth import AuthContext, decode_token, login, require_auth
from .security import LoginRateLimiter, client_ip
from .store import HOLDING_STATUSES, READ_STATUSES, StoreError, get_store, new_id, now_iso


@asynccontextmanager
async def _lifespan(app):
    # converge the database on every startup, however the app is run —
    # local uvicorn or container alike (schema + sample library; both
    # idempotent and best-effort, see app/bootstrap.py)
    bootstrap.run()
    yield


app = FastAPI(title="book-bot", docs_url=None, redoc_url=None, lifespan=_lifespan)

# app-level brute-force protection now that Authelia no longer fronts the
# app (same policy Sync_Plex uses: 5 failures / 15 min locks the key)
login_limiter = LoginRateLimiter()
signup_limiter = LoginRateLimiter()


@app.middleware("http")
async def security_headers(request, call_next):
    """Edge hardening that used to come with the Authelia/SWAG include.
    TLS/HSTS stay at the proxy; these are the app's own responsibility.
    CSP: everything is served from here (vendored JS, local CSS), covers
    load from the book catalogs' CDNs, and the PWA needs workers."""
    response = await call_next(request)
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("X-Frame-Options", "DENY")
    response.headers.setdefault("Referrer-Policy", "same-origin")
    response.headers.setdefault(
        "Content-Security-Policy",
        "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; "
        "img-src 'self' https: data: blob:; media-src 'self' blob:; "
        "connect-src 'self'; worker-src 'self' blob:; "
        "frame-ancestors 'none'; base-uri 'self'; form-action 'self'",
    )
    return response


@app.middleware("http")
async def no_stale_assets(request, call_next):
    """Static responses must always revalidate. StaticFiles sends no
    Cache-Control, and Cloudflare edge-caches extension-matched assets
    (.js/.css/...) for hours when the origin is silent — so deploys served
    fresh HTML with stale scripts, and the edge handed auth-gated assets to
    anonymous requests. no-cache keeps ETag/304 revalidation cheap."""
    response = await call_next(request)
    if not request.url.path.startswith("/api/"):
        response.headers["Cache-Control"] = "no-cache"
    return response


class LoginBody(BaseModel):
    username: str
    password: str


class AddBookBody(BaseModel):
    status: str  # 'library' (physical) | 'digital' | 'wishlist'
    metadata: dict
    format: str | None = None
    notes: str | None = None
    library_id: str | None = None  # defaults to the user's first library


class UpdateBookBody(BaseModel):
    status: str | None = None
    notes: str | None = None
    format: str | None = None
    copies: int | None = None
    genre: str | None = None


class EnrichGenresBody(BaseModel):
    limit: int = 12


class LibraryBody(BaseModel):
    name: str


class MemberBody(BaseModel):
    username: str


class ReadBody(BaseModel):
    work_id: str | None = None
    metadata: dict | None = None  # used to resolve/create the work when no work_id
    edition_id: str | None = None
    status: str  # 'want_to_read' | 'reading' | 'read'
    rating: int | None = None
    notes: str | None = None
    started_at: str | None = None
    finished_at: str | None = None


@app.exception_handler(StoreError)
async def store_error_handler(request, exc: StoreError):
    from fastapi.responses import JSONResponse
    return JSONResponse(status_code=exc.status_code, content={"detail": str(exc)})


@app.get("/api/health")
def health():
    return {"status": "ok", "mode": config.MODE}


# --------------------------------------------------------------------------
# login + libraries
# --------------------------------------------------------------------------

def _ensure_libraries(auth: AuthContext, username: str | None = None) -> list[dict]:
    """A user with no library gets a personal one on the spot. Existing
    users keep whatever they're a member of (e.g. the migrated shared
    Family Library) — brand-new users never see anyone else's shelves."""
    store = get_store()
    libraries = store.libraries_for_user(auth.token, auth.user_id)
    if libraries:
        return libraries
    if not username:
        username = store.usernames_for_ids(auth.token, [auth.user_id]).get(str(auth.user_id))
    name = f"{username}'s library" if username else "My library"
    library = {"id": new_id(), "name": name, "created_at": now_iso()}
    store.create_library(auth.token, library)
    store.add_member(auth.token, {
        "library_id": library["id"], "user_id": auth.user_id,
        "role": "owner", "added_at": now_iso(),
    })
    return [{**library, "role": "owner"}]


def _library_ids(auth: AuthContext) -> list[str]:
    return [lib["id"] for lib in _ensure_libraries(auth)]


def _target_library(auth: AuthContext, library_id: str | None) -> str:
    libraries = _ensure_libraries(auth)
    if library_id is None:
        return libraries[0]["id"]
    if library_id not in {lib["id"] for lib in libraries}:
        raise HTTPException(404, "no such library (or you are not a member)")
    return library_id


def _check_lockout(limiter: LoginRateLimiter, *keys: str) -> None:
    remaining = limiter.locked_for(*keys)
    if remaining:
        raise HTTPException(
            429, f"too many attempts — locked for {int(remaining // 60) + 1} more minute(s)")


@app.post("/api/login")
def api_login(body: LoginBody, request: Request):
    username = body.username.strip()  # exact-match: pre-signup accounts may be mixed-case
    keys = (f"user:{username.lower()}", f"ip:{client_ip(request)}")
    _check_lockout(login_limiter, *keys)
    try:
        token = login(username, body.password)
    except HTTPException as exc:
        if exc.status_code == 401:
            login_limiter.record_failure(*keys)
        raise
    login_limiter.record_success(*keys)
    _ensure_libraries(decode_token(token), username=username)
    return {"token": token}


class SignupBody(BaseModel):
    username: str
    password: str


@app.post("/api/signup")
def api_signup(body: SignupBody, request: Request):
    """Create an account and give it an empty personal library. The
    shared Sample Library is visible to every account for browsing."""
    if not config.SIGNUP_ENABLED:
        raise HTTPException(403, "signups are disabled — ask for an invite")
    ip_key = f"ip:{client_ip(request)}"
    _check_lockout(signup_limiter, ip_key)
    try:
        username = security.validate_username(body.username)
        security.validate_password(body.password)
    except ValueError as exc:
        raise HTTPException(400, str(exc))
    # every attempt counts: signup has no "failure" a caller shouldn't
    # control, so the limiter throttles account-creation churn per IP
    signup_limiter.record_failure(ip_key)
    accounts.create_user(username, body.password)
    token = login(username, body.password)
    _ensure_libraries(decode_token(token), username=username)
    return {"token": token}


@app.get("/api/me")
def api_me(auth: AuthContext = Depends(require_auth)):
    store = get_store()
    libraries = _ensure_libraries(auth)
    members = store.members_for_libraries(auth.token, [lib["id"] for lib in libraries])
    by_library = {}
    username = None
    for m in members:
        by_library.setdefault(str(m["library_id"]), []).append(
            {"user_id": str(m["user_id"]), "username": m["username"], "role": m["role"]})
        if str(m["user_id"]) == str(auth.user_id):
            username = m["username"]
    out = [{**lib, "members": by_library.get(str(lib["id"]), [])} for lib in libraries]
    # the shared Sample Library rides along read-only for everyone (last,
    # so the user's own library stays the default selection)
    sample = store.get_library(auth.token, config.SAMPLE_LIBRARY_ID)
    if sample:
        out.append({**sample, "role": "viewer", "members": []})
    return {"user_id": auth.user_id, "username": username, "libraries": out}


@app.post("/api/libraries")
def api_create_library(body: LibraryBody, auth: AuthContext = Depends(require_auth)):
    name = body.name.strip()
    if not name:
        raise HTTPException(400, "a name is required")
    store = get_store()
    library = {"id": new_id(), "name": name, "created_at": now_iso()}
    store.create_library(auth.token, library)
    store.add_member(auth.token, {
        "library_id": library["id"], "user_id": auth.user_id,
        "role": "owner", "added_at": now_iso(),
    })
    return {"library": {**library, "role": "owner"}}


@app.patch("/api/libraries/{library_id}")
def api_rename_library(library_id: str, body: LibraryBody, auth: AuthContext = Depends(require_auth)):
    name = body.name.strip()
    if not name:
        raise HTTPException(400, "a name is required")
    _target_library(auth, library_id)
    get_store().update_library(auth.token, library_id, {"name": name})
    return {"renamed": True}


@app.post("/api/libraries/{library_id}/members")
def api_add_member(library_id: str, body: MemberBody, auth: AuthContext = Depends(require_auth)):
    """Share a library: any current member can add another user by username."""
    _target_library(auth, library_id)
    store = get_store()
    user = store.find_user_by_username(auth.token, body.username.strip())
    if not user:
        raise HTTPException(404, f"no user named {body.username.strip()!r}")
    members = store.members_for_libraries(auth.token, [library_id])
    if any(str(m["user_id"]) == str(user["id"]) for m in members):
        raise HTTPException(409, f"{user['username']} is already a member")
    store.add_member(auth.token, {
        "library_id": library_id, "user_id": str(user["id"]),
        "role": "owner", "added_at": now_iso(),
    })
    return {"added": user["username"]}


# --------------------------------------------------------------------------
# work resolution + ownership
# --------------------------------------------------------------------------

def _resolve_work(auth: AuthContext, meta: dict) -> dict:
    """Find the work this edition belongs to, or create it.

    Match order: Open Library work key (authoritative), then normalized
    title+author (catches editions OL doesn't link). Backfills the OL key
    and a cover onto matched works so future scans and the read-history
    view get richer over time.
    """
    store = get_store()
    nkey = meta.get("norm_key") or metadata.norm_key(meta.get("title", ""), meta.get("authors"))
    work = store.find_work(auth.token, meta.get("ol_work_key"), nkey)
    if work:
        backfill = {}
        if meta.get("ol_work_key") and not work.get("ol_work_key"):
            backfill["ol_work_key"] = meta["ol_work_key"]
        if meta.get("cover_url") and not work.get("cover_url"):
            backfill["cover_url"] = meta["cover_url"]
        if backfill:
            store.update_work(auth.token, work["id"], backfill)
            work.update(backfill)
        return work
    return store.create_work(auth.token, {
        "id": new_id(),
        "ol_work_key": meta.get("ol_work_key"),
        "norm_key": nkey,
        "title": (meta.get("title") or "").split(":")[0].strip() or "unknown",
        "authors": ", ".join(meta.get("authors") or []),
        "cover_url": meta.get("cover_url"),
        "created_at": now_iso(),
    })


def _flatten_book(holding: dict) -> dict:
    """Merge a library_books row with its embedded edition into the flat
    shape the frontend renders. Holding fields win the name collisions
    (id, added_at)."""
    edition = holding.get("edition") or {}
    flat = {**edition}
    for key in ("id", "library_id", "edition_id", "status", "notes", "copies",
                "added_at", "status_changed_at"):
        if key in holding:
            flat[key] = holding[key]
    return flat


def _read_state_public(state: dict | None) -> dict | None:
    if not state:
        return None
    return {k: state.get(k) for k in (
        "work_id", "edition_id", "status", "rating", "notes",
        "started_at", "finished_at", "updated_at")}


def _ownership(auth: AuthContext, meta: dict, edition=None, work=None, exclude_holding_id=None) -> dict:
    """Everything the shopper needs to know: is this exact ISBN on one of
    my shelves, which *other* editions of the same work do my libraries
    hold, and have I read it."""
    store = get_store()
    library_ids = _library_ids(auth)
    if edition is None and meta.get("isbn13"):
        edition = store.get_edition_by_isbn(auth.token, meta["isbn13"])
    if work is None:
        if edition:
            work = store.get_work(auth.token, edition["work_id"])
        else:
            nkey = meta.get("norm_key") or metadata.norm_key(meta.get("title", ""), meta.get("authors"))
            work = store.find_work(auth.token, meta.get("ol_work_key"), nkey)

    exact, related = None, []
    if work:
        editions = {e["id"]: e for e in store.editions_for_work(auth.token, work["id"])}
        holdings = store.holdings_for_editions(auth.token, library_ids, list(editions))
        for holding in holdings:
            if exclude_holding_id and holding["id"] == exclude_holding_id:
                continue
            flat = _flatten_book({**holding, "edition": editions.get(holding["edition_id"])})
            if exact is None and edition and holding["edition_id"] == edition["id"]:
                exact = flat
            else:
                related.append(flat)
    read_state = store.get_read_state(auth.token, auth.user_id, work["id"]) if work else None
    return {
        "exact": exact,
        "related": related,
        "work": work,
        "read_state": _read_state_public(read_state),
    }


def _annotate_read_status(auth: AuthContext, items: list[dict]) -> None:
    """Stamp my read status onto flattened book rows (by work)."""
    work_ids = list({item["work_id"] for item in items if item.get("work_id")})
    states = get_store().read_states_for_works(auth.token, auth.user_id, work_ids)
    by_work = {str(s["work_id"]): s for s in states}
    for item in items:
        state = by_work.get(str(item.get("work_id")))
        item["read_status"] = state["status"] if state else None
        item["read_rating"] = state["rating"] if state else None


# --------------------------------------------------------------------------
# lookup / search
# --------------------------------------------------------------------------

@app.get("/api/lookup")
def api_lookup(code: str, auth: AuthContext = Depends(require_auth)):
    norm = metadata.normalize_code(code)
    if not norm["ok"]:
        return {"ok": False, "reason": norm["reason"]}
    store = get_store()

    meta = None
    if norm.get("upc"):
        # UPC barcodes don't encode the ISBN — resolve via catalog identifier search
        meta = metadata.lookup_upc(norm["upc"])
        if meta is None:
            return {"ok": True, "isbn13": None, "found": False, "metadata": None,
                    "ownership": {"exact": None, "related": [], "work": None, "read_state": None}}
        isbn13 = meta.get("isbn13")
    else:
        isbn13 = norm["isbn13"]

    # the shared catalog doubles as a metadata cache
    existing = store.get_edition_by_isbn(auth.token, isbn13) if isbn13 else None
    if existing:
        meta = {k: existing.get(k) for k in (
            "isbn13", "isbn10", "title", "subtitle", "publisher", "published_date",
            "description", "format", "cover_url", "google_volume_id",
            "ol_edition_key", "page_count", "language", "genre")}
        meta["authors"] = [a.strip() for a in (existing.get("authors") or "").split(",") if a.strip()]
        return {"ok": True, "isbn13": isbn13, "found": True, "metadata": meta,
                "ownership": _ownership(auth, meta, edition=existing)}

    if meta is None:
        meta = metadata.lookup_isbn(isbn13)
    if meta is None:
        return {"ok": True, "isbn13": isbn13, "found": False, "metadata": None,
                "ownership": {"exact": None, "related": [], "work": None, "read_state": None}}
    return {"ok": True, "isbn13": isbn13, "found": True, "metadata": meta,
            "ownership": _ownership(auth, meta)}


def _matches_query(item: dict, q: str) -> bool:
    if q.isdigit():
        return item.get("isbn13") == q or q in (item.get("title") or "")
    haystack = f"{item.get('title') or ''} {item.get('authors') or ''}".lower()
    return all(term in haystack for term in q.lower().split())


@app.get("/api/search")
def api_search(q: str, auth: AuthContext = Depends(require_auth)):
    q = q.strip()
    if not q:
        return {"local": [], "external": []}
    store = get_store()
    library_ids = _library_ids(auth)

    # An ISBN typed into the search box gets a direct lookup — free-text
    # search misses ISBNs for editions the catalogs haven't cross-indexed.
    # A UPC gets the same treatment via the catalogs' identifier indexes.
    norm = metadata.normalize_code(q)
    if norm.get("isbn13"):
        q = norm["isbn13"]
    local = [_flatten_book(h) for h in store.list_library_books(auth.token, library_ids)]
    local = [item for item in local if _matches_query(item, q)]
    _annotate_read_status(auth, local)

    external = []
    if norm.get("isbn13"):
        meta = metadata.lookup_isbn(norm["isbn13"])
        if meta:
            external = [meta]
    elif norm.get("upc"):
        meta = metadata.lookup_upc(norm["upc"])
        if meta:
            external = [meta]
    if not external:
        external = metadata.search_external(q)

    # annotate external results with what's already on my shelves + read
    nkeys = [m["norm_key"] for m in external if m.get("norm_key")]
    olkeys = [m["ol_work_key"] for m in external if m.get("ol_work_key")]
    works = store.works_by_norm_keys(auth.token, nkeys, olkeys)
    works_by_nkey = {w["norm_key"]: w for w in works}
    works_by_ol = {w["ol_work_key"]: w for w in works if w.get("ol_work_key")}
    editions = store.editions_for_works(auth.token, [w["id"] for w in works])
    holdings = store.holdings_for_editions(auth.token, library_ids, [e["id"] for e in editions])
    editions_by_id = {e["id"]: e for e in editions}
    held_by_work = {}
    held_isbns = set()
    for h in holdings:
        edition = editions_by_id.get(h["edition_id"]) or {}
        held_by_work.setdefault(str(edition.get("work_id")), []).append(
            {"id": h["id"], "format": edition.get("format"), "status": h["status"],
             "isbn13": edition.get("isbn13"), "library_id": h["library_id"]})
        if edition.get("isbn13"):
            held_isbns.add(edition["isbn13"])
    read_by_work = {str(s["work_id"]): s for s in
                    store.read_states_for_works(auth.token, auth.user_id, [w["id"] for w in works])}
    for m in external:
        work = works_by_ol.get(m.get("ol_work_key")) or works_by_nkey.get(m.get("norm_key"))
        m["owned_exact"] = bool(m.get("isbn13") and m["isbn13"] in held_isbns)
        m["owned_editions"] = held_by_work.get(str(work["id"]), []) if work else []
        state = read_by_work.get(str(work["id"])) if work else None
        m["read_status"] = state["status"] if state else None
        # the full state, so the sheet's reading editor starts from what's
        # saved instead of overwriting it with blanks
        m["read_state"] = _read_state_public(state)
    return {"local": local, "external": external}


# --------------------------------------------------------------------------
# books CRUD (library_books — a library's copy of an edition)
# --------------------------------------------------------------------------

def _resolve_edition(auth: AuthContext, meta: dict, fmt: str | None) -> dict:
    """Find the catalog edition for this metadata, or create it."""
    store = get_store()
    if meta.get("isbn13"):
        edition = store.get_edition_by_isbn(auth.token, meta["isbn13"])
        if edition:
            if fmt and not edition.get("format"):
                edition = store.update_edition(auth.token, edition["id"], {"format": fmt}) or edition
            return edition
    work = _resolve_work(auth, meta)
    return store.insert_edition(auth.token, {
        "id": new_id(),
        "work_id": work["id"],
        "isbn13": meta.get("isbn13"),
        "isbn10": meta.get("isbn10"),
        "title": meta.get("title"),
        "subtitle": meta.get("subtitle"),
        "authors": ", ".join(meta.get("authors") or []),
        "publisher": meta.get("publisher"),
        "published_date": meta.get("published_date"),
        "description": meta.get("description"),
        "format": fmt or meta.get("format"),
        "cover_url": meta.get("cover_url"),
        "google_volume_id": meta.get("google_volume_id"),
        "ol_edition_key": meta.get("ol_edition_key"),
        "page_count": meta.get("page_count"),
        "language": meta.get("language"),
        "genre": meta.get("genre"),
        "added_at": now_iso(),
    })


@app.post("/api/books")
def api_add_book(body: AddBookBody, auth: AuthContext = Depends(require_auth)):
    if body.status not in HOLDING_STATUSES:
        raise HTTPException(400, f"status must be one of {', '.join(HOLDING_STATUSES)}")
    meta = body.metadata or {}
    if not (meta.get("title") or "").strip():
        raise HTTPException(400, "a title is required")
    store = get_store()
    library_id = _target_library(auth, body.library_id)

    edition = _resolve_edition(auth, meta, body.format)
    existing = store.find_library_book(auth.token, library_id, edition["id"])
    if existing:
        fields = {"status": body.status, "status_changed_at": now_iso()}
        if body.notes is not None:
            fields["notes"] = body.notes
        updated = store.update_library_book(auth.token, existing["id"], fields)
        return {"book": _flatten_book({**updated, "edition": edition}), "existed": True}

    created = store.insert_library_book(auth.token, {
        "id": new_id(),
        "library_id": library_id,
        "edition_id": edition["id"],
        "status": body.status,
        "notes": body.notes,
        "copies": 1,
        "added_at": now_iso(),
        "status_changed_at": now_iso(),
    })
    return {"book": _flatten_book({**created, "edition": edition}), "existed": False}


@app.get("/api/books")
def api_list_books(status: str | None = None, q: str | None = None,
                   library_id: str | None = None, auth: AuthContext = Depends(require_auth)):
    if status is not None and status not in HOLDING_STATUSES:
        raise HTTPException(400, f"status must be one of {', '.join(HOLDING_STATUSES)}")
    if library_id == config.SAMPLE_LIBRARY_ID:
        # the shared Sample Library is browsable by everyone (view only —
        # every write path resolves through _target_library, which knows
        # nothing of it)
        library_ids = [library_id]
    elif library_id:
        library_ids = [_target_library(auth, library_id)]
    else:
        library_ids = _library_ids(auth)
    items = [_flatten_book(h) for h in
             get_store().list_library_books(auth.token, library_ids, status=status)]
    if q and q.strip():
        items = [item for item in items if _matches_query(item, q.strip())]
    _annotate_read_status(auth, items)
    return {"items": items}


def _get_owned_book(auth: AuthContext, book_id: str) -> dict:
    book = get_store().get_library_book(auth.token, book_id)
    if not book or book["library_id"] not in _library_ids(auth):
        raise HTTPException(404, "book not found")
    return book


@app.get("/api/books/{book_id}")
def api_get_book(book_id: str, auth: AuthContext = Depends(require_auth)):
    book = _get_owned_book(auth, book_id)
    edition = book.get("edition") or {}
    ownership = _ownership(auth, {}, edition=edition,
                           work=get_store().get_work(auth.token, edition.get("work_id")),
                           exclude_holding_id=book["id"])
    return {"book": _flatten_book(book), "related": ownership["related"],
            "read_state": ownership["read_state"]}


@app.patch("/api/books/{book_id}")
def api_update_book(book_id: str, body: UpdateBookBody, auth: AuthContext = Depends(require_auth)):
    book = _get_owned_book(auth, book_id)
    store = get_store()
    fields = {}
    if body.status is not None:
        if body.status not in HOLDING_STATUSES:
            raise HTTPException(400, f"status must be one of {', '.join(HOLDING_STATUSES)}")
        fields["status"] = body.status
        fields["status_changed_at"] = now_iso()
    if body.notes is not None:
        fields["notes"] = body.notes
    if body.copies is not None:
        fields["copies"] = max(1, body.copies)
    # format and genre live on the shared catalog edition, not the holding
    edition = book.get("edition") or {}
    edition_fields = {}
    if body.format is not None and body.format != edition.get("format"):
        edition_fields["format"] = body.format or None
    if body.genre is not None and body.genre != edition.get("genre"):
        edition_fields["genre"] = body.genre
    if edition_fields:
        edition = store.update_edition(auth.token, book["edition_id"], edition_fields)
    if not fields and not edition_fields:
        raise HTTPException(400, "nothing to update")
    updated = store.update_library_book(auth.token, book_id, fields) if fields else book
    return {"book": _flatten_book({**updated, "edition": edition})}


@app.delete("/api/books/{book_id}")
def api_delete_book(book_id: str, auth: AuthContext = Depends(require_auth)):
    _get_owned_book(auth, book_id)
    # the catalog edition/work stay: other libraries and read histories
    # may point at them
    get_store().delete_library_book(auth.token, book_id)
    return {"deleted": True}


# --------------------------------------------------------------------------
# read states (per-user, per-work — Goodreads-style shelves)
# --------------------------------------------------------------------------

@app.get("/api/reads")
def api_list_reads(status: str | None = None, auth: AuthContext = Depends(require_auth)):
    if status is not None and status not in READ_STATUSES:
        raise HTTPException(400, f"status must be one of {', '.join(READ_STATUSES)}")
    store = get_store()
    library_ids = _library_ids(auth)
    states = store.read_states_for_user(auth.token, auth.user_id)
    if status:
        states = [s for s in states if s["status"] == status]

    # which of these works do my libraries actually hold?
    holdings = store.list_library_books(auth.token, library_ids)
    held_by_work = {}
    for h in holdings:
        edition = h.get("edition") or {}
        held_by_work.setdefault(str(edition.get("work_id")), []).append(
            {"id": h["id"], "format": edition.get("format"), "status": h["status"],
             "library_id": h["library_id"]})

    items = []
    for s in states:
        work = s.get("work") or {}
        held = held_by_work.get(str(s["work_id"]), [])
        owned_physical = any(h["status"] == "library" for h in held)
        owned_digital = any(h["status"] == "digital" for h in held)
        items.append({
            **(_read_state_public(s) or {}),
            "title": work.get("title"),
            "authors": work.get("authors"),
            "cover_url": work.get("cover_url"),
            "owned": owned_physical or owned_digital,
            # trophy-hunt distinction: read on a kindle or borrowed from the
            # library ≠ a copy on the shelf
            "owned_physical": owned_physical,
            "owned_digital": owned_digital,
            "owned_editions": held,
        })
    return {"items": items}


@app.post("/api/reads")
def api_upsert_read(body: ReadBody, auth: AuthContext = Depends(require_auth)):
    if body.status not in READ_STATUSES:
        raise HTTPException(400, f"status must be one of {', '.join(READ_STATUSES)}")
    if body.rating is not None and not (1 <= body.rating <= 5):
        raise HTTPException(400, "rating must be 1-5")
    store = get_store()

    if body.work_id:
        work = store.get_work(auth.token, body.work_id)
        if not work:
            raise HTTPException(404, "work not found")
    elif body.metadata and (body.metadata.get("title") or "").strip():
        # marking something as read straight from scan/search — the work
        # may not exist yet (read but never owned)
        work = _resolve_work(auth, body.metadata)
    else:
        raise HTTPException(400, "work_id or metadata with a title is required")

    fields = {
        "edition_id": body.edition_id,
        "status": body.status,
        "rating": body.rating,
        "notes": body.notes,
        "started_at": body.started_at or None,
        "finished_at": body.finished_at or None,
        "updated_at": now_iso(),
    }
    existing = store.get_read_state(auth.token, auth.user_id, work["id"])
    if existing:
        state = store.update_read_state(auth.token, auth.user_id, work["id"], fields)
    else:
        state = store.insert_read_state(auth.token, {
            "id": new_id(), "user_id": auth.user_id, "work_id": work["id"],
            "created_at": now_iso(), **fields,
        })
    return {"read_state": _read_state_public(state), "work": work}


@app.delete("/api/reads/{work_id}")
def api_delete_read(work_id: str, auth: AuthContext = Depends(require_auth)):
    store = get_store()
    if not store.get_read_state(auth.token, auth.user_id, work_id):
        raise HTTPException(404, "no read state for this work")
    store.delete_read_state(auth.token, auth.user_id, work_id)
    return {"deleted": True}


# --------------------------------------------------------------------------
# genre enrichment
# --------------------------------------------------------------------------

@app.post("/api/enrich/genres")
def api_enrich_genres(body: EnrichGenresBody | None = None, auth: AuthContext = Depends(require_auth)):
    """Backfill genres for catalog editions my libraries hold that predate
    genre support (or whose sources had none). Works in small batches so
    the frontend can poll until remaining hits 0. Editions no source knows
    a genre for are marked with '' (tried, unknown) so they aren't retried
    forever. The response lists affected *holdings* — that's the id the
    shelves views key their books by."""
    limit = max(1, min(30, body.limit if body else 12))
    store = get_store()
    holdings = store.list_library_books(auth.token, _library_ids(auth))
    # null genre = never tried; '' = tried and unknown (skip those).
    # physical shelves enrich first — they're what the shelf views show.
    rank = {"library": 0, "digital": 1, "wishlist": 2}
    pending, holdings_by_edition = [], {}
    for holding in sorted(holdings, key=lambda h: rank.get(h["status"], 3)):
        edition = holding.get("edition") or {}
        if not edition.get("id"):
            continue
        holdings_by_edition.setdefault(edition["id"], []).append(holding["id"])
        if edition.get("genre") is None and len(holdings_by_edition[edition["id"]]) == 1:
            pending.append(edition)
    updated = []
    for edition in pending[:limit]:
        try:
            genre = metadata.genre_for_edition(edition) or ""
        except Exception:
            genre = ""  # one flaky external lookup shouldn't kill the batch
        store.update_edition(auth.token, edition["id"], {"genre": genre})
        for holding_id in holdings_by_edition[edition["id"]]:
            updated.append({"id": holding_id, "genre": genre})
    return {"updated": updated, "remaining": max(0, len(pending) - min(limit, len(pending)))}


# --------------------------------------------------------------------------
# stats / misc
# --------------------------------------------------------------------------

@app.get("/api/stats")
def api_stats(auth: AuthContext = Depends(require_auth)):
    store = get_store()
    holdings = store.list_library_books(auth.token, _library_ids(auth))
    states = store.read_states_for_user(auth.token, auth.user_id)
    return {
        "library": sum(1 for h in holdings if h["status"] == "library"),
        "digital": sum(1 for h in holdings if h["status"] == "digital"),
        "wishlist": sum(1 for h in holdings if h["status"] == "wishlist"),
        "works": len({(h.get("edition") or {}).get("work_id") for h in holdings}),
        "read": sum(1 for s in states if s["status"] == "read"),
    }


@app.get("/api/formats")
def api_formats():
    return {"formats": metadata.FORMATS}


static_dir = os.path.join(os.path.dirname(__file__), "static")
app.mount("/", StaticFiles(directory=static_dir, html=True), name="static")
