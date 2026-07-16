"""API behavior: per-user libraries, sharing, copies, and read states."""

from conftest import make_meta, valid_isbn13

ISBN_A = valid_isbn13("978000000001")   # hardcover
ISBN_B = valid_isbn13("978000000002")   # paperback, same story
ISBN_C = valid_isbn13("978000000003")   # unrelated book


def add_book(client, headers, meta, status="library", **kw):
    resp = client.post("/api/books", headers=headers, json={"status": status, "metadata": meta, **kw})
    assert resp.status_code == 200, resp.text
    return resp.json()


# --------------------------------------------------------------------------
# accounts + isolation
# --------------------------------------------------------------------------

def test_new_user_gets_a_personal_library(client, users):
    me = client.get("/api/me", headers=users("jason")).json()
    assert me["username"] == "jason"
    # own personal library plus the shared, view-only Sample Library
    own = [lib for lib in me["libraries"] if lib["role"] != "viewer"]
    assert len(own) == 1
    assert me["libraries"][0]["name"] == "jason's library"
    assert [m["username"] for m in me["libraries"][0]["members"]] == ["jason"]


def test_users_cannot_see_each_others_books(client, users):
    jason, beca = users("jason"), users("beca")
    add_book(client, jason, make_meta("A Private Book", ISBN_A))

    assert client.get("/api/books", headers=beca).json()["items"] == []
    me = client.get("/api/me", headers=beca).json()
    assert me["libraries"][0]["name"] == "beca's library"

    # beca scans the same isbn: the shared catalog knows the edition, but
    # ownership comes back empty for her
    look = client.get(f"/api/lookup?code={ISBN_A}", headers=beca).json()
    assert look["found"] is True  # catalog doubles as metadata cache
    assert look["ownership"]["exact"] is None
    assert look["ownership"]["related"] == []

    jason_book = client.get("/api/books", headers=jason).json()["items"][0]
    # and she can't touch his holding
    assert client.get(f"/api/books/{jason_book['id']}", headers=beca).status_code == 404
    assert client.patch(f"/api/books/{jason_book['id']}", headers=beca,
                        json={"status": "wishlist"}).status_code == 404
    assert client.delete(f"/api/books/{jason_book['id']}", headers=beca).status_code == 404


def test_sharing_a_library(client, users):
    jason, beca = users("jason"), users("beca")
    add_book(client, jason, make_meta("Shared Shelf Book", ISBN_A))
    lib = client.get("/api/me", headers=jason).json()["libraries"][0]

    # non-members can't invite
    resp = client.post(f"/api/libraries/{lib['id']}/members", headers=beca,
                       json={"username": "beca"})
    assert resp.status_code == 404

    resp = client.post(f"/api/libraries/{lib['id']}/members", headers=jason,
                       json={"username": "beca"})
    assert resp.status_code == 200

    me = client.get("/api/me", headers=beca).json()
    names = {lib["name"] for lib in me["libraries"]}
    assert "jason's library" in names and "beca's library" in names

    # beca now sees and can manage the shared book
    items = client.get(f"/api/books?library_id={lib['id']}", headers=beca).json()["items"]
    assert [i["title"] for i in items] == ["Shared Shelf Book"]
    assert client.patch(f"/api/books/{items[0]['id']}", headers=beca,
                        json={"notes": "beca was here"}).status_code == 200

    # duplicate invite is a 409, unknown user a 404
    assert client.post(f"/api/libraries/{lib['id']}/members", headers=jason,
                       json={"username": "beca"}).status_code == 409
    assert client.post(f"/api/libraries/{lib['id']}/members", headers=jason,
                       json={"username": "nobody"}).status_code == 404


def test_third_user_stays_out_of_shared_library(client, users):
    jason, new = users("jason"), users("newuser")
    add_book(client, jason, make_meta("Family Book", ISBN_A))
    me = client.get("/api/me", headers=new).json()
    assert [lib["name"] for lib in me["libraries"]
            if lib["role"] != "viewer"] == ["newuser's library"]
    assert client.get("/api/books", headers=new).json()["items"] == []
    assert client.get("/api/stats", headers=new).json()["library"] == 0


# --------------------------------------------------------------------------
# editions + copies
# --------------------------------------------------------------------------

def test_multiple_editions_and_identical_copies(client, users):
    jason = users("jason")
    meta_hb = make_meta("The Way of Kings", ISBN_A, authors=["Brandon Sanderson"], fmt="hardcover")
    meta_pb = make_meta("The Way of Kings", ISBN_B, authors=["Brandon Sanderson"], fmt="paperback")
    add_book(client, jason, meta_hb)
    pb = add_book(client, jason, meta_pb)["book"]

    # two soft backs of the same printing: copies = 2 on one holding
    resp = client.patch(f"/api/books/{pb['id']}", headers=jason, json={"copies": 2})
    assert resp.json()["book"]["copies"] == 2

    items = client.get("/api/books", headers=jason).json()["items"]
    assert len(items) == 2  # one holding per edition
    # both editions share one work
    assert len({i["work_id"] for i in items}) == 1
    assert client.get("/api/stats", headers=jason).json() == {
        "library": 2, "digital": 0, "wishlist": 0, "works": 1, "read": 0}

    # scanning the paperback again: exact hit + the hardcover as related
    look = client.get(f"/api/lookup?code={ISBN_B}", headers=jason).json()
    assert look["ownership"]["exact"]["copies"] == 2
    assert [r["format"] for r in look["ownership"]["related"]] == ["hardcover"]


def test_digital_ownership_state(client, users):
    jason = users("jason")
    add_book(client, jason, make_meta("Kindle Book", ISBN_A, fmt="ebook"), status="digital")
    add_book(client, jason, make_meta("Paper Book", ISBN_B))

    digital = client.get("/api/books?status=digital", headers=jason).json()["items"]
    assert [i["title"] for i in digital] == ["Kindle Book"]
    stats = client.get("/api/stats", headers=jason).json()
    assert stats["library"] == 1 and stats["digital"] == 1

    # a digital copy can be promoted to the physical shelf
    resp = client.patch(f"/api/books/{digital[0]['id']}", headers=jason, json={"status": "library"})
    assert resp.json()["book"]["status"] == "library"
    assert client.get("/api/stats", headers=jason).json()["digital"] == 0

    # unknown statuses are rejected everywhere
    resp = client.post("/api/books", headers=jason,
                       json={"status": "floppy-disk", "metadata": make_meta("X")})
    assert resp.status_code == 400
    assert client.get("/api/books?status=floppy-disk", headers=jason).status_code == 400


def test_trophies_read_but_no_physical_copy(client, users):
    jason = users("jason")
    # read on a kindle: owned digitally, no physical copy → trophy candidate
    digital = add_book(client, jason, make_meta("Kindle Read", ISBN_A), status="digital")["book"]
    client.post("/api/reads", headers=jason, json={"work_id": digital["work_id"], "status": "read"})
    # read a borrowed book: not owned at all → trophy candidate
    client.post("/api/reads", headers=jason, json={
        "metadata": make_meta("Borrowed Read"), "status": "read"})
    # read and shelved: not a trophy
    shelved = add_book(client, jason, make_meta("Shelf Read", ISBN_B))["book"]
    client.post("/api/reads", headers=jason, json={"work_id": shelved["work_id"], "status": "read"})

    reads = {r["title"]: r for r in client.get("/api/reads", headers=jason).json()["items"]}
    kindle, borrowed, shelf = reads["Kindle Read"], reads["Borrowed Read"], reads["Shelf Read"]
    assert kindle["owned_digital"] and not kindle["owned_physical"] and kindle["owned"]
    assert not borrowed["owned_digital"] and not borrowed["owned_physical"] and not borrowed["owned"]
    assert shelf["owned_physical"] and shelf["owned"]
    # the trophy filter (read + no physical copy) is applied client-side on
    # exactly these two flags
    trophies = [r for r in reads.values() if r["status"] == "read" and not r["owned_physical"]]
    assert {r["title"] for r in trophies} == {"Kindle Read", "Borrowed Read"}


def test_format_backfills_a_formatless_catalog_edition(client, users):
    jason, beca = users("jason"), users("beca")
    add_book(client, jason, make_meta("Fmt Book", ISBN_A))  # no format known
    # beca's separate library adds the same isbn and picks a format: the
    # user's choice must not be dropped just because the catalog row exists
    added = add_book(client, beca, make_meta("Fmt Book", ISBN_A), format="paperback")
    assert added["book"]["format"] == "paperback"
    # jason's holding shows the enriched catalog format too
    assert client.get("/api/books", headers=jason).json()["items"][0]["format"] == "paperback"


def test_re_adding_same_isbn_updates_instead_of_duplicating(client, users):
    jason = users("jason")
    add_book(client, jason, make_meta("Wish Book", ISBN_C), status="wishlist")
    second = add_book(client, jason, make_meta("Wish Book", ISBN_C), status="library")
    assert second["existed"] is True
    items = client.get("/api/books", headers=jason).json()["items"]
    assert len(items) == 1
    assert items[0]["status"] == "library"


# --------------------------------------------------------------------------
# read states
# --------------------------------------------------------------------------

def test_read_state_lifecycle(client, users):
    jason, beca = users("jason"), users("beca")
    book = add_book(client, jason, make_meta("Read Me", ISBN_A))["book"]

    resp = client.post("/api/reads", headers=jason, json={
        "work_id": book["work_id"], "status": "read", "rating": 5,
        "notes": "loved it", "started_at": "2026-01-01", "finished_at": "2026-02-01",
    })
    assert resp.status_code == 200, resp.text

    reads = client.get("/api/reads", headers=jason).json()["items"]
    assert len(reads) == 1
    assert reads[0]["status"] == "read"
    assert reads[0]["rating"] == 5
    assert reads[0]["finished_at"] == "2026-02-01"
    assert reads[0]["owned"] is True
    assert reads[0]["title"] == "Read Me"

    # read states are personal: beca (even as a co-member later) sees none
    assert client.get("/api/reads", headers=beca).json()["items"] == []

    # upsert, not duplicate
    client.post("/api/reads", headers=jason, json={
        "work_id": book["work_id"], "status": "reading", "rating": 3})
    reads = client.get("/api/reads", headers=jason).json()["items"]
    assert len(reads) == 1
    assert reads[0]["status"] == "reading"

    # my read status decorates the shelf list
    items = client.get("/api/books", headers=jason).json()["items"]
    assert items[0]["read_status"] == "reading"
    beca_look = client.get("/api/books", headers=beca).json()["items"]
    assert beca_look == []

    client.delete(f"/api/reads/{book['work_id']}", headers=jason)
    assert client.get("/api/reads", headers=jason).json()["items"] == []


def test_read_but_not_owned(client, users):
    jason = users("jason")
    # marked read straight from search — never added to any library
    resp = client.post("/api/reads", headers=jason, json={
        "metadata": make_meta("Borrowed From The Library", cover="http://x/c.jpg"),
        "status": "read", "finished_at": "2025-12-31",
    })
    assert resp.status_code == 200, resp.text

    reads = client.get("/api/reads", headers=jason).json()["items"]
    assert len(reads) == 1
    assert reads[0]["owned"] is False
    assert reads[0]["owned_editions"] == []
    assert reads[0]["cover_url"] == "http://x/c.jpg"

    # shelves stay empty; stats count the read
    assert client.get("/api/books", headers=jason).json()["items"] == []
    stats = client.get("/api/stats", headers=jason).json()
    assert stats["library"] == 0 and stats["read"] == 1

    # owning it later flips the flag (same work via title+author key)
    add_book(client, jason, make_meta("Borrowed From The Library", ISBN_A))
    reads = client.get("/api/reads", headers=jason).json()["items"]
    assert reads[0]["owned"] is True


def test_search_annotates_full_read_state(client, users, monkeypatch):
    """External results carry the saved read state, so the sheet's reading
    editor starts from it instead of overwriting rating/notes with blanks."""
    from app import metadata as md

    jason = users("jason")
    client.post("/api/reads", headers=jason, json={
        "metadata": make_meta("Loaner"), "status": "read", "rating": 5, "notes": "great"})
    nk = md.norm_key("Loaner", ["Test Author"])
    monkeypatch.setattr(md, "search_external", lambda q, limit=12: [
        {"title": "Loaner", "authors": ["Test Author"], "norm_key": nk, "isbn13": None}])

    ext = client.get("/api/search?q=loaner", headers=jason).json()["external"][0]
    assert ext["read_status"] == "read"
    assert ext["read_state"]["rating"] == 5
    assert ext["read_state"]["notes"] == "great"
    assert ext["read_state"]["work_id"]


def test_read_state_validation(client, users):
    jason = users("jason")
    assert client.post("/api/reads", headers=jason, json={
        "metadata": make_meta("X"), "status": "devoured"}).status_code == 400
    assert client.post("/api/reads", headers=jason, json={
        "metadata": make_meta("X"), "status": "read", "rating": 9}).status_code == 400
    assert client.post("/api/reads", headers=jason, json={
        "status": "read"}).status_code == 400
    assert client.delete("/api/reads/no-such-work", headers=jason).status_code == 404


# --------------------------------------------------------------------------
# libraries API
# --------------------------------------------------------------------------

def test_create_and_rename_library(client, users):
    jason = users("jason")
    resp = client.post("/api/libraries", headers=jason, json={"name": "cabin books"})
    lib_id = resp.json()["library"]["id"]
    assert resp.status_code == 200

    add_book(client, jason, make_meta("Cabin Book", ISBN_C), library_id=lib_id)
    items = client.get(f"/api/books?library_id={lib_id}", headers=jason).json()["items"]
    assert [i["title"] for i in items] == ["Cabin Book"]
    # default listing spans all my libraries
    assert len(client.get("/api/books", headers=jason).json()["items"]) == 1

    assert client.patch(f"/api/libraries/{lib_id}", headers=jason,
                        json={"name": "lake house"}).status_code == 200
    me = client.get("/api/me", headers=jason).json()
    assert "lake house" in {lib["name"] for lib in me["libraries"]}

    # another user can't add to it
    beca = users("beca")
    resp = client.post("/api/books", headers=beca, json={
        "status": "library", "metadata": make_meta("Sneaky"), "library_id": lib_id})
    assert resp.status_code == 404
