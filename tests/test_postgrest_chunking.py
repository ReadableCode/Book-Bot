"""Unbounded in.() id lists must go to PostgREST in chunks.

The proxy chain in front of PostgREST rejects request lines beyond ~4KB
(about 90 quoted uuids), killing the connection — the 300-book sample
library first tripped this via /api/books' read-status annotation.
"""

import uuid

from app.store import PostgrestStore


class SpyStore(PostgrestStore):
    def __init__(self):
        super().__init__()
        self.calls = []

    def _request(self, method, table, token, params=None, body=None, minimal=False):
        self.calls.append(params)
        return [{"id": "u1", "username": "x", "user_id": "u1", "work_id": "w"}]


def ids(n):
    return [str(uuid.uuid4()) for _ in range(n)]


def in_count(params, key):
    return params[key].count(",") + 1


def test_large_id_lists_are_chunked():
    store = SpyStore()
    work_ids = ids(300)

    store.read_states_for_works("t", "u", work_ids)
    assert len(store.calls) == 6  # 300 / CHUNK_IDS(50)
    assert all(in_count(p, "work_id") <= PostgrestStore.CHUNK_IDS for p in store.calls)
    # every id went out exactly once
    sent = ",".join(p["work_id"] for p in store.calls)
    assert all(w in sent for w in work_ids)

    store.calls.clear()
    store.editions_for_works("t", work_ids)
    assert len(store.calls) == 6

    store.calls.clear()
    store.holdings_for_editions("t", ["lib1"], ids(120))
    assert len(store.calls) == 3
    assert all(in_count(p, "edition_id") <= PostgrestStore.CHUNK_IDS for p in store.calls)

    store.calls.clear()
    result = store.usernames_for_ids("t", ids(101))
    assert len(store.calls) == 3
    assert result == {"u1": "x"}  # merged across chunks


def test_small_lists_stay_single_request():
    store = SpyStore()
    store.read_states_for_works("t", "u", ids(10))
    assert len(store.calls) == 1


def test_chunk_size_stays_under_the_proxy_url_limit():
    # ~90 quoted uuids ≈ 4KB request line; chunks must leave headroom
    filt = PostgrestStore._in(ids(PostgrestStore.CHUNK_IDS))
    assert len(filt) < 2500
