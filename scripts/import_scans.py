"""Import previously scanned ISBNs into a running book-bot.

Feeds the same JSONL records the old barcode-scanner workflow produced
(~/SyncthingDB/Book-Bot/*.jsonl) — or any file with one ISBN per line —
through the live API, so every book gets full metadata + work grouping.

    uv run python scripts/import_scans.py --file ~/SyncthingDB/Book-Bot/HoneyCrisp.jsonl \
        --url http://127.0.0.1:8010 --username beca --password '...' [--dry-run]

JSONL records look like:
    {"iso_time": "...", "ISBN": "9781982156473", "State": "Wrapped", ...}
State "Wishlist" -> wishlist; anything else (e.g. "Wrapped") -> library.
"""

import argparse
import json
import time

import requests


def parse_line(line: str):
    line = line.strip()
    if not line:
        return None
    if line.startswith("{"):
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            return None
        isbn = str(rec.get("ISBN") or rec.get("isbn") or "").strip()
        state = "wishlist" if str(rec.get("State", "")).lower() == "wishlist" else "library"
        return (isbn, state) if isbn else None
    digits = "".join(c for c in line if c.isdigit() or c in "Xx")
    return (digits, "library") if len(digits) in (10, 13) else None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", required=True)
    parser.add_argument("--url", default="http://127.0.0.1:8010")
    parser.add_argument("--username", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    base = args.url.rstrip("/")
    resp = requests.post(f"{base}/api/login", json={"username": args.username, "password": args.password}, timeout=15)
    resp.raise_for_status()
    headers = {"Authorization": f"Bearer {resp.json()['token']}"}

    entries = {}
    with open(args.file) as f:
        for line in f:
            parsed = parse_line(line)
            if parsed:
                entries[parsed[0]] = parsed[1]  # last state per ISBN wins

    print(f"{len(entries)} unique isbn(s) in {args.file}")
    added = skipped = failed = 0
    for isbn, status in entries.items():
        if args.dry_run:
            print(f"would add {isbn} -> {status}")
            continue
        try:
            look = requests.get(f"{base}/api/lookup", params={"code": isbn}, headers=headers, timeout=30).json()
            if not look.get("ok") or not look.get("found"):
                print(f"  ?? {isbn}: {look.get('reason', 'no metadata found')} — skipped")
                failed += 1
                continue
            if look["ownership"]["exact"]:
                skipped += 1
                continue
            resp = requests.post(f"{base}/api/books", headers=headers, timeout=30,
                                 json={"status": status, "metadata": look["metadata"]})
            resp.raise_for_status()
            title = look["metadata"].get("title", "?")
            print(f"  ok {isbn} -> {status}: {title}")
            added += 1
            time.sleep(0.4)  # be polite to google books / open library
        except requests.RequestException as exc:
            print(f"  !! {isbn}: {exc}")
            failed += 1
    print(f"done: {added} added, {skipped} already present, {failed} failed")


if __name__ == "__main__":
    main()
