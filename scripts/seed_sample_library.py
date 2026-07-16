"""Manually stock the shared, view-only Sample Library.

The app does this itself at startup when the shelf is empty
(app/bootstrap.py), so this wrapper exists for hand-runs: restocking
after a manifest rebuild (--force tops it up; per-book idempotent) or
stocking without starting the app.

    uv run python scripts/seed_sample_library.py
    uv run python scripts/seed_sample_library.py --force

Rebuild the manifest itself with scripts/build_sample_library.py.
"""

import argparse
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import bootstrap, config  # noqa: E402

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="[seed_sample] %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", default=config.SAMPLE_BOOKS_PATH)
    parser.add_argument("--force", action="store_true",
                        help="stock even if the shelf already has books")
    args = parser.parse_args()

    print(f"mode={config.MODE} — sample library {config.SAMPLE_LIBRARY_ID}")
    result = bootstrap.stock_sample_library(manifest_path=args.manifest, force=args.force)
    if result["already_stocked"]:
        print(f"already stocked ({result['existing']} books) — nothing to do (use --force to top up)")
    else:
        print(f"done: {result['added']} added, {result['existing']} already on the shelf, "
              f"{result['failed']} failed")
    sys.exit(1 if result["failed"] else 0)
