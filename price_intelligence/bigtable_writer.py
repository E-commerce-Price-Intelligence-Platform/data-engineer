"""
Bigtable Writer — reads output JSON files and writes rows to Bigtable.
Run: python bigtable_writer.py
Row key: {site}#{brand}#{model}#{timestamp_ms}
"""
import json
import os
import re
import time
from datetime import datetime

from google.cloud import bigtable
from google.cloud.bigtable import row

PROJECT_ID  = "price-intelligence"
INSTANCE_ID = "price-intel-instance"
TABLE_ID    = "smartphones"
OUTPUT_DIR  = "output"

SOURCES = ["jumia", "electroplanet", "amazon"]


def _encode(value):
    if value is None:
        return b""
    return str(value).encode("utf-8")


def _row_key(item: dict) -> bytes:
    site  = item.get("source_site", "unknown").lower()
    brand = re.sub(r"[^a-z0-9]", "_", (item.get("brand") or "unknown").lower())
    model = re.sub(r"[^a-z0-9]", "_", (item.get("model") or "unknown").lower())
    ts    = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")[:17]
    return f"{site}#{brand}#{model}#{ts}".encode("utf-8")


def write_items(table, items: list, source: str):
    ts = int(time.time() * 1000)  # millisecond timestamp for cell version
    rows = []
    for item in items:
        key = _row_key(item)
        r   = table.direct_row(key)

        # price_cf
        for col in ("price", "old_price", "currency", "discount"):
            r.set_cell("price_cf", col, _encode(item.get(col)), timestamp_micros=ts * 1000)

        # metadata_cf
        for col in ("name", "brand", "model", "url", "rating", "reviews", "scraped_at"):
            r.set_cell("metadata_cf", col, _encode(item.get(col)), timestamp_micros=ts * 1000)

        rows.append(r)

    if rows:
        response = table.mutate_rows(rows)
        errors = [e for e in response if e.code != 0]
        if errors:
            for e in errors:
                print(f"  ⚠ Row mutation error: {e.message}")
        print(f"  ✅ {source}: {len(rows) - len(errors)}/{len(rows)} rows written")
    else:
        print(f"  ⚠ {source}: no items to write")


def run():
    host = os.environ.get("BIGTABLE_EMULATOR_HOST", "localhost:8086")
    os.environ["BIGTABLE_EMULATOR_HOST"] = host

    print("=== Bigtable Writer ===")
    print(f"Emulator : {host}")

    client   = bigtable.Client(project=PROJECT_ID, admin=False)
    instance = client.instance(INSTANCE_ID)
    table    = instance.table(TABLE_ID)

    total = 0
    for source in SOURCES:
        path = os.path.join(OUTPUT_DIR, f"{source}.json")
        if not os.path.exists(path):
            print(f"  ⚠ {source}: {path} not found, skipping")
            continue

        with open(path, encoding="utf-8") as f:
            try:
                items = json.load(f)
            except json.JSONDecodeError as e:
                print(f"  ⚠ {source}: JSON parse error — {e}")
                continue

        print(f"\n[{source}] {len(items)} products")
        write_items(table, items, source)
        total += len(items)

    print(f"\n✅ Done — {total} products written to Bigtable")


if __name__ == "__main__":
    run()
