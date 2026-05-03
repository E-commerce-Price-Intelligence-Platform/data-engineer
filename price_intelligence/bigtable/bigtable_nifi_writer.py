"""
NiFi ExecuteStreamCommand target.
Reads one JSON record from stdin, writes one row to GCP Bigtable.
Exit 0 = success  → NiFi routes to 'success'
Exit 1 = failure  → NiFi routes to 'nonzero status'

Env vars (injected by NiFi ExecuteStreamCommand or docker-compose):
  GCP_PROJECT, BIGTABLE_INSTANCE_ID, BIGTABLE_TABLE_ID,
  GOOGLE_APPLICATION_CREDENTIALS
"""
import json, os, re, sys
from datetime import datetime, timezone
from google.cloud import bigtable

PROJECT_ID  = os.environ.get("GCP_PROJECT",         "regal-unfolding-490222-g5")
INSTANCE_ID = os.environ.get("BIGTABLE_INSTANCE_ID", "price-intel-instance")
TABLE_ID    = os.environ.get("BIGTABLE_TABLE_ID",    "smartphones")


def _encode(value):
    if value is None:
        return b""
    return str(value).encode("utf-8")


def _row_key(item: dict) -> bytes:
    site  = item.get("source_site", "unknown").lower()
    brand = re.sub(r"[^a-z0-9]", "_", (item.get("brand") or "unknown").lower())
    model = re.sub(r"[^a-z0-9]", "_", (item.get("model") or "unknown").lower())
    ts    = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")[:17]
    return f"{site}#{brand}#{model}#{ts}".encode("utf-8")


def main():
    raw = sys.stdin.read().strip()
    if not raw:
        print("[nifi_writer] Empty stdin — nothing to write", file=sys.stderr)
        sys.exit(1)

    try:
        item = json.loads(raw)
    except json.JSONDecodeError as e:
        print(f"[nifi_writer] JSON parse error: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        client   = bigtable.Client(project=PROJECT_ID, admin=False)
        instance = client.instance(INSTANCE_ID)
        table    = instance.table(TABLE_ID)

        key = _row_key(item)
        row = table.direct_row(key)

        for col in ("price", "old_price", "currency", "discount"):
            row.set_cell("price_cf", col, _encode(item.get(col)))

        for col in ("name", "brand", "model", "url", "rating", "reviews", "scraped_at"):
            row.set_cell("metadata_cf", col, _encode(item.get(col)))

        row.commit()
        print(f"[nifi_writer] OK  {key.decode()}", file=sys.stderr)
        sys.exit(0)

    except Exception as e:
        print(f"[nifi_writer] Bigtable error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
