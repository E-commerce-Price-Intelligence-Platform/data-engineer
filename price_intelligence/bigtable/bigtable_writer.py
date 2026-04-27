"""
Bigtable Writer — reads output JSON files and writes rows to Bigtable.
Run: python bigtable_writer.py
Row key: {site}#{brand}#{model}#{timestamp}

Configuration via environment variables:
  - GCP_PROJECT: GCP project ID (required)
  - BIGTABLE_INSTANCE_ID: Bigtable instance ID (default: price-intel-instance)
  - BIGTABLE_TABLE_ID: Bigtable table ID (default: smartphones)
  - OUTPUT_DIR: Directory containing JSON files (default: ./output)
  - GOOGLE_APPLICATION_CREDENTIALS: Path to service account key (optional)
"""
import json, os, re, time, glob
from datetime import datetime
from google.cloud import bigtable

# Configuration from environment with sensible defaults
PROJECT_ID = os.environ.get("GCP_PROJECT", "regal-unfolding-490222-g5")
INSTANCE_ID = os.environ.get("BIGTABLE_INSTANCE_ID", "price-intel-instance")
TABLE_ID = os.environ.get("BIGTABLE_TABLE_ID", "smartphones")
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", r"C:\Users\hp\Desktop\price-intelligence\output")

SOURCES = ["jumia", "electroplanet", "amazon"]


def _encode(value):
    if value is None:
        return b""
    return str(value).encode("utf-8")


def _row_key(item):
    site  = item.get("source_site", "unknown").lower()
    brand = re.sub(r"[^a-z0-9]", "_", (item.get("brand") or "unknown").lower())
    model = re.sub(r"[^a-z0-9]", "_", (item.get("model") or "unknown").lower())
    ts    = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")[:17]
    return f"{site}#{brand}#{model}#{ts}".encode("utf-8")


def find_latest_json(source):
    """Trouve le fichier JSON pour un site donné"""
    simple = os.path.join(OUTPUT_DIR, f"{source}.json")
    if os.path.exists(simple):
        return simple

    pattern = os.path.join(OUTPUT_DIR, f"{source}_*.json")
    files   = glob.glob(pattern)
    if files:
        return max(files, key=os.path.getctime)
    return None


# ✅ VERSION SANS TIMESTAMP
def write_items(table, items, source):
    rows = []

    for item in items:
        key = _row_key(item)
        r   = table.direct_row(key)

        # price_cf
        for col in ("price", "old_price", "currency", "discount"):
            r.set_cell("price_cf", col, _encode(item.get(col)))

        # metadata_cf
        for col in ("name", "brand", "model", "url",
                    "rating", "reviews", "scraped_at"):
            r.set_cell("metadata_cf", col, _encode(item.get(col)))

        rows.append(r)

    if rows:
        response = table.mutate_rows(rows)
        errors   = [e for e in response if e.code != 0]
        written  = len(rows) - len(errors)
        print(f"  ✅ {source}: {written}/{len(rows)} lignes écrites")
        return written
    else:
        print(f"  ⚠ {source}: aucun item")
        return 0


def run():
    print("=== Bigtable Writer (GCP) ===")
    print(f"Project   : {PROJECT_ID}")
    print(f"Instance  : {INSTANCE_ID}")
    print(f"Table     : {TABLE_ID}")
    print(f"Output dir: {OUTPUT_DIR}")

    # Verify credentials
    creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if creds_path:
        print(f"Credentials: {creds_path}")
    else:
        print("Credentials: Using default application credentials (ADC)")
    print()

    client   = bigtable.Client(project=PROJECT_ID, admin=False)
    instance = client.instance(INSTANCE_ID)
    table    = instance.table(TABLE_ID)

    total = 0

    for source in SOURCES:
        path = find_latest_json(source)

        if not path:
            print(f"\n  ⚠ {source}: aucun fichier trouvé dans {OUTPUT_DIR}")
            continue

        print(f"\n[{source}] Fichier : {os.path.basename(path)}")

        with open(path, encoding="utf-8") as f:
            try:
                items = json.load(f)
            except json.JSONDecodeError as e:
                print(f"  ✗ Erreur JSON : {e}")
                continue

        print(f"  → {len(items)} produits")
        written = write_items(table, items, source)
        total  += written

    print(f"\n{'='*40}")
    print(f"TOTAL ÉCRIT : {total} lignes")
    print(f"{'='*40}")

    # Vérification — afficher 5 lignes
    print("\n🔍 Aperçu Bigtable — 5 premières lignes :")
    try:
        count = 0
        for bt_row in table.read_rows(limit=5):
            count += 1
            key   = bt_row.row_key.decode()
            name  = bt_row.cells.get("metadata_cf", {}).get(b"name",  [None])[0]
            price = bt_row.cells.get("price_cf",    {}).get(b"price", [None])[0]

            name_str  = name.value.decode()  if name  else "N/A"
            price_str = price.value.decode() if price else "N/A"

            print(f"  [{count}] {key[:45]} | {name_str[:35]} | {price_str}")

        if count == 0:
            print("  (table vide)")

    except Exception as e:
        print(f"  ✗ Erreur lecture : {e}")

    print("\n✅ Terminé !")


if __name__ == "__main__":
    run()