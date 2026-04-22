"""
Bigtable Setup — creates table and column families.
Run: python bigtable_setup.py

Note: the cbtemulator does not implement Instance Admin APIs (GetInstance,
CreateInstance). We skip the instance check and work directly with the
Table Admin API, which the emulator does support.
"""
import os

from google.cloud import bigtable
from google.api_core.exceptions import AlreadyExists

PROJECT_ID  = "price-intelligence"
INSTANCE_ID = "price-intel-instance"
TABLE_ID    = "smartphones"


def setup_bigtable():
    host = os.environ.get("BIGTABLE_EMULATOR_HOST", "localhost:8086")
    os.environ["BIGTABLE_EMULATOR_HOST"] = host

    print("=== Bigtable Setup ===")
    print(f"Emulator : {host}")

    client   = bigtable.Client(project=PROJECT_ID, admin=True)
    instance = client.instance(INSTANCE_ID)
    table    = instance.table(TABLE_ID)

    # cbtemulator does not support instance admin APIs — skip instance check
    print(f"Using instance '{INSTANCE_ID}' (emulator, no instance admin)")

    if not table.exists():
        print(f"Creating table '{TABLE_ID}'...")
        table.create()
        print("✅ Table created")
    else:
        print(f"✅ Table '{TABLE_ID}' already exists")

    families = {
        "price_cf":    "price, old_price, currency, discount",
        "metadata_cf": "name, brand, model, url, rating, reviews, scraped_at",
        "agg_cf":      "statistical aggregations (populated by dbt)",
    }
    for cf_name, description in families.items():
        cf = table.column_family(cf_name)
        try:
            cf.create()
            print(f"✅ Column family '{cf_name}' — {description}")
        except (AlreadyExists, Exception):
            print(f"   '{cf_name}' already exists")

    print("\n=== Schema ===")
    print(f"Project  : {PROJECT_ID}")
    print(f"Instance : {INSTANCE_ID}")
    print(f"Table    : {TABLE_ID}")
    print(f"Row key  : site#brand#model#timestamp")
    print(f"Families : price_cf | metadata_cf | agg_cf")
    print("\n✅ Setup done!")


if __name__ == "__main__":
    setup_bigtable()
