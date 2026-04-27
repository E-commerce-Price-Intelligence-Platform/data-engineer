"""
Bigtable Setup — creates instance, table, and column families for GCP Bigtable.

Run from project root:
  python price_intelligence\\bigtable\\bigtable_setup.py
"""

import os

from google.cloud import bigtable
from google.cloud import bigtable_admin_v2 as bt_admin
from google.api_core.exceptions import AlreadyExists, NotFound, GoogleAPICallError


PROJECT_ID = os.environ.get("GCP_PROJECT", "regal-unfolding-490222-g5")
INSTANCE_ID = os.environ.get("BIGTABLE_INSTANCE_ID", "price-intel-instance")
TABLE_ID = os.environ.get("BIGTABLE_TABLE_ID", "smartphones")

# Bigtable cluster needs a ZONE, not a region.
BIGTABLE_ZONE = os.environ.get("BIGTABLE_ZONE", "us-central1-b")


def get_instance_admin_client():
    return bt_admin.BigtableInstanceAdminClient()


def get_data_client():
    return bigtable.Client(project=PROJECT_ID, admin=True)


def create_instance(admin_client, instance_id, zone):
    project_name = f"projects/{PROJECT_ID}"
    instance_name = f"{project_name}/instances/{instance_id}"
    location_name = f"{project_name}/locations/{zone}"

    try:
        instance = admin_client.get_instance(name=instance_name)
        print(f"✅ Instance '{instance_id}' already exists")
        return instance
    except NotFound:
        pass

    instance_config = bt_admin.Instance(
        display_name=instance_id,
        type_=bt_admin.Instance.Type.DEVELOPMENT,
        labels={
            "environment": "development",
            "application": "price-intelligence",
        },
    )

    cluster_id = f"{instance_id}-c1"

    cluster = bt_admin.Cluster(
        location=location_name,
        default_storage_type=bt_admin.StorageType.HDD,
    )

    try:
        print(f"Creating instance '{instance_id}' in zone '{zone}'...")
        operation = admin_client.create_instance(
            parent=project_name,
            instance_id=instance_id,
            instance=instance_config,
            clusters={cluster_id: cluster},
        )
        operation.result(timeout=300)
        print(f"✅ Instance '{instance_id}' created successfully")
        return admin_client.get_instance(name=instance_name)

    except AlreadyExists:
        print(f"✅ Instance '{instance_id}' already exists")
        return admin_client.get_instance(name=instance_name)

    except GoogleAPICallError as e:
        raise RuntimeError(f"Could not create Bigtable instance: {e}") from e


def create_table(instance, table_id):
    table = instance.table(table_id)

    if table.exists():
        print(f"✅ Table '{table_id}' already exists")
        return table

    try:
        print(f"Creating table '{table_id}'...")
        table.create()
        print(f"✅ Table '{table_id}' created successfully")
        return table

    except AlreadyExists:
        print(f"✅ Table '{table_id}' already exists")
        return table


def create_column_families(table):
    families = {
        "price_cf": "price, old_price, currency, discount",
        "metadata_cf": "name, brand, model, url, rating, reviews, scraped_at",
        "agg_cf": "statistical aggregations",
    }

    for cf_name, description in families.items():
        cf = table.column_family(cf_name)

        try:
            cf.create()
            print(f"✅ Column family '{cf_name}' created — {description}")

        except AlreadyExists:
            print(f"✅ Column family '{cf_name}' already exists")


def verify_setup(table):
    print("\n=== Verification ===")
    try:
        rows = table.read_rows(limit=1)
        count = sum(1 for _ in rows)
        print(f"✅ Table is accessible. Sample rows read: {count}")
    except Exception as e:
        print(f"⚠️ Could not verify table access: {e}")


def setup_bigtable():
    print("=== Bigtable Setup (GCP) ===")
    print(f"Project  : {PROJECT_ID}")
    print(f"Instance : {INSTANCE_ID}")
    print(f"Table    : {TABLE_ID}")
    print(f"Zone     : {BIGTABLE_ZONE}")

    creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    print(f"Credentials: {creds_path or 'Default Application Credentials'}")
    print()

    admin_client = get_instance_admin_client()
    create_instance(admin_client, INSTANCE_ID, BIGTABLE_ZONE)

    data_client = get_data_client()
    instance = data_client.instance(INSTANCE_ID)

    table = create_table(instance, TABLE_ID)
    create_column_families(table)
    verify_setup(table)

    print("\n✅ Bigtable setup complete.")
    print(f"Project  : {PROJECT_ID}")
    print(f"Instance : {INSTANCE_ID}")
    print(f"Table    : {TABLE_ID}")
    print("Families : price_cf | metadata_cf | agg_cf")


if __name__ == "__main__":
    setup_bigtable()