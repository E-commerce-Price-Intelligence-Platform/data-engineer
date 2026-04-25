"""
BigQuery Loader — reads output JSON files and loads them into BigQuery raw tables.
Each load TRUNCATES the target table so it always reflects the latest scrape run.

Run: python bigquery_loader.py
Env: GOOGLE_APPLICATION_CREDENTIALS, GCP_PROJECT, OUTPUT_DIR
"""
import json
import os
from io import StringIO

from google.cloud import bigquery

PROJECT_ID  = os.environ.get("GCP_PROJECT", "regal-unfolding-490222-g5")
RAW_DATASET = "raw_prices"
SOURCES     = ["jumia", "electroplanet", "amazon"]

SCHEMA = [
    bigquery.SchemaField("name",        "STRING"),
    bigquery.SchemaField("brand",       "STRING"),
    bigquery.SchemaField("model",       "STRING"),
    bigquery.SchemaField("price",       "FLOAT64"),
    bigquery.SchemaField("old_price",   "FLOAT64"),
    bigquery.SchemaField("currency",    "STRING"),
    bigquery.SchemaField("discount",    "STRING"),
    bigquery.SchemaField("rating",      "FLOAT64"),
    bigquery.SchemaField("reviews",     "FLOAT64"),
    bigquery.SchemaField("url",         "STRING"),
    bigquery.SchemaField("source_site", "STRING"),
    bigquery.SchemaField("scraped_at",  "TIMESTAMP"),
]


def _ensure_dataset(client: bigquery.Client):
    dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{RAW_DATASET}")
    dataset_ref.location = os.environ.get("BQ_LOCATION", "EU")
    try:
        client.create_dataset(dataset_ref, timeout=30)
        print(f"✅ Dataset '{RAW_DATASET}' created")
    except Exception:
        print(f"   Dataset '{RAW_DATASET}' already exists")


def _load_source(client: bigquery.Client, source: str, output_dir: str):
    path = os.path.join(output_dir, f"{source}.json")
    if not os.path.exists(path):
        print(f"  ⚠ {source}: {path} not found, skipping")
        return 0

    with open(path, encoding="utf-8") as f:
        try:
            records = json.load(f)
        except json.JSONDecodeError as e:
            print(f"  ⚠ {source}: JSON parse error — {e}")
            return 0

    if not records:
        print(f"  ⚠ {source}: empty file, skipping")
        return 0

    ndjson = "\n".join(json.dumps(row) for row in records)

    job_config = bigquery.LoadJobConfig(
        schema=SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )

    table_id = f"{PROJECT_ID}.{RAW_DATASET}.{source}"
    job = client.load_table_from_file(
        StringIO(ndjson),
        table_id,
        job_config=job_config,
    )
    job.result()

    table = client.get_table(table_id)
    print(f"  ✅ {source}: {table.num_rows} rows → {table_id}")
    return table.num_rows


def run():
    output_dir = os.environ.get("OUTPUT_DIR", "output")

    print("=== BigQuery Loader ===")
    print(f"Project : {PROJECT_ID}")
    print(f"Dataset : {RAW_DATASET}")
    print(f"Output  : {output_dir}")

    client = bigquery.Client(project=PROJECT_ID)
    _ensure_dataset(client)

    total = 0
    for source in SOURCES:
        total += _load_source(client, source, output_dir)

    print(f"\n✅ Done — {total} rows loaded to BigQuery")


if __name__ == "__main__":
    run()
