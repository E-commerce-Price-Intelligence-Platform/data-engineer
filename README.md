# Price Intelligence — Smartphone Price Tracking Pipeline

An end-to-end data engineering pipeline that scrapes smartphone prices from three Moroccan/international e-commerce sites, loads them into Google Cloud BigQuery, transforms them with dbt, stores raw rows in Cloud Bigtable, and orchestrates every step daily with Apache Airflow.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                          Data Sources                            │
│      Jumia.ma          Electroplanet.ma          Amazon.fr       │
└──────────┬─────────────────────┬──────────────────────┬──────────┘
           │                     │                      │
           ▼                     ▼                      ▼
┌──────────────────────────────────────────────────────────────────┐
│                     Step 1 — Scrapers                            │
│   Scrapy (Jumia)    Selenium (Electroplanet)   Selenium (Amazon) │
│                  output/  *.json + *.csv                         │
└────────────────────────────┬─────────────────────────────────────┘
                             │
              ┌──────────────┼───────────────┐
              │              │               │
              ▼              ▼               ▼
┌─────────────────┐  ┌──────────────┐  ┌────────────────────────────┐
│  Step 2 — NiFi  │  │  Step 4 —    │  │   Step 4b — BigQuery Load  │
│  Streaming      │  │  Bigtable    │  │   GCP project:             │
│  Watches output/│  │  Emulator    │  │   regal-unfolding-490222   │
│  1.55 MB ingest │  │  (Docker)    │  │   Dataset: raw_prices      │
│  → fan-out      │  │  587 rows    │  │   Tables: jumia /          │
└─────────────────┘  └──────────────┘  │          electroplanet /   │
                                       │          amazon            │
                                       │             │              │
                                       │             ▼              │
                                       │   Step 5 — dbt (BigQuery)  │
                                       │   staging  → marts         │
                                       │   8 models · 53 tests      │
                                       └────────────────────────────┘

          All scraping + storage steps orchestrated by:
┌──────────────────────────────────────────────────────────────────┐
│                  Step 3 — Airflow (daily 06:00 UTC)              │
│                                                                  │
│  [scrape_jumia, scrape_electroplanet]                            │
│       └──► scrape_amazon                                         │
│               ├──► setup_bigtable ──► write_to_bigtable ──┐      │
│               └──► load_to_bigquery                        │      │
│                        └──► dbt_deps ──► dbt_run ──────────┤      │
│                                             └──► validate ─┘      │
│                                                    └──► report    │
└──────────────────────────────────────────────────────────────────┘
```

---

## Pipeline Steps

### Step 1 — Scraping (✅ ~587 products/run)

| Spider | Site | Method | Products |
|---|---|---|---|
| `jumia_spider.py` | jumia.ma | Scrapy (HTTP) | ~347 |
| `electroplanet_spider.py` | electroplanet.ma | Selenium + Remote Chrome | ~115 |
| `amazon_spider.py` | amazon.fr | Selenium + Remote Chrome | ~125 |

Each spider searches for `samsung galaxy` and `apple iphone`, paginates through results, and extracts:

```json
{
  "name": "SAMSUNG GALAXY S26 ULTRA 12+256GB",
  "brand": "samsung",
  "model": "S26 Ultra",
  "price": 17730.0,
  "old_price": 18389.0,
  "currency": "DH",
  "discount": "4%",
  "rating": null,
  "reviews": null,
  "url": "https://...",
  "source_site": "electroplanet",
  "scraped_at": "2026-04-25T14:22:35"
}
```

Output written to `output/jumia.json`, `output/electroplanet.json`, `output/amazon.json` (overwritten each run).

---

### Step 2 — NiFi Streaming (✅ 1.55 MB ingested)

Apache NiFi flow (`nifi/NiFi_Flow.json`) watches the `output/` directory and routes the JSON files downstream. Handles schema validation and fan-out to multiple consumers.

---

### Step 3 — Airflow Orchestration (✅ DAG operational)

DAG `price_intelligence_daily` runs every day at 06:00 UTC.

```
[scrape_jumia, scrape_electro]
        └──► scrape_amazon
                ├──► setup_bigtable ──► write_to_bigtable ──┐
                └──► load_to_bigquery ──► dbt_deps ──► dbt_run ──┤
                                                                  └──► validate_output
                                                                              └──► generate_report
```

- Jumia and Electroplanet scrapers run **in parallel**
- Amazon runs after both complete
- Bigtable branch and BigQuery/dbt branch run **in parallel** after Amazon
- Both branches converge at `validate_output` before the final report
- `dbt test` is excluded from the daily run (53 tests × ~60s each = 50+ min); run manually when needed

Sample report output:
```
========================================
RAPPORT SCRAPING — 2026-04-25 14:22
========================================
  amazon               : 125 produits
  electroplanet        : 115 produits
  jumia                : 347 produits
  TOTAL                : 587 produits
========================================
```

---

### Step 4 — Bigtable Storage (✅ operational)

**Schema:**

| Column Family | Columns | Purpose |
|---|---|---|
| `price_cf` | price, old_price, currency, discount | Pricing data |
| `metadata_cf` | name, brand, model, url, rating, reviews, scraped_at | Product metadata |
| `agg_cf` | *(reserved for aggregations)* | Statistical aggregations |

**Row key design:** `{site}#{brand}#{model}#{timestamp}`

```
electroplanet#samsung#s26_ultra#20260425142235
jumia#apple#iphone_16_pro#20260425142237
amazon#samsung#s25#20260425142240
```

Enables efficient prefix scans by site, brand, or model.

---

### Step 5 — dbt Transformations (✅ 8 models · PASS=8 WARN=0 ERROR=0)

Data flows: `raw_prices` (BigQuery) → staging views → ephemeral intermediate → mart tables.

**BigQuery datasets created:**

| Dataset | Contents |
|---|---|
| `raw_prices` | Raw tables: `jumia`, `electroplanet`, `amazon` |
| `dbt_price_intelligence_staging` | Cleaned views: `stg_jumia`, `stg_electroplanet`, `stg_amazon` |
| `dbt_price_intelligence_marts` | Mart tables (see below) |

**Mart models:**

| Model | Type | Rows | Description |
|---|---|---|---|
| `mart_daily_prices` | Incremental (merge) | 52 | Avg/min/max price per site × brand × model × day |
| `mart_price_comparison` | Table | 52 | Same model across sites — cross-site price comparison |
| `mart_brand_stats` | Table | 2 | Count, min/max/avg/stddev, discount rate per brand |
| `mart_discount_leaders` | Table | 100 | Top 100 products ranked by computed discount % |
| `mart_price_velocity` | Table | 0* | Day-over-day price change (populates from day 2 onward) |

*`mart_price_velocity` requires ≥2 daily snapshots — will populate after the second scrape run.

**dbt tests:** 53 data tests including `unique`, `not_null`, `accepted_values`, `expression_is_true`, and 3 custom SQL tests:
- `assert_price_positive` — no product with price ≤ 0
- `assert_price_velocity_reasonable` — price change ≤ 50% per day
- `assert_no_future_scraped_at` — `scraped_at` not in the future

---

## Stack

| Component | Technology |
|---|---|
| Scraping (static) | Python + Scrapy |
| Scraping (dynamic) | Python + Selenium 4 + Remote Chrome |
| Browser automation | Selenium Grid (Docker) |
| Streaming ingestion | Apache NiFi |
| Orchestration | Apache Airflow 2.9 (SequentialExecutor + SQLite) |
| NoSQL storage | Google Cloud Bigtable (cbtemulator in Docker) |
| Data warehouse | Google BigQuery (GCP project `regal-unfolding-490222-g5`) |
| Transformations | dbt-bigquery 1.9 (dbt Core 1.8) |
| Containerization | Docker + Docker Compose |

---

## Project Structure

```
price-intelligence/
├── price_intelligence/
│   ├── spiders/
│   │   ├── jumia_spider.py             # Scrapy spider — jumia.ma
│   │   ├── electroplanet_spider.py     # Selenium spider — electroplanet.ma
│   │   └── amazon_spider.py            # Selenium spider — amazon.fr
│   ├── bigtable/
│   │   ├── bigtable_setup.py           # Creates Bigtable table + column families
│   │   └── bigtable_writer.py          # Reads JSON output → writes to Bigtable
│   ├── bigquery_loader.py              # Loads output/*.json → BigQuery raw_prices
│   ├── pipelines.py                    # Scrapy CSV + JSON pipelines
│   └── settings.py                     # Scrapy settings
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── packages.yml                    # dbt_utils dependency
│   ├── models/
│   │   ├── staging/                    # sources.yml + stg_*.sql views
│   │   ├── intermediate/               # int_prices_unified.sql (ephemeral)
│   │   └── marts/                      # 5 mart models + schema.yml
│   └── tests/                          # 3 custom SQL data quality tests
├── airflow/
│   ├── dags/
│   │   └── price_intelligence_dag.py   # Main DAG (12 tasks)
│   ├── Dockerfile                      # Airflow + Scrapy + Selenium + dbt-bigquery
│   └── docker-compose.yml              # Airflow + Selenium Grid + Bigtable emulator
├── nifi/
│   └── NiFi_Flow.json                  # NiFi flow definition
├── output/
│   ├── jumia.json / .csv
│   ├── electroplanet.json / .csv
│   └── amazon.json / .csv
└── scrapy.cfg
```

---

## Running Locally

**Prerequisites:** Docker Desktop · Python 3.12 · GCP service account key at `gcp-credentials.json`

```bash
# Start all services
cd airflow
docker-compose up -d --build

# Airflow UI  → http://localhost:8080  (admin / admin123)
# Selenium    → http://localhost:4444
# Bigtable    → internal only (bigtable:8086 via socat proxy)
```

**Trigger a full pipeline run** from the Airflow UI: DAG `price_intelligence_daily` → Trigger DAG.

**Run individual components directly:**

```bash
# Scrapy spider (Jumia)
cd price_intelligence && scrapy crawl jumia

# Selenium spiders (need Selenium Grid running)
SELENIUM_REMOTE_URL=http://localhost:4444 python price_intelligence/spiders/electroplanet_spider.py
SELENIUM_REMOTE_URL=http://localhost:4444 python price_intelligence/spiders/amazon_spider.py

# BigQuery loader
GCP_PROJECT=regal-unfolding-490222-g5 OUTPUT_DIR=/opt/airflow/price_intelligence/output \
  python price_intelligence/bigquery_loader.py

# dbt (from inside the container)
docker exec -it airflow bash -c "
  dbt deps --project-dir /opt/airflow/price_intelligence/dbt --profiles-dir /opt/airflow/price_intelligence/dbt &&
  dbt run  --project-dir /opt/airflow/price_intelligence/dbt --profiles-dir /opt/airflow/price_intelligence/dbt &&
  dbt test --project-dir /opt/airflow/price_intelligence/dbt --profiles-dir /opt/airflow/price_intelligence/dbt
"

# Bigtable setup/writer (inside container)
docker exec -it airflow bash -c "python /opt/airflow/price_intelligence/price_intelligence/bigtable/bigtable_setup.py"
docker exec -it airflow bash -c "python /opt/airflow/price_intelligence/price_intelligence/bigtable/bigtable_writer.py"
```

---

## Results

| Step | Status | Output |
|---|---|---|
| Step 1 — Scraping | ✅ | ~587 products/run · 3 sites |
| Step 2 — NiFi streaming | ✅ | 1.55 MB ingested |
| Step 3 — Airflow orchestration | ✅ | 12-task DAG · daily 06:00 UTC |
| Step 4 — Bigtable storage | ✅ | 587 rows · 3 column families |
| Step 5 — dbt transformations | ✅ | 8 models · PASS=8 · 53 tests |
