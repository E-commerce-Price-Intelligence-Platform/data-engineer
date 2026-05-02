# Price Intelligence — Smartphone Price Tracking Pipeline

End-to-end data engineering pipeline: scrapes smartphone prices from three e-commerce sites, loads into Google BigQuery, transforms with dbt, stores raw rows in Cloud Bigtable, and orchestrates every step with Apache Airflow.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                          Data Sources                            │
│      Jumia.ma          Electroplanet.ma          Amazon.fr       │
└──────────┬─────────────────────┬──────────────────────┬──────────┘
           │ Scrapy              │ Selenium             │ Selenium
           ▼                     ▼                      ▼
┌──────────────────────────────────────────────────────────────────┐
│                   Scrapers → output/*.json                        │
│  ~356 products      ~56 products           ~136 products         │
└────────────────────────────┬─────────────────────────────────────┘
                             │
              ┌──────────────┴──────────────┐
              ▼                             ▼
┌─────────────────────────┐   ┌──────────────────────────────────┐
│  Cloud Bigtable (GCP)   │   │  BigQuery raw_prices             │
│  price-intel-instance   │   │  jumia / electroplanet / amazon  │
│  548 rows · 3 families  │   │  552 rows                        │
└─────────────────────────┘   └──────────────┬───────────────────┘
                                             │
                                             ▼
                                  ┌──────────────────────┐
                                  │  dbt-bigquery        │
                                  │  staging → marts     │
                                  │  8 models · 53 tests │
                                  └──────────────────────┘

          All steps orchestrated by Apache Airflow (manual trigger):

  [scrape_jumia, scrape_electroplanet]
        └──► scrape_amazon
                ├──► setup_bigtable ──► write_to_bigtable ──┐
                └──► load_to_bigquery ──► dbt_run ──► dbt_test (non-blocking)
                                                    └──► validate_output ──► generate_report
```

---

## Pipeline Steps

### Step 1 — Scraping (~548 products/run)

| Spider | Site | Method | Products/run | Fields |
|---|---|---|---|---|
| `jumia_spider.py` | jumia.ma | Scrapy (HTTP) | ~356 | price, rating, reviews |
| `electroplanet_spider.py` | electroplanet.ma | Selenium + Remote Chrome | ~56 | price (data-price-amount), rating (%), reviews |
| `amazon_spider.py` | amazon.fr | Selenium + Remote Chrome | ~136 | price (whole+fraction), rating, reviews |

Each spider filters smartphones only (excludes cases, chargers, earphones, watches). Output written to `output/*.json` (overwritten each run).

**Data quality guards in `bigquery_loader.py`:**
- Null-price rate > 50% (Jumia/Electroplanet) or > 80% (Amazon) → hard fail before load
- `_ensure_dataset` catches only `Conflict`, re-raises all other GCP errors

---

### Step 2 — Airflow Orchestration (DAG: `price_intelligence_daily`)

Schedule: **manual trigger only** (`schedule=None`) — switch to `"0 6 * * *"` for production.

```
[scrape_jumia, scrape_electroplanet] ──► scrape_amazon
                                              │
                    ┌─────────────────────────┼──────────────────────┐
                    ▼                         ▼                      │
          setup_bigtable             load_to_bigquery                │
                    │                         │                      │
                    ▼                         ▼                      │
          write_to_bigtable              dbt_run                     │
                    │                         │                      │
                    │                   dbt_test (ALL_DONE,          │
                    │                   no timeout, non-blocking)    │
                    └──────────► validate_output ◄──────────────────┘
                                       │
                                 generate_report
```

Sample report:
```
========================================
RAPPORT SCRAPING — 2026-05-02 21:47
========================================
  electroplanet        : 56 produits
  amazon               : 136 produits
  jumia                : 356 produits
  TOTAL                : 548 produits
========================================
```

---

### Step 3 — Cloud Bigtable (GCP)

Instance `price-intel-instance` · Table `smartphones` · Zone `us-central1-b`

**Row key:** `{site}#{brand}#{model}#{timestamp}` — prefix scans by site/brand/model.

```
electroplanet#samsung#s26_ultra#20260502214611
jumia#apple#iphone_16_pro#20260502194702
amazon_fr#apple#iphone_13#20260502215702
```

**Column families:**

| Family | Columns |
|---|---|
| `price_cf` | price, old_price, currency, discount |
| `metadata_cf` | name, brand, model, url, rating, reviews, scraped_at |
| `agg_cf` | *(reserved — statistical aggregations)* |

---

### Step 4 — BigQuery + dbt

Raw tables loaded by `bigquery_loader.py` → dbt transforms in 3 layers:

```
raw_prices (BigQuery)
    └──► staging views (stg_jumia, stg_electroplanet, stg_amazon)
              └──► int_prices_unified (ephemeral)
                        └──► mart tables
```

**Mart models:**

| Model | Type | Description |
|---|---|---|
| `mart_daily_prices` | Incremental (merge on `price_day_sk`) | Avg/min/max price per site × brand × model × day |
| `mart_price_comparison` | Table | Same model across sites — cross-site comparison |
| `mart_brand_stats` | Table | Count, avg/stddev, discount rate per brand |
| `mart_discount_leaders` | Table | All products with discount, ranked by % |
| `mart_price_velocity` | Table | Day-over-day price change (populates from day 2) |

**dbt test results (last run):**

```
53 tests · PASS=50 · WARN=3 · ERROR=0
```

| Warn | Reason |
|---|---|
| `source_not_null_raw_prices_amazon_price` (90) | Amazon hides prices for marketplace/B2B listings |
| `source_not_null_raw_prices_jumia_price` (1) | One product with no price |
| `assert_price_velocity_reasonable` (9) | No prior day to compare — expected on first run |

---

## Stack

| Component | Technology |
|---|---|
| Scraping (static) | Python + Scrapy 2.x |
| Scraping (dynamic) | Python + Selenium 4 + Remote Chrome |
| Browser automation | Selenium Grid (Docker `standalone-chrome`) |
| Orchestration | Apache Airflow 2.9 (SequentialExecutor + SQLite) |
| NoSQL storage | Google Cloud Bigtable (`price-intel-instance`) |
| Data warehouse | Google BigQuery (`regal-unfolding-490222-g5`) |
| Transformations | dbt-core 1.8.7 + dbt-bigquery 1.8.2 |
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
│   │   ├── bigtable_setup.py           # Creates instance, table, column families
│   │   └── bigtable_writer.py          # Reads output/*.json → writes to Bigtable
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
│   │   └── price_intelligence_dag.py   # Main DAG (11 tasks, manual trigger)
│   ├── Dockerfile                      # Airflow + Scrapy + Selenium + dbt-bigquery 1.8
│   └── docker-compose.yml              # Airflow service + Selenium Grid
├── output/
│   ├── jumia.json / .csv
│   ├── electroplanet.json / .csv
│   └── amazon.json / .csv
└── scrapy.cfg
```

---

## Setup & Running

**Prerequisites:** Docker · GCP service account key saved as `gcp-credentials.json` at project root

```bash
# Start services
cd airflow
docker-compose up -d --build

# Airflow UI  → http://localhost:8080  (admin / admin123)
# Selenium    → http://localhost:4444
```

**First-time dbt setup (run once after deploy):**
```bash
docker exec airflow dbt deps \
  --project-dir /opt/airflow/price_intelligence/dbt \
  --profiles-dir /opt/airflow/price_intelligence/dbt
```

**Trigger full pipeline:** Airflow UI → `price_intelligence_daily` → ▶ Trigger DAG

**Run components individually (inside container):**
```bash
# Scrapy spider
docker exec airflow bash -c "cd /opt/airflow/price_intelligence && python -m scrapy crawl jumia -s LOG_LEVEL=WARNING"

# Selenium spiders
docker exec airflow bash -c "cd /opt/airflow/price_intelligence && python price_intelligence/spiders/electroplanet_spider.py"
docker exec airflow bash -c "cd /opt/airflow/price_intelligence && python price_intelligence/spiders/amazon_spider.py"

# BigQuery loader
docker exec airflow bash -c "
  GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/price_intelligence/gcp-credentials.json \
  OUTPUT_DIR=/opt/airflow/price_intelligence/output \
  python /opt/airflow/price_intelligence/price_intelligence/bigquery_loader.py"

# dbt
docker exec airflow bash -c "
  cd /opt/airflow/price_intelligence/dbt && \
  GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/price_intelligence/gcp-credentials.json \
  python3 -c \"
from dbt.cli.main import dbtRunner
r = dbtRunner()
r.invoke(['run',  '--project-dir', '.', '--profiles-dir', '.'])
r.invoke(['test', '--project-dir', '.', '--profiles-dir', '.'])
\""
```

---

## Results

| Step | Status | Output |
|---|---|---|
| Scraping | ✅ | ~548 products/run · 3 sites |
| Airflow orchestration | ✅ | 11-task DAG · manual trigger |
| Bigtable storage | ✅ | 453+ rows · GCP · 3 column families |
| BigQuery load | ✅ | 552 rows · raw_prices dataset |
| dbt transformations | ✅ | 8 models · 50 PASS · 3 WARN · 0 ERROR |
