# Price Intelligence — Smartphone Price Tracking Pipeline

An end-to-end data engineering pipeline that scrapes smartphone prices from three Moroccan/international e-commerce sites, orchestrates daily runs with Apache Airflow, streams data through Apache NiFi, and stores everything in Google Cloud Bigtable.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        Data Sources                         │
│   Jumia.ma        Electroplanet.ma        Amazon.fr         │
└────────┬──────────────────┬───────────────────┬─────────────┘
         │                  │                   │
         ▼                  ▼                   ▼
┌──────────────────────────────────────────────────────────────┐
│                    Step 1 — Scrapers                         │
│  Scrapy (Jumia)   Selenium (Electroplanet)  Selenium (Amazon)│
│            output/jumia.json / .csv                          │
│            output/electroplanet.json / .csv                  │
│            output/amazon.json / .csv                         │
└────────────────────────────┬─────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────┐
│                 Step 2 — NiFi Streaming                     │
│         Watches output/ → routes JSON to Bigtable           │
│                       1.55 MB ingested                      │
└────────────────────────────┬────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────┐
│               Step 3 — Airflow Orchestration                │
│                  DAG: price_intelligence_daily              │
│   scrape_jumia ──┐                                          │
│   scrape_electro ┼──► scrape_amazon ──► setup_bigtable      │
│                  │         ──► write_to_bigtable            │
│                  │         ──► validate_output              │
│                  └─────────── generate_report               │
└────────────────────────────┬────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────┐
│               Step 4 — Bigtable Storage (GCP)               │
│   Instance: price-intel-instance   Table: smartphones       │
│   Row key: site#brand#model#timestamp                       │
│   Families: price_cf | metadata_cf | agg_cf                 │
└─────────────────────────────────────────────────────────────┘
```

---

## Pipeline Steps

### Step 1 — Scraping (✅ 524 products/day)

| Spider | Site | Method | Products |
|---|---|---|---|
| `jumia_spider.py` | jumia.ma | Scrapy (HTTP) | ~354 |
| `electroplanet_spider.py` | electroplanet.ma | Selenium + Remote Chrome | ~51 |
| `amazon_spider.py` | amazon.fr | Selenium + Remote Chrome | ~119 |

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
  "scraped_at": "2026-04-22T19:34:59"
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
scrape_jumia ──┐
               ├──► scrape_amazon ──► setup_bigtable ──► write_to_bigtable ──► validate_output ──► generate_report
scrape_electro ┘
```

- Jumia and Electroplanet scrapers run **in parallel**
- Amazon runs after both complete
- Bigtable setup is idempotent (safe to re-run)
- Validation checks all 3 JSON files exist and are non-empty
- Final report logs product counts per source

Sample report output:
```
========================================
RAPPORT SCRAPING — 2026-04-22 19:34
========================================
  amazon               : 119 produits
  electroplanet        : 51 produits
  jumia                : 354 produits
  TOTAL                : 524 produits
========================================
```

---

### Step 4 — Bigtable Storage (✅ operational)

**Schema:**

| Column Family | Columns | Purpose |
|---|---|---|
| `price_cf` | price, old_price, currency, discount | Pricing data |
| `metadata_cf` | name, brand, model, url, rating, reviews, scraped_at | Product metadata |
| `agg_cf` | *(populated by dbt — Step 5)* | Statistical aggregations |

**Row key design:** `{site}#{brand}#{model}#{timestamp}`

Examples:
```
electroplanet#samsung#s26_ultra#20260422193459
jumia#apple#iphone_16_pro#20260422193501
amazon#samsung#s25#20260422193512
```

This layout enables efficient prefix scans: scan all Samsung products, all prices from a specific site, or all entries for a given model.

---

## Stack

| Component | Technology |
|---|---|
| Scraping (static) | Python + Scrapy |
| Scraping (dynamic) | Python + Selenium 4 + Remote Chrome |
| Browser automation | Selenium Grid (Docker) |
| Streaming ingestion | Apache NiFi |
| Orchestration | Apache Airflow 2.9 (SequentialExecutor + SQLite) |
| Storage | Google Cloud Bigtable (emulator in Docker) |
| Containerization | Docker + Docker Compose |

---

## Project Structure

```
price-intelligence/
├── price_intelligence/
│   ├── spiders/
│   │   ├── jumia_spider.py          # Scrapy spider — jumia.ma
│   │   ├── electroplanet_spider.py  # Selenium spider — electroplanet.ma
│   │   └── amazon_spider.py         # Selenium spider — amazon.fr
│   ├── pipelines.py                 # Scrapy CSV + JSON pipelines
│   ├── settings.py                  # Scrapy settings
│   ├── bigtable_setup.py            # Creates Bigtable instance/table/families
│   └── bigtable_writer.py           # Reads JSON output → writes to Bigtable
├── airflow/
│   ├── dags/
│   │   └── price_intelligence_dag.py  # Main DAG definition
│   ├── Dockerfile                     # Airflow + pip deps
│   └── docker-compose.yml             # Airflow + Selenium + Bigtable emulator
├── nifi/
│   └── NiFi_Flow.json               # NiFi flow definition
├── output/                          # Generated — gitignored
│   ├── jumia.json / .csv
│   ├── electroplanet.json / .csv
│   └── amazon.json / .csv
└── scrapy.cfg
```

---

## Running Locally

**Prerequisites:** Docker Desktop, Python 3.12

```bash
# Start all services
cd airflow
docker-compose up -d --build

# Airflow UI → http://localhost:8080  (admin / admin123)
# Selenium Grid → http://localhost:4444
# Bigtable emulator → internal (bigtable:8086)
```

**Trigger a manual run** from the Airflow UI: DAG `price_intelligence_daily` → Trigger DAG.

**Run a spider directly (outside Airflow):**
```bash
# Jumia (Scrapy)
cd price_intelligence
scrapy crawl jumia

# Electroplanet / Amazon (Selenium — needs Selenium Grid running)
SELENIUM_REMOTE_URL=http://localhost:4444 python price_intelligence/spiders/electroplanet_spider.py
```

**Run Bigtable setup/writer directly:**
```bash
BIGTABLE_EMULATOR_HOST=localhost:8086 python price_intelligence/bigtable_setup.py
BIGTABLE_EMULATOR_HOST=localhost:8086 python price_intelligence/bigtable_writer.py
```

---

## Roadmap

| Step | Status |
|---|---|
| Step 1 — Scraping (Jumia, Electroplanet, Amazon) | ✅ 524 products/day |
| Step 2 — NiFi streaming | ✅ 1.55 MB ingested |
| Step 3 — Airflow orchestration | ✅ DAG operational |
| Step 4 — Bigtable storage | ✅ Emulator running, rows written |
| Step 5 — dbt transformations | ⏳ |