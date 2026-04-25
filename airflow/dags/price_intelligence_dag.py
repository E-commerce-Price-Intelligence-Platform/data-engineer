from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import os
import logging

logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────
default_args = {
    "owner":            "data_engineer",
    "retries":          1,
    "retry_delay":      timedelta(minutes=2),
    "email_on_failure": False,
}

SPIDERS_PATH  = "/opt/airflow/price_intelligence/price_intelligence/spiders"
OUTPUT_DIR    = "/opt/airflow/price_intelligence/output"
PROJECT_ROOT  = "/opt/airflow/price_intelligence"
BIGTABLE_PATH = "/opt/airflow/price_intelligence/price_intelligence/bigtable"
BQ_LOADER     = "/opt/airflow/price_intelligence/price_intelligence/bigquery_loader.py"
DBT_DIR       = "/opt/airflow/price_intelligence/dbt"


def _run(cmd, cwd=PROJECT_ROOT, extra_env=None):
    """Run a command and stream its output line-by-line into the Airflow log."""
    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        cwd=cwd,
        env=env,
    )
    try:
        for line in iter(process.stdout.readline, ""):
            logger.info(line.rstrip())
    except Exception:
        process.kill()
        process.wait()
        raise
    process.stdout.close()
    process.wait()
    if process.returncode != 0:
        raise Exception(f"Command failed (exit {process.returncode}): {' '.join(cmd)}")


# ── Scrapers ──────────────────────────────────────────────
def run_jumia_spider():
    _run(["python", "-m", "scrapy", "crawl", "jumia"])
    logger.info("✅ Jumia spider terminé")


def run_electroplanet_spider():
    _run(["python", "-u", f"{SPIDERS_PATH}/electroplanet_spider.py"])
    logger.info("✅ Electroplanet spider terminé")


def run_amazon_spider():
    _run(["python", "-u", f"{SPIDERS_PATH}/amazon_spider.py"])
    logger.info("✅ Amazon spider terminé")


# ── Bigtable ──────────────────────────────────────────────
def setup_bigtable():
    _run(["python", f"{BIGTABLE_PATH}/bigtable_setup.py"])
    logger.info("✅ Bigtable setup done")


def write_to_bigtable():
    _run(["python", f"{BIGTABLE_PATH}/bigtable_writer.py"], cwd=OUTPUT_DIR)
    logger.info("✅ Bigtable write done")


# ── BigQuery + dbt ────────────────────────────────────────
def load_to_bigquery():
    _run(
        ["python", BQ_LOADER],
        cwd=OUTPUT_DIR,
        extra_env={"OUTPUT_DIR": OUTPUT_DIR},
    )
    logger.info("✅ BigQuery load done")


def dbt_deps():
    _run(
        ["dbt", "deps", "--project-dir", DBT_DIR, "--profiles-dir", DBT_DIR],
        cwd=DBT_DIR,
    )
    logger.info("✅ dbt deps installed")


def dbt_run():
    _run(
        ["dbt", "run", "--project-dir", DBT_DIR, "--profiles-dir", DBT_DIR],
        cwd=DBT_DIR,
    )
    logger.info("✅ dbt run complete")


def dbt_test():
    _run(
        ["dbt", "test", "--project-dir", DBT_DIR, "--profiles-dir", DBT_DIR],
        cwd=DBT_DIR,
    )
    logger.info("✅ dbt test complete")


# ── Validation & report ───────────────────────────────────
def validate_output():
    import glob
    files = glob.glob(f"{OUTPUT_DIR}/*.json")
    if not files:
        raise Exception("Aucun fichier JSON trouvé dans output/")
    for f in files:
        size = os.path.getsize(f)
        logger.info(f"  {os.path.basename(f)} — {size} bytes")
        if size < 10:
            raise Exception(f"Fichier trop petit : {f}")
    logger.info(f"✅ Validation OK — {len(files)} fichiers trouvés")


def generate_report():
    import json, glob
    total   = 0
    summary = {}
    for f in glob.glob(f"{OUTPUT_DIR}/*.json"):
        try:
            with open(f) as fp:
                data  = json.load(fp)
                count = len(data)
                site  = os.path.basename(f).replace(".json", "")
                summary[site] = count
                total += count
        except Exception as e:
            logger.warning(f"  Erreur lecture {f}: {e}")
    logger.info("=" * 40)
    logger.info(f"RAPPORT SCRAPING — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    logger.info("=" * 40)
    for site, count in summary.items():
        logger.info(f"  {site:20} : {count} produits")
    logger.info(f"  {'TOTAL':20} : {total} produits")
    logger.info("=" * 40)


# ── DAG ───────────────────────────────────────────────────
with DAG(
    dag_id="price_intelligence_daily",
    description="Scraping quotidien des prix smartphones",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="0 6 * * *",
    catchup=False,
    tags=["scraping", "price", "smartphones", "dbt"],
) as dag:

    task_jumia = PythonOperator(
        task_id="scrape_jumia",
        python_callable=run_jumia_spider,
        execution_timeout=timedelta(minutes=10),
    )

    task_electro = PythonOperator(
        task_id="scrape_electroplanet",
        python_callable=run_electroplanet_spider,
        execution_timeout=timedelta(minutes=15),
    )

    task_amazon = PythonOperator(
        task_id="scrape_amazon",
        python_callable=run_amazon_spider,
        execution_timeout=timedelta(minutes=10),
    )

    task_bigtable_setup = PythonOperator(
        task_id="setup_bigtable",
        python_callable=setup_bigtable,
    )

    task_bigtable_write = PythonOperator(
        task_id="write_to_bigtable",
        python_callable=write_to_bigtable,
        execution_timeout=timedelta(minutes=5),
    )

    task_load_bq = PythonOperator(
        task_id="load_to_bigquery",
        python_callable=load_to_bigquery,
        execution_timeout=timedelta(minutes=10),
    )

    task_dbt_deps = PythonOperator(
        task_id="dbt_deps",
        python_callable=dbt_deps,
        execution_timeout=timedelta(minutes=5),
    )

    task_dbt_run = PythonOperator(
        task_id="dbt_run",
        python_callable=dbt_run,
        execution_timeout=timedelta(minutes=15),
    )

    task_validate = PythonOperator(
        task_id="validate_output",
        python_callable=validate_output,
    )

    task_report = PythonOperator(
        task_id="generate_report",
        python_callable=generate_report,
    )

    # ── Pipeline order ────────────────────────────────────
    # Jumia + Electroplanet scrape in parallel → Amazon → fan out to Bigtable and BigQuery
    # dbt_test is NOT in the daily pipeline — run manually via:
    #   docker exec -it airflow bash -c "dbt test --project-dir ... --profiles-dir ..."

    [task_jumia, task_electro] >> task_amazon

    task_amazon >> task_bigtable_setup >> task_bigtable_write
    task_amazon >> task_load_bq >> task_dbt_deps >> task_dbt_run

    [task_bigtable_write, task_dbt_run] >> task_validate >> task_report
