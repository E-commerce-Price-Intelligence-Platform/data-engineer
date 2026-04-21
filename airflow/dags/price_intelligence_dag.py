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

SPIDERS_PATH = "/opt/airflow/price_intelligence/price_intelligence/spiders"
OUTPUT_DIR   = "/opt/airflow/price_intelligence/output"
PROJECT_ROOT = "/opt/airflow/price_intelligence"


def _run(cmd, cwd=PROJECT_ROOT):
    """Run a command and stream its output line-by-line into the Airflow log."""
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        cwd=cwd,
    )
    for line in iter(process.stdout.readline, ""):
        logger.info(line.rstrip())
    process.stdout.close()
    process.wait()
    if process.returncode != 0:
        raise Exception(f"Command failed (exit {process.returncode}): {' '.join(cmd)}")


# ── Fonctions Python ──────────────────────────────────────
def run_jumia_spider():
    _run(["python", "-m", "scrapy", "crawl", "jumia"])
    logger.info("✅ Jumia spider terminé")


def run_electroplanet_spider():
    _run(["python", "-u", f"{SPIDERS_PATH}/electroplanet_spider.py"])
    logger.info("✅ Electroplanet spider terminé")


def run_amazon_spider():
    _run(["python", "-u", f"{SPIDERS_PATH}/amazon_spider.py"])
    logger.info("✅ Amazon spider terminé")


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
    tags=["scraping", "price", "smartphones"],
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

    task_validate = PythonOperator(
        task_id="validate_output",
        python_callable=validate_output,
    )

    task_report = PythonOperator(
        task_id="generate_report",
        python_callable=generate_report,
    )

    [task_jumia, task_electro] >> task_amazon >> task_validate >> task_report
