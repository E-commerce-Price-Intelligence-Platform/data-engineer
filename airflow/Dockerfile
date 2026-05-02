FROM apache/airflow:2.9.0

USER airflow

RUN pip install --no-cache-dir \
    scrapy \
    selenium \
    webdriver-manager \
    google-cloud-bigtable \
    google-cloud-bigquery \
    "dbt-core==1.8.7" \
    "dbt-bigquery==1.8.2"

# dbt packages are installed at runtime (dbt project is mounted as a volume).
# Run this once after first deploy:
#   docker exec -it airflow dbt deps \
#     --project-dir /opt/airflow/price_intelligence/dbt \
#     --profiles-dir /opt/airflow/price_intelligence/dbt
