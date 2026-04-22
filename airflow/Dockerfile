FROM apache/airflow:2.9.0

USER root
RUN apt-get update && apt-get install -y socat && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir \
    scrapy \
    selenium \
    webdriver-manager \
    google-cloud-bigtable
