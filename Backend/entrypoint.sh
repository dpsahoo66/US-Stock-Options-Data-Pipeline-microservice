#!/bin/bash
set -e

airflow db migrate
airflow users create \
    --username airflow \
    --firstname Airflow \
    --lastname Admin \
    --role Admin \
    --email hussainathar96@gmail.com \
    --password airflow || true
airflow webserver & airflow scheduler