"""Airflow DAG: stock_fetch_pipeline

Runs `scripts/fetch_stock.py` to fetch latest quotes and write to Postgres.
Environment variables are passed through from the service environment to keep
configuration centralized in `.env`.
"""

import os
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

schedule = os.getenv("PIPELINE_SCHEDULE_CRON", "0 * * * *")  # default hourly

with DAG(
    dag_id="stock_fetch_pipeline",
    default_args=DEFAULT_ARGS,
    description="Fetch stock quotes from Alpha Vantage and store in Postgres",
    schedule_interval=schedule,
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["stocks", "example"],
) as dag:

    # Run the fetch script with Python (script is in /opt/airflow/scripts)
    fetch_stocks = BashOperator(
        task_id="fetch_stocks",
        bash_command='python /opt/airflow/scripts/fetch_stock.py --symbols "$STOCK_SYMBOLS"',
        env={
            # allow override in DAG runtime if needed
            "ALPHAVANTAGE_API_KEY": os.getenv("ALPHAVANTAGE_API_KEY"),
            "STOCK_SYMBOLS": os.getenv("STOCK_SYMBOLS"),
            "POSTGRES_HOST": os.getenv("POSTGRES_HOST"),
            "POSTGRES_PORT": os.getenv("POSTGRES_PORT"),
            "POSTGRES_DB": os.getenv("POSTGRES_DB"),
            "POSTGRES_USER": os.getenv("POSTGRES_USER"),
            "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD"),
        },
    )

    fetch_stocks
