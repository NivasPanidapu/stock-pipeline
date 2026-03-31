import sys
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

sys.path.insert(0, "/opt/airflow/scripts")

log = logging.getLogger(__name__)

TICKERS = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "TSLA", "META"]

default_args = {
    "owner": "data-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def task_fetch_stock_data(**context):
    from fetch_stock_data import fetch_and_store

    logical_date = context["data_interval_end"]
    end_date = logical_date.strftime("%Y-%m-%d")
    start_date = (logical_date - timedelta(days=90)).strftime("%Y-%m-%d")

    result = fetch_and_store(tickers=TICKERS, start_date=start_date, end_date=end_date)
    log.info(f"Fetch result: {result}")

    context["ti"].xcom_push(key="fetch_result", value=result)

    if result["rows_fetched"] == 0 and not result["tickers_ok"]:
        raise ValueError("No data fetched for any ticker!")


def task_clean_stock_data(**context):
    from clean_stock_data import clean_and_store

    result = clean_and_store(tickers=TICKERS)
    log.info(f"Clean result: {result}")

    context["ti"].xcom_push(key="clean_result", value=result)


def task_log_pipeline_run(**context):
    import os
    import psycopg2

    ti = context["ti"]
    fetch_result = ti.xcom_pull(task_ids="fetch_stock_data", key="fetch_result") or {}
    clean_result = ti.xcom_pull(task_ids="clean_stock_data", key="clean_result") or {}

    rows_fetched  = fetch_result.get("rows_fetched", 0)
    rows_cleaned  = clean_result.get("rows_cleaned", 0)
    rows_rejected = clean_result.get("rows_rejected", 0)

    if rows_rejected > 0 and rows_cleaned == 0:
        status = "failed"
    elif rows_rejected > 0:
        status = "partial"
    else:
        status = "success"

    conn = psycopg2.connect(
        host=os.getenv("STOCK_DB_HOST", "stock-db"),
        port=int(os.getenv("STOCK_DB_PORT", 5432)),
        dbname=os.getenv("STOCK_DB_NAME", "stocks"),
        user=os.getenv("STOCK_DB_USER", "stocks_user"),
        password=os.getenv("STOCK_DB_PASSWORD", "stocks_password"),
    )
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO pipeline_runs
                    (run_date, tickers, rows_fetched, rows_cleaned, rows_rejected,
                     status, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, NOW())
            """, (context["ds"], TICKERS, rows_fetched, rows_cleaned, rows_rejected, status))
        conn.commit()
        log.info(f"Run logged: status={status}, fetched={rows_fetched}, cleaned={rows_cleaned}")
    finally:
        conn.close()


with DAG(
    dag_id="stock_market_pipeline",
    description="Fetch, clean, and store daily stock market data",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="0 18 * * 1-5",
    catchup=False,
    tags=["stocks", "finance", "etl"],
    max_active_runs=1,
) as dag:

    start   = EmptyOperator(task_id="start")
    end     = EmptyOperator(task_id="end")

    fetch   = PythonOperator(task_id="fetch_stock_data",   python_callable=task_fetch_stock_data)
    clean   = PythonOperator(task_id="clean_stock_data",   python_callable=task_clean_stock_data)
    log_run = PythonOperator(task_id="log_pipeline_run",   python_callable=task_log_pipeline_run)

    start >> fetch >> clean >> log_run >> end
