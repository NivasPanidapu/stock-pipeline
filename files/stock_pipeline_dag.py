"""
stock_pipeline_dag.py
======================
The Airflow DAG (Directed Acyclic Graph) that orchestrates the full
stock data pipeline on a regular schedule.

What is a DAG?
  A DAG is a workflow defined as a set of tasks with dependencies.
  Airflow reads this file and turns it into a visual pipeline in the UI.

Pipeline structure:
  start
    │
    ▼
  fetch_stock_data        ← downloads from Yahoo Finance → raw_stock_prices
    │
    ▼
  clean_stock_data        ← validates & cleans → cleaned_stock_prices
    │
    ▼
  log_pipeline_run        ← writes audit record to pipeline_runs
    │
    ▼
  end

Schedule: Every weekday at 6:00 PM UTC (after US markets close at ~9:30 PM UTC)
"""

import sys
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

# Add the scripts directory to Python path so we can import our modules
sys.path.insert(0, "/opt/airflow/scripts")

log = logging.getLogger(__name__)

# ── Tickers to track ──────────────────────────────────────
# Add or remove any ticker symbols you want to follow.
TICKERS = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "TSLA", "META"]


# ── Default arguments ──────────────────────────────────────
# These apply to every task in the DAG unless overridden per-task.
default_args = {
    "owner": "data-team",

    # If the pipeline fails, retry once after 5 minutes
    "retries": 1,
    "retry_delay": timedelta(minutes=5),

    # Send email on failure (configure SMTP in airflow.cfg to enable)
    "email_on_failure": False,
    "email_on_retry": False,

    # Don't backfill runs from before today when the DAG is first turned on
    "depends_on_past": False,
}


# ── Task functions ────────────────────────────────────────
# Each function below becomes one task (box) in the Airflow UI.
# Airflow calls these functions at the scheduled time.

def task_fetch_stock_data(**context):
    """
    Task 1: Download stock prices from Yahoo Finance.

    `context` is injected by Airflow and contains metadata like
    the logical run date (data_interval_end = market close of that day).
    """
    from fetch_stock_data import fetch_and_store

    # Use the Airflow run's logical date as our end date.
    # This ensures backfills work correctly too.
    logical_date = context["data_interval_end"]
    end_date = logical_date.strftime("%Y-%m-%d")
    start_date = (logical_date - timedelta(days=3)).strftime("%Y-%m-%d")
    # We fetch 3 days to handle weekends/holidays where there's no trading data.

    log.info(f"Fetching {TICKERS} from {start_date} to {end_date}")
    result = fetch_and_store(
        tickers=TICKERS,
        start_date=start_date,
        end_date=end_date,
    )

    log.info(f"Fetch complete: {result}")

    # Push result to XCom so downstream tasks can read it.
    # XCom = "Cross-Communication" — Airflow's way of passing data between tasks.
    context["ti"].xcom_push(key="fetch_result", value=result)

    # If all tickers failed, fail the task
    if result["rows_fetched"] == 0 and not result["tickers_ok"]:
        raise ValueError("No data fetched for any ticker!")


def task_clean_stock_data(**context):
    """
    Task 2: Validate and clean the raw data.
    Only runs after task_fetch_stock_data succeeds.
    """
    from clean_stock_data import clean_and_store

    result = clean_and_store(tickers=TICKERS)
    log.info(f"Clean complete: {result}")

    context["ti"].xcom_push(key="clean_result", value=result)

    if result["rows_rejected"] > 0:
        log.warning(
            f"{result['rows_rejected']} rows rejected. "
            f"Reasons: {result['rejection_reasons']}"
        )


def task_log_pipeline_run(**context):
    """
    Task 3: Write an audit record to the pipeline_runs table.
    Pulls results from the previous tasks via XCom.
    """
    import os
    import psycopg2

    ti = context["ti"]
    fetch_result = ti.xcom_pull(task_ids="fetch_stock_data", key="fetch_result") or {}
    clean_result = ti.xcom_pull(task_ids="clean_stock_data", key="clean_result") or {}

    rows_fetched = fetch_result.get("rows_fetched", 0)
    rows_cleaned = clean_result.get("rows_cleaned", 0)
    rows_rejected = clean_result.get("rows_rejected", 0)

    status = "success"
    if rows_rejected > 0 and rows_cleaned == 0:
        status = "failed"
    elif rows_rejected > 0:
        status = "partial"

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
                    (run_date, tickers, rows_fetched, rows_cleaned,
                     rows_rejected, status, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, NOW())
            """, (
                context["ds"],      # logical date as YYYY-MM-DD string
                TICKERS,
                rows_fetched,
                rows_cleaned,
                rows_rejected,
                status,
            ))
        conn.commit()
        log.info(f"Pipeline run logged: status={status}, "
                 f"fetched={rows_fetched}, cleaned={rows_cleaned}, "
                 f"rejected={rows_rejected}")
    finally:
        conn.close()


# ── DAG definition ─────────────────────────────────────────
with DAG(
    dag_id="stock_market_pipeline",      # Unique ID shown in Airflow UI
    description="Fetch, clean, and store daily stock market data",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),     # DAG becomes active from this date
    schedule="0 18 * * 1-5",            # 6 PM UTC, Mon–Fri (cron format)
    catchup=False,                       # Don't run missed historical runs
    tags=["stocks", "finance", "etl"],   # Tags for filtering in the UI
    max_active_runs=1,                   # Only one run at a time
) as dag:

    # ── Sentinel tasks (for visual clarity in the UI) ──────
    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end")

    # ── Real tasks ─────────────────────────────────────────
    fetch = PythonOperator(
        task_id="fetch_stock_data",
        python_callable=task_fetch_stock_data,
    )

    clean = PythonOperator(
        task_id="clean_stock_data",
        python_callable=task_clean_stock_data,
    )

    log_run = PythonOperator(
        task_id="log_pipeline_run",
        python_callable=task_log_pipeline_run,
    )

    # ── Task dependencies ──────────────────────────────────
    # The >> operator means "then". This defines the execution order.
    #   start → fetch → clean → log_run → end
    start >> fetch >> clean >> log_run >> end
