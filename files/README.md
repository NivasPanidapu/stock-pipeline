# 📈 Stock Market Data Pipeline

An end-to-end data engineering pipeline using **Yahoo Finance**, **Python**,
**PostgreSQL**, **Apache Airflow**, and **Docker**.

```
Yahoo Finance API
      │
      ▼
  [FETCH]  fetch_stock_data.py   →  raw_stock_prices table
      │
      ▼
  [CLEAN]  clean_stock_data.py   →  cleaned_stock_prices table
      │
      ▼
  [LOG]    DAG audit record      →  pipeline_runs table
```

---

## Project Structure

```
stock_pipeline/
├── docker-compose.yml          # Spins up all services
├── dags/
│   └── stock_pipeline_dag.py   # Airflow DAG (the workflow definition)
├── scripts/
│   ├── fetch_stock_data.py     # Step 1: download from Yahoo Finance
│   └── clean_stock_data.py     # Step 2: validate & clean
├── sql/
│   └── init.sql                # Database schema (runs once on first start)
└── logs/                       # Airflow task logs (auto-populated)
```

---

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed
- At least 4 GB RAM available to Docker
- Ports **8080** and **5433** free on your machine

---

## Quick Start

### 1. Start all services

```bash
cd stock_pipeline
docker compose up -d
```

This starts 5 containers:
| Container | Purpose |
|---|---|
| `airflow-init` | One-time setup (creates DB, admin user) — then exits |
| `airflow-webserver` | Airflow UI at http://localhost:8080 |
| `airflow-scheduler` | Triggers DAGs on schedule |
| `airflow-db` | Postgres for Airflow's own metadata |
| `stock-db` | Postgres for your stock data (port 5433) |

First startup takes ~3 minutes while Docker pulls images.

### 2. Check all containers are running

```bash
docker compose ps
```

You should see `airflow-webserver`, `airflow-scheduler`, `airflow-db`,
and `stock-db` all showing `Up`.

### 3. Open the Airflow UI

Go to **http://localhost:8080**

- Username: `admin`
- Password: `admin`

### 4. Enable and trigger the pipeline

1. In the Airflow UI, find the DAG named **`stock_market_pipeline`**
2. Toggle the switch on the left to **enable** it
3. Click the **▶ Trigger DAG** button (play icon) to run it immediately
4. Click on the DAG name → **Graph** tab to watch tasks execute in real time

---

## Understanding the Pipeline

### What is a DAG?
A **DAG** (Directed Acyclic Graph) is how Airflow represents a workflow.
It's a set of tasks connected by dependencies — tasks run in order, and
Airflow tracks which ones succeed or fail.

### The 5 tasks in this DAG:
```
start → fetch_stock_data → clean_stock_data → log_pipeline_run → end
```

| Task | What it does |
|---|---|
| `start` | Visual placeholder (no code) |
| `fetch_stock_data` | Downloads OHLCV data from Yahoo Finance |
| `clean_stock_data` | Runs quality checks, calculates daily returns |
| `log_pipeline_run` | Writes audit record with row counts & status |
| `end` | Visual placeholder (no code) |

### Schedule
The DAG is scheduled with the cron expression `0 18 * * 1-5`:
- `0 18` = 6:00 PM UTC
- `* *` = every month, every day of month
- `1-5` = Monday through Friday

To run every hour instead: change schedule to `"@hourly"` or `"0 * * * *"`

---

## Querying Your Data

Connect to the stock database:

```bash
# Using Docker
docker exec -it stock_pipeline-stock-db-1 psql -U stocks_user -d stocks

# Using a local psql client
psql -h localhost -p 5433 -U stocks_user -d stocks
# Password: stocks_password
```

### Useful queries:

```sql
-- Latest price for each stock
SELECT * FROM latest_prices;

-- 30-day price history for Apple
SELECT price_date, close_price, daily_return
FROM cleaned_stock_prices
WHERE ticker = 'AAPL'
ORDER BY price_date DESC
LIMIT 30;

-- Check pipeline audit log
SELECT * FROM pipeline_runs ORDER BY started_at DESC LIMIT 10;

-- How many rows per ticker?
SELECT ticker, COUNT(*) as trading_days
FROM cleaned_stock_prices
GROUP BY ticker
ORDER BY ticker;

-- Find the best and worst trading days
SELECT ticker, price_date, daily_return
FROM cleaned_stock_prices
ORDER BY daily_return DESC
LIMIT 10;
```

---

## Customization

### Change which stocks to track
Edit the `TICKERS` list in `dags/stock_pipeline_dag.py`:

```python
TICKERS = ["AAPL", "MSFT", "GOOGL"]  # Add any Yahoo Finance ticker symbol
```

### Change the schedule
Edit the `schedule` parameter in the DAG definition:

```python
schedule="0 18 * * 1-5"    # weekdays at 6 PM UTC (current)
schedule="@daily"           # every day at midnight
schedule="0 */6 * * *"      # every 6 hours
schedule="@hourly"          # every hour
```

### Add more quality checks
In `scripts/clean_stock_data.py`, add a new function and register it:

```python
def check_52_week_range(row):
    """Example: reject prices above $10,000 (catches data errors)."""
    return row["close_price"] < 10000

ALL_CHECKS = [
    ...existing checks...,
    ("52_week_range", check_52_week_range),
]
```

---

## Stopping the Pipeline

```bash
# Stop all containers (keeps data)
docker compose stop

# Stop and remove containers (keeps data in volumes)
docker compose down

# Stop and remove EVERYTHING including stored data
docker compose down -v
```

---

## Troubleshooting

**Containers won't start:**
```bash
docker compose logs airflow-init
```

**DAG not showing in Airflow UI:**
- Wait 30 seconds (scheduler scans for new DAGs every 30s)
- Check for Python syntax errors: `docker compose logs airflow-scheduler`

**Task failing with database error:**
```bash
docker compose logs stock-db
```

**Want to see task logs:**
- Click any task in the Airflow Graph view → **Logs** button

---

## Architecture Concepts

| Concept | What it means here |
|---|---|
| **ETL** | Extract (Yahoo Finance) → Transform (clean) → Load (Postgres) |
| **Idempotency** | Re-running the pipeline doesn't create duplicate rows |
| **Raw layer** | Keep original data before cleaning (for auditability) |
| **Cleaned layer** | Validated data ready for analysis/reporting |
| **Orchestration** | Airflow manages task order, retries, scheduling |
| **XCom** | Airflow mechanism to pass data between tasks |
