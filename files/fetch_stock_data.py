"""
fetch_stock_data.py
===================
Step 1 of the pipeline: Download stock market data from Yahoo Finance
and insert it into the raw_stock_prices table.

How it works:
  - Uses the `yfinance` library (a free, unofficial Yahoo Finance API wrapper)
  - Downloads OHLCV data for a configurable list of tickers
  - Inserts rows into Postgres, skipping any that already exist (idempotent)
  - Returns a dict with stats so the DAG can log results
"""

import os
import logging
from datetime import datetime, timedelta

import yfinance as yf
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# ── Logging setup ─────────────────────────────────────────
# Airflow captures these logs and shows them in the UI task view.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)


# ── Database connection helper ────────────────────────────
def get_db_connection():
    """
    Connect to the stock-db Postgres container.
    Reads credentials from environment variables (set in docker-compose.yml).
    """
    return psycopg2.connect(
        host=os.getenv("STOCK_DB_HOST", "localhost"),
        port=int(os.getenv("STOCK_DB_PORT", 5432)),
        dbname=os.getenv("STOCK_DB_NAME", "stocks"),
        user=os.getenv("STOCK_DB_USER", "stocks_user"),
        password=os.getenv("STOCK_DB_PASSWORD", "stocks_password"),
    )


# ── Main fetch function ───────────────────────────────────
def fetch_and_store(
    tickers: list[str],
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict:
    """
    Download stock data and store it in raw_stock_prices.

    Args:
        tickers:    List of ticker symbols, e.g. ['AAPL', 'MSFT', 'GOOGL']
        start_date: 'YYYY-MM-DD' string (defaults to 7 days ago)
        end_date:   'YYYY-MM-DD' string (defaults to today)

    Returns:
        dict with 'rows_fetched', 'tickers_ok', 'tickers_failed'
    """
    # Default to the past 7 days if no dates given
    if end_date is None:
        end_date = datetime.today().strftime("%Y-%m-%d")
    if start_date is None:
        start_date = (datetime.today() - timedelta(days=7)).strftime("%Y-%m-%d")

    log.info(f"Fetching data for {tickers} from {start_date} to {end_date}")

    rows_fetched = 0
    tickers_ok = []
    tickers_failed = []

    conn = get_db_connection()

    try:
        for ticker in tickers:
            try:
                # ── Download from Yahoo Finance ────────────────
                # yf.download returns a pandas DataFrame indexed by Date.
                # auto_adjust=True adjusts OHLC prices for splits & dividends.
                df = yf.download(
                    ticker,
                    start=start_date,
                    end=end_date,
                    auto_adjust=False,   # keep both Close and Adj Close
                    progress=False,      # suppress progress bar in logs
                )

                if df.empty:
                    log.warning(f"No data returned for {ticker}")
                    tickers_failed.append(ticker)
                    continue

                # ── Flatten multi-level columns if present ─────
                # yfinance sometimes returns MultiIndex columns like
                # ('Close', 'AAPL') — we flatten to plain 'Close'.
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = df.columns.get_level_values(0)

                # ── Prepare rows for bulk insert ───────────────
                records = []
                for date, row in df.iterrows():
                    records.append((
                        ticker,
                        date.date(),         # trading date (no time component)
                        float(row.get("Open", 0) or 0),
                        float(row.get("High", 0) or 0),
                        float(row.get("Low", 0) or 0),
                        float(row.get("Close", 0) or 0),
                        float(row.get("Adj Close", 0) or 0),
                        int(row.get("Volume", 0) or 0),
                    ))

                # ── Bulk insert with conflict handling ─────────
                # ON CONFLICT DO NOTHING = if (ticker, date) already exists,
                # skip it. This makes the task safe to re-run (idempotent).
                insert_sql = """
                    INSERT INTO raw_stock_prices
                        (ticker, price_date, open_price, high_price, low_price,
                         close_price, adj_close_price, volume)
                    VALUES %s
                    ON CONFLICT (ticker, price_date) DO NOTHING
                """
                with conn.cursor() as cur:
                    execute_values(cur, insert_sql, records)
                conn.commit()

                log.info(f"✓ {ticker}: inserted {len(records)} rows")
                rows_fetched += len(records)
                tickers_ok.append(ticker)

            except Exception as e:
                log.error(f"✗ Failed to fetch {ticker}: {e}")
                tickers_failed.append(ticker)
                conn.rollback()   # rollback only this ticker's transaction

    finally:
        conn.close()

    return {
        "rows_fetched": rows_fetched,
        "tickers_ok": tickers_ok,
        "tickers_failed": tickers_failed,
    }


# ── Standalone test ───────────────────────────────────────
# Run this directly with: python fetch_stock_data.py
if __name__ == "__main__":
    result = fetch_and_store(
        tickers=["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA"],
        start_date="2024-01-01",
    )
    print(result)
