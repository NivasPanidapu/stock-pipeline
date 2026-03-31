"""
clean_stock_data.py
===================
Step 2 of the pipeline: Validate and clean the raw stock data,
then write approved rows to the cleaned_stock_prices table.

Quality checks performed:
  1. No NULL values in critical price columns
  2. Prices must be > 0
  3. High must be >= Low (basic sanity check)
  4. Volume must be > 0 (zero volume = no trading = suspicious)
  5. Close price must be within ±50% of Open (extreme moves flagged)
  6. Calculate daily_return (% change from previous close)
"""

import os
import logging
from datetime import datetime

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)


def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("STOCK_DB_HOST", "localhost"),
        port=int(os.getenv("STOCK_DB_PORT", 5432)),
        dbname=os.getenv("STOCK_DB_NAME", "stocks"),
        user=os.getenv("STOCK_DB_USER", "stocks_user"),
        password=os.getenv("STOCK_DB_PASSWORD", "stocks_password"),
    )


# ── Quality check functions ───────────────────────────────
# Each returns True if the row PASSES (is clean), False if it fails.

def check_no_nulls(row: pd.Series) -> bool:
    """Reject rows where any price column is null/NaN."""
    critical = ["open_price", "high_price", "low_price", "close_price",
                "adj_close_price", "volume"]
    return not row[critical].isnull().any()

def check_positive_prices(row: pd.Series) -> bool:
    """All prices must be strictly positive."""
    return (
        row["open_price"] > 0 and
        row["high_price"] > 0 and
        row["low_price"] > 0 and
        row["close_price"] > 0 and
        row["adj_close_price"] > 0
    )

def check_high_low(row: pd.Series) -> bool:
    """Daily high must be >= daily low."""
    return row["high_price"] >= row["low_price"]

def check_positive_volume(row: pd.Series) -> bool:
    """Volume must be greater than zero."""
    return row["volume"] > 0

def check_extreme_move(row: pd.Series, threshold: float = 0.5) -> bool:
    """
    Flag extreme intraday moves.
    A close that is 50%+ away from open likely indicates bad data.
    """
    if row["open_price"] == 0:
        return False
    pct_change = abs(row["close_price"] - row["open_price"]) / row["open_price"]
    return pct_change <= threshold


ALL_CHECKS = [
    ("no_nulls",        check_no_nulls),
    ("positive_prices", check_positive_prices),
    ("high_gte_low",    check_high_low),
    ("positive_volume", check_positive_volume),
    ("extreme_move",    check_extreme_move),
]


# ── Main clean function ───────────────────────────────────
def clean_and_store(tickers: list[str] | None = None) -> dict:
    """
    Read from raw_stock_prices, apply quality checks,
    calculate daily_return, and write to cleaned_stock_prices.

    Args:
        tickers: Optional list to process specific tickers only.
                 If None, processes all tickers in raw table.

    Returns:
        dict with rows_cleaned, rows_rejected, rejection_reasons
    """
    conn = get_db_connection()
    rows_cleaned = 0
    rows_rejected = 0
    rejection_reasons: dict[str, int] = {check: 0 for check, _ in ALL_CHECKS}

    try:
        # ── 1. Load raw data ───────────────────────────────
        # Only fetch rows NOT already in the cleaned table (avoid re-processing)
        ticker_filter = ""
        params = []
        if tickers:
            ticker_filter = "AND r.ticker = ANY(%s)"
            params = [tickers]

        query = f"""
            SELECT
                r.ticker,
                r.price_date,
                r.open_price,
                r.high_price,
                r.low_price,
                r.close_price,
                r.adj_close_price,
                r.volume
            FROM raw_stock_prices r
            LEFT JOIN cleaned_stock_prices c
                ON r.ticker = c.ticker AND r.price_date = c.price_date
            WHERE c.id IS NULL    -- only unprocessed rows
            {ticker_filter}
            ORDER BY r.ticker, r.price_date
        """

        with conn.cursor() as cur:
            cur.execute(query, params or None)
            rows = cur.fetchall()
            col_names = [desc[0] for desc in cur.description]

        if not rows:
            log.info("No new rows to clean.")
            return {"rows_cleaned": 0, "rows_rejected": 0, "rejection_reasons": {}}

        df = pd.DataFrame(rows, columns=col_names)
        log.info(f"Loaded {len(df)} raw rows to process.")

        # ── 2. Apply quality checks ────────────────────────
        clean_rows = []

        for ticker, group in df.groupby("ticker"):
            group = group.sort_values("price_date").reset_index(drop=True)

            # Calculate daily return: (today_close - yesterday_close) / yesterday_close
            group["daily_return"] = group["close_price"].pct_change()

            for _, row in group.iterrows():
                passed = True
                for check_name, check_fn in ALL_CHECKS:
                    if not check_fn(row):
                        log.warning(
                            f"REJECTED {ticker} {row['price_date']}: "
                            f"failed '{check_name}'"
                        )
                        rejection_reasons[check_name] += 1
                        rows_rejected += 1
                        passed = False
                        break   # stop at first failure

                if passed:
                    clean_rows.append((
                        row["ticker"],
                        row["price_date"],
                        float(row["open_price"]),
                        float(row["high_price"]),
                        float(row["low_price"]),
                        float(row["close_price"]),
                        float(row["adj_close_price"]),
                        int(row["volume"]),
                        # daily_return is NaN for first row in each ticker group
                        float(row["daily_return"]) if pd.notna(row["daily_return"]) else None,
                    ))

        # ── 3. Bulk insert clean rows ──────────────────────
        if clean_rows:
            insert_sql = """
                INSERT INTO cleaned_stock_prices
                    (ticker, price_date, open_price, high_price, low_price,
                     close_price, adj_close_price, volume, daily_return)
                VALUES %s
                ON CONFLICT (ticker, price_date) DO NOTHING
            """
            with conn.cursor() as cur:
                execute_values(cur, insert_sql, clean_rows)
            conn.commit()
            rows_cleaned = len(clean_rows)
            log.info(f"✓ Inserted {rows_cleaned} clean rows.")

        # Remove zero-count rejection reasons from the report
        active_rejections = {k: v for k, v in rejection_reasons.items() if v > 0}

    finally:
        conn.close()

    return {
        "rows_cleaned": rows_cleaned,
        "rows_rejected": rows_rejected,
        "rejection_reasons": active_rejections,
    }


if __name__ == "__main__":
    result = clean_and_store()
    print(result)
