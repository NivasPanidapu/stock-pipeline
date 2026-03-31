import os
import logging

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("STOCK_DB_HOST", "localhost"),
        port=int(os.getenv("STOCK_DB_PORT", 5432)),
        dbname=os.getenv("STOCK_DB_NAME", "stocks"),
        user=os.getenv("STOCK_DB_USER", "stocks_user"),
        password=os.getenv("STOCK_DB_PASSWORD", "stocks_password"),
    )


# ── Quality checks ─────────────────────────────────────────
# Each returns True if the row PASSES, False if it should be rejected

def check_no_nulls(row):
    cols = ["open_price", "high_price", "low_price", "close_price", "adj_close_price", "volume"]
    return not row[cols].isnull().any()

def check_positive_prices(row):
    return all(row[c] > 0 for c in ["open_price", "high_price", "low_price", "close_price"])

def check_high_gte_low(row):
    return row["high_price"] >= row["low_price"]

def check_positive_volume(row):
    return row["volume"] > 0

def check_extreme_move(row):
    if row["open_price"] == 0:
        return False
    return abs(row["close_price"] - row["open_price"]) / row["open_price"] <= 0.5


ALL_CHECKS = [
    ("no_nulls",        check_no_nulls),
    ("positive_prices", check_positive_prices),
    ("high_gte_low",    check_high_gte_low),
    ("positive_volume", check_positive_volume),
    ("extreme_move",    check_extreme_move),
]


def clean_and_store(tickers=None):
    conn = get_db_connection()
    rows_cleaned = 0
    rows_rejected = 0
    rejection_reasons = {check: 0 for check, _ in ALL_CHECKS}

    try:
        ticker_filter = "AND r.ticker = ANY(%s)" if tickers else ""
        params = [tickers] if tickers else None

        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT r.ticker, r.price_date, r.open_price, r.high_price,
                       r.low_price, r.close_price, r.adj_close_price, r.volume
                FROM raw_stock_prices r
                LEFT JOIN cleaned_stock_prices c
                    ON r.ticker = c.ticker AND r.price_date = c.price_date
                WHERE c.id IS NULL
                {ticker_filter}
                ORDER BY r.ticker, r.price_date
            """, params)
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]

        if not rows:
            log.info("No new rows to clean.")
            return {"rows_cleaned": 0, "rows_rejected": 0, "rejection_reasons": {}}

        df = pd.DataFrame(rows, columns=cols)
        clean_rows = []

        for ticker, group in df.groupby("ticker"):
            group = group.sort_values("price_date").reset_index(drop=True)
            group["daily_return"] = group["close_price"].pct_change()

            for _, row in group.iterrows():
                passed = True
                for check_name, check_fn in ALL_CHECKS:
                    if not check_fn(row):
                        log.warning(f"REJECTED {ticker} {row['price_date']}: failed '{check_name}'")
                        rejection_reasons[check_name] += 1
                        rows_rejected += 1
                        passed = False
                        break

                if passed:
                    clean_rows.append((
                        row["ticker"], row["price_date"],
                        float(row["open_price"]), float(row["high_price"]),
                        float(row["low_price"]), float(row["close_price"]),
                        float(row["adj_close_price"]), int(row["volume"]),
                        float(row["daily_return"]) if pd.notna(row["daily_return"]) else None,
                    ))

        if clean_rows:
            with conn.cursor() as cur:
                execute_values(cur, """
                    INSERT INTO cleaned_stock_prices
                        (ticker, price_date, open_price, high_price, low_price,
                         close_price, adj_close_price, volume, daily_return)
                    VALUES %s
                    ON CONFLICT (ticker, price_date) DO NOTHING
                """, clean_rows)
            conn.commit()
            rows_cleaned = len(clean_rows)
            log.info(f"✓ {rows_cleaned} clean rows inserted.")

    finally:
        conn.close()

    return {
        "rows_cleaned": rows_cleaned,
        "rows_rejected": rows_rejected,
        "rejection_reasons": {k: v for k, v in rejection_reasons.items() if v > 0},
    }


if __name__ == "__main__":
    print(clean_and_store())