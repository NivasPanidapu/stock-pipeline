import os
import logging
from datetime import datetime, timedelta

import yfinance as yf
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


def fetch_and_store(tickers, start_date=None, end_date=None):
    if end_date is None:
        end_date = datetime.today().strftime("%Y-%m-%d")
    if start_date is None:
        start_date = (datetime.today() - timedelta(days=90)).strftime("%Y-%m-%d")

    rows_fetched = 0
    tickers_ok = []
    tickers_failed = []
    conn = get_db_connection()

    try:
        for ticker in tickers:
            try:
                df = yf.download(ticker, start=start_date, end=end_date,
                                 auto_adjust=False, progress=False)

                if df.empty:
                    log.warning(f"No data for {ticker}")
                    tickers_failed.append(ticker)
                    continue

                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = df.columns.get_level_values(0)

                records = []
                for date, row in df.iterrows():
                    records.append((
                        ticker,
                        date.date(),
                        float(row.get("Open") or 0),
                        float(row.get("High") or 0),
                        float(row.get("Low") or 0),
                        float(row.get("Close") or 0),
                        float(row.get("Adj Close") or 0),
                        int(row.get("Volume") or 0),
                    ))

                with conn.cursor() as cur:
                    execute_values(cur, """
                        INSERT INTO raw_stock_prices
                            (ticker, price_date, open_price, high_price, low_price,
                             close_price, adj_close_price, volume)
                        VALUES %s
                        ON CONFLICT (ticker, price_date) DO NOTHING
                    """, records)
                conn.commit()

                log.info(f"✓ {ticker}: {len(records)} rows inserted")
                rows_fetched += len(records)
                tickers_ok.append(ticker)

            except Exception as e:
                log.error(f"✗ {ticker} failed: {e}")
                tickers_failed.append(ticker)
                conn.rollback()
    finally:
        conn.close()

    return {"rows_fetched": rows_fetched, "tickers_ok": tickers_ok, "tickers_failed": tickers_failed}


if __name__ == "__main__":
    result = fetch_and_store(["AAPL", "MSFT", "GOOGL"])
    print(result)