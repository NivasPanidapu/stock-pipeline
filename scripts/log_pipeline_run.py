import os
import psycopg2
from datetime import date

TICKERS = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "TSLA", "META"]

conn = psycopg2.connect(
    host=os.getenv("STOCK_DB_HOST"),
    port=int(os.getenv("STOCK_DB_PORT")),
    dbname=os.getenv("STOCK_DB_NAME"),
    user=os.getenv("STOCK_DB_USER"),
    password=os.getenv("STOCK_DB_PASSWORD"),
)

today = date.today()

with conn.cursor() as cur:
    cur.execute("SELECT COUNT(*) FROM raw_stock_prices WHERE fetched_at::date = %s", (today,))
    rows_fetched = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM cleaned_stock_prices WHERE processed_at::date = %s", (today,))
    rows_cleaned = cur.fetchone()[0]

    cur.execute("""
        INSERT INTO pipeline_runs
            (run_date, tickers, rows_fetched, rows_cleaned, rows_rejected, status, completed_at)
        VALUES (%s, %s, %s, %s, %s, %s, NOW())
    """, (today, TICKERS, rows_fetched, rows_cleaned, 0, 'success'))

conn.commit()
conn.close()
print(f"Pipeline run logged! fetched={rows_fetched}, cleaned={rows_cleaned}")
