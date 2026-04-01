import os
import requests
import psycopg2

# ── Connect to database ──────────────────────────────────
conn = psycopg2.connect(
    host=os.getenv("STOCK_DB_HOST"),
    port=int(os.getenv("STOCK_DB_PORT")),
    dbname=os.getenv("STOCK_DB_NAME"),
    user=os.getenv("STOCK_DB_USER"),
    password=os.getenv("STOCK_DB_PASSWORD"),
)

# ── Fetch latest market data ─────────────────────────────
with conn.cursor() as cur:
    cur.execute("""
        SELECT ticker, close_price, daily_return, volume
        FROM cleaned_stock_prices
        WHERE price_date = (
            SELECT MAX(price_date) FROM cleaned_stock_prices
            WHERE daily_return IS NOT NULL
        )
        ORDER BY ticker
    """)
    rows = cur.fetchall()

data_str = "\n".join([
    f"{r[0]}: ${float(r[1]):.2f}, Return: {float(r[2])*100:.2f}%, Volume: {int(r[3])//1000000}M"
    for r in rows
])

print(f"Market data:\n{data_str}")

# ── Build prompt ─────────────────────────────────────────
prompt = f"""You are a professional stock market analyst. Here is today's data:

{data_str}

Write a concise market summary (max 150 words) covering overall sentiment, best and worst performer, and one key insight for investors. Plain text only, no bullet points or markdown symbols."""

# ── Call Groq API ────────────────────────────────────────
api_key = os.getenv("GROQ_API_KEY")
url = "https://api.groq.com/openai/v1/chat/completions"

response = requests.post(
    url,
    headers={
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    },
    json={
        "model": "llama-3.1-8b-instant",
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 300
    }
)

print(f"Status: {response.status_code}")
result = response.json()

if "error" in result:
    raise Exception(f"Groq API error: {result['error']}")

summary = result["choices"][0]["message"]["content"]
print(f"Summary generated: {summary}")

# ── Save to database ─────────────────────────────────────
with conn.cursor() as cur:
    cur.execute("""
        INSERT INTO daily_summary (summary, generated_at)
        VALUES (%s, NOW())
    """, (summary,))
conn.commit()
conn.close()
print("Summary saved to database!")
