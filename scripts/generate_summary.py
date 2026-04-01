import os
import requests
import psycopg2

conn = psycopg2.connect(
    host=os.getenv("STOCK_DB_HOST"),
    port=int(os.getenv("STOCK_DB_PORT")),
    dbname=os.getenv("STOCK_DB_NAME"),
    user=os.getenv("STOCK_DB_USER"),
    password=os.getenv("STOCK_DB_PASSWORD"),
)

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

prompt = f"""You are a professional stock market analyst. Here is today's data:

{data_str}

Write a concise market summary (max 150 words) covering overall sentiment, best and worst performer, and one key insight. Plain text only."""

# Call Gemini API
api_key = os.getenv("GEMINI_API_KEY")
url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash-lite:generateContent?key={api_key}"

response = requests.post(url, json={
    "contents": [{"parts": [{"text": prompt}]}]
})

print(f"Status: {response.status_code}")
print(f"Response: {response.text[:500]}")

result = response.json()

if "error" in result:
    raise Exception(f"Gemini API error: {result['error']}")

if "candidates" not in result:
    raise Exception(f"Unexpected response: {list(result.keys())}")

summary = result["candidates"][0]["content"]["parts"][0]["text"]
print(f"Summary generated: {summary[:100]}")

# Save to database
with conn.cursor() as cur:
    cur.execute("""
        INSERT INTO daily_summary (summary, generated_at)
        VALUES (%s, NOW())
    """, (summary,))
conn.commit()
conn.close()
print("Summary saved!")
