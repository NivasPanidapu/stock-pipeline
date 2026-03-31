-- ============================================================
-- init.sql  –  Stock Database Schema
-- ============================================================
-- This file runs automatically the FIRST TIME the stock-db
-- container starts (thanks to the Docker volume mount).
-- It creates all tables needed by the pipeline.
-- ============================================================


-- ── TABLE 1: raw_stock_prices ──────────────────────────────
-- Stores the raw OHLCV data exactly as received from Yahoo Finance.
-- "Raw" means we haven't cleaned or transformed it yet.
-- Having a raw layer lets you re-process data without re-fetching it.
CREATE TABLE IF NOT EXISTS raw_stock_prices (
    id              SERIAL PRIMARY KEY,
    ticker          VARCHAR(10)    NOT NULL,   -- e.g. 'AAPL', 'MSFT'
    price_date      DATE           NOT NULL,   -- trading date
    open_price      NUMERIC(12, 4),            -- opening price
    high_price      NUMERIC(12, 4),            -- daily high
    low_price       NUMERIC(12, 4),            -- daily low
    close_price     NUMERIC(12, 4),            -- closing price
    adj_close_price NUMERIC(12, 4),            -- adjusted for splits/dividends
    volume          BIGINT,                    -- shares traded
    fetched_at      TIMESTAMP DEFAULT NOW(),   -- when we pulled the data
    UNIQUE (ticker, price_date)                -- prevent duplicate rows
);

-- ── TABLE 2: cleaned_stock_prices ─────────────────────────
-- Stores validated, cleaned data. The pipeline copies rows here
-- only after they pass all quality checks (no nulls, sane prices, etc.)
CREATE TABLE IF NOT EXISTS cleaned_stock_prices (
    id              SERIAL PRIMARY KEY,
    ticker          VARCHAR(10)    NOT NULL,
    price_date      DATE           NOT NULL,
    open_price      NUMERIC(12, 4) NOT NULL,
    high_price      NUMERIC(12, 4) NOT NULL,
    low_price       NUMERIC(12, 4) NOT NULL,
    close_price     NUMERIC(12, 4) NOT NULL,
    adj_close_price NUMERIC(12, 4) NOT NULL,
    volume          BIGINT         NOT NULL,
    daily_return    NUMERIC(8, 6),             -- % change from previous close
    processed_at    TIMESTAMP DEFAULT NOW(),
    UNIQUE (ticker, price_date)
);

-- ── TABLE 3: pipeline_runs ────────────────────────────────
-- Audit log: records every pipeline execution with stats.
-- Useful for debugging, monitoring, and data lineage.
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id              SERIAL PRIMARY KEY,
    run_date        DATE           NOT NULL,
    tickers         TEXT[]         NOT NULL,   -- array of tickers processed
    rows_fetched    INTEGER DEFAULT 0,
    rows_cleaned    INTEGER DEFAULT 0,
    rows_rejected   INTEGER DEFAULT 0,         -- failed quality checks
    status          VARCHAR(20)    NOT NULL,   -- 'success', 'partial', 'failed'
    error_message   TEXT,                      -- NULL if no error
    started_at      TIMESTAMP DEFAULT NOW(),
    completed_at    TIMESTAMP
);

-- ── INDEX: speed up common queries ────────────────────────
-- These indexes make time-series queries much faster.
CREATE INDEX IF NOT EXISTS idx_raw_ticker_date
    ON raw_stock_prices (ticker, price_date DESC);

CREATE INDEX IF NOT EXISTS idx_clean_ticker_date
    ON cleaned_stock_prices (ticker, price_date DESC);

-- ── HELPFUL VIEW ──────────────────────────────────────────
-- A ready-made view for the most recent price of each ticker.
CREATE OR REPLACE VIEW latest_prices AS
SELECT DISTINCT ON (ticker)
    ticker,
    price_date,
    close_price,
    adj_close_price,
    volume,
    daily_return
FROM cleaned_stock_prices
ORDER BY ticker, price_date DESC;

-- Let the user know setup succeeded
DO $$
BEGIN
    RAISE NOTICE 'Stock database schema initialized successfully.';
END $$;
