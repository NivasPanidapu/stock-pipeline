CREATE TABLE IF NOT EXISTS raw_stock_prices (
    id              SERIAL PRIMARY KEY,
    ticker          VARCHAR(10)    NOT NULL,
    price_date      DATE           NOT NULL,
    open_price      NUMERIC(12, 4),
    high_price      NUMERIC(12, 4),
    low_price       NUMERIC(12, 4),
    close_price     NUMERIC(12, 4),
    adj_close_price NUMERIC(12, 4),
    volume          BIGINT,
    fetched_at      TIMESTAMP DEFAULT NOW(),
    UNIQUE (ticker, price_date)
);

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
    daily_return    NUMERIC(8, 6),
    processed_at    TIMESTAMP DEFAULT NOW(),
    UNIQUE (ticker, price_date)
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    id              SERIAL PRIMARY KEY,
    run_date        DATE      NOT NULL,
    tickers         TEXT[]    NOT NULL,
    rows_fetched    INTEGER   DEFAULT 0,
    rows_cleaned    INTEGER   DEFAULT 0,
    rows_rejected   INTEGER   DEFAULT 0,
    status          VARCHAR(20) NOT NULL,
    error_message   TEXT,
    started_at      TIMESTAMP DEFAULT NOW(),
    completed_at    TIMESTAMP
);