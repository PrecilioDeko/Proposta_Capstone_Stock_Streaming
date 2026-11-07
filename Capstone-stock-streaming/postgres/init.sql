-- =====================================================================
-- Skema database PostgreSQL untuk Stock Market Streaming Analytics
-- Tabel ini otomatis dibuat saat container Postgres pertama kali start
-- =====================================================================

CREATE TABLE IF NOT EXISTS stock_prices (
    id              BIGSERIAL PRIMARY KEY,
    ticker          VARCHAR(20)     NOT NULL,
    company         VARCHAR(100)    NOT NULL,
    trade_date      DATE            NOT NULL,
    open            DOUBLE PRECISION,
    high            DOUBLE PRECISION,
    low             DOUBLE PRECISION,
    close           DOUBLE PRECISION,
    adjclose        DOUBLE PRECISION,
    volume          BIGINT,
    price_change    DOUBLE PRECISION,
    pct_change      DOUBLE PRECISION,
    volatility_pct  DOUBLE PRECISION,
    trend           VARCHAR(10),
    event_time      TIMESTAMP,
    processed_at    TIMESTAMP DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_stock_prices_ticker ON stock_prices (ticker);
CREATE INDEX IF NOT EXISTS idx_stock_prices_date ON stock_prices (trade_date);
CREATE INDEX IF NOT EXISTS idx_stock_prices_ticker_date ON stock_prices (ticker, trade_date);
