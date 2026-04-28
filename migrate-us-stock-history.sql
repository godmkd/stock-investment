CREATE TABLE IF NOT EXISTS us_stock_history (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  ticker TEXT NOT NULL,
  trade_date DATE NOT NULL,
  open_price NUMERIC,
  high_price NUMERIC,
  low_price NUMERIC,
  close_price NUMERIC,
  volume BIGINT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(ticker, trade_date)
);

ALTER TABLE us_stock_history ENABLE ROW LEVEL SECURITY;
CREATE POLICY "us_stock_history_read" ON us_stock_history FOR SELECT TO authenticated USING (true);
CREATE POLICY "us_stock_history_write" ON us_stock_history FOR ALL USING (true) WITH CHECK (true);
CREATE INDEX IF NOT EXISTS idx_us_stock_history_ticker_date ON us_stock_history(ticker, trade_date);
