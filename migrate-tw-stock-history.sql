CREATE TABLE IF NOT EXISTS tw_stock_history (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  stock_code TEXT NOT NULL,
  trade_date DATE NOT NULL,
  open_price NUMERIC,
  high_price NUMERIC,
  low_price NUMERIC,
  close_price NUMERIC,
  volume BIGINT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(stock_code, trade_date)
);

-- Watchlist table for which stocks to track
CREATE TABLE IF NOT EXISTS tw_stock_watchlist (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  stock_code TEXT NOT NULL UNIQUE,
  stock_name TEXT,
  added_at TIMESTAMPTZ DEFAULT NOW()
);

ALTER TABLE tw_stock_history ENABLE ROW LEVEL SECURITY;
ALTER TABLE tw_stock_watchlist ENABLE ROW LEVEL SECURITY;

CREATE POLICY "tw_stock_history_read" ON tw_stock_history FOR SELECT TO authenticated USING (true);
CREATE POLICY "tw_stock_history_write" ON tw_stock_history FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "tw_stock_watchlist_read" ON tw_stock_watchlist FOR SELECT TO authenticated USING (true);
CREATE POLICY "tw_stock_watchlist_write" ON tw_stock_watchlist FOR ALL USING (true) WITH CHECK (true);

CREATE INDEX IF NOT EXISTS idx_tw_stock_history_code_date ON tw_stock_history(stock_code, trade_date);

-- Insert some default popular stocks
INSERT INTO tw_stock_watchlist (stock_code, stock_name) VALUES
  ('2330', '台積電'), ('2317', '鴻海'), ('2454', '聯發科'),
  ('2308', '台達電'), ('2382', '廣達'), ('2881', '富邦金'),
  ('2882', '國泰金'), ('2891', '中信金'), ('0050', '元大台灣50'),
  ('0056', '元大高股息')
ON CONFLICT (stock_code) DO NOTHING;
