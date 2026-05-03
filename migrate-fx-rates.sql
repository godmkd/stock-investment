-- USD/TWD daily FX rate history
CREATE TABLE IF NOT EXISTS fx_rates (
  pair TEXT NOT NULL,           -- e.g. 'USDTWD'
  trade_date DATE NOT NULL,
  close_rate NUMERIC,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (pair, trade_date)
);

ALTER TABLE fx_rates ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "fx_rates public read" ON fx_rates;
CREATE POLICY "fx_rates public read" ON fx_rates
  FOR SELECT USING (true);

-- Service-role writes only (no anon write policy)
