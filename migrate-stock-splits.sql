-- Stock split history (forward + reverse splits)
CREATE TABLE IF NOT EXISTS stock_splits (
  market TEXT NOT NULL,           -- 'us' or 'tw'
  ticker TEXT NOT NULL,
  split_date DATE NOT NULL,
  numerator NUMERIC NOT NULL,     -- e.g. 1 for a 1-for-15 reverse split
  denominator NUMERIC NOT NULL,   -- e.g. 15 for a 1-for-15 reverse split
  ratio_text TEXT,                -- e.g. "1:15" for display
  created_at TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (market, ticker, split_date)
);

ALTER TABLE stock_splits ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "stock_splits public read" ON stock_splits;
CREATE POLICY "stock_splits public read" ON stock_splits
  FOR SELECT USING (true);
