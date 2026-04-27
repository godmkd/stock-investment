-- Add institutional trading columns to tw_stock_history
-- 三大法人買賣超

ALTER TABLE tw_stock_history ADD COLUMN IF NOT EXISTS foreign_buy BIGINT DEFAULT 0;
ALTER TABLE tw_stock_history ADD COLUMN IF NOT EXISTS foreign_sell BIGINT DEFAULT 0;
ALTER TABLE tw_stock_history ADD COLUMN IF NOT EXISTS foreign_net BIGINT DEFAULT 0;
ALTER TABLE tw_stock_history ADD COLUMN IF NOT EXISTS trust_buy BIGINT DEFAULT 0;
ALTER TABLE tw_stock_history ADD COLUMN IF NOT EXISTS trust_sell BIGINT DEFAULT 0;
ALTER TABLE tw_stock_history ADD COLUMN IF NOT EXISTS trust_net BIGINT DEFAULT 0;
ALTER TABLE tw_stock_history ADD COLUMN IF NOT EXISTS dealer_net BIGINT DEFAULT 0;
ALTER TABLE tw_stock_history ADD COLUMN IF NOT EXISTS institutional_net BIGINT DEFAULT 0;

-- EPS & Dividend table (per stock, per quarter/year)
CREATE TABLE IF NOT EXISTS tw_stock_fundamentals (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  stock_code TEXT NOT NULL,
  data_type TEXT NOT NULL,  -- 'eps_quarterly', 'eps_annual', 'dividend'
  period TEXT NOT NULL,     -- '2026Q1', '2025', etc.
  value NUMERIC,
  ex_date DATE,             -- 除息日 (for dividends)
  pay_date DATE,            -- 發放日 (for dividends)
  created_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(stock_code, data_type, period)
);

ALTER TABLE tw_stock_fundamentals ENABLE ROW LEVEL SECURITY;
CREATE POLICY "tw_stock_fundamentals_read" ON tw_stock_fundamentals FOR SELECT TO authenticated USING (true);
CREATE POLICY "tw_stock_fundamentals_write" ON tw_stock_fundamentals FOR ALL USING (true) WITH CHECK (true);
