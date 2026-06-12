-- L3 主題動能 dashboard: scanner results + per-user watchlist
-- Apply via Supabase SQL Editor before deploying the new frontend tab.

-- ============ Scanner results (shared, public read) ============
CREATE TABLE IF NOT EXISTS public.scanner_results (
  id BIGSERIAL PRIMARY KEY,
  market TEXT NOT NULL CHECK (market IN ('tw', 'us')),
  scan_type TEXT NOT NULL CHECK (scan_type IN ('momentum', 'chip')),
  scan_date DATE NOT NULL,
  rank INT NOT NULL,
  ticker TEXT NOT NULL,
  name TEXT DEFAULT '',
  score NUMERIC(10,2),
  payload JSONB,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE (market, scan_type, scan_date, ticker)
);

CREATE INDEX IF NOT EXISTS scanner_results_market_date_idx
  ON public.scanner_results (market, scan_type, scan_date DESC, rank);
CREATE INDEX IF NOT EXISTS scanner_results_ticker_idx
  ON public.scanner_results (ticker, market, scan_date DESC);

ALTER TABLE public.scanner_results ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "scanner_results_public_read" ON public.scanner_results;
CREATE POLICY "scanner_results_public_read"
  ON public.scanner_results FOR SELECT USING (true);

-- Service role (used by scanner GitHub Actions) bypasses RLS automatically;
-- no explicit insert policy needed.

-- ============ User watchlist (per-user, secured) ============
CREATE TABLE IF NOT EXISTS public.user_watchlist (
  id BIGSERIAL PRIMARY KEY,
  user_id UUID NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
  market TEXT NOT NULL CHECK (market IN ('tw', 'us')),
  ticker TEXT NOT NULL,
  sort_order INT DEFAULT 0,
  note TEXT DEFAULT '',
  created_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE (user_id, market, ticker)
);

CREATE INDEX IF NOT EXISTS user_watchlist_user_idx
  ON public.user_watchlist (user_id, market, sort_order);

ALTER TABLE public.user_watchlist ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "user_watchlist_owner" ON public.user_watchlist;
CREATE POLICY "user_watchlist_owner"
  ON public.user_watchlist FOR ALL TO authenticated
  USING (user_id = auth.uid())
  WITH CHECK (user_id = auth.uid());

COMMENT ON TABLE public.scanner_results IS 'Daily momentum/chip scanner output, public read';
COMMENT ON TABLE public.user_watchlist IS 'Per-user tracked tickers shown at top of 主題動能 tab';
