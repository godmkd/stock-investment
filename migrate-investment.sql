-- 投資追蹤系統 — Supabase 資料表
-- 在 Supabase SQL Editor 執行

-- 1. 投資帳戶
CREATE TABLE IF NOT EXISTS public.inv_accounts (
  id TEXT PRIMARY KEY,
  user_id UUID NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
  name TEXT NOT NULL,
  owner TEXT DEFAULT '',
  bank TEXT DEFAULT '',
  market TEXT DEFAULT '',
  ownership_pct DECIMAL(5,2) DEFAULT 100,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 2. 美股交易
CREATE TABLE IF NOT EXISTS public.inv_us_trades (
  id BIGINT PRIMARY KEY,
  user_id UUID NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
  account_id TEXT NOT NULL,
  date TEXT NOT NULL,
  ticker TEXT NOT NULL,
  type TEXT NOT NULL CHECK (type IN ('buy', 'sell')),
  shares DECIMAL(12,4) NOT NULL,
  price DECIMAL(12,4) NOT NULL,
  fee DECIMAL(10,2) DEFAULT 0,
  adjust DECIMAL(10,2) DEFAULT 0,
  invest_amount DECIMAL(12,2),
  actual_cost DECIMAL(12,2),
  created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 3. 美股資金
CREATE TABLE IF NOT EXISTS public.inv_us_funds (
  id BIGINT PRIMARY KEY,
  user_id UUID NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
  account_id TEXT NOT NULL,
  date TEXT NOT NULL,
  type TEXT NOT NULL CHECK (type IN ('deposit', 'withdraw')),
  amount DECIMAL(12,2) NOT NULL,
  twd_amount DECIMAL(12,2),
  rate DECIMAL(8,4),
  note TEXT DEFAULT '',
  created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 4. 美股收益
CREATE TABLE IF NOT EXISTS public.inv_us_income (
  id BIGINT PRIMARY KEY,
  user_id UUID NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
  account_id TEXT NOT NULL,
  date TEXT NOT NULL,
  type TEXT NOT NULL CHECK (type IN ('dividend', 'interest', 'tax', 'adjust')),
  source TEXT DEFAULT '',
  amount DECIMAL(12,2) NOT NULL,
  note TEXT DEFAULT '',
  created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 5. 台股交易
CREATE TABLE IF NOT EXISTS public.inv_tw_trades (
  id BIGINT PRIMARY KEY,
  user_id UUID NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
  account_id TEXT NOT NULL,
  date TEXT NOT NULL,
  ticker TEXT NOT NULL,
  type TEXT NOT NULL CHECK (type IN ('buy', 'sell')),
  shares DECIMAL(12,4) NOT NULL,
  price DECIMAL(12,4) NOT NULL,
  fee DECIMAL(10,2) DEFAULT 0,
  adjust DECIMAL(10,2) DEFAULT 0,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 6. 台股資金
CREATE TABLE IF NOT EXISTS public.inv_tw_funds (
  id BIGINT PRIMARY KEY,
  user_id UUID NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
  account_id TEXT NOT NULL,
  date TEXT NOT NULL,
  type TEXT NOT NULL CHECK (type IN ('deposit', 'withdraw')),
  amount DECIMAL(12,2) NOT NULL,
  note TEXT DEFAULT '',
  created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 7. 台股收益
CREATE TABLE IF NOT EXISTS public.inv_tw_income (
  id BIGINT PRIMARY KEY,
  user_id UUID NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
  account_id TEXT NOT NULL,
  date TEXT NOT NULL,
  type TEXT NOT NULL CHECK (type IN ('dividend', 'interest', 'tax', 'adjust')),
  source TEXT DEFAULT '',
  amount DECIMAL(12,2) NOT NULL,
  note TEXT DEFAULT '',
  created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 8. 房產/貸款
CREATE TABLE IF NOT EXISTS public.inv_mortgages (
  id TEXT PRIMARY KEY,
  user_id UUID NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
  name TEXT NOT NULL,
  owner TEXT DEFAULT '',
  account_id TEXT,
  property_type TEXT DEFAULT 'existing' CHECK (property_type IN ('presale', 'existing')),
  total_price DECIMAL(12,2) DEFAULT 0,
  construction_pct DECIMAL(5,2) DEFAULT 0,
  payments JSONB DEFAULT '[]',
  remaining_loan DECIMAL(12,2) DEFAULT 0,
  remaining_months INT DEFAULT 0,
  loan_rate DECIMAL(5,2) DEFAULT 0,
  paid_principal DECIMAL(12,2) DEFAULT 0,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 9. 使用者設定（API key 等）
CREATE TABLE IF NOT EXISTS public.inv_user_settings (
  user_id UUID PRIMARY KEY REFERENCES auth.users(id) ON DELETE CASCADE,
  finnhub_key TEXT DEFAULT '',
  settings JSONB DEFAULT '{}',
  created_at TIMESTAMPTZ DEFAULT NOW()
);

-- RLS
ALTER TABLE public.inv_accounts ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.inv_us_trades ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.inv_us_funds ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.inv_us_income ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.inv_tw_trades ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.inv_tw_funds ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.inv_tw_income ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.inv_mortgages ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.inv_user_settings ENABLE ROW LEVEL SECURITY;

-- 每個使用者只能存取自己的資料
CREATE POLICY "inv_accounts_user" ON public.inv_accounts FOR ALL TO authenticated USING (user_id = auth.uid()) WITH CHECK (user_id = auth.uid());
CREATE POLICY "inv_us_trades_user" ON public.inv_us_trades FOR ALL TO authenticated USING (user_id = auth.uid()) WITH CHECK (user_id = auth.uid());
CREATE POLICY "inv_us_funds_user" ON public.inv_us_funds FOR ALL TO authenticated USING (user_id = auth.uid()) WITH CHECK (user_id = auth.uid());
CREATE POLICY "inv_us_income_user" ON public.inv_us_income FOR ALL TO authenticated USING (user_id = auth.uid()) WITH CHECK (user_id = auth.uid());
CREATE POLICY "inv_tw_trades_user" ON public.inv_tw_trades FOR ALL TO authenticated USING (user_id = auth.uid()) WITH CHECK (user_id = auth.uid());
CREATE POLICY "inv_tw_funds_user" ON public.inv_tw_funds FOR ALL TO authenticated USING (user_id = auth.uid()) WITH CHECK (user_id = auth.uid());
CREATE POLICY "inv_tw_income_user" ON public.inv_tw_income FOR ALL TO authenticated USING (user_id = auth.uid()) WITH CHECK (user_id = auth.uid());
CREATE POLICY "inv_mortgages_user" ON public.inv_mortgages FOR ALL TO authenticated USING (user_id = auth.uid()) WITH CHECK (user_id = auth.uid());
CREATE POLICY "inv_user_settings_user" ON public.inv_user_settings FOR ALL TO authenticated USING (user_id = auth.uid()) WITH CHECK (user_id = auth.uid());
