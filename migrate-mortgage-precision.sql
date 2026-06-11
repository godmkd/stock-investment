-- Bump mortgage numeric precision from DECIMAL(N,2) to (N,4) so the
-- UI's 4-decimal entry round-trips through Supabase without truncation.
-- Symptom this fixes: user enters 631.467 萬 for 剩餘貸款, reopens the
-- app, sees 631.47 萬 (DB truncated to 2 decimals on insert).

ALTER TABLE public.inv_mortgages
  ALTER COLUMN total_price TYPE DECIMAL(14,4) USING total_price::DECIMAL(14,4),
  ALTER COLUMN construction_pct TYPE DECIMAL(7,4) USING construction_pct::DECIMAL(7,4),
  ALTER COLUMN remaining_loan TYPE DECIMAL(14,4) USING remaining_loan::DECIMAL(14,4),
  ALTER COLUMN paid_principal TYPE DECIMAL(14,4) USING paid_principal::DECIMAL(14,4);

-- Sanity check (won't error if columns are already wider; pg only widens):
COMMENT ON COLUMN public.inv_mortgages.total_price IS '房屋總價（萬元），4 decimals';
COMMENT ON COLUMN public.inv_mortgages.remaining_loan IS '剩餘貸款（萬元），4 decimals';
COMMENT ON COLUMN public.inv_mortgages.paid_principal IS '已繳貸款本金（萬元），4 decimals';
COMMENT ON COLUMN public.inv_mortgages.construction_pct IS '工程款佔總價百分比，4 decimals';
