-- 股票匯撥 (inter-account stock transfer) support.
--   - Extend trade type CHECK to allow 'transfer_in' / 'transfer_out'
--   - Add transfer_id column linking the paired trades
--   - Add transferred_lots JSONB so transfer_in can rebuild the original
--     cost basis lots (FIFO from the sending account at transfer time)
--
-- Apply via Supabase SQL Editor.

-- ============ inv_us_trades ============
ALTER TABLE public.inv_us_trades
  DROP CONSTRAINT IF EXISTS inv_us_trades_type_check;

ALTER TABLE public.inv_us_trades
  ADD CONSTRAINT inv_us_trades_type_check
  CHECK (type IN ('buy', 'sell', 'transfer_in', 'transfer_out'));

ALTER TABLE public.inv_us_trades
  ADD COLUMN IF NOT EXISTS transfer_id TEXT,
  ADD COLUMN IF NOT EXISTS transferred_lots JSONB;

CREATE INDEX IF NOT EXISTS inv_us_trades_transfer_id_idx
  ON public.inv_us_trades (transfer_id) WHERE transfer_id IS NOT NULL;

-- ============ inv_tw_trades ============
ALTER TABLE public.inv_tw_trades
  DROP CONSTRAINT IF EXISTS inv_tw_trades_type_check;

ALTER TABLE public.inv_tw_trades
  ADD CONSTRAINT inv_tw_trades_type_check
  CHECK (type IN ('buy', 'sell', 'transfer_in', 'transfer_out'));

ALTER TABLE public.inv_tw_trades
  ADD COLUMN IF NOT EXISTS transfer_id TEXT,
  ADD COLUMN IF NOT EXISTS transferred_lots JSONB;

CREATE INDEX IF NOT EXISTS inv_tw_trades_transfer_id_idx
  ON public.inv_tw_trades (transfer_id) WHERE transfer_id IS NOT NULL;

COMMENT ON COLUMN public.inv_us_trades.transfer_id IS 'Shared UUID linking the transfer_out/transfer_in pair';
COMMENT ON COLUMN public.inv_us_trades.transferred_lots IS 'FIFO buyLot snapshot at transfer time, used to restore cost basis on the receiving side';
COMMENT ON COLUMN public.inv_tw_trades.transfer_id IS 'Shared UUID linking the transfer_out/transfer_in pair';
COMMENT ON COLUMN public.inv_tw_trades.transferred_lots IS 'FIFO buyLot snapshot at transfer time, used to restore cost basis on the receiving side';
