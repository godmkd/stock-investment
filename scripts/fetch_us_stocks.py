"""
Fetch US stock daily data from Finnhub and upsert into Supabase.

- Reads tracked tickers from inv_user_settings (us_prices keys)
- Backfills last 6 months of daily candles on first run
- Adds daily close prices on subsequent runs
- Upserts into us_stock_history with ON CONFLICT handling
"""

import os
import sys
import time
import json
import requests
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from supabase import create_client, Client

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")
FINNHUB_KEY = os.environ.get("FINNHUB_KEY")

FINNHUB_QUOTE_URL = "https://finnhub.io/api/v1/quote"
FINNHUB_CANDLE_URL = "https://finnhub.io/api/v1/stock/candle"
API_DELAY_SECONDS = 1.1  # 60 calls/min on free tier
BACKFILL_MONTHS = 6

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_finnhub_key(supabase: Client) -> str:
    """Get Finnhub API key from env or from inv_user_settings table."""
    if FINNHUB_KEY:
        return FINNHUB_KEY
    print("FINNHUB_KEY not in env, checking inv_user_settings...")
    resp = supabase.table("inv_user_settings").select("finnhub_key").not_.is_("finnhub_key", "null").limit(1).execute()
    if resp.data and resp.data[0].get("finnhub_key"):
        key = resp.data[0]["finnhub_key"]
        print(f"  Found Finnhub key from inv_user_settings (length={len(key)})")
        return key
    return ""


def get_tracked_tickers(supabase: Client) -> list[str]:
    """
    Get list of US stock tickers to track.
    Sources:
    1. Tickers already in us_stock_history
    2. Tickers from inv_user_settings.settings.us_prices
    """
    tickers = set()

    # 1. Existing tickers in us_stock_history
    try:
        resp = supabase.rpc("", {}).execute()  # Can't do DISTINCT easily, use select
    except Exception:
        pass
    try:
        resp = supabase.table("us_stock_history").select("ticker").execute()
        if resp.data:
            for row in resp.data:
                tickers.add(row["ticker"])
    except Exception as e:
        print(f"  [WARN] Could not query us_stock_history: {e}")

    # 2. Tickers from inv_user_settings cached prices
    try:
        resp = supabase.table("inv_user_settings").select("settings").execute()
        if resp.data:
            for row in resp.data:
                settings = row.get("settings") or {}
                us_prices = settings.get("us_prices") or {}
                for key in us_prices:
                    # Keys are ticker symbols; skip non-ticker entries
                    if key.isupper() and len(key) <= 5 and key.isalpha():
                        tickers.add(key)
    except Exception as e:
        print(f"  [WARN] Could not query inv_user_settings: {e}")

    return sorted(tickers)


def fetch_candles(ticker: str, api_key: str, from_ts: int, to_ts: int) -> list[dict]:
    """
    Fetch daily candles from Finnhub for a date range.
    Returns list of row dicts ready for Supabase upsert.
    """
    params = {
        "symbol": ticker,
        "resolution": "D",
        "from": from_ts,
        "to": to_ts,
        "token": api_key,
    }
    try:
        resp = requests.get(FINNHUB_CANDLE_URL, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as exc:
        print(f"  [WARN] Candle fetch error for {ticker}: {exc}")
        return []
    except ValueError:
        print(f"  [WARN] Invalid JSON for {ticker} candles")
        return []

    if data.get("s") != "ok":
        print(f"  [INFO] No candle data for {ticker} (s={data.get('s', 'N/A')})")
        return []

    rows = []
    timestamps = data.get("t", [])
    opens = data.get("o", [])
    highs = data.get("h", [])
    lows = data.get("l", [])
    closes = data.get("c", [])
    volumes = data.get("v", [])

    for i in range(len(timestamps)):
        trade_date = datetime.utcfromtimestamp(timestamps[i]).strftime("%Y-%m-%d")
        rows.append({
            "ticker": ticker,
            "trade_date": trade_date,
            "open_price": opens[i] if i < len(opens) else None,
            "high_price": highs[i] if i < len(highs) else None,
            "low_price": lows[i] if i < len(lows) else None,
            "close_price": closes[i] if i < len(closes) else None,
            "volume": int(volumes[i]) if i < len(volumes) else None,
        })

    return rows


def fetch_current_quote(ticker: str, api_key: str) -> dict | None:
    """
    Fetch current quote from Finnhub.
    Returns a row dict for today's date, or None if unavailable.
    """
    params = {"symbol": ticker, "token": api_key}
    try:
        resp = requests.get(FINNHUB_QUOTE_URL, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except (requests.RequestException, ValueError) as exc:
        print(f"  [WARN] Quote fetch error for {ticker}: {exc}")
        return None

    if not data.get("c") or data["c"] == 0:
        print(f"  [INFO] No quote data for {ticker}")
        return None

    # Use timestamp if available, else today
    trade_date = datetime.utcnow().strftime("%Y-%m-%d")
    if data.get("t") and data["t"] > 0:
        trade_date = datetime.utcfromtimestamp(data["t"]).strftime("%Y-%m-%d")

    return {
        "ticker": ticker,
        "trade_date": trade_date,
        "open_price": data.get("o"),
        "high_price": data.get("h"),
        "low_price": data.get("l"),
        "close_price": data.get("c"),
        "volume": None,  # Quote endpoint doesn't return volume
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        print("ERROR: SUPABASE_URL and SUPABASE_SERVICE_KEY must be set.")
        sys.exit(1)

    supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

    # 1. Get Finnhub API key
    api_key = get_finnhub_key(supabase)
    if not api_key:
        print("ERROR: No Finnhub API key found (env or inv_user_settings).")
        sys.exit(1)
    print(f"Using Finnhub API key: {api_key[:4]}...{api_key[-4:]}")

    # 2. Get tracked tickers
    print("=== Fetching tracked US tickers ===")
    tickers = get_tracked_tickers(supabase)
    if not tickers:
        print("No US tickers to track. Nothing to do.")
        return
    print(f"Tracking {len(tickers)} tickers: {', '.join(tickers)}")

    # 3. For each ticker, check existing data and backfill/update
    today = datetime.utcnow()
    total_upserted = 0

    for ticker in tickers:
        print(f"\n--- {ticker} ---")

        # Check if we have existing data
        existing_resp = (
            supabase.table("us_stock_history")
            .select("trade_date")
            .eq("ticker", ticker)
            .order("trade_date", desc=True)
            .limit(1)
            .execute()
        )

        latest_existing = None
        if existing_resp.data:
            latest_existing = existing_resp.data[0]["trade_date"]
            print(f"  Latest existing record: {latest_existing}")

        # Determine if we need backfill or just daily update
        needs_backfill = True
        if latest_existing:
            latest_date = datetime.strptime(latest_existing, "%Y-%m-%d")
            days_since = (today - latest_date).days
            if days_since <= 5:
                needs_backfill = False
                print(f"  Data is recent ({days_since} days old), fetching daily quote only")

        if needs_backfill:
            # Backfill last 6 months
            from_date = today - relativedelta(months=BACKFILL_MONTHS)
            from_ts = int(from_date.timestamp())
            to_ts = int(today.timestamp())
            print(f"  Backfilling {from_date.strftime('%Y-%m-%d')} to {today.strftime('%Y-%m-%d')} ...")

            rows = fetch_candles(ticker, api_key, from_ts, to_ts)
            time.sleep(API_DELAY_SECONDS)

            if rows:
                # Upsert in batches of 200
                for i in range(0, len(rows), 200):
                    batch = rows[i:i+200]
                    upserted = (
                        supabase.table("us_stock_history")
                        .upsert(batch, on_conflict="ticker,trade_date")
                        .execute()
                    )
                    count = len(upserted.data) if upserted.data else 0
                    total_upserted += count
                print(f"  Upserted {len(rows)} candle rows")
            else:
                print("  (no candle data)")
        else:
            # Just fetch current quote for today
            quote = fetch_current_quote(ticker, api_key)
            time.sleep(API_DELAY_SECONDS)

            if quote:
                upserted = (
                    supabase.table("us_stock_history")
                    .upsert([quote], on_conflict="ticker,trade_date")
                    .execute()
                )
                count = len(upserted.data) if upserted.data else 0
                total_upserted += count
                print(f"  Upserted daily quote (close={quote['close_price']})")
            else:
                print("  (no quote data)")

    print(f"\n=== Done. Total rows upserted: {total_upserted} ===")


if __name__ == "__main__":
    main()
