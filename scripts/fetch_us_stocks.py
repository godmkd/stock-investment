"""
Fetch US stock daily data and upsert into Supabase.

- Reads tracked tickers from inv_user_settings (us_prices keys)
- Uses Yahoo Finance for historical data (no API key needed)
- Uses Finnhub quote API as fallback for today's price
- Backfills last 6 months on first run, daily updates thereafter
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

API_DELAY_SECONDS = 0.5
BACKFILL_MONTHS = 6

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_finnhub_key(supabase: Client) -> str:
    if FINNHUB_KEY:
        return FINNHUB_KEY
    try:
        resp = supabase.table("inv_user_settings").select("finnhub_key").not_.is_("finnhub_key", "null").limit(1).execute()
        if resp.data and resp.data[0].get("finnhub_key"):
            return resp.data[0]["finnhub_key"]
    except Exception:
        pass
    return ""


def get_tracked_tickers(supabase: Client) -> list[str]:
    tickers = set()
    try:
        resp = supabase.table("us_stock_history").select("ticker").execute()
        if resp.data:
            for row in resp.data:
                tickers.add(row["ticker"])
    except Exception:
        pass
    try:
        resp = supabase.table("inv_user_settings").select("settings").execute()
        if resp.data:
            for row in resp.data:
                settings = row.get("settings") or {}
                us_prices = settings.get("us_prices") or {}
                for key in us_prices:
                    if key.isupper() and 1 <= len(key) <= 5:
                        tickers.add(key)
    except Exception as e:
        print(f"  [WARN] Could not query inv_user_settings: {e}")
    # Service key bypasses RLS — pull tickers users actually traded
    try:
        resp = supabase.table("inv_us_trades").select("ticker").execute()
        if resp.data:
            for row in resp.data:
                tk = (row.get("ticker") or "").strip().upper()
                if tk:
                    tickers.add(tk)
    except Exception as e:
        print(f"  [WARN] Could not query inv_us_trades: {e}")
    return sorted(tickers)


def fetch_yahoo_history(ticker: str, period1: int, period2: int) -> list[dict]:
    """Fetch daily OHLCV from Yahoo Finance (no API key needed)."""
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
    params = {
        "period1": period1,
        "period2": period2,
        "interval": "1d",
        "includePrePost": "false",
    }
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        resp = requests.get(url, params=params, headers=headers, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except (requests.RequestException, ValueError) as exc:
        print(f"  [WARN] Yahoo fetch error for {ticker}: {exc}")
        return []

    result = data.get("chart", {}).get("result", [])
    if not result:
        print(f"  [INFO] No Yahoo data for {ticker}")
        return []

    r = result[0]
    timestamps = r.get("timestamp", [])
    quote = r.get("indicators", {}).get("quote", [{}])[0]
    opens = quote.get("open", [])
    highs = quote.get("high", [])
    lows = quote.get("low", [])
    closes = quote.get("close", [])
    volumes = quote.get("volume", [])

    rows = []
    for i in range(len(timestamps)):
        if closes[i] is None:
            continue
        trade_date = datetime.utcfromtimestamp(timestamps[i]).strftime("%Y-%m-%d")
        rows.append({
            "ticker": ticker,
            "trade_date": trade_date,
            "open_price": round(opens[i], 2) if opens[i] else None,
            "high_price": round(highs[i], 2) if highs[i] else None,
            "low_price": round(lows[i], 2) if lows[i] else None,
            "close_price": round(closes[i], 2) if closes[i] else None,
            "volume": int(volumes[i]) if volumes[i] else None,
        })
    return rows


def fetch_finnhub_quote(ticker: str, api_key: str) -> dict | None:
    """Fetch current quote from Finnhub as fallback."""
    if not api_key:
        return None
    params = {"symbol": ticker, "token": api_key}
    try:
        resp = requests.get("https://finnhub.io/api/v1/quote", params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except (requests.RequestException, ValueError):
        return None

    if not data.get("c") or data["c"] == 0:
        return None

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
        "volume": None,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        print("ERROR: SUPABASE_URL and SUPABASE_SERVICE_KEY must be set.")
        sys.exit(1)

    supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
    api_key = get_finnhub_key(supabase)
    if api_key:
        print(f"Finnhub API key available: {api_key[:4]}...{api_key[-4:]}")
    else:
        print("No Finnhub key, using Yahoo Finance only")

    # Get tracked tickers
    print("=== Fetching tracked US tickers ===")
    tickers = get_tracked_tickers(supabase)
    if not tickers:
        print("No US tickers to track.")
        return
    print(f"Tracking {len(tickers)} tickers: {', '.join(tickers)}")

    today = datetime.utcnow()
    total_upserted = 0

    for ticker in tickers:
        print(f"\n--- {ticker} ---")

        # Check existing data
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
            print(f"  Latest existing: {latest_existing}")

        # Determine range to fetch
        if latest_existing:
            latest_date = datetime.strptime(latest_existing, "%Y-%m-%d")
            days_since = (today - latest_date).days
            if days_since <= 1:
                print(f"  Already up to date, skipping")
                continue
            from_date = latest_date - timedelta(days=1)
        else:
            from_date = today - relativedelta(months=BACKFILL_MONTHS)

        from_ts = int(from_date.timestamp())
        to_ts = int(today.timestamp())
        print(f"  Fetching {from_date.strftime('%Y-%m-%d')} to {today.strftime('%Y-%m-%d')} ...")

        # Try Yahoo Finance first
        rows = fetch_yahoo_history(ticker, from_ts, to_ts)
        time.sleep(API_DELAY_SECONDS)

        # Fallback to Finnhub quote if Yahoo fails
        if not rows and api_key:
            print(f"  Yahoo failed, trying Finnhub quote...")
            quote = fetch_finnhub_quote(ticker, api_key)
            if quote:
                rows = [quote]
            time.sleep(API_DELAY_SECONDS)

        if rows:
            # Upsert in batches
            for i in range(0, len(rows), 200):
                batch = rows[i:i+200]
                upserted = (
                    supabase.table("us_stock_history")
                    .upsert(batch, on_conflict="ticker,trade_date")
                    .execute()
                )
                count = len(upserted.data) if upserted.data else 0
                total_upserted += count
            print(f"  Upserted {len(rows)} rows (latest: {rows[-1]['trade_date']} close={rows[-1]['close_price']})")
        else:
            print("  (no data)")

    print(f"\n=== Done. Total rows upserted: {total_upserted} ===")


if __name__ == "__main__":
    main()
