"""
Fetch USDTWD daily FX rate from Yahoo Finance and upsert into fx_rates.

- One Yahoo call returns multi-year history
- Backfills since 2018-01-01 on first run; subsequent runs only update
  recent days
"""

import os
import sys
import time
import urllib.parse
import urllib.request
import json
from datetime import datetime, timedelta, timezone
from supabase import create_client, Client

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

PAIR = "USDTWD"
YAHOO_SYMBOL = "USDTWD=X"
BACKFILL_START = datetime(2018, 1, 1, tzinfo=timezone.utc)
USER_AGENT = "Mozilla/5.0 (compatible; fx-fetcher/1.0)"


def fetch_yahoo(period1: int, period2: int) -> list[dict]:
    url = (
        "https://query1.finance.yahoo.com/v8/finance/chart/"
        f"{urllib.parse.quote(YAHOO_SYMBOL)}"
        f"?period1={period1}&period2={period2}&interval=1d"
    )
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read())
    result = (data.get("chart") or {}).get("result") or []
    if not result:
        print("No Yahoo data returned")
        return []
    r = result[0]
    timestamps = r.get("timestamp") or []
    closes = ((r.get("indicators") or {}).get("quote") or [{}])[0].get("close") or []
    rows = []
    for ts, close in zip(timestamps, closes):
        if close is None:
            continue
        d = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
        rows.append({"pair": PAIR, "trade_date": d, "close_rate": round(close, 6)})
    return rows


def main():
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        print("ERROR: SUPABASE_URL and SUPABASE_SERVICE_KEY must be set.")
        sys.exit(1)

    supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

    # Determine fetch range based on existing data
    existing = (
        supabase.table("fx_rates")
        .select("trade_date")
        .eq("pair", PAIR)
        .order("trade_date", desc=False)
        .limit(1)
        .execute()
    )
    today = datetime.now(timezone.utc)
    earliest = existing.data[0]["trade_date"] if existing.data else None
    earliest_dt = datetime.strptime(earliest, "%Y-%m-%d").replace(tzinfo=timezone.utc) if earliest else None

    if earliest_dt and earliest_dt <= BACKFILL_START:
        # Already fully backfilled — only fetch recent ~10 days for incremental
        from_dt = today - timedelta(days=10)
        print(f"Incremental fetch from {from_dt.strftime('%Y-%m-%d')}")
    else:
        from_dt = BACKFILL_START
        print(f"Full backfill from {from_dt.strftime('%Y-%m-%d')}")

    rows = fetch_yahoo(int(from_dt.timestamp()), int(today.timestamp()))
    if not rows:
        print("Nothing to upsert.")
        return
    print(f"Fetched {len(rows)} rows; sample latest: {rows[-1]}")

    # Upsert in batches to be safe (Supabase REST default max ~1000)
    BATCH = 500
    upserted = 0
    for i in range(0, len(rows), BATCH):
        chunk = rows[i:i + BATCH]
        resp = supabase.table("fx_rates").upsert(
            chunk, on_conflict="pair,trade_date"
        ).execute()
        upserted += len(resp.data or [])
        time.sleep(0.2)
    print(f"Upserted {upserted} rows total.")


if __name__ == "__main__":
    main()
