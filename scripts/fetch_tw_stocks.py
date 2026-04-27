"""
Fetch Taiwan stock daily data from TWSE and upsert into Supabase.

- Reads watchlist from tw_stock_watchlist table
- Fetches current month + backfills last 6 months if missing
- Parses ROC date format (e.g. 115/04/17 → 2026-04-17)
- Upserts into tw_stock_history with ON CONFLICT handling
"""

import os
import sys
import time
import requests
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from supabase import create_client, Client

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

TWSE_STOCK_DAY_URL = "https://www.twse.com.tw/exchangeReport/STOCK_DAY"
API_DELAY_SECONDS = 1.5  # be polite to TWSE
BACKFILL_MONTHS = 6

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def roc_to_date(roc_str: str) -> str:
    """Convert ROC date string like '115/04/17' to ISO date '2026-04-17'."""
    parts = roc_str.strip().split("/")
    if len(parts) != 3:
        raise ValueError(f"Unexpected ROC date format: {roc_str}")
    year = int(parts[0]) + 1911
    month = int(parts[1])
    day = int(parts[2])
    return f"{year:04d}-{month:02d}-{day:02d}"


def parse_number(value: str) -> float | None:
    """Parse a TWSE numeric string (may contain commas or '--')."""
    cleaned = value.strip().replace(",", "")
    if cleaned in ("--", ""):
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def parse_volume(value: str) -> int | None:
    """Parse volume string to int."""
    cleaned = value.strip().replace(",", "")
    if cleaned in ("--", ""):
        return None
    try:
        return int(cleaned)
    except ValueError:
        return None


def fetch_twse_month(stock_code: str, year: int, month: int) -> list[dict]:
    """
    Fetch one month of daily data for a stock from TWSE.
    Returns list of row dicts ready for Supabase upsert.
    """
    date_str = f"{year:04d}{month:02d}01"
    params = {
        "response": "json",
        "date": date_str,
        "stockNo": stock_code,
    }

    try:
        resp = requests.get(TWSE_STOCK_DAY_URL, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as exc:
        print(f"  [WARN] HTTP error for {stock_code} {year}/{month:02d}: {exc}")
        return []
    except ValueError:
        print(f"  [WARN] Invalid JSON for {stock_code} {year}/{month:02d}")
        return []

    if data.get("stat") != "OK" or "data" not in data:
        print(f"  [INFO] No data for {stock_code} {year}/{month:02d} (stat={data.get('stat', 'N/A')})")
        return []

    rows = []
    for row in data["data"]:
        # TWSE columns: 日期, 成交股數, 成交金額, 開盤價, 最高價, 最低價, 收盤價, 漲跌價差, 成交筆數
        if len(row) < 7:
            continue
        try:
            trade_date = roc_to_date(row[0])
        except ValueError:
            continue

        rows.append({
            "stock_code": stock_code,
            "trade_date": trade_date,
            "open_price": parse_number(row[3]),
            "high_price": parse_number(row[4]),
            "low_price": parse_number(row[5]),
            "close_price": parse_number(row[6]),
            "volume": parse_volume(row[1]),
        })

    return rows


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        print("ERROR: SUPABASE_URL and SUPABASE_SERVICE_KEY must be set.")
        sys.exit(1)

    supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

    # 1. Get watchlist
    print("=== Fetching watchlist ===")
    watchlist_resp = supabase.table("tw_stock_watchlist").select("stock_code, stock_name").execute()
    watchlist = watchlist_resp.data
    if not watchlist:
        print("Watchlist is empty. Nothing to do.")
        return

    stock_codes = [w["stock_code"] for w in watchlist]
    stock_names = {w["stock_code"]: w.get("stock_name", "") for w in watchlist}
    print(f"Tracking {len(stock_codes)} stocks: {', '.join(stock_codes)}")

    # 2. Determine months to fetch
    today = datetime.now()
    current_month = today.replace(day=1)
    months_to_fetch = []
    for i in range(BACKFILL_MONTHS, -1, -1):
        m = current_month - relativedelta(months=i)
        months_to_fetch.append((m.year, m.month))

    # 3. For each stock, check existing data and decide what to backfill
    total_upserted = 0
    for stock_code in stock_codes:
        name = stock_names.get(stock_code, "")
        print(f"\n--- {stock_code} {name} ---")

        # Check earliest existing date to skip already-backfilled months
        existing_resp = (
            supabase.table("tw_stock_history")
            .select("trade_date")
            .eq("stock_code", stock_code)
            .order("trade_date")
            .limit(1)
            .execute()
        )
        earliest_existing = None
        if existing_resp.data:
            earliest_existing = existing_resp.data[0]["trade_date"]
            print(f"  Earliest existing record: {earliest_existing}")

        for year, month in months_to_fetch:
            month_start = f"{year:04d}-{month:02d}-01"

            # If we already have data from before this month, skip backfill
            # but always fetch the current month (may have new trading days)
            is_current_month = (year == today.year and month == today.month)
            if earliest_existing and month_start > earliest_existing and not is_current_month:
                # We have older data, and this isn't the current month –
                # still fetch in case there are gaps. But skip if month is
                # fully in the past and we already have data for it.
                count_resp = (
                    supabase.table("tw_stock_history")
                    .select("id", count="exact")
                    .eq("stock_code", stock_code)
                    .gte("trade_date", month_start)
                    .lt("trade_date", f"{year:04d}-{month + 1:02d}-01" if month < 12 else f"{year + 1:04d}-01-01")
                    .execute()
                )
                if count_resp.count and count_resp.count >= 15:
                    print(f"  Skipping {year}/{month:02d} — already has {count_resp.count} rows")
                    continue

            print(f"  Fetching {year}/{month:02d} ...", end=" ", flush=True)
            rows = fetch_twse_month(stock_code, year, month)

            if not rows:
                print("(no rows)")
                time.sleep(API_DELAY_SECONDS)
                continue

            # Upsert into Supabase
            upserted = (
                supabase.table("tw_stock_history")
                .upsert(rows, on_conflict="stock_code,trade_date")
                .execute()
            )
            count = len(upserted.data) if upserted.data else 0
            total_upserted += count
            print(f"upserted {count} rows")

            time.sleep(API_DELAY_SECONDS)

    print(f"\n=== Done. Total rows upserted: {total_upserted} ===")


if __name__ == "__main__":
    main()
