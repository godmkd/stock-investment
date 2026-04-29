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
TWSE_T86_URL = "https://www.twse.com.tw/fund/T86"  # 三大法人買賣超
TWSE_EPS_URL = "https://mops.twse.com.tw/mops/web/ajax_t163sb04"  # EPS
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


def fetch_institutional_data(date_str: str, stock_codes: list[str]) -> dict[str, dict]:
    """
    Fetch 三大法人買賣超 for a given date (YYYYMMDD format).
    Returns dict: stock_code -> { foreign_buy, foreign_sell, foreign_net, trust_buy, trust_sell, trust_net, dealer_net, institutional_net }
    """
    params = {"response": "json", "date": date_str, "selectType": "ALL"}
    try:
        resp = requests.get(TWSE_T86_URL, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except (requests.RequestException, ValueError) as exc:
        print(f"  [WARN] T86 fetch error for {date_str}: {exc}")
        return {}

    if data.get("stat") != "OK" or "data" not in data:
        return {}

    result = {}
    codes_set = set(stock_codes)
    for row in data["data"]:
        code = row[0].strip()
        if code not in codes_set:
            continue
        # T86 columns: 證券代號, 證券名稱, 外陸資買(不含自營), 外陸資賣(不含自營), 外陸資淨買,
        #   外資自營商買, 外資自營商賣, 外資自營商淨買, 投信買, 投信賣, 投信淨買,
        #   自營商淨買賣, 自營商(自行)買, 自營商(自行)賣, 自營商(自行)淨買賣,
        #   自營商(避險)買, 自營商(避險)賣, 自營商(避險)淨買賣, 三大法人淨買超
        try:
            result[code] = {
                "foreign_buy": parse_volume(row[2]),
                "foreign_sell": parse_volume(row[3]),
                "foreign_net": parse_volume(row[4]),
                "trust_buy": parse_volume(row[8]),
                "trust_sell": parse_volume(row[9]),
                "trust_net": parse_volume(row[10]),
                "dealer_net": parse_volume(row[11]),
                "institutional_net": parse_volume(row[18]) if len(row) > 18 else parse_volume(row[11]),
            }
        except (IndexError, TypeError):
            continue
    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        print("ERROR: SUPABASE_URL and SUPABASE_SERVICE_KEY must be set.")
        sys.exit(1)

    supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

    # 1. Get watchlist (curated list)
    print("=== Fetching watchlist ===")
    watchlist_resp = supabase.table("tw_stock_watchlist").select("stock_code, stock_name").execute()
    watchlist = watchlist_resp.data or []
    stock_names = {w["stock_code"]: w.get("stock_name", "") for w in watchlist}
    print(f"  Watchlist: {len(stock_names)} stocks")

    # 1b. Also pull tickers users actually hold (so new buys auto-track)
    print("=== Pulling tickers from inv_tw_trades ===")
    user_tickers: set[str] = set()
    try:
        # Service key bypasses RLS — sees all users' trades
        trades_resp = supabase.table("inv_tw_trades").select("ticker").execute()
        for t in trades_resp.data or []:
            tk = (t.get("ticker") or "").strip()
            if tk:
                user_tickers.add(tk)
        print(f"  Found {len(user_tickers)} unique tickers in user trades")
    except Exception as e:
        print(f"  [WARN] Could not query inv_tw_trades: {e}")

    all_codes = set(stock_names.keys()) | user_tickers
    if not all_codes:
        print("No stocks to track. Nothing to do.")
        return

    stock_codes = sorted(all_codes)
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

    # 4. Fetch institutional trading data (三大法人) for recent trading days
    print("\n=== Fetching institutional data (三大法人) ===")
    # Fetch last 5 trading days
    for days_ago in range(5):
        d = today - timedelta(days=days_ago)
        if d.weekday() >= 5:  # skip weekends
            continue
        date_str = d.strftime("%Y%m%d")
        print(f"  Fetching T86 for {date_str} ...", end=" ", flush=True)
        inst_data = fetch_institutional_data(date_str, stock_codes)
        if not inst_data:
            print("(no data)")
            time.sleep(API_DELAY_SECONDS)
            continue

        trade_date = d.strftime("%Y-%m-%d")
        updated = 0
        for code, vals in inst_data.items():
            supabase.table("tw_stock_history").update({
                "foreign_buy": vals.get("foreign_buy", 0),
                "foreign_sell": vals.get("foreign_sell", 0),
                "foreign_net": vals.get("foreign_net", 0),
                "trust_buy": vals.get("trust_buy", 0),
                "trust_sell": vals.get("trust_sell", 0),
                "trust_net": vals.get("trust_net", 0),
                "dealer_net": vals.get("dealer_net", 0),
                "institutional_net": vals.get("institutional_net", 0),
            }).eq("stock_code", code).eq("trade_date", trade_date).execute()
            updated += 1
        print(f"updated {updated} stocks")
        time.sleep(API_DELAY_SECONDS)

    print(f"\n=== Done. Total price rows upserted: {total_upserted} ===")


if __name__ == "__main__":
    main()
