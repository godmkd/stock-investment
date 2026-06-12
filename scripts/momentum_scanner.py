"""
Daily momentum scanner for Taiwan and US markets.

Usage:
    python scripts/momentum_scanner.py tw
    python scripts/momentum_scanner.py us
    python scripts/momentum_scanner.py tw --limit 20 --dry-run

Environment:
    DISCORD_WEBHOOK_URL  — webhook to push top-20 results into.
    SCANNER_TEST_SYMBOLS — comma-separated list to override the universe
                           (useful for local smoke tests).

The scanner:
  1. Builds the market's universe via momentum_universes.
  2. Downloads ~6 months of daily OHLCV via yfinance (batched).
  3. Computes per-symbol momentum indicators.
  4. Filters illiquid / micro-cap / data-sparse names.
  5. Ranks by a composite momentum score and takes top N.
  6. Formats an embed-style Discord message and POSTs it to the webhook.
"""

from __future__ import annotations

import argparse
import logging
import math
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Iterable

import pandas as pd
import requests
import yfinance as yf

import momentum_universes as mu

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

MARKET_CONFIG = {
    "tw": {
        "benchmark": "^TWII",
        "display_name": "台股",
        "tz": timezone(timedelta(hours=8)),
        "min_close": 5.0,           # NTD
        "min_dollar_volume": 20_000_000,  # NTD per day, ~$650k USD
        "min_market_cap": 50_000_000_000,  # 500 億 TWD
        "chunk_size": 80,           # yfinance chunk size — TWSE is slower
        "currency": "TWD",
    },
    "us": {
        "benchmark": "^GSPC",
        "display_name": "美股",
        "tz": timezone(timedelta(hours=-4)),  # ET (rough — doesn't track DST exactly, OK for stamp)
        "min_close": 5.0,           # USD
        "min_dollar_volume": 5_000_000,  # USD per day
        "min_market_cap": 5_000_000_000,  # 50 億 USD
        "chunk_size": 150,
        "currency": "USD",
    },
}

# How many top-ranked candidates to fetch market cap for. Must be large enough
# that we still have ≥20 names after the market-cap filter, even when many
# momentum-strong small caps don't meet the cap floor.
MARKET_CAP_CANDIDATE_POOL = 300

CATEGORY_LABELS = {
    "tw_stock": "個股",
    "tw_etf_regular": "ETF",
    "tw_etf_leveraged": "ETF·槓反",
    "tw_etf_active": "ETF·主動",
    "us_sp500": "S&P500",
    "us_nasdaq100": "Nasdaq100",
    "us_broad": "US 廣股",
    "us_etf_sector": "Sector ETF",
    "us_etf_thematic": "Theme ETF",
    "us_adr": "ADR",
}

# Scoring weights — sum doesn't need to be 1, just relative.
WEIGHTS = {
    "ret_20d_rank": 1.5,
    "ret_60d_rank": 1.5,
    "vol_ratio_rank": 1.0,
    "near_high_score": 1.0,
    "ma_alignment_score": 1.0,
    "rel_strength_rank": 1.5,
}

# ---------------------------------------------------------------------------
# Indicator computation
# ---------------------------------------------------------------------------

@dataclass
class SymbolMetrics:
    symbol: str
    name: str
    category: str
    last_close: float
    prev_close: float
    pct_1d: float
    ret_20d: float
    ret_60d: float
    ma20: float
    ma60: float
    ma120: float
    above_ma20: bool
    above_ma60: bool
    above_ma120: bool
    high_60d: float
    dist_from_high: float  # negative number, close to 0 = near high
    avg_vol_5d: float
    avg_vol_20d: float
    vol_ratio: float
    avg_dollar_vol_20d: float
    last_dollar_vol: float   # 最近一個交易日的成交額
    rel_strength_60d: float  # ret_60d minus benchmark's ret_60d


def compute_metrics_for_symbol(
    symbol: str,
    name: str,
    category: str,
    df: pd.DataFrame,
    bench_ret_60d: float,
) -> SymbolMetrics | None:
    """Compute momentum metrics from a single-symbol OHLCV DataFrame.

    df must have columns Open/High/Low/Close/Volume, indexed by date ascending.
    Returns None if data is insufficient.
    """
    if df is None or df.empty:
        return None
    df = df.dropna(subset=["Close", "Volume"])
    if len(df) < 65:
        return None  # need at least 60d + buffer

    close = df["Close"]
    volume = df["Volume"]

    last_close = float(close.iloc[-1])
    prev_close = float(close.iloc[-2])
    if last_close <= 0 or prev_close <= 0:
        return None

    pct_1d = (last_close / prev_close - 1.0)
    ret_20d = (last_close / float(close.iloc[-21]) - 1.0) if len(close) >= 21 else float("nan")
    ret_60d = (last_close / float(close.iloc[-61]) - 1.0) if len(close) >= 61 else float("nan")

    ma20 = float(close.tail(20).mean())
    ma60 = float(close.tail(60).mean())
    ma120 = float(close.tail(120).mean()) if len(close) >= 120 else float("nan")

    high_60d = float(close.tail(60).max())
    dist_from_high = last_close / high_60d - 1.0  # ≤ 0

    avg_vol_5d = float(volume.tail(5).mean())
    avg_vol_20d = float(volume.tail(20).mean())
    vol_ratio = avg_vol_5d / avg_vol_20d if avg_vol_20d > 0 else float("nan")

    # Dollar volume proxy: use close * volume avg
    avg_dollar_vol_20d = float((close.tail(20) * volume.tail(20)).mean())
    last_dollar_vol = float(close.iloc[-1] * volume.iloc[-1])

    rel_strength_60d = ret_60d - bench_ret_60d if not math.isnan(ret_60d) else float("nan")

    return SymbolMetrics(
        symbol=symbol,
        name=name,
        category=category,
        last_close=last_close,
        prev_close=prev_close,
        pct_1d=pct_1d,
        ret_20d=ret_20d,
        ret_60d=ret_60d,
        ma20=ma20,
        ma60=ma60,
        ma120=ma120,
        above_ma20=last_close > ma20,
        above_ma60=last_close > ma60,
        above_ma120=(not math.isnan(ma120)) and last_close > ma120,
        high_60d=high_60d,
        dist_from_high=dist_from_high,
        avg_vol_5d=avg_vol_5d,
        avg_vol_20d=avg_vol_20d,
        vol_ratio=vol_ratio,
        avg_dollar_vol_20d=avg_dollar_vol_20d,
        last_dollar_vol=last_dollar_vol,
        rel_strength_60d=rel_strength_60d,
    )


def compute_composite_score(rows: list[SymbolMetrics]) -> pd.DataFrame:
    """Turn raw metrics into ranked DataFrame with a composite score.

    Each component is converted to a 0..1 percentile rank within the surviving universe
    (so the score is normalized to the day's distribution rather than absolute thresholds).
    """
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame([row.__dict__ for row in rows])

    # Percentile ranks (higher = better)
    df["ret_20d_rank"] = df["ret_20d"].rank(pct=True, na_option="bottom")
    df["ret_60d_rank"] = df["ret_60d"].rank(pct=True, na_option="bottom")
    df["vol_ratio_rank"] = df["vol_ratio"].rank(pct=True, na_option="bottom")
    df["rel_strength_rank"] = df["rel_strength_60d"].rank(pct=True, na_option="bottom")

    # Near-high score: maps dist_from_high (≤ 0) to (1, 0).
    # 0% from high → 1.0,  -5% → 0.5,  -10% or worse → 0.0.
    df["near_high_score"] = (1.0 + df["dist_from_high"] / 0.10).clip(0.0, 1.0)

    # MA alignment: 0..1 = (count of MAs above) / 3
    df["ma_alignment_score"] = (
        df["above_ma20"].astype(int)
        + df["above_ma60"].astype(int)
        + df["above_ma120"].astype(int)
    ) / 3.0

    df["score"] = sum(df[col] * w for col, w in WEIGHTS.items())
    # normalize to 0..100 for human readability
    max_possible = sum(WEIGHTS.values())
    df["score"] = (df["score"] / max_possible * 100).round(1)

    return df.sort_values("score", ascending=False).reset_index(drop=True)


def reason_flags(row: pd.Series) -> list[str]:
    """Generate human-readable reasons for why a stock made the top list."""
    flags: list[str] = []
    if row.get("near_high_score", 0) >= 0.8:
        flags.append("逼近60日新高")
    if row.get("above_ma20") and row.get("above_ma60") and row.get("above_ma120"):
        flags.append("均線多頭排列")
    if row.get("vol_ratio_rank", 0) >= 0.9:
        flags.append("量能爆發")
    if row.get("ret_20d_rank", 0) >= 0.9:
        flags.append(f"20日漲幅{row['ret_20d']*100:+.1f}%")
    if row.get("ret_60d_rank", 0) >= 0.9:
        flags.append(f"60日漲幅{row['ret_60d']*100:+.1f}%")
    if row.get("rel_strength_rank", 0) >= 0.9:
        flags.append(f"領先大盤{row['rel_strength_60d']*100:+.1f}%")
    return flags

# ---------------------------------------------------------------------------
# Data fetching
# ---------------------------------------------------------------------------

def chunked(seq: list, n: int) -> Iterable[list]:
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def download_history(symbols: list[str], chunk_size: int) -> dict[str, pd.DataFrame]:
    """Download ~6 months of daily history for `symbols` in chunks.

    Returns dict symbol → OHLCV DataFrame (date ascending).
    Symbols that fail to download silently get skipped.
    """
    out: dict[str, pd.DataFrame] = {}
    total = len(symbols)
    for i, batch in enumerate(chunked(symbols, chunk_size), start=1):
        log.info("yfinance batch %d/%d (%d symbols)",
                 i, math.ceil(total / chunk_size), len(batch))
        try:
            data = yf.download(
                tickers=batch,
                period="6mo",
                interval="1d",
                group_by="ticker",
                auto_adjust=True,
                progress=False,
                threads=True,
            )
        except Exception as e:
            log.warning("  batch failed: %s", e)
            continue

        if data is None or data.empty:
            continue

        # Multi-ticker download: top-level columns are tickers.
        # Single-ticker: data is a simple frame.
        if isinstance(data.columns, pd.MultiIndex):
            for sym in batch:
                if sym not in data.columns.get_level_values(0):
                    continue
                sub = data[sym].dropna(how="all")
                if not sub.empty:
                    out[sym] = sub
        else:
            sym = batch[0]
            sub = data.dropna(how="all")
            if not sub.empty:
                out[sym] = sub
    return out


def fetch_market_cap(symbol: str) -> float | None:
    """Return market cap in the symbol's quote currency (TWD for .TW, USD for US).

    Uses yfinance's lightweight fast_info first; falls back to shares × price
    if market_cap isn't reported. Returns None if neither is available.
    """
    try:
        fi = yf.Ticker(symbol).fast_info
        # fast_info uses camelCase: marketCap, lastPrice, shares
        mc = fi.get("marketCap") or fi.get("market_cap")
        if mc and mc > 0:
            return float(mc)
        shares = fi.get("shares")
        price = fi.get("lastPrice") or fi.get("last_price")
        if shares and price and shares > 0 and price > 0:
            return float(shares) * float(price)
    except Exception as e:
        log.debug("market cap fetch failed for %s: %s", symbol, e)
    return None


def annotate_market_caps(df: pd.DataFrame, max_workers: int = 12) -> pd.DataFrame:
    """Fetch market caps for every symbol in df in parallel, attach as a new column."""
    import concurrent.futures
    symbols = df["symbol"].tolist()
    log.info("Fetching market cap for %d candidates ...", len(symbols))
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
        caps = list(ex.map(fetch_market_cap, symbols))
    df = df.copy()
    df["market_cap"] = caps
    n_with = sum(1 for c in caps if c is not None)
    log.info("  → got market cap for %d/%d", n_with, len(symbols))
    return df


def _is_etf_category(category: str) -> bool:
    return "etf" in category


def format_market_cap(mc: float | None, currency: str) -> str:
    if mc is None or pd.isna(mc):
        return "—"
    if currency == "TWD":
        return f"{mc / 1e8:.0f}億"
    # USD
    if mc >= 1e12:
        return f"{mc / 1e12:.1f}T"
    if mc >= 1e9:
        return f"{mc / 1e9:.1f}B"
    return f"{mc / 1e6:.0f}M"


def fetch_benchmark_60d_return(symbol: str) -> float:
    """Return the benchmark's 60-trading-day return."""
    try:
        df = yf.download(symbol, period="6mo", interval="1d",
                         auto_adjust=True, progress=False)
        if df is None or df.empty or len(df) < 61:
            return 0.0
        close = df["Close"].dropna()
        # Newer yfinance returns a 1-col DataFrame for single-ticker downloads;
        # squeeze it down to a Series so .iloc[-1] yields a scalar.
        if hasattr(close, "squeeze"):
            close = close.squeeze()
        return float(close.iloc[-1]) / float(close.iloc[-61]) - 1.0
    except Exception as e:
        log.warning("benchmark fetch failed: %s", e)
        return 0.0


# ---------------------------------------------------------------------------
# Discord push
# ---------------------------------------------------------------------------

DISCORD_MAX_EMBEDS = 10
DISCORD_MAX_FIELDS_PER_EMBED = 25
DISCORD_MAX_TOTAL_CHARS = 6000  # per message


def format_money(v: float, currency: str) -> str:
    if currency == "TWD":
        return f"${v:,.2f}"
    return f"${v:,.2f}"


def build_discord_payload(market: str, ranked: pd.DataFrame, limit: int) -> dict:
    cfg = MARKET_CONFIG[market]
    now = datetime.now(cfg["tz"]).strftime("%Y-%m-%d %H:%M %Z")
    top = ranked.head(limit)

    title = f"📈 {cfg['display_name']} 動能 Top {limit}  ·  {now}"

    if top.empty:
        return {"content": f"{title}\n\n_（今日無資料或全部被過濾）_"}

    # Build a table-style message in markdown — Discord renders code blocks monospaced.
    header_lines = [
        f"**{title}**",
        f"_掃描 {len(ranked)} 檔合格標的_",
        "",
    ]

    entries = []
    for i, (_, row) in enumerate(top.iterrows(), start=1):
        sym_disp = row["symbol"].replace(".TW", "")
        cat = CATEGORY_LABELS.get(row["category"], row["category"])
        flags = reason_flags(row)
        flag_str = " · ".join(flags) if flags else "綜合動能強"
        mc_str = format_market_cap(row.get("market_cap"), cfg["currency"])
        turnover_str = format_market_cap(row.get("last_dollar_vol"), cfg["currency"])
        entry = (
            f"**{i:>2}. {sym_disp}** {row['name']}  `[{cat}]`\n"
            f"     收盤 {format_money(row['last_close'], cfg['currency'])} "
            f"({row['pct_1d']*100:+.2f}%)   成交額 {turnover_str}   市值 {mc_str}   score {row['score']:.1f}\n"
            f"     ✦ {flag_str}"
        )
        entries.append(entry)

    # Separate header and each entry by blank lines so the splitter in
    # post_to_discord can pack them into ≤2000-char Discord messages.
    content = "\n".join(header_lines).rstrip() + "\n\n" + "\n\n".join(entries)

    return {"content": content}


def post_to_discord(webhook_url: str, payload: dict) -> None:
    """POST a payload to a Discord webhook. Splits long content across multiple messages."""
    content = payload.get("content", "")
    chunks: list[str] = []
    # Split on double-newline to keep entries intact.
    parts = content.split("\n\n")
    current = ""
    for p in parts:
        sep = "\n\n" if current else ""
        if len(current) + len(sep) + len(p) > 1900:
            if current:
                chunks.append(current)
            current = p
        else:
            current += sep + p
    if current:
        chunks.append(current)

    for i, ch in enumerate(chunks):
        body = {"content": ch}
        r = requests.post(webhook_url, json=body, timeout=15)
        if r.status_code >= 300:
            log.error("Discord webhook returned %s: %s", r.status_code, r.text[:200])
            r.raise_for_status()
        log.info("posted chunk %d/%d (%d chars)", i + 1, len(chunks), len(ch))


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def run(market: str, limit: int, dry_run: bool, test_symbols: list[str] | None) -> int:
    cfg = MARKET_CONFIG[market]
    log.info("=== %s scanner starting ===", cfg["display_name"])

    if test_symbols:
        universe = [{"symbol": s.strip(), "name": s.strip(), "category": "tw_stock" if market == "tw" else "us_sp500"}
                    for s in test_symbols if s.strip()]
        log.info("Using test universe override: %d symbols", len(universe))
    elif market == "tw":
        universe = mu.build_tw_universe()
    else:
        universe = mu.build_us_universe()

    log.info("Universe size: %d", len(universe))
    symbols = [u["symbol"] for u in universe]
    name_map = {u["symbol"]: u["name"] for u in universe}
    cat_map = {u["symbol"]: u["category"] for u in universe}

    bench_ret_60d = fetch_benchmark_60d_return(cfg["benchmark"])
    log.info("Benchmark %s 60d return: %+.2f%%", cfg["benchmark"], bench_ret_60d * 100)

    histories = download_history(symbols, cfg["chunk_size"])
    log.info("Got history for %d/%d symbols", len(histories), len(symbols))

    rows: list[SymbolMetrics] = []
    for sym, df in histories.items():
        m = compute_metrics_for_symbol(sym, name_map[sym], cat_map[sym], df, bench_ret_60d)
        if m is None:
            continue
        # Liquidity filters
        if m.last_close < cfg["min_close"]:
            continue
        if m.avg_dollar_vol_20d < cfg["min_dollar_volume"]:
            continue
        rows.append(m)

    log.info("After price/liquidity filters: %d symbols qualify", len(rows))
    ranked = compute_composite_score(rows)

    if ranked.empty:
        log.warning("No qualifying symbols; nothing to push.")
        return 0

    # Market cap filter — fetch caps for top N momentum candidates, drop those
    # below the cap floor (ETFs are exempt; user explicitly asked to include them).
    candidates = ranked.head(MARKET_CAP_CANDIDATE_POOL).copy()
    candidates = annotate_market_caps(candidates)

    cap_floor = cfg["min_market_cap"]
    def passes_mcap(row: pd.Series) -> bool:
        if _is_etf_category(row["category"]):
            return True
        mc = row.get("market_cap")
        return mc is not None and not pd.isna(mc) and mc >= cap_floor

    pre = len(candidates)
    candidates = candidates[candidates.apply(passes_mcap, axis=1)].reset_index(drop=True)
    log.info("After market cap filter (≥ %s): %d/%d remain",
             format_market_cap(cap_floor, cfg["currency"]), len(candidates), pre)

    if candidates.empty:
        log.warning("All candidates filtered out by market cap; nothing to push.")
        return 0

    # Persist top-N codes to JSON so downstream scanners (e.g. chip_scanner)
    # can pick them up without re-running the momentum pipeline.
    top_codes_path = os.environ.get(
        "SCANNER_TOP_CODES_PATH",
        f"/tmp/momentum_top_codes_{market}.json",
    )
    top_codes_n = int(os.environ.get("SCANNER_TOP_CODES_N", "50"))
    top_n = candidates.head(top_codes_n)
    top_entries = [
        {
            "code": row["symbol"].replace(".TW", "").replace(".TWO", ""),
            "name": row["name"],
            "score": float(row["score"]),
        }
        for _, row in top_n.iterrows()
    ]
    try:
        with open(top_codes_path, "w", encoding="utf-8") as f:
            import json as _json
            _json.dump({
                "market": market,
                "generated_at": datetime.now(cfg["tz"]).isoformat(),
                "entries": top_entries,
            }, f, ensure_ascii=False, indent=2)
        log.info("wrote top-%d entries to %s", len(top_entries), top_codes_path)
    except OSError as e:
        log.warning("could not persist top entries to %s: %s", top_codes_path, e)

    payload = build_discord_payload(market, candidates, limit)

    # Persist the top N (same set as Discord push) to Supabase scanner_results
    # so the 主題動能 frontend tab can read history.
    try:
        upsert_momentum_results(market, candidates.head(limit), cfg)
    except Exception as e:
        log.warning("scanner_results upsert failed: %s", e)

    if dry_run:
        log.info("--- DRY RUN ---\n%s", payload["content"])
        return 0

    webhook = os.environ.get("DISCORD_WEBHOOK_URL")
    if not webhook:
        log.error("DISCORD_WEBHOOK_URL not set; cannot post.")
        return 1

    post_to_discord(webhook, payload)
    log.info("Done.")
    return 0


def upsert_momentum_results(market: str, top: pd.DataFrame, cfg: dict) -> None:
    """Push the day's top-N momentum rows into Supabase scanner_results."""
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_KEY")
    if not url or not key:
        log.info("SUPABASE_URL/SUPABASE_SERVICE_KEY not set, skipping DB upsert")
        return
    from supabase import create_client
    sb = create_client(url, key)
    scan_date = datetime.now(cfg["tz"]).date().isoformat()
    rows = []
    for i, (_, row) in enumerate(top.iterrows(), start=1):
        ticker = row["symbol"].replace(".TW", "").replace(".TWO", "")
        flags = reason_flags(row)
        payload = {
            "category": row.get("category"),
            "last_close": float(row.get("last_close", 0)),
            "pct_1d": float(row.get("pct_1d", 0)),
            "ret_20d": float(row.get("ret_20d", 0)) if not pd.isna(row.get("ret_20d", float("nan"))) else None,
            "ret_60d": float(row.get("ret_60d", 0)) if not pd.isna(row.get("ret_60d", float("nan"))) else None,
            "rel_strength_60d": float(row.get("rel_strength_60d", 0)) if not pd.isna(row.get("rel_strength_60d", float("nan"))) else None,
            "market_cap": float(row.get("market_cap")) if row.get("market_cap") is not None and not pd.isna(row.get("market_cap")) else None,
            "last_dollar_vol": float(row.get("last_dollar_vol", 0)) if not pd.isna(row.get("last_dollar_vol", float("nan"))) else None,
            "above_ma20": bool(row.get("above_ma20", False)),
            "above_ma60": bool(row.get("above_ma60", False)),
            "above_ma120": bool(row.get("above_ma120", False)),
            "flags": flags,
        }
        rows.append({
            "market": market,
            "scan_type": "momentum",
            "scan_date": scan_date,
            "rank": i,
            "ticker": ticker,
            "name": str(row.get("name", "")),
            "score": float(row.get("score", 0)),
            "payload": payload,
        })
    if not rows:
        return
    # Clear today's momentum rows for this market so re-runs don't pile up stale rank slots
    sb.table("scanner_results").delete().eq("market", market).eq("scan_type", "momentum").eq("scan_date", scan_date).execute()
    sb.table("scanner_results").upsert(rows, on_conflict="market,scan_type,scan_date,ticker").execute()
    log.info("upserted %d momentum rows to scanner_results", len(rows))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("market", choices=["tw", "us"])
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--dry-run", action="store_true",
                        help="Print formatted output instead of posting to Discord")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    test_override = os.environ.get("SCANNER_TEST_SYMBOLS")
    test_symbols = test_override.split(",") if test_override else None

    return run(args.market, args.limit, args.dry_run, test_symbols)


if __name__ == "__main__":
    sys.exit(main())
