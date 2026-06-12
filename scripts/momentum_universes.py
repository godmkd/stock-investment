"""
Build TW and US stock universes for the momentum scanner.

TW universe:
  - All TWSE-listed common stocks (excludes 全額交割, 興櫃, 創新板)
  - 一般 ETF (curated popular list)
  - 槓桿/反向 ETF
  - 主動式 ETF (2024+)

US universe:
  - S&P 500 (Wikipedia)
  - Nasdaq 100 (Wikipedia)
  - Russell 2000 (iShares IWM holdings CSV)
  - Sector SPDR ETFs
  - Thematic ETFs
  - Top 100 ADRs (curated)

All functions return list[dict] with keys: symbol, name, category.
"symbol" is yfinance-compatible: TW uses ".TW" suffix (e.g. "2330.TW").
"""

from __future__ import annotations

import csv
import io
import logging
import re
from typing import Iterable

import requests

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Taiwan
# ---------------------------------------------------------------------------

TWSE_LISTING_URL = "https://openapi.twse.com.tw/v1/opendata/t187ap03_L"
TWSE_STOCK_DAY_ALL_URL = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"
TWSE_HEADERS = {"User-Agent": "Mozilla/5.0 momentum-scanner/1.0"}

# Stocks that are full-cash-delivery (全額交割) or otherwise excluded.
# This is a moving target — TWSE publishes it via a separate API.
# We use STOCK_DAY_ALL which only returns actively traded stocks,
# so 全額交割 with no trading on a given day naturally gets filtered.

# Curated ETF lists — kept short to keep the universe focused on liquid names.
TW_ETF_REGULAR = [
    ("0050", "元大台灣50"),
    ("0056", "元大高股息"),
    ("006208", "富邦台50"),
    ("00878", "國泰永續高股息"),
    ("00713", "元大台灣高息低波"),
    ("00919", "群益台灣精選高息"),
    ("00929", "復華台灣科技優息"),
    ("00940", "元大臺灣價值高息"),
    ("00939", "統一台灣高息動能"),
    ("00936", "台新永續高息中小"),
    ("00701", "國泰股利精選30"),
    ("00692", "富邦公司治理"),
    ("00850", "元大臺灣ESG永續"),
    ("00891", "中信關鍵半導體"),
    ("00892", "富邦台灣半導體"),
    ("0052", "富邦科技"),
    ("00733", "富邦中小"),
    ("00757", "統一FANG+"),
    ("00646", "元大S&P500"),
    ("00662", "富邦NASDAQ"),
    ("00830", "國泰費城半導體"),
    ("00876", "元大全球5G"),
    ("00881", "國泰台灣5G+"),
    ("00885", "富邦越南"),
    ("00888", "永豐台灣ESG"),
    ("00893", "國泰智能電動車"),
    ("00895", "富邦未來車"),
    ("00896", "中信綠能及電動車"),
    ("00900", "富邦特選高股息30"),
    ("00904", "新光臺灣半導體30"),
    ("00905", "FT臺灣Smart"),
    ("00912", "中信臺灣智慧50"),
    ("00915", "凱基優選高股息30"),
    ("00916", "國泰全球品牌50"),
    ("00918", "大華優利高填息30"),
    ("00922", "國泰台灣領袖50"),
    ("00923", "群益台ESG低碳50"),
    ("00927", "群益半導體收益"),
    ("00930", "永豐ESG低碳高息"),
    ("00932", "兆豐永續高息等權"),
    ("00935", "野村臺灣新科技50"),
    ("00937B", "群益ESG投等債20+"),
]

TW_ETF_LEVERAGED_INVERSE = [
    ("00631L", "元大台灣50正2"),
    ("00632R", "元大台灣50反1"),
    ("00633L", "富邦上証正2"),
    ("00634R", "富邦上証反1"),
    ("00637L", "元大滬深300正2"),
    ("00638R", "元大滬深300反1"),
    ("00640L", "富邦日本正2"),
    ("00641R", "富邦日本反1"),
    ("00647L", "元大S&P500正2"),
    ("00648R", "元大S&P500反1"),
    ("00650L", "FH香港正2"),
    ("00651R", "FH香港反1"),
    ("00652", "富邦印度"),
    ("00653L", "富邦印度正2"),
    ("00654R", "富邦印度反1"),
    ("00655L", "國泰中國A50正2"),
    ("00656R", "國泰中國A50反1"),
    ("00663L", "國泰臺灣加權正2"),
    ("00664R", "國泰臺灣加權反1"),
    ("00665L", "富邦恒生國企正2"),
    ("00666R", "富邦恒生國企反1"),
    ("00670L", "富邦NASDAQ正2"),
    ("00671R", "富邦NASDAQ反1"),
    ("00675L", "富邦臺灣加權正2"),
    ("00676R", "富邦臺灣加權反1"),
    ("00680L", "元大美債20正2"),
    ("00681R", "元大美債20反1"),
    ("00685L", "群益臺灣加權正2"),
    ("00686R", "群益臺灣加權反1"),
    ("00688L", "國泰20年美債正2"),
    ("00689R", "國泰20年美債反1"),
    ("00706L", "期元大S&P日圓正2"),
    ("00707R", "期元大S&P日圓反1"),
    ("00708L", "期元大S&P黃金正2"),
    ("00715L", "期街口布蘭特正2"),
]

# 主動式 ETF — 台灣 2024 後開始有真正的主動式 ETF.
# Symbols ending with "A" by convention for some issuers.
TW_ETF_ACTIVE = [
    ("00980A", "凱基台灣優選"),
    ("00981A", "統一台股增長主動"),
    ("00982A", "富邦核心動能主動"),
    ("00983A", "群益全球科技主動"),
    ("00984A", "野村台灣優選主動"),
    ("00985A", "元大台灣優選主動"),
    ("00986A", "國泰台灣優選主動"),
]


def _fetch_json_with_retry(url: str, headers: dict, timeout: int = 30, retries: int = 3,
                           backoff: float = 2.0) -> list | dict:
    """GET a JSON endpoint with retries on empty body / decode errors.

    TWSE OpenAPI occasionally returns an empty 200 response (especially on
    weekends and right at the top of the hour). Wrap json() in retry/backoff
    so a transient blank doesn't kill the whole scanner run.
    """
    import json as _json
    import time as _time
    last_err: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, headers=headers, timeout=timeout)
            r.raise_for_status()
            body = r.text.strip()
            if not body:
                raise ValueError("empty response body")
            return _json.loads(body)
        except (requests.RequestException, ValueError) as e:
            last_err = e
            log.warning("[%s] attempt %d/%d failed: %s", url, attempt, retries, e)
            if attempt < retries:
                _time.sleep(backoff * attempt)
    raise RuntimeError(f"failed to fetch {url} after {retries} attempts: {last_err}")


def fetch_tw_listed_common_stocks() -> list[dict]:
    """Fetch all TWSE-listed common stocks via STOCK_DAY_ALL (the daily-all endpoint).

    Returns list of {symbol: '2330.TW', code: '2330', name: '台積電', category: 'tw_stock'}.
    Filters out anything whose code is non-4-digit (warrants, beneficiary certs, etc).
    """
    log.info("Fetching TWSE listed common stocks ...")
    rows = _fetch_json_with_retry(TWSE_STOCK_DAY_ALL_URL, TWSE_HEADERS)
    if not isinstance(rows, list):
        raise RuntimeError(f"unexpected TWSE payload shape: {type(rows).__name__}")
    out = []
    for row in rows:
        code = (row.get("Code") or "").strip()
        name = (row.get("Name") or "").strip()
        if not re.fullmatch(r"\d{4}", code):
            # exclude warrants (6-digit), ETFs (5-digit, we add manually),
            # and special instruments
            continue
        if not name:
            continue
        # Skip names that signal special status
        if any(tag in name for tag in ("DR", "特")):
            # Taiwan DRs and preferred shares — usually low liquidity
            continue
        out.append({
            "symbol": f"{code}.TW",
            "code": code,
            "name": name,
            "category": "tw_stock",
        })
    log.info("  → %d common stocks", len(out))
    return out


def build_tw_etfs() -> list[dict]:
    """Combine the three curated ETF lists into one universe slice."""
    out = []
    for code, name in TW_ETF_REGULAR:
        out.append({"symbol": f"{code}.TW", "code": code, "name": name, "category": "tw_etf_regular"})
    for code, name in TW_ETF_LEVERAGED_INVERSE:
        out.append({"symbol": f"{code}.TW", "code": code, "name": name, "category": "tw_etf_leveraged"})
    for code, name in TW_ETF_ACTIVE:
        out.append({"symbol": f"{code}.TW", "code": code, "name": name, "category": "tw_etf_active"})
    return out


def build_tw_universe() -> list[dict]:
    """Build the full TW universe: stocks + ETFs, de-duplicated by symbol.

    ETFs first so that for any code that appears in both the curated ETF list
    and the TWSE listed feed (e.g. 0050, 00878), the ETF category wins.
    """
    items = build_tw_etfs() + fetch_tw_listed_common_stocks()
    seen = set()
    deduped = []
    for it in items:
        if it["symbol"] in seen:
            continue
        seen.add(it["symbol"])
        deduped.append(it)
    return deduped


# ---------------------------------------------------------------------------
# United States
# ---------------------------------------------------------------------------

SP500_WIKI = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
NASDAQ100_WIKI = "https://en.wikipedia.org/wiki/Nasdaq-100"
# NASDAQ Trader publishes the canonical US symbol directory nightly.
# We use this in place of a true Russell 2000 holdings download — iShares blocks
# direct CSV fetches without JS-rendered cookies. The trade-off: we get a
# broader universe (all NASDAQ + NYSE common stocks), and rely on the scanner's
# liquidity filter to cut out micro-caps.
NASDAQ_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
NASDAQ_OTHER_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"

US_SECTOR_ETFS = [
    ("XLK", "Technology Select Sector SPDR"),
    ("XLF", "Financial Select Sector SPDR"),
    ("XLE", "Energy Select Sector SPDR"),
    ("XLV", "Health Care Select Sector SPDR"),
    ("XLY", "Consumer Discretionary Select Sector SPDR"),
    ("XLP", "Consumer Staples Select Sector SPDR"),
    ("XLI", "Industrial Select Sector SPDR"),
    ("XLB", "Materials Select Sector SPDR"),
    ("XLU", "Utilities Select Sector SPDR"),
    ("XLRE", "Real Estate Select Sector SPDR"),
    ("XLC", "Communication Services Select Sector SPDR"),
]

US_THEMATIC_ETFS = [
    ("SMH", "VanEck Semiconductor"),
    ("SOXX", "iShares Semiconductor"),
    ("ARKK", "ARK Innovation"),
    ("ARKG", "ARK Genomic Revolution"),
    ("ARKW", "ARK Next Gen Internet"),
    ("ARKF", "ARK Fintech Innovation"),
    ("ARKQ", "ARK Autonomous Tech & Robotics"),
    ("ICLN", "iShares Global Clean Energy"),
    ("TAN", "Invesco Solar"),
    ("LIT", "Global X Lithium & Battery"),
    ("KWEB", "KraneShares CSI China Internet"),
    ("MCHI", "iShares MSCI China"),
    ("EWJ", "iShares MSCI Japan"),
    ("EWY", "iShares MSCI South Korea"),
    ("INDA", "iShares MSCI India"),
    ("EWZ", "iShares MSCI Brazil"),
    ("BOTZ", "Global X Robotics & AI"),
    ("ROBO", "ROBO Global Robotics & Automation"),
    ("HACK", "ETFMG Prime Cyber Security"),
    ("FINX", "Global X FinTech"),
    ("BLOK", "Amplify Transformational Data"),
    ("DRIV", "Global X Autonomous & EV"),
    ("URA", "Global X Uranium"),
    ("GDX", "VanEck Gold Miners"),
    ("SLV", "iShares Silver Trust"),
    ("GLD", "SPDR Gold Shares"),
    ("USO", "United States Oil Fund"),
    ("UNG", "United States Natural Gas Fund"),
    ("TLT", "iShares 20+ Year Treasury Bond"),
    ("HYG", "iShares iBoxx High Yield Corporate Bond"),
    ("IBIT", "iShares Bitcoin Trust"),
    ("FBTC", "Fidelity Wise Origin Bitcoin"),
]

# Curated Top ADRs — well-known foreign companies trading on US exchanges.
US_TOP_ADRS = [
    # Taiwan
    ("TSM", "Taiwan Semiconductor"),
    ("UMC", "United Microelectronics"),
    ("ASX", "ASE Technology Holding"),
    # Europe / semis
    ("ASML", "ASML Holding"),
    ("SAP", "SAP SE"),
    ("NVO", "Novo Nordisk"),
    ("AZN", "AstraZeneca"),
    ("GSK", "GSK plc"),
    ("UL", "Unilever"),
    ("DEO", "Diageo"),
    ("BTI", "British American Tobacco"),
    ("BP", "BP plc"),
    ("SHEL", "Shell plc"),
    ("TTE", "TotalEnergies"),
    ("E", "Eni"),
    ("EQNR", "Equinor"),
    ("RIO", "Rio Tinto"),
    ("BHP", "BHP Group"),
    ("VALE", "Vale"),
    ("STM", "STMicroelectronics"),
    ("NVS", "Novartis"),
    ("RHHBY", "Roche Holding"),
    ("LYG", "Lloyds Banking Group"),
    ("BCS", "Barclays"),
    ("HSBC", "HSBC Holdings"),
    ("UBS", "UBS Group"),
    ("ING", "ING Groep"),
    ("SAN", "Banco Santander"),
    ("BBVA", "BBVA"),
    ("RY", "Royal Bank of Canada"),
    ("TD", "Toronto-Dominion Bank"),
    ("BNS", "Bank of Nova Scotia"),
    ("ERIC", "Ericsson"),
    ("NOK", "Nokia"),
    ("PHG", "Koninklijke Philips"),
    # Japan
    ("TM", "Toyota Motor"),
    ("HMC", "Honda Motor"),
    ("SONY", "Sony Group"),
    ("MUFG", "Mitsubishi UFJ Financial"),
    ("SMFG", "Sumitomo Mitsui Financial"),
    ("MFG", "Mizuho Financial"),
    ("NMR", "Nomura Holdings"),
    ("HTHIY", "Hitachi"),
    # Korea
    ("KB", "KB Financial Group"),
    ("SHG", "Shinhan Financial"),
    ("KEP", "Korea Electric Power"),
    ("PKX", "POSCO Holdings"),
    # China (US-listed Chinese ADRs)
    ("BABA", "Alibaba Group"),
    ("JD", "JD.com"),
    ("PDD", "PDD Holdings"),
    ("BIDU", "Baidu"),
    ("NIO", "NIO Inc"),
    ("LI", "Li Auto"),
    ("XPEV", "XPeng"),
    ("TCOM", "Trip.com"),
    ("NTES", "NetEase"),
    ("BILI", "Bilibili"),
    ("YMM", "Full Truck Alliance"),
    ("ZTO", "ZTO Express"),
    ("BEKE", "KE Holdings"),
    ("TAL", "TAL Education"),
    ("EDU", "New Oriental Education"),
    ("HTHT", "H World Group"),
    ("DIDI", "DiDi Global"),
    # India / SE Asia / LatAm
    ("INFY", "Infosys"),
    ("WIT", "Wipro"),
    ("HDB", "HDFC Bank"),
    ("IBN", "ICICI Bank"),
    ("SE", "Sea Limited"),
    ("GRAB", "Grab Holdings"),
    ("MELI", "MercadoLibre"),
    ("VIST", "Vista Energy"),
    ("PBR", "Petrobras"),
    ("ITUB", "Itaú Unibanco"),
    ("BBD", "Banco Bradesco"),
    ("ABEV", "Ambev"),
    ("STNE", "StoneCo"),
    ("PAGS", "PagSeguro"),
    ("NU", "Nu Holdings"),
    # Israel
    ("CHKP", "Check Point Software"),
    ("CYBR", "CyberArk Software"),
    ("WIX", "Wix.com"),
    ("MNDY", "Monday.com"),
    ("NICE", "Nice Ltd"),
    ("TEVA", "Teva Pharmaceutical"),
    # Other tech / consumer
    ("SHOP", "Shopify"),
    ("SPOT", "Spotify"),
    ("CRWD", "CrowdStrike"),  # technically US but often grouped — keeping if user wants
    ("TCEHY", "Tencent (OTC ADR)"),
    ("PCRFY", "Panasonic"),
    ("MBGAF", "Mercedes-Benz Group"),
    ("BMWYY", "BMW"),
    ("VWAGY", "Volkswagen"),
    ("RACE", "Ferrari"),
    ("STLA", "Stellantis"),
    ("FERG", "Ferguson"),
    ("LIN", "Linde plc"),
    ("CNI", "Canadian National Railway"),
    ("CP", "Canadian Pacific Kansas City"),
    ("ENB", "Enbridge"),
    ("SU", "Suncor Energy"),
]


def _wiki_table_symbols(url: str, symbol_col: str, name_col: str | None = None) -> list[tuple[str, str]]:
    """Fetch a Wikipedia page (with User-Agent — Wikipedia rejects bare urllib) and parse
    the first table containing the given symbol column."""
    import pandas as pd
    log.info("Fetching %s ...", url)
    r = requests.get(url, headers={"User-Agent": "momentum-scanner/1.0 (research)"}, timeout=30)
    r.raise_for_status()
    tables = pd.read_html(io.StringIO(r.text))
    for tbl in tables:
        if symbol_col in tbl.columns:
            symbols = tbl[symbol_col].astype(str).str.strip().tolist()
            names = (tbl[name_col].astype(str).str.strip().tolist()
                     if name_col and name_col in tbl.columns
                     else [""] * len(symbols))
            return list(zip(symbols, names))
    raise RuntimeError(f"No table with column '{symbol_col}' found at {url}")


def fetch_sp500() -> list[dict]:
    pairs = _wiki_table_symbols(SP500_WIKI, "Symbol", "Security")
    return [{"symbol": s.replace(".", "-"), "name": n, "category": "us_sp500"}
            for s, n in pairs if s]


def fetch_nasdaq100() -> list[dict]:
    # Nasdaq-100 page uses 'Ticker' column.
    try:
        pairs = _wiki_table_symbols(NASDAQ100_WIKI, "Ticker", "Company")
    except RuntimeError:
        # Fallback: older versions used 'Symbol'
        pairs = _wiki_table_symbols(NASDAQ100_WIKI, "Symbol", "Company")
    return [{"symbol": s.replace(".", "-"), "name": n, "category": "us_nasdaq100"}
            for s, n in pairs if s]


_BAD_NAME_PATTERNS = (
    "warrant",      # "Class A Warrants"
    "rights",       # "Subscription Rights"
    " right",
    " unit",        # "Class A Ordinary Shares and one-half of one Warrant" → units
    "units",
    "preferred",
    "depositary",   # depositary shares of preferreds
    "convertible",
    "notes due",
    "subordinated",
    "trust pref",
    "%",            # bond-like, e.g. "6.75% Notes"
    "when issued",
)


def _is_common_stock(name: str, is_etf: str) -> bool:
    if is_etf == "Y":
        return False
    lower = name.lower()
    return not any(p in lower for p in _BAD_NAME_PATTERNS)


def fetch_us_broad_market() -> list[dict]:
    """Fetch the broad US common-stock universe from NASDAQ Trader's official symbol files.

    This stands in for Russell 2000 coverage — we end up with NASDAQ + NYSE
    + AMEX + Arca common stocks (~5-7k symbols). The scanner's liquidity filter
    cuts out the long tail of micro-caps that wouldn't make a top-20 anyway.
    """
    log.info("Fetching NASDAQ Trader symbol directories ...")
    out: list[dict] = []

    # nasdaqlisted.txt: pipe-delimited, columns include Symbol, Security Name,
    # Test Issue, ETF.
    r = requests.get(NASDAQ_LISTED_URL, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
    r.raise_for_status()
    for row in csv.DictReader(io.StringIO(r.text), delimiter="|"):
        sym = (row.get("Symbol") or "").strip()
        name = (row.get("Security Name") or "").strip()
        if not sym or sym == "Symbol" or sym.startswith("File Creation Time"):
            continue
        if row.get("Test Issue") == "Y":
            continue
        if not _is_common_stock(name, row.get("ETF", "")):
            continue
        # Strip common-stock disambiguation suffixes from name
        clean_name = name.split(" - Common Stock")[0].split(" Common Stock")[0].strip()
        out.append({"symbol": sym.replace(".", "-"), "name": clean_name, "category": "us_broad"})

    # otherlisted.txt: NYSE / AMEX / Arca / Cboe BZX
    r = requests.get(NASDAQ_OTHER_LISTED_URL, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
    r.raise_for_status()
    for row in csv.DictReader(io.StringIO(r.text), delimiter="|"):
        sym = (row.get("ACT Symbol") or "").strip()
        name = (row.get("Security Name") or "").strip()
        if not sym or sym == "ACT Symbol" or sym.startswith("File Creation Time"):
            continue
        if row.get("Test Issue") == "Y":
            continue
        if not _is_common_stock(name, row.get("ETF", "")):
            continue
        # Exclude NYSE Arca-listed ETPs and weird suffixes (already filtered by ETF=Y mostly)
        clean_name = name.split(" - Common Stock")[0].split(" Common Stock")[0].strip()
        out.append({"symbol": sym.replace(".", "-"), "name": clean_name, "category": "us_broad"})

    log.info("  → %d broad-market common stocks", len(out))
    return out


def build_us_etfs() -> list[dict]:
    out = []
    for sym, name in US_SECTOR_ETFS:
        out.append({"symbol": sym, "name": name, "category": "us_etf_sector"})
    for sym, name in US_THEMATIC_ETFS:
        out.append({"symbol": sym, "name": name, "category": "us_etf_thematic"})
    return out


def build_us_adrs() -> list[dict]:
    return [{"symbol": s, "name": n, "category": "us_adr"} for s, n in US_TOP_ADRS]


def build_us_universe() -> list[dict]:
    """Build the full US universe, deduplicating by symbol while preserving first category seen.

    Order matters: we keep S&P 500 / Nasdaq 100 categories over Russell 2000
    (a large/mid-cap stock should be tagged as S&P 500, not Russell 2000).
    """
    items: list[dict] = []
    items.extend(fetch_sp500())
    items.extend(fetch_nasdaq100())
    items.extend(build_us_etfs())
    items.extend(build_us_adrs())
    items.extend(fetch_us_broad_market())
    seen = set()
    deduped = []
    for it in items:
        sym = it["symbol"]
        if not sym or sym in seen:
            continue
        seen.add(sym)
        deduped.append(it)
    return deduped


# ---------------------------------------------------------------------------
# CLI for manual testing
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("market", choices=["tw", "us", "both"])
    args = parser.parse_args()

    if args.market in ("tw", "both"):
        tw = build_tw_universe()
        print(f"TW universe: {len(tw)} symbols")
        print("  sample:", tw[:5])
    if args.market in ("us", "both"):
        us = build_us_universe()
        print(f"US universe: {len(us)} symbols")
        print("  sample:", us[:5])
