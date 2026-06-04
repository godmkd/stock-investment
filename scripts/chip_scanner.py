"""
Daily chip / broker-branch (分點) scanner for Taiwan stocks.

For each stock in the input list:
  1. Fetch TWSE bsr.twse.com.tw per-branch buy/sell breakdown
     (CAPTCHA solved with ddddocr, retried on failure)
  2. Group rows by broker branch, sum net (buy − sell) shares
  3. Compute signals:
       - 主力買賣超     = sum of top-15 buyers' net + bottom-15 sellers' net
       - 籌碼集中度 CR15 = |主力買賣超| / total volume
       - 隔日沖比例     = known day-trade branches' volume / total volume
  4. Rank stocks by 主力買超 strength
  5. Post a Discord summary via DISCORD_WEBHOOK_URL

Usage:
    python scripts/chip_scanner.py 2330 2317 2454       # explicit codes
    python scripts/chip_scanner.py --from-file top.json # JSON list of codes
    python scripts/chip_scanner.py 2330 --dry-run       # don't post to Discord
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Iterable

import requests

log = logging.getLogger(__name__)

TZ_TW = timezone(timedelta(hours=8))

BSR_MENU_URL = "https://bsr.twse.com.tw/bshtm/bsMenu.aspx"
BSR_BASE_URL = "https://bsr.twse.com.tw/bshtm/"
BSR_CAPTCHA_URL = "https://bsr.twse.com.tw/bshtm/CaptchaImage.aspx"

# Best-effort browser-like UA; TWSE doesn't fingerprint hard but blocks bare requests.
DEFAULT_UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Chrome/120.0 Safari/537.36"

TOP_N_FOR_MAIN_PLAYER = 15  # top buyers + top sellers each side

# Round-trip threshold for day-trade detection: a branch where
# min(buy, sell) / (buy + sell) ≥ this is treated as "round-tripping today".
# 0.40 is a conservative cutoff (perfect round-trip = 0.50).
DAYTRADE_BRANCH_THRESHOLD = 0.40

# ---------------------------------------------------------------------------
# TWSE bsr fetcher
# ---------------------------------------------------------------------------

@dataclass
class BranchRow:
    branch_code: str   # e.g. "1020", "9800F"
    branch_name: str   # e.g. "1020合　　庫", "9800F富邦-板橋"
    price: float
    buy_shares: int
    sell_shares: int


class CaptchaError(Exception):
    pass


def _new_session() -> requests.Session:
    s = requests.Session()
    s.headers["User-Agent"] = DEFAULT_UA
    return s


_RE_GUID = re.compile(r"CaptchaImage\.aspx\?guid=([0-9a-f-]+)")
_RE_VS = re.compile(r'id="__VIEWSTATE" value="([^"]+)"')
_RE_VSG = re.compile(r'id="__VIEWSTATEGENERATOR" value="([^"]+)"')
_RE_EV = re.compile(r'id="__EVENTVALIDATION" value="([^"]+)"')
_RE_CONTENT_LINK = re.compile(r'href="(bsContent\.aspx\?StkNo=\d+[^"]*)"')
_RE_ERR_MSG = re.compile(r'Label_ErrorMsg[^>]*>([^<]+)')


def _solve_captcha(image_bytes: bytes, _ocr_cache: list = []) -> str:
    """Lazy-init the OCR engine so test runs that skip fetching don't pay startup cost."""
    if not _ocr_cache:
        import ddddocr
        _ocr_cache.append(ddddocr.DdddOcr(show_ad=False))
    return _ocr_cache[0].classification(image_bytes).strip()


def _fetch_bsr_text(stock_code: str, max_retries: int = 8) -> str | None:
    """Run the TWSE bsr form flow for one stock; retry on CAPTCHA failures."""
    for attempt in range(1, max_retries + 1):
        try:
            s = _new_session()
            r = s.get(BSR_MENU_URL, timeout=15)
            r.raise_for_status()
            html = r.text
            guid_m = _RE_GUID.search(html)
            vs_m = _RE_VS.search(html)
            vsg_m = _RE_VSG.search(html)
            ev_m = _RE_EV.search(html)
            if not all((guid_m, vs_m, vsg_m, ev_m)):
                log.warning("[%s] menu page missing expected fields, attempt %d", stock_code, attempt)
                time.sleep(1.0)
                continue

            cap_r = s.get(f"{BSR_CAPTCHA_URL}?guid={guid_m.group(1)}", timeout=15)
            cap_r.raise_for_status()
            solved = _solve_captcha(cap_r.content)
            if len(solved) != 5 or not solved.isalnum():
                log.debug("[%s] OCR returned suspect %r, skipping POST", stock_code, solved)
                continue

            form = {
                "__EVENTTARGET": "", "__EVENTARGUMENT": "", "__LASTFOCUS": "",
                "__VIEWSTATE": vs_m.group(1),
                "__VIEWSTATEGENERATOR": vsg_m.group(1),
                "__EVENTVALIDATION": ev_m.group(1),
                "RadioButton_Normal": "RadioButton_Normal",
                "TextBox_Stkno": stock_code,
                "CaptchaControl1": solved,
                "btnOK": "查詢",
            }
            post_r = s.post(BSR_MENU_URL, data=form, timeout=20)
            link = _RE_CONTENT_LINK.search(post_r.text)
            if link:
                content_r = s.get(BSR_BASE_URL + link.group(1), timeout=20)
                content_r.raise_for_status()
                log.info("[%s] fetched on attempt %d (CAPTCHA=%s)", stock_code, attempt, solved)
                return content_r.text

            err = _RE_ERR_MSG.search(post_r.text)
            if err and "找不到" in err.group(1):
                # No 分點 data for this stock today (probably not traded)
                log.info("[%s] no data: %s", stock_code, err.group(1).strip())
                return None
        except requests.RequestException as e:
            log.warning("[%s] network error attempt %d: %s", stock_code, attempt, e)
        time.sleep(0.6)
    log.warning("[%s] gave up after %d attempts", stock_code, max_retries)
    return None


def _parse_bsr_text(text: str) -> list[BranchRow]:
    """Parse the comma-separated rows out of a bsContent.aspx page."""
    # Strip HTML, normalize whitespace
    plain = re.sub(r"<[^>]+>", "\n", text)
    plain = re.sub(r"&nbsp;", " ", plain)
    rows: list[BranchRow] = []

    # The body has lines like:
    #   1,1020合　　庫,2415.00,200,0,,2,1020合　　庫,2420.00,3959,153
    # Each line carries TWO records joined by ",,". Iterate over all such records.
    record_re = re.compile(
        r"(\d+),([^,]+?),(\d+\.\d+),(\d+),(\d+)"
    )
    for m in record_re.finditer(plain):
        broker_field = m.group(2).strip()
        if "券商" in broker_field or "代碼" in broker_field:
            continue  # header artifact
        # Branch code = first whitespace-free token up to first non-alnum (e.g. "1020", "102A", "9800F")
        code_m = re.match(r"([0-9A-Z]+)", broker_field)
        if not code_m:
            continue
        rows.append(BranchRow(
            branch_code=code_m.group(1),
            branch_name=broker_field,
            price=float(m.group(3)),
            buy_shares=int(m.group(4)),
            sell_shares=int(m.group(5)),
        ))
    return rows


# ---------------------------------------------------------------------------
# Signal computation
# ---------------------------------------------------------------------------

@dataclass
class ChipMetrics:
    stock_code: str
    stock_name: str              # display name (e.g. 台積電); empty if unknown
    momentum_score: float | None # carried from momentum_scanner if input includes it
    total_buy: int
    total_sell: int
    total_volume: int            # buy + sell, in shares
    main_player_net: int         # 主力買賣超 in shares
    concentration: float         # |主力買賣超| / total_volume, 0..1
    top_buyers: list[tuple[str, int]]   # (branch_label, net_shares)
    top_sellers: list[tuple[str, int]]
    intraday_ratio: float        # share of volume from branches that round-tripped today (當沖)


def compute_metrics(
    stock_code: str,
    rows: list[BranchRow],
    stock_name: str = "",
    momentum_score: float | None = None,
) -> ChipMetrics | None:
    if not rows:
        return None

    # Aggregate per branch
    by_branch: dict[str, dict] = {}
    for r in rows:
        b = by_branch.setdefault(r.branch_code, {
            "name": r.branch_name, "buy": 0, "sell": 0,
        })
        b["buy"] += r.buy_shares
        b["sell"] += r.sell_shares

    total_buy = sum(b["buy"] for b in by_branch.values())
    total_sell = sum(b["sell"] for b in by_branch.values())
    total_volume = total_buy + total_sell
    if total_volume == 0:
        return None

    # Net per branch
    nets = [
        (code, info["name"], info["buy"] - info["sell"], info["buy"], info["sell"])
        for code, info in by_branch.items()
    ]
    nets.sort(key=lambda x: x[2], reverse=True)

    top_buyers_full = nets[:TOP_N_FOR_MAIN_PLAYER]
    top_sellers_full = nets[-TOP_N_FOR_MAIN_PLAYER:][::-1]  # most-negative first

    top_buyers = [(_short_branch(n), net) for (_, n, net, _, _) in top_buyers_full]
    top_sellers = [(_short_branch(n), net) for (_, n, net, _, _) in top_sellers_full]

    main_player_net = sum(net for _, _, net, _, _ in top_buyers_full) + \
                      sum(net for _, _, net, _, _ in top_sellers_full)

    concentration = abs(main_player_net) / total_volume

    # Day-trade ratio (data-driven, no hardcoded broker list):
    # for each branch, "round-trip volume" = 2 × min(buy, sell). Branches that
    # round-trip a meaningful fraction of their volume are day-trading by
    # definition — they bought and sold the same stock on the same day. We sum
    # this across branches that exceed DAYTRADE_BRANCH_THRESHOLD and divide
    # by total market volume to get an aggregate intraday-trade ratio.
    intraday_volume = 0
    for info in by_branch.values():
        branch_total = info["buy"] + info["sell"]
        if branch_total == 0:
            continue
        rt_ratio = min(info["buy"], info["sell"]) / branch_total
        if rt_ratio >= DAYTRADE_BRANCH_THRESHOLD:
            intraday_volume += 2 * min(info["buy"], info["sell"])
    intraday_ratio = intraday_volume / total_volume if total_volume else 0.0

    return ChipMetrics(
        stock_code=stock_code,
        stock_name=stock_name,
        momentum_score=momentum_score,
        total_buy=total_buy,
        total_sell=total_sell,
        total_volume=total_volume,
        main_player_net=main_player_net,
        concentration=concentration,
        top_buyers=top_buyers,
        top_sellers=top_sellers,
        intraday_ratio=intraday_ratio,
    )


def _short_branch(name: str) -> str:
    """Trim padding spaces in TWSE branch labels for display."""
    # Remove full-width and ordinary spaces inside the broker name
    return re.sub(r"[ 　]+", " ", name).strip()


# ---------------------------------------------------------------------------
# Discord push
# ---------------------------------------------------------------------------

def _shares_to_lots(shares: int, signed: bool = True) -> str:
    """Format share count as 'N張' (1 lot = 1000 shares). signed=False for volumes."""
    lots = shares / 1000
    fmt_num = f"{lots:+,.0f}" if signed else f"{lots:,.0f}"
    fmt_wan = f"{lots/10000:+.1f}" if signed else f"{lots/10000:.1f}"
    if abs(lots) >= 10000:
        return f"{fmt_wan}萬張"
    return f"{fmt_num}張"


def build_discord_content(metrics_list: list[ChipMetrics], limit: int) -> str:
    """Format the top-N stocks ordered by main_player_net (descending)."""
    now = datetime.now(TZ_TW).strftime("%Y-%m-%d %H:%M %Z")
    metrics_list = sorted(metrics_list, key=lambda m: m.main_player_net, reverse=True)
    top = metrics_list[:limit]

    header = (
        f"**📊 台股 籌碼分析 Top {len(top)}  ·  {now}**\n"
        f"_掃描 {len(metrics_list)} 檔，依「主力買賣超」排序_"
    )
    entries = []
    for i, m in enumerate(top, 1):
        buyer_names = " / ".join(name for name, _ in m.top_buyers[:3])
        flag_parts = []
        if m.concentration >= 0.15:
            flag_parts.append(f"📌 集中度高")
        if m.intraday_ratio >= 0.55:
            flag_parts.append(f"⚠ 當沖盤居多")
        if m.main_player_net > 0 and m.concentration >= 0.10:
            flag_parts.append("✅ 主力有撐")
        if not flag_parts:
            flag_parts.append("籌碼結構中性")
        flag_str = " · ".join(flag_parts)
        name_part = f" {m.stock_name}" if m.stock_name else ""
        score_part = f"   動能 {m.momentum_score:.0f}" if m.momentum_score is not None else ""
        entry = (
            f"**{i:>2}. {m.stock_code}{name_part}**{score_part}\n"
            f"     主力 {_shares_to_lots(m.main_player_net)}   "
            f"集中 {m.concentration*100:.1f}%   "
            f"當沖 {m.intraday_ratio*100:.0f}%   "
            f"成交 {_shares_to_lots(m.total_volume, signed=False)}\n"
            f"     ✦ 前三買: {buyer_names}\n"
            f"     ✦ {flag_str}"
        )
        entries.append(entry)
    return header + "\n\n" + "\n\n".join(entries)


def post_to_discord(webhook_url: str, content: str) -> None:
    """Same chunking strategy as momentum_scanner: split by blank lines into ≤1900-char POSTs."""
    chunks: list[str] = []
    current = ""
    for part in content.split("\n\n"):
        sep = "\n\n" if current else ""
        if len(current) + len(sep) + len(part) > 1900:
            if current:
                chunks.append(current)
            current = part
        else:
            current += sep + part
    if current:
        chunks.append(current)
    for i, ch in enumerate(chunks, 1):
        r = requests.post(webhook_url, json={"content": ch}, timeout=15)
        if r.status_code >= 300:
            log.error("Discord webhook returned %s: %s", r.status_code, r.text[:200])
            r.raise_for_status()
        log.info("posted chunk %d/%d (%d chars)", i, len(chunks), len(ch))


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def _load_entries(args: argparse.Namespace) -> list[dict]:
    """Returns list of {code, name, score} dicts. name/score may be empty/None."""
    entries: list[dict] = []
    if args.from_file:
        with open(args.from_file, encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            if "entries" in data:
                for e in data["entries"]:
                    entries.append({
                        "code": str(e.get("code", "")).strip(),
                        "name": str(e.get("name", "")).strip(),
                        "score": e.get("score"),
                    })
            elif "codes" in data:
                entries.extend({"code": str(c).strip(), "name": "", "score": None}
                               for c in data["codes"])
            else:
                raise ValueError(f"{args.from_file}: expected entries or codes key")
        elif isinstance(data, list):
            entries.extend({"code": str(c).strip(), "name": "", "score": None}
                           for c in data)
        else:
            raise ValueError(f"{args.from_file}: unexpected JSON shape")
    entries.extend({"code": c.strip(), "name": "", "score": None} for c in args.codes if c)
    # Strip .TW suffixes and drop empties
    cleaned = []
    for e in entries:
        code = e["code"].replace(".TW", "").replace(".TWO", "")
        if code:
            e["code"] = code
            cleaned.append(e)
    return cleaned


def run(entries: list[dict], limit: int, dry_run: bool, request_delay: float) -> int:
    log.info("=== chip scanner starting on %d stocks ===", len(entries))
    results: list[ChipMetrics] = []
    for i, entry in enumerate(entries, 1):
        code = entry["code"]
        name = entry.get("name", "") or ""
        score = entry.get("score")
        log.info("[%d/%d] fetching %s %s ...", i, len(entries), code, name)
        text = _fetch_bsr_text(code)
        if not text:
            continue
        rows = _parse_bsr_text(text)
        if not rows:
            log.info("[%s] no rows parsed; skipping", code)
            continue
        m = compute_metrics(code, rows, stock_name=name, momentum_score=score)
        if m:
            results.append(m)
        time.sleep(request_delay)

    log.info("got metrics for %d/%d stocks", len(results), len(entries))
    if not results:
        log.warning("nothing to post.")
        return 0

    content = build_discord_content(results, limit)
    if dry_run:
        log.info("--- DRY RUN ---\n%s", content)
        return 0

    webhook = os.environ.get("DISCORD_WEBHOOK_URL")
    if not webhook:
        log.error("DISCORD_WEBHOOK_URL not set; cannot post.")
        return 1
    post_to_discord(webhook, content)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("codes", nargs="*", help="TW stock codes (e.g. 2330 2317)")
    parser.add_argument("--from-file", help="JSON file with list of codes")
    parser.add_argument("--limit", type=int, default=20, help="top-N to push")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--delay", type=float, default=0.8,
                        help="seconds to sleep between stocks (be nice to TWSE)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    entries = _load_entries(args)
    if not entries:
        parser.error("provide at least one code or --from-file")

    return run(entries, args.limit, args.dry_run, args.delay)


if __name__ == "__main__":
    sys.exit(main())
