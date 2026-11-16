#!/usr/bin/env python3
"""
Fetch the top-N resolved Polymarket markets in a category (default: "tech"),
ranked by volume, and write them to a CSV with the columns:

    slug, category, volume, close_date, price_7d, price_3d, price_1d, outcome

price_Nd = YES-token price N days before the market closed
outcome  = 1 if YES won, 0 if NO won

APIs used (public, no key needed):
    Gamma API  https://gamma-api.polymarket.com   -> tags, markets, metadata
    CLOB API   https://clob.polymarket.com        -> /prices-history

Usage:
    pip install requests
    python polymarket_tech.py                       # top 500 tech -> polymarket_tech.csv
    python polymarket_tech.py --tag ai --n 200 --out ai.csv
"""

import argparse
import csv
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import requests

GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"
DAY = 86_400
PAGE_SIZE = 100
COLUMNS = ["slug", "category", "volume", "close_date",
           "price_7d", "price_3d", "price_1d", "outcome"]

session = requests.Session()
session.headers.update({"User-Agent": "polymarket-tech-csv/1.0"})


# --------------------------------------------------------------------------- #
# HTTP helper with retry/backoff (handles 429 rate limits and 5xx errors)
# --------------------------------------------------------------------------- #
def get_json(url, params=None, retries=5):
    for attempt in range(retries):
        try:
            r = session.get(url, params=params, timeout=30)
            if r.status_code == 429 or r.status_code >= 500:
                raise requests.HTTPError(f"HTTP {r.status_code}")
            r.raise_for_status()
            return r.json()
        except (requests.RequestException, ValueError) as e:
            if attempt == retries - 1:
                raise
            wait = 2 ** attempt
            print(f"  retry {attempt + 1} for {url} ({e}); sleeping {wait}s",
                  file=sys.stderr)
            time.sleep(wait)


# --------------------------------------------------------------------------- #
# Parsing helpers
# --------------------------------------------------------------------------- #
def parse_list(value):
    """Gamma returns outcomes/outcomePrices/clobTokenIds as JSON strings."""
    if isinstance(value, list):
        return value
    if isinstance(value, str) and value.strip():
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return []
    return []


def parse_ts(value):
    """Parse Gamma timestamps like '2026-02-04 12:00:00+00' or '2026-02-04T12:00:00Z'."""
    if not value:
        return None
    s = str(value).strip().replace("Z", "+00:00")
    # '+00' -> '+00:00' so fromisoformat accepts it on all Python 3 versions
    if len(s) >= 3 and s[-3] in "+-" and s[-2:].isdigit():
        s += ":00"
    for candidate in (s, s.replace(" ", "T", 1)):
        try:
            dt = datetime.fromisoformat(candidate)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except ValueError:
            continue
    return None


def get_volume(m):
    for key in ("volumeNum", "volume"):
        try:
            return float(m[key])
        except (KeyError, TypeError, ValueError):
            continue
    return 0.0


# --------------------------------------------------------------------------- #
# Gamma API: tag lookup + market listing
# --------------------------------------------------------------------------- #
def get_tag_id(tag_slug):
    tag = get_json(f"{GAMMA}/tags/slug/{tag_slug}")
    if not tag or "id" not in tag:
        sys.exit(f"Tag '{tag_slug}' not found on Polymarket.")
    print(f"Tag '{tag_slug}' -> id {tag['id']} ({tag.get('label')})", file=sys.stderr)
    return tag["id"]


def iter_closed_markets(tag_id, related_tags=False):
    """Yield closed markets for a tag, highest volume first."""
    offset = 0
    while True:
        params = {
            "tag_id": tag_id,
            "closed": "true",
            "order": "volumeNum",
            "ascending": "false",
            "limit": PAGE_SIZE,
            "offset": offset,
        }
        if related_tags:
            params["related_tags"] = "true"
        page = get_json(f"{GAMMA}/markets", params)
        if not page:
            return
        yield from page
        if len(page) < PAGE_SIZE:
            return
        offset += PAGE_SIZE


# --------------------------------------------------------------------------- #
# Market -> row
# --------------------------------------------------------------------------- #
def resolve_binary(m):
    """Return (yes_token_id, outcome) for a cleanly resolved Yes/No market, else None."""
    outcomes = [str(o).strip().lower() for o in parse_list(m.get("outcomes"))]
    prices = parse_list(m.get("outcomePrices"))
    tokens = parse_list(m.get("clobTokenIds"))
    if outcomes != ["yes", "no"] or len(prices) != 2 or len(tokens) != 2:
        return None
    try:
        yes_final = float(prices[0])
    except (TypeError, ValueError):
        return None
    if yes_final >= 0.99:
        return tokens[0], 1
    if yes_final <= 0.01:
        return tokens[0], 0
    return None  # unresolved, 50/50 refund, or still settling


def fetch_history(token_id, close_ts):
    """Price history for the 8 days before close.

    Resolved markets often only return coarse (12h) data, so we try
    fine -> coarse -> 'max' before giving up.
    """
    start = close_ts - 8 * DAY
    attempts = [
        {"startTs": start, "endTs": close_ts, "fidelity": 60},
        {"startTs": start, "endTs": close_ts, "fidelity": 720},
        {"interval": "max", "fidelity": 720},
    ]
    for extra in attempts:
        try:
            data = get_json(f"{CLOB}/prices-history", {"market": token_id, **extra})
        except requests.RequestException:
            continue
        history = (data or {}).get("history") or []
        if history:
            return sorted(history, key=lambda p: p["t"])
    return []


def price_at(history, target_ts):
    """Last traded price at or before target_ts (None if no data that early)."""
    best = None
    for point in history:
        if point["t"] <= target_ts:
            best = point["p"]
        else:
            break
    return None if best is None else round(float(best), 4)


def build_row(m, category_label):
    resolved = resolve_binary(m)
    if not resolved:
        return None
    yes_token, outcome = resolved

    close_dt = parse_ts(m.get("closedTime")) or parse_ts(m.get("endDate"))
    if not close_dt:
        return None
    close_ts = int(close_dt.timestamp())

    history = fetch_history(yes_token, close_ts)
    p7, p3, p1 = (price_at(history, close_ts - d * DAY) for d in (7, 3, 1))
    if p7 is None and p3 is None and p1 is None:
        return None  # no usable price history

    return {
        "slug": m.get("slug"),
        "category": category_label,
        "volume": get_volume(m),
        "close_date": close_dt.date().isoformat(),
        "price_7d": p7,
        "price_3d": p3,
        "price_1d": p1,
        "outcome": outcome,
    }


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--tag", default="tech", help="Polymarket tag slug (default: tech)")
    ap.add_argument("--category", default=None,
                    help="Label for the category column (default: tag title-cased)")
    ap.add_argument("--n", type=int, default=500, help="Number of markets (default: 500)")
    ap.add_argument("--out", default="polymarket_tech.csv", help="Output CSV path")
    ap.add_argument("--workers", type=int, default=8, help="Parallel price-history requests")
    ap.add_argument("--related-tags", action="store_true",
                    help="Also include markets from tags related to --tag")
    ap.add_argument("--max-scan", type=int, default=5000,
                    help="Stop after scanning this many candidate markets")
    args = ap.parse_args()

    label = args.category or args.tag.replace("-", " ").title()
    tag_id = get_tag_id(args.tag)

    rows, seen, scanned = [], set(), 0
    batch = []

    def process(batch):
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            for row in pool.map(lambda mk: build_row(mk, label), batch):
                if row and len(rows) < args.n:
                    rows.append(row)

    for m in iter_closed_markets(tag_id, args.related_tags):
        slug = m.get("slug")
        if not slug or slug in seen:
            continue
        seen.add(slug)
        scanned += 1
        batch.append(m)
        if len(batch) >= PAGE_SIZE:
            process(batch)
            batch = []
            print(f"  scanned {scanned} markets, kept {len(rows)}", file=sys.stderr)
        if len(rows) >= args.n or scanned >= args.max_scan:
            break
    if batch and len(rows) < args.n:
        process(batch)

    rows.sort(key=lambda r: r["volume"], reverse=True)
    rows = rows[: args.n]

    with open(args.out, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {len(rows)} rows to {args.out} (scanned {scanned} markets)",
          file=sys.stderr)
    if len(rows) < args.n:
        print(f"Note: only {len(rows)} resolved Yes/No markets with price history "
              f"were found for tag '{args.tag}'. Try --related-tags.", file=sys.stderr)


if __name__ == "__main__":
    main()