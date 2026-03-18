"""
CSV import for historical pool snapshots.

Supported format (tab or comma separated):
  date          M/D/YYYY or YYYY-MM-DD  e.g. 2/6/2026
  Pool          TOKEN0/TOKEN1           e.g. USDT/KOGE
  Model         v3 | v4
  Fee Tier      e.g. 0.01%
  TVL           e.g. $7,400,000
  Volume (24hr) e.g. $33,500,000
  APR           e.g. 16.49%  (optional)
  Chain         bnb | arbitrum          (optional, defaults to 'bnb')

Run directly:
  python -m src.csv_import <file.csv>
  python -m src.csv_import <file.csv> --dry-run
  python -m src.csv_import --template
"""

import csv
import sqlite3
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Optional, List

from src.collector import init_db, DB_PATH, V3_PROTOCOL_FEE_FRACTION

# ── Header aliases ────────────────────────────────────────────────────────────

ALIASES = {
    "date":       ["date", "snapshot_date", "day"],
    "pool":       ["pool", "pair", "pool_name", "pool name"],
    "token0":     ["token0", "token0_symbol", "base_token"],
    "token1":     ["token1", "token1_symbol", "quote_token"],
    "version":    ["model", "version", "protocol", "ver"],
    "fee_tier":   ["fee tier", "fee_tier", "fee", "fee_bps", "feeTier"],
    "tvl_usd":    ["tvl", "tvl_usd", "total_value_locked", "total value locked"],
    "volume_24h": ["volume (24hr)", "volume_24h", "volume_24h_usd", "volume", "vol_24h", "vol (24hr)"],
    "apr":        ["apr", "apy"],
    "chain":      ["chain", "network"],
    "fees_24h":   ["fees_24h", "fees_24h_usd", "fees"],
    "tx_count":   ["tx_count", "transactions", "txns"],
    "pool_address": ["pool_address", "address", "pool_id"],
}

# Required: either (token0 + token1) OR pool column, plus these:
REQUIRED_ALWAYS = {"date", "version", "fee_tier", "tvl_usd", "volume_24h"}


# ── Parsers ───────────────────────────────────────────────────────────────────

def _parse_usd(val: str) -> float:
    """Parse '$7,400,000', '$14.3M', '2.1K', '500000' → float."""
    if not val or str(val).strip() in ("", "-", "N/A"):
        return 0.0
    s = str(val).strip().replace(",", "").replace("$", "").upper()
    multiplier = 1.0
    if s.endswith("B"):
        multiplier = 1_000_000_000; s = s[:-1]
    elif s.endswith("M"):
        multiplier = 1_000_000; s = s[:-1]
    elif s.endswith("K"):
        multiplier = 1_000; s = s[:-1]
    try:
        return float(s) * multiplier
    except ValueError:
        return 0.0


def _parse_fee_tier(val: str) -> int:
    """'0.01%' → 100, '0.05%' → 500, '0.3%' → 3000, '1%' → 10000, '500' → 500."""
    s = str(val).strip().replace("%", "")
    try:
        num = float(s)
        if num < 100:              # percentage like 0.01, 0.05, 0.3, 1.0
            return int(round(num * 10000))
        return int(num)            # already in basis points
    except ValueError:
        return 0


def _parse_pct(val: str) -> Optional[float]:
    """'16.49%' → 0.1649, '16.49' → 0.1649"""
    if not val or str(val).strip() in ("", "-", "N/A"):
        return None
    try:
        return float(str(val).strip().replace("%", "")) / 100
    except ValueError:
        return None


def _parse_date(val: str) -> date:
    """Handle M/D/YYYY, MM/DD/YYYY, YYYY-MM-DD."""
    s = str(val).strip()
    for fmt in ("%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"Cannot parse date: {s!r}")


def _normalize_chain(val: str) -> str:
    v = str(val).strip().lower()
    if v in ("bnb", "bsc", "binance", "bnb chain", "bnbchain"):
        return "bnb"
    if v in ("arbitrum", "arb", "arbitrum one"):
        return "arbitrum"
    if v in ("base", "base chain"):
        return "base"
    if v in ("monad", "monad chain"):
        return "monad"
    return v or "bnb"


def _normalize_version(val: str) -> str:
    v = str(val).strip().lower().replace(" ", "")
    if v in ("v3", "uniswapv3", "3"):
        return "v3"
    if v in ("v4", "uniswapv4", "4"):
        return "v4"
    return v


# ── Header mapping ────────────────────────────────────────────────────────────

def _map_headers(headers: List[str]) -> dict:
    """Return {canonical_name: actual_csv_column}. Case-insensitive, strips spaces."""
    lower_map = {h.strip().lower(): h for h in headers}
    result = {}
    for canonical, aliases in ALIASES.items():
        for alias in aliases:
            if alias.lower() in lower_map:
                result[canonical] = lower_map[alias.lower()]
                break
    return result


def _detect_delimiter(path: str) -> str:
    with open(path, newline="", encoding="utf-8-sig") as f:
        sample = f.read(2048)
    tabs = sample.count("\t")
    commas = sample.count(",")
    return "\t" if tabs > commas else ","


# ── Main import ───────────────────────────────────────────────────────────────

def import_csv(path: str, dry_run: bool = False, default_chain: str = "bnb") -> dict:
    """
    Import historical pool data from CSV.
    Returns {"rows_ok": int, "rows_error": int, "errors": [str]}.
    """
    init_db()
    conn = sqlite3.connect(DB_PATH)
    rows_ok = 0
    rows_error = 0
    errors = []

    delim = _detect_delimiter(path)

    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f, delimiter=delim)
        headers = reader.fieldnames or []
        col = _map_headers(headers)

        # Validate required columns
        missing = REQUIRED_ALWAYS - set(col.keys())
        has_pool = "pool" in col
        has_tokens = "token0" in col and "token1" in col
        if missing or (not has_pool and not has_tokens):
            return {
                "rows_ok": 0, "rows_error": 0,
                "errors": [
                    f"Missing required columns: {missing or {'pool OR (token0+token1)'}}\n"
                    f"Found headers: {headers}\n"
                    "Expected: date, Pool, Model, Fee Tier, TVL, Volume (24hr)"
                ]
            }

        for i, row in enumerate(reader, start=2):
            try:
                snapshot_date = _parse_date(row[col["date"]])

                # Chain: from column if present, else default
                chain = _normalize_chain(row[col["chain"]]) if col.get("chain") else default_chain

                version = _normalize_version(row[col["version"]])
                fee_tier = _parse_fee_tier(row[col["fee_tier"]])
                tvl = _parse_usd(row[col["tvl_usd"]])
                vol = _parse_usd(row[col["volume_24h"]])

                # Token symbols: from "Pool" = "USDT/KOGE" or separate token0/token1 columns
                if has_pool:
                    pool_str = row[col["pool"]].strip()
                    parts = pool_str.replace(" ", "").split("/")
                    t0 = parts[0].upper() if len(parts) >= 1 else ""
                    t1 = parts[1].upper() if len(parts) >= 2 else ""
                else:
                    t0 = row[col["token0"]].strip().upper()
                    t1 = row[col["token1"]].strip().upper()

                fees = _parse_usd(row.get(col.get("fees_24h", ""), "") or "") if col.get("fees_24h") else vol * (fee_tier / 1_000_000)
                tx = None
                if col.get("tx_count"):
                    raw_tx = (row.get(col["tx_count"]) or "").strip()
                    tx = int(float(raw_tx)) if raw_tx else None

                apr = _parse_pct(row.get(col.get("apr", ""), "") or "") if col.get("apr") else None

                addr = (row.get(col.get("pool_address", ""), "") or "").strip().lower() or f"manual:{t0}-{t1}-{fee_tier}"

                proto, lp = None, None
                if version == "v3":
                    frac = V3_PROTOCOL_FEE_FRACTION.get(fee_tier)
                    if frac:
                        proto = fees * frac
                        lp    = fees - proto

                if not dry_run:
                    conn.execute("""
                        INSERT INTO pool_snapshots
                            (snapshot_date, chain, version, pool_address, token0_symbol, token1_symbol,
                             fee_tier, hooks, tick_spacing, tvl_usd, volume_24h_usd, fees_24h_usd,
                             tx_count, protocol_fee_est_usd, lp_fee_usd, apr, source)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'manual')
                        ON CONFLICT(snapshot_date, chain, version, pool_address)
                        DO UPDATE SET
                            tvl_usd=excluded.tvl_usd, volume_24h_usd=excluded.volume_24h_usd,
                            fees_24h_usd=excluded.fees_24h_usd, tx_count=excluded.tx_count,
                            protocol_fee_est_usd=excluded.protocol_fee_est_usd,
                            lp_fee_usd=excluded.lp_fee_usd, apr=excluded.apr, source='manual'
                    """, (
                        snapshot_date.isoformat(), chain, version, addr, t0, t1,
                        fee_tier, None, None, tvl, vol, fees, tx, proto, lp, apr,
                    ))
                rows_ok += 1

            except Exception as e:
                rows_error += 1
                errors.append(f"Row {i}: {e}")

    if not dry_run:
        conn.commit()
    conn.close()
    return {"rows_ok": rows_ok, "rows_error": rows_error, "errors": errors}


# ── Template generator ────────────────────────────────────────────────────────

def generate_template(path: str = "data/template.csv") -> str:
    """Write a template CSV in the user's preferred format."""
    Path(path).parent.mkdir(exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["date", "Pool", "Chain", "Model", "Fee Tier", "TVL", "Volume (24hr)", "APR"])
        w.writerow(["2/6/2026",  "USDT/KOGE",  "bnb",      "v3", "0.01%", "$7,400,000",  "$33,500,000", "16.49%"])
        w.writerow(["2/6/2026",  "WBNB/USDT",  "bnb",      "v3", "0.05%", "$14,300,000", "$2,100,000",  "5.38%"])
        w.writerow(["2/6/2026",  "BNB/USDT",   "bnb",      "v4", "0.05%", "$1,601,000",  "$890,000",    ""])
        w.writerow(["2/6/2026",  "WETH/USDC",  "arbitrum", "v3", "0.05%", "$45,200,000", "$8,100,000",  "3.28%"])
        w.writerow(["2/7/2026",  "USDT/KOGE",  "bnb",      "v3", "0.01%", "$7,800,000",  "$31,200,000", "14.62%"])
    return path


# ── CLI entry ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python -m src.csv_import <file.csv> [--dry-run]")
        print("       python -m src.csv_import --template")
        sys.exit(1)

    from dotenv import load_dotenv
    load_dotenv()

    if sys.argv[1] == "--template":
        p = generate_template()
        print(f"Template written to {p}")
        sys.exit(0)

    dry = "--dry-run" in sys.argv
    result = import_csv(sys.argv[1], dry_run=dry)
    tag = "[DRY RUN] " if dry else ""
    print(f"{tag}Imported: {result['rows_ok']} ok, {result['rows_error']} errors")
    for e in result["errors"][:20]:
        print(f"  ⚠ {e}")
