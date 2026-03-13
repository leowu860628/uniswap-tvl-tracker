"""
Fetch Uniswap v3 and v4 pool data from GeckoTerminal (free, no API key).
Falls back to The Graph decentralized network if GRAPH_API_KEY is set in .env.
Normalizes results and upserts into SQLite.
"""

import sqlite3
import requests
import os
from datetime import date
from pathlib import Path
from typing import Optional, List, Tuple

DB_PATH = Path(__file__).parent.parent / "data" / "tvl.db"

# GeckoTerminal network + DEX slugs (per chain/version)
GECKO_DEX_SLUGS = {
    "bnb": {
        "v3": ("bsc", "uniswap-bsc"),
        "v4": ("bsc", "uniswap-v4-bsc"),
    },
    "arbitrum": {
        "v3": ("arbitrum", "uniswap_v3_arbitrum"),
        "v4": ("arbitrum", "uniswap-v4-arbitrum"),
    },
}
GECKO_BASE = "https://api.geckoterminal.com/api/v2"

# The Graph decentralized network (optional, requires GRAPH_API_KEY)
GRAPH_SUBGRAPH_IDS = {
    "v3": {
        "bnb":      "FbCGRftH4a3yZugY7TnbYgPJVEv2LvMT6oF1fxPe9asc",
        "arbitrum": "FQ6JYszEKApsBpAmiHesRsd9Ygc6mzmpNRANeVQFYoVX",
    },
}

V3_PROTOCOL_FEE_FRACTION = {
    100:   1 / 4,
    500:   1 / 4,
    3000:  1 / 6,
    10000: 1 / 6,
}


def init_db():
    DB_PATH.parent.mkdir(exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pool_snapshots (
            id                   INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_date        DATE    NOT NULL,
            chain                TEXT    NOT NULL,
            version              TEXT    NOT NULL,
            pool_address         TEXT    NOT NULL,
            token0_symbol        TEXT,
            token1_symbol        TEXT,
            fee_tier             INTEGER,
            hooks                TEXT,
            tick_spacing         INTEGER,
            tvl_usd              REAL,
            volume_24h_usd       REAL,
            fees_24h_usd         REAL,
            tx_count             INTEGER,
            protocol_fee_est_usd REAL,
            lp_fee_usd           REAL,
            apr                  REAL,
            source               TEXT DEFAULT 'subgraph',
            created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(snapshot_date, chain, version, pool_address)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS report_log (
            report_date  DATE PRIMARY KEY,
            completed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            status       TEXT DEFAULT 'ok'
        )
    """)
    # Migrations for existing DBs
    for migration in [
        "ALTER TABLE pool_snapshots ADD COLUMN apr REAL",
        "ALTER TABLE pool_snapshots ADD COLUMN is_deleted INTEGER DEFAULT 0",
    ]:
        try:
            conn.execute(migration)
        except Exception:
            pass
    conn.commit()
    conn.close()


# ── GeckoTerminal ─────────────────────────────────────────────────────────────

_RETRY_DELAYS = [15, 30, 60]
_GECKO_HEADERS = {"Accept": "application/json;version=20230302"}


def _gecko_get(url: str, params: dict) -> Optional[requests.Response]:
    """GET with exponential backoff on 429. Returns Response or None after exhausting retries."""
    import time
    resp = requests.get(url, params=params, headers=_GECKO_HEADERS, timeout=30)
    if resp.status_code != 429:
        return resp
    for delay in _RETRY_DELAYS:
        print(f"[collector] GeckoTerminal rate limit (429), waiting {delay}s...")
        time.sleep(delay)
        resp = requests.get(url, params=params, headers=_GECKO_HEADERS, timeout=30)
        if resp.status_code != 429:
            return resp
    print(f"[collector] GeckoTerminal still rate-limited after all retries, giving up.")
    return None


def _gecko_fetch(network: str, dex: str, pages: int = 3) -> List[dict]:
    """Fetch up to pages*20 pools from GeckoTerminal, sorted locally by TVL."""
    import time
    pools = []
    for page in range(1, pages + 1):
        url = f"{GECKO_BASE}/networks/{network}/dexes/{dex}/pools"
        try:
            resp = _gecko_get(url, {"page": page})
            if resp is None:
                break  # rate limit exhausted — keep any already-fetched pages
            resp.raise_for_status()
            data = resp.json().get("data", [])
            if not data:
                break
            pools.extend(data)
            if page < pages:
                time.sleep(1)  # polite rate limiting
        except Exception as e:
            print(f"[collector] GeckoTerminal {network}/{dex} page {page}: {e}")
            break
    # Sort locally by TVL descending
    pools.sort(key=lambda p: float(p.get("attributes", {}).get("reserve_in_usd", 0) or 0), reverse=True)
    return pools[:50]


def _parse_gecko(pool: dict, chain: str, version: str) -> Optional[dict]:
    """Normalize a GeckoTerminal pool object."""
    try:
        attrs = pool["attributes"]
        addr = pool["id"].split("_", 1)[-1].lower()  # e.g. "bsc_0xabc..." → "0xabc..."

        # Token symbols from relationships or name field
        name = attrs.get("name", "")  # e.g. "WBNB / USDT 0.05%"
        parts = name.split(" / ")
        token0 = parts[0].strip() if len(parts) >= 1 else ""
        token1_fee = parts[1].strip() if len(parts) >= 2 else ""
        # token1_fee might be "USDT 0.05%" — split off fee
        token1_parts = token1_fee.rsplit(" ", 1)
        token1 = token1_parts[0] if len(token1_parts) == 2 else token1_fee

        # Fee tier: derive from name or default 0
        fee_tier = 0
        fee_str = token1_parts[1].replace("%", "") if len(token1_parts) == 2 else ""
        try:
            fee_tier = int(float(fee_str) * 10000)  # 0.05% → 500
        except (ValueError, IndexError):
            pass

        tvl = float(attrs.get("reserve_in_usd", 0) or 0)
        vol = float(attrs.get("volume_usd", {}).get("h24", 0) or 0)
        # GeckoTerminal doesn't expose fees directly; estimate from volume × fee_rate
        fee_rate = fee_tier / 1_000_000  # e.g. 500 → 0.0005
        fees_24h = vol * fee_rate

        proto, lp = _estimate_protocol_fee(fees_24h, fee_tier, version)

        return {
            "version":             version,
            "chain":               chain,
            "pool_address":        addr,
            "token0_symbol":       token0,
            "token1_symbol":       token1,
            "fee_tier":            fee_tier,
            "hooks":               None,
            "tick_spacing":        None,
            "tvl_usd":             tvl,
            "volume_24h_usd":      vol,
            "fees_24h_usd":        fees_24h,
            "tx_count":            (lambda t: t.get("buys", 0) + t.get("sells", 0))(
                                   attrs.get("transactions", {}).get("h24", {}) or {}),
            "protocol_fee_est_usd": proto,
            "lp_fee_usd":          lp,
        }
    except Exception as e:
        print(f"[collector] Failed to parse GeckoTerminal pool: {e}")
        return None


# ── The Graph (optional fallback) ─────────────────────────────────────────────

def _graph_fetch(chain: str, version: str) -> Optional[List[dict]]:
    api_key = os.environ.get("GRAPH_API_KEY")
    if not api_key:
        return None
    subgraph_id = GRAPH_SUBGRAPH_IDS.get(version, {}).get(chain)
    if not subgraph_id:
        return None
    url = f"https://gateway.thegraph.com/api/{api_key}/subgraphs/id/{subgraph_id}"
    query = """
    {
      pools(first: 50, orderBy: totalValueLockedUSD, orderDirection: desc,
            where: {totalValueLockedUSD_gt: "1000"}) {
        id
        token0 { symbol }
        token1 { symbol }
        feeTier
        totalValueLockedUSD
        poolDayData(first: 1, orderBy: date, orderDirection: desc) {
          volumeUSD
          feesUSD
          txCount
        }
      }
    }
    """
    try:
        resp = requests.post(url, json={"query": query}, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if "errors" in data:
            print(f"[collector] Graph errors for {chain} {version}: {data['errors']}")
            return None
        return data.get("data", {}).get("pools", [])
    except Exception as e:
        print(f"[collector] Graph fetch failed for {chain} {version}: {e}")
        return None


def _normalize_graph_v3(pool: dict, chain: str) -> dict:
    days = pool.get("poolDayData") or []
    d = days[0] if days else {}
    fee_tier = int(pool.get("feeTier", 0) or 0)
    fees = float(d.get("feesUSD", 0) or 0)
    proto, lp = _estimate_protocol_fee(fees, fee_tier, "v3")
    return {
        "version":             "v3",
        "chain":               chain,
        "pool_address":        pool["id"].lower(),
        "token0_symbol":       pool.get("token0", {}).get("symbol", ""),
        "token1_symbol":       pool.get("token1", {}).get("symbol", ""),
        "fee_tier":            fee_tier,
        "hooks":               None,
        "tick_spacing":        None,
        "tvl_usd":             float(pool.get("totalValueLockedUSD", 0) or 0),
        "volume_24h_usd":      float(d.get("volumeUSD", 0) or 0),
        "fees_24h_usd":        fees,
        "tx_count":            int(d.get("txCount", 0) or 0),
        "protocol_fee_est_usd": proto,
        "lp_fee_usd":          lp,
    }


# ── Shared helpers ─────────────────────────────────────────────────────────────

def _estimate_protocol_fee(fees_24h: float, fee_tier: int, version: str) -> Tuple[Optional[float], Optional[float]]:
    if version == "v4":
        return None, None
    fraction = V3_PROTOCOL_FEE_FRACTION.get(fee_tier)
    if fraction is None:
        return None, None
    proto = fees_24h * fraction
    return proto, fees_24h - proto


def _upsert(conn: sqlite3.Connection, rows: List[dict], snapshot_date: date):
    for r in rows:
        conn.execute("""
            INSERT INTO pool_snapshots
                (snapshot_date, chain, version, pool_address, token0_symbol, token1_symbol,
                 fee_tier, hooks, tick_spacing, tvl_usd, volume_24h_usd, fees_24h_usd,
                 tx_count, protocol_fee_est_usd, lp_fee_usd, source)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'subgraph')
            ON CONFLICT(snapshot_date, chain, version, pool_address)
            DO UPDATE SET
                tvl_usd              = excluded.tvl_usd,
                volume_24h_usd       = excluded.volume_24h_usd,
                fees_24h_usd         = excluded.fees_24h_usd,
                tx_count             = excluded.tx_count,
                protocol_fee_est_usd = excluded.protocol_fee_est_usd,
                lp_fee_usd           = excluded.lp_fee_usd
        """, (
            snapshot_date.isoformat(),
            r["chain"], r["version"], r["pool_address"],
            r["token0_symbol"], r["token1_symbol"],
            r["fee_tier"], r["hooks"], r["tick_spacing"],
            r["tvl_usd"], r["volume_24h_usd"], r["fees_24h_usd"],
            r["tx_count"], r["protocol_fee_est_usd"], r["lp_fee_usd"],
        ))


# ── Main entry ────────────────────────────────────────────────────────────────

def collect_all(snapshot_date: Optional[date] = None) -> dict:
    """Fetch v3+v4 pools for BNB and Arbitrum, store in DB.
    Returns {"total": int, "skipped": list[str]}."""
    if snapshot_date is None:
        snapshot_date = date.today()

    init_db()
    conn = sqlite3.connect(DB_PATH)
    total = 0
    skipped = []

    for chain in ("bnb", "arbitrum"):
        chain_got_data = False
        for version in ("v3", "v4"):
            rows = []

            # 1) Try The Graph decentralized network (if API key set, v3 only)
            if version == "v3":
                graph_pools = _graph_fetch(chain, version)
                if graph_pools is not None:
                    rows = [_normalize_graph_v3(p, chain) for p in graph_pools]
                    print(f"[collector] {chain} {version}: {len(rows)} pools (The Graph)")

            # 2) Fall back to GeckoTerminal
            if not rows:
                slug_entry = GECKO_DEX_SLUGS.get(chain, {}).get(version)
                if slug_entry is None:
                    print(f"[collector] {chain} {version}: skipped (no data source available)")
                else:
                    network, dex = slug_entry
                    gecko_pools = _gecko_fetch(network, dex)
                    if gecko_pools:
                        rows = [r for p in gecko_pools if (r := _parse_gecko(p, chain, version))]
                        print(f"[collector] {chain} {version}: {len(rows)} pools (GeckoTerminal)")
                    else:
                        print(f"[collector] {chain} {version}: skipped (no data returned)")

            if rows:
                _upsert(conn, rows, snapshot_date)
                total += len(rows)
                chain_got_data = True

        if not chain_got_data:
            skipped.append(chain)

    conn.commit()
    conn.close()
    print(f"[collector] Done — {total} total rows for {snapshot_date}")
    if skipped:
        print(f"[collector] WARNING: no data collected for chains: {skipped}")
    return {"total": total, "skipped": skipped}
