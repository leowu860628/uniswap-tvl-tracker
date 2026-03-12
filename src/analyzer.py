"""
Compute TVL/volume changes and detect significant movers.
"""

import sqlite3
from datetime import date, timedelta
from pathlib import Path
from typing import Optional, List

DB_PATH = Path(__file__).parent.parent / "data" / "tvl.db"

TIMEFRAMES = {
    "day":      1,
    "weekly":   7,
    "biweekly": 14,
}


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def get_snapshots(snapshot_date: date, chain: Optional[str] = None, version: Optional[str] = None) -> List[dict]:
    """Return all pool rows for a given date, optionally filtered."""
    conn = _conn()
    wheres = ["snapshot_date = ?"]
    params = [snapshot_date.isoformat()]
    if chain:
        wheres.append("chain = ?")
        params.append(chain)
    if version:
        wheres.append("version = ?")
        params.append(version)
    wheres.append("(is_deleted IS NULL OR is_deleted = 0)")
    rows = conn.execute(
        f"SELECT * FROM pool_snapshots WHERE {' AND '.join(wheres)} ORDER BY tvl_usd DESC",
        params,
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_changes(
    chain: Optional[str] = None,
    version: Optional[str] = None,
    date_today: Optional[date] = None,
    days_back: int = 1,
) -> List[dict]:
    """
    Compare today's snapshot vs N days ago.
    Returns list of dicts augmented with tvl_change_pct, volume_change_pct.
    """
    if date_today is None:
        date_today = date.today()
    date_prev = date_today - timedelta(days=days_back)

    today_rows = {
        (r["chain"], r["version"], r["pool_address"]): r
        for r in get_snapshots(date_today, chain, version)
    }
    prev_rows = {
        (r["chain"], r["version"], r["pool_address"]): r
        for r in get_snapshots(date_prev, chain, version)
    }

    result = []
    for key, today in today_rows.items():
        row = dict(today)
        prev = prev_rows.get(key)
        if prev:
            row["tvl_change_pct"] = _pct(today["tvl_usd"], prev["tvl_usd"])
            row["volume_change_pct"] = _pct(today["volume_24h_usd"], prev["volume_24h_usd"])
            row["tvl_prev"] = prev["tvl_usd"]
            row["volume_prev"] = prev["volume_24h_usd"]
        else:
            row["tvl_change_pct"] = None
            row["volume_change_pct"] = None
            row["tvl_prev"] = None
            row["volume_prev"] = None
        result.append(row)

    return result


def _pct(new: float, old: float) -> Optional[float]:
    if old is None or old == 0:
        return None
    return (new - old) / old


def get_significant_movers(
    chain: Optional[str] = None,
    version: Optional[str] = None,
    threshold: float = 0.10,
    date_today: Optional[date] = None,
    days_back: int = 1,
) -> List[dict]:
    """Return pools where TVL or volume changed by more than threshold."""
    rows = get_changes(chain, version, date_today, days_back)
    movers = []
    for r in rows:
        tvl_sig = r["tvl_change_pct"] is not None and abs(r["tvl_change_pct"]) >= threshold
        vol_sig = r["volume_change_pct"] is not None and abs(r["volume_change_pct"]) >= threshold
        if tvl_sig or vol_sig:
            movers.append(r)
    movers.sort(key=lambda r: abs(r["tvl_change_pct"] or 0), reverse=True)
    return movers


def get_protocol_fee_totals(snapshot_date: Optional[date] = None) -> dict:
    """Sum estimated v3 protocol fees by chain for a given date."""
    if snapshot_date is None:
        snapshot_date = date.today()
    conn = _conn()
    rows = conn.execute("""
        SELECT chain, SUM(protocol_fee_est_usd) as total
        FROM pool_snapshots
        WHERE snapshot_date = ? AND version = 'v3' AND protocol_fee_est_usd IS NOT NULL
        GROUP BY chain
    """, (snapshot_date.isoformat(),)).fetchall()
    conn.close()
    return {r["chain"]: r["total"] or 0.0 for r in rows}


def get_available_dates() -> list[str]:
    """Return distinct snapshot dates in descending order."""
    try:
        conn = _conn()
        rows = conn.execute(
            "SELECT DISTINCT snapshot_date FROM pool_snapshots ORDER BY snapshot_date DESC"
        ).fetchall()
        conn.close()
        return [r["snapshot_date"] for r in rows]
    except Exception:
        return []
