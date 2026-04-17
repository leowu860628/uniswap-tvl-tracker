"""
APScheduler: run daily_report() at 11:00 AM UTC+8.
"""

from dotenv import load_dotenv
load_dotenv()

from datetime import date
from typing import Optional
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz

import sqlite3

from src.collector import collect_all, DB_PATH, POOL_WATCHLIST
from src.analyzer import get_changes_fallback, get_snapshots, get_protocol_fee_totals
from src.summarizer import generate_defensive_suggestion, generate_cross_chain_insight
from src.notifier import build_chain_message, send_telegram, send_chain_report

# Chains to report on — add new chains here as data sources become available
CHAINS = ["bnb", "arbitrum", "base", "monad"]

THRESHOLD = 0.10   # 10% significance threshold for movers


def _report_already_sent(d: date) -> bool:
    try:
        conn = sqlite3.connect(DB_PATH)
        row = conn.execute(
            "SELECT 1 FROM report_log WHERE report_date = ? AND status = 'ok'",
            (d.isoformat(),),
        ).fetchone()
        conn.close()
        return row is not None
    except Exception:
        return False


def _update_local_seed():
    """Regenerate data/seed.json.gz from the live DB.

    Called after each successful daily report so that a Railway restart
    (same container, no new deploy) re-seeds with current data instead of
    the stale seed bundled at deploy time.
    """
    import gzip, json
    from pathlib import Path
    try:
        import sqlite3 as _sql
        conn = _sql.connect(DB_PATH)
        conn.row_factory = _sql.Row
        rows = conn.execute("""
            SELECT snapshot_date, chain, version, pool_address,
                   token0_symbol, token1_symbol, fee_tier,
                   tvl_usd, volume_24h_usd, fees_24h_usd,
                   protocol_fee_est_usd, lp_fee_usd, apr, source,
                   COALESCE(is_deleted, 0) AS is_deleted
            FROM pool_snapshots
            ORDER BY snapshot_date, chain, version, pool_address
        """).fetchall()
        conn.close()
        seed_path = Path(DB_PATH).parent / "seed.json.gz"
        with gzip.open(seed_path, "wt", encoding="utf-8") as f:
            json.dump([dict(r) for r in rows], f)
        print(f"[scheduler] Local seed updated: {len(rows)} rows → {seed_path}")
    except Exception as e:
        print(f"[scheduler] WARNING: failed to update local seed: {e}")


def _mark_report_sent(d: date):
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute(
            "INSERT OR REPLACE INTO report_log (report_date, status) VALUES (?, 'ok')",
            (d.isoformat(),),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[scheduler] Failed to mark report sent: {e}")


def _merge_changes(today_rows, dod_map, weekly_map):
    """
    Build a combined per-pool list that includes both DoD and weekly change fields.
    Keyed by (chain, version, pool_address).
    """
    result = []
    for key, row in today_rows.items():
        r = dict(row)
        dod  = dod_map.get(key, {})
        wkly = weekly_map.get(key, {})

        r["dod_tvl_chg"]  = dod.get("tvl_change_pct")
        r["dod_vol_chg"]  = dod.get("volume_change_pct")
        r["wkly_tvl_chg"] = wkly.get("tvl_change_pct")
        r["wkly_vol_chg"] = wkly.get("volume_change_pct")

        # is_mover flags
        r["is_dod_mover"]  = (
            (r["dod_tvl_chg"]  is not None and abs(r["dod_tvl_chg"])  >= THRESHOLD) or
            (r["dod_vol_chg"]  is not None and abs(r["dod_vol_chg"])  >= THRESHOLD)
        )
        r["is_wkly_mover"] = (
            (r["wkly_tvl_chg"] is not None and abs(r["wkly_tvl_chg"]) >= THRESHOLD) or
            (r["wkly_vol_chg"] is not None and abs(r["wkly_vol_chg"]) >= THRESHOLD)
        )
        result.append(r)
    return result


def _collect_chain_data(snapshot_date: date, chain: str, version: str):
    """Return (combined_rows, dod_date, weekly_date) for one chain/version."""
    dod_rows,    dod_date    = get_changes_fallback(chain, version, snapshot_date, days_back=1)
    weekly_rows, weekly_date = get_changes_fallback(chain, version, snapshot_date, days_back=7)

    today_map  = {(r["chain"], r["version"], r["pool_address"]): r
                  for r in get_snapshots(snapshot_date, chain, version)}
    dod_map    = {(r["chain"], r["version"], r["pool_address"]): r for r in dod_rows}
    weekly_map = {(r["chain"], r["version"], r["pool_address"]): r for r in weekly_rows}

    combined = _merge_changes(today_map, dod_map, weekly_map)
    combined.sort(key=lambda r: r["tvl_usd"] or 0, reverse=True)
    return combined, dod_date, weekly_date


def daily_report(snapshot_date: Optional[date] = None):
    if snapshot_date is None:
        snapshot_date = date.today()

    if _report_already_sent(snapshot_date):
        print(f"[scheduler] Report for {snapshot_date} already sent. Skipping.")
        return

    print(f"[scheduler] Starting daily report for {snapshot_date}")

    result = collect_all(snapshot_date)
    skipped = result.get("skipped", [])
    if skipped:
        send_telegram(
            f"⚠️ Data collection incomplete for {snapshot_date}.\n"
            f"Skipped chains: {', '.join(skipped)}"
        )

    proto_fees = get_protocol_fee_totals(snapshot_date)

    all_chain_data = {}
    for chain in CHAINS:
        v3, dod_date_v3, wkly_date_v3 = _collect_chain_data(snapshot_date, chain, "v3")
        v4, dod_date_v4, wkly_date_v4 = _collect_chain_data(snapshot_date, chain, "v4")

        # Use the v3 dates as the chain-level reference (v4 should be the same)
        dod_date    = dod_date_v3  or dod_date_v4
        weekly_date = wkly_date_v3 or wkly_date_v4

        all_chain_data[chain] = {
            "v3":          v3,
            "v4":          v4,
            "dod_date":    dod_date,
            "weekly_date": weekly_date,
        }

    # Send one message per chain: data changes + highlights + defensive suggestion
    any_sent = False
    for chain in CHAINS:
        d = all_chain_data[chain]

        suggestion = generate_defensive_suggestion(
            chain,
            d["v3"], d["v4"],
            d["dod_date"], d["weekly_date"],
        )
        msg = build_chain_message(
            snapshot_date, chain,
            d["v3"], d["v4"],
            d["dod_date"], d["weekly_date"],
            proto_fees.get(chain),
            suggestion=suggestion,
            watchlist_v3=POOL_WATCHLIST.get((chain, "v3"), []),
            watchlist_v4=POOL_WATCHLIST.get((chain, "v4"), []),
        )
        if send_chain_report(chain, msg):
            any_sent = True

        print(f"[scheduler] Sent report for {chain}.")

    # Cross-chain insight (optional — only sent when a real pattern is detected)
    if len(CHAINS) >= 2:
        insight = generate_cross_chain_insight(all_chain_data)
        if insight:
            cross_msg = "🔗 <b>Cross-Chain Pattern Detected</b>\n\n" + insight
            send_telegram(cross_msg)
            print("[scheduler] Cross-chain insight sent.")

    if not any_sent:
        print(f"[scheduler] ERROR: All Telegram sends failed for {snapshot_date}. Report NOT marked as sent.")
        return

    _mark_report_sent(snapshot_date)
    _update_local_seed()
    print("[scheduler] Daily report complete.")


def start():
    tz = pytz.timezone("Asia/Shanghai")  # UTC+8
    scheduler = BlockingScheduler()
    scheduler.add_job(daily_report, CronTrigger(hour=3, minute=0, timezone=tz))
    print("[scheduler] Scheduled daily report at 3:00 AM UTC+8. Press Ctrl+C to stop.")
    scheduler.start()
