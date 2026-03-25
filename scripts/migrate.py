"""
One-time migration script: seeds pool data and whitelist.
Run by start.sh before Streamlit starts. Safe to re-run (INSERT OR IGNORE).
Version-gated so future migrations can increment MIGRATION_VERSION.
"""

import gzip
import json
import os
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from dotenv import load_dotenv
load_dotenv()

from src.collector import init_db, DB_PATH

MIGRATION_VERSION = "v3"
FLAG = DB_PATH.parent / f".migrated_{MIGRATION_VERSION}"
SEED_PATH = Path(__file__).parent.parent / "data" / "seed.json.gz"

_owner = os.environ.get("DASHBOARD_OWNER_EMAIL", "")
WHITELIST = [e for e in [
    (_owner,                      "owner") if _owner else None,
    ("doxie.k@pancakeswap.com",   "team"),
    ("maroon@pancakeswap.com",    "team"),
    ("madeline@pancakeswap.com",  "team"),
] if e]


def seed_pool_data(conn: sqlite3.Connection):
    if not SEED_PATH.exists():
        print(f"[migrate] seed.json.gz not found at {SEED_PATH}, skipping pool data.")
        return
    print(f"[migrate] Loading {SEED_PATH} ...")
    with gzip.open(SEED_PATH, "rt", encoding="utf-8") as f:
        rows = json.load(f)
    conn.executemany(
        """INSERT OR IGNORE INTO pool_snapshots
           (snapshot_date, chain, version, pool_address, token0_symbol, token1_symbol,
            fee_tier, tvl_usd, volume_24h_usd, fees_24h_usd, protocol_fee_est_usd,
            lp_fee_usd, apr, source, is_deleted)
           VALUES (:snapshot_date,:chain,:version,:pool_address,:token0_symbol,:token1_symbol,
                   :fee_tier,:tvl_usd,:volume_24h_usd,:fees_24h_usd,:protocol_fee_est_usd,
                   :lp_fee_usd,:apr,:source,:is_deleted)""",
        rows,
    )
    print(f"[migrate] Inserted up to {len(rows)} pool rows (duplicates skipped).")


def seed_whitelist(conn: sqlite3.Connection):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS auth_whitelist (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            email     TEXT NOT NULL UNIQUE,
            added_by  TEXT DEFAULT 'cli',
            added_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            notes     TEXT
        )
    """)
    for email, notes in WHITELIST:
        conn.execute(
            "INSERT OR IGNORE INTO auth_whitelist (email, added_by, notes) VALUES (?,?,?)",
            (email.lower(), "migrate", notes),
        )
    print(f"[migrate] Whitelist seeded ({len(WHITELIST)} entries, duplicates skipped).")


def main():
    if FLAG.exists():
        print(f"[migrate] Already migrated ({MIGRATION_VERSION}), skipping.")
        return

    print(f"[migrate] Running migration {MIGRATION_VERSION} ...")
    print(f"[migrate] DB path: {DB_PATH}")

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    init_db()

    conn = sqlite3.connect(DB_PATH)
    try:
        seed_pool_data(conn)
        seed_whitelist(conn)
        conn.commit()
    except Exception as e:
        print(f"[migrate] ERROR: {e}")
        conn.rollback()
        conn.close()
        sys.exit(1)
    conn.close()

    FLAG.touch()
    print(f"[migrate] Done. Flag written to {FLAG}")


if __name__ == "__main__":
    main()
