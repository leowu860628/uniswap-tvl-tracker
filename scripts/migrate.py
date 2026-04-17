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

MIGRATION_VERSION = "v5"
FLAG = DB_PATH.parent / f".migrated_{MIGRATION_VERSION}"
SEED_PATH = Path(__file__).parent.parent / "data" / "seed.json.gz"

_owner = os.environ.get("DASHBOARD_OWNER_EMAIL", "")
WHITELIST = [e for e in [
    (_owner,                      "owner") if _owner else None,
    ("doxie.k@pancakeswap.com",   "team"),
    ("maroon@pancakeswap.com",    "team"),
    ("madeline@pancakeswap.com",  "team"),
] if e]


def seed_from_url(conn: sqlite3.Connection):
    """Download tvl.db from DB_SEED_URL and merge all rows via INSERT OR IGNORE."""
    url = os.environ.get("DB_SEED_URL")
    if not url:
        return
    import tempfile, urllib.request
    print(f"[migrate] Downloading DB seed from {url} ...")
    tmp = Path(tempfile.mktemp(suffix=".db"))
    try:
        urllib.request.urlretrieve(url, tmp)
        src = sqlite3.connect(tmp)
        src.row_factory = sqlite3.Row
        # Run init_db migrations on the source so old DBs have apr/is_deleted columns
        for col_sql in [
            "ALTER TABLE pool_snapshots ADD COLUMN apr REAL",
            "ALTER TABLE pool_snapshots ADD COLUMN is_deleted INTEGER DEFAULT 0",
        ]:
            try: src.execute(col_sql)
            except Exception: pass
        rows = src.execute("""
            SELECT snapshot_date, chain, version, pool_address,
                   token0_symbol, token1_symbol, fee_tier,
                   tvl_usd, volume_24h_usd, fees_24h_usd,
                   protocol_fee_est_usd, lp_fee_usd, apr, source,
                   COALESCE(is_deleted, 0) AS is_deleted
            FROM pool_snapshots
        """).fetchall()
        src.close()
        conn.executemany(
            """INSERT OR IGNORE INTO pool_snapshots
               (snapshot_date, chain, version, pool_address, token0_symbol, token1_symbol,
                fee_tier, tvl_usd, volume_24h_usd, fees_24h_usd, protocol_fee_est_usd,
                lp_fee_usd, apr, source, is_deleted)
               VALUES (:snapshot_date,:chain,:version,:pool_address,:token0_symbol,:token1_symbol,
                       :fee_tier,:tvl_usd,:volume_24h_usd,:fees_24h_usd,:protocol_fee_est_usd,
                       :lp_fee_usd,:apr,:source,:is_deleted)""",
            [dict(r) for r in rows],
        )
        print(f"[migrate] Merged {len(rows)} rows from remote DB (duplicates skipped).")
    except Exception as e:
        print(f"[migrate] WARNING: Failed to seed from URL: {e}")
    finally:
        tmp.unlink(missing_ok=True)


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


def _db_is_stale(conn: sqlite3.Connection) -> bool:
    """Return True if the DB has no snapshot data from the past 14 days."""
    from datetime import date, timedelta
    cutoff = (date.today() - timedelta(days=14)).isoformat()
    row = conn.execute(
        "SELECT 1 FROM pool_snapshots WHERE snapshot_date >= ? LIMIT 1", (cutoff,)
    ).fetchone()
    return row is None


def main():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    init_db()

    conn = sqlite3.connect(DB_PATH)
    try:
        # Always re-seed when DB lacks recent data (covers fresh deploys and restarts)
        if _db_is_stale(conn):
            print(f"[migrate] DB has no recent data — seeding ...")
            seed_from_url(conn)
            seed_pool_data(conn)
        else:
            print(f"[migrate] DB has recent data, skipping seed.")

        # Schema/whitelist migrations: run once per version
        if not FLAG.exists():
            print(f"[migrate] Running one-time migration {MIGRATION_VERSION} ...")
            seed_whitelist(conn)
            conn.commit()
            FLAG.touch()
            print(f"[migrate] Done. Flag written to {FLAG}")
        else:
            print(f"[migrate] One-time migration {MIGRATION_VERSION} already ran.")
            conn.commit()
    except Exception as e:
        print(f"[migrate] ERROR: {e}")
        conn.rollback()
        conn.close()
        sys.exit(1)
    conn.close()


if __name__ == "__main__":
    main()
