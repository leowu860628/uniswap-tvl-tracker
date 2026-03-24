"""
Google OAuth 2.0 + whitelist authentication for the Uniswap TVL dashboard.
"""

import gzip
import hashlib
import hmac
import json
import os
import sqlite3
import time
import urllib.parse
from pathlib import Path
from typing import Optional

import requests

DB_PATH = Path(os.environ.get("DATA_DIR", str(Path(__file__).parent.parent / "data"))) / "tvl.db"

GOOGLE_AUTH_URI  = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URI = "https://oauth2.googleapis.com/token"
GOOGLE_USERINFO  = "https://openidconnect.googleapis.com/v1/userinfo"
SCOPES = "openid email profile"


def _seed_from_bundle() -> None:
    """Load pool_snapshots from the bundled seed file. Runs once (guarded by a flag file)."""
    seed_path = Path(__file__).parent.parent / "data" / "seed.json.gz"
    flag_path = DB_PATH.parent / ".seeded"
    if flag_path.exists() or not seed_path.exists():
        return
    with gzip.open(seed_path, "rt", encoding="utf-8") as f:
        rows = json.load(f)
    conn = sqlite3.connect(DB_PATH)
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
    conn.commit()
    conn.close()
    flag_path.touch()  # Mark as seeded so this never runs again


def init_auth_db() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS auth_whitelist (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            email     TEXT NOT NULL UNIQUE,
            added_by  TEXT DEFAULT 'cli',
            added_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            notes     TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS auth_access_log (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            email       TEXT NOT NULL,
            name        TEXT,
            picture_url TEXT,
            access_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            was_allowed INTEGER NOT NULL
        )
    """)
    conn.commit()
    conn.close()


def _sign_state(timestamp: str) -> str:
    secret = os.environ.get("GOOGLE_CLIENT_SECRET", "fallback")
    return hmac.new(secret.encode(), timestamp.encode(), hashlib.sha256).hexdigest()[:24]


def generate_state() -> str:
    """Generate a signed, time-stamped state token (no session state needed)."""
    ts = str(int(time.time()))
    return f"{ts}.{_sign_state(ts)}"


def verify_state(state: str) -> bool:
    """Verify state token without session state. Valid for 10 minutes."""
    try:
        ts, sig = state.rsplit(".", 1)
        if abs(time.time() - int(ts)) > 600:
            return False
        return hmac.compare_digest(sig, _sign_state(ts))
    except Exception:
        return False


def get_auth_url() -> str:
    params = {
        "client_id":     os.environ["GOOGLE_CLIENT_ID"],
        "redirect_uri":  os.environ["GOOGLE_REDIRECT_URI"],
        "response_type": "code",
        "scope":         SCOPES,
        "state":         generate_state(),
        "access_type":   "online",
        "prompt":        "select_account",
    }
    return GOOGLE_AUTH_URI + "?" + urllib.parse.urlencode(params)


def handle_callback(code: str) -> dict:
    """Exchange auth code for user info. Raises RuntimeError on failure."""
    token_resp = requests.post(GOOGLE_TOKEN_URI, data={
        "code":          code,
        "client_id":     os.environ["GOOGLE_CLIENT_ID"],
        "client_secret": os.environ["GOOGLE_CLIENT_SECRET"],
        "redirect_uri":  os.environ["GOOGLE_REDIRECT_URI"],
        "grant_type":    "authorization_code",
    }, timeout=15)
    token_resp.raise_for_status()
    access_token = token_resp.json().get("access_token")
    if not access_token:
        raise RuntimeError("No access token in Google response.")

    user_resp = requests.get(
        GOOGLE_USERINFO,
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=15,
    )
    user_resp.raise_for_status()
    user = user_resp.json()
    if not user.get("email_verified"):
        raise RuntimeError("Google account email is not verified.")
    return user


def is_whitelisted(email: str) -> bool:
    # Owner is always allowed regardless of the whitelist table
    owner = os.environ.get("DASHBOARD_OWNER_EMAIL", "")
    if owner and email.lower() == owner.lower():
        return True
    conn = sqlite3.connect(DB_PATH)
    row = conn.execute(
        "SELECT 1 FROM auth_whitelist WHERE email = ?", (email.lower(),)
    ).fetchone()
    conn.close()
    return row is not None


def log_access(email: str, name: str, was_allowed: bool, picture_url: str = "") -> None:
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "INSERT INTO auth_access_log (email, name, picture_url, was_allowed) VALUES (?,?,?,?)",
        (email.lower(), name, picture_url, int(was_allowed)),
    )
    conn.commit()
    conn.close()


def add_to_whitelist(email: str, added_by: str = "cli", notes: str = "") -> bool:
    """Returns True if newly added, False if already present."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.execute(
        "INSERT OR IGNORE INTO auth_whitelist (email, added_by, notes) VALUES (?,?,?)",
        (email.lower(), added_by, notes),
    )
    conn.commit()
    added = cursor.rowcount > 0
    conn.close()
    return added


def remove_from_whitelist(email: str) -> bool:
    """Returns True if a row was deleted."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.execute(
        "DELETE FROM auth_whitelist WHERE email = ?", (email.lower(),)
    )
    conn.commit()
    removed = cursor.rowcount > 0
    conn.close()
    return removed


def get_whitelist() -> list:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT id, email, added_by, added_at, notes FROM auth_whitelist ORDER BY added_at DESC"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_access_log(limit: int = 100) -> list:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT email, name, access_time, was_allowed FROM auth_access_log "
        "ORDER BY access_time DESC LIMIT ?",
        (limit,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]
