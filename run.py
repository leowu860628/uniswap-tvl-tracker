"""
Entry point: load env, start scheduler.
Run the dashboard separately: streamlit run dashboard/app.py
"""

from dotenv import load_dotenv
load_dotenv()

import os
import sys
from datetime import datetime
import pytz

def _today_sgt():
    """Return today's date in UTC+8 (SGT/CST) as an ISO string."""
    return datetime.now(pytz.timezone("Asia/Shanghai")).date()

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "collect":
        # One-off collection: python run.py collect
        from src.collector import collect_all
        result = collect_all(_today_sgt())
        print(result)
    elif len(sys.argv) > 1 and sys.argv[1] == "report":
        # One-off report (collect + send Telegram): python run.py report
        from src.scheduler import daily_report
        from src.notifier import send_telegram
        try:
            daily_report()
        except Exception as e:
            send_telegram(f"❌ Uniswap TVL bot FAILED ({_today_sgt()}):\n{e}")
            raise
    elif len(sys.argv) > 1 and sys.argv[1] == "watchdog":
        # Check if today's report was sent; alert if not: python run.py watchdog
        import sqlite3
        from pathlib import Path
        from src.notifier import send_telegram
        DB_PATH = Path(os.environ.get("DATA_DIR", str(Path(__file__).parent / "data"))) / "tvl.db"
        today = _today_sgt()
        sent = False
        try:
            conn = sqlite3.connect(DB_PATH)
            row = conn.execute(
                "SELECT 1 FROM report_log WHERE report_date = ? AND status = 'ok'",
                (today.isoformat(),),
            ).fetchone()
            conn.close()
            sent = row is not None
        except Exception as e:
            print(f"[watchdog] Could not check report_log (table may not exist yet): {e}")
            sent = False

        if sent:
            print(f"[watchdog] Report for {today} confirmed sent. All good.")
        else:
            msg = (
                f"⚠️ ALERT: Uniswap TVL report for {today} was not sent by 12:30 PM UTC+8.\n"
                f"Trigger manually: python run.py report"
            )
            print(f"[watchdog] {msg}")
            send_telegram(msg)
    elif len(sys.argv) > 1 and sys.argv[1] == "whitelist":
        from dashboard.auth import init_auth_db, add_to_whitelist, remove_from_whitelist, get_whitelist
        init_auth_db()
        action = sys.argv[2] if len(sys.argv) > 2 else ""
        if action == "add" and len(sys.argv) >= 4:
            email = sys.argv[3]
            notes = sys.argv[4] if len(sys.argv) > 4 else ""
            ok = add_to_whitelist(email, added_by="cli", notes=notes)
            print(f"Added {email}." if ok else f"{email} already whitelisted.")
        elif action == "remove" and len(sys.argv) >= 4:
            ok = remove_from_whitelist(sys.argv[3])
            print(f"Removed {sys.argv[3]}." if ok else f"{sys.argv[3]} not found.")
        elif action == "list":
            entries = get_whitelist()
            if not entries:
                print("Whitelist is empty.")
            else:
                print(f"{'EMAIL':<40} {'ADDED_BY':<15} ADDED_AT")
                print("-" * 70)
                for e in entries:
                    print(f"{e['email']:<40} {e['added_by']:<15} {e['added_at']}")
        else:
            print("Usage: python run.py whitelist add <email> [notes]")
            print("       python run.py whitelist remove <email>")
            print("       python run.py whitelist list")
    else:
        print("Dashboard: streamlit run dashboard/app.py")
        print("Scheduler starting...")
        from src.scheduler import start
        start()
