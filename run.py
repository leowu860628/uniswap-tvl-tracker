"""
Entry point: load env, start scheduler.
Run the dashboard separately: streamlit run dashboard/app.py
"""

from dotenv import load_dotenv
load_dotenv()

import sys
from datetime import date

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "collect":
        # One-off collection: python run.py collect
        from src.collector import collect_all
        result = collect_all()
        print(result)
    elif len(sys.argv) > 1 and sys.argv[1] == "report":
        # One-off report (collect + send Telegram): python run.py report
        from src.scheduler import daily_report
        from src.notifier import send_telegram
        try:
            daily_report()
        except Exception as e:
            send_telegram(f"❌ Uniswap TVL bot FAILED ({date.today()}):\n{e}")
            raise
    elif len(sys.argv) > 1 and sys.argv[1] == "watchdog":
        # Check if today's report was sent; alert if not: python run.py watchdog
        import sqlite3
        from pathlib import Path
        from src.notifier import send_telegram
        DB_PATH = Path(__file__).parent / "data" / "tvl.db"
        today = date.today()
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
    else:
        print("Dashboard: streamlit run dashboard/app.py")
        print("Scheduler starting...")
        from src.scheduler import start
        start()
