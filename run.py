"""
Entry point: load env, start scheduler.
Run the dashboard separately: streamlit run dashboard/app.py
"""

from dotenv import load_dotenv
load_dotenv()

import sys

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "collect":
        # One-off collection: python run.py collect
        from src.collector import collect_all
        collect_all()
    elif len(sys.argv) > 1 and sys.argv[1] == "report":
        # One-off report (collect + send Telegram): python run.py report
        from src.scheduler import daily_report
        daily_report()
    else:
        print("Dashboard: streamlit run dashboard/app.py")
        print("Scheduler starting...")
        from src.scheduler import start
        start()
