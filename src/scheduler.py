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

from src.collector import collect_all
from src.analyzer import get_significant_movers, get_snapshots, get_protocol_fee_totals
from src.summarizer import generate_summary
from src.notifier import build_daily_message, send_telegram


def daily_report(snapshot_date: Optional[date] = None):
    if snapshot_date is None:
        snapshot_date = date.today()
    print(f"[scheduler] Starting daily report for {snapshot_date}")

    collect_all(snapshot_date)

    bnb_v3_movers  = get_significant_movers("bnb",      "v3")
    bnb_v4_movers  = get_significant_movers("bnb",      "v4")
    arb_v3_movers  = get_significant_movers("arbitrum", "v3")
    arb_v4_movers  = get_significant_movers("arbitrum", "v4")

    top_bnb_v3  = get_snapshots(snapshot_date, "bnb",      "v3")[:5]
    top_bnb_v4  = get_snapshots(snapshot_date, "bnb",      "v4")[:5]
    top_arb_v3  = get_snapshots(snapshot_date, "arbitrum", "v3")[:5]
    top_arb_v4  = get_snapshots(snapshot_date, "arbitrum", "v4")[:5]

    proto_fees = get_protocol_fee_totals(snapshot_date)

    ai_summary = generate_summary(
        bnb_v3_movers, bnb_v4_movers,
        arb_v3_movers, arb_v4_movers,
        top_bnb_v3, top_bnb_v4,
        top_arb_v3, top_arb_v4,
    )

    message = build_daily_message(
        snapshot_date,
        bnb_v3_movers, bnb_v4_movers,
        arb_v3_movers, arb_v4_movers,
        top_bnb_v3, top_bnb_v4,
        top_arb_v3, top_arb_v4,
        proto_fees,
        ai_summary,
    )

    send_telegram(message)
    print("[scheduler] Daily report sent.")


def start():
    tz = pytz.timezone("Asia/Shanghai")  # UTC+8
    scheduler = BlockingScheduler()
    scheduler.add_job(daily_report, CronTrigger(hour=11, minute=0, timezone=tz))
    print("[scheduler] Scheduled daily report at 11:00 AM UTC+8. Press Ctrl+C to stop.")
    scheduler.start()
