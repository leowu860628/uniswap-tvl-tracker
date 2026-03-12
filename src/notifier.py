"""
Send Telegram messages via Bot API.
"""

import os
import requests
from datetime import date
from typing import Optional, List


def send_telegram(message: str) -> bool:
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        print("[notifier] Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID")
        return False
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": message, "parse_mode": "HTML"},
            timeout=15,
        )
        resp.raise_for_status()
        return True
    except Exception as e:
        print(f"[notifier] Telegram send failed: {e}")
        return False


def _fmt_usd(val: float) -> str:
    if val >= 1_000_000:
        return f"${val/1_000_000:.1f}M"
    if val >= 1_000:
        return f"${val/1_000:.1f}K"
    return f"${val:.0f}"


def _fmt_pct(val: Optional[float]) -> str:
    if val is None:
        return "N/A"
    sign = "+" if val >= 0 else ""
    return f"{sign}{val*100:.1f}%"


def _movers_section(movers: list[dict], label: str) -> str:
    if not movers:
        return ""
    lines = [f"<b>[{label}]</b>"]
    for r in movers[:5]:
        pair = f"{r['token0_symbol']}/{r['token1_symbol']} {r['fee_tier']/10000:.2f}%"
        tvl = _fmt_usd(r["tvl_usd"])
        tvl_chg = _fmt_pct(r["tvl_change_pct"])
        vol_chg = _fmt_pct(r["volume_change_pct"])
        lines.append(f"  • {pair}: TVL {tvl} ({tvl_chg}), Vol {vol_chg}")
    return "\n".join(lines)


def _top_section(pools: list[dict], label: str) -> str:
    if not pools:
        return ""
    lines = [f"<b>[{label}]</b>"]
    for i, r in enumerate(pools[:5], 1):
        pair = f"{r['token0_symbol']}/{r['token1_symbol']} {r['fee_tier']/10000:.2f}%"
        tvl = _fmt_usd(r["tvl_usd"])
        chg = _fmt_pct(r.get("tvl_change_pct"))
        lines.append(f"  {i}. {pair}: {tvl} ({chg})")
    return "\n".join(lines)


def build_daily_message(
    snapshot_date: date,
    bnb_v3_movers: list[dict],
    bnb_v4_movers: list[dict],
    arb_v3_movers: list[dict],
    arb_v4_movers: list[dict],
    top_bnb_v3: list[dict],
    top_bnb_v4: list[dict],
    top_arb_v3: list[dict],
    top_arb_v4: list[dict],
    proto_fees: dict,
    ai_summary: str,
) -> str:
    parts = [
        f"🦄 <b>Uniswap TVL Daily Report — BNB + Arbitrum</b>",
        f"📅 {snapshot_date.isoformat()}",
    ]

    # Significant movers
    all_movers = [
        _movers_section(bnb_v3_movers, "BNB v3"),
        _movers_section(bnb_v4_movers, "BNB v4"),
        _movers_section(arb_v3_movers, "Arbitrum v3"),
        _movers_section(arb_v4_movers, "Arbitrum v4"),
    ]
    mover_text = "\n".join(s for s in all_movers if s)
    if mover_text:
        parts.append("\n🔥 <b>Significant Changes (&gt;10%):</b>\n" + mover_text)
    else:
        parts.append("\n✅ No significant changes today.")

    # Top pools
    top_text = "\n".join(filter(None, [
        _top_section(top_bnb_v3, "BNB v3"),
        _top_section(top_bnb_v4, "BNB v4"),
        _top_section(top_arb_v3, "Arbitrum v3"),
        _top_section(top_arb_v4, "Arbitrum v4"),
    ]))
    if top_text:
        parts.append("\n📊 <b>Top 5 TVL Today:</b>\n" + top_text)

    # Protocol fees
    if proto_fees:
        fee_parts = []
        for chain, total in proto_fees.items():
            fee_parts.append(f"{chain.upper()}: {_fmt_usd(total)}")
        parts.append("\n💰 <b>Est. Protocol Fees Today (v3):</b>\n  " + " | ".join(fee_parts))

    # AI summary
    parts.append(f"\n🤖 <b>Trend Summary:</b>\n{ai_summary}")

    return "\n".join(parts)
