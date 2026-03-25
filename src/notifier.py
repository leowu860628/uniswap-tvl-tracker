"""
Send Telegram messages via Bot API.
"""

import os
import requests
from datetime import date
from typing import Optional

CHAIN_EMOJI = {"bnb": "🟡", "arbitrum": "🔵", "base": "🔷", "monad": "🟣"}
CHAIN_LABEL = {"bnb": "BNB Chain", "arbitrum": "Arbitrum", "base": "Base", "monad": "Monad"}

MIN_TVL        = 100_000   # $100K minimum TVL to appear in any section
MOVER_THRESHOLD = 0.10     # 10% change = significant mover


def send_telegram(message: str) -> bool:
    token   = os.environ.get("TELEGRAM_BOT_TOKEN")
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


# ── Formatters ────────────────────────────────────────────────────────────────

def _fmt_usd(val: float) -> str:
    if val >= 1_000_000:
        return f"${val/1_000_000:.1f}M"
    if val >= 1_000:
        return f"${val/1_000:.1f}K"
    return f"${val:.0f}"


def _fmt_pct(val: Optional[float], placeholder: str = "–") -> str:
    if val is None:
        return placeholder
    sign = "+" if val >= 0 else ""
    return f"{sign}{val*100:.1f}%"


def _date_label(d: Optional[date]) -> str:
    return d.strftime("%-m/%-d") if d else "?"


def _sort_key(r: dict, tvl_key: str, vol_key: str):
    """Primary: TVL change desc. Secondary: Vol change desc."""
    return (-(r.get(tvl_key) or 0), -(r.get(vol_key) or 0))


def _fmt_fee(fee_tier, version="") -> str:
    if fee_tier:
        return f"{fee_tier/10000:.4g}%"
    return "N/A" if version == "v4" else "0%"


def _pool_line(r: dict) -> str:
    pair = f"{r['token0_symbol']}/{r['token1_symbol']} {_fmt_fee(r.get('fee_tier', 0) or 0, r.get('version', ''))}"
    tvl      = _fmt_usd(r["tvl_usd"])
    dod_tvl  = _fmt_pct(r["dod_tvl_chg"])
    wkly_tvl = _fmt_pct(r["wkly_tvl_chg"])
    dod_vol  = _fmt_pct(r["dod_vol_chg"])
    wkly_vol = _fmt_pct(r["wkly_vol_chg"])
    return (
        f"  • {pair}: TVL {tvl}\n"
        f"    TVL DoD {dod_tvl} | WoW {wkly_tvl}  ·  Vol DoD {dod_vol} | WoW {wkly_vol}"
    )


# ── Section builders ──────────────────────────────────────────────────────────

def _movers_section(pools: list[dict], label: str, use_dod: bool) -> str:
    """
    Significant movers for one timeframe (DoD or weekly).

    Eligibility: TVL >= $100K AND change >= 10% in the given timeframe.
    Ranking: TVL change desc (primary), Vol change desc (secondary).
    Cap: pools with TVL change >= +10% are all listed; others capped at 5.
    """
    tvl_key = "dod_tvl_chg"  if use_dod else "wkly_tvl_chg"
    vol_key = "dod_vol_chg"  if use_dod else "wkly_vol_chg"
    flag    = "is_dod_mover" if use_dod else "is_wkly_mover"

    eligible = [
        r for r in pools
        if (r.get("tvl_usd") or 0) >= MIN_TVL and r.get(flag)
    ]
    if not eligible:
        return ""

    eligible.sort(key=lambda r: _sort_key(r, tvl_key, vol_key))

    # Pools with positive TVL ≥ +10%: show all. Others: cap at 5.
    positive_tvl = [r for r in eligible if (r.get(tvl_key) or 0) >= MOVER_THRESHOLD]
    other        = [r for r in eligible if (r.get(tvl_key) or 0) <  MOVER_THRESHOLD]

    shown = positive_tvl + other[:5]

    lines = [f"<b>{label}</b>"]
    for r in shown:
        lines.append(_pool_line(r))
    return "\n".join(lines)


def _top_section(pools: list[dict], label: str) -> str:
    """
    Top pools by TVL (>= $100K), showing DoD and weekly % inline.
    Ranked: TVL change desc (primary), Vol change desc (secondary).
    All pools with positive TVL change >= +10% are listed; remainder capped at 5.
    """
    eligible = [r for r in pools if (r.get("tvl_usd") or 0) >= MIN_TVL]
    if not eligible:
        return ""

    eligible.sort(key=lambda r: _sort_key(r, "dod_tvl_chg", "dod_vol_chg"))

    positive_tvl = [r for r in eligible if (r.get("dod_tvl_chg") or 0) >= MOVER_THRESHOLD]
    other        = sorted(
        [r for r in eligible if (r.get("dod_tvl_chg") or 0) < MOVER_THRESHOLD],
        key=lambda r: -(r.get("tvl_usd") or 0),
    )[:5]

    shown = positive_tvl + other

    lines = [f"<b>{label}</b>"]
    for i, r in enumerate(shown, 1):
        pair = f"{r['token0_symbol']}/{r['token1_symbol']} {_fmt_fee(r.get('fee_tier', 0) or 0, r.get('version', ''))}"
        tvl      = _fmt_usd(r["tvl_usd"])
        dod_tvl  = _fmt_pct(r["dod_tvl_chg"])
        wkly_tvl = _fmt_pct(r["wkly_tvl_chg"])
        lines.append(f"  {i}. {pair}: {tvl}  (DoD {dod_tvl} | WoW {wkly_tvl})")
    return "\n".join(lines)


# ── Message builders ──────────────────────────────────────────────────────────

def build_chain_message(
    snapshot_date: date,
    chain: str,
    v3_pools: list[dict],
    v4_pools: list[dict],
    dod_date: Optional[date],
    weekly_date: Optional[date],
    proto_fee_total: Optional[float],
) -> str:
    """Data message: movers + top pools + protocol fees. No AI analysis."""
    emoji = CHAIN_EMOJI.get(chain, "⛓")
    label = CHAIN_LABEL.get(chain, chain.upper())

    dod_label    = f"vs {_date_label(dod_date)}"    if dod_date    else "N/A"
    weekly_label = f"vs {_date_label(weekly_date)}" if weekly_date else "N/A"

    parts = [
        f"🦄 <b>Uniswap — {label}</b>  {emoji}",
        f"📅 {snapshot_date.strftime('%-m/%-d/%Y')}  ·  DoD {dod_label}  ·  Weekly {weekly_label}",
    ]

    # DoD movers
    dod_blocks = list(filter(None, [
        _movers_section(v3_pools, "V3 — Day-over-day", use_dod=True),
        _movers_section(v4_pools, "V4 — Day-over-day", use_dod=True),
    ]))
    if dod_blocks:
        parts.append("\n🔥 <b>Notable DoD Changes (&gt;10%, TVL &gt;$100K):</b>\n" + "\n\n".join(dod_blocks))
    else:
        parts.append(f"\n✅ No significant DoD changes ({dod_label}).")

    # Weekly movers
    wkly_blocks = list(filter(None, [
        _movers_section(v3_pools, "V3 — Weekly", use_dod=False),
        _movers_section(v4_pools, "V4 — Weekly", use_dod=False),
    ]))
    if wkly_blocks:
        parts.append("\n📈 <b>Notable Weekly Changes (&gt;10%, TVL &gt;$100K):</b>\n" + "\n\n".join(wkly_blocks))
    else:
        parts.append(f"\n✅ No significant weekly changes ({weekly_label}).")

    # Top pools
    top_blocks = list(filter(None, [
        _top_section(v3_pools, "V3 — Highlighted Pools"),
        _top_section(v4_pools, "V4 — Highlighted Pools"),
    ]))
    if top_blocks:
        parts.append("\n📊 <b>Highlighted Pools  (DoD | Weekly):</b>\n" + "\n\n".join(top_blocks))

    # Protocol fees
    if proto_fee_total is not None and proto_fee_total > 0:
        parts.append(f"\n💰 <b>Est. V3 Protocol Fees:</b> {_fmt_usd(proto_fee_total)}")

    return "\n".join(parts)


def build_analysis_message(
    snapshot_date: date,
    chain: str,
    ai_analysis: str,
) -> str:
    """Separate analysis message so the full text is never truncated."""
    emoji = CHAIN_EMOJI.get(chain, "⛓")
    label = CHAIN_LABEL.get(chain, chain.upper())
    return (
        f"🤖 <b>Analysis — {label}</b>  {emoji}\n"
        f"📅 {snapshot_date.strftime('%-m/%-d/%Y')}\n\n"
        f"{ai_analysis}"
    )
