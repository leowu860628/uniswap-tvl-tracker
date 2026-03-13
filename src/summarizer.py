"""
Generate AI trend summaries using Claude.
"""

from datetime import date
from typing import Optional
from anthropic import Anthropic

_client: Optional[Anthropic] = None


def _get_client() -> Anthropic:
    global _client
    if _client is None:
        _client = Anthropic()
    return _client


def _fmt_pool(r: dict) -> str:
    pair = f"{r['token0_symbol']}/{r['token1_symbol']} {r['fee_tier']/10000:.2g}%"
    dod  = f"{r['dod_tvl_chg']*100:+.1f}%"  if r.get("dod_tvl_chg")  is not None else "N/A"
    wkly = f"{r['wkly_tvl_chg']*100:+.1f}%" if r.get("wkly_tvl_chg") is not None else "N/A"
    vdod = f"{r['dod_vol_chg']*100:+.1f}%"  if r.get("dod_vol_chg")  is not None else "N/A"
    vwk  = f"{r['wkly_vol_chg']*100:+.1f}%" if r.get("wkly_vol_chg") is not None else "N/A"
    return (f"  {pair}: TVL=${r['tvl_usd']:,.0f} "
            f"TVL DoD {dod}/Wk {wkly}  Vol DoD {vdod}/Wk {vwk}")


def _section(pools: list[dict], label: str, mover_flag: str = None) -> str:
    rows = [r for r in pools if r.get(mover_flag)] if mover_flag else pools[:5]
    if not rows:
        return f"{label}: none"
    return f"{label}:\n" + "\n".join(_fmt_pool(r) for r in rows[:5])


def generate_chain_summary(
    chain: str,
    v3_pools: list[dict],
    v4_pools: list[dict],
    dod_date: Optional[date] = None,
    weekly_date: Optional[date] = None,
) -> str:
    """Generate a 2-3 sentence analysis for a single chain, covering both timeframes."""
    chain_label = {"bnb": "BNB Chain", "arbitrum": "Arbitrum"}.get(chain, chain.upper())
    dod_ref  = dod_date.isoformat()    if dod_date    else "previous day"
    wkly_ref = weekly_date.isoformat() if weekly_date else "7 days ago"

    data = "\n".join(filter(None, [
        _section(v3_pools, "V3 DoD movers",    "is_dod_mover"),
        _section(v3_pools, "V3 weekly movers",  "is_wkly_mover"),
        _section(v3_pools, "V3 top by TVL"),
        _section(v4_pools, "V4 DoD movers",    "is_dod_mover"),
        _section(v4_pools, "V4 weekly movers",  "is_wkly_mover"),
        _section(v4_pools, "V4 top by TVL"),
    ]))

    prompt = (
        f"You are an experienced DeFi analyst. Write a thorough analysis of today's "
        f"Uniswap TVL and volume trends on {chain_label}, covering both V3 and V4 pools. "
        f"The day-over-day comparison is vs {dod_ref}; the weekly comparison is vs {wkly_ref}.\n\n"
        f"Your analysis should cover:\n"
        f"1. The most significant pool-level moves (both DoD and weekly), with specific pairs and %s\n"
        f"2. Whether the moves are short-term spikes or sustained weekly trends\n"
        f"3. Any notable V3 vs V4 differences in momentum or liquidity migration\n"
        f"4. Overall market direction and what it might signal\n\n"
        f"Be specific and factual. Do not mention other chains. "
        f"There is no word limit — write as much as is useful.\n\n{data}"
    )

    try:
        resp = _get_client().messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1024,
            messages=[{"role": "user", "content": prompt}],
        )
        return resp.content[0].text.strip()
    except Exception as e:
        print(f"[summarizer] Claude API error (chain summary): {e}")
        return "Unable to generate analysis at this time."


def generate_cross_chain_insight(
    bnb_v3: list[dict],
    bnb_v4: list[dict],
    arb_v3: list[dict],
    arb_v4: list[dict],
) -> Optional[str]:
    """
    Compare BNB and Arbitrum data. Returns a cross-chain insight message if a
    meaningful pattern is detected, or None if nothing notable is found.
    """
    def chain_block(pools_v3, pools_v4, label):
        movers = [r for r in pools_v3 + pools_v4
                  if r.get("is_dod_mover") or r.get("is_wkly_mover")]
        top    = sorted(pools_v3 + pools_v4, key=lambda r: r["tvl_usd"] or 0, reverse=True)[:5]
        rows   = movers[:6] if movers else top[:5]
        return f"{label}:\n" + "\n".join(_fmt_pool(r) for r in rows)

    data = (
        chain_block(bnb_v3, bnb_v4, "BNB Chain") + "\n\n" +
        chain_block(arb_v3, arb_v4, "Arbitrum")
    )

    prompt = (
        "You are an experienced DeFi analyst monitoring Uniswap pools across chains.\n\n"
        "Review today's data from BNB Chain and Arbitrum. "
        "Each pool row shows: TVL, TVL DoD %, TVL weekly %, Vol DoD %, Vol weekly %.\n\n"
        "Identify ONLY genuinely notable cross-chain patterns — for example: "
        "the same token pair moving significantly on both chains (DoD or weekly), "
        "a broad correlated directional shift (both chains TVL/volume up or down together), "
        "V3→V4 migration momentum showing on both chains simultaneously, "
        "or any other signal that would be actionable to a DeFi analyst.\n\n"
        "If you find something worth flagging, respond with 2-4 sentences describing the "
        "pattern and why it matters. Be specific — cite pairs and percentages.\n\n"
        "If there is NO meaningful cross-chain pattern today, respond with exactly: NONE\n\n"
        f"{data}"
    )

    try:
        resp = _get_client().messages.create(
            model="claude-sonnet-4-6",
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}],
        )
        result = resp.content[0].text.strip()
        if result.upper().startswith("NONE"):
            print("[summarizer] No cross-chain pattern detected.")
            return None
        return result
    except Exception as e:
        print(f"[summarizer] Claude API error (cross-chain): {e}")
        return None
