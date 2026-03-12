"""
Generate an AI trend summary using Claude.
"""

import os
from anthropic import Anthropic

_client: Anthropic | None = None


def _get_client() -> Anthropic:
    global _client
    if _client is None:
        _client = Anthropic()
    return _client


def _fmt_movers(movers: list[dict], label: str) -> str:
    if not movers:
        return f"{label}: no significant movers"
    lines = []
    for r in movers[:5]:
        pair = f"{r['token0_symbol']}/{r['token1_symbol']} {r['fee_tier']/10000:.2f}%"
        tvl_chg = f"{r['tvl_change_pct']*100:+.1f}%" if r["tvl_change_pct"] is not None else "N/A"
        vol_chg = f"{r['volume_change_pct']*100:+.1f}%" if r["volume_change_pct"] is not None else "N/A"
        lines.append(f"  {pair}: TVL {tvl_chg}, Vol {vol_chg}, TVL=${r['tvl_usd']:,.0f}")
    return f"{label}:\n" + "\n".join(lines)


def _fmt_top(pools: list[dict], label: str) -> str:
    if not pools:
        return f"{label}: no data"
    lines = []
    for r in pools[:5]:
        pair = f"{r['token0_symbol']}/{r['token1_symbol']} {r['fee_tier']/10000:.2f}%"
        lines.append(f"  {pair}: TVL=${r['tvl_usd']:,.0f}, Vol=${r['volume_24h_usd']:,.0f}")
    return f"{label}:\n" + "\n".join(lines)


def generate_summary(
    bnb_v3_movers: list[dict],
    bnb_v4_movers: list[dict],
    arb_v3_movers: list[dict],
    arb_v4_movers: list[dict],
    top_bnb_v3: list[dict],
    top_bnb_v4: list[dict],
    top_arb_v3: list[dict],
    top_arb_v4: list[dict],
) -> str:
    all_movers = (
        _fmt_movers(bnb_v3_movers, "BNB v3") + "\n" +
        _fmt_movers(bnb_v4_movers, "BNB v4") + "\n" +
        _fmt_movers(arb_v3_movers, "Arbitrum v3") + "\n" +
        _fmt_movers(arb_v4_movers, "Arbitrum v4")
    )
    top_pools = (
        _fmt_top(top_bnb_v3, "BNB v3 top pools") + "\n" +
        _fmt_top(top_bnb_v4, "BNB v4 top pools") + "\n" +
        _fmt_top(top_arb_v3, "Arbitrum v3 top pools") + "\n" +
        _fmt_top(top_arb_v4, "Arbitrum v4 top pools")
    )

    prompt = f"""You are a DeFi analyst. Write a concise 2-3 sentence summary of today's Uniswap TVL and volume trends on BNB Chain and Arbitrum, covering both v3 and v4 pools. Highlight the most notable shifts, any cross-version differences worth noting, and overall market direction. Be factual and specific — cite token pairs and percentages where relevant.

Significant movers (>10% change vs yesterday):
{all_movers}

Top pools by TVL:
{top_pools}"""

    try:
        resp = _get_client().messages.create(
            model="claude-sonnet-4-6",
            max_tokens=256,
            messages=[{"role": "user", "content": prompt}],
        )
        return resp.content[0].text.strip()
    except Exception as e:
        print(f"[summarizer] Claude API error: {e}")
        return "Unable to generate AI summary at this time."
