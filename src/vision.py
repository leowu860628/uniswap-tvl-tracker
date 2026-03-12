"""
Parse a Uniswap pool screenshot using Claude Vision API.
Returns a list of pool dicts in the same shape as collector.py normalizes.

Uses pipe-delimited text output (not JSON) to avoid all JSON parsing issues.
"""

import base64
import os
from datetime import date
from typing import Optional, List

from anthropic import Anthropic

_client: Optional[Anthropic] = None


def _get_client() -> Anthropic:
    global _client
    if _client is None:
        _client = Anthropic()
    return _client


def _encode_image(image_bytes: bytes, media_type: str = "image/png") -> str:
    return base64.standard_b64encode(image_bytes).decode("utf-8")


# ── Compact number parser ──────────────────────────────────────────────────────

def _parse_usd(s: str) -> float:
    """'$13.1M'→13100000, '$966.6K'→966600, '$88.2'→88.2, '-'→0"""
    s = s.strip().replace(",", "").replace("$", "").upper()
    if not s or s in ("-", "N/A", "NULL", "NONE"):
        return 0.0
    mult = 1.0
    if s.endswith("B"):
        mult = 1_000_000_000; s = s[:-1]
    elif s.endswith("M"):
        mult = 1_000_000; s = s[:-1]
    elif s.endswith("K"):
        mult = 1_000; s = s[:-1]
    try:
        return float(s) * mult
    except ValueError:
        return 0.0


def _parse_nullable_usd(s: str) -> Optional[float]:
    s = s.strip()
    if not s or s in ("-", "N/A", "NULL", "NONE"):
        return None
    return _parse_usd(s)


def _parse_apr(s: str) -> Optional[float]:
    """'22.33%'→0.2233, '-'→None"""
    s = s.strip().replace("%", "")
    if not s or s in ("-", "N/A", "NULL", "NONE"):
        return None
    try:
        return float(s) / 100.0
    except ValueError:
        return None


def _parse_fee_tier(s: str) -> int:
    """'500'→500, '0.05'→500 (already multiplied), '0.05%'→500"""
    s = s.strip().replace("%", "")
    try:
        v = float(s)
        # Values < 100 are percentages that need ×10000; values ≥ 100 are already fee_tier units
        return int(round(v * 10000)) if v < 100 else int(round(v))
    except ValueError:
        return 0


# ── Prompt ─────────────────────────────────────────────────────────────────────

EXTRACT_PROMPT = """You are a data extraction assistant. The user uploaded a screenshot from the Uniswap pool explorer (app.uniswap.org/explore/pools/bnb or /arbitrum).

The table columns (left to right) are:
  #  |  Pool  |  Protocol  |  Fee tier  |  TVL  |  Pool APR  |  Reward APR  |  1D vol  |  30D vol  |  1D vol/TVL

Extract EVERY visible pool row and output them as plain pipe-delimited lines.
Output ONE line per pool, with EXACTLY these 9 fields separated by | (pipe):

token0|token1|version|fee_tier_pct|tvl|vol_24h|apr_pct|vol_30d|chain

Field rules:
- token0: LEFT token symbol (e.g. "BTCB"). Keep Chinese/non-ASCII exactly as shown.
- token1: RIGHT token symbol (e.g. "BNB"). Keep Chinese/non-ASCII exactly as shown.
- version: v3 or v4 (from Protocol column)
- fee_tier_pct: fee percentage as a plain number, NO percent sign (e.g. 0.05, 0.01, 0.3, 1, 0.0001, 0.242, 3, 0.98)
- tvl: TVL as a plain number. Parse compact notation: $13.1M→13100000, $966.6K→966600, $88.2→88.2
- vol_24h: 1D vol as a plain number. Use 0 if "-" or empty.
- apr_pct: Pool APR as a plain number NO percent sign (e.g. 22.33, 470.65). Use - if not shown.
- vol_30d: 30D vol as a plain number. Use 0 if "-" or empty.
- chain: bnb for BNB chain screenshots, arbitrum for Arbitrum screenshots.

CRITICAL:
- Output ONLY the data lines. No header row. No explanation. No markdown. No JSON.
- Do NOT put quotes around values. Do NOT add extra spaces around |.
- If a token symbol contains | (rare), replace it with a dash.
- Numbers only — no $, no %, no K/M/B suffixes in the output."""


# ── Parser ─────────────────────────────────────────────────────────────────────

V3_PROTOCOL_FEE_FRACTION = {
    100:   1 / 4,
    500:   1 / 4,
    3000:  1 / 6,
    10000: 1 / 6,
}


def _parse_pipe_line(line: str, snapshot_date: date) -> Optional[dict]:
    """Parse one pipe-delimited pool line. Returns None if malformed."""
    parts = line.split("|")
    if len(parts) < 8:
        return None

    t0      = parts[0].strip()
    t1      = parts[1].strip()
    version = parts[2].strip().lower()
    if version not in ("v3", "v4"):
        version = "v3"
    chain   = parts[8].strip().lower() if len(parts) > 8 else "bnb"
    if chain not in ("bnb", "arbitrum"):
        chain = "bnb"

    try:
        fee_tier = _parse_fee_tier(parts[3])
        tvl      = _parse_usd(parts[4])
        vol      = _parse_nullable_usd(parts[5]) or 0.0
        apr      = _parse_apr(parts[6])
        fee_rate = fee_tier / 1_000_000
        fees_24h = vol * fee_rate
    except Exception:
        return None

    proto, lp = None, None
    if version == "v3":
        fraction = V3_PROTOCOL_FEE_FRACTION.get(fee_tier)
        if fraction:
            proto = fees_24h * fraction
            lp    = fees_24h - proto

    if not t0 or not t1:
        return None

    return {
        "version":              version,
        "chain":                chain,
        "pool_address":         f"screenshot:{t0}-{t1}-{fee_tier}",
        "token0_symbol":        t0,
        "token1_symbol":        t1,
        "fee_tier":             fee_tier,
        "hooks":                None,
        "tick_spacing":         None,
        "tvl_usd":              tvl,
        "volume_24h_usd":       vol,
        "fees_24h_usd":         fees_24h,
        "tx_count":             None,
        "protocol_fee_est_usd": proto,
        "lp_fee_usd":           lp,
        "apr":                  apr,
        "source":               "screenshot",
    }


# ── Main API ───────────────────────────────────────────────────────────────────

def parse_screenshot(
    image_bytes: bytes,
    media_type: str = "image/png",
    snapshot_date: Optional[date] = None,
) -> List[dict]:
    """
    Send screenshot to Claude Vision. Returns list of normalized pool dicts.
    Uses pipe-delimited text extraction (immune to JSON parsing errors).
    """
    if snapshot_date is None:
        snapshot_date = date.today()

    b64 = _encode_image(image_bytes, media_type)

    resp = _get_client().messages.create(
        model="claude-sonnet-4-6",
        max_tokens=8192,
        messages=[{
            "role": "user",
            "content": [
                {
                    "type": "image",
                    "source": {
                        "type": "base64",
                        "media_type": media_type,
                        "data": b64,
                    },
                },
                {"type": "text", "text": EXTRACT_PROMPT},
            ],
        }],
    )

    raw = resp.content[0].text.strip()
    print(f"[vision] stop_reason={resp.stop_reason}, len={len(raw)}")
    print(f"[vision] first 3 lines:\n" + "\n".join(raw.splitlines()[:3]))

    result = []
    for line in raw.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "|" not in line:
            continue
        pool = _parse_pipe_line(line, snapshot_date)
        if pool:
            result.append(pool)

    print(f"[vision] parsed {len(result)} pools")
    return result
