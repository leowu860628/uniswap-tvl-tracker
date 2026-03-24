"""
Streamlit dashboard for Uniswap TVL tracker.
Run: streamlit run dashboard/app.py
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env")

import secrets as _secrets
import os as _os
from dashboard.auth import (
    init_auth_db, get_auth_url, verify_state, handle_callback, is_whitelisted,
    log_access, get_whitelist, add_to_whitelist, remove_from_whitelist, get_access_log,
    _seed_from_bundle,
)
init_auth_db()

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import sqlite3
from datetime import date, timedelta

from src.analyzer import (
    get_changes, get_snapshots, get_protocol_fee_totals, get_available_dates
)
from src.collector import collect_all, DB_PATH, init_db
init_db()
_seed_from_bundle()
import importlib as _importlib
import src.csv_import as _csv_import_mod
_importlib.reload(_csv_import_mod)
from src.csv_import import import_csv, generate_template

st.set_page_config(page_title="Uniswap TVL Tracker", layout="wide", page_icon="🦄")

# ── Auth gate ─────────────────────────────────────────────────────────────────

def _render_login_page():
    st.title("🦄 Uniswap TVL Tracker")
    st.markdown("Sign in with your Google account to continue.")
    st.link_button("Sign in with Google", get_auth_url(), type="primary")
    st.stop()

def _render_denied_page(user: dict):
    st.title("Access Denied")
    st.error(f"**{user['email']}** is not authorized. Contact the dashboard owner to request access.")
    if st.button("Sign out / Try another account"):
        for k in ["auth_user", "auth_allowed"]:
            st.session_state.pop(k, None)
        st.query_params.clear()
        st.rerun()
    st.stop()

if _os.environ.get("AUTH_ENABLED", "true").lower() != "false":
    _query = st.query_params
    if "code" in _query and "auth_user" not in st.session_state:
        if not verify_state(_query.get("state", "")):
            st.error("Invalid or expired login link — please try again.")
            st.query_params.clear()
            st.stop()
        try:
            _user = handle_callback(_query["code"])
        except Exception as _e:
            st.error(f"Authentication failed: {_e}")
            st.query_params.clear()
            st.stop()
        st.query_params.clear()
        _allowed = is_whitelisted(_user["email"])
        log_access(_user["email"], _user.get("name", ""), _allowed, _user.get("picture", ""))
        st.session_state.auth_user = _user
        st.session_state.auth_allowed = _allowed
        st.rerun()

    if "auth_user" not in st.session_state:
        _render_login_page()
    if not st.session_state.get("auth_allowed", False):
        _render_denied_page(st.session_state["auth_user"])
else:
    st.session_state.auth_user = {"email": "local", "name": "Local User"}

# ── Uniswap-inspired CSS ──────────────────────────────────────────────────────

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
html, body, [class*="css"] { font-family: 'Inter', sans-serif !important; }
.stApp { background-color: #0A0B0D; }
[data-testid="metric-container"] {
    background: #131720; border: 1px solid #2D2F3E;
    border-radius: 16px; padding: 16px 20px;
}
[data-testid="stMetricLabel"] p {
    color: #9B9B9B !important; font-size: 11px !important;
    text-transform: uppercase; letter-spacing: 0.08em;
}
[data-testid="stMetricValue"] {
    color: #FFFFFF !important; font-size: 22px !important; font-weight: 700 !important;
}
[data-testid="stSidebar"] { background: #0D0F14 !important; border-right: 1px solid #2D2F3E; }
.stTabs [data-baseweb="tab-list"] {
    background: transparent; border-bottom: 1px solid #2D2F3E; gap: 4px;
}
.stTabs [data-baseweb="tab"] {
    color: #9B9B9B; font-weight: 500; font-size: 13px; padding: 8px 14px;
}
.stTabs [aria-selected="true"] {
    color: #FC72FF !important; border-bottom: 2px solid #FC72FF !important;
    background: transparent;
}
.stButton > button {
    background: #1B1E29; color: #FFFFFF; border: 1px solid #2D2F3E;
    border-radius: 12px; font-weight: 600; font-size: 13px;
    padding: 6px 18px; transition: border-color 0.2s, color 0.2s;
}
.stButton > button:hover { border-color: #FC72FF; color: #FC72FF; }
.stButton > button[kind="primary"] {
    background: linear-gradient(135deg, #FC72FF 0%, #7B3FE4 100%);
    border: none; color: #000 !important; font-weight: 700;
}
.stButton > button[kind="primary"]:hover { opacity: 0.9; color: #000 !important; }
hr { border: none; border-top: 1px solid #2D2F3E; margin: 16px 0; }
h1 {
    font-size: 26px !important; font-weight: 700 !important;
    background: linear-gradient(135deg, #FC72FF, #7B3FE4);
    -webkit-background-clip: text; -webkit-text-fill-color: transparent;
}
[data-testid="stExpander"] { border: 1px solid #2D2F3E; border-radius: 12px; }
</style>
""", unsafe_allow_html=True)

# ── Color palette ─────────────────────────────────────────────────────────────

CHAIN_VER_COLORS = {
    ("bnb",      "v3"): "#FC72FF",
    ("bnb",      "v4"): "#9C4DCC",
    ("arbitrum", "v3"): "#28A0F0",
    ("arbitrum", "v4"): "#21C95E",
    ("base",     "v3"): "#0052FF",
    ("base",     "v4"): "#0033CC",
    ("monad",    "v3"): "#836EF9",
    ("monad",    "v4"): "#6B4FD8",
}

# Base Plotly layout (no xaxis/yaxis — pass those per-chart to avoid keyword conflicts)
PLOTLY_BASE = dict(
    paper_bgcolor="#0A0B0D",
    plot_bgcolor="#0A0B0D",
    font=dict(color="#9B9B9B", family="Inter, sans-serif", size=12),
    legend=dict(bgcolor="rgba(0,0,0,0)", bordercolor="#2D2F3E"),
    margin=dict(t=40, b=10, l=10, r=10),
)
AXIS = dict(gridcolor="#1E2030", linecolor="#2D2F3E", tickcolor="#2D2F3E")


def _layout(**extra):
    """Merge PLOTLY_BASE with per-chart overrides."""
    return dict(**PLOTLY_BASE, **extra)


# ── Sidebar ───────────────────────────────────────────────────────────────────

st.sidebar.title("🦄 Uniswap TVL")
st.sidebar.caption(f"Signed in as {st.session_state['auth_user']['email']}")
if st.sidebar.button("Sign Out"):
    for _k in ["auth_user", "auth_allowed"]:
        st.session_state.pop(_k, None)
    st.rerun()
st.sidebar.divider()
chain_opt     = st.sidebar.selectbox("Chain",            ["Both", "BNB", "Arbitrum", "Base", "Monad"])
version_opt   = st.sidebar.selectbox("Protocol Version", ["Both", "V3", "V4"])
timeframe_opt = st.sidebar.selectbox("Timeframe",        ["Day-over-day", "Weekly", "Biweekly"])
threshold     = st.sidebar.slider("Significance Threshold (%)", 5, 50, 10) / 100

available_dates = get_available_dates()
if available_dates:
    _jump = st.session_state.pop("_jump_date", None)
    _default_idx = available_dates.index(_jump) if _jump and _jump in available_dates else 0
    selected_date_str = st.sidebar.selectbox("Snapshot Date", available_dates, index=_default_idx)
    selected_date = date.fromisoformat(selected_date_str)
else:
    selected_date = date.today()

st.sidebar.divider()
if st.sidebar.button("Collect Data Now"):
    with st.spinner("Fetching from GeckoTerminal..."):
        n = collect_all()
    st.sidebar.success(f"Collected {n} pool snapshots.")
    st.rerun()

# ── Helpers ───────────────────────────────────────────────────────────────────

TIMEFRAME_DAYS = {"Day-over-day": 1, "Weekly": 7, "Biweekly": 14}
days_back      = TIMEFRAME_DAYS[timeframe_opt]
chain_filter   = None if chain_opt   == "Both" else chain_opt.lower()
version_filter = None if version_opt == "Both" else version_opt.lower()


def fmt_usd(val) -> str:
    if val is None:
        return ""
    val = float(val)
    if abs(val) >= 1_000_000:
        return f"${val/1_000_000:.2f}M"
    if abs(val) >= 1_000:
        return f"${val/1_000:.2f}K"
    return f"${val:.2f}"


def fmt_pct(val) -> str:
    if val is None:
        return "N/A"
    sign = "+" if val >= 0 else ""
    return f"{sign}{val*100:.1f}%"


def _fmt_fee(fee_tier, version="") -> str:
    if fee_tier is not None:
        return f"{fee_tier/10000:.4g}%"
    return "N/A"


def pool_label(r) -> str:
    fee = r.get("fee_tier", 0) or 0
    return (f"{r.get('token0_symbol','?')}/{r.get('token1_symbol','?')} "
            f"{_fmt_fee(fee, r.get('version',''))} [{r.get('chain','').upper()} {r.get('version','').upper()}]")


def highlight_pct(val) -> str:
    """Highlight a numeric percentage value (e.g. 5.2 means 5.2%)."""
    try:
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return ""
        num = float(val)
        if num >= threshold * 100:
            return "background-color: #0E2A1A; color: #4FC86F"
        if num <= -threshold * 100:
            return "background-color: #2A0E0E; color: #FF4343"
    except Exception:
        pass
    return ""


def _pct100(v):
    """Convert a 0-1 fraction to a rounded percentage float, or None."""
    return round(v * 100, 2) if v is not None else None


_PCT  = dict(format="%.2f%%")
_MUSD = dict(format="$%.2fM")   # values stored in millions  (tvl, volume)
_KUSD = dict(format="$%.2fK")   # values stored in thousands (fees)


def _to_m(v):
    return v / 1_000_000 if v is not None else None


def _to_k(v):
    return v / 1_000 if v is not None else None


def soft_delete_entry(entry_id: int):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("UPDATE pool_snapshots SET is_deleted = 1 WHERE id = ?", (entry_id,))
    conn.commit()
    conn.close()


def restore_entry(entry_id: int):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("UPDATE pool_snapshots SET is_deleted = 0 WHERE id = ?", (entry_id,))
    conn.commit()
    conn.close()


def get_deleted_entries(snapshot_date: date, chain: str = None, version: str = None) -> list:
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        wheres = ["snapshot_date = ?", "is_deleted = 1"]
        params = [snapshot_date.isoformat()]
        if chain:
            wheres.append("chain = ?"); params.append(chain)
        if version:
            wheres.append("version = ?"); params.append(version)
        rows = conn.execute(
            f"SELECT * FROM pool_snapshots WHERE {' AND '.join(wheres)} ORDER BY tvl_usd DESC",
            params,
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception:
        return []


def find_duplicates_for_date(snapshot_date: date) -> list:
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        rows = conn.execute("""
            SELECT token0_symbol, token1_symbol, fee_tier, chain, version,
                   COUNT(*) as cnt, GROUP_CONCAT(id) as ids,
                   GROUP_CONCAT(source) as sources
            FROM pool_snapshots
            WHERE snapshot_date = ? AND (is_deleted IS NULL OR is_deleted = 0)
            GROUP BY UPPER(token0_symbol), UPPER(token1_symbol), fee_tier, chain, version
            HAVING COUNT(*) > 1
        """, (snapshot_date.isoformat(),)).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception:
        return []


def deduplicate_date(snapshot_date: date) -> int:
    """Soft-delete lower-TVL duplicates, always keeping one entry per group."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    dups = find_duplicates_for_date(snapshot_date)
    hidden = 0
    for dup in dups:
        ids = [int(i) for i in dup["ids"].split(",")]
        rows = conn.execute(
            f"SELECT id, tvl_usd FROM pool_snapshots WHERE id IN ({','.join('?'*len(ids))})",
            ids,
        ).fetchall()
        keep_id = max(rows, key=lambda r: r["tvl_usd"] or 0)["id"]
        for r in rows:
            if r["id"] != keep_id:
                conn.execute("UPDATE pool_snapshots SET is_deleted = 1 WHERE id = ?", (r["id"],))
                hidden += 1
    conn.commit()
    conn.close()
    return hidden


# ── No data guard ─────────────────────────────────────────────────────────────

if not available_dates:
    st.title("Uniswap TVL Tracker")
    st.warning("No data yet. Click **Collect Data Now** in the sidebar.")
    st.stop()

# ── Load data ─────────────────────────────────────────────────────────────────

all_changes = get_changes(chain_filter, version_filter, selected_date, days_back)
v3_changes  = [r for r in all_changes if r["version"] == "v3"]
v4_changes  = [r for r in all_changes if r["version"] == "v4"]

movers = [r for r in all_changes if
          (r["tvl_change_pct"]    is not None and abs(r["tvl_change_pct"])    >= threshold) or
          (r["volume_change_pct"] is not None and abs(r["volume_change_pct"]) >= threshold)]
movers.sort(key=lambda r: abs(r.get("tvl_change_pct") or 0), reverse=True)

proto_fees = get_protocol_fee_totals(selected_date)


def agg_chain_ver(rows):
    out = {}
    for r in rows:
        key = (r["chain"], r["version"])
        if key not in out:
            out[key] = {"tvl": 0, "vol": 0}
        out[key]["tvl"] += r["tvl_usd"] or 0
        out[key]["vol"] += r["volume_24h_usd"] or 0
    return out


today_agg = agg_chain_ver(all_changes)
prev_rows = get_snapshots(selected_date - timedelta(days=days_back), chain_filter, version_filter)
prev_agg  = agg_chain_ver(prev_rows)


def agg_pct(key, metric):
    t = today_agg.get(key, {}).get(metric, 0)
    p = prev_agg.get(key, {}).get(metric, 0)
    if not p:
        return None
    return (t - p) / p


# ── KPI row ───────────────────────────────────────────────────────────────────

st.title("Uniswap TVL Tracker")
st.caption(f"Snapshot: **{selected_date}** · Timeframe: **{timeframe_opt}** · Threshold: **{threshold*100:.0f}%**")

tvl_v3      = sum(r["tvl_usd"] or 0 for r in v3_changes)
tvl_v4      = sum(r["tvl_usd"] or 0 for r in v4_changes)
vol_v3      = sum(r["volume_24h_usd"] or 0 for r in v3_changes)
vol_v4      = sum(r["volume_24h_usd"] or 0 for r in v4_changes)
total_proto = sum(proto_fees.values())

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("V3 TVL",                  fmt_usd(tvl_v3),      help="Total value locked across all V3 pools")
c2.metric("V4 TVL",                  fmt_usd(tvl_v4),      help="Total value locked across all V4 pools")
c3.metric("V3 24h Volume",           fmt_usd(vol_v3))
c4.metric("V4 24h Volume",           fmt_usd(vol_v4))
c5.metric("Protocol Fees (V3 est.)", fmt_usd(total_proto), help="V4 protocol fees not estimable (governance-controlled)")

st.divider()

# ── Cross-chain overview charts ───────────────────────────────────────────────

CHAIN_VER_ORDER = [
    ("bnb",      "v3", "BNB V3"),
    ("bnb",      "v4", "BNB V4"),
    ("arbitrum", "v3", "ARB V3"),
    ("arbitrum", "v4", "ARB V4"),
    ("base",     "v3", "BASE V3"),
    ("base",     "v4", "BASE V4"),
    ("monad",    "v3", "MON V3"),
    ("monad",    "v4", "MON V4"),
]

visible = [
    (ch, ver, lbl) for ch, ver, lbl in CHAIN_VER_ORDER
    if (not chain_filter or ch == chain_filter) and (not version_filter or ver == version_filter)
]

chart_labels = [lbl for _, _, lbl in visible]
tvl_vals     = [today_agg.get((ch, ver), {}).get("tvl", 0) for ch, ver, _ in visible]
vol_vals     = [today_agg.get((ch, ver), {}).get("vol", 0) for ch, ver, _ in visible]
tvl_pcts     = [agg_pct((ch, ver), "tvl") for ch, ver, _ in visible]
vol_pcts     = [agg_pct((ch, ver), "vol") for ch, ver, _ in visible]
bar_colors   = [CHAIN_VER_COLORS[(ch, ver)] for ch, ver, _ in visible]


def bar_text(val, pct):
    base = fmt_usd(val)
    if pct is None:
        return base
    sign = "+" if pct >= 0 else ""
    return f"{base}\n{sign}{pct*100:.1f}%"


if any(v > 0 for v in tvl_vals + vol_vals):
    ov1, ov2 = st.columns(2)

    with ov1:
        tvl_max = max(tvl_vals) if tvl_vals else 1
        fig_tvl = go.Figure(go.Bar(
            x=chart_labels, y=tvl_vals,
            marker_color=bar_colors,
            text=[bar_text(v, p) for v, p in zip(tvl_vals, tvl_pcts)],
            textposition="outside",
            hovertemplate="%{x}: %{y:$,.0f}<extra></extra>",
        ))
        fig_tvl.update_layout(
            **_layout(
                title=dict(text="TVL by Chain & Protocol", font=dict(color="#FFFFFF", size=14)),
                height=320, showlegend=False,
                xaxis=AXIS,
                yaxis=dict(**AXIS, tickprefix="$", range=[0, tvl_max * 1.25]),
            )
        )
        st.plotly_chart(fig_tvl, use_container_width=True)

    with ov2:
        vol_max = max(vol_vals) if vol_vals else 1
        fig_vol = go.Figure(go.Bar(
            x=chart_labels, y=vol_vals,
            marker_color=bar_colors,
            text=[bar_text(v, p) for v, p in zip(vol_vals, vol_pcts)],
            textposition="outside",
            hovertemplate="%{x}: %{y:$,.0f}<extra></extra>",
        ))
        fig_vol.update_layout(
            **_layout(
                title=dict(text="24h Volume by Chain & Protocol", font=dict(color="#FFFFFF", size=14)),
                height=320, showlegend=False,
                xaxis=AXIS,
                yaxis=dict(**AXIS, tickprefix="$", range=[0, vol_max * 1.25]),
            )
        )
        st.plotly_chart(fig_vol, use_container_width=True)

    legend_html = " &nbsp; ".join(
        f'<span style="display:inline-block;width:10px;height:10px;border-radius:2px;'
        f'background:{CHAIN_VER_COLORS[(ch,ver)]};margin-right:4px"></span>'
        f'<span style="color:#9B9B9B;font-size:12px">{lbl}</span>'
        for ch, ver, lbl in visible
    )
    st.markdown(legend_html, unsafe_allow_html=True)

st.divider()

# ── Tabs ──────────────────────────────────────────────────────────────────────

tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8 = st.tabs([
    "Overview", "Significant Movers", "Pool Detail", "Timeframe Comparison",
    "Upload Screenshot", "Import CSV", "Manage Data", "Access Control",
])

# ── Tab 1: Overview ───────────────────────────────────────────────────────────

with tab1:
    def render_pool_table(rows: list, version: str):
        if not rows:
            st.info(f"No {version.upper()} data for selected filters.")
            return
        df = pd.DataFrame(rows)
        df["Pool"]        = df.apply(pool_label, axis=1)
        df["tvl_m"]       = df["tvl_usd"].apply(_to_m)
        df["vol_m"]       = df["volume_24h_usd"].apply(_to_m)
        df["fees_k"]      = df["fees_24h_usd"].apply(_to_k)
        df["tvl_chg_pct"] = df["tvl_change_pct"].apply(_pct100)
        df["vol_chg_pct"] = df["volume_change_pct"].apply(_pct100)

        base_cfg = {
            "tvl_m":       st.column_config.NumberColumn("TVL",        **_MUSD),
            "tvl_chg_pct": st.column_config.NumberColumn("TVL Chg %",  **_PCT),
            "vol_m":       st.column_config.NumberColumn("24h Volume", **_MUSD),
            "vol_chg_pct": st.column_config.NumberColumn("Vol Chg %",  **_PCT),
            "fees_k":      st.column_config.NumberColumn("24h Fees",   **_KUSD),
        }

        if version == "v3":
            df["proto_k"] = df["protocol_fee_est_usd"].apply(_to_k)
            df["lp_k"]    = df["lp_fee_usd"].apply(_to_k)
            col_cfg = {
                **base_cfg,
                "proto_k": st.column_config.NumberColumn("Protocol Fee", **_KUSD),
                "lp_k":    st.column_config.NumberColumn("LP Fee",       **_KUSD),
            }
            cols = ["Pool", "tvl_m", "tvl_chg_pct", "vol_m", "vol_chg_pct",
                    "fees_k", "proto_k", "lp_k"]
        else:
            df["Hooks"] = df["hooks"].apply(
                lambda h: h[:10] + "…" if h and h != "0x0000000000000000000000000000000000000000" else "None"
            )
            col_cfg = {**base_cfg, "Hooks": st.column_config.TextColumn("Hooks")}
            cols = ["Pool", "tvl_m", "tvl_chg_pct", "vol_m", "vol_chg_pct", "fees_k", "Hooks"]

        styled = df[cols].style.map(highlight_pct, subset=["tvl_chg_pct", "vol_chg_pct"])
        st.dataframe(styled, column_config=col_cfg, use_container_width=True, hide_index=True)

    if version_filter is None:
        with st.expander(f"V3 Pools ({len(v3_changes)} pools)", expanded=True):
            render_pool_table(v3_changes, "v3")
        with st.expander(f"V4 Pools ({len(v4_changes)} pools)", expanded=True):
            render_pool_table(v4_changes, "v4")
    else:
        with st.expander(f"{version_filter.upper()} Pools ({len(all_changes)} pools)", expanded=True):
            render_pool_table(all_changes, version_filter)

# ── Tab 2: Significant Movers ─────────────────────────────────────────────────

with tab2:
    st.subheader(f"Significant Movers  ·  >{threshold*100:.0f}% change  ·  {timeframe_opt}")

    if not movers:
        st.success("No pools exceeded the significance threshold for this period.")
    else:
        df_m = pd.DataFrame(movers)
        df_m["Pool"]     = df_m.apply(pool_label, axis=1)
        df_m["_tvl_pct"] = df_m["tvl_change_pct"].fillna(0) * 100
        df_m["_vol_pct"] = df_m["volume_change_pct"].fillna(0) * 100
        df_m["_tvl_clr"] = df_m["_tvl_pct"].apply(lambda v: "#4FC86F" if v >= 0 else "#FF4343")
        df_m["_vol_clr"] = df_m["_vol_pct"].apply(lambda v: "#28A0F0" if v >= 0 else "#FF8800")
        df_m["_tvl_txt"] = df_m["_tvl_pct"].apply(lambda v: f"{'+' if v>=0 else ''}{v:.1f}%")
        df_m["_vol_txt"] = df_m["_vol_pct"].apply(lambda v: f"{'+' if v>=0 else ''}{v:.1f}%")

        col_tvl, col_vol = st.columns(2)
        with col_tvl:
            tvl_pct_max = df_m["_tvl_pct"].abs().max() or 1
            tvl_pct_top = df_m["_tvl_pct"].max()
            tvl_pct_bot = df_m["_tvl_pct"].min()
            fig = go.Figure(go.Bar(
                x=df_m["Pool"], y=df_m["_tvl_pct"],
                marker_color=df_m["_tvl_clr"],
                text=df_m["_tvl_txt"], textposition="outside",
                hovertemplate="%{x}: %{y:+.1f}%<extra></extra>",
            ))
            fig.update_layout(
                **_layout(
                    title=dict(text="TVL % Change", font=dict(color="#FFFFFF", size=13)),
                    xaxis=dict(**AXIS, tickangle=-35),
                    yaxis=dict(**AXIS, ticksuffix="%",
                               range=[min(0, tvl_pct_bot * 1.25), max(0, tvl_pct_top * 1.25)]),
                    height=360,
                )
            )
            st.plotly_chart(fig, use_container_width=True)

        with col_vol:
            vol_pct_top = df_m["_vol_pct"].max()
            vol_pct_bot = df_m["_vol_pct"].min()
            fig2 = go.Figure(go.Bar(
                x=df_m["Pool"], y=df_m["_vol_pct"],
                marker_color=df_m["_vol_clr"],
                text=df_m["_vol_txt"], textposition="outside",
                hovertemplate="%{x}: %{y:+.1f}%<extra></extra>",
            ))
            fig2.update_layout(
                **_layout(
                    title=dict(text="24h Volume % Change", font=dict(color="#FFFFFF", size=13)),
                    xaxis=dict(**AXIS, tickangle=-35),
                    yaxis=dict(**AXIS, ticksuffix="%",
                               range=[min(0, vol_pct_bot * 1.25), max(0, vol_pct_top * 1.25)]),
                    height=360,
                )
            )
            st.plotly_chart(fig2, use_container_width=True)

        df_m["tvl_prev_m"]   = df_m["tvl_prev"].apply(_to_m)
        df_m["tvl_m"]        = df_m["tvl_usd"].apply(_to_m)
        df_m["tvl_chg_pct"]  = df_m["tvl_change_pct"].apply(_pct100)
        df_m["vol_chg_pct"]  = df_m["volume_change_pct"].apply(_pct100)
        df_m["proto_k"]      = df_m.apply(
            lambda r: _to_k(r["protocol_fee_est_usd"]) if r["version"] == "v3" else None, axis=1
        )
        mover_cfg = {
            "tvl_prev_m":  st.column_config.NumberColumn("TVL Prev",         **_MUSD),
            "tvl_m":       st.column_config.NumberColumn("TVL",              **_MUSD),
            "tvl_chg_pct": st.column_config.NumberColumn("TVL Chg %",        **_PCT),
            "vol_chg_pct": st.column_config.NumberColumn("Vol Chg %",        **_PCT),
            "proto_k":     st.column_config.NumberColumn("Protocol Fee Est.", **_KUSD),
        }
        styled_m = df_m[["Pool", "tvl_prev_m", "tvl_m", "tvl_chg_pct", "vol_chg_pct", "proto_k"]].style \
            .map(highlight_pct, subset=["tvl_chg_pct", "vol_chg_pct"])
        st.dataframe(styled_m, column_config=mover_cfg, use_container_width=True, hide_index=True)

# ── Tab 3: Pool Detail ────────────────────────────────────────────────────────

with tab3:
    st.subheader("Pool History")
    all_today = get_snapshots(selected_date, chain_filter, version_filter)
    if not all_today:
        st.info("No pools available for selected filters.")
    else:
        pool_options = {
            pool_label(r): r["pool_address"] + "|" + r["chain"] + "|" + r["version"]
            for r in all_today
        }
        chosen_label = st.selectbox("Select Pool", list(pool_options.keys()))
        addr, ch, ver = pool_options[chosen_label].split("|")
        color = CHAIN_VER_COLORS.get((ch, ver), "#FC72FF")

        try:
            conn = sqlite3.connect(DB_PATH)
            conn.row_factory = sqlite3.Row
            hist = conn.execute("""
                SELECT snapshot_date, tvl_usd, volume_24h_usd, fees_24h_usd,
                       protocol_fee_est_usd, lp_fee_usd, apr
                FROM pool_snapshots
                WHERE pool_address = ? AND chain = ? AND version = ?
                ORDER BY snapshot_date ASC
            """, (addr, ch, ver)).fetchall()
            conn.close()
        except Exception:
            hist = []

        if not hist:
            st.info("No history available for this pool.")
        elif len(hist) < 2:
            st.info("Only 1 data point so far — check back after more daily collections.")
            row = dict(hist[0])
            mc1, mc2, mc3 = st.columns(3)
            mc1.metric("TVL",        fmt_usd(row["tvl_usd"]))
            mc2.metric("24h Volume", fmt_usd(row["volume_24h_usd"]))
            mc3.metric("24h Fees",   fmt_usd(row["fees_24h_usd"]))
        else:
            df_h = pd.DataFrame([dict(r) for r in hist])
            df_h["snapshot_date"] = pd.to_datetime(df_h["snapshot_date"])

            r, g, b = int(color[1:3], 16), int(color[3:5], 16), int(color[5:7], 16)
            fig = go.Figure(go.Scatter(
                x=df_h["snapshot_date"], y=df_h["tvl_usd"],
                name="TVL", line=dict(color=color, width=2),
                fill="tozeroy", fillcolor=f"rgba({r},{g},{b},0.09)",
            ))
            fig.update_layout(
                **_layout(
                    title=dict(text="TVL Over Time", font=dict(color="#FFFFFF", size=14)),
                    xaxis=AXIS, yaxis=dict(**AXIS, tickprefix="$"), height=280,
                )
            )
            st.plotly_chart(fig, use_container_width=True)

            col_vol, col_fee = st.columns(2)
            with col_vol:
                fig2 = go.Figure(go.Bar(
                    x=df_h["snapshot_date"], y=df_h["volume_24h_usd"],
                    marker_color=color, name="24h Volume",
                ))
                fig2.update_layout(
                    **_layout(
                        title=dict(text="24h Volume", font=dict(color="#FFFFFF", size=13)),
                        xaxis=AXIS, yaxis=dict(**AXIS, tickprefix="$"), height=240,
                    )
                )
                st.plotly_chart(fig2, use_container_width=True)

            with col_fee:
                if ver == "v3":
                    fig3 = go.Figure()
                    fig3.add_trace(go.Bar(
                        x=df_h["snapshot_date"], y=df_h["lp_fee_usd"],
                        name="LP Fees", marker_color="#4FC86F",
                    ))
                    fig3.add_trace(go.Bar(
                        x=df_h["snapshot_date"], y=df_h["protocol_fee_est_usd"],
                        name="Protocol Fee Est.", marker_color="#FF9900",
                    ))
                    fig3.update_layout(
                        **_layout(
                            title=dict(text="Fee Breakdown (V3)", font=dict(color="#FFFFFF", size=13)),
                            barmode="stack", xaxis=AXIS,
                            yaxis=dict(**AXIS, tickprefix="$"), height=240,
                        )
                    )
                    st.plotly_chart(fig3, use_container_width=True)
                else:
                    fig3 = go.Figure(go.Bar(
                        x=df_h["snapshot_date"], y=df_h["fees_24h_usd"],
                        marker_color="#FF9900", name="24h Fees",
                    ))
                    fig3.update_layout(
                        **_layout(
                            title=dict(text="24h Fees (V4 — total, no protocol split)",
                                       font=dict(color="#FFFFFF", size=13)),
                            xaxis=AXIS, yaxis=dict(**AXIS, tickprefix="$"), height=240,
                        )
                    )
                    st.plotly_chart(fig3, use_container_width=True)

            if df_h["apr"].notna().any():
                fig4 = go.Figure(go.Scatter(
                    x=df_h["snapshot_date"], y=df_h["apr"] * 100,
                    name="APR %", line=dict(color="#FC72FF", width=2),
                ))
                fig4.update_layout(
                    **_layout(
                        title=dict(text="APR Over Time", font=dict(color="#FFFFFF", size=14)),
                        xaxis=AXIS, yaxis=dict(**AXIS, ticksuffix="%"), height=220,
                    )
                )
                st.plotly_chart(fig4, use_container_width=True)

# ── Tab 4: Timeframe Comparison ───────────────────────────────────────────────

with tab4:
    st.subheader("Timeframe Comparison  ·  D / W / 2W")
    today_rows = get_snapshots(selected_date, chain_filter, version_filter)
    if not today_rows:
        st.info("No data for selected filters.")
    else:
        key_map = {(r["chain"], r["version"], r["pool_address"]): r for r in today_rows}

        def chg_map(days):
            rows = get_changes(chain_filter, version_filter, selected_date, days)
            return {(r["chain"], r["version"], r["pool_address"]): r for r in rows}

        d1, d7, d14 = chg_map(1), chg_map(7), chg_map(14)

        rows_out = []
        for key, r in key_map.items():
            rows_out.append({
                "Pool":       pool_label(r),
                "Version":    r["version"].upper(),
                "tvl_m":      _to_m(r["tvl_usd"]),
                "d_tvl_chg":  _pct100(d1.get(key,  {}).get("tvl_change_pct")),
                "d_vol_chg":  _pct100(d1.get(key,  {}).get("volume_change_pct")),
                "w_tvl_chg":  _pct100(d7.get(key,  {}).get("tvl_change_pct")),
                "w_vol_chg":  _pct100(d7.get(key,  {}).get("volume_change_pct")),
                "2w_tvl_chg": _pct100(d14.get(key, {}).get("tvl_change_pct")),
                "2w_vol_chg": _pct100(d14.get(key, {}).get("volume_change_pct")),
            })

        df_tf = pd.DataFrame(rows_out)
        chg_cols = ["d_tvl_chg", "d_vol_chg", "w_tvl_chg", "w_vol_chg", "2w_tvl_chg", "2w_vol_chg"]
        tf_cfg = {
            "tvl_m":      st.column_config.NumberColumn("TVL",          **_MUSD),
            "d_tvl_chg":  st.column_config.NumberColumn("D TVL Chg %",  **_PCT),
            "d_vol_chg":  st.column_config.NumberColumn("D Vol Chg %",  **_PCT),
            "w_tvl_chg":  st.column_config.NumberColumn("W TVL Chg %",  **_PCT),
            "w_vol_chg":  st.column_config.NumberColumn("W Vol Chg %",  **_PCT),
            "2w_tvl_chg": st.column_config.NumberColumn("2W TVL Chg %", **_PCT),
            "2w_vol_chg": st.column_config.NumberColumn("2W Vol Chg %", **_PCT),
        }
        st.dataframe(
            df_tf[["Pool", "Version", "tvl_m"] + chg_cols].style.map(highlight_pct, subset=chg_cols),
            column_config=tf_cfg, use_container_width=True, hide_index=True,
        )

# ── Tab 5: Upload Screenshot ──────────────────────────────────────────────────

with tab5:
    st.subheader("Upload Uniswap Pool Screenshot")
    st.caption(
        "Drop a screenshot from app.uniswap.org/explore/pools/bnb (or /arbitrum, /base, /monad). "
        "Claude Vision will extract pool data automatically."
    )

    if "ss_pools" not in st.session_state:
        st.session_state.ss_pools = []
    if "ss_saved" not in st.session_state:
        st.session_state.ss_saved = False

    ss_date    = st.date_input("Snapshot date", value=date.today(), key="ss_date")
    ss_chain   = st.selectbox("Chain",   ["BNB", "Arbitrum", "Base", "Monad", "Auto-detect"], key="ss_chain")
    ss_version = st.selectbox("Version", ["Auto-detect", "V3", "V4"],        key="ss_version")

    uploaded = st.file_uploader(
        "Drop screenshot here (PNG, JPG, WebP)",
        type=["png", "jpg", "jpeg", "webp"],
        key="ss_upload",
    )

    if uploaded:
        if st.button("Extract Pool Data with Claude Vision"):
            import importlib, src.vision as _vision_mod
            importlib.reload(_vision_mod)
            parse_screenshot = _vision_mod.parse_screenshot
            ext  = uploaded.name.rsplit(".", 1)[-1].lower()
            mime = {"jpg": "image/jpeg", "jpeg": "image/jpeg",
                    "png": "image/png",  "webp": "image/webp"}.get(ext, "image/png")
            with st.spinner("Analysing screenshot..."):
                try:
                    pools = parse_screenshot(uploaded.read(), media_type=mime, snapshot_date=ss_date)
                    for p in pools:
                        if ss_chain != "Auto-detect":
                            p["chain"] = ss_chain.lower()
                        if ss_version != "Auto-detect":
                            p["version"] = ss_version.lower()
                    st.session_state.ss_pools = pools
                    st.session_state.ss_saved = False
                except Exception as e:
                    st.error(f"Extraction failed: {e}")
                    st.session_state.ss_pools = []

    if st.session_state.ss_pools and not st.session_state.ss_saved:
        pools = st.session_state.ss_pools
        st.success(f"Extracted {len(pools)} pools. Review before saving:")

        preview_df = pd.DataFrame([{
            "Pool":              f"{p['token0_symbol']}/{p['token1_symbol']} {_fmt_fee(p.get('fee_tier') or 0, p.get('version', ''))}",
            "Chain / Ver":       f"{p['chain'].upper()} {p['version'].upper()}",
            "TVL":               fmt_usd(p["tvl_usd"]),
            "1D Volume":         fmt_usd(p["volume_24h_usd"]),
            "Pool APR":          fmt_pct(p.get("apr")),
            "Protocol Fee Est.": fmt_usd(p["protocol_fee_est_usd"]),
        } for p in pools])
        st.dataframe(preview_df, use_container_width=True, hide_index=True)

        col_save, col_discard = st.columns(2)
        with col_save:
            if st.button("Save to Database", type="primary"):
                init_db()
                conn = sqlite3.connect(DB_PATH)
                for p in pools:
                    conn.execute("""
                        INSERT INTO pool_snapshots
                            (snapshot_date, chain, version, pool_address,
                             token0_symbol, token1_symbol, fee_tier, hooks, tick_spacing,
                             tvl_usd, volume_24h_usd, fees_24h_usd, tx_count,
                             protocol_fee_est_usd, lp_fee_usd, apr, source)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'screenshot')
                        ON CONFLICT(snapshot_date, chain, version, pool_address) DO UPDATE SET
                            tvl_usd=excluded.tvl_usd, volume_24h_usd=excluded.volume_24h_usd,
                            fees_24h_usd=excluded.fees_24h_usd,
                            protocol_fee_est_usd=excluded.protocol_fee_est_usd,
                            lp_fee_usd=excluded.lp_fee_usd, apr=excluded.apr,
                            source='screenshot'
                    """, (
                        ss_date.isoformat(), p["chain"], p["version"], p["pool_address"],
                        p["token0_symbol"], p["token1_symbol"], p["fee_tier"], None, None,
                        p["tvl_usd"], p["volume_24h_usd"], p["fees_24h_usd"], p.get("tx_count"),
                        p["protocol_fee_est_usd"], p["lp_fee_usd"], p.get("apr"),
                    ))
                conn.commit()
                conn.close()
                st.session_state.ss_saved = True
                st.session_state.ss_pools = []
                st.session_state._jump_date = ss_date.isoformat()
                st.rerun()

        with col_discard:
            if st.button("Discard"):
                st.session_state.ss_pools = []
                st.rerun()

    elif st.session_state.ss_saved:
        st.info("Data saved. Upload another screenshot or switch to a different tab.")

# ── Tab 6: Import CSV ─────────────────────────────────────────────────────────

with tab6:
    st.subheader("Import Historical Data from CSV")

    with st.expander("CSV Format & Template", expanded=True):
        st.markdown("""
**Expected columns** (comma or tab separated, headers required):

| Column | Required | Example |
|--------|----------|---------|
| `date` | ✅ | `2/6/2026` or `2026-02-06` |
| `Pool` | ✅ | `USDT/KOGE` |
| `Model` | ✅ | `v3` or `v4` |
| `Fee Tier` | ✅ | `0.01%` |
| `TVL` | ✅ | `$7,400,000` |
| `Volume (24hr)` | ✅ | `$33,500,000` |
| `APR` | optional | `16.49%` |
| `Chain` | optional | `bnb` or `arbitrum` |

**Protocol fee** is auto-calculated (V3 only). V4 rows set protocol fee to N/A.
        """)
        template_path = Path(__file__).parent.parent / "data" / "template.csv"
        generate_template(str(template_path))
        with open(template_path) as f:
            st.download_button(
                "Download Template CSV", f.read(),
                file_name="uniswap_tvl_template.csv", mime="text/csv",
            )

    default_chain_sel = st.selectbox(
        "Default chain (used when CSV has no 'Chain' column)",
        ["bnb", "arbitrum", "base", "monad"], key="csv_default_chain",
    )
    csv_file = st.file_uploader("Upload your CSV", type=["csv"], key="csv_upload")

    if csv_file:
        import tempfile, os as _os
        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv", mode="wb") as tmp:
            tmp.write(csv_file.read())
            tmp_path = tmp.name

        col_dry, col_import = st.columns(2)
        with col_dry:
            if st.button("Validate (Dry Run)"):
                result = import_csv(tmp_path, dry_run=True, default_chain=default_chain_sel)
                if result["errors"]:
                    st.warning(f"{result['rows_ok']} rows OK, {result['rows_error']} errors:")
                    for e in result["errors"][:15]:
                        st.text(e)
                else:
                    st.success(f"{result['rows_ok']} rows validated — no errors found.")

        with col_import:
            if st.button("Import into Database", type="primary"):
                result = import_csv(tmp_path, dry_run=False, default_chain=default_chain_sel)
                if result["rows_error"]:
                    st.warning(f"Imported {result['rows_ok']} rows. {result['rows_error']} errors:")
                    for e in result["errors"][:15]:
                        st.text(e)
                else:
                    st.success(f"Successfully imported {result['rows_ok']} rows!")
                    st.rerun()

        _os.unlink(tmp_path)

# ── Tab 7: Manage Data ────────────────────────────────────────────────────────

with tab7:
    st.subheader("Manage Data")

    # ── Database restore (owner only) ──────────────────────────────────────────
    _is_owner = (
        st.session_state.get("auth_user", {}).get("email", "").lower()
        == _os.environ.get("DASHBOARD_OWNER_EMAIL", "").lower()
    )
    if _is_owner:
        with st.expander("⚠️ Restore Database from Backup"):
            st.caption("Upload a tvl.db file to replace the current database. This overwrites all existing data.")
            _db_upload = st.file_uploader("Upload tvl.db", type=["db"], key="db_restore")
            if _db_upload and st.button("Restore Database", type="primary"):
                import shutil as _shutil
                _dest = Path(DB_PATH)
                _dest.parent.mkdir(parents=True, exist_ok=True)
                with open(_dest, "wb") as _f:
                    _f.write(_db_upload.read())
                st.success("Database restored successfully. Reload the page to see your data.")

    # ── Deduplication ─────────────────────────────────────────────────────────
    st.markdown("#### Deduplicate Entries")
    st.caption(
        "Finds pools recorded multiple times on the same date (e.g. from both a "
        "screenshot and a CSV import). Keeps the row with the highest TVL."
    )

    dups = find_duplicates_for_date(selected_date)
    if not dups:
        st.success(f"No duplicates found for {selected_date}.")
    else:
        st.warning(f"Found {len(dups)} duplicate group(s) on {selected_date}:")
        dup_df = pd.DataFrame([{
            "Pair":     f"{d['token0_symbol']}/{d['token1_symbol']}",
            "Chain":    d["chain"].upper(),
            "Version":  d["version"].upper(),
            "Fee Tier": _fmt_fee(d.get('fee_tier') or 0, d.get('version', '')),
            "Copies":   d["cnt"],
            "Sources":  d["sources"],
        } for d in dups])
        st.dataframe(dup_df, use_container_width=True, hide_index=True)

        if st.button("Deduplicate — keep highest TVL", type="primary"):
            hidden = deduplicate_date(selected_date)
            st.success(f"Hid {hidden} lower-TVL duplicate entr{'y' if hidden == 1 else 'ies'} (restorable below).")
            st.rerun()

    st.divider()

    # ── Hide specific entries ──────────────────────────────────────────────────
    st.markdown("#### Hide Entries")
    st.caption("Hidden entries are removed from all charts and tables but can be restored at any time.")

    manage_rows = get_snapshots(selected_date, chain_filter, version_filter)
    if not manage_rows:
        st.info("No active entries found for the current date and filters.")
    else:
        df_manage = pd.DataFrame(manage_rows)
        df_manage["Label"]   = df_manage.apply(
            lambda r: f"{pool_label(r)} [ID:{r['id']} · {r['source']}]", axis=1
        )
        df_manage["tvl_m"]   = df_manage["tvl_usd"].apply(_to_m)
        df_manage["Version"] = df_manage["version"].str.upper()
        df_manage["Chain"]   = df_manage["chain"].str.upper()

        st.dataframe(
            df_manage[["Label", "Chain", "Version", "tvl_m", "source"]].rename(
                columns={"source": "Source"}
            ),
            column_config={"tvl_m": st.column_config.NumberColumn("TVL", **_MUSD)},
            use_container_width=True, hide_index=True,
        )

        label_to_id = dict(zip(df_manage["Label"], df_manage["id"]))
        selected_to_hide = st.multiselect(
            "Select entries to hide",
            list(label_to_id.keys()),
            placeholder="Choose pools to hide…",
        )

        if selected_to_hide:
            n = len(selected_to_hide)
            st.warning(f"{n} entr{'y' if n == 1 else 'ies'} selected.")
            if st.button("Hide Selected Entries", type="primary"):
                for lbl in selected_to_hide:
                    soft_delete_entry(label_to_id[lbl])
                st.success(f"Hid {n} entr{'y' if n == 1 else 'ies'}. Restore them in the section below.")
                st.rerun()

    st.divider()

    # ── Restore hidden entries ─────────────────────────────────────────────────
    st.markdown("#### Restore Hidden Entries")
    st.caption("All entries hidden on the selected date. Select any to make them visible again.")

    deleted_rows = get_deleted_entries(selected_date, chain_filter, version_filter)
    if not deleted_rows:
        st.success(f"No hidden entries for {selected_date}.")
    else:
        df_del = pd.DataFrame(deleted_rows)
        df_del["Label"]   = df_del.apply(
            lambda r: f"{pool_label(r)} [ID:{r['id']} · {r['source']}]", axis=1
        )
        df_del["tvl_m"]   = df_del["tvl_usd"].apply(_to_m)
        df_del["Version"] = df_del["version"].str.upper()
        df_del["Chain"]   = df_del["chain"].str.upper()

        st.dataframe(
            df_del[["Label", "Chain", "Version", "tvl_m", "source"]].rename(
                columns={"source": "Source"}
            ),
            column_config={"tvl_m": st.column_config.NumberColumn("TVL", **_MUSD)},
            use_container_width=True, hide_index=True,
        )

        restore_label_to_id = dict(zip(df_del["Label"], df_del["id"]))
        selected_to_restore = st.multiselect(
            "Select entries to restore",
            list(restore_label_to_id.keys()),
            placeholder="Choose pools to restore…",
        )

        col_restore, col_restore_all = st.columns(2)
        with col_restore:
            if selected_to_restore:
                n = len(selected_to_restore)
                if st.button(f"Restore {n} Selected", type="primary"):
                    for lbl in selected_to_restore:
                        restore_entry(restore_label_to_id[lbl])
                    st.success(f"Restored {n} entr{'y' if n == 1 else 'ies'}.")
                    st.rerun()
        with col_restore_all:
            if st.button("Restore All Hidden"):
                for rid in df_del["id"]:
                    restore_entry(int(rid))
                st.success(f"Restored all {len(deleted_rows)} hidden entries.")
                st.rerun()

# ── Tab 8: Access Control (owner only) ────────────────────────────────────────

with tab8:
    _owner_email = _os.environ.get("DASHBOARD_OWNER_EMAIL", "")
    if st.session_state["auth_user"]["email"].lower() != _owner_email.lower():
        st.warning("This section is restricted to the dashboard owner.")
    else:
        st.subheader("Access Control")

        st.markdown("#### Whitelist")
        _wl = get_whitelist()
        if _wl:
            st.dataframe(
                pd.DataFrame(_wl)[["email", "added_by", "added_at", "notes"]],
                use_container_width=True, hide_index=True,
            )
        else:
            st.info("Whitelist is empty.")

        _col_add, _col_rem = st.columns(2)
        with _col_add:
            _new_email = st.text_input("Add email", placeholder="user@example.com")
            _new_notes = st.text_input("Notes (optional)")
            if st.button("Add to Whitelist", type="primary"):
                if _new_email:
                    _added = add_to_whitelist(
                        _new_email,
                        added_by=st.session_state["auth_user"]["email"],
                        notes=_new_notes,
                    )
                    if _added:
                        st.success(f"Added {_new_email}")
                    else:
                        st.info(f"{_new_email} is already whitelisted.")
                    st.rerun()
        with _col_rem:
            if _wl:
                _to_remove = st.selectbox(
                    "Remove email", ["— select —"] + [r["email"] for r in _wl]
                )
                if st.button("Remove from Whitelist"):
                    if _to_remove != "— select —":
                        remove_from_whitelist(_to_remove)
                        st.success(f"Removed {_to_remove}")
                        st.rerun()

        st.divider()
        st.markdown("#### Recent Access Log")
        _log = get_access_log(50)
        if _log:
            _df_log = pd.DataFrame(_log)
            _df_log["Status"] = _df_log["was_allowed"].map({1: "✅ Granted", 0: "❌ Denied"})
            st.dataframe(
                _df_log[["email", "name", "access_time", "Status"]],
                use_container_width=True, hide_index=True,
            )
        else:
            st.info("No access attempts logged yet.")
