# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sqlite3
import numpy as np
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

from utils.definitions import DEFINITIONS_MD, DEFINITIONS_VERSION
from utils.paths import get_db_path
from utils.bootstrap import bootstrap_page
from utils.snapshot import get_active_snapshot_id
DB_PATH = get_db_path()
user = bootstrap_page(DB_PATH)
ACTIVE_SNAPSHOT_ID = get_active_snapshot_id(DB_PATH)
def _as_bool(v) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return False
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}

DEBUG = _as_bool(st.secrets.get("DEBUG", False))

st.sidebar.caption(f"DEBUG(secrets) raw: `{st.secrets.get('DEBUG', None)}`")

if DEBUG:
    st.sidebar.markdown("### 🔎 Debug")
    st.sidebar.caption(f"DB_PATH: `{DB_PATH}`")
    st.sidebar.caption(f"exists: `{os.path.exists(DB_PATH)}`")
    if os.path.exists(DB_PATH):
        st.sidebar.caption(f"size: `{os.path.getsize(DB_PATH)} bytes`")

    @st.cache_data(ttl=300)
    def _debug_read_one_row(db_path: str):
        conn = sqlite3.connect(db_path)
        df = pd.read_sql_query("SELECT * FROM fact_daily_ops WHERE snapshot_id=? ORDER BY date LIMIT 1", conn,
        params=(ACTIVE_SNAPSHOT_ID,), parse_dates=["date"])
        conn.close()
        return list(df.columns), df.to_dict(orient="records")[0]

    if os.path.exists(DB_PATH):
        try:
            cols, row0 = _debug_read_one_row(DB_PATH)
            st.sidebar.caption(f"cols: `{len(cols)}`")
            with st.sidebar.expander("columns"):
                st.write(cols)
            with st.sidebar.expander("row[0]"):
                st.write(row0)
        except Exception as e:
            st.sidebar.error("Read DB failed:")
            st.sidebar.exception(e)
            st.stop()
    else:
        st.sidebar.error("DB file not found. Stop.")
        st.stop()


@st.cache_data(ttl=5)
def load_data() -> pd.DataFrame:
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT * FROM fact_daily_ops WHERE snapshot_id=? ORDER BY date", conn,
        params=(ACTIVE_SNAPSHOT_ID,), parse_dates=["date"])
    conn.close()
    return df


def add_daily_elec(df: pd.DataFrame) -> pd.DataFrame:
    """电表读数为抄表点：相邻抄表差分 -> 均摊到区间内每一天，得到 daily_elec_kwh。"""
    df = df.sort_values("date").copy()
    df["daily_elec_kwh"] = np.nan

    meter = df[["date", "elec_meter_kwh"]].dropna().drop_duplicates("date").sort_values("date")
    if len(meter) < 2:
        return df

    for (d0, v0), (d1, v1) in zip(
        meter[["date", "elec_meter_kwh"]].values[:-1],
        meter[["date", "elec_meter_kwh"]].values[1:],
    ):
        d0 = pd.Timestamp(d0)
        d1 = pd.Timestamp(d1)
        days = (d1 - d0).days
        if days <= 0:
            continue

        delta = float(v1) - float(v0)
        per_day = delta / days

        # 均摊到 (d0, d1]：不含上一抄表日，含本次抄表日
        m = (df["date"] > d0) & (df["date"] <= d1)
        df.loc[m, "daily_elec_kwh"] = per_day

    return df


def safe_div(a, b):
    if b is None or b == 0 or (isinstance(b, float) and np.isnan(b)):
        return np.nan
    return a / b


def kpi_pack(dfx: pd.DataFrame) -> dict:
    incoming_ton = float(dfx["incoming_ton"].sum(skipna=True))
    incoming_trips = float(dfx["incoming_trips"].sum(skipna=True))
    slag_total = float(dfx["slag_total_ton"].sum(skipna=True))
    water_m3 = float(dfx["water_m3"].sum(skipna=True))
    elec_kwh = float(dfx["daily_elec_kwh"].sum(skipna=True))
    slag_rate = safe_div(slag_total, incoming_ton)
    water_intensity = safe_div(water_m3, incoming_ton)
    elec_intensity = safe_div(elec_kwh, incoming_ton)
    return dict(
        incoming_ton=incoming_ton,
        incoming_trips=incoming_trips,
        slag_total=slag_total,
        water_m3=water_m3,
        elec_kwh=elec_kwh,
        slag_rate=slag_rate,
        water_intensity=water_intensity,
        elec_intensity=elec_intensity,
    )


def pct_change(cur, prev):
    if prev is None or prev == 0 or (isinstance(prev, float) and np.isnan(prev)):
        return np.nan
    return (cur - prev) / prev


def align_month_series(dfx: pd.DataFrame, value_col: str) -> pd.DataFrame:
    """按月内日序对齐：day(1..31), value（同日多行合并 sum）。"""
    tmp = dfx[["date", value_col]].copy()
    tmp = tmp.dropna(subset=["date"])
    tmp["day"] = tmp["date"].dt.day
    tmp = tmp.groupby("day", as_index=False)[value_col].sum()
    return tmp


def polish_fig(fig, title: str | None = None):
    if title:
        fig.update_layout(title=title)
    fig.update_layout(
        hovermode="x unified",
        margin=dict(l=10, r=10, t=55, b=10),
        legend_title_text="",
    )
    fig.update_xaxes(showgrid=True, gridcolor="rgba(255,255,255,0.06)")
    fig.update_yaxes(showgrid=True, gridcolor="rgba(255,255,255,0.06)")
    return fig


# ---------------- UI ----------------
st.set_page_config(page_title="总览 | 海吉星果蔬项目", layout="wide")
st.title("总览")

st.markdown("""
<style>
/* ===== Fix: headings/text too dim ===== */

/* 1) 主标题/小标题统一提亮 */
h1, h2, h3, h4, h5, h6 {
  color: rgba(255,255,255,0.96) !important;
  text-shadow: 0 0 10px rgba(255,255,255,0.06);
}

/* 2) Streamlit 标题组件（部分版本用 data-testid） */
div[data-testid="stHeader"] * ,
div[data-testid="stMarkdownContainer"] h1,
div[data-testid="stMarkdownContainer"] h2,
div[data-testid="stMarkdownContainer"] h3 {
  color: rgba(255,255,255,0.96) !important;
}

/* 3) 左侧 sidebar 文字提亮（含“控制台”） */
section[data-testid="stSidebar"] * {
  color: rgba(255,255,255,0.92);
}

/* 4) 让 “总览/物料平衡/水电能耗/数据质量” 这些导航更清晰 */
section[data-testid="stSidebar"] a,
section[data-testid="stSidebar"] span {
  opacity: 0.95;
}

/* 5) 扫描线太强会吃字：降低透明度（或你也可以直接删掉 scanline 那段） */
.block-container:before{
  opacity: 0.12 !important;   /* 原来 0.25，改低 */
}
</style>
""", unsafe_allow_html=True)

df = load_data()
if df.empty:
    st.warning("数据库为空。请先导入 Excel。")
    st.stop()

df = add_daily_elec(df)
min_d, max_d = df["date"].min(), df["date"].max()

# Header（赛博渐变）
st.markdown(f"""
<div style="
  padding:16px 18px;
  border-radius:18px;
  background: linear-gradient(90deg, rgba(34,197,94,0.18), rgba(59,130,246,0.10));
  border: 1px solid rgba(255,255,255,0.08);
  margin-bottom: 12px;">
  <div style="font-size:20px;font-weight:800;">海吉星果蔬项目 · 运营驾驶舱</div>
  <div style="opacity:0.78;margin-top:4px;">
    数据范围：{min_d.date()} ~ {max_d.date()} ｜ 口径版本：{DEFINITIONS_VERSION}
  </div>
</div>
""", unsafe_allow_html=True)

with st.expander("📌口径说明", expanded=False):
    st.markdown(DEFINITIONS_MD)

# ===== Sidebar 控制台 =====
months = sorted(df["date"].dt.to_period("M").astype(str).unique().tolist())

if "start_date" not in st.session_state:
    st.session_state.start_date = min_d.date()
if "end_date" not in st.session_state:
    st.session_state.end_date = max_d.date()
if "month_pick" not in st.session_state:
    st.session_state.month_pick = "自定义"
if "pick_date_filter" not in st.session_state:
    st.session_state.pick_date_filter = "（不筛选）"

def clamp_date(d):
    return min(max(d, min_d.date()), max_d.date())

def apply_month_range(m: str):
    first = pd.Period(m).start_time.date()
    last = pd.Period(m).end_time.date()
    st.session_state.start_date = clamp_date(first)
    st.session_state.end_date = clamp_date(last)

def month_from_start() -> str:
    return pd.Timestamp(st.session_state.start_date).to_period("M").strftime("%Y-%m")

def set_month_range_from_selectbox():
    m = st.session_state.month_pick
    if m == "自定义":
        return
    apply_month_range(m)

with st.sidebar:
    st.header("🧭 控制台")
    cur_m = month_from_start()

    st.selectbox(
        "快捷月份",
        options=["自定义"] + months,
        key="month_pick",
        on_change=set_month_range_from_selectbox,
    )
    st.caption(f"当前：{cur_m}（按钮切月不改下拉显示）")

    cA, cB = st.columns(2)
    with cA:
        if st.button("◀ 上一月", use_container_width=True, key="btn_prev_month"):
            if cur_m in months:
                i = months.index(cur_m)
                if i > 0:
                    apply_month_range(months[i - 1])
                    st.rerun()
    with cB:
        if st.button("下一月 ▶", use_container_width=True, key="btn_next_month"):
            if cur_m in months:
                i = months.index(cur_m)
                if i < len(months) - 1:
                    apply_month_range(months[i + 1])
                    st.rerun()

    st.divider()

    start = st.date_input(
        "开始日期",
        value=st.session_state.start_date,
        min_value=min_d.date(),
        max_value=max_d.date(),
        key="start_date",
    )
    end = st.date_input(
        "结束日期",
        value=st.session_state.end_date,
        min_value=min_d.date(),
        max_value=max_d.date(),
        key="end_date",
    )

    if start > end:
        start, end = end, start
        st.session_state.start_date = start
        st.session_state.end_date = end

    compare_on = st.toggle("开启对比：本月 vs 上月（环比）", value=True)

# ===== 当前区间数据 =====
mask = (df["date"].dt.date >= start) & (df["date"].dt.date <= end)
dfr = df.loc[mask].copy()

# ===== Cyber Status Banner =====
issues = []
if dfr.empty:
    level = "danger"
    issues.append("当前区间无数据")
else:
    # 你可以按实际再加规则
    if dfr["incoming_ton"].isna().any():
        issues.append("来料吨存在缺失")
    if dfr["slag_total_ton"].isna().any():
        issues.append("出渣合计存在缺失")
    if dfr["water_m3"].isna().any():
        issues.append("用水量存在缺失")
    # 电耗：只有抄表点不足才提示
    if dfr["daily_elec_kwh"].notna().sum() == 0:
        issues.append("电表抄表点不足，无法生成每日电耗")

    if len(issues) == 0:
        level = "ok"
    elif len(issues) <= 2:
        level = "warn"
    else:
        level = "danger"

badge = {"ok":"🟢 STABLE", "warn":"🟠 WATCH", "danger":"🔴 ALERT"}[level]
grad = {
  "ok":"linear-gradient(90deg, rgba(34,197,94,0.22), rgba(59,130,246,0.10))",
  "warn":"linear-gradient(90deg, rgba(245,158,11,0.22), rgba(59,130,246,0.10))",
  "danger":"linear-gradient(90deg, rgba(239,68,68,0.22), rgba(59,130,246,0.10))",
}[level]
glow = {"ok":"0 0 18px rgba(34,197,94,0.18)",
        "warn":"0 0 18px rgba(245,158,11,0.18)",
        "danger":"0 0 18px rgba(239,68,68,0.18)"}[level]

st.markdown(f"""
<div style="
  padding:14px 16px;
  border-radius:18px;
  background: {grad};
  border: 1px solid rgba(255,255,255,0.10);
  box-shadow: {glow}, 0 10px 30px rgba(0,0,0,0.35);
  margin: 10px 0 14px 0;">
  <div style="display:flex;align-items:center;justify-content:space-between;gap:12px;">
    <div style="font-size:14px;letter-spacing:0.08em;opacity:0.92;font-weight:800;">{badge}</div>
    <div style="opacity:0.78;">区间：{start} ~ {end} ｜ 天数：{len(dfr)}</div>
  </div>
  <div style="margin-top:8px;opacity:0.88;">
    {"✅ 无明显缺失/异常信号" if len(issues)==0 else "；".join(issues)}
  </div>
</div>
""", unsafe_allow_html=True)

st.caption(f"当前区间：{start} ~ {end}（{len(dfr)} 天）")

# ===== 对比区（整月 vs 整月）=====
cur_period = pd.Timestamp(start).to_period("M")
prev_period = (cur_period - 1)
cur_start = clamp_date(cur_period.start_time.date())
cur_end = clamp_date(cur_period.end_time.date())
prev_start = clamp_date(prev_period.start_time.date())
prev_end = clamp_date(prev_period.end_time.date())

df_cur = df[(df["date"].dt.date >= cur_start) & (df["date"].dt.date <= cur_end)].copy()
df_prev = df[(df["date"].dt.date >= prev_start) & (df["date"].dt.date <= prev_end)].copy()
df_cur = add_daily_elec(df_cur)
df_prev = add_daily_elec(df_prev)

if compare_on:
    st.divider()
    st.subheader("📈 本月 vs 上月（环比）")
    st.caption(f"对比区间：本月 {cur_start} ~ {cur_end}  vs  上月 {prev_start} ~ {prev_end}")

    cur_k = kpi_pack(df_cur)
    prev_k = kpi_pack(df_prev)

    t1, t2, t3 = st.tabs(["总量环比", "效率环比", "趋势对齐"])

    with t1:
        cc1, cc2, cc3, cc4 = st.columns(4)
        delta = pct_change(cur_k["incoming_ton"], prev_k["incoming_ton"])
        cc1.metric("来料(吨)", f"{cur_k['incoming_ton']:,.0f}", f"{delta*100:.1f}%" if not np.isnan(delta) else "—")
        delta = pct_change(cur_k["slag_total"], prev_k["slag_total"])
        cc2.metric("出渣合计(吨)", f"{cur_k['slag_total']:,.0f}", f"{delta*100:.1f}%" if not np.isnan(delta) else "—")
        delta = pct_change(cur_k["water_m3"], prev_k["water_m3"])
        cc3.metric("用水量(m³)", f"{cur_k['water_m3']:,.0f}", f"{delta*100:.1f}%" if not np.isnan(delta) else "—")
        delta = pct_change(cur_k["elec_kwh"], prev_k["elec_kwh"])
        cc4.metric("用电量(kWh)", f"{cur_k['elec_kwh']:,.0f}", f"{delta*100:.1f}%" if not np.isnan(delta) else "—")

    with t2:
        cc5, cc6, cc7, cc8 = st.columns(4)

        delta = pct_change(cur_k["slag_rate"], prev_k["slag_rate"])
        cc5.metric("出渣率(吨/吨)", "-" if np.isnan(cur_k["slag_rate"]) else f"{cur_k['slag_rate']:.3f}",
                   f"{delta*100:.1f}%" if not np.isnan(delta) else "—")

        delta = pct_change(cur_k["water_intensity"], prev_k["water_intensity"])
        cc6.metric("水耗强度(m³/吨)", "-" if np.isnan(cur_k["water_intensity"]) else f"{cur_k['water_intensity']:.3f}",
                   f"{delta*100:.1f}%" if not np.isnan(delta) else "—")

        delta = pct_change(cur_k["elec_intensity"], prev_k["elec_intensity"])
        cc7.metric("电耗强度(kWh/吨)", "-" if np.isnan(cur_k["elec_intensity"]) else f"{cur_k['elec_intensity']:.1f}",
                   f"{delta*100:.1f}%" if not np.isnan(delta) else "—")

        cur_days = max((pd.Timestamp(cur_end) - pd.Timestamp(cur_start)).days + 1, 1)
        prev_days = max((pd.Timestamp(prev_end) - pd.Timestamp(prev_start)).days + 1, 1)
        cur_avg_in = cur_k["incoming_ton"] / cur_days
        prev_avg_in = prev_k["incoming_ton"] / prev_days
        delta = pct_change(cur_avg_in, prev_avg_in)
        cc8.metric("平均来料(吨/天)", f"{cur_avg_in:.1f}", f"{delta*100:.1f}%" if not np.isnan(delta) else "—")

    with t3:
        def plot_aligned(value_col: str, title: str):
            cur_s = align_month_series(df_cur, value_col).rename(columns={value_col: "本月"})
            prev_s = align_month_series(df_prev, value_col).rename(columns={value_col: "上月"})
            m = pd.merge(cur_s, prev_s, on="day", how="outer").sort_values("day")
            fig = px.line(m, x="day", y=["本月", "上月"], title=title)
            st.plotly_chart(polish_fig(fig), use_container_width=True)

        plot_aligned("incoming_ton", "来料(吨)（按月内日序对齐）")
        plot_aligned("slag_total_ton", "出渣合计(吨)（按月内日序对齐）")
        plot_aligned("water_m3", "用水量(m³)（按月内日序对齐）")
        plot_aligned("daily_elec_kwh", "每日电耗(kWh)（按月内日序对齐，均摊口径）")

# ===== 当前区间 KPI（对比开时不重复）=====
incoming_ton = float(dfr["incoming_ton"].sum(skipna=True))
slag_total = float(dfr["slag_total_ton"].sum(skipna=True))
water_m3 = float(dfr["water_m3"].sum(skipna=True))
elec_kwh = float(dfr["daily_elec_kwh"].sum(skipna=True))

if not compare_on:
    st.divider()
    st.subheader("📊 当前区间 KPI")

    elec_intensity = safe_div(elec_kwh, incoming_ton)

    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("来料(吨)", f"{incoming_ton:,.0f}")
    k2.metric("来料(车)", f"{dfr['incoming_trips'].sum(skipna=True):,.0f}")
    k3.metric("出渣合计(吨)", f"{slag_total:,.0f}")
    k4.metric("用水量(m³)", f"{water_m3:,.0f}")
    k5.metric("用电量(kWh)", "-" if np.isnan(elec_kwh) else f"{elec_kwh:,.0f}")

    c1, c2, c3, c4 = st.columns(4)
    slag_rate = safe_div(slag_total, incoming_ton)
    water_intensity = safe_div(water_m3, incoming_ton)
    c1.metric("出渣率(吨/吨)", "-" if np.isnan(slag_rate) else f"{slag_rate:.3f}")
    c2.metric("水耗强度(m³/吨)", "-" if np.isnan(water_intensity) else f"{water_intensity:.3f}")
    c3.metric("电耗强度(kWh/吨)", "-" if np.isnan(elec_intensity) else f"{elec_intensity:.1f}")
    c4.metric("平均来料(吨/天)", "-" if len(dfr) == 0 else f"{incoming_ton/len(dfr):.1f}")

# ===== 当前区间趋势（来料/出渣）=====
st.divider()
st.subheader("📉 当前区间趋势")

left, right = st.columns(2)
with left:
    fig = px.line(dfr, x="date", y="incoming_ton", title="来料(吨) 日趋势")
    st.plotly_chart(polish_fig(fig), use_container_width=True)
with right:
    fig = px.line(dfr, x="date", y="slag_total_ton", title="出渣合计(吨) 日趋势")
    st.plotly_chart(polish_fig(fig), use_container_width=True)

# ===== 用水（满宽：柱状+7日均线）=====
st.divider()
st.subheader("💧 用水量（m³）")

tmp_w = dfr[["date", "water_m3"]].copy()
tmp_w["water_m3_ma7"] = tmp_w["water_m3"].rolling(7, min_periods=3).mean()

fig = go.Figure()
fig.add_trace(go.Bar(x=tmp_w["date"], y=tmp_w["water_m3"], name="用水量(m³)"))
fig.add_trace(go.Scatter(x=tmp_w["date"], y=tmp_w["water_m3_ma7"], mode="lines", name="7日均线"))
fig.update_layout(title="用水量(m³)（日）", xaxis_title="日期", yaxis_title="m³", legend_title_text="", hovermode="x unified")
fig = polish_fig(fig)
st.plotly_chart(fig, use_container_width=True)

# ===== 用电（满宽：柱状+7日均线）=====
st.divider()
st.subheader("⚡ 每日电耗（kWh，均摊口径）")

elec_valid = dfr["daily_elec_kwh"].notna().sum()
if elec_valid == 0:
    st.info("当前区间电表抄表点不足，无法按差分均摊生成每日电耗；可扩大日期范围或补充抄表。")
else:
    tmp_e = dfr[["date", "daily_elec_kwh"]].copy()
    tmp_e["daily_elec_kwh_ma7"] = tmp_e["daily_elec_kwh"].rolling(7, min_periods=3).mean()

    fig = go.Figure()
    fig.add_trace(go.Bar(x=tmp_e["date"], y=tmp_e["daily_elec_kwh"], name="每日电耗(kWh)"))
    fig.add_trace(go.Scatter(x=tmp_e["date"], y=tmp_e["daily_elec_kwh_ma7"], mode="lines", name="7日均线"))
    fig.update_layout(title="每日电耗(kWh)（均摊口径）", xaxis_title="日期", yaxis_title="kWh", legend_title_text="", hovermode="x unified")
    fig = polish_fig(fig)
    st.plotly_chart(fig, use_container_width=True)

# ===== 快速定位 + 明细导出 =====
st.divider()
st.subheader("🔎 快速定位到某一天")

b1, b2, b3 = st.columns(3)

if b1.button("跳到最大出渣率日", use_container_width=True):
    tmp = dfr.copy()
    tmp = tmp[(tmp["incoming_ton"].notna()) & (tmp["incoming_ton"] > 0)]
    tmp["slag_rate"] = tmp["slag_total_ton"] / tmp["incoming_ton"]
    tmp = tmp.dropna(subset=["slag_rate"])
    if not tmp.empty:
        best = tmp.sort_values("slag_rate", ascending=False).iloc[0]["date"].date()
        st.session_state.pick_date_filter = str(best)
        st.rerun()

if b2.button("跳到最大用水量日", use_container_width=True):
    tmp = dfr.dropna(subset=["water_m3"])
    if not tmp.empty:
        best = tmp.sort_values("water_m3", ascending=False).iloc[0]["date"].date()
        st.session_state.pick_date_filter = str(best)
        st.rerun()

if b3.button("跳到最大用电量日", use_container_width=True):
    tmp = dfr.dropna(subset=["daily_elec_kwh"])
    if not tmp.empty:
        best = tmp.sort_values("daily_elec_kwh", ascending=False).iloc[0]["date"].date()
        st.session_state.pick_date_filter = str(best)
        st.rerun()

date_options = sorted(dfr["date"].dt.date.dropna().unique().tolist())
pick = st.selectbox(
    "选择日期（用于过滤下方明细表）",
    options=["（不筛选）"] + [str(d) for d in date_options],
    index=0,
    key="pick_date_filter",
)

if pick != "（不筛选）":
    pick_date = pd.to_datetime(pick).date()
    dfr_show = dfr[dfr["date"].dt.date == pick_date].copy()
else:
    dfr_show = dfr

st.subheader("明细（可下载）")
st.dataframe(dfr_show, use_container_width=True, hide_index=True)

csv = dfr_show.to_csv(index=False).encode("utf-8-sig")
suffix = "all" if pick == "（不筛选）" else pick
st.download_button(
    "下载当前区间 CSV",
    data=csv,
    file_name=f"ops_{start}_{end}_{suffix}_{DEFINITIONS_VERSION}.csv",
    mime="text/csv",
)