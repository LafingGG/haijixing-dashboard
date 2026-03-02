# -*- coding: utf-8 -*-
from __future__ import annotations
from utils.definitions import DEFINITIONS_MD, DEFINITIONS_VERSION

import os
import sqlite3
import numpy as np
import pandas as pd
import streamlit as st
import plotly.express as px

def add_daily_elec(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values("date").copy()
    df["daily_elec_kwh"] = np.nan

    meter = df[["date", "elec_meter_kwh"]].dropna().drop_duplicates("date").sort_values("date")
    if len(meter) < 2:
        return df

    for (d0, v0), (d1, v1) in zip(meter[["date","elec_meter_kwh"]].values[:-1],
                                 meter[["date","elec_meter_kwh"]].values[1:]):
        d0 = pd.Timestamp(d0)
        d1 = pd.Timestamp(d1)
        days = (d1 - d0).days
        if days <= 0:
            continue
        delta = float(v1) - float(v0)
        per_day = delta / days

        # 均摊到 (d0, d1] 区间内的每一天：d0 不含，d1 含
        m = (df["date"] > d0) & (df["date"] <= d1)
        df.loc[m, "daily_elec_kwh"] = per_day

    return df

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "db", "ops.sqlite")


@st.cache_data(ttl=5)
def load_data() -> pd.DataFrame:
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT * FROM fact_daily_ops ORDER BY date", conn, parse_dates=["date"])
    conn.close()
    return df


def safe_div(a, b):
    if b is None or b == 0 or (isinstance(b, float) and np.isnan(b)):
        return np.nan
    return a / b


st.set_page_config(page_title="总览 | 海吉星果蔬项目", layout="wide")
st.title("总览")

with st.expander("📌口径说明", expanded=False):
    st.markdown(DEFINITIONS_MD)

df = load_data()

df = add_daily_elec(df)
if df.empty:
    st.warning("数据库为空。请先导入 Excel。")
    st.stop()

min_d, max_d = df["date"].min(), df["date"].max()

# ===== 月份快捷切换 =====
# 可选月份：从数据里取出所有出现过的 YYYY-MM
# ===== 月份快捷切换（升级：上一月/下一月）=====
# ===== 月份快捷切换（稳定版：按钮不改 month_pick）=====
months = sorted(df["date"].dt.to_period("M").astype(str).unique().tolist())

if "start_date" not in st.session_state:
    st.session_state.start_date = min_d.date()
if "end_date" not in st.session_state:
    st.session_state.end_date = max_d.date()
if "month_pick" not in st.session_state:
    st.session_state.month_pick = "自定义"

def clamp_date(d):
    return min(max(d, min_d.date()), max_d.date())

def apply_month_range(m: str):
    """只改日期范围，不改 month_pick（避免 Streamlit APIException）"""
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

cur_m = month_from_start()

mcol1, mcol2, mcol3 = st.columns([1.2, 1, 1])

with mcol1:
    st.selectbox(
        "快捷月份",
        options=["自定义"] + months,
        key="month_pick",
        on_change=set_month_range_from_selectbox,
    )
    st.caption(f"当前：{cur_m}（按钮切月会保持为自定义）")

with mcol2:
    if st.button("◀ 上一月", use_container_width=True, key="btn_prev_month"):
        if cur_m in months:
            i = months.index(cur_m)
            if i > 0:
                apply_month_range(months[i - 1])
                st.rerun()

with mcol3:
    if st.button("下一月 ▶", use_container_width=True, key="btn_next_month"):
        if cur_m in months:
            i = months.index(cur_m)
            if i < len(months) - 1:
                apply_month_range(months[i + 1])
                st.rerun()
                
col1, col2 = st.columns(2)
with col1:
    start = st.date_input(
        "开始日期",
        value=st.session_state.start_date,
        min_value=min_d.date(),
        max_value=max_d.date(),
        key="start_date",
    )
with col2:
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

mask = (df["date"].dt.date >= start) & (df["date"].dt.date <= end)
dfr = df.loc[mask].copy()

st.caption(f"当前区间：{start} ~ {end}（{len(dfr)} 天）")

incoming_ton = float(dfr["incoming_ton"].sum(skipna=True))
slag_total = float(dfr["slag_total_ton"].sum(skipna=True))
water_m3 = float(dfr["water_m3"].sum(skipna=True))

elec_kwh = float(dfr["daily_elec_kwh"].sum(skipna=True))
elec_intensity = safe_div(elec_kwh, incoming_ton)
##slurry_m3 = float(dfr["slurry_m3"].sum(skipna=True))

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("来料(吨)", f"{incoming_ton:,.0f}")
k2.metric("来料(车)", f"{dfr['incoming_trips'].sum(skipna=True):,.0f}")
k3.metric("出渣合计(吨)", f"{slag_total:,.0f}")
k4.metric("用水量(m³)", f"{water_m3:,.0f}")
k5.metric("用电量(kWh)", "-" if np.isnan(elec_kwh) else f"{elec_kwh:,.0f}")
##k5.metric("制浆量(m³)", f"{slurry_m3:,.0f}")

c1, c2, c3, c4 = st.columns(4)
slag_rate = safe_div(slag_total, incoming_ton)
water_intensity = safe_div(water_m3, incoming_ton)

c1.metric("出渣率(吨/吨)", "-" if np.isnan(slag_rate) else f"{slag_rate:.3f}")
c2.metric("水耗强度(m³/吨)", "-" if np.isnan(water_intensity) else f"{water_intensity:.3f}")
c3.metric("电耗强度(kWh/吨)", "-" if np.isnan(elec_intensity) else f"{elec_intensity:.1f}")
c4.metric("平均来料(吨/天)", "-" if len(dfr)==0 else f"{incoming_ton/len(dfr):.1f}")
st.divider()

left, right = st.columns(2)

with left:
    fig = px.line(dfr, x="date", y="incoming_ton", title="来料(吨) 日趋势")
    st.plotly_chart(fig, use_container_width=True)

with right:
    fig = px.line(dfr, x="date", y="slag_total_ton", title="出渣合计(吨) 日趋势")
    st.plotly_chart(fig, use_container_width=True)

st.divider()
st.subheader("用水量（m³）")

tmp_w = dfr[["date", "water_m3"]].copy()
tmp_w["water_m3_ma7"] = tmp_w["water_m3"].rolling(7, min_periods=3).mean()

import plotly.graph_objects as go
fig = go.Figure()
fig.add_trace(go.Bar(x=tmp_w["date"], y=tmp_w["water_m3"], name="用水量(m³)"))
fig.add_trace(go.Scatter(x=tmp_w["date"], y=tmp_w["water_m3_ma7"], mode="lines", name="7日均线"))

fig.update_layout(
    title="用水量(m³)（日）",
    xaxis_title="日期",
    yaxis_title="m³",
    legend_title_text="",
    hovermode="x unified",
)

st.plotly_chart(fig, use_container_width=True)

st.divider()
st.subheader("每日电耗（kWh，均摊口径）")
# ===== 用电：柱状 + 7日均线（更适合有缺失的日数据）=====
elec_valid = dfr["daily_elec_kwh"].notna().sum()
if elec_valid == 0:
    st.info("当前区间电表抄表点不足，无法按差分均摊生成每日电耗；可扩大日期范围或补充抄表。")
else:
    tmp_e = dfr[["date", "daily_elec_kwh"]].copy()
    # 7日移动平均（不填 0，避免把缺失当作 0）
    tmp_e["daily_elec_kwh_ma7"] = tmp_e["daily_elec_kwh"].rolling(7, min_periods=3).mean()

    import plotly.graph_objects as go
    fig = go.Figure()

    fig.add_trace(
        go.Bar(
            x=tmp_e["date"],
            y=tmp_e["daily_elec_kwh"],
            name="每日电耗(kWh)",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=tmp_e["date"],
            y=tmp_e["daily_elec_kwh_ma7"],
            mode="lines",
            name="7日均线",
        )
    )

    fig.update_layout(
        title="每日电耗(kWh)（均摊口径）",
        xaxis_title="日期",
        yaxis_title="kWh",
        legend_title_text="",
        hovermode="x unified",
    )

    st.plotly_chart(fig, use_container_width=True)

st.divider()
st.subheader("快速定位到某一天")

b1, b2, b3 = st.columns(3)

if b1.button("跳到最大出渣率日", use_container_width=True):
    tmp = dfr.copy()
    tmp = tmp[(tmp["incoming_ton"].notna()) & (tmp["incoming_ton"] > 0)]
    tmp["slag_rate"] = tmp["slag_total_ton"] / tmp["incoming_ton"]
    tmp = tmp.dropna(subset=["slag_rate"])
    if not tmp.empty:
        best = tmp.sort_values("slag_rate", ascending=False).iloc[0]["date"].date()
        if "pick_date_filter" not in st.session_state:
            st.session_state.pick_date_filter = "（不筛选）"
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

st.dataframe(dfr_show, use_container_width=True, hide_index=True)

csv = dfr_show.to_csv(index=False).encode("utf-8-sig")
st.download_button(
    "下载当前区间 CSV",
    data=csv,
    file_name=f"ops_{start}_{end}_{('all' if pick=='（不筛选）' else pick)}_{DEFINITIONS_VERSION}.csv",
    mime="text/csv",
)