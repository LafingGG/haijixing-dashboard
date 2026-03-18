# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sqlite3

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

st.set_page_config(page_title="水电能耗 | 海吉星果蔬项目", layout="wide")

from utils.bootstrap import bootstrap_page
from utils.definitions import DEFINITIONS_MD, DEFINITIONS_VERSION
from utils.paths import get_db_path
from utils.sidebar_filters import render_global_sidebar_by_df
from utils.snapshot import get_active_snapshot_id
from utils.ops_analysis import prepare_ops_metrics


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


def polish_fig(fig, title: str | None = None, height: int = 340):
    if title:
        fig.update_layout(title=title)
    fig.update_layout(
        height=height,
        hovermode="x unified",
        margin=dict(l=10, r=10, t=40, b=10),
        legend_title_text="",
    )
    fig.update_xaxes(showgrid=True, gridcolor="rgba(255,255,255,0.06)")
    fig.update_yaxes(showgrid=True, gridcolor="rgba(255,255,255,0.06)")
    return fig


if DEBUG:
    st.sidebar.markdown("### 🔎 Debug")
    st.sidebar.caption(f"DB_PATH: `{DB_PATH}`")
    st.sidebar.caption(f"exists: `{os.path.exists(DB_PATH)}`")
    if os.path.exists(DB_PATH):
        st.sidebar.caption(f"size: `{os.path.getsize(DB_PATH)} bytes`")

    @st.cache_data(ttl=300)
    def _debug_read_one_row(db_path: str):
        conn = sqlite3.connect(db_path)
        df = pd.read_sql_query(
            "SELECT * FROM fact_daily_ops WHERE snapshot_id=? ORDER BY date LIMIT 1",
            conn,
            params=(ACTIVE_SNAPSHOT_ID,),
            parse_dates=["date"],
        )
        conn.close()
        row0 = df.to_dict(orient="records")[0] if not df.empty else {}
        return list(df.columns), row0

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


@st.cache_data(ttl=10)
def load_data() -> pd.DataFrame:
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(
        "SELECT * FROM fact_daily_ops WHERE snapshot_id=? ORDER BY date",
        conn,
        params=(ACTIVE_SNAPSHOT_ID,),
        parse_dates=["date"],
    )
    conn.close()
    return df


st.title("水电能耗")

with st.expander("📌 口径说明", expanded=False):
    st.markdown(DEFINITIONS_MD)

raw_df = load_data()
if raw_df.empty:
    st.warning("数据库为空。请先导入 Excel。")
    st.stop()

df = prepare_ops_metrics(raw_df)
if df.empty:
    st.warning("当前快照下暂无可分析数据。")
    st.stop()

df["date"] = pd.to_datetime(df["date"], errors="coerce")
df = df[df["date"].notna()].copy()

start_date, end_date, date_meta = render_global_sidebar_by_df(df, date_col="date")
st.caption(f"当前筛选区间：{date_meta['label']}")

dfr = df[
    (df["date"].dt.date >= start_date) &
    (df["date"].dt.date <= end_date)
].copy()

if dfr.empty:
    st.warning("当前筛选区间内无数据，请调整侧边栏时间范围。")
    st.stop()

# ============================================================
# 核心指标
# ============================================================
total_water = float(dfr["water_m3"].sum(skipna=True)) if "water_m3" in dfr.columns else 0.0
total_elec = float(dfr["daily_elec_kwh"].sum(skipna=True)) if "daily_elec_kwh" in dfr.columns else 0.0
incoming_ton = float(dfr["incoming_ton"].sum(skipna=True)) if "incoming_ton" in dfr.columns else 0.0

avg_water_intensity = total_water / incoming_ton if incoming_ton > 0 else np.nan
avg_elec_intensity = total_elec / incoming_ton if incoming_ton > 0 else np.nan

water_valid_days = int(dfr["water_m3"].notna().sum()) if "water_m3" in dfr.columns else 0
elec_valid_days = int(dfr["daily_elec_kwh"].notna().sum()) if "daily_elec_kwh" in dfr.columns else 0

k1, k2, k3, k4 = st.columns(4)
k1.metric("累计用水量（m³）", f"{total_water:,.1f}")
k2.metric("平均水耗强度（m³/吨）", f"{avg_water_intensity:.2f}" if not pd.isna(avg_water_intensity) else "-")
k3.metric("累计电耗（kWh）", f"{total_elec:,.1f}" if elec_valid_days > 0 else "-")
k4.metric("平均电耗强度（kWh/吨）", f"{avg_elec_intensity:.2f}" if not pd.isna(avg_elec_intensity) else "-")

c1, c2, c3 = st.columns(3)
c1.metric("处理量（吨）", f"{incoming_ton:,.1f}")
c2.metric("有用水记录天数", f"{water_valid_days}")
c3.metric("有电耗记录天数", f"{elec_valid_days}")

# ============================================================
# 水
# ============================================================
st.markdown("## 一、用水分析")

tmp_w = dfr[["date", "water_m3", "water_per_ton"]].copy() if "water_m3" in dfr.columns else pd.DataFrame()

if tmp_w.empty or tmp_w["water_m3"].notna().sum() == 0:
    st.info("当前区间暂无用水数据。")
else:
    tmp_w["water_m3_ma7"] = tmp_w["water_m3"].rolling(7, min_periods=3).mean()

    w1, w2 = st.columns(2)

    with w1:
        st.markdown("**用水量（m³）**")
        fig = go.Figure()
        fig.add_trace(go.Bar(x=tmp_w["date"], y=tmp_w["water_m3"], name="用水量(m³)"))
        fig.add_trace(go.Scatter(x=tmp_w["date"], y=tmp_w["water_m3_ma7"], mode="lines", name="7日均线"))
        fig.update_layout(xaxis_title="日期", yaxis_title="m³")
        st.plotly_chart(polish_fig(fig), use_container_width=True)

    with w2:
        st.markdown("**水耗强度（m³/吨）**")
        fig = px.line(tmp_w, x="date", y="water_per_ton", markers=True)
        fig.update_layout(xaxis_title="日期", yaxis_title="m³/吨")
        st.plotly_chart(polish_fig(fig), use_container_width=True)

# ============================================================
# 电
# ============================================================
st.markdown("## 二、用电分析")

elec_valid = dfr["daily_elec_kwh"].notna().sum() if "daily_elec_kwh" in dfr.columns else 0
if elec_valid == 0:
    st.info("当前区间电表抄表点不足，无法生成每日电耗；可扩大日期范围或补充抄表。")
else:
    tmp_e = dfr[["date", "daily_elec_kwh", "elec_per_ton"]].copy()
    tmp_e["daily_elec_kwh_ma7"] = tmp_e["daily_elec_kwh"].rolling(7, min_periods=3).mean()

    e1, e2 = st.columns(2)

    with e1:
        st.markdown("**每日电耗（kWh）**")
        fig = go.Figure()
        fig.add_trace(go.Bar(x=tmp_e["date"], y=tmp_e["daily_elec_kwh"], name="每日电耗(kWh)"))
        fig.add_trace(go.Scatter(x=tmp_e["date"], y=tmp_e["daily_elec_kwh_ma7"], mode="lines", name="7日均线"))
        fig.update_layout(xaxis_title="日期", yaxis_title="kWh")
        st.plotly_chart(polish_fig(fig), use_container_width=True)

    with e2:
        st.markdown("**电耗强度（kWh/吨）**")
        fig = px.line(tmp_e, x="date", y="elec_per_ton", markers=True)
        fig.update_layout(xaxis_title="日期", yaxis_title="kWh/吨")
        st.plotly_chart(polish_fig(fig), use_container_width=True)

# ============================================================
# 水电对比
# ============================================================
st.markdown("## 三、水电强度对比")

compare_cols = []
if "water_per_ton" in dfr.columns:
    compare_cols.append("water_per_ton")
if "elec_per_ton" in dfr.columns:
    compare_cols.append("elec_per_ton")

if len(compare_cols) == 0:
    st.info("当前区间暂无可用于对比的水电强度数据。")
else:
    fig = px.line(dfr, x="date", y=compare_cols, markers=True)
    fig.update_layout(xaxis_title="日期", yaxis_title="强度")
    st.plotly_chart(polish_fig(fig), use_container_width=True)

# ============================================================
# 原始明细
# ============================================================
with st.expander("🔎 原始明细", expanded=False):
    show_cols = [
        c for c in [
            "date",
            "incoming_ton",
            "water_m3",
            "water_per_ton",
            "water_meter_m3",
            "daily_elec_kwh",
            "elec_per_ton",
            "elec_meter_kwh",
        ] if c in dfr.columns
    ]

    st.dataframe(
        dfr[show_cols].sort_values("date", ascending=False),
        use_container_width=True,
        hide_index=True,
    )

st.caption(f"口径版本：{DEFINITIONS_VERSION}；电耗为抄表差分均摊到日的估算口径。")