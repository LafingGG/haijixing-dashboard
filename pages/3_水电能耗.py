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
DB_PATH = get_db_path()


import os
import streamlit as st
import sqlite3
import pandas as pd

from utils.paths import get_db_path

DB_PATH = get_db_path()

st.sidebar.markdown("### 🔎 Debug")
st.sidebar.caption(f"DB_PATH: `{DB_PATH}`")
st.sidebar.caption(f"exists: `{os.path.exists(DB_PATH)}`")
if os.path.exists(DB_PATH):
    st.sidebar.caption(f"size: `{os.path.getsize(DB_PATH)} bytes`")

@st.cache_data(ttl=300)
def _debug_read_one_row(db_path: str):
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query("SELECT * FROM fact_daily_ops ORDER BY date LIMIT 1", conn, parse_dates=["date"])
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
    df = pd.read_sql_query("SELECT * FROM fact_daily_ops ORDER BY date", conn, parse_dates=["date"])
    conn.close()
    return df


def add_daily_elec(df: pd.DataFrame) -> pd.DataFrame:
    """电表读数为抄表点：相邻抄表差分 -> 均摊到区间内每一天，得到 daily_elec_kwh。"""
    df = df.sort_values("date").copy()
    df["daily_elec_kwh"] = np.nan

    meter = (
        df[["date", "elec_meter_kwh"]]
        .dropna()
        .drop_duplicates("date")
        .sort_values("date")
    )
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


st.set_page_config(page_title="水电能耗 | 海吉星果蔬项目", layout="wide")
st.title("水电能耗")

with st.expander("📌口径说明", expanded=False):
    st.markdown(DEFINITIONS_MD)

df = load_data()
if df.empty:
    st.warning("数据库为空。请先导入 Excel。")
    st.stop()

df = add_daily_elec(df)

min_d, max_d = df["date"].min(), df["date"].max()
start, end = st.slider(
    "选择区间",
    min_value=min_d.date(),
    max_value=max_d.date(),
    value=(min_d.date(), max_d.date()),
)

dfr = df[(df["date"].dt.date >= start) & (df["date"].dt.date <= end)].copy()

# 强度计算：来料<=0 的天不计算强度，避免 inf
valid_incoming = dfr["incoming_ton"].where(dfr["incoming_ton"] > 0)
dfr["water_intensity"] = dfr["water_m3"] / valid_incoming
dfr["elec_intensity_kwh_per_ton_daily"] = dfr["daily_elec_kwh"] / valid_incoming

# =========================
# 水
# =========================
st.subheader("水")

# 用水量：柱状 + 7日均线（满宽）
st.markdown("**用水量（m³）**")
tmp_w = dfr[["date", "water_m3"]].copy()
tmp_w["water_m3_ma7"] = tmp_w["water_m3"].rolling(7, min_periods=3).mean()

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

# 水耗强度：折线（满宽）
st.markdown("**水耗强度（m³/吨）**")
fig = px.line(dfr, x="date", y="water_intensity", title="水耗强度(m³/吨) 日趋势")
st.plotly_chart(fig, use_container_width=True)

# =========================
# 电（均摊到日）
# =========================
st.divider()
st.subheader("电（按抄表差分均摊到日）")

elec_valid = dfr["daily_elec_kwh"].notna().sum()
if elec_valid == 0:
    st.info("当前区间电表抄表点不足，无法按差分均摊生成每日电耗；可扩大日期范围或补充抄表。")
else:
    # 每日电耗：柱状 + 7日均线（满宽）
    st.markdown("**每日电耗（kWh）**")
    tmp_e = dfr[["date", "daily_elec_kwh"]].copy()
    tmp_e["daily_elec_kwh_ma7"] = tmp_e["daily_elec_kwh"].rolling(7, min_periods=3).mean()

    fig = go.Figure()
    fig.add_trace(go.Bar(x=tmp_e["date"], y=tmp_e["daily_elec_kwh"], name="每日电耗(kWh)"))
    fig.add_trace(go.Scatter(x=tmp_e["date"], y=tmp_e["daily_elec_kwh_ma7"], mode="lines", name="7日均线"))
    fig.update_layout(
        title="每日电耗(kWh)（均摊口径）",
        xaxis_title="日期",
        yaxis_title="kWh",
        legend_title_text="",
        hovermode="x unified",
    )
    st.plotly_chart(fig, use_container_width=True)

    # 电耗强度：折线（满宽）
    st.markdown("**电耗强度（kWh/吨）**")
    fig = px.line(dfr, x="date", y="elec_intensity_kwh_per_ton_daily", title="电耗强度(kWh/吨) 日趋势（均摊口径）")
    st.plotly_chart(fig, use_container_width=True)

st.caption(f"口径版本：{DEFINITIONS_VERSION}；电耗为抄表差分均摊到日的估算口径。")