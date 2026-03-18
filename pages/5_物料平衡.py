# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sqlite3

import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st

st.set_page_config(page_title="物料平衡 | 海吉星果蔬项目", layout="wide")

from utils.bootstrap import bootstrap_page
from utils.config import load_thresholds, get_bucket_to_ton
from utils.definitions import DEFINITIONS_MD
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


st.title("物料平衡")
st.caption(f"桶数按 1 桶 ≈ {get_bucket_to_ton():.2f} 吨换算，仅用于运营分析口径。")

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

incoming = float(dfr["incoming_ton"].sum(skipna=True)) if "incoming_ton" in dfr.columns else 0.0
slag = float(dfr["slag_total_ton"].sum(skipna=True)) if "slag_total_ton" in dfr.columns else 0.0
incoming_bucket = float(dfr["incoming_bucket_count"].sum(skipna=True)) if "incoming_bucket_count" in dfr.columns else 0.0
compress_bucket = float(dfr["compress_bucket_count"].sum(skipna=True)) if "compress_bucket_count" in dfr.columns else 0.0
actual_slurry = float(dfr["actual_slurry_m3"].sum(skipna=True)) if "actual_slurry_m3" in dfr.columns else 0.0

slag_rate_total = np.nan if incoming == 0 else slag / incoming
slurry_rate_total = np.nan if incoming == 0 else actual_slurry / incoming

# ============================================================
# 核心指标
# ============================================================
k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("来料(吨)", f"{incoming:,.0f}")
k2.metric("出渣合计(吨)", f"{slag:,.0f}")
k3.metric("来料(桶)", f"{incoming_bucket:,.0f}")
k4.metric("真实浆料产出(m³)", f"{actual_slurry:,.0f}")
k5.metric("出渣率(吨/吨)", "-" if np.isnan(slag_rate_total) else f"{slag_rate_total:.3f}")

q1, q2 = st.columns(2)
q1.metric("浆料产出强度(m³/吨)", "-" if np.isnan(slurry_rate_total) else f"{slurry_rate_total:.3f}")
q2.metric("压缩箱累计(桶)", f"{compress_bucket:,.0f}")

# ============================================================
# 日趋势
# ============================================================
st.divider()
st.subheader("出渣率（日）")

fig = px.line(dfr, x="date", y="slag_rate", markers=True)
fig.update_layout(xaxis_title="", yaxis_title="出渣率", yaxis_tickformat=".0%")
st.plotly_chart(polish_fig(fig), use_container_width=True)

if "slurry_per_ton" in dfr.columns and dfr["slurry_per_ton"].notna().sum() > 0:
    st.subheader("浆料产出强度（日）")
    fig = px.line(dfr, x="date", y="slurry_per_ton", markers=True)
    fig.update_layout(xaxis_title="", yaxis_title="m³/吨")
    st.plotly_chart(polish_fig(fig), use_container_width=True)

if "compress_bucket_count" in dfr.columns and dfr["compress_bucket_count"].notna().sum() > 0:
    st.subheader("压缩箱积压趋势")
    fig = px.bar(dfr, x="date", y="compress_bucket_count")
    fig.update_layout(xaxis_title="", yaxis_title="桶")
    st.plotly_chart(polish_fig(fig), use_container_width=True)

# ============================================================
# 分布
# ============================================================
st.subheader("出渣率分布")

hist_df = dfr.dropna(subset=["slag_rate"]).copy()
hist_df = hist_df[hist_df["slag_rate"].between(0, 2)]

fig = px.histogram(hist_df, x="slag_rate", nbins=30)
fig.update_layout(xaxis_title="出渣率", yaxis_title="天数")
st.plotly_chart(polish_fig(fig), use_container_width=True)

# ============================================================
# 异常日
# ============================================================
st.subheader("异常日（出渣率过高）")

th = load_thresholds()
threshold = st.number_input(
    "阈值（出渣率 > 阈值 列为异常）",
    value=float(th.slag_rate_high),
    step=0.05,
    min_value=0.0,
    max_value=2.0,
)

cols = ["date", "incoming_ton", "slag_total_ton", "slag_rate"]
if "actual_slurry_m3" in dfr.columns:
    cols.append("actual_slurry_m3")
if "slurry_per_ton" in dfr.columns:
    cols.append("slurry_per_ton")
if "compress_bucket_count" in dfr.columns:
    cols.append("compress_bucket_count")

abn = (
    dfr.dropna(subset=["slag_rate"])
    .loc[lambda x: x["slag_rate"] > threshold, cols]
    .sort_values("slag_rate", ascending=False)
)

st.dataframe(abn, use_container_width=True, hide_index=True)

# ============================================================
# 原始明细
# ============================================================
with st.expander("🔎 原始明细", expanded=False):
    show_cols = [
        c for c in [
            "date",
            "incoming_ton",
            "slag_total_ton",
            "incoming_bucket_count",
            "compress_bucket_count",
            "centrifuge_feed_m3",
            "actual_slurry_m3",
            "slag_rate",
            "slurry_per_ton",
            "line1_feed_bucket_count",
            "line2_feed_bucket_count",
            "line1_runtime_hours",
            "line2_runtime_hours",
        ] if c in dfr.columns
    ]

    st.dataframe(
        dfr[show_cols].sort_values("date", ascending=False),
        use_container_width=True,
        hide_index=True,
    )