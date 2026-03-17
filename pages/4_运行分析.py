# -*- coding: utf-8 -*-
from __future__ import annotations

import plotly.express as px
import streamlit as st

st.set_page_config(page_title="运行分析", page_icon="📈", layout="wide")

from utils.bootstrap import bootstrap_page
from utils.config import get_bucket_to_ton
from utils.sidebar_filters import render_global_sidebar_by_df
from utils.paths import get_db_path
from utils.snapshot import get_active_snapshot_id
from utils.debug import get_debug_flag, render_debug_sidebar
from utils.data_access import load_daily_ops_data, filter_df_by_date_range
from utils.ops_analysis import (
    prepare_ops_metrics,
    summarize_ops_period,
    judge_process_stability,
    build_monthly_ops_summary,
)


def polish_fig(fig):
    fig.update_layout(
        hovermode="x unified",
        margin=dict(l=10, r=10, t=20, b=10),
        legend_title_text="",
    )
    fig.update_xaxes(showgrid=True, gridcolor="rgba(255,255,255,0.06)")
    fig.update_yaxes(showgrid=True, gridcolor="rgba(255,255,255,0.06)")
    return fig


def has_series(df, cols: list[str]) -> bool:
    for c in cols:
        if c in df.columns and df[c].notna().sum() > 0:
            return True
    return False


db_path = get_db_path()
bootstrap_page(db_path)

st.title("📈 运行分析")
st.caption(f"整合处理量、出渣率、水耗、电耗，并叠加双线桶数与离心机真实产出。当前按 1 桶 ≈ {get_bucket_to_ton():.2f} 吨换算。")

snapshot_id = get_active_snapshot_id(db_path)

raw_df = load_daily_ops_data(db_path, snapshot_id=snapshot_id)
ops_df = prepare_ops_metrics(raw_df)

render_debug_sidebar(
    db_path,
    snapshot_id,
    ops_df.head(3) if get_debug_flag() and not ops_df.empty else None,
)

if ops_df.empty:
    st.warning("当前快照下暂无运行数据。")
    st.stop()

ops_df["date"] = ops_df["date"].copy()

start_date, end_date, date_meta = render_global_sidebar_by_df(ops_df, date_col="date")
st.caption(f"当前筛选区间：{date_meta['label']}")

view_df = filter_df_by_date_range(ops_df, start_date, end_date)

if view_df.empty:
    st.warning("当前筛选范围内无数据。")
    st.stop()

summary = summarize_ops_period(view_df)
stability, reasons = judge_process_stability(view_df)

m1, m2, m3, m4, m5, m6 = st.columns(6)
m1.metric("处理量（吨）", f"{summary['incoming_ton']:.1f}")
m2.metric("来料（桶）", f"{summary['incoming_bucket_count']:.0f}")
m3.metric(
    "平均出渣率",
    f"{summary['avg_slag_rate']:.1%}"
    if summary["avg_slag_rate"] == summary["avg_slag_rate"]
    else "-",
)
m4.metric(
    "真实浆料产出（m³）",
    f"{summary['actual_slurry_m3']:.1f}",
)
m5.metric(
    "浆料产出强度（m³/吨）",
    f"{summary['avg_slurry_per_ton']:.2f}"
    if summary["avg_slurry_per_ton"] == summary["avg_slurry_per_ton"]
    else "-",
)
m6.metric(
    "压缩箱累计（桶）",
    f"{view_df['compress_bucket_count'].fillna(0).sum():.0f}" if "compress_bucket_count" in view_df.columns else "-",
)

n1, n2, n3, n4 = st.columns(4)
n1.metric(
    "平均单吨水耗",
    f"{summary['avg_water_per_ton']:.2f}"
    if summary["avg_water_per_ton"] == summary["avg_water_per_ton"]
    else "-",
)
n2.metric(
    "平均单吨电耗",
    f"{summary['avg_elec_per_ton']:.2f}"
    if summary["avg_elec_per_ton"] == summary["avg_elec_per_ton"]
    else "-",
)
n3.metric(
    "1线平均效率（吨/小时）",
    f"{summary['line1_avg_tph']:.2f}" if summary["line1_avg_tph"] == summary["line1_avg_tph"] else "-",
)
n4.metric(
    "2线平均效率（吨/小时）",
    f"{summary['line2_avg_tph']:.2f}" if summary["line2_avg_tph"] == summary["line2_avg_tph"] else "-",
)

st.markdown("### 工艺稳定性判断")
if stability == "稳定":
    st.success(f"运行状态：{stability}")
elif stability == "基本稳定":
    st.info(f"运行状态：{stability}")
else:
    st.warning(f"运行状态：{stability}")

for reason in reasons:
    st.caption(f"• {reason}")

if "compress_warning" in view_df.columns and view_df["compress_warning"].fillna(False).any():
    latest_warn = view_df.loc[view_df["compress_warning"].fillna(False), ["date", "compress_bucket_count", "compress_bucket_diff"]].tail(1)
    if not latest_warn.empty:
        row = latest_warn.iloc[-1]
        st.warning(f"压缩箱存在积压迹象：{row['date'].date()} 当日较前一日增加 {row['compress_bucket_diff']:.0f} 桶。")

st.markdown("### 日趋势")
t1, t2 = st.columns(2)

with t1:
    st.markdown("**处理量（吨）**")
    fig = px.line(view_df, x="date", y="incoming_ton", markers=True)
    fig.update_layout(xaxis_title="", yaxis_title="吨")
    st.plotly_chart(polish_fig(fig), use_container_width=True)

with t2:
    st.markdown("**真实浆料产出（m³）**")
    fig = px.line(view_df, x="date", y="actual_slurry_m3", markers=True)
    fig.update_layout(xaxis_title="", yaxis_title="m³")
    st.plotly_chart(polish_fig(fig), use_container_width=True)

t3, t4 = st.columns(2)

with t3:
    st.markdown("**出渣率**")
    fig = px.line(view_df, x="date", y="slag_rate", markers=True)
    fig.update_layout(xaxis_title="", yaxis_title="出渣率", yaxis_tickformat=".0%")
    st.plotly_chart(polish_fig(fig), use_container_width=True)

with t4:
    st.markdown("**浆料产出强度（m³/吨）**")
    fig = px.line(view_df, x="date", y="slurry_per_ton", markers=True)
    fig.update_layout(xaxis_title="", yaxis_title="m³/吨")
    st.plotly_chart(polish_fig(fig), use_container_width=True)

u1, u2 = st.columns(2)
with u1:
    st.markdown("**单吨水耗（m³/吨）**")
    fig = px.line(view_df, x="date", y="water_per_ton", markers=True)
    fig.update_layout(xaxis_title="", yaxis_title="m³/吨")
    st.plotly_chart(polish_fig(fig), use_container_width=True)

with u2:
    st.markdown("**单吨电耗（kWh/吨）**")
    fig = px.line(view_df, x="date", y="elec_per_ton", markers=True)
    fig.update_layout(xaxis_title="", yaxis_title="kWh/吨")
    st.plotly_chart(polish_fig(fig), use_container_width=True)

if has_series(view_df, ["line1_feed_bucket_count", "line2_feed_bucket_count"]):
    st.markdown("### 双线对比")
    l1, l2 = st.columns(2)
    with l1:
        fig = px.bar(
            view_df,
            x="date",
            y=[c for c in ["line1_feed_bucket_count", "line2_feed_bucket_count"] if c in view_df.columns],
            barmode="group",
        )
        fig.update_layout(xaxis_title="", yaxis_title="桶")
        st.plotly_chart(polish_fig(fig), use_container_width=True)
    with l2:
        eff_cols = [c for c in ["line1_feed_tph", "line2_feed_tph"] if c in view_df.columns]
        if eff_cols:
            fig = px.line(view_df, x="date", y=eff_cols, markers=True)
            fig.update_layout(xaxis_title="", yaxis_title="吨/小时")
            st.plotly_chart(polish_fig(fig), use_container_width=True)

st.markdown("### 月度趋势")
monthly_df = build_monthly_ops_summary(view_df)

if not monthly_df.empty:
    a1, a2 = st.columns(2)
    with a1:
        st.markdown("**月度处理量**")
        fig = px.bar(monthly_df, x="month_label", y="incoming_ton")
        fig.update_layout(xaxis_title="", yaxis_title="吨")
        st.plotly_chart(polish_fig(fig), use_container_width=True)

    with a2:
        st.markdown("**月度真实浆料产出**")
        fig = px.bar(monthly_df, x="month_label", y="actual_slurry_m3")
        fig.update_layout(xaxis_title="", yaxis_title="m³")
        st.plotly_chart(polish_fig(fig), use_container_width=True)

    a3, a4 = st.columns(2)
    with a3:
        st.markdown("**月度单吨水耗**")
        fig = px.line(monthly_df, x="month_label", y="water_per_ton", markers=True)
        fig.update_layout(xaxis_title="", yaxis_title="m³/吨")
        st.plotly_chart(polish_fig(fig), use_container_width=True)

    with a4:
        st.markdown("**月度单吨电耗**")
        fig = px.line(monthly_df, x="month_label", y="elec_per_ton", markers=True)
        fig.update_layout(xaxis_title="", yaxis_title="kWh/吨")
        st.plotly_chart(polish_fig(fig), use_container_width=True)

st.markdown("### 原始明细")
show_cols = [
    c
    for c in [
        "date",
        "incoming_trips",
        "incoming_ton",
        "incoming_bucket_count",
        "line1_feed_bucket_count",
        "line1_slag_bucket_count",
        "line2_feed_bucket_count",
        "line2_slag_bucket_count",
        "compress_bucket_count",
        "centrifuge_feed_m3",
        "actual_slurry_m3",
        "line1_runtime_hours",
        "line2_runtime_hours",
        "line1_feed_tph",
        "line2_feed_tph",
        "slag_trips",
        "slag_ton",
        "slag_total_ton",
        "water_m3",
        "daily_elec_kwh",
        "slag_rate",
        "water_per_ton",
        "elec_per_ton",
        "slurry_per_ton",
        "to_wwtp_m3",
        "arrive_wwtp_m3",
        "wwtp_gap_m3",
    ]
    if c in view_df.columns
]
st.dataframe(
    view_df[show_cols].sort_values("date", ascending=False),
    use_container_width=True,
    hide_index=True,
)
