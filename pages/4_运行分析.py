# -*- coding: utf-8 -*-
from __future__ import annotations

import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

st.set_page_config(page_title="运行分析", page_icon="📈", layout="wide")

from utils.bootstrap import bootstrap_page
from utils.config import get_bucket_to_ton
from utils.sidebar_filters import render_global_sidebar_by_df
from utils.paths import get_db_path
from utils.snapshot import get_active_snapshot_id
from utils.debug import get_debug_flag, render_debug_sidebar
from utils.data_access import load_daily_ops_data, filter_df_by_date_range, safe_div
from utils.ops_analysis import (
    prepare_ops_metrics,
    summarize_ops_period,
    judge_process_stability,
    build_monthly_ops_summary,
)


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


def has_series(df, cols: list[str]) -> bool:
    for c in cols:
        if c in df.columns and df[c].notna().sum() > 0:
            return True
    return False


db_path = get_db_path()
bootstrap_page(db_path)

st.title("📈 运行分析")
bucket_to_ton = get_bucket_to_ton()
st.caption(f"整合处理量、出渣率、水耗、电耗，并叠加双线桶数、开机时长、吨/小时与离心机真实产出。当前按 1 桶 ≈ {bucket_to_ton:.2f} 吨换算。")

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

# ============================================================
# 一、运行总览
# ============================================================
st.markdown("## 一、运行总览")

m1, m2, m3, m4 = st.columns(4)
m1.metric("处理量（吨）", f"{summary['incoming_ton']:.1f}")
m2.metric("来料（桶）", f"{summary['incoming_bucket_count']:.0f}")
m3.metric("真实浆料产出（m³）", f"{summary['actual_slurry_m3']:.1f}")
m4.metric(
    "压缩箱累计（桶）",
    f"{view_df['compress_bucket_count'].fillna(0).sum():.0f}" if "compress_bucket_count" in view_df.columns else "-",
)

n1, n2, n3, n4 = st.columns(4)
n1.metric(
    "平均出渣率",
    f"{summary['avg_slag_rate']:.1%}" if summary["avg_slag_rate"] == summary["avg_slag_rate"] else "-",
)
n2.metric(
    "浆料产出强度（m³/吨）",
    f"{summary['avg_slurry_per_ton']:.2f}" if summary["avg_slurry_per_ton"] == summary["avg_slurry_per_ton"] else "-",
)
n3.metric(
    "平均单吨水耗",
    f"{summary['avg_water_per_ton']:.2f}" if summary["avg_water_per_ton"] == summary["avg_water_per_ton"] else "-",
)
n4.metric(
    "平均单吨电耗",
    f"{summary['avg_elec_per_ton']:.2f}" if summary["avg_elec_per_ton"] == summary["avg_elec_per_ton"] else "-",
)

# ============================================================
# 二、双线效率总览
# ============================================================
st.markdown("## 二、双线效率总览")

line1_runtime = summary.get("line1_runtime_hours", 0.0)
line2_runtime = summary.get("line2_runtime_hours", 0.0)

line1_feed_bucket = summary.get("line1_feed_bucket_count", 0.0)
line2_feed_bucket = summary.get("line2_feed_bucket_count", 0.0)

line1_feed_ton = line1_feed_bucket * bucket_to_ton
line2_feed_ton = line2_feed_bucket * bucket_to_ton

line1_util = safe_div(line1_runtime, summary["days"] * 24)
line2_util = safe_div(line2_runtime, summary["days"] * 24)

r1c1, r1c2, r1c3, r1c4 = st.columns(4)
r1c1.metric("1线开机时长", f"{line1_runtime:.1f} h" if line1_runtime > 0 else "-")
r1c2.metric("1线利用率", f"{line1_util:.1%}" if line1_util == line1_util else "-")
r1c3.metric("1线累计处理", f"{line1_feed_ton:.1f} 吨" if line1_feed_ton > 0 else "-")
r1c4.metric("1线吨/小时", f"{summary['line1_avg_tph']:.2f}" if summary["line1_avg_tph"] == summary["line1_avg_tph"] else "-")

r2c1, r2c2, r2c3, r2c4 = st.columns(4)
r2c1.metric("2线开机时长", f"{line2_runtime:.1f} h" if line2_runtime > 0 else "-")
r2c2.metric("2线利用率", f"{line2_util:.1%}" if line2_util == line2_util else "-")
r2c3.metric("2线累计处理", f"{line2_feed_ton:.1f} 吨" if line2_feed_ton > 0 else "-")
r2c4.metric("2线吨/小时", f"{summary['line2_avg_tph']:.2f}" if summary["line2_avg_tph"] == summary["line2_avg_tph"] else "-")

# ============================================================
# 三、工艺稳定性判断
# ============================================================
st.markdown("## 三、工艺稳定性判断")

if stability == "稳定":
    st.success(f"运行状态：{stability}")
elif stability == "基本稳定":
    st.info(f"运行状态：{stability}")
else:
    st.warning(f"运行状态：{stability}")

for reason in reasons:
    st.caption(f"• {reason}")

# if "compress_warning" in view_df.columns and view_df["compress_warning"].fillna(False).any():
#     latest_warn = view_df.loc[
#         view_df["compress_warning"].fillna(False),
#         ["date", "compress_bucket_count", "compress_bucket_diff"]
#     ].tail(1)
#     if not latest_warn.empty:
#         row = latest_warn.iloc[-1]
#         st.warning(f"压缩箱存在积压迹象：{row['date'].date()} 当日较前一日增加 {row['compress_bucket_diff']:.0f} 桶。")

# ============================================================
# 四、日趋势
# ============================================================
st.markdown("## 四、日趋势")

# 1. 生产规模趋势：处理量 + 真实浆料
st.markdown("### 生产规模趋势")

scale_fig = go.Figure()
scale_fig.add_bar(
    x=view_df["date"],
    y=view_df["incoming_ton"],
    name="处理量（吨）",
)

if "actual_slurry_m3" in view_df.columns:
    scale_fig.add_scatter(
        x=view_df["date"],
        y=view_df["actual_slurry_m3"],
        mode="lines+markers",
        name="真实浆料（m³）",
        yaxis="y2",
    )

scale_fig.update_layout(
    xaxis_title="",
    yaxis_title="处理量（吨）",
    yaxis2=dict(
        title="真实浆料（m³）",
        overlaying="y",
        side="right",
    ),
)
st.plotly_chart(polish_fig(scale_fig, height=380), use_container_width=True)

# 2. 工艺质量趋势
q1, q2 = st.columns(2)

with q1:
    st.markdown("**出渣率**")
    fig = px.line(view_df, x="date", y="slag_rate", markers=True)
    fig.update_layout(xaxis_title="", yaxis_title="出渣率", yaxis_tickformat=".0%")
    st.plotly_chart(polish_fig(fig), use_container_width=True)

with q2:
    st.markdown("**浆料产出强度（m³/吨）**")
    fig = px.line(view_df, x="date", y="slurry_per_ton", markers=True)
    fig.update_layout(xaxis_title="", yaxis_title="m³/吨")
    st.plotly_chart(polish_fig(fig), use_container_width=True)

# 3. 单耗趋势
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

# ============================================================
# 五、双线对比
# ============================================================
if has_series(view_df, ["line1_feed_bucket_count", "line2_feed_bucket_count"]):
    st.markdown("## 五、双线对比")

    l1, l2 = st.columns(2)

    with l1:
        st.markdown("**双线处理桶数**")
        fig = px.bar(
            view_df,
            x="date",
            y=[c for c in ["line1_feed_bucket_count", "line2_feed_bucket_count"] if c in view_df.columns],
            barmode="group",
        )
        fig.update_layout(xaxis_title="", yaxis_title="桶")
        st.plotly_chart(polish_fig(fig), use_container_width=True)

    with l2:
        st.markdown("**双线吨/小时**")
        eff_cols = [c for c in ["line1_feed_tph", "line2_feed_tph"] if c in view_df.columns]
        if eff_cols:
            fig = px.line(view_df, x="date", y=eff_cols, markers=True)
            fig.update_layout(xaxis_title="", yaxis_title="吨/小时")
            st.plotly_chart(polish_fig(fig), use_container_width=True)
        else:
            st.info("暂无双线吨/小时数据。")

    if has_series(view_df, ["line1_runtime_hours", "line2_runtime_hours"]):
        st.markdown("**双线开机时长趋势**")
        runtime_cols = [c for c in ["line1_runtime_hours", "line2_runtime_hours"] if c in view_df.columns]
        fig = px.bar(view_df, x="date", y=runtime_cols, barmode="group")
        fig.update_layout(xaxis_title="", yaxis_title="小时")
        st.plotly_chart(polish_fig(fig), use_container_width=True)

# ============================================================
# 六、月度趋势（基于全量已发布数据）
# ============================================================
st.markdown("## 六、月度趋势")

monthly_df = build_monthly_ops_summary(ops_df)

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

# ============================================================
# 七、原始明细
# ============================================================
with st.expander("🔎 原始明细", expanded=False):
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
            "line1_runtime_utilization",
            "line2_runtime_utilization",
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