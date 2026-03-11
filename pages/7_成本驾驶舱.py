# -*- coding: utf-8 -*-
from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

st.set_page_config(page_title="成本驾驶舱", page_icon="💰", layout="wide")

from utils.bootstrap import bootstrap_page
from utils.paths import get_db_path
from utils.snapshot import get_active_snapshot_id
from utils.cost_analytics import (
    load_cost_detail_data,
    build_monthly_cost_summary,
    build_month_total_cost,
    attach_monthly_ton_cost,
    get_latest_month,
    build_current_month_category_table,
)
from utils.data_access import load_daily_ops_data
from utils.ops_analysis import prepare_ops_metrics, build_monthly_ops_summary

bootstrap_page("成本驾驶舱")

st.title("💰 成本驾驶舱")
st.caption("基于采购付款明细，按运营归属月份分析成本结构与吨均成本。")

db_path = get_db_path()
snapshot_id = get_active_snapshot_id(db_path)

detail_df = load_cost_detail_data(db_path, snapshot_id=snapshot_id)
ops_df = prepare_ops_metrics(load_daily_ops_data(db_path, snapshot_id=snapshot_id))
ops_monthly_df = build_monthly_ops_summary(ops_df)

if detail_df.empty:
    st.warning("当前快照下暂无成本明细数据。")
    st.stop()

monthly_cat_df = build_monthly_cost_summary(detail_df)
month_total_df = build_month_total_cost(monthly_cat_df)
month_total_df = attach_monthly_ton_cost(month_total_df, ops_monthly_df)

latest_month = get_latest_month(month_total_df)
month_options = month_total_df["analysis_month"].tolist() if not month_total_df.empty else []
default_idx = month_options.index(latest_month) if latest_month in month_options else max(len(month_options) - 1, 0)

selected_month = st.selectbox("选择归属月份", options=month_options, index=default_idx if month_options else None)

cur_total = month_total_df[month_total_df["analysis_month"] == selected_month].copy()
cur_cat = build_current_month_category_table(monthly_cat_df, selected_month)
cur_detail = detail_df[detail_df["analysis_month"] == selected_month].copy() if "analysis_month" in detail_df.columns else pd.DataFrame()

m1, m2, m3, m4 = st.columns(4)
month_cost = float(cur_total["amount"].sum()) if not cur_total.empty else 0.0
month_ton = float(cur_total["incoming_ton"].sum()) if ("incoming_ton" in cur_total.columns and not cur_total.empty) else 0.0
month_cost_per_ton = float(cur_total["cost_per_ton"].mean()) if ("cost_per_ton" in cur_total.columns and not cur_total.empty) else 0.0
month_count = len(cur_detail)

m1.metric("当月运营费用", f"{month_cost:,.0f}")
m2.metric("当月处理量（吨）", f"{month_ton:,.1f}")
m3.metric("吨均运营成本", f"{month_cost_per_ton:,.2f}" if month_cost_per_ton == month_cost_per_ton else "-")
m4.metric("费用笔数", month_count)

st.markdown("### 一级分类结构")
c1, c2 = st.columns([1.1, 1.2])

with c1:
    if not cur_cat.empty:
        pie_df = cur_cat.copy()
        fig = px.pie(pie_df, names="category_level1", values="amount", title=f"{selected_month} 一级分类占比")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("当前月份无分类数据。")

with c2:
    if not cur_cat.empty:
        show_df = cur_cat.copy()
        show_df["amount_share"] = show_df["amount_share"].map(lambda x: f"{x:.1%}" if x == x else "-")
        show_df = show_df.rename(columns={
            "category_level1": "一级分类",
            "amount": "金额",
            "amount_share": "占比",
        })
        st.dataframe(show_df, use_container_width=True, hide_index=True)
    else:
        st.info("暂无分类明细。")

st.markdown("### 月度趋势")
t1, t2 = st.columns(2)

with t1:
    if not month_total_df.empty:
        fig = px.bar(month_total_df, x="month_label", y="amount", title="月度运营费用")
        fig.update_layout(xaxis_title="", yaxis_title="金额")
        st.plotly_chart(fig, use_container_width=True)

with t2:
    if not month_total_df.empty:
        fig = px.line(month_total_df, x="month_label", y="cost_per_ton", markers=True, title="吨均运营成本趋势")
        fig.update_layout(xaxis_title="", yaxis_title="元/吨")
        st.plotly_chart(fig, use_container_width=True)

st.markdown("### 一级分类月度趋势")
if not monthly_cat_df.empty:
    fig = px.line(
        monthly_cat_df,
        x="month_label",
        y="amount",
        color="category_level1",
        markers=True,
        title="一级分类月度趋势",
    )
    fig.update_layout(xaxis_title="", yaxis_title="金额")
    st.plotly_chart(fig, use_container_width=True)

st.markdown("### 当月原始付款明细")
if not cur_detail.empty:
    show_cols = [
        c for c in [
            "pay_date", "biz_date_start", "biz_date_end", "analysis_month",
            "item_name", "vendor_name", "category_level1", "amount", "allocated_amount", "remark"
        ] if c in cur_detail.columns
    ]
    st.dataframe(
        cur_detail[show_cols].sort_values(
            [c for c in ["pay_date", "biz_date_start"] if c in cur_detail.columns],
            ascending=False
        ),
        use_container_width=True,
        hide_index=True,
    )
else:
    st.info("当前月份无原始付款明细。")