# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import tempfile

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
from etl.parse_purchase_excel import parse_purchase_workbook
from utils.cost_store import ensure_cost_schema, replace_purchase_cost_batch

user = bootstrap_page("成本驾驶舱")

st.title("💰 成本驾驶舱")
st.caption("基于采购付款明细，按运营归属月份分析成本结构与吨均成本。")

db_path = get_db_path()
snapshot_id = get_active_snapshot_id(db_path)
is_admin = getattr(user, "role", "") == "admin"

# ============================================================
# 管理员上传区
# 说明：当前成本模块为“直接导入生效”，不是 staging/publish 双轨
# ============================================================
if is_admin:
    with st.expander("🛠 管理后台：采购费用 Excel 导入", expanded=False):
        st.caption("支持 sheet：费用明细表 / 费用明细 / 项目垃圾处理费用。导入后将直接替换当前成本数据。")

        up = st.file_uploader(
            "上传采购费用 Excel",
            type=["xlsx", "xls"],
            key="cost_uploader",
        )

        if up is not None:
            with st.spinner("解析采购费用 Excel..."):
                suffix = os.path.splitext(up.name)[1] or ".xlsx"
                tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
                tmp.write(up.getbuffer())
                tmp.close()

                try:
                    preview_df = parse_purchase_workbook(tmp.name)
                finally:
                    try:
                        os.unlink(tmp.name)
                    except Exception:
                        pass

            if preview_df.empty:
                st.error("未解析到采购费用数据，请检查 sheet 名、表头字段或表格结构。")
            else:
                c1, c2 = st.columns([1.1, 1.9])

                with c1:
                    st.markdown("#### 导入检查")
                    st.write({
                        "rows": int(len(preview_df)),
                        "date_min": preview_df["expense_date"].min() if "expense_date" in preview_df.columns else None,
                        "date_max": preview_df["expense_date"].max() if "expense_date" in preview_df.columns else None,
                        "months": sorted(preview_df["analysis_month"].dropna().astype(str).unique().tolist())[:12]
                        if "analysis_month" in preview_df.columns else [],
                    })

                    if "level1_name" in preview_df.columns:
                        lvl = (
                            preview_df["level1_name"]
                            .fillna("未分类")
                            .value_counts(dropna=False)
                            .rename_axis("一级分类")
                            .reset_index(name="条数")
                        )
                        st.markdown("#### 分类预览")
                        st.dataframe(lvl, use_container_width=True, hide_index=True)

                with c2:
                    st.markdown("#### 数据预览（前 20 行）")
                    show_cols = [
                        c for c in [
                            "expense_date",
                            "analysis_month",
                            "item_name",
                            "payee",
                            "amount",
                            "category_name",
                            "level1_name",
                            "remark",
                            "date_source",
                        ] if c in preview_df.columns
                    ]
                    st.dataframe(preview_df[show_cols].head(20), use_container_width=True, hide_index=True)

                if st.button("导入并替换当前成本数据", type="primary", key="btn_import_cost"):
                    try:
                        ensure_cost_schema(db_path)

                        suffix = os.path.splitext(up.name)[1] or ".xlsx"
                        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
                        tmp.write(up.getbuffer())
                        tmp.close()

                        try:
                            df_import = parse_purchase_workbook(tmp.name)
                            info = replace_purchase_cost_batch(
                                db_path=db_path,
                                df=df_import,
                                imported_by=getattr(user, "username", "admin"),
                                source_file=up.name,
                            )
                        finally:
                            try:
                                os.unlink(tmp.name)
                            except Exception:
                                pass

                        st.success(
                            f"导入成功：写入 {info.get('rows_written', 0)} 行，batch_id = {info.get('batch_id', '-')}"
                        )
                        st.cache_data.clear()
                        st.rerun()
                    except Exception as e:
                        st.error(f"导入失败：{e}")
else:
    st.caption("你当前为查看者账号：仅可查看已导入的成本数据。")

# ============================================================
# 读取成本 + 运行数据
# ============================================================
detail_df = load_cost_detail_data(db_path, snapshot_id=snapshot_id)
ops_df = prepare_ops_metrics(load_daily_ops_data(db_path, snapshot_id=snapshot_id))
ops_monthly_df = build_monthly_ops_summary(ops_df)

if detail_df.empty:
    st.warning("当前暂无成本明细数据。管理员可在本页上方上传采购费用 Excel。")
    st.stop()

monthly_cat_df = build_monthly_cost_summary(detail_df)
month_total_df = build_month_total_cost(monthly_cat_df)
month_total_df = attach_monthly_ton_cost(month_total_df, ops_monthly_df)

if month_total_df.empty:
    st.warning("已读取到成本明细，但未能生成月度汇总。请检查 analysis_month 或金额字段。")
    st.stop()

latest_month = get_latest_month(month_total_df)
month_options = month_total_df["analysis_month"].tolist() if not month_total_df.empty else []
default_idx = month_options.index(latest_month) if latest_month in month_options else max(len(month_options) - 1, 0)

selected_month = st.selectbox(
    "选择归属月份",
    options=month_options,
    index=default_idx if month_options else 0,
)

cur_total = month_total_df[month_total_df["analysis_month"] == selected_month].copy()
cur_cat = build_current_month_category_table(monthly_cat_df, selected_month)
cur_detail = detail_df[detail_df["analysis_month"] == selected_month].copy() if "analysis_month" in detail_df.columns else pd.DataFrame()

# ============================================================
# KPI
# ============================================================
m1, m2, m3, m4 = st.columns(4)

month_cost = float(cur_total["amount"].sum()) if not cur_total.empty else 0.0
month_ton = float(cur_total["incoming_ton"].sum()) if ("incoming_ton" in cur_total.columns and not cur_total.empty) else 0.0
month_cost_per_ton = float(cur_total["cost_per_ton"].mean()) if ("cost_per_ton" in cur_total.columns and not cur_total.empty) else 0.0
month_count = len(cur_detail)

m1.metric("当月运营费用", f"{month_cost:,.0f}")
m2.metric("当月处理量（吨）", f"{month_ton:,.1f}")
m3.metric("吨均运营成本", f"{month_cost_per_ton:,.2f}" if pd.notna(month_cost_per_ton) else "-")
m4.metric("费用笔数", month_count)

# ============================================================
# 一级分类结构
# ============================================================
st.markdown("### 一级分类结构")
c1, c2 = st.columns([1.1, 1.2])

with c1:
    if not cur_cat.empty:
        pie_df = cur_cat.copy()
        fig = px.pie(
            pie_df,
            names="category_level1",
            values="amount",
            title=f"{selected_month} 一级分类占比",
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("当前月份无分类数据。")

with c2:
    if not cur_cat.empty:
        show_df = cur_cat.copy()
        show_df["amount_share"] = show_df["amount_share"].map(lambda x: f"{x:.1%}" if pd.notna(x) else "-")
        show_df = show_df.rename(columns={
            "category_level1": "一级分类",
            "amount": "金额",
            "amount_share": "占比",
        })
        st.dataframe(show_df, use_container_width=True, hide_index=True)
    else:
        st.info("暂无分类明细。")

# ============================================================
# 月度趋势
# ============================================================
st.markdown("### 月度趋势")
t1, t2 = st.columns(2)

with t1:
    if not month_total_df.empty:
        fig = px.bar(month_total_df, x="month_label", y="amount", title="月度运营费用")
        fig.update_layout(xaxis_title="", yaxis_title="金额")
        st.plotly_chart(fig, use_container_width=True)

with t2:
    if not month_total_df.empty:
        fig = px.line(
            month_total_df,
            x="month_label",
            y="cost_per_ton",
            markers=True,
            title="吨均运营成本趋势",
        )
        fig.update_layout(xaxis_title="", yaxis_title="元/吨")
        st.plotly_chart(fig, use_container_width=True)

# ============================================================
# 一级分类月度趋势
# ============================================================
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

# ============================================================
# 当月原始明细
# ============================================================
st.markdown("### 当月原始付款明细")
if not cur_detail.empty:
    show_cols = [
        c for c in [
            "pay_date",
            "biz_date_start",
            "biz_date_end",
            "analysis_month",
            "item_name",
            "vendor_name",
            "category_code",
            "category_name",
            "category_level1",
            "amount",
            "allocated_amount",
            "remark",
        ] if c in cur_detail.columns
    ]

    sort_cols = [c for c in ["pay_date", "biz_date_start"] if c in cur_detail.columns]
    if sort_cols:
        cur_detail = cur_detail.sort_values(sort_cols, ascending=False)

    st.dataframe(
        cur_detail[show_cols],
        use_container_width=True,
        hide_index=True,
    )
else:
    st.info("当前月份无原始付款明细。")