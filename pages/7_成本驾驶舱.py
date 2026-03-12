# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import tempfile

import pandas as pd
import plotly.express as px
import streamlit as st

st.set_page_config(page_title="成本驾驶舱", page_icon="💰", layout="wide")

from etl.parse_purchase_excel import parse_purchase_workbook
from utils.bootstrap import bootstrap_page
from utils.cost_analytics import (
    attach_monthly_ton_cost,
    build_month_total_cost,
    build_monthly_cost_summary,
    load_cost_detail_data,
)
from utils.cost_store import ensure_cost_schema, replace_purchase_cost_batch
from utils.data_access import load_daily_ops_data
from utils.ops_analysis import build_monthly_ops_summary, prepare_ops_metrics
from utils.paths import get_db_path
from utils.sidebar_filters import render_global_sidebar_by_df
from utils.snapshot import get_active_snapshot_id


def polish_fig(fig):
    fig.update_layout(
        hovermode="x unified",
        margin=dict(l=10, r=10, t=20, b=10),
        legend_title_text="",
    )
    fig.update_xaxes(showgrid=True, gridcolor="rgba(255,255,255,0.06)")
    fig.update_yaxes(showgrid=True, gridcolor="rgba(255,255,255,0.06)")
    return fig


def find_first_existing_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def safe_div(a, b):
    if b is None or b == 0 or pd.isna(b):
        return float("nan")
    return a / b


def normalize_text_series(s: pd.Series) -> pd.Series:
    return s.fillna("未分类").astype(str).str.strip().replace({"": "未分类"})


def build_focus_category_kpis(view_detail_df: pd.DataFrame, period_ton: float) -> dict[str, float]:
    if view_detail_df.empty:
        return {}

    work = view_detail_df.copy()
    code_col = find_first_existing_col(work, ["category_code"])

    if code_col is None or "amount" not in work.columns:
        return {}

    grouped = work.groupby(code_col, as_index=True)["amount"].sum()

    # 对齐 utils.cost_store.CATEGORY_SEED
    focus_code_map = {
        "固渣费吨成本": "slag",
        "碳源费吨成本": "carbon_source",
        "能源费吨成本": "energy_cost",
        "维修费吨成本": "repair",
    }

    out = {}
    for label, code in focus_code_map.items():
        amt = float(grouped.get(code, 0.0))
        out[label] = safe_div(amt, period_ton)
    return out

db_path = get_db_path()
user = bootstrap_page(db_path)

st.title("💰 成本驾驶舱")
st.caption("基于采购付款明细，按当前筛选区间分析成本结构、月度趋势与吨均成本。")

snapshot_id = get_active_snapshot_id(db_path)
is_admin = getattr(user, "role", "") == "admin"

# ============================================================
# 管理员上传区
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
                    st.write(
                        {
                            "rows": int(len(preview_df)),
                            "date_min": preview_df["expense_date"].min() if "expense_date" in preview_df.columns else None,
                            "date_max": preview_df["expense_date"].max() if "expense_date" in preview_df.columns else None,
                            "months": sorted(preview_df["analysis_month"].dropna().astype(str).unique().tolist())[:12]
                            if "analysis_month" in preview_df.columns
                            else [],
                        }
                    )

                    if "category_name" in preview_df.columns:
                        cat_preview = (
                            preview_df["category_name"]
                            .pipe(normalize_text_series)
                            .value_counts(dropna=False)
                            .rename_axis("分类")
                            .reset_index(name="条数")
                        )
                        st.markdown("#### 分类预览")
                        st.dataframe(cat_preview, use_container_width=True, hide_index=True)

                with c2:
                    st.markdown("#### 数据预览（前 20 行）")
                    show_cols = [
                        c
                        for c in [
                            "expense_date",
                            "analysis_month",
                            "item_name",
                            "payee",
                            "amount",
                            "category_name",
                            "category_code",
                            "level1_name",
                            "remark",
                            "date_source",
                        ]
                        if c in preview_df.columns
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

date_col = find_first_existing_col(
    detail_df,
    ["expense_date", "pay_date", "biz_date_start", "biz_date_end"],
)

if date_col is None:
    st.warning("成本明细中缺少可用于时间筛选的日期字段（expense_date / pay_date / biz_date_start / biz_date_end）。")
    st.stop()

detail_df = detail_df.copy()
detail_df[date_col] = pd.to_datetime(detail_df[date_col], errors="coerce")
detail_df = detail_df[detail_df[date_col].notna()].copy()

if detail_df.empty:
    st.warning("成本明细存在记录，但日期字段无法解析。请检查导入数据。")
    st.stop()

# ============================================================
# 固定趋势数据：始终取完整成本数据里的最近 6 个月
# 不受侧边栏当前时间范围影响
# ============================================================
trend_monthly_cat_df = build_monthly_cost_summary(detail_df)
trend_month_total_df = build_month_total_cost(trend_monthly_cat_df)
trend_month_total_df = attach_monthly_ton_cost(trend_month_total_df, ops_monthly_df)

if not trend_month_total_df.empty and "analysis_month" in trend_month_total_df.columns:
    trend_month_total_df = trend_month_total_df.copy()
    trend_month_total_df["analysis_month"] = trend_month_total_df["analysis_month"].astype(str)
    trend_month_total_df = trend_month_total_df.sort_values("analysis_month").tail(6).copy()

    recent_6_months = trend_month_total_df["analysis_month"].tolist()

    if not trend_monthly_cat_df.empty and "analysis_month" in trend_monthly_cat_df.columns:
        trend_monthly_cat_df = trend_monthly_cat_df.copy()
        trend_monthly_cat_df["analysis_month"] = trend_monthly_cat_df["analysis_month"].astype(str)
        trend_monthly_cat_df = trend_monthly_cat_df[
            trend_monthly_cat_df["analysis_month"].isin(recent_6_months)
        ].copy()
else:
    trend_month_total_df = pd.DataFrame()
    trend_monthly_cat_df = pd.DataFrame()

start_date, end_date, date_meta = render_global_sidebar_by_df(
    detail_df.rename(columns={date_col: "date"}),
    date_col="date",
)
st.caption(f"当前筛选区间：{date_meta['label']}")

view_detail_df = detail_df[
    (detail_df[date_col].dt.date >= start_date) & (detail_df[date_col].dt.date <= end_date)
].copy()

if view_detail_df.empty:
    st.warning("当前筛选区间内无成本明细，请调整侧边栏时间范围。")
    st.stop()

# ============================================================
# 基于筛选区间重建汇总
# ============================================================
monthly_cat_df = build_monthly_cost_summary(view_detail_df)
month_total_df = build_month_total_cost(monthly_cat_df)
month_total_df = attach_monthly_ton_cost(month_total_df, ops_monthly_df)

if month_total_df.empty:
    st.warning("当前筛选区间已读取到成本明细，但未能生成月度汇总。请检查 analysis_month 或金额字段。")
    st.stop()

# ============================================================
# 当前区间分类汇总：按 category_name / category_code 统计细分类
# ============================================================
category_name_col = find_first_existing_col(view_detail_df, ["category_name"])
category_code_col = find_first_existing_col(view_detail_df, ["category_code"])

if category_name_col is None:
    st.warning("当前成本数据缺少分类字段（category_name）。")
    st.stop()

group_cols = [category_name_col]
if category_code_col is not None:
    group_cols = [category_code_col, category_name_col]

cur_cat = (
    view_detail_df.groupby(group_cols, as_index=False)["amount"]
    .sum()
    .sort_values("amount", ascending=False)
    .copy()
)

if category_name_col != "category_name":
    cur_cat = cur_cat.rename(columns={category_name_col: "category_name"})
if category_code_col and category_code_col != "category_code" and category_code_col in cur_cat.columns:
    cur_cat = cur_cat.rename(columns={category_code_col: "category_code"})

cur_cat["category_name"] = normalize_text_series(cur_cat["category_name"])
total_amount = float(cur_cat["amount"].sum()) if not cur_cat.empty else 0.0
cur_cat["amount_share"] = cur_cat["amount"] / total_amount if total_amount else 0.0

# ============================================================
# 处理量口径修复：
# 不再从成本汇总表取处理量，直接按当前时间区间从运营日报统计
# ============================================================
period_cost = float(month_total_df["amount"].sum()) if not month_total_df.empty else 0.0

period_ton = 0.0
if not ops_df.empty and "date" in ops_df.columns and "incoming_ton" in ops_df.columns:
    ops_df = ops_df.copy()
    ops_df["date"] = pd.to_datetime(ops_df["date"], errors="coerce")
    ops_view_df = ops_df[
        (ops_df["date"].dt.date >= start_date) &
        (ops_df["date"].dt.date <= end_date)
    ].copy()
    period_ton = float(ops_view_df["incoming_ton"].sum(skipna=True)) if not ops_view_df.empty else 0.0

period_cost_per_ton = safe_div(period_cost, period_ton)
period_count = len(view_detail_df)

# ============================================================
# KPI
# ============================================================
m1, m2, m3, m4 = st.columns(4)
m1.metric("当前区间运营费用", f"{period_cost:,.0f}")
m2.metric("当前区间处理量（吨）", f"{period_ton:,.1f}")
m3.metric("当前区间吨均成本", f"{period_cost_per_ton:,.2f}" if pd.notna(period_cost_per_ton) else "-")
m4.metric("费用笔数", period_count)

# ============================================================
# 重点分类吨成本
# ============================================================
focus_kpis = build_focus_category_kpis(view_detail_df, period_ton)
st.markdown("### 重点分类吨成本")

f1, f2, f3, f4 = st.columns(4)

f1.metric(
    "固渣费吨成本",
    f"{focus_kpis.get('固渣费吨成本', float('nan')):,.2f}"
    if pd.notna(focus_kpis.get("固渣费吨成本"))
    else "-"
)

f2.metric(
    "碳源费吨成本",
    f"{focus_kpis.get('碳源费吨成本', float('nan')):,.2f}"
    if pd.notna(focus_kpis.get("碳源费吨成本"))
    else "-"
)

f3.metric(
    "能源费吨成本",
    f"{focus_kpis.get('能源费吨成本', float('nan')):,.2f}"
    if pd.notna(focus_kpis.get("能源费吨成本"))
    else "-"
)

f4.metric(
    "维修费吨成本",
    f"{focus_kpis.get('维修费吨成本', float('nan')):,.2f}"
    if pd.notna(focus_kpis.get("维修费吨成本"))
    else "-"
)

# ============================================================
# 分类结构
# ============================================================
st.markdown("### 分类结构")
c1, c2 = st.columns([1.1, 1.2])

with c1:
    if not cur_cat.empty:
        fig = px.pie(
            cur_cat,
            names="category_name",
            values="amount",
        )
        st.plotly_chart(polish_fig(fig), use_container_width=True)
    else:
        st.info("当前区间无分类数据。")

with c2:
    if not cur_cat.empty:
        show_df = cur_cat.copy()
        show_df["amount_share"] = show_df["amount_share"].map(lambda x: f"{x:.1%}" if pd.notna(x) else "-")
        rename_map = {
            "category_name": "分类",
            "amount": "金额",
            "amount_share": "占比",
        }
        if "category_code" in show_df.columns:
            rename_map["category_code"] = "分类编码"
        show_df = show_df.rename(columns=rename_map)
        cols = [c for c in ["分类编码", "分类", "金额", "占比"] if c in show_df.columns]
        st.dataframe(show_df[cols], use_container_width=True, hide_index=True)
    else:
        st.info("暂无分类明细。")

# ============================================================
# 成本结构条形图
# ============================================================
st.markdown("### 成本结构排序")
if not cur_cat.empty:
    bar_df = cur_cat.sort_values("amount", ascending=True).copy()
    fig = px.bar(bar_df, x="amount", y="category_name", orientation="h")
    fig.update_layout(xaxis_title="金额", yaxis_title="")
    st.plotly_chart(polish_fig(fig), use_container_width=True)

# ============================================================
# 月度趋势（固定最近 6 个月）
# ============================================================
st.markdown("### 月度趋势（最近 6 个月）")
t1, t2 = st.columns(2)

with t1:
    if not trend_month_total_df.empty:
        st.markdown("**月度运营费用**")
        fig = px.bar(trend_month_total_df, x="month_label", y="amount")
        fig.update_layout(xaxis_title="", yaxis_title="金额")
        st.plotly_chart(polish_fig(fig), use_container_width=True)

with t2:
    if not trend_month_total_df.empty:
        st.markdown("**吨均运营成本趋势**")
        fig = px.line(trend_month_total_df, x="month_label", y="cost_per_ton", markers=True)
        fig.update_layout(xaxis_title="", yaxis_title="元/吨")
        st.plotly_chart(polish_fig(fig), use_container_width=True)

# ============================================================
# 分类月度趋势（固定最近 6 个月）
# ============================================================
st.markdown("### 分类月度趋势（最近 6 个月）")
if not trend_monthly_cat_df.empty:
    trend_month_cat_show = trend_monthly_cat_df.copy()

    trend_cat_name_col = find_first_existing_col(trend_month_cat_show, ["category_name"])
    if trend_cat_name_col is None:
        trend_cat_name_col = find_first_existing_col(trend_month_cat_show, ["category_level1", "level1_name"])

    if trend_cat_name_col is not None:
        if trend_cat_name_col != "category_name":
            trend_month_cat_show = trend_month_cat_show.rename(columns={trend_cat_name_col: "category_name"})
        trend_month_cat_show["category_name"] = normalize_text_series(trend_month_cat_show["category_name"])

        fig = px.line(
            trend_month_cat_show,
            x="month_label",
            y="amount",
            color="category_name",
            markers=True,
        )
        fig.update_layout(xaxis_title="", yaxis_title="金额")
        st.plotly_chart(polish_fig(fig), use_container_width=True)

# ============================================================
# 成本异常提示
# ============================================================
st.markdown("### 成本异常提示")
a1, a2 = st.columns(2)

with a1:
    if not cur_cat.empty:
        top_cat = cur_cat.sort_values("amount", ascending=False).iloc[0]
        st.info(f"当前区间最大成本分类：**{top_cat['category_name']}**，金额 **{top_cat['amount']:,.0f}** 元。")
    else:
        st.info("暂无分类数据。")

with a2:
    if not view_detail_df.empty and "amount" in view_detail_df.columns:
        max_row = view_detail_df.sort_values("amount", ascending=False).iloc[0]
        item_name = max_row["item_name"] if "item_name" in max_row.index else "-"
        st.info(f"当前区间最大单笔支出：**{item_name}**，金额 **{float(max_row['amount']):,.0f}** 元。")
    else:
        st.info("暂无明细数据。")

# ============================================================
# 当前区间原始明细
# ============================================================
st.markdown("### 当前区间原始付款明细")

vendor_col = find_first_existing_col(view_detail_df, ["vendor_name", "payee"])
level1_col = find_first_existing_col(view_detail_df, ["category_level1", "level1_name"])
category_code_col = find_first_existing_col(view_detail_df, ["category_code"])
category_name_col = find_first_existing_col(view_detail_df, ["category_name"])
allocated_col = find_first_existing_col(view_detail_df, ["allocated_amount"])
remark_col = find_first_existing_col(view_detail_df, ["remark"])

show_cols = [
    c
    for c in [
        "expense_date",
        "pay_date",
        "biz_date_start",
        "biz_date_end",
        "analysis_month",
        "item_name",
        vendor_col,
        category_code_col,
        category_name_col,
        level1_col,
        "amount",
        allocated_col,
        remark_col,
    ]
    if c and c in view_detail_df.columns
]

sort_cols = [c for c in [date_col, "biz_date_start"] if c in view_detail_df.columns]
if sort_cols:
    view_detail_df = view_detail_df.sort_values(sort_cols, ascending=False)

if show_cols:
    rename_map = {}
    if vendor_col:
        rename_map[vendor_col] = "收款方"
    if category_code_col:
        rename_map[category_code_col] = "分类编码"
    if category_name_col:
        rename_map[category_name_col] = "分类名称"
    if level1_col:
        rename_map[level1_col] = "一级分类"
    if allocated_col:
        rename_map[allocated_col] = "分摊金额"
    if remark_col:
        rename_map[remark_col] = "备注"

    display_df = view_detail_df[show_cols].rename(columns=rename_map)
    st.dataframe(display_df, use_container_width=True, hide_index=True)
else:
    st.info("当前区间无原始付款明细。")