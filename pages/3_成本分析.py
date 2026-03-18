# -*- coding: utf-8 -*-
from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

st.set_page_config(page_title="成本驾驶舱", page_icon="💰", layout="wide")

from utils.bootstrap import bootstrap_page
from utils.cost_analytics import (
    attach_monthly_ton_cost,
    build_month_total_cost,
    build_monthly_cost_summary,
    load_cost_detail_data,
)
from utils.cost_store import ensure_cost_schema, get_latest_cost_import_info
from utils.data_access import load_daily_ops_data
from utils.ops_analysis import build_monthly_ops_summary, prepare_ops_metrics
from utils.paths import get_db_path
from utils.sidebar_filters import render_global_sidebar_by_df
from utils.snapshot import get_active_snapshot_id


# ============================================================
# 基础设置
# ============================================================
DB_PATH = get_db_path()
user = bootstrap_page(DB_PATH)
ensure_cost_schema(DB_PATH)


def polish_fig(fig):
    fig.update_layout(
        hovermode="x unified",
        margin=dict(l=10, r=10, t=40, b=10),
        legend_title_text="",
    )
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


def build_focus_category_kpis(view_detail_df: pd.DataFrame, period_ton: float) -> dict[str, float]:
    if view_detail_df is None or view_detail_df.empty:
        return {}

    work = view_detail_df.copy()
    code_col = find_first_existing_col(work, ["category_code"])

    # 没有 category_code 就无法精确按预设分类编码统计
    if code_col is None:
        return {}

    amount_col = None
    for c in ["allocated_amount", "amount"]:
        if c in work.columns:
            amount_col = c
            break

    if amount_col is None:
        return {}

    work[amount_col] = pd.to_numeric(work[amount_col], errors="coerce").fillna(0.0)

    grouped = work.groupby(code_col, as_index=True)[amount_col].sum()

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


@st.cache_data(ttl=60)
def load_latest_cost_import_cached(db_path: str):
    return get_latest_cost_import_info(db_path)


@st.cache_data(ttl=60)
def load_cost_data_cached(db_path: str) -> pd.DataFrame:
    df = load_cost_detail_data(db_path)
    if df is None or df.empty:
        return pd.DataFrame()

    out = df.copy()

    if "pay_date" in out.columns:
        out["pay_date"] = pd.to_datetime(out["pay_date"], errors="coerce")

    if "biz_date_start" in out.columns:
        out["biz_date_start"] = pd.to_datetime(out["biz_date_start"], errors="coerce")

    if "biz_date_end" in out.columns:
        out["biz_date_end"] = pd.to_datetime(out["biz_date_end"], errors="coerce")

    for c in ["amount", "allocated_amount"]:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0.0)

    for c in [
        "analysis_month",
        "category_level1",
        "category_name",
        "item_name",
        "vendor_name",
        "remark",
    ]:
        if c in out.columns:
            out[c] = out[c].fillna("").astype(str)

    return out


@st.cache_data(ttl=60)
def load_ops_data_cached(db_path: str, snapshot_id: str) -> pd.DataFrame:
    df = load_daily_ops_data(db_path, snapshot_id=snapshot_id)
    if df is None or df.empty:
        return pd.DataFrame()

    out = df.copy()
    if "date" in out.columns:
        out["date"] = pd.to_datetime(out["date"], errors="coerce")
    return out


# ============================================================
# 页面标题
# ============================================================
st.title("💰 成本驾驶舱")

latest_cost_import = load_latest_cost_import_cached(DB_PATH)
active_snapshot_id = get_active_snapshot_id(DB_PATH)

c1, c2 = st.columns([1.8, 2.2])

with c1:
    st.info(f"当前运营发布快照：`{active_snapshot_id}`")

with c2:
    if latest_cost_import:
        st.success(
            "当前成本版本："
            f"{latest_cost_import.get('imported_at', '-')}"
            f" ｜ 导入人：{latest_cost_import.get('imported_by', '-')}"
            f" ｜ 文件：{latest_cost_import.get('source_file', '-')}"
            f" ｜ 行数：{latest_cost_import.get('rows_written', '-')}"
        )
    else:
        st.warning("当前还没有成本导入记录，请先到首页 app.py 里导入成本 Excel。")

st.caption("说明：成本 Excel 上传入口已统一迁移到首页 `app.py`，本页仅负责分析展示。")


# ============================================================
# 读取数据
# ============================================================
cost_df = load_cost_data_cached(DB_PATH)
ops_df = load_ops_data_cached(DB_PATH, active_snapshot_id)

if cost_df.empty:
    st.warning("当前没有成本数据，请先到首页导入成本 Excel。")
    st.stop()

if ops_df.empty:
    st.warning("当前没有已发布运营数据，吨成本相关指标将无法正确计算。")


# ============================================================
# 成本预处理
# ============================================================
monthly_cost_df = build_monthly_cost_summary(cost_df)
month_total_cost_df = build_month_total_cost(monthly_cost_df)

ops_metrics = prepare_ops_metrics(ops_df) if not ops_df.empty else pd.DataFrame()
monthly_ops_df = build_monthly_ops_summary(ops_metrics) if not ops_metrics.empty else pd.DataFrame()

month_cost_with_ton_df = (
    attach_monthly_ton_cost(month_total_cost_df, monthly_ops_df)
    if not month_total_cost_df.empty and not monthly_ops_df.empty
    else month_total_cost_df.copy()
)

for df_ in [cost_df, monthly_cost_df, month_total_cost_df, monthly_ops_df, month_cost_with_ton_df]:
    if df_ is not None and not df_.empty and "analysis_month" in df_.columns:
        df_["analysis_month"] = df_["analysis_month"].astype(str)


# ============================================================
# 侧边栏筛选
# ============================================================
filter_base_df = cost_df.copy()
use_date_col = None

if "pay_date" in filter_base_df.columns and filter_base_df["pay_date"].notna().any():
    use_date_col = "pay_date"
elif "biz_date_start" in filter_base_df.columns and filter_base_df["biz_date_start"].notna().any():
    use_date_col = "biz_date_start"
elif "analysis_month" in filter_base_df.columns:
    filter_base_df = filter_base_df.copy()
    filter_base_df["__month_date"] = pd.to_datetime(
        filter_base_df["analysis_month"] + "-01",
        errors="coerce"
    )
    use_date_col = "__month_date"

if use_date_col is None:
    st.warning("缺少可筛选的成本时间字段。")
    st.stop()

start_date, end_date, sidebar_meta = render_global_sidebar_by_df(
    filter_base_df,
    date_col=use_date_col,
    title="成本分析筛选",
    show_data_hint=True,
)


def filter_cost_detail(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    start_month = pd.Timestamp(start_date).to_period("M").strftime("%Y-%m")
    end_month = pd.Timestamp(end_date).to_period("M").strftime("%Y-%m")

    if "analysis_month" in out.columns:
        out = out[
            (out["analysis_month"] >= start_month) &
            (out["analysis_month"] <= end_month)
        ]

    return out


def filter_month_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    start_month = pd.Timestamp(start_date).to_period("M").strftime("%Y-%m")
    end_month = pd.Timestamp(end_date).to_period("M").strftime("%Y-%m")

    if "analysis_month" in out.columns:
        out = out[
            (out["analysis_month"] >= start_month) &
            (out["analysis_month"] <= end_month)
        ]

    return out


cost_df_view = filter_cost_detail(cost_df)
monthly_cost_df_view = filter_month_df(monthly_cost_df)
month_total_cost_df_view = filter_month_df(month_total_cost_df)
month_cost_with_ton_df_view = filter_month_df(month_cost_with_ton_df)


# ============================================================
# 顶部指标
# ============================================================

total_cost = (
    float(cost_df_view["allocated_amount"].sum())
    if not cost_df_view.empty and "allocated_amount" in cost_df_view.columns
    else float(cost_df_view["amount"].sum()) if not cost_df_view.empty and "amount" in cost_df_view.columns
    else 0.0
)

current_period_total = (
    float(month_total_cost_df_view["amount"].sum())
    if not month_total_cost_df_view.empty and "amount" in month_total_cost_df_view.columns
    else total_cost
)

incoming_ton = None
cost_per_ton = None

if not month_cost_with_ton_df_view.empty:
    if "incoming_ton" in month_cost_with_ton_df_view.columns:
        incoming_ton = pd.to_numeric(
            month_cost_with_ton_df_view["incoming_ton"], errors="coerce"
        ).fillna(0).sum()

    if "cost_per_ton" in month_cost_with_ton_df_view.columns:
        cps = pd.to_numeric(
            month_cost_with_ton_df_view["cost_per_ton"], errors="coerce"
        ).dropna()
        cost_per_ton = cps.mean() if not cps.empty else None

# 当前区间处理量与吨成本
period_ton = incoming_ton if incoming_ton is not None else 0.0
period_cost_per_ton = cost_per_ton

st.markdown("## 一、核心指标")

m1, m2, m3, m4 = st.columns(4)
m1.metric("费用总额（元）", f"{total_cost:,.0f}")
m2.metric("区间汇总成本（元）", f"{current_period_total:,.0f}")
m3.metric("区间进料量（吨）", f"{period_ton:,.1f}" if period_ton is not None else "-")
m4.metric("吨成本（元/吨）", f"{period_cost_per_ton:,.2f}" if period_cost_per_ton is not None and pd.notna(period_cost_per_ton) else "-")

focus_kpis = build_focus_category_kpis(cost_df_view, period_ton)

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
# 成本结构分析
# ============================================================
st.markdown("## 二、成本结构分析")

if monthly_cost_df_view.empty:
    st.info("当前筛选条件下暂无成本结构数据。")
else:
    view = monthly_cost_df_view.copy()

    group_col = "category_level1" if "category_level1" in view.columns else None
    value_col = "amount" if "amount" in view.columns else None

    if group_col is None or value_col is None:
        st.warning("成本结构数据缺少必要字段。")
    else:
        pie_df = (
            view.groupby(group_col, as_index=False)[value_col]
            .sum()
            .sort_values(value_col, ascending=False)
        )

        c1, c2 = st.columns([1.2, 1.8])

        with c1:
            fig = px.pie(
                pie_df,
                names=group_col,
                values=value_col,
                title="一级分类成本占比",
            )
            st.plotly_chart(polish_fig(fig), use_container_width=True)

        with c2:
            fig2 = px.bar(
                pie_df,
                x=group_col,
                y=value_col,
                title="一级分类成本对比",
                text_auto=".0f",
            )
            st.plotly_chart(polish_fig(fig2), use_container_width=True)

        st.dataframe(pie_df, use_container_width=True)

# ============================================================
# 月度趋势
# ============================================================
st.markdown("## 三、月度趋势")

if month_cost_with_ton_df.empty:
    st.info("暂无月度趋势数据。")
else:
    trend_df = month_cost_with_ton_df.copy()

    trend_cols = ["analysis_month"]
    if "amount" in trend_df.columns:
        trend_cols.append("amount")
    if "incoming_ton" in trend_df.columns:
        trend_cols.append("incoming_ton")
    if "cost_per_ton" in trend_df.columns:
        trend_cols.append("cost_per_ton")

    trend_df = trend_df[trend_cols].drop_duplicates().sort_values("analysis_month")

    if "amount" in trend_df.columns:
        fig = px.bar(
            trend_df,
            x="analysis_month",
            y="amount",
            title="月度总成本趋势",
            text_auto=".0f",
        )
        st.plotly_chart(polish_fig(fig), use_container_width=True)

    if "cost_per_ton" in trend_df.columns:
        fig2 = px.line(
            trend_df,
            x="analysis_month",
            y="cost_per_ton",
            markers=True,
            title="月度吨成本趋势",
        )
        st.plotly_chart(polish_fig(fig2), use_container_width=True)

# ============================================================
# 分类汇总明细
# ============================================================
st.markdown("## 四、分类汇总明细")

if monthly_cost_df_view.empty or "amount" not in monthly_cost_df_view.columns:
    st.info("当前筛选条件下暂无分类汇总明细。")
else:
    summary_cols = [c for c in ["analysis_month", "category_level1"] if c in monthly_cost_df_view.columns]
    summary_df = monthly_cost_df_view[summary_cols + ["amount"]].sort_values(summary_cols)
    st.dataframe(summary_df, use_container_width=True)


# ============================================================
# 原始明细
# ============================================================
st.markdown("## 五、原始成本明细")

show_cols_priority = [
    "analysis_month",
    "pay_date",
    "biz_date_start",
    "biz_date_end",
    "category_level1",
    "category_name",
    "item_name",
    "vendor_name",
    "amount",
    "allocated_amount",
    "remark",
]

show_cols = [c for c in show_cols_priority if c in cost_df_view.columns]
remaining_cols = [c for c in cost_df_view.columns if c not in show_cols]
final_cols = show_cols + remaining_cols

if cost_df_view.empty:
    st.info("当前筛选条件下暂无原始成本明细。")
else:
    sort_col = "pay_date" if "pay_date" in cost_df_view.columns else "analysis_month"
    st.dataframe(
        cost_df_view[final_cols].sort_values(by=sort_col, ascending=False),
        use_container_width=True,
    )