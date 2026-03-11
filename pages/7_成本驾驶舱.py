# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import tempfile

import pandas as pd
import plotly.express as px
import streamlit as st

from etl.parse_purchase_excel import parse_purchase_workbook
from utils.bootstrap import bootstrap_page
from utils.cost_store import (
    build_cost_dashboard_dataset,
    get_latest_cost_import_info,
    replace_purchase_cost_batch,
)
from utils.paths import get_db_path

DB_PATH = get_db_path()
user = bootstrap_page(DB_PATH)
is_admin = getattr(user, "role", "") == "admin"

st.set_page_config(page_title="成本驾驶舱 | 海吉星运营驾驶舱", layout="wide")
st.title("成本驾驶舱")
st.caption("当前默认口径：优先按费用事项中可识别的清运/处理日期归属成本月份；若识别不到，再回退到付款日期。跨月区间会按天数拆分金额。")


def fmt_money(x) -> str:
    if x is None or pd.isna(x):
        return "-"
    return f"¥{x:,.0f}"


def fmt_num(x, digits=2) -> str:
    if x is None or pd.isna(x):
        return "-"
    return f"{x:,.{digits}f}"


def is_valid_month_str(x: str) -> bool:
    try:
        s = str(x).strip()
        if len(s) != 7 or s[4] != "-":
            return False
        y = int(s[:4])
        m = int(s[5:7])
        return 2020 <= y <= 2035 and 1 <= m <= 12
    except Exception:
        return False


latest_import = get_latest_cost_import_info(DB_PATH)
if latest_import:
    st.info(
        f"最近一次成本导入：{latest_import['imported_at']}（UTC） by {latest_import['imported_by'] or '-'} ｜ "
        f"文件：{latest_import['source_file'] or '-'} ｜ 行数：{latest_import['rows_written']}"
    )
else:
    st.warning("当前还没有成本数据。请先导入采购付款明细表。")

if is_admin:
    with st.expander("🛠 管理后台：采购费用导入", expanded=latest_import is None):
        up = st.file_uploader(
            "上传采购 Excel（支持：费用明细表 / 费用明细 / 项目垃圾处理费用）",
            type=["xlsx", "xls"],
            key="cost_uploader",
        )
        st.caption("当前策略：每次导入会整体替换现有成本分析底表，避免旧口径和新口径混杂。")
        if up is not None:
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(up.name)[1] or ".xlsx")
            tmp.write(up.getbuffer())
            tmp.close()
            try:
                with st.spinner("解析采购表..."):
                    df_new = parse_purchase_workbook(tmp.name)
                if df_new.empty:
                    st.error("未解析到采购费用数据。请检查 sheet 名称、表头字段是否包含：费用日期 / 费用事项 / 收款方 / 金额 / 分类 / 备注。")
                else:
                    st.success(f"解析成功：{len(df_new)} 行")
                    c1, c2, c3, c4 = st.columns(4)
                    c1.metric("最早付款日", str(df_new["expense_date"].min()))
                    c2.metric("最晚付款日", str(df_new["expense_date"].max()))
                    c3.metric("费用合计", fmt_money(df_new["amount"].sum()))
                    parsed_ratio = (df_new["date_source"] != "payment_date").mean() if len(df_new) else 0
                    c4.metric("归属日期识别率", f"{parsed_ratio*100:.0f}%")

                    st.dataframe(
                        df_new[
                            [
                                "expense_date",
                                "analysis_month",
                                "date_source",
                                "item_name",
                                "amount",
                                "category_name",
                                "source_sheet",
                            ]
                        ].head(20),
                        use_container_width=True,
                    )

                    if st.button("写入成本数据库（替换当前成本数据）", type="primary"):
                        info = replace_purchase_cost_batch(DB_PATH, df_new, imported_by=user.username, source_file=up.name)
                        st.success(f"导入完成：{info['rows_written']} 行，batch_id={info['batch_id'][:8]}")
                        st.rerun()
            finally:
                os.unlink(tmp.name)

bundle = build_cost_dashboard_dataset(DB_PATH)
cost_df = bundle["cost_df"]
purchase_df = bundle["purchase_df"]
monthly = bundle["monthly"]
level1_monthly = bundle["level1_monthly"]
monthly_with_ops = bundle["monthly_with_ops"]

# 统一把月份列转成字符串，并过滤非法月份，同时增加短标签 month_label（如 2026-02 -> 26-02）
for df_name, df_obj, col in [
    ("monthly", monthly, "analysis_month"),
    ("level1_monthly", level1_monthly, "analysis_month"),
    ("monthly_with_ops", monthly_with_ops, "analysis_month"),
]:
    if not df_obj.empty and col in df_obj.columns:
        df_obj[col] = df_obj[col].astype(str)
        df_obj = df_obj[df_obj[col].map(is_valid_month_str)].copy()
        df_obj.sort_values(col, inplace=True)
        df_obj["month_label"] = df_obj[col].str[2:]

        if df_name == "monthly":
            monthly = df_obj
        elif df_name == "level1_monthly":
            level1_monthly = df_obj
        elif df_name == "monthly_with_ops":
            monthly_with_ops = df_obj

if cost_df.empty:
    st.stop()

months = []
if not monthly_with_ops.empty and "analysis_month" in monthly_with_ops.columns:
    months = [
        str(x) for x in monthly_with_ops["analysis_month"].dropna().tolist()
        if is_valid_month_str(str(x))
    ]
elif "analysis_month" in cost_df.columns:
    months = sorted([
        str(x) for x in cost_df["analysis_month"].dropna().unique().tolist()
        if is_valid_month_str(str(x))
    ])

if not months:
    st.warning("当前没有可展示的归属月份，请重新导入采购表。")
    st.stop()

pick_month = st.sidebar.selectbox("查看归属月份", options=months, index=len(months) - 1)

month_df = cost_df[cost_df["analysis_month"].astype(str) == str(pick_month)].copy()
month_total = float(month_df["amount"].sum())

month_ops_row = monthly_with_ops[
    monthly_with_ops["analysis_month"].astype(str) == str(pick_month)
]
month_incoming_ton = (
    float(month_ops_row["incoming_ton"].iloc[0])
    if not month_ops_row.empty and pd.notna(month_ops_row["incoming_ton"].iloc[0])
    else None
)
month_ton_cost = (
    float(month_ops_row["ton_cost"].iloc[0])
    if not month_ops_row.empty and pd.notna(month_ops_row["ton_cost"].iloc[0])
    else None
)

level1_sum = (
    month_df.groupby("level1_name", as_index=False)
    .agg(total_cost=("amount", "sum"))
    .sort_values("total_cost", ascending=False)
)
category_sum = (
    month_df.groupby("category_name", as_index=False)
    .agg(total_cost=("amount", "sum"))
    .sort_values("total_cost", ascending=False)
)

c1, c2, c3, c4 = st.columns(4)
c1.metric("本月运营费用", fmt_money(month_total))
c2.metric("吨均运营成本", fmt_num(month_ton_cost, 2) if month_ton_cost is not None else "-")
c3.metric("本月处理量", fmt_num(month_incoming_ton, 2) if month_incoming_ton is not None else "-")
month_purchase_df = purchase_df[purchase_df["analysis_month"].astype(str) == str(pick_month)].copy()
c4.metric("本月费用笔数", f"{len(month_purchase_df):,}")

highlight_map = {
    "材料费用": "材料费用",
    "维修费用": "维修费用",
    "运营费用": "运营费用",
    "运输费用": "运输费用",
}
cols = st.columns(4)
for idx, key in enumerate(highlight_map.keys()):
    amt = level1_sum.loc[level1_sum["level1_name"] == key, "total_cost"]
    cols[idx].metric(highlight_map[key], fmt_money(float(amt.iloc[0])) if not amt.empty else "-")

left, right = st.columns([1.4, 1])

with left:
    fig_ton = px.line(
        monthly_with_ops,
        x="month_label",
        y="ton_cost",
        markers=True,
        title="吨均运营成本趋势（按归属月份）",
        category_orders={"month_label": monthly_with_ops["month_label"].tolist()},
    )
    fig_ton.update_layout(
        margin=dict(l=10, r=10, t=50, b=10),
        xaxis_title="月份",
        yaxis_title="元/吨",
    )
    fig_ton.update_xaxes(type="category")
    st.plotly_chart(fig_ton, use_container_width=True)

    fig_level1_trend = px.bar(
        level1_monthly,
        x="month_label",
        y="total_cost",
        color="level1_name",
        barmode="stack",
        title="一级分类月度趋势（按归属月份）",
        category_orders={"month_label": level1_monthly["month_label"].drop_duplicates().tolist()},
    )
    fig_level1_trend.update_layout(
        margin=dict(l=10, r=10, t=50, b=10),
        xaxis_title="月份",
        yaxis_title="费用",
        legend_title_text="一级分类",
    )
    fig_level1_trend.update_xaxes(type="category")
    st.plotly_chart(fig_level1_trend, use_container_width=True)

with right:
    fig_pie = px.pie(
        level1_sum,
        names="level1_name",
        values="total_cost",
        title=f"{pick_month} 一级分类占比",
    )
    fig_pie.update_layout(margin=dict(l=10, r=10, t=50, b=10))
    st.plotly_chart(fig_pie, use_container_width=True)

st.markdown("### 分类明细")
cx1, cx2 = st.columns(2)
with cx1:
    st.dataframe(
        level1_sum.rename(columns={"level1_name": "一级分类", "total_cost": "金额"}),
        use_container_width=True,
    )
with cx2:
    st.dataframe(
        category_sum.rename(columns={"category_name": "费用分类", "total_cost": "金额"}),
        use_container_width=True,
    )

st.markdown("### 原始付款明细（展示归属月份）")
detail_df = purchase_df[purchase_df["analysis_month"].astype(str) == str(pick_month)].copy()
show_cols = [
    "expense_date",
    "analysis_month",
    "date_source",
    "item_name",
    "payee",
    "amount",
    "category_name",
    "level1_name",
    "remark",
    "source_sheet",
]
show_df = detail_df[show_cols].rename(
    columns={
        "expense_date": "付款日期",
        "analysis_month": "归属月份",
        "date_source": "归属来源",
        "item_name": "费用事项",
        "payee": "收款方",
        "amount": "金额",
        "category_name": "分类",
        "level1_name": "一级分类",
        "remark": "备注",
        "source_sheet": "来源Sheet",
    }
)
st.dataframe(
    show_df.sort_values(["归属月份", "付款日期", "金额"], ascending=[False, False, False]),
    use_container_width=True,
    height=520,
)