# -*- coding: utf-8 -*-
from __future__ import annotations

import sqlite3
from typing import Optional

import numpy as np
import pandas as pd
import streamlit as st

from utils.data_access import is_valid_month_str, normalize_month_column, safe_div


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    cur = conn.cursor()
    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    )
    return cur.fetchone() is not None


def _pick_first_existing(df: pd.DataFrame, candidates: list[str]) -> Optional[str]:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _normalize_cost_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    out = df.copy()

    # 统一列名映射
    month_col = _pick_first_existing(out, [
        "analysis_month", "cost_month", "month", "stat_month", "归属月份", "所属月份"
    ])
    amount_col = _pick_first_existing(out, [
        "allocated_amount", "amount", "cost_amount", "total_amount", "金额", "费用金额"
    ])
    category_col = _pick_first_existing(out, [
        "category_level1", "category", "cost_category", "一级分类", "分类", "运营分类"
    ])
    pay_date_col = _pick_first_existing(out, [
        "pay_date", "expense_date", "date", "付款日期", "费用日期"
    ])
    biz_start_col = _pick_first_existing(out, [
        "biz_date_start", "start_date", "业务开始日期", "开始日期"
    ])
    biz_end_col = _pick_first_existing(out, [
        "biz_date_end", "end_date", "业务结束日期", "结束日期"
    ])
    item_col = _pick_first_existing(out, [
        "item_name", "expense_item", "item", "事项", "费用事项"
    ])
    vendor_col = _pick_first_existing(out, [
        "vendor_name", "vendor", "supplier", "收款方", "供应商"
    ])
    remark_col = _pick_first_existing(out, [
        "remark", "notes", "note", "备注"
    ])

    rename_map = {}
    if month_col:
        rename_map[month_col] = "analysis_month"
    if amount_col:
        rename_map[amount_col] = "amount"
    if category_col:
        rename_map[category_col] = "category_level1"
    if pay_date_col:
        rename_map[pay_date_col] = "pay_date"
    if biz_start_col:
        rename_map[biz_start_col] = "biz_date_start"
    if biz_end_col:
        rename_map[biz_end_col] = "biz_date_end"
    if item_col:
        rename_map[item_col] = "item_name"
    if vendor_col:
        rename_map[vendor_col] = "vendor_name"
    if remark_col:
        rename_map[remark_col] = "remark"

    out = out.rename(columns=rename_map)

    # 类型处理
    for col in ["amount"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")

    for col in ["pay_date", "biz_date_start", "biz_date_end"]:
        if col in out.columns:
            out[col] = pd.to_datetime(out[col], errors="coerce")

    # 生成 allocated_amount
    if "allocated_amount" not in out.columns:
        out["allocated_amount"] = out["amount"] if "amount" in out.columns else np.nan
    out["allocated_amount"] = pd.to_numeric(out["allocated_amount"], errors="coerce")

    # 生成 analysis_month
    if "analysis_month" not in out.columns or out["analysis_month"].isna().all():
        if "biz_date_start" in out.columns and out["biz_date_start"].notna().any():
            out["analysis_month"] = out["biz_date_start"].dt.strftime("%Y-%m")
        elif "pay_date" in out.columns and out["pay_date"].notna().any():
            out["analysis_month"] = out["pay_date"].dt.strftime("%Y-%m")
        else:
            out["analysis_month"] = np.nan
    else:
        # 尽量规范成 YYYY-MM
        out["analysis_month"] = out["analysis_month"].astype(str).str.strip()

        def _norm_month(x: str) -> str:
            try:
                x = str(x).strip()
                if len(x) == 7 and x[4] == "-":
                    return x
                dt = pd.to_datetime(x, errors="coerce")
                if pd.notna(dt):
                    return dt.strftime("%Y-%m")
                return x
            except Exception:
                return x

        out["analysis_month"] = out["analysis_month"].map(_norm_month)

    if "category_level1" not in out.columns:
        out["category_level1"] = "未分类"
    out["category_level1"] = out["category_level1"].fillna("未分类").astype(str)

    keep_cols = [
        c for c in [
            "analysis_month",
            "pay_date",
            "biz_date_start",
            "biz_date_end",
            "item_name",
            "vendor_name",
            "category_level1",
            "amount",
            "allocated_amount",
            "remark",
        ] if c in out.columns
    ]
    out = out[keep_cols].copy()

    if "analysis_month" in out.columns:
        out = out[out["analysis_month"].map(is_valid_month_str)].copy()

    return out


@st.cache_data(ttl=30)
def load_cost_detail_data(db_path: str, snapshot_id: Optional[str] = None) -> pd.DataFrame:
    conn = sqlite3.connect(db_path)
    try:
        # 优先读取已按运营归属整理好的表
        if _table_exists(conn, "fact_operational_cost"):
            df = pd.read_sql_query("SELECT * FROM fact_operational_cost", conn)
            df = _normalize_cost_columns(df)
            if not df.empty:
                return df

        # 兜底读取采购费用表
        if _table_exists(conn, "fact_purchase_expense"):
            df = pd.read_sql_query("SELECT * FROM fact_purchase_expense", conn)
            df = _normalize_cost_columns(df)
            if not df.empty:
                return df

        return pd.DataFrame()
    finally:
        conn.close()


def build_monthly_cost_summary(detail_df: pd.DataFrame) -> pd.DataFrame:
    if detail_df is None or detail_df.empty or "analysis_month" not in detail_df.columns:
        return pd.DataFrame()

    amount_col = "allocated_amount" if "allocated_amount" in detail_df.columns else "amount"

    out = (
        detail_df.groupby(["analysis_month", "category_level1"], dropna=False, as_index=False)[amount_col]
        .sum()
        .rename(columns={amount_col: "amount"})
    )

    out["category_level1"] = out["category_level1"].fillna("未分类")
    out = normalize_month_column(out, "analysis_month")
    return out


def build_month_total_cost(monthly_df: pd.DataFrame) -> pd.DataFrame:
    if monthly_df is None or monthly_df.empty:
        return pd.DataFrame()

    out = monthly_df.groupby("analysis_month", as_index=False)["amount"].sum()
    out = normalize_month_column(out, "analysis_month")
    return out


def attach_monthly_ton_cost(month_total_df: pd.DataFrame, ops_monthly_df: pd.DataFrame) -> pd.DataFrame:
    if month_total_df is None or month_total_df.empty:
        return pd.DataFrame()

    if ops_monthly_df is None or ops_monthly_df.empty:
        out = month_total_df.copy()
        out["incoming_ton"] = np.nan
        out["cost_per_ton"] = np.nan
        return out

    ops = ops_monthly_df.copy().rename(columns={"month": "analysis_month"})
    keep_cols = [c for c in ["analysis_month", "incoming_ton"] if c in ops.columns]
    ops = ops[keep_cols].copy()

    out = month_total_df.merge(ops, on="analysis_month", how="left")
    out["cost_per_ton"] = out.apply(
        lambda r: safe_div(r["amount"], r["incoming_ton"]) if "incoming_ton" in out.columns else np.nan,
        axis=1,
    )
    return out


def get_latest_month(month_total_df: pd.DataFrame) -> Optional[str]:
    if month_total_df is None or month_total_df.empty or "analysis_month" not in month_total_df.columns:
        return None
    vals = [x for x in month_total_df["analysis_month"].dropna().astype(str).tolist() if is_valid_month_str(x)]
    return max(vals) if vals else None


def build_current_month_category_table(monthly_df: pd.DataFrame, month: str) -> pd.DataFrame:
    if monthly_df is None or monthly_df.empty:
        return pd.DataFrame()

    cur = monthly_df[monthly_df["analysis_month"] == month].copy()
    if cur.empty:
        return cur

    total = cur["amount"].sum()
    cur["amount_share"] = np.where(total > 0, cur["amount"] / total, np.nan)
    cur = cur.sort_values("amount", ascending=False).reset_index(drop=True)
    return cur
