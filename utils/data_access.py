# -*- coding: utf-8 -*-
from __future__ import annotations

import sqlite3
from typing import Iterable, Optional

import numpy as np
import pandas as pd
import streamlit as st

from utils.config import get_bucket_to_ton
from utils.snapshot import ensure_snapshot_schema, get_active_snapshot_id


OPS_NUMERIC_COLUMNS: tuple[str, ...] = (
    "incoming_trips",
    "incoming_ton",
    "slag_trips",
    "slag_ton",
    "slag_total_ton",
    "slurry_m3",
    "water_meter_m3",
    "water_m3",
    "elec_meter_x1e3kwh",
    "elec_meter_kwh",
    "proj_flow_m3",
    "to_wwtp_m3",
    "wwtp_flow_m3",
    "arrive_wwtp_m3",
    "incoming_bucket_count",
    "line1_feed_bucket_count",
    "line1_slag_bucket_count",
    "line2_feed_bucket_count",
    "line2_slag_bucket_count",
    "compress_bucket_count",
    "centrifuge_meter_m3",
    "centrifuge_feed_m3",
    "line1_runtime_hours",
    "line2_runtime_hours",
)


@st.cache_data(ttl=30)
def load_daily_ops_data(db_path: str, snapshot_id: Optional[str] = None) -> pd.DataFrame:
    ensure_snapshot_schema(db_path)
    sid = snapshot_id or get_active_snapshot_id(db_path)
    conn = sqlite3.connect(db_path)
    try:
        df = pd.read_sql_query(
            "SELECT * FROM fact_daily_ops WHERE snapshot_id=? ORDER BY date",
            conn,
            params=(sid,),
            parse_dates=["date"],
        )
    finally:
        conn.close()

    if df.empty:
        return df

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df[df["date"].notna()].copy()
        df = df.sort_values("date")

    for col in OPS_NUMERIC_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "elec_meter_kwh" not in df.columns and "elec_meter_x1e3kwh" in df.columns:
        df["elec_meter_kwh"] = df["elec_meter_x1e3kwh"] * 1000

    if "slag_total_ton" not in df.columns and "slag_ton" in df.columns:
        df["slag_total_ton"] = df["slag_ton"]

    bucket_to_ton = get_bucket_to_ton()
    if "incoming_bucket_count" in df.columns and "incoming_bucket_ton" not in df.columns:
        df["incoming_bucket_ton"] = df["incoming_bucket_count"] * bucket_to_ton
    if "compress_bucket_count" in df.columns and "compress_bucket_ton" not in df.columns:
        df["compress_bucket_ton"] = df["compress_bucket_count"] * bucket_to_ton
    if "line1_feed_bucket_count" in df.columns and "line1_feed_bucket_ton" not in df.columns:
        df["line1_feed_bucket_ton"] = df["line1_feed_bucket_count"] * bucket_to_ton
    if "line2_feed_bucket_count" in df.columns and "line2_feed_bucket_ton" not in df.columns:
        df["line2_feed_bucket_ton"] = df["line2_feed_bucket_count"] * bucket_to_ton
    if "line1_slag_bucket_count" in df.columns and "line1_slag_bucket_ton" not in df.columns:
        df["line1_slag_bucket_ton"] = df["line1_slag_bucket_count"] * bucket_to_ton
    if "line2_slag_bucket_count" in df.columns and "line2_slag_bucket_ton" not in df.columns:
        df["line2_slag_bucket_ton"] = df["line2_slag_bucket_count"] * bucket_to_ton

    return df


def filter_df_by_date_range(df: pd.DataFrame, start_date, end_date, date_col: str = "date") -> pd.DataFrame:
    if df.empty or date_col not in df.columns:
        return df.copy()
    out = df.copy()
    s = pd.Timestamp(start_date)
    e = pd.Timestamp(end_date)
    return out[(out[date_col] >= s) & (out[date_col] <= e)].copy()


def add_daily_electricity(df: pd.DataFrame, meter_col: str = "elec_meter_kwh") -> pd.DataFrame:
    """电表读数为抄表点：相邻抄表差分 -> 均摊到区间内每一天。"""
    out = df.sort_values("date").copy()
    out["daily_elec_kwh"] = np.nan

    if meter_col not in out.columns or out.empty:
        return out

    meter = out[["date", meter_col]].dropna().drop_duplicates("date").sort_values("date")
    if len(meter) < 2:
        return out

    for (d0, v0), (d1, v1) in zip(meter[["date", meter_col]].values[:-1], meter[["date", meter_col]].values[1:]):
        d0 = pd.Timestamp(d0)
        d1 = pd.Timestamp(d1)
        days = (d1 - d0).days
        if days <= 0:
            continue
        try:
            delta = float(v1) - float(v0)
        except Exception:
            continue
        if delta < 0:
            continue
        per_day = delta / days
        m = (out["date"] > d0) & (out["date"] <= d1)
        out.loc[m, "daily_elec_kwh"] = per_day
    return out


def safe_div(numerator, denominator):
    if denominator is None or denominator == 0 or pd.isna(denominator):
        return np.nan
    if numerator is None or pd.isna(numerator):
        return np.nan
    return numerator / denominator


def get_valid_production_records(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    incoming = df["incoming_ton"].fillna(0) if "incoming_ton" in df.columns else 0
    slag = df["slag_ton"].fillna(0) if "slag_ton" in df.columns else 0
    bucket = df["incoming_bucket_count"].fillna(0) if "incoming_bucket_count" in df.columns else 0
    centrifuge = df["centrifuge_feed_m3"].fillna(0) if "centrifuge_feed_m3" in df.columns else 0
    return df[(incoming > 0) | (slag > 0) | (bucket > 0) | (centrifuge > 0)].copy()


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


def normalize_month_column(df: pd.DataFrame, column: str = "analysis_month") -> pd.DataFrame:
    if df.empty or column not in df.columns:
        return df.copy()
    out = df.copy()
    out[column] = out[column].astype(str)
    out = out[out[column].map(is_valid_month_str)].copy()
    out = out.sort_values(column)
    out["month_label"] = out[column].str[2:]
    return out


def default_date_window(df: pd.DataFrame, date_col: str = "date"):
    if df.empty or date_col not in df.columns:
        today = pd.Timestamp.today().normalize()
        return today.date(), today.date()
    return df[date_col].min().date(), df[date_col].max().date()


def sum_columns(df: pd.DataFrame, columns: Iterable[str]) -> dict[str, float]:
    out: dict[str, float] = {}
    for col in columns:
        out[col] = float(df[col].sum(skipna=True)) if col in df.columns else 0.0
    return out
