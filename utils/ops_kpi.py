# -*- coding: utf-8 -*-
from __future__ import annotations

import sqlite3
from datetime import datetime
from typing import Optional, Dict

import numpy as np
import pandas as pd

from utils.snapshot import ensure_snapshot_schema, get_active_snapshot_id


def load_daily_ops_frame(db_path: str) -> pd.DataFrame:
    ensure_snapshot_schema(db_path)
    snapshot_id = get_active_snapshot_id(db_path)

    conn = sqlite3.connect(db_path)
    try:
        df = pd.read_sql_query(
            """
            SELECT
                date,
                incoming_ton,
                slag_ton
            FROM fact_daily_ops
            WHERE snapshot_id = ?
            ORDER BY date
            """,
            conn,
            params=(snapshot_id,),
        )
    finally:
        conn.close()

    if df.empty:
        return df

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["incoming_ton"] = pd.to_numeric(df["incoming_ton"], errors="coerce").fillna(0.0)
    df["slag_ton"] = pd.to_numeric(df["slag_ton"], errors="coerce").fillna(0.0)

    df["slag_ratio"] = np.where(
        df["incoming_ton"] > 0,
        df["slag_ton"] / df["incoming_ton"],
        np.nan,
    )
    return df


def get_latest_ops_kpis(db_path: str) -> Optional[Dict]:
    df = load_daily_ops_frame(db_path)
    if df.empty:
        return None

    df = df.dropna(subset=["date"]).sort_values("date")
    if df.empty:
        return None

    # 只取“最近有效生产记录”，避免把月底空白占位行当成最近数据
    valid_df = df[
        (df["incoming_ton"].fillna(0) > 0) |
        (df["slag_ton"].fillna(0) > 0)
    ].copy()

    if valid_df.empty:
        return None

    latest = valid_df.iloc[-1]

    latest_date = latest["date"]
    incoming_ton = float(latest["incoming_ton"]) if pd.notna(latest["incoming_ton"]) else None
    slag_ton = float(latest["slag_ton"]) if pd.notna(latest["slag_ton"]) else None
    slag_ratio = float(latest["slag_ratio"]) if pd.notna(latest["slag_ratio"]) else None

    days_lag = None
    if pd.notna(latest_date):
        today = pd.Timestamp(datetime.today().date())
        days_lag = int((today - pd.Timestamp(latest_date.date())).days)

    return {
        "date": latest_date,
        "incoming_ton": incoming_ton,
        "slag_ton": slag_ton,
        "slag_ratio": slag_ratio,
        "days_lag": days_lag,
    }


def get_recent_ops_trend(db_path: str, days: int = 7) -> pd.DataFrame:
    df = load_daily_ops_frame(db_path)
    if df.empty:
        return df

    df = df.dropna(subset=["date"]).sort_values("date")
    if df.empty:
        return df

    # 只保留有效生产记录，避免月底空白占位行进入趋势图
    valid_df = df[
        (df["incoming_ton"].fillna(0) > 0) |
        (df["slag_ton"].fillna(0) > 0)
    ].copy()

    if valid_df.empty:
        return valid_df

    return valid_df.tail(days).copy()


def classify_data_freshness(days_lag: Optional[int]) -> str:
    if days_lag is None:
        return "未知"
    if days_lag <= 1:
        return "正常"
    if days_lag <= 3:
        return "轻微延迟"
    return "更新延迟"


def classify_slag_ratio(ratio: Optional[float]) -> str:
    if ratio is None or pd.isna(ratio):
        return "—"
    if 0.18 <= ratio <= 0.24:
        return "正常"
    if 0.24 < ratio <= 0.28:
        return "偏高"
    if 0.12 <= ratio < 0.18:
        return "偏低"
    return "异常"
