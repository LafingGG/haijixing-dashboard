# -*- coding: utf-8 -*-
from __future__ import annotations

import numpy as np
import pandas as pd

from utils.data_access import add_daily_electricity, safe_div


def prepare_ops_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """统一生成运行分析常用指标。"""
    if df is None or df.empty:
        return pd.DataFrame()

    out = df.copy()
    if "date" in out.columns:
        out["date"] = pd.to_datetime(out["date"], errors="coerce")
        out = out[out["date"].notna()].sort_values("date").copy()

    out = add_daily_electricity(out)

    if "slag_total_ton" not in out.columns and "slag_ton" in out.columns:
        out["slag_total_ton"] = out["slag_ton"]

    # 出渣率
    incoming = out["incoming_ton"] if "incoming_ton" in out.columns else np.nan
    slag_total = out["slag_total_ton"] if "slag_total_ton" in out.columns else np.nan
    out["slag_rate"] = np.where(
        pd.to_numeric(incoming, errors="coerce").fillna(0) > 0,
        pd.to_numeric(slag_total, errors="coerce") / pd.to_numeric(incoming, errors="coerce"),
        np.nan,
    )

    # 单吨水耗
    if "water_m3" in out.columns and "incoming_ton" in out.columns:
        out["water_per_ton"] = np.where(
            out["incoming_ton"].fillna(0) > 0,
            out["water_m3"] / out["incoming_ton"],
            np.nan,
        )
    else:
        out["water_per_ton"] = np.nan

    # 单吨电耗
    if "daily_elec_kwh" in out.columns and "incoming_ton" in out.columns:
        out["elec_per_ton"] = np.where(
            out["incoming_ton"].fillna(0) > 0,
            out["daily_elec_kwh"] / out["incoming_ton"],
            np.nan,
        )
    else:
        out["elec_per_ton"] = np.nan

    # 水厂相关差异
    if "to_wwtp_m3" in out.columns and "arrive_wwtp_m3" in out.columns:
        out["wwtp_gap_m3"] = out["to_wwtp_m3"] - out["arrive_wwtp_m3"]
    else:
        out["wwtp_gap_m3"] = np.nan

    return out


def summarize_ops_period(df: pd.DataFrame) -> dict:
    if df is None or df.empty:
        return {
            "days": 0,
            "incoming_ton": 0.0,
            "slag_total_ton": 0.0,
            "avg_slag_rate": np.nan,
            "water_m3": 0.0,
            "avg_water_per_ton": np.nan,
            "daily_elec_kwh": 0.0,
            "avg_elec_per_ton": np.nan,
        }

    incoming_ton = float(df["incoming_ton"].sum(skipna=True)) if "incoming_ton" in df.columns else 0.0
    slag_total_ton = float(df["slag_total_ton"].sum(skipna=True)) if "slag_total_ton" in df.columns else 0.0
    water_m3 = float(df["water_m3"].sum(skipna=True)) if "water_m3" in df.columns else 0.0
    elec_kwh = float(df["daily_elec_kwh"].sum(skipna=True)) if "daily_elec_kwh" in df.columns else 0.0

    return {
        "days": int(df["date"].nunique()) if "date" in df.columns else len(df),
        "incoming_ton": incoming_ton,
        "slag_total_ton": slag_total_ton,
        "avg_slag_rate": safe_div(slag_total_ton, incoming_ton),
        "water_m3": water_m3,
        "avg_water_per_ton": safe_div(water_m3, incoming_ton),
        "daily_elec_kwh": elec_kwh,
        "avg_elec_per_ton": safe_div(elec_kwh, incoming_ton),
    }


def judge_process_stability(df: pd.DataFrame) -> tuple[str, list[str]]:
    """
    给运行分析页一个简单、可解释的稳定性判断。
    """
    if df is None or df.empty:
        return "无数据", ["当前时间范围内没有有效运行数据"]

    issues: list[str] = []

    valid_days = int(df["date"].nunique()) if "date" in df.columns else len(df)
    if valid_days < 5:
        issues.append("有效运行天数较少，判断参考性有限")

    if "slag_rate" in df.columns:
        sr = df["slag_rate"].dropna()
        if len(sr) >= 5:
            if sr.std() > 0.08:
                issues.append("出渣率波动较大")
            if (sr > 0.5).sum() >= 2:
                issues.append("存在较高出渣率记录，建议复核工艺与记录口径")

    if "water_per_ton" in df.columns:
        wp = df["water_per_ton"].dropna()
        if len(wp) >= 5 and wp.std() > 0.8:
            issues.append("单吨水耗波动较大")

    if "elec_per_ton" in df.columns:
        ep = df["elec_per_ton"].dropna()
        if len(ep) >= 5 and ep.std() > 15:
            issues.append("单吨电耗波动较大")

    if len(issues) == 0:
        return "稳定", ["主要运行指标波动处于可接受范围"]
    if len(issues) <= 2:
        return "基本稳定", issues
    return "需关注", issues


def build_monthly_ops_summary(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    out = df.copy()
    out["month"] = out["date"].dt.strftime("%Y-%m")

    agg = out.groupby("month", as_index=False).agg(
        incoming_ton=("incoming_ton", "sum"),
        slag_total_ton=("slag_total_ton", "sum"),
        water_m3=("water_m3", "sum"),
        daily_elec_kwh=("daily_elec_kwh", "sum"),
        days=("date", "nunique"),
    )

    agg["slag_rate"] = np.where(agg["incoming_ton"] > 0, agg["slag_total_ton"] / agg["incoming_ton"], np.nan)
    agg["water_per_ton"] = np.where(agg["incoming_ton"] > 0, agg["water_m3"] / agg["incoming_ton"], np.nan)
    agg["elec_per_ton"] = np.where(agg["incoming_ton"] > 0, agg["daily_elec_kwh"] / agg["incoming_ton"], np.nan)
    agg["month_label"] = agg["month"].str[2:]
    return agg.sort_values("month").reset_index(drop=True)