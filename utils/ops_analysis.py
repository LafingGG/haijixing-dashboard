# -*- coding: utf-8 -*-
from __future__ import annotations

import numpy as np
import pandas as pd

from utils.config import get_bucket_to_ton
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
    bucket_to_ton = get_bucket_to_ton()

    if "slag_total_ton" not in out.columns and "slag_ton" in out.columns:
        out["slag_total_ton"] = out["slag_ton"]

    for col in [
        "incoming_bucket_count",
        "line1_feed_bucket_count",
        "line1_slag_bucket_count",
        "line2_feed_bucket_count",
        "line2_slag_bucket_count",
        "compress_bucket_count",
        "centrifuge_feed_m3",
        "line1_runtime_hours",
        "line2_runtime_hours",
    ]:
        if col not in out.columns:
            out[col] = np.nan

    out["incoming_bucket_ton"] = out["incoming_bucket_count"] * bucket_to_ton
    out["line1_feed_bucket_ton"] = out["line1_feed_bucket_count"] * bucket_to_ton
    out["line1_slag_bucket_ton"] = out["line1_slag_bucket_count"] * bucket_to_ton
    out["line2_feed_bucket_ton"] = out["line2_feed_bucket_count"] * bucket_to_ton
    out["line2_slag_bucket_ton"] = out["line2_slag_bucket_count"] * bucket_to_ton
    out["compress_bucket_ton"] = out["compress_bucket_count"] * bucket_to_ton

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

    # 真实浆料产出相关
    out["actual_slurry_m3"] = out["centrifuge_feed_m3"].where(out["centrifuge_feed_m3"].fillna(0) > 0, out.get("slurry_m3", np.nan))
    out["slurry_per_ton"] = np.where(
        out["incoming_ton"].fillna(0) > 0,
        out["actual_slurry_m3"] / out["incoming_ton"],
        np.nan,
    )
    out["slurry_per_bucket"] = np.where(
        out["incoming_bucket_count"].fillna(0) > 0,
        out["actual_slurry_m3"] / out["incoming_bucket_count"],
        np.nan,
    )

    # 双线效率
    out["line1_feed_tph"] = np.where(
        out["line1_runtime_hours"].fillna(0) > 0,
        out["line1_feed_bucket_ton"] / out["line1_runtime_hours"],
        np.nan,
    )
    out["line2_feed_tph"] = np.where(
        out["line2_runtime_hours"].fillna(0) > 0,
        out["line2_feed_bucket_ton"] / out["line2_runtime_hours"],
        np.nan,
    )
    out["line1_feed_bucket_per_hour"] = np.where(
        out["line1_runtime_hours"].fillna(0) > 0,
        out["line1_feed_bucket_count"] / out["line1_runtime_hours"],
        np.nan,
    )
    out["line2_feed_bucket_per_hour"] = np.where(
        out["line2_runtime_hours"].fillna(0) > 0,
        out["line2_feed_bucket_count"] / out["line2_runtime_hours"],
        np.nan,
    )
    out["line1_slag_rate_by_bucket"] = np.where(
        out["line1_feed_bucket_count"].fillna(0) > 0,
        out["line1_slag_bucket_count"] / out["line1_feed_bucket_count"],
        np.nan,
    )
    out["line2_slag_rate_by_bucket"] = np.where(
        out["line2_feed_bucket_count"].fillna(0) > 0,
        out["line2_slag_bucket_count"] / out["line2_feed_bucket_count"],
        np.nan,
    )

    # 压缩箱预警
    compress_diff = out["compress_bucket_count"].diff()
    out["compress_bucket_diff"] = compress_diff
    out["compress_warning"] = compress_diff >= 10

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
            "incoming_bucket_count": 0.0,
            "avg_bucket_ton": np.nan,
            "actual_slurry_m3": 0.0,
            "avg_slurry_per_ton": np.nan,
            "line1_feed_bucket_count": 0.0,
            "line2_feed_bucket_count": 0.0,
            "line1_avg_tph": np.nan,
            "line2_avg_tph": np.nan,
        }

    incoming_ton = float(df["incoming_ton"].sum(skipna=True)) if "incoming_ton" in df.columns else 0.0
    slag_total_ton = float(df["slag_total_ton"].sum(skipna=True)) if "slag_total_ton" in df.columns else 0.0
    water_m3 = float(df["water_m3"].sum(skipna=True)) if "water_m3" in df.columns else 0.0
    elec_kwh = float(df["daily_elec_kwh"].sum(skipna=True)) if "daily_elec_kwh" in df.columns else 0.0
    incoming_bucket_count = float(df["incoming_bucket_count"].sum(skipna=True)) if "incoming_bucket_count" in df.columns else 0.0
    actual_slurry_m3 = float(df["actual_slurry_m3"].sum(skipna=True)) if "actual_slurry_m3" in df.columns else 0.0
    line1_feed_bucket_count = float(df["line1_feed_bucket_count"].sum(skipna=True)) if "line1_feed_bucket_count" in df.columns else 0.0
    line2_feed_bucket_count = float(df["line2_feed_bucket_count"].sum(skipna=True)) if "line2_feed_bucket_count" in df.columns else 0.0
    line1_feed_ton = float(df["line1_feed_bucket_ton"].sum(skipna=True)) if "line1_feed_bucket_ton" in df.columns else 0.0
    line2_feed_ton = float(df["line2_feed_bucket_ton"].sum(skipna=True)) if "line2_feed_bucket_ton" in df.columns else 0.0
    line1_runtime_hours = float(df["line1_runtime_hours"].sum(skipna=True)) if "line1_runtime_hours" in df.columns else 0.0
    line2_runtime_hours = float(df["line2_runtime_hours"].sum(skipna=True)) if "line2_runtime_hours" in df.columns else 0.0

    return {
        "days": int(df["date"].nunique()) if "date" in df.columns else len(df),
        "incoming_ton": incoming_ton,
        "slag_total_ton": slag_total_ton,
        "avg_slag_rate": safe_div(slag_total_ton, incoming_ton),
        "water_m3": water_m3,
        "avg_water_per_ton": safe_div(water_m3, incoming_ton),
        "daily_elec_kwh": elec_kwh,
        "avg_elec_per_ton": safe_div(elec_kwh, incoming_ton),
        "incoming_bucket_count": incoming_bucket_count,
        "avg_bucket_ton": safe_div(incoming_ton, incoming_bucket_count),
        "actual_slurry_m3": actual_slurry_m3,
        "avg_slurry_per_ton": safe_div(actual_slurry_m3, incoming_ton),
        "line1_feed_bucket_count": line1_feed_bucket_count,
        "line2_feed_bucket_count": line2_feed_bucket_count,
        "line1_avg_tph": safe_div(line1_feed_ton, line1_runtime_hours),
        "line2_avg_tph": safe_div(line2_feed_ton, line2_runtime_hours),
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

    if "line1_feed_tph" in df.columns and "line2_feed_tph" in df.columns:
        l1 = df["line1_feed_tph"].dropna()
        l2 = df["line2_feed_tph"].dropna()
        if len(l1) >= 3 and len(l2) >= 3:
            diff = abs(l1.mean() - l2.mean())
            if diff > 1.5:
                issues.append("两条线效率差异较大，建议排查设备或操作差异")

    if "compress_warning" in df.columns and df["compress_warning"].fillna(False).sum() >= 2:
        issues.append("压缩箱存在连续积压迹象")

    if len(issues) == 0:
        return "稳定", ["主要运行指标波动处于可接受范围"]
    if len(issues) <= 2:
        return "基本稳定", issues
    return "需关注", issues


def build_monthly_ops_summary(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    out = df.copy()
    out["month"] = pd.to_datetime(out["date"]).dt.to_period("M").astype(str)
    grouped = (
        out.groupby("month", dropna=False)
        .agg(
            incoming_ton=("incoming_ton", "sum"),
            slag_total_ton=("slag_total_ton", "sum"),
            water_m3=("water_m3", "sum"),
            daily_elec_kwh=("daily_elec_kwh", "sum"),
            incoming_bucket_count=("incoming_bucket_count", "sum"),
            actual_slurry_m3=("actual_slurry_m3", "sum"),
            line1_feed_bucket_count=("line1_feed_bucket_count", "sum"),
            line2_feed_bucket_count=("line2_feed_bucket_count", "sum"),
            line1_feed_bucket_ton=("line1_feed_bucket_ton", "sum"),
            line2_feed_bucket_ton=("line2_feed_bucket_ton", "sum"),
            line1_runtime_hours=("line1_runtime_hours", "sum"),
            line2_runtime_hours=("line2_runtime_hours", "sum"),
        )
        .reset_index()
    )
    grouped["slag_rate"] = grouped.apply(lambda r: safe_div(r["slag_total_ton"], r["incoming_ton"]), axis=1)
    grouped["water_per_ton"] = grouped.apply(lambda r: safe_div(r["water_m3"], r["incoming_ton"]), axis=1)
    grouped["elec_per_ton"] = grouped.apply(lambda r: safe_div(r["daily_elec_kwh"], r["incoming_ton"]), axis=1)
    grouped["slurry_per_ton"] = grouped.apply(lambda r: safe_div(r["actual_slurry_m3"], r["incoming_ton"]), axis=1)
    grouped["line1_feed_tph"] = grouped.apply(lambda r: safe_div(r["line1_feed_bucket_ton"], r["line1_runtime_hours"]), axis=1)
    grouped["line2_feed_tph"] = grouped.apply(lambda r: safe_div(r["line2_feed_bucket_ton"], r["line2_runtime_hours"]), axis=1)
    grouped["month_label"] = pd.to_datetime(grouped["month"] + "-01").dt.strftime("%y-%m")
    return grouped
