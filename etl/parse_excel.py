# -*- coding: utf-8 -*-
from __future__ import annotations

import pandas as pd
import numpy as np
from typing import Dict, List, Optional

EXCEL_EPOCH = pd.Timestamp("1899-12-30")  # pandas 的 Excel 序列号基准（兼容多数表）


def _excel_date_to_ts(x) -> pd.Timestamp | pd.NaT:
    """把 Excel 序列号（例如 45992）转成日期。"""
    if pd.isna(x):
        return pd.NaT
    # 有些情况下“日期”已经是 datetime/date
    if isinstance(x, (pd.Timestamp, )):
        return x.normalize()
    # python datetime/date
    if hasattr(x, "year") and hasattr(x, "month") and hasattr(x, "day"):
        return pd.Timestamp(x).normalize()

    try:
        v = float(x)
        # 45992 这类
        return (EXCEL_EPOCH + pd.to_timedelta(int(v), unit="D")).normalize()
    except Exception:
        return pd.NaT


def _clean_columns(cols: List) -> List[str]:
    out = []
    for c in cols:
        if c is None:
            out.append("")
            continue
        s = str(c)
        s = s.replace("\n", "").strip()
        out.append(s)
    return out


def parse_workbook(xlsx_path: str, sheet_names: Optional[List[str]] = None) -> pd.DataFrame:
    """
    读取你这类“月度运营表”（每个 sheet 一个月），规范化成一张按天的事实表。
    兼容 2025.11/12 与 2026.1/2 字段差异：缺的字段填 NaN。
    """
    xl = pd.ExcelFile(xlsx_path)
    sheets = sheet_names or xl.sheet_names

    frames = []
    for sh in sheets:
        df = pd.read_excel(xlsx_path, sheet_name=sh, header=2)
        df.columns = _clean_columns(list(df.columns))

        # 过滤掉没有日期的行（合计/空行/标题行）
        if "日期" not in df.columns:
            continue
        df = df[df["日期"].notna()].copy()

        df["date"] = df["日期"].apply(_excel_date_to_ts)
        df = df[df["date"].notna()].copy()

        def pick(col: str) -> pd.Series:
            return df[col] if col in df.columns else pd.Series([np.nan] * len(df), index=df.index)

        out = pd.DataFrame({
            "date": df["date"],
            "incoming_trips": pick("来料(车)"),
            "incoming_ton": pick("来料(吨)"),
            "slag_trips": pick("出渣(车)"),
            "slag_ton": pick("出渣(吨)"),
            # 11/12 有“出渣合计(吨)”，1/2 没有则用出渣吨
            "slag_total_ton": pick("出渣合计(吨)"),
            "slurry_m3": pick("制桨量m3"),
            "water_meter_m3": pick("水表读数m3"),
            "water_m3": pick("用水量m3"),
            "elec_meter_x1e3kwh": pick("电表读数X103kw*h"),
            "proj_flow_m3": pick("项目流量计m3"),
            "to_wwtp_m3": pick("去水厂的浆料m3"),
            "wwtp_flow_m3": pick("水厂流量计m3"),
            "arrive_wwtp_m3": pick("到水厂的浆料m3"),
        })

        # 列名在不同 sheet 里可能是 “制桨量\nm3” 这类，前面已经去掉换行；但仍可能存在空格差异
        # 再兜底一次：如果主列全空但存在相近列名，就补上
        def fallback(dst: str, candidates: List[str]):
            if out[dst].notna().any():
                return
            for c in candidates:
                if c in df.columns:
                    out[dst] = df[c]
                    return

        fallback("slurry_m3", ["制桨量m3", "制浆量m3", "制桨量 m3", "制浆量 m3"])
        fallback("water_meter_m3", ["水表读数m3", "水表读数 m3"])
        fallback("water_m3", ["用水量m3", "用水量 m3"])
        fallback("elec_meter_x1e3kwh", ["电表读数X103kw*h", "电表读数 X103kw*h", "电表读数X10^3kw*h"])

        # 出渣合计：若没有则用出渣吨
        if out["slag_total_ton"].isna().all():
            out["slag_total_ton"] = out["slag_ton"]

        # 电表换算到 kWh：表头显示 X10^3 kWh
        out["elec_meter_kwh"] = out["elec_meter_x1e3kwh"] * 1000.0

        out["source_sheet"] = sh
        frames.append(out)

    if not frames:
        return pd.DataFrame()

    all_df = pd.concat(frames, ignore_index=True)

    # 统一数值列为 numeric
    num_cols = [c for c in all_df.columns if c not in ("date", "source_sheet")]
    for c in num_cols:
        all_df[c] = pd.to_numeric(all_df[c], errors="coerce")

    all_df = all_df.sort_values("date").reset_index(drop=True)
    return all_df
