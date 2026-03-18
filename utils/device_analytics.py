# -*- coding: utf-8 -*-
from __future__ import annotations

import re
from datetime import datetime
from typing import List, Optional

import numpy as np
import pandas as pd

from utils.device_store import get_published_device_excel_path


def _find_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    cols = list(df.columns)
    for c in candidates:
        if c in cols:
            return c
    for c in cols:
        s = str(c)
        for cand in candidates:
            if cand in s:
                return c
    return None


def _as_text(x) -> str:
    if pd.isna(x):
        return ""
    return str(x).strip()


def _read_excel_safely(xlsx_path: str, sheet_name: str) -> pd.DataFrame:
    try:
        return pd.read_excel(xlsx_path, sheet_name=sheet_name)
    except Exception:
        return pd.DataFrame()


def _parse_duration_to_hours(x) -> float:
    """
    把停机时长统一解析成小时。
    支持：
    - 2 / 2.5
    - 2小时 / 2.5小时
    - 120分钟
    - 1小时30分
    - 1h / 1.5h
    - 0:30 / 1:20 / 02:15:00
    - Excel time / timedelta
    """
    if pd.isna(x):
        return np.nan

    if isinstance(x, pd.Timedelta):
        return x.total_seconds() / 3600.0

    if isinstance(x, datetime):
        return x.hour + x.minute / 60.0 + x.second / 3600.0

    if isinstance(x, pd.Timestamp):
        return x.hour + x.minute / 60.0 + x.second / 3600.0

    if isinstance(x, (int, float, np.number)):
        v = float(x)
        # Excel 中时间常常存成“天的小数”
        if 0 <= v < 1:
            return v * 24.0
        return v

    s = str(x).strip()
    if not s:
        return np.nan

    # HH:MM 或 HH:MM:SS
    m = re.match(r"^(\d{1,2}):(\d{1,2})(?::(\d{1,2}))?$", s)
    if m:
        hh = int(m.group(1))
        mm = int(m.group(2))
        ss = int(m.group(3) or 0)
        return hh + mm / 60.0 + ss / 3600.0

    try:
        return float(s)
    except Exception:
        pass

    m = re.search(r"(\d+)\s*小时\s*(\d+)\s*分", s)
    if m:
        return float(m.group(1)) + float(m.group(2)) / 60.0

    m = re.search(r"(\d+(?:\.\d+)?)\s*小时", s)
    if m:
        hours = float(m.group(1))
        m2 = re.search(r"(\d+(?:\.\d+)?)\s*分", s)
        if m2:
            hours += float(m2.group(1)) / 60.0
        return hours

    m = re.search(r"(\d+(?:\.\d+)?)\s*h(?:ours?)?$", s, re.I)
    if m:
        return float(m.group(1))

    m = re.search(r"(\d+(?:\.\d+)?)\s*分钟", s)
    if m:
        return float(m.group(1)) / 60.0

    m = re.search(r"(\d+(?:\.\d+)?)\s*分$", s)
    if m:
        return float(m.group(1)) / 60.0

    return np.nan


def _load_equipment_base(xlsx_path: str) -> pd.DataFrame:
    xl = pd.ExcelFile(xlsx_path)

    equip_frames = []
    for sh in ["预处理设备", "水解酸化设备", "除臭设备", "车间基础", "车间基础整改"]:
        if sh not in xl.sheet_names:
            continue

        df = _read_excel_safely(xlsx_path, sh)
        if df.empty:
            continue

        id_col = _find_col(df, ["设备id", "设备ID", "设备编号", "equipment_id", "ID"])
        name_col = _find_col(df, ["设备名称", "设备", "名称", "equipment_name", "设备名"])
        status_col = _find_col(df, ["设备状态", "当前状态", "状态", "运行状态"])

        out = pd.DataFrame()
        out["设备id"] = df[id_col] if id_col else pd.NA
        out["设备名称"] = df[name_col] if name_col else pd.NA
        out["当前状态"] = df[status_col] if status_col else pd.NA
        out["系统"] = sh

        out["设备id"] = out["设备id"].astype(str).str.strip().replace({"nan": "", "None": ""})
        out["设备名称"] = out["设备名称"].astype(str).str.strip().replace({"nan": "", "None": ""})
        out["当前状态"] = out["当前状态"].astype(str).str.strip().replace({"nan": "", "None": ""})

        out = out[(out["设备id"] != "") | (out["设备名称"] != "")].copy()
        equip_frames.append(out)

    if not equip_frames:
        return pd.DataFrame(columns=["设备id", "设备名称", "当前状态", "系统"])

    equip = pd.concat(equip_frames, ignore_index=True)
    equip = equip.drop_duplicates(subset=["设备id", "设备名称"], keep="first").copy()
    return equip


def _load_fault_records(xlsx_path: str) -> pd.DataFrame:
    xl = pd.ExcelFile(xlsx_path)
    if "异常记录" not in xl.sheet_names:
        return pd.DataFrame()

    df = _read_excel_safely(xlsx_path, "异常记录")
    if df.empty:
        return pd.DataFrame()

    date_col = _find_col(df, ["日期", "记录日期", "日期/时间", "时间", "日期时间"])
    eid_col = _find_col(df, ["设备id", "设备ID", "设备编号", "equipment_id"])
    name_col = _find_col(df, ["设备名称", "设备", "名称", "equipment_name", "设备名"])
    stop_col = _find_col(df, ["是否停机（是/否）", "是否停机", "停机", "是否停产"])
    content_col = _find_col(df, ["异常描述", "异常情况", "故障现象", "问题描述", "内容", "备注"])
    duration_col = _find_col(df, ["停机时长（小时）", "停机时长(小时)", "停机时长", "停机小时"])

    out = pd.DataFrame()
    out["日期"] = pd.to_datetime(df[date_col], errors="coerce") if date_col else pd.NaT
    out["设备id"] = df[eid_col] if eid_col else ""
    out["设备名称"] = df[name_col] if name_col else ""
    out["是否停机"] = df[stop_col] if stop_col else ""
    out["异常内容"] = df[content_col] if content_col else ""
    out["停机小时"] = df[duration_col].apply(_parse_duration_to_hours) if duration_col else np.nan

    out["设备id"] = out["设备id"].astype(str).str.strip().replace({"nan": "", "None": ""})
    out["设备名称"] = out["设备名称"].astype(str).str.strip().replace({"nan": "", "None": ""})
    out["是否停机"] = out["是否停机"].astype(str).str.strip().replace({"nan": "", "None": ""})
    out["异常内容"] = out["异常内容"].astype(str).str.strip().replace({"nan": "", "None": ""})

    out = out[(out["设备id"] != "") | (out["设备名称"] != "")].copy()

    stop_mask = out["是否停机"].eq("是")
    out["有效停机"] = stop_mask & out["停机小时"].notna()

    return out


def get_device_fault_ranking(db_path: str, recent_days: int = 90) -> pd.DataFrame:
    xlsx_path = get_published_device_excel_path(db_path)
    if not xlsx_path:
        return pd.DataFrame(
            columns=["设备名称", "系统", "异常次数", "停机次数", "停机总时长（小时）", "最近异常日期", "当前状态"]
        )

    equip = _load_equipment_base(xlsx_path)
    faults = _load_fault_records(xlsx_path)

    if faults.empty and equip.empty:
        return pd.DataFrame(
            columns=["设备名称", "系统", "异常次数", "停机次数", "停机总时长（小时）", "最近异常日期", "当前状态"]
        )

    if not faults.empty:
        if faults["日期"].notna().any():
            latest_date = faults["日期"].max()
            cutoff = latest_date - pd.Timedelta(days=recent_days)
            faults = faults[(faults["日期"].isna()) | (faults["日期"] >= cutoff)].copy()

        faults["设备键"] = faults.apply(
            lambda r: r["设备id"] if str(r["设备id"]).strip() else str(r["设备名称"]).strip(),
            axis=1,
        )

        agg = faults.groupby("设备键", as_index=False).agg(
            异常次数=("设备键", "count"),
            停机次数=("有效停机", "sum"),
            停机总时长小时=("停机小时", lambda x: float(pd.to_numeric(x, errors="coerce").where(faults.loc[x.index, "有效停机"]).dropna().sum())),
            最近异常日期=("日期", "max"),
            设备名称=("设备名称", "last"),
            设备id=("设备id", "last"),
        )
    else:
        agg = pd.DataFrame(
            columns=["设备键", "异常次数", "停机次数", "停机总时长小时", "最近异常日期", "设备名称", "设备id"]
        )

    if not equip.empty:
        equip["设备键"] = equip.apply(
            lambda r: r["设备id"] if str(r["设备id"]).strip() else str(r["设备名称"]).strip(),
            axis=1,
        )
        merged = pd.merge(
            agg,
            equip[["设备键", "设备名称", "系统", "当前状态"]].rename(columns={"设备名称": "台账设备名称"}),
            on="设备键",
            how="left",
        )
        merged["设备名称"] = merged["台账设备名称"].fillna(merged["设备名称"])
        merged = merged.drop(columns=["台账设备名称"])
    else:
        merged = agg.copy()
        merged["系统"] = ""
        merged["当前状态"] = ""

    if merged.empty:
        return pd.DataFrame(
            columns=["设备名称", "系统", "异常次数", "停机次数", "停机总时长（小时）", "最近异常日期", "当前状态"]
        )

    merged["设备名称"] = merged["设备名称"].fillna("").astype(str).str.strip()
    merged["系统"] = merged["系统"].fillna("").astype(str).str.strip()
    merged["当前状态"] = merged["当前状态"].fillna("").astype(str).str.strip()
    merged["停机总时长小时"] = pd.to_numeric(merged["停机总时长小时"], errors="coerce").fillna(0.0)

    merged["最近异常日期"] = pd.to_datetime(merged["最近异常日期"], errors="coerce")
    merged = merged.sort_values(
        ["停机总时长小时", "停机次数", "异常次数", "最近异常日期"],
        ascending=[False, False, False, False],
    ).reset_index(drop=True)

    out = merged[["设备名称", "系统", "异常次数", "停机次数", "停机总时长小时", "最近异常日期", "当前状态"]].copy()
    out = out.rename(columns={"停机总时长小时": "停机总时长（小时）"})
    out["最近异常日期"] = out["最近异常日期"].dt.strftime("%Y-%m-%d")
    out["最近异常日期"] = out["最近异常日期"].fillna("-")
    out["停机总时长（小时）"] = out["停机总时长（小时）"].map(lambda x: round(float(x), 2))
    return out