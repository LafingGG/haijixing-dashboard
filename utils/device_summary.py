# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Optional, Dict, List

import pandas as pd

from utils.device_store import get_published_device_excel_path


STATUS_LEVEL_TEXT = {
    0: "无数据",
    1: "正常",
    2: "有异常",
    3: "当前停机",
}

STATUS_LEVEL_EMOJI = {
    0: "⚪",
    1: "🟢",
    2: "🟡",
    3: "🔴",
}


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


def _status_to_level(status_text: str) -> int:
    """
    首页汇总口径（比设备健康页更粗）：
    红：停机 / 故障 / 检修
    黄：维修 / 带病运行 / 异常 / 报警 / 不稳定 / 问题
    绿：正常 / 运行 / 完好 / 良好
    其余：不参与抬灯
    """
    s = _as_text(status_text)
    if not s:
        return 0

    if any(k in s for k in ["停机", "故障", "检修"]):
        return 3

    if any(k in s for k in ["维修", "带病", "异常", "报警", "不稳定", "问题"]):
        return 2

    if any(k in s for k in ["正常", "运行", "完好", "良好"]):
        return 1

    return 0


def _normalize_equipment_df(df: pd.DataFrame, default_system: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["设备id", "设备名称", "系统", "设备状态"])

    id_col = _find_col(df, ["设备id", "设备ID", "设备编号", "equipment_id", "ID"])
    name_col = _find_col(df, ["设备名称", "设备", "名称", "equipment_name", "设备名"])
    status_col = _find_col(df, ["设备状态", "当前状态", "状态", "运行状态"])

    out = pd.DataFrame()
    out["设备id"] = df[id_col] if id_col else pd.NA
    out["设备名称"] = df[name_col] if name_col else pd.NA
    out["设备状态"] = df[status_col] if status_col else pd.NA

    out["设备id"] = out["设备id"].astype(str).str.strip()
    out["设备名称"] = out["设备名称"].astype(str).str.strip()
    out["系统"] = default_system
    out["设备状态"] = out["设备状态"].astype(str).str.strip()

    out["设备id"] = out["设备id"].replace({"nan": "", "None": ""})
    out["设备名称"] = out["设备名称"].replace({"nan": "", "None": ""})
    out["设备状态"] = out["设备状态"].replace({"nan": "", "None": ""})

    out = out[(out["设备id"] != "") | (out["设备名称"] != "")].copy()
    if out.empty:
        return pd.DataFrame(columns=["设备id", "设备名称", "系统", "设备状态"])
    return out


def _read_equipment_and_faults(xlsx_path: str):
    xl = pd.ExcelFile(xlsx_path)

    equip_frames = []
    for sh in ["预处理设备", "水解酸化设备", "除臭设备", "车间基础"]:
        if sh in xl.sheet_names:
            try:
                df = pd.read_excel(xlsx_path, sheet_name=sh)
                equip_frames.append(_normalize_equipment_df(df, sh))
            except Exception:
                pass

    equip = (
        pd.concat(equip_frames, ignore_index=True)
        if equip_frames else
        pd.DataFrame(columns=["设备id", "设备名称", "系统", "设备状态"])
    )

    faults = pd.DataFrame()
    if "异常记录" in xl.sheet_names:
        try:
            faults = pd.read_excel(xlsx_path, sheet_name="异常记录")
        except Exception:
            faults = pd.DataFrame()

    return equip, faults


def _normalize_fault_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    out = df.copy()

    date_col = _find_col(out, ["日期", "记录日期", "日期/时间", "时间", "日期时间"])
    if date_col:
        out["日期"] = pd.to_datetime(out[date_col], errors="coerce")
    else:
        out["日期"] = pd.NaT

    eid_col = _find_col(out, ["设备id", "设备ID", "设备编号", "equipment_id"])
    if eid_col:
        out["设备id"] = out[eid_col].astype(str).str.strip()
    else:
        out["设备id"] = ""

    stop_col = _find_col(out, ["是否停机", "停机", "是否停产"])
    if stop_col:
        out["是否停机"] = out[stop_col].astype(str).str.strip()
    else:
        out["是否停机"] = ""

    end_col = _find_col(out, ["故障结束时间", "结束时间", "停机结束时间"])
    if end_col:
        out["故障结束时间_raw"] = out[end_col]
    else:
        out["故障结束时间_raw"] = pd.NA

    out = out[out["日期"].notna() | (out["设备id"] != "")].copy()
    return out


def _has_unfinished_stop(x) -> bool:
    """
    兼容几种情况：
    - 空值 / NaN -> 未结束
    - 空字符串 -> 未结束
    - 有值（哪怕是 Excel time）-> 视为已结束
    """
    if pd.isna(x):
        return True
    s = str(x).strip()
    return s == ""


def get_home_device_status(db_path: str, recent_days: int = 30) -> Dict:
    p = get_published_device_excel_path(db_path)
    if not p:
        return {
            "level": 0,
            "label": f"{STATUS_LEVEL_EMOJI[0]} {STATUS_LEVEL_TEXT[0]}",
            "detail": "未发布设备记录表",
        }

    try:
        equip, faults = _read_equipment_and_faults(p)
    except Exception as e:
        return {
            "level": 0,
            "label": f"{STATUS_LEVEL_EMOJI[0]} {STATUS_LEVEL_TEXT[0]}",
            "detail": f"设备表读取失败：{e}",
        }

    # ---------- 设备台账抬灯 ----------
    base_level = 0
    red_n = yellow_n = green_n = 0

    if equip is not None and not equip.empty:
        equip["_level"] = equip["设备状态"].map(_status_to_level)
        red_n = int((equip["_level"] == 3).sum())
        yellow_n = int((equip["_level"] == 2).sum())
        green_n = int((equip["_level"] == 1).sum())

        if red_n > 0:
            base_level = 3
        elif yellow_n > 0:
            base_level = 2
        elif green_n > 0:
            base_level = 1

    # ---------- 异常记录抬灯 ----------
    fault_level = 0
    recent_fault_count = 0

    if faults is not None and not faults.empty:
        f = _normalize_fault_df(faults)

        if not f.empty and f["日期"].notna().any():
            latest_date = f["日期"].max()
            cutoff = latest_date - pd.Timedelta(days=recent_days)
            f = f[f["日期"] >= cutoff].copy()

        recent_fault_count = len(f)

        if not f.empty:
            stop_mask = f["是否停机"].astype(str).str.contains("是", na=False)

            # 只有“停机且未结束”才抬红
            if stop_mask.any():
                sf = f[stop_mask].copy()
                unfinished_stop = sf["故障结束时间_raw"].map(_has_unfinished_stop).any()
                if unfinished_stop:
                    fault_level = 3
                else:
                    fault_level = 2
            else:
                # 近期存在异常记录，但没有停机，抬黄
                fault_level = 2

    final_level = max(base_level, fault_level)

    detail_parts = []
    if equip is not None and not equip.empty:
        total = len(equip)
        detail_parts.append(f"台账设备 {total} 台（绿 {green_n} / 黄 {yellow_n} / 红 {red_n}）")

    if recent_fault_count > 0:
        detail_parts.append(f"近期异常 {recent_fault_count} 条")

    detail = "；".join(detail_parts) if detail_parts else "无可用设备数据"

    return {
        "level": final_level,
        "label": f"{STATUS_LEVEL_EMOJI[final_level]} {STATUS_LEVEL_TEXT[final_level]}",
        "detail": detail,
    }