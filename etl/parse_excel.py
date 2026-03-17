# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

EXCEL_EPOCH = pd.Timestamp("1899-12-30")

HEADER_ALIASES: Dict[str, List[str]] = {
    "日期": ["日期", "日期 "],
    "来料(车)": ["来料(车)", "来料车次", "来料车", "进料车次"],
    "来料(吨)": ["来料(吨)", "来料吨", "进料(吨)", "处理量(吨)"],
    "出渣(车)": ["出渣(车)", "出渣车次", "出渣车"],
    "出渣(吨)": ["出渣(吨)", "出渣吨"],
    "出渣合计(吨)": ["出渣合计(吨)", "出渣合计吨", "总出渣(吨)"],
    "制桨量m3": ["制桨量m3", "制浆量m3", "制桨量", "制浆量", "制桨量_m3", "制浆量_m3"],
    "水表读数m3": ["水表读数m3", "水表读数", "水表累计m3", "水表读数_m3"],
    "用水量m3": ["用水量m3", "用水量", "日用水量m3", "用水量_m3"],
    "电表读数X103kw*h": [
        "电表读数X103kw*h",
        "电表读数X10^3kw*h",
        "电表读数x103kw*h",
        "电表读数(千kWh)",
        "电表读数_X103kw*h",
        "电_电表读数X103kw*h",
        "电_电表读数X10^3kw*h",
        "电_电表读数x103kw*h",
    ],
    "项目流量计m3": ["项目流量计m3", "项目流量计", "项目流量"],
    "去水厂的浆料m3": ["去水厂的浆料m3", "去水厂浆料m3", "去水厂浆料"],
    "水厂流量计m3": ["水厂流量计m3", "水厂流量计", "水厂流量"],
    "到水厂的浆料m3": ["到水厂的浆料m3", "到水厂浆料m3", "到水厂浆料"],

    "总来料桶数": ["来料(桶)", "总来料桶数", "总进料桶数", "总桶数"],

    "1线打料桶数": [
        "1#线(桶)_打料", "1号线(桶)_打料", "1线(桶)_打料",
        "1线打料", "1#线打料", "打料", "1#线(桶)打料"
    ],
    "1线出渣桶数": [
        "1#线(桶)_出渣", "1号线(桶)_出渣", "1线(桶)_出渣",
        "1线出渣", "1#线出渣", "出渣", "1#线(桶)出渣"
    ],
    "2线打料桶数": [
        "2号线(桶)_打料", "2#线(桶)_打料", "2线(桶)_打料",
        "2线打料", "2#线打料", "2号线打料", "2#线(桶)打料"
    ],
    "2线出渣桶数": [
        "2号线(桶)_出渣", "2#线(桶)_出渣", "2线(桶)_出渣",
        "2线出渣", "2#线出渣", "2号线出渣", "2#线(桶)出渣", "出渣.1"
    ],

    "压缩箱桶数": ["压缩箱(桶)", "压缩箱桶数", "压缩箱_桶", "压缩箱"],

    "离心机表数m3": [
        "离心机进料(m3)_表数", "离心机表数m3", "离心机表数",
        "离心机进料表数", "表数"
    ],
    "离心机进料量m3": [
        "离心机进料(m3)_进料量", "离心机进料量m3", "离心机进料量",
        "离心机进料", "进料量"
    ],

    "1线开机时间": ["1线开机时间", "1#线开机时间", "1号线开机时间"],
    "1线停机时间": ["1线停机时间", "1#线停机时间", "1号线停机时间"],
    "2线开机时间": ["2线开机时间", "2#线开机时间", "2号线开机时间"],
    "2线停机时间": ["2线停机时间", "2#线停机时间", "2号线停机时间"],
}

NUMERIC_OUTPUT_COLUMNS = [
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
]


def _clean_text(x) -> str:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return ""
    return str(x).replace("\n", "").replace(" ", "").strip()


def _excel_date_to_ts(x) -> pd.Timestamp | pd.NaT:
    if pd.isna(x):
        return pd.NaT
    if isinstance(x, pd.Timestamp):
        return x.normalize()
    if hasattr(x, "year") and hasattr(x, "month") and hasattr(x, "day"):
        return pd.Timestamp(x).normalize()
    try:
        return (EXCEL_EPOCH + pd.to_timedelta(int(float(x)), unit="D")).normalize()
    except Exception:
        out = pd.to_datetime(x, errors="coerce")
        return out.normalize() if pd.notna(out) else pd.NaT


def _find_header_row(raw: pd.DataFrame, scan_rows: int = 8) -> Tuple[Optional[int], Dict[str, int]]:
    best_row = None
    best_score = -1
    best_map: Dict[str, int] = {}
    alias_map = {std: {_clean_text(a) for a in aliases} for std, aliases in HEADER_ALIASES.items()}
    for i in range(min(len(raw), scan_rows)):
        row = [_clean_text(v) for v in raw.iloc[i].tolist()]
        cur_map: Dict[str, int] = {}
        score = 0
        for std, aliases in alias_map.items():
            for idx, cell in enumerate(row):
                if cell in aliases:
                    cur_map[std] = idx
                    score += 1
                    break
        if score > best_score:
            best_row = i
            best_score = score
            best_map = cur_map
    if best_score < 2 or "日期" not in best_map:
        return None, {}
    return best_row, best_map


def _make_unique(names: List[str]) -> List[str]:
    counts: dict[str, int] = {}
    out: list[str] = []
    for name in names:
        base = name or "unnamed"
        k = counts.get(base, 0)
        counts[base] = k + 1
        out.append(base if k == 0 else f"{base}.{k}")
    return out


def _build_combined_headers(raw: pd.DataFrame, header_row: int) -> List[str]:
    parent_row = header_row - 1 if header_row - 1 >= 0 else None
    names: List[str] = []
    ncols = raw.shape[1]
    for idx in range(ncols):
        child = _clean_text(raw.iloc[header_row, idx]) if idx < raw.shape[1] else ""
        parent = _clean_text(raw.iloc[parent_row, idx]) if parent_row is not None else ""

        if child and parent and parent != child:
            name = f"{parent}_{child}"
        elif child:
            name = child
        elif parent:
            name = parent
        else:
            name = f"unnamed_{idx}"
        names.append(name)
    return _make_unique(names)


def _pick_series(df: pd.DataFrame, logical_name: str) -> pd.Series:
    if logical_name not in HEADER_ALIASES:
        return pd.Series([np.nan] * len(df), index=df.index)

    for alias in HEADER_ALIASES[logical_name]:
        if alias in df.columns:
            return df[alias]

    clean_cols = {_clean_text(c): c for c in df.columns}
    for alias in HEADER_ALIASES[logical_name]:
        hit = clean_cols.get(_clean_text(alias))
        if hit is not None:
            return df[hit]

    return pd.Series([np.nan] * len(df), index=df.index)


def _sheet_to_daily_df(xlsx_path: str, sheet_name: str) -> pd.DataFrame:
    raw = pd.read_excel(xlsx_path, sheet_name=sheet_name, header=None)
    if raw.empty:
        return pd.DataFrame()

    header_row, _ = _find_header_row(raw)
    if header_row is None:
        return pd.DataFrame()

    df = pd.read_excel(xlsx_path, sheet_name=sheet_name, header=header_row)
    df.columns = _build_combined_headers(raw, header_row)

    date_s = _pick_series(df, "日期")
    out = pd.DataFrame({
        "date": date_s.apply(_excel_date_to_ts),
        "incoming_trips": _pick_series(df, "来料(车)"),
        "incoming_ton": _pick_series(df, "来料(吨)"),
        "slag_trips": _pick_series(df, "出渣(车)"),
        "slag_ton": _pick_series(df, "出渣(吨)"),
        "slag_total_ton": _pick_series(df, "出渣合计(吨)"),
        "slurry_m3": _pick_series(df, "制桨量m3"),
        "water_meter_m3": _pick_series(df, "水表读数m3"),
        "water_m3": _pick_series(df, "用水量m3"),
        "elec_meter_x1e3kwh": _pick_series(df, "电表读数X103kw*h"),
        "proj_flow_m3": _pick_series(df, "项目流量计m3"),
        "to_wwtp_m3": _pick_series(df, "去水厂的浆料m3"),
        "wwtp_flow_m3": _pick_series(df, "水厂流量计m3"),
        "arrive_wwtp_m3": _pick_series(df, "到水厂的浆料m3"),
        "incoming_bucket_count": _pick_series(df, "总来料桶数"),
        "line1_feed_bucket_count": _pick_series(df, "1线打料桶数"),
        "line1_slag_bucket_count": _pick_series(df, "1线出渣桶数"),
        "line2_feed_bucket_count": _pick_series(df, "2线打料桶数"),
        "line2_slag_bucket_count": _pick_series(df, "2线出渣桶数"),
        "compress_bucket_count": _pick_series(df, "压缩箱桶数"),
        "centrifuge_meter_m3": _pick_series(df, "离心机表数m3"),
        "centrifuge_feed_m3": _pick_series(df, "离心机进料量m3"),
        "line1_runtime_hours": _pick_series(df, "1线开机时间"),
        "line2_runtime_hours": _pick_series(df, "2线开机时间"),
    })

    out = out[out["date"].notna()].copy()
    if out.empty:
        return out

    for col in NUMERIC_OUTPUT_COLUMNS:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")

    out["slag_total_ton"] = out["slag_total_ton"].fillna(out["slag_ton"])
    out["elec_meter_kwh"] = out["elec_meter_x1e3kwh"] * 1000
    out["source_sheet"] = sheet_name

    return out


def parse_workbook(xlsx_path: str, sheet_names: Optional[List[str]] = None) -> pd.DataFrame:
    xl = pd.ExcelFile(xlsx_path)
    sheets = sheet_names or xl.sheet_names
    frames: list[pd.DataFrame] = []

    for sh in sheets:
        try:
            cur = _sheet_to_daily_df(xlsx_path, sh)
        except Exception:
            cur = pd.DataFrame()
        if not cur.empty:
            frames.append(cur)

    if not frames:
        return pd.DataFrame(columns=["date", *NUMERIC_OUTPUT_COLUMNS, "source_sheet"])

    df = pd.concat(frames, ignore_index=True)
    df = (
        df.sort_values(["date", "source_sheet"])
        .drop_duplicates(subset=["date"], keep="last")
        .reset_index(drop=True)
    )
    return df