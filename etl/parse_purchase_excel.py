# -*- coding: utf-8 -*-
from __future__ import annotations

import calendar
import hashlib
import json
import re
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

import pandas as pd

from utils.cost_store import CATEGORY_SEED


HEADER_ALIASES = {
    "expense_date": ["费用日期", "日期", "付款日期", "报销日期", "发生日期"],
    "item_name": ["费用事项", "事项", "费用内容", "内容", "摘要"],
    "payee": ["收款方", "供应商", "对方单位", "付款对象", "收款单位"],
    "amount": ["金额", "付款金额", "含税金额", "实付金额"],
    "category_name": ["分类", "费用分类", "类别", "一级分类"],
    "remark": ["备注", "说明", "备注说明"],
}

DEFAULT_SHEETS = ["费用明细表", "费用明细", "项目垃圾处理费用"]

CATEGORY_NAME_ALIASES = {
    # 固渣
    "固渣费": "固渣处理费",
    "固渣处理": "固渣处理费",
    "固渣处理费用": "固渣处理费",
    "渣处理费": "固渣处理费",
    "渣外运费": "固渣处理费",
    "固废处理费": "固渣处理费",

    # 碳源
    "碳源费": "碳源处理费",
    "碳源": "碳源处理费",
    "碳源处理费": "碳源处理费",
    "碳源处置费": "碳源处理费",
    "有机酸费": "碳源处理费",
    "有机酸处理费": "碳源处理费",
    "发酵液处理费": "碳源处理费",
    "浆料处理费": "碳源处理费",

    # 能源
    "能源费": "能源费用",
    "水电费": "能源费用",
    "电费水费": "能源费用",

    # 维修
    "维修费": "维修费用",
    "检修费": "维修费用",
    "保养费": "维修费用",

    # 运输
    "物流费": "运输费",
    "运费": "运输费",

    # 行政/后勤
    "宿舍费用": "宿舍后勤",
    "后勤费用": "宿舍后勤",
}

def _clean_value(x):
    if pd.isna(x):
        return None
    if isinstance(x, pd.Timestamp):
        return x.strftime("%Y-%m-%d")
    return str(x).strip()


def _norm_col(s: str) -> str:
    return str(s).replace("\n", "").replace(" ", "").strip()


def _normalize_category_name(x: str) -> str:
    s = str(x or "").strip()
    s = s.replace("（", "(").replace("）", ")")
    s = s.replace(" ", "")
    return CATEGORY_NAME_ALIASES.get(s, s)


def _find_header_row(df0: pd.DataFrame) -> Tuple[Optional[int], Dict[str, int]]:
    best_row = None
    best_map: Dict[str, int] = {}
    best_score = -1
    sample_rows = min(len(df0), 12)
    for i in range(sample_rows):
        row = [_norm_col(v) for v in df0.iloc[i].tolist()]
        cur_map: Dict[str, int] = {}
        score = 0
        for std_col, aliases in HEADER_ALIASES.items():
            alias_norm = {_norm_col(a) for a in aliases}
            for idx, cell in enumerate(row):
                if cell in alias_norm:
                    cur_map[std_col] = idx
                    score += 1
                    break
        if score > best_score:
            best_score = score
            best_row = i
            best_map = cur_map
    if best_score < 4:
        return None, {}
    return best_row, best_map


def _build_category_lookup() -> Dict[str, Tuple[str, str]]:
    """
    返回：
        {
            "能源费用": ("energy_cost", "能源费用"),
            "固渣处理费": ("slag", "运营费用"),
            "有机酸处理费": ("acid", "运营费用"),
            ...
        }
    """
    out = {}
    for category_code, category_name, level1_name, *_ in CATEGORY_SEED:
        key = str(category_name).strip()
        if key:
            out[key] = (str(category_code).strip(), str(level1_name).strip())
    return out


def _infer_year(month: int, expense_dt: date) -> int:
    if month > expense_dt.month + 1:
        return expense_dt.year - 1
    if month < expense_dt.month - 10:
        return expense_dt.year + 1
    return expense_dt.year


def _last_day_of_month(y: int, m: int) -> int:
    return calendar.monthrange(y, m)[1]


def _daterange(d1: date, d2: date) -> List[date]:
    if d2 < d1:
        d1, d2 = d2, d1
    days = (d2 - d1).days + 1
    return [d1 + timedelta(days=i) for i in range(days)]


def _parse_month_only_token(text: str, expense_dt: date) -> List[date]:
    months = []
    for y, m in re.findall(r"(\d{4})[./年\-](\d{1,2})月?份?", text):
        months.append((int(y), int(m)))
    if not months:
        for m in re.findall(r"(?<!\d)(\d{1,2})月?份", text):
            mm = int(m)
            yy = _infer_year(mm, expense_dt)
            months.append((yy, mm))
    out = []
    for yy, mm in months:
        if 1 <= mm <= 12:
            out.append(date(yy, mm, 1))
    return out


def _parse_full_dates(text: str, expense_dt: date) -> List[date]:
    out = []
    for y, m, d in re.findall(r"(\d{4})[./年\-](\d{1,2})[./月\-](\d{1,2})日?", text):
        try:
            out.append(date(int(y), int(m), int(d)))
        except Exception:
            pass
    for m, d in re.findall(r"(?<!\d)(\d{1,2})[./月](\d{1,2})(?!\d)", text):
        mm = int(m)
        dd = int(d)
        yy = _infer_year(mm, expense_dt)
        try:
            out.append(date(yy, mm, dd))
        except Exception:
            pass
    return sorted(set(out))


def _parse_compact_token(token: str, expense_dt: date) -> List[date]:
    token = token.strip()
    if not token:
        return []

    month_only = _parse_month_only_token(token, expense_dt)
    if month_only and ("月份" in token or token.endswith("月") or token.endswith("月份")):
        return month_only

    fulls = _parse_full_dates(token, expense_dt)
    if fulls and "-" not in token and "至" not in token and "到" not in token:
        return fulls

    tk = token.replace("至", "-").replace("到", "-").replace("—", "-").replace("–", "-")
    tk = tk.replace("（", "(").replace("）", ")")
    tk = re.sub(r"\(.*?\)", "", tk).strip()

    m = re.fullmatch(r"(\d{1,2})[./月](\d{1,2})\s*-\s*(\d{1,2})[./月](\d{1,2})", tk)
    if m:
        m1, d1, m2, d2 = map(int, m.groups())
        y1 = _infer_year(m1, expense_dt)
        y2 = _infer_year(m2, expense_dt)
        try:
            return _daterange(date(y1, m1, d1), date(y2, m2, d2))
        except Exception:
            return []

    m = re.fullmatch(r"(\d{1,2})[./月](\d{1,2})\s*-\s*(\d{1,2})", tk)
    if m:
        mm, d1, d2 = map(int, m.groups())
        yy = _infer_year(mm, expense_dt)
        try:
            return _daterange(date(yy, mm, d1), date(yy, mm, d2))
        except Exception:
            return []

    m = re.fullmatch(r"(\d{1,2})[./月](\d{1,2})", tk)
    if m:
        mm, dd = map(int, m.groups())
        yy = _infer_year(mm, expense_dt)
        try:
            return [date(yy, mm, dd)]
        except Exception:
            return []

    if month_only:
        return month_only

    return []


def _parse_service_dates(item_name: str, expense_date_str: str) -> Tuple[List[date], str]:
    if not item_name:
        return [], "payment_date"
    try:
        expense_dt = datetime.strptime(str(expense_date_str), "%Y-%m-%d").date()
    except Exception:
        expense_dt = datetime.today().date()

    text = str(item_name).strip()
    if not text:
        return [], "payment_date"

    normalized = (
        text.replace("，", ",")
        .replace("、", ",")
        .replace("；", ",")
        .replace(";", ",")
        .replace(" ", "")
    )

    tokens = [t for t in normalized.split(",") if t]
    out_dates: List[date] = []
    month_only_dates: List[date] = []

    for tk in tokens:
        ds = _parse_compact_token(tk, expense_dt)
        if not ds:
            continue
        if any(d.day == 1 for d in ds) and ("月份" in tk or tk.endswith("月") or tk.endswith("月份")):
            month_only_dates.extend(ds)
        else:
            out_dates.extend(ds)

    if out_dates:
        return sorted(set(out_dates)), "service_item_parsed"
    if month_only_dates:
        return sorted(set(month_only_dates)), "service_month_parsed"

    whole = _parse_compact_token(normalized, expense_dt)
    if whole:
        src = "service_item_parsed"
        if any(d.day == 1 for d in whole) and ("月份" in normalized or normalized.endswith("月") or normalized.endswith("月份")):
            src = "service_month_parsed"
        return sorted(set(whole)), src

    return [], "payment_date"


def parse_purchase_workbook(xlsx_path: str, sheet_names: Optional[List[str]] = None) -> pd.DataFrame:
    xl = pd.ExcelFile(xlsx_path)
    sheets = sheet_names or [s for s in xl.sheet_names if s in DEFAULT_SHEETS] or xl.sheet_names
    category_lookup = _build_category_lookup()
    frames = []

    for sh in sheets:
        raw = pd.read_excel(xlsx_path, sheet_name=sh, header=None)
        if raw.empty:
            continue

        header_row, col_map = _find_header_row(raw)
        if header_row is None:
            continue

        body = raw.iloc[header_row + 1 :].copy().reset_index(drop=True)
        rename_map = {idx: std_col for std_col, idx in col_map.items()}
        body = body.rename(columns=rename_map)
        keep_cols = list(rename_map.values())
        body = body[[c for c in keep_cols if c in body.columns]].copy()

        if "expense_date" not in body.columns or "amount" not in body.columns:
            continue

        body["expense_date"] = pd.to_datetime(body["expense_date"], errors="coerce")
        body["amount"] = pd.to_numeric(body["amount"], errors="coerce")
        body = body[body["expense_date"].notna() & body["amount"].notna()].copy()
        if body.empty:
            continue

        if "item_name" not in body.columns:
            body["item_name"] = None
        if "payee" not in body.columns:
            body["payee"] = None
        if "category_name" not in body.columns:
            body["category_name"] = "其他"
        if "remark" not in body.columns:
            body["remark"] = None

        body["category_name"] = (
            body["category_name"]
            .fillna("其他")
            .astype(str)
            .map(_normalize_category_name)
            .replace({"": "其他"})
        )

        body["category_code"] = body["category_name"].map(
            lambda x: category_lookup.get(x, ("other", "其他费用"))[0]
        )
        body["level1_name"] = body["category_name"].map(
            lambda x: category_lookup.get(x, ("other", "其他费用"))[1]
        )

        body["expense_date"] = body["expense_date"].dt.strftime("%Y-%m-%d")
        body["expense_month"] = pd.to_datetime(body["expense_date"]).dt.to_period("M").astype(str)
        body["source_sheet"] = sh
        body["source_row_no"] = body.index + header_row + 2

        parsed_dates = body.apply(
            lambda r: _parse_service_dates(str(r.get("item_name") or ""), str(r["expense_date"])),
            axis=1,
        )
        body["service_dates_list"] = parsed_dates.map(lambda x: x[0])
        body["date_source"] = parsed_dates.map(lambda x: x[1])
        body["service_date"] = body["service_dates_list"].map(lambda xs: xs[0].isoformat() if xs else None)
        body["service_month"] = body["service_dates_list"].map(
            lambda xs: xs[0].strftime("%Y-%m") if xs else None
        )

        def _safe_analysis_month(service_month: str | None, expense_month: str) -> str:
            def _ok(s: str | None) -> bool:
                try:
                    if not s:
                        return False
                    s = str(s).strip()
                    if len(s) != 7 or s[4] != "-":
                        return False
                    y = int(s[:4])
                    m = int(s[5:7])
                    return 2020 <= y <= 2035 and 1 <= m <= 12
                except Exception:
                    return False

            return service_month if _ok(service_month) else expense_month

        body["analysis_month"] = body.apply(
            lambda r: _safe_analysis_month(r["service_month"], r["expense_month"]),
            axis=1,
        )

        body["service_dates_json"] = body["service_dates_list"].map(
            lambda xs: json.dumps([d.isoformat() for d in xs], ensure_ascii=False)
        )
        body["service_months_json"] = body["service_dates_list"].map(
            lambda xs: json.dumps(sorted({d.strftime("%Y-%m") for d in xs}), ensure_ascii=False)
        )

        raw_cols = [
            c
            for c in ["expense_date", "item_name", "payee", "amount", "category_name", "remark"]
            if c in body.columns
        ]
        body["raw_json"] = body[raw_cols].apply(
            lambda r: json.dumps({k: _clean_value(v) for k, v in r.to_dict().items()}, ensure_ascii=False),
            axis=1,
        )

        body["row_hash"] = body.apply(
            lambda r: hashlib.md5(
                "|".join(
                    [
                        str(r.get("expense_date") or ""),
                        str(r.get("item_name") or ""),
                        str(r.get("payee") or ""),
                        f"{float(r.get('amount') or 0):.2f}",
                        str(r.get("category_name") or ""),
                        str(r.get("remark") or ""),
                        str(sh),
                        str(r.get("source_row_no") or ""),
                    ]
                ).encode("utf-8")
            ).hexdigest(),
            axis=1,
        )

        frames.append(
            body[
                [
                    "expense_date",
                    "expense_month",
                    "item_name",
                    "payee",
                    "amount",
                    "category_name",
                    "category_code",
                    "level1_name",
                    "remark",
                    "source_sheet",
                    "source_row_no",
                    "service_date",
                    "service_month",
                    "analysis_month",
                    "date_source",
                    "service_dates_json",
                    "service_months_json",
                    "raw_json",
                    "row_hash",
                ]
            ]
        )

    if not frames:
        return pd.DataFrame()

    out = pd.concat(frames, ignore_index=True)
    out = out.sort_values(["expense_date", "source_sheet", "source_row_no"]).reset_index(drop=True)
    return out