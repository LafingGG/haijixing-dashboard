# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import date
from io import BytesIO
from typing import List, Optional, Tuple

import pandas as pd
import plotly.express as px
import streamlit as st
from streamlit.errors import StreamlitSecretNotFoundError


# ============================================================
# Debug flag (safe without secrets)
# ============================================================
def get_debug_flag() -> bool:
    try:
        v = st.secrets.get("DEBUG", False)
        if isinstance(v, bool):
            return v
        if isinstance(v, (int, float)):
            return bool(v)
        if isinstance(v, str):
            return v.strip().lower() in {"1", "true", "yes", "y", "on"}
        return False
    except StreamlitSecretNotFoundError:
        return False


DEBUG = get_debug_flag()


# ============================================================
# Helpers
# ============================================================
def _norm_date_series(s: pd.Series) -> pd.Series:
    if pd.api.types.is_datetime64_any_dtype(s):
        return pd.to_datetime(s).dt.normalize()

    out = pd.to_datetime(s, errors="coerce")
    if out.notna().any():
        return out.dt.normalize()

    # Excel serial
    try:
        num = pd.to_numeric(s, errors="coerce")
        out2 = pd.to_datetime("1899-12-30") + pd.to_timedelta(num.fillna(-1).astype("int64"), unit="D")
        out2 = out2.where(num.notna(), pd.NaT)
        return out2.dt.normalize()
    except Exception:
        return pd.to_datetime(s, errors="coerce").dt.normalize()


def _to_dt(s: pd.Series) -> pd.Series:
    if pd.api.types.is_datetime64_any_dtype(s):
        return pd.to_datetime(s, errors="coerce")
    return pd.to_datetime(s, errors="coerce")


def _as_text(x) -> str:
    if pd.isna(x):
        return ""
    return str(x).strip()


def _find_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    cols = list(df.columns)
    for c in candidates:
        if c in cols:
            return c
    for c in cols:
        for cand in candidates:
            if cand in str(c):
                return c
    return None


def _infer_system_from_eid(eid: str) -> str:
    if not isinstance(eid, str):
        return "其他"
    eid = eid.strip()
    if eid.startswith("EQ_PRE_"):
        return "预处理"
    if eid.startswith("EQ_ACID_"):
        return "水解酸化"
    if eid.startswith("EQ_ODOR_"):
        return "除臭"
    if eid.startswith("EQ_BASE_"):
        return "车间基础"
    if eid.startswith("CAR_"):
        return "车辆"
    return "其他"


# ============================================================
# Loaders: equipment status + fault events
# ============================================================
@st.cache_data(ttl=60)
def _read_excel_bytes(content: bytes, sheet_name: str):
    bio = BytesIO(content)
    return pd.read_excel(bio, sheet_name=sheet_name)


@st.cache_data(ttl=60)
def _read_excel_path(path: str, sheet_name: str):
    return pd.read_excel(path, sheet_name=sheet_name)


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

    out = out[out["设备id"] != ""].copy()
    if out.empty:
        return pd.DataFrame(columns=["设备id", "设备名称", "系统", "设备状态"])
    return out


def _calc_downtime_hours(df: pd.DataFrame) -> pd.Series:
    for col in ["停机时长（小时）", "停机时长", "停机时长(小时)", "停机小时", "停机时长（h）"]:
        if col in df.columns:
            h = pd.to_numeric(df[col], errors="coerce")
            if h.notna().any():
                return h

    start_col = None
    end_col = None
    for c in ["故障开始时间", "开始时间", "停机开始时间"]:
        if c in df.columns:
            start_col = c
            break
    for c in ["故障结束时间", "结束时间", "停机结束时间"]:
        if c in df.columns:
            end_col = c
            break

    if start_col and end_col:
        stt = _to_dt(df[start_col])
        edt = _to_dt(df[end_col])
        delta = (edt - stt).dt.total_seconds() / 3600.0
        return delta.where(delta >= 0)

    return pd.Series([pd.NA] * len(df), index=df.index, dtype="float64")


def _normalize_fault_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    if "日期" not in df.columns:
        c = _find_col(df, ["记录日期", "日期/时间", "时间", "日期时间", "日期"])
        if c:
            df["日期"] = df[c]

    if "设备id" not in df.columns:
        c = _find_col(df, ["设备id", "设备ID", "设备编号", "equipment_id"])
        if c:
            df["设备id"] = df[c]

    if "设备名称" not in df.columns:
        c = _find_col(df, ["设备名称", "设备", "名称", "equipment_name"])
        if c:
            df["设备名称"] = df[c]

    if "日期" not in df.columns:
        return pd.DataFrame()

    df["日期"] = _norm_date_series(df["日期"])
    df = df[df["日期"].notna()].copy()
    if df.empty:
        return pd.DataFrame()

    df["设备id"] = df["设备id"].astype(str).str.strip() if "设备id" in df.columns else ""
    df["设备名称"] = df["设备名称"].astype(str).str.strip() if "设备名称" in df.columns else ""
    df["系统"] = df["设备id"].apply(_infer_system_from_eid) if "设备id" in df.columns else "其他"

    stop_col = _find_col(df, ["是否停机（是/否）", "是否停机", "停机", "是否停机?"])
    df["是否停机"] = df[stop_col].astype(str).str.strip().replace({"nan": "", "None": ""}) if stop_col else ""

    cat_col = _find_col(df, ["异常类别（下拉选择）", "异常类别", "故障类别", "类别"])
    df["异常类别"] = df[cat_col] if cat_col else ""

    df["停机小时"] = _calc_downtime_hours(df)

    start_col = _find_col(df, ["故障开始时间", "开始时间", "停机开始时间"])
    end_col = _find_col(df, ["故障结束时间", "结束时间", "停机结束时间"])
    df["_start_dt"] = _to_dt(df[start_col]) if start_col else pd.NaT
    df["_end_dt"] = _to_dt(df[end_col]) if end_col else pd.NaT

    return df


@st.cache_data(ttl=60)
def load_all_data_from_path(path: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if not isinstance(path, str) or not path.strip():
        return pd.DataFrame(), pd.DataFrame()
    if not os.path.exists(path):
        return pd.DataFrame(), pd.DataFrame()

    equip_frames = []
    for sheet, sysname in [
        ("预处理设备", "预处理"),
        ("水解酸化设备", "水解酸化"),
        ("除臭设备", "除臭"),
        ("车间基础整改", "车间基础"),
        ("车间基础", "车间基础"),
    ]:
        try:
            df = _read_excel_path(path, sheet)
            equip_frames.append(_normalize_equipment_df(df, sysname))
        except Exception:
            continue
    equip = pd.concat(equip_frames, ignore_index=True) if equip_frames else pd.DataFrame(
        columns=["设备id", "设备名称", "系统", "设备状态"]
    )

    try:
        faults_raw = _read_excel_path(path, "异常记录")
        faults = _normalize_fault_df(faults_raw)
    except Exception:
        faults = pd.DataFrame()

    return equip, faults


@st.cache_data(ttl=60)
def load_all_data_from_bytes(filename: str, content: bytes) -> Tuple[pd.DataFrame, pd.DataFrame]:
    equip_frames = []
    for sheet, sysname in [
        ("预处理设备", "预处理"),
        ("水解酸化设备", "水解酸化"),
        ("除臭设备", "除臭"),
        ("车间基础整改", "车间基础"),
        ("车间基础", "车间基础"),
    ]:
        try:
            df = _read_excel_bytes(content, sheet)
            equip_frames.append(_normalize_equipment_df(df, sysname))
        except Exception:
            continue
    equip = pd.concat(equip_frames, ignore_index=True) if equip_frames else pd.DataFrame(
        columns=["设备id", "设备名称", "系统", "设备状态"]
    )

    try:
        faults_raw = _read_excel_bytes(content, "异常记录")
        faults = _normalize_fault_df(faults_raw)
    except Exception:
        faults = pd.DataFrame()

    return equip, faults

def health_summary(items):
    total = len(items)
    healthy = sum(1 for x in items if x.final_level == 1)
    return healthy, total


# ============================================================
# Control Tower mapping + status logic
# ============================================================
@dataclass
class NodeResolved:
    block: str
    line: str
    node: str
    match_rule: str
    equip_id: str
    equip_name: str
    base_status_text: str
    final_level: int  # 0 none, 1 green, 2 yellow, 3 red


LEVEL_EMOJI = {0: "⚪", 1: "🟢", 2: "🟡", 3: "🔴"}
LEVEL_TEXT = {0: "无数据", 1: "正常", 2: "异常", 3: "停机/严重"}


def _status_to_level(status_text: str) -> int:
    s = _as_text(status_text)
    if not s:
        return 0
    if any(k in s for k in ["停机", "故障", "检修", "维修"]):
        return 3
    if any(k in s for k in ["异常", "带病", "报警", "不稳定", "问题"]):
        return 2
    if any(k in s for k in ["正常", "运行", "完好", "良好"]):
        return 1
    return 0


def _overlay_faults_level(eid: str, faults_in_range: pd.DataFrame) -> int:
    if faults_in_range is None or faults_in_range.empty:
        return 0
    if not eid:
        return 0
    f = faults_in_range[faults_in_range["设备id"] == eid]
    if f.empty:
        return 0

    stop_mask = f["是否停机"].astype(str).str.contains("是", na=False)
    if stop_mask.any():
        sf = f[stop_mask].copy()
        latest = sf.sort_values("日期", ascending=False).head(1)
        end_dt = latest["_end_dt"].iloc[0]
        if pd.isna(end_dt):
            return 3
        return 2

    return 2


def _find_equip_by_id(equip: pd.DataFrame, eid: str) -> Optional[pd.Series]:
    if equip is None or equip.empty:
        return None
    hit = equip[equip["设备id"] == eid]
    if hit.empty:
        return None
    return hit.iloc[0]


def _find_equip_by_name_like(equip: pd.DataFrame, keyword: str, extra_pattern: Optional[str] = None) -> Optional[pd.Series]:
    if equip is None or equip.empty:
        return None
    kw = keyword.strip()
    if not kw:
        return None
    cand = equip[equip["设备名称"].astype(str).str.contains(re.escape(kw), na=False)]
    if extra_pattern:
        cand2 = cand[cand["设备名称"].astype(str).str.contains(extra_pattern, na=False)]
        if not cand2.empty:
            return cand2.iloc[0]
    if not cand.empty:
        return cand.iloc[0]
    return None


def resolve_node(
    equip: pd.DataFrame,
    faults_in_range: pd.DataFrame,
    block: str,
    line: str,
    node: str,
    prefer_id: Optional[str] = None,
    keyword: Optional[str] = None,
    extra_regex: Optional[str] = None,
) -> NodeResolved:
    row = None
    rule = ""
    if prefer_id:
        row = _find_equip_by_id(equip, prefer_id)
        rule = f"id={prefer_id}"
    if row is None and keyword:
        row = _find_equip_by_name_like(equip, keyword, extra_regex)
        rule = f"name~{keyword}" + (f" & {extra_regex}" if extra_regex else "")
    if row is None:
        return NodeResolved(block, line, node, rule or "未匹配", "", "", "", 0)

    eid = _as_text(row.get("设备id", ""))
    ename = _as_text(row.get("设备名称", ""))
    base = _as_text(row.get("设备状态", ""))

    base_level = _status_to_level(base)
    overlay = _overlay_faults_level(eid, faults_in_range)
    final = max(base_level, overlay)

    return NodeResolved(block, line, node, rule, eid, ename, base, final)


def worst_level(levels: List[int]) -> int:
    return max(levels) if levels else 0


def merge_nodes(block: str, line: str, node: str, nodes: List[NodeResolved], match_rule: str) -> NodeResolved:
    # 合并状态：取最差
    lvl = worst_level([x.final_level for x in nodes])
    # 名称：优先展示匹配到的设备名称（可能两个）
    names = [x.equip_name for x in nodes if x.equip_name]
    ids = [x.equip_id for x in nodes if x.equip_id]
    disp_name = " / ".join(names) if names else " / ".join(ids)
    base_text = " / ".join([x.base_status_text for x in nodes if x.base_status_text])

    return NodeResolved(
        block=block,
        line=line,
        node=node,
        match_rule=match_rule,
        equip_id=" / ".join(ids),
        equip_name=disp_name,
        base_status_text=base_text,
        final_level=lvl,
    )


def display_label(x: NodeResolved) -> str:
    """
    ✅ 按你的要求：直接显示匹配出来的设备名称
    - 优先：equip_name
    - 其次：equip_id
    - 再其次：node
    """
    return x.equip_name or x.equip_id or x.node


# ============================================================
# Streamlit page
# ============================================================
st.set_page_config(page_title="设备健康", layout="wide")
st.title("🧭 设备健康（控制塔）")
st.caption("按工艺流程展示关键设备状态（预处理2线 / 水解酸化4线 / 离心过滤 / 除臭）")

if DEBUG:
    try:
        st.sidebar.caption(f"DEBUG(secrets) raw: {st.secrets.get('DEBUG', False)}")
    except StreamlitSecretNotFoundError:
        st.sidebar.caption("DEBUG(secrets) raw: <no secrets>")

if "device_source_mode" not in st.session_state:
    st.session_state.device_source_mode = "上传Excel"

# ------------------------------------------------------------
# Load data from session_state first (so time console can render above data source)
# ------------------------------------------------------------
equip = pd.DataFrame()
faults = pd.DataFrame()
mode = st.session_state.get("device_source_mode", "上传Excel")

if mode == "上传Excel":
    b = st.session_state.get("device_uploaded_bytes", b"")
    n = st.session_state.get("device_uploaded_name", "uploaded.xlsx")
    if b:
        equip, faults = load_all_data_from_bytes(n, b)
else:
    p = st.session_state.get("device_xlsx_path", "")
    if p:
        equip, faults = load_all_data_from_path(p)

# sidebar containers (order requirement)
time_box = st.sidebar.container()
filter_box = st.sidebar.container()
data_box = st.sidebar.container()

# ------------------------------------------------------------
# If no data: show hint in time section, then data source below
# ------------------------------------------------------------
if equip.empty and faults.empty:
    with time_box:
        st.markdown("## 🕒 时间选择")
        st.info("请先在下方「数据源」选择 Excel（上传或路径），加载数据后即可选择时间范围。")

    with filter_box:
        st.divider()
        show_raw = st.checkbox("显示原始明细表", value=False, key="dev_show_raw")
        system_filter = []

    with data_box:
        st.divider()
        st.markdown("## ⚙️ 数据源")

        source_mode = st.radio(
            "选择数据来源",
            ["上传Excel", "本地路径"],
            horizontal=True,
            key="device_source_mode",
        )

        if source_mode == "上传Excel":
            uploaded = st.file_uploader("上传设备记录表（xlsx）", type=["xlsx"], key="device_uploader")
            st.caption("要求：包含工作表「异常记录」以及设备台账 Sheets（预处理/水解酸化/除臭）")
            if uploaded is not None:
                st.session_state.device_uploaded_name = uploaded.name
                st.session_state.device_uploaded_bytes = uploaded.getvalue()
                st.rerun()
        else:
            pp = st.text_input(
                "设备记录表路径（xlsx）",
                value=st.session_state.get("device_xlsx_path", ""),
                placeholder="/Users/xxx/Desktop/海吉星果蔬项目设备记录表2026_图片转路径版.xlsx",
                key="device_xlsx_path",
            )
            st.caption("要求：包含工作表「异常记录」以及设备台账 Sheets（预处理/水解酸化/除臭）")
            if pp and os.path.exists(pp):
                st.rerun()

    st.warning("未加载到数据。请在侧边栏下方选择数据源。")
    st.stop()

# ------------------------------------------------------------
# Time bounds (prefer faults date if available; else fallback)
# ------------------------------------------------------------
if not faults.empty:
    date_min = faults["日期"].dt.date.min()
    date_max = faults["日期"].dt.date.max()
else:
    date_min = date.today()
    date_max = date.today()

months = []
if not faults.empty:
    months = sorted(faults["日期"].dt.to_period("M").astype(str).unique().tolist())

if "dev_start_date" not in st.session_state:
    st.session_state.dev_end_date = date_max
    st.session_state.dev_start_date = max(date_min, (pd.Timestamp(date_max) - pd.Timedelta(days=30)).date())
if "dev_end_date" not in st.session_state:
    st.session_state.dev_end_date = date_max
if "dev_month_pick" not in st.session_state:
    st.session_state.dev_month_pick = "自定义"


def clamp_date(d: date) -> date:
    return min(max(d, date_min), date_max)


def apply_month_range(m: str) -> None:
    first = pd.Period(m).start_time.date()
    last = pd.Period(m).end_time.date()
    st.session_state.dev_start_date = clamp_date(first)
    st.session_state.dev_end_date = clamp_date(last)


def month_from_start() -> str:
    return pd.Timestamp(st.session_state.dev_start_date).to_period("M").strftime("%Y-%m")


def set_month_range_from_selectbox() -> None:
    m = st.session_state.dev_month_pick
    if m == "自定义":
        return
    apply_month_range(m)


with time_box:
    st.markdown("## 🕒 时间选择")

    if months:
        cur_m = month_from_start()
        st.selectbox(
            "快捷月份",
            options=["自定义"] + months,
            key="dev_month_pick",
            on_change=set_month_range_from_selectbox,
        )
        st.caption(f"当前：{cur_m}（按钮切月不改下拉显示）")

        cA, cB = st.columns(2)
        with cA:
            if st.button("◀ 上一月", use_container_width=True, key="dev_btn_prev_month"):
                if cur_m in months:
                    i = months.index(cur_m)
                    if i > 0:
                        apply_month_range(months[i - 1])
                        st.rerun()
        with cB:
            if st.button("下一月 ▶", use_container_width=True, key="dev_btn_next_month"):
                if cur_m in months:
                    i = months.index(cur_m)
                    if i < len(months) - 1:
                        apply_month_range(months[i + 1])
                        st.rerun()
    else:
        st.caption("未发现异常记录日期维度（或异常记录为空），仅提供自定义日期。")

    st.divider()

    start_date = st.date_input(
        "开始日期",
        value=st.session_state.dev_start_date,
        min_value=date_min,
        max_value=date_max,
        key="dev_start_date",
    )
    end_date = st.date_input(
        "结束日期",
        value=st.session_state.dev_end_date,
        min_value=date_min,
        max_value=date_max,
        key="dev_end_date",
    )

    if start_date > end_date:
        start_date, end_date = end_date, start_date
        st.session_state.dev_start_date = start_date
        st.session_state.dev_end_date = end_date

with filter_box:
    st.divider()
    show_raw = st.checkbox("显示原始明细表", value=False, key="dev_show_raw")
    system_filter = st.multiselect(
        "系统筛选（用于统计/明细）",
        options=sorted(faults["系统"].unique().tolist()) if not faults.empty else [],
        default=sorted(faults["系统"].unique().tolist()) if not faults.empty else [],
        key="dev_system_filter",
    )

with data_box:
    st.divider()
    st.markdown("## ⚙️ 数据源")

    source_mode = st.radio(
        "选择数据来源",
        ["上传Excel", "本地路径"],
        horizontal=True,
        key="device_source_mode",
    )

    if source_mode == "上传Excel":
        uploaded = st.file_uploader("上传设备记录表（xlsx）", type=["xlsx"], key="device_uploader")
        st.caption("要求：包含工作表「异常记录」以及设备台账 Sheets（预处理/水解酸化/除臭）")
        if uploaded is not None:
            st.session_state.device_uploaded_name = uploaded.name
            st.session_state.device_uploaded_bytes = uploaded.getvalue()
            st.rerun()
    else:
        pp = st.text_input(
            "设备记录表路径（xlsx）",
            value=st.session_state.get("device_xlsx_path", ""),
            placeholder="/Users/xxx/Desktop/海吉星果蔬项目设备记录表2026_图片转路径版.xlsx",
            key="device_xlsx_path",
        )
        st.caption("要求：包含工作表「异常记录」以及设备台账 Sheets（预处理/水解酸化/除臭）")
        if pp and os.path.exists(pp):
            st.rerun()

# ------------------------------------------------------------
# Filter faults in range
# ------------------------------------------------------------
faults_in_range = pd.DataFrame()
if not faults.empty:
    f = faults[(faults["日期"].dt.date >= start_date) & (faults["日期"].dt.date <= end_date)].copy()
    if system_filter:
        f = f[f["系统"].isin(system_filter)].copy()
    faults_in_range = f

# ============================================================
# Control Tower nodes (per your latest rules)
# ============================================================
tower_nodes: List[NodeResolved] = []

# ---- 预处理系统（2线）
# 1线：显示提桶机1/2、破袋机、闸板阀、细破碎机（粗破已拆除，不显示）
pre_line1 = [
    ("提桶机#1", "EQ_PRE_002", None),
    ("提桶机#2", "EQ_PRE_003", None),
    ("破袋机", None, "破袋"),
    ("闸板阀", None, "闸板"),
    ("细破碎机", None, "细破"),
]
for node, eid, kw in pre_line1:
    tower_nodes.append(
        resolve_node(
            equip=equip,
            faults_in_range=faults_in_range,
            block="预处理系统",
            line="1线",
            node=node,
            prefer_id=eid,
            keyword=kw,
        )
    )

# 2线：显示提桶机3/4、破袋机、闸板阀、粗破碎机、细破碎机
pre_line2 = [
    ("提桶机#3", "EQ_PRE_004", None),
    ("提桶机#4", "EQ_PRE_005", None),
    ("破袋机", None, "破袋"),
    ("闸板阀", None, "闸板"),
    ("粗破碎机", None, "粗破"),
    ("细破碎机", None, "细破"),
]
for node, eid, kw in pre_line2:
    tower_nodes.append(
        resolve_node(
            equip=equip,
            faults_in_range=faults_in_range,
            block="预处理系统",
            line="2线",
            node=node,
            prefer_id=eid,
            keyword=kw,
        )
    )

# ---- 水解酸化系统（4线）
for ln in [1, 2, 3, 4]:

    # 水解罐合并
    a = resolve_node(equip, faults_in_range, "水解酸化系统", f"{ln}线", f"水解罐{ln}-1", None, f"水解罐{ln}-1")
    b = resolve_node(equip, faults_in_range, "水解酸化系统", f"{ln}线", f"水解罐{ln}-2", None, f"水解罐{ln}-2")

    tower_nodes.append(
        merge_nodes(
            block="水解酸化系统",
            line=f"{ln}线",
            node=f"水解罐",
            nodes=[a, b],
            match_rule=f"merge(水解罐{ln}-1, 水解罐{ln}-2)",
        )
    )

    # 酸化罐
    tower_nodes.append(
        resolve_node(
            equip,
            faults_in_range,
            "水解酸化系统",
            f"{ln}线",
            "酸化罐",
            None,
            "酸化罐",
            extra_regex=fr"{ln}",
        )
    )

    # 水解循环泵
    pump1 = resolve_node(
        equip,
        faults_in_range,
        "水解酸化系统",
        f"{ln}线",
        f"水解循环泵{ln}",
        None,
        "水解循环泵",
        extra_regex=fr"{ln}",
    )

    # 酸化循环泵
    pump2 = resolve_node(
        equip,
        faults_in_range,
        "水解酸化系统",
        f"{ln}线",
        f"酸化循环泵{ln}",
        None,
        "酸化循环泵",
        extra_regex=fr"{ln}",
    )

    # 合并循环泵状态
    tower_nodes.append(
        merge_nodes(
            block="水解酸化系统",
            line=f"{ln}线",
            node="循环泵组",
            nodes=[pump1, pump2],
            match_rule=f"merge(循环泵{ln})",
        )
    )

# ---- 离心过滤系统：显示 EQ_ACID_001~004
for eid in ["EQ_ACID_001", "EQ_ACID_002", "EQ_ACID_003", "EQ_ACID_004"]:
    tower_nodes.append(
        resolve_node(
            equip=equip,
            faults_in_range=faults_in_range,
            block="离心过滤系统",
            line="—",
            node=eid,
            prefer_id=eid,
            keyword=None,
        )
    )

# ---- 除臭系统（合并）：显示 EQ_ODOR_001~006
for eid in ["EQ_ODOR_001", "EQ_ODOR_002", "EQ_ODOR_003", "EQ_ODOR_004", "EQ_ODOR_005", "EQ_ODOR_006"]:
    tower_nodes.append(
        resolve_node(
            equip=equip,
            faults_in_range=faults_in_range,
            block="除臭系统",
            line="合并",
            node=eid,
            prefer_id=eid,
            keyword=None,
        )
    )

# ============================================================
# Render: Control Tower (no "来料/地磅" column)
# ============================================================
st.subheader("🏗️ 工艺设备健康控制塔")
st.markdown(
    f"**图例：** {LEVEL_EMOJI[1]} 正常 ｜ {LEVEL_EMOJI[2]} 异常（区间内有事件/报警/带病） ｜ {LEVEL_EMOJI[3]} 停机/严重 ｜ {LEVEL_EMOJI[0]} 无数据/未匹配"
)


def group(block: str, line: Optional[str] = None) -> List[NodeResolved]:
    items = [x for x in tower_nodes if x.block == block]
    if line is not None:
        items = [x for x in items if x.line == line]
    return items


def render_list(items: List[NodeResolved]):
    for x in items:
        st.markdown(f"- {LEVEL_EMOJI[x.final_level]} **{display_label(x)}**")


# Top row: 3 columns -> 预处理 / 水解酸化 / 离心过滤+除臭
c1, c2, c3 = st.columns(3)

with c1:
    items_all = group("预处理系统")
    h, t = health_summary(items_all)
    st.markdown(f"### 🧩 预处理系统   **{h} / {t} 正常**")

    for ln in ["1线", "2线"]:
        items = group("预处理系统", ln)
        overall = worst_level([x.final_level for x in items])

        with st.expander(f"{LEVEL_EMOJI[overall]} 预处理{ln}", expanded=True):
            render_list(items)


with c2:
    items_all = group("水解酸化系统")
    h, t = health_summary(items_all)
    st.markdown(f"### 🧪 水解酸化系统   **{h} / {t} 正常**")

    for ln in ["1线", "2线", "3线", "4线"]:
        items = group("水解酸化系统", ln)
        overall = worst_level([x.final_level for x in items])

        with st.expander(f"{LEVEL_EMOJI[overall]} 水解酸化{ln}", expanded=True):
            render_list(items)


with c3:

    # 离心过滤系统
    items = group("离心过滤系统")
    h, t = health_summary(items)
    overall = worst_level([x.final_level for x in items])

    st.markdown(f"### {LEVEL_EMOJI[overall]} 离心过滤系统   **{h} / {t} 正常**")
    render_list(items)

    st.divider()

    # 除臭系统
    items = group("除臭系统", "合并")
    h, t = health_summary(items)
    overall = worst_level([x.final_level for x in items])

    st.markdown(f"### {LEVEL_EMOJI[overall]} 🌬️ 除臭系统   **{h} / {t} 正常**")
    render_list(items)

# Mapping diagnostics
with st.expander("🧩 节点映射诊断（建议首次上线先看一眼）", expanded=False):
    diag = pd.DataFrame([{
        "区块": x.block,
        "线别": x.line,
        "节点": x.node,
        "显示名": display_label(x),
        "状态": f"{LEVEL_EMOJI[x.final_level]} {LEVEL_TEXT[x.final_level]}",
        "匹配规则": x.match_rule,
        "设备id": x.equip_id,
        "设备名称": x.equip_name,
        "台账状态": x.base_status_text,
    } for x in tower_nodes])
    st.dataframe(diag, use_container_width=True, hide_index=True)

    missing = diag[(diag["设备id"] == "") & (diag["设备名称"] == "")]
    if len(missing) > 0:
        st.warning(
            f"有 {len(missing)} 个节点未匹配到设备台账（显示为⚪）。"
            "通常是设备名称/编号不一致导致；把这些行的台账真实名称发我，我帮你调匹配规则。"
        )

st.divider()

# ============================================================
# Analytics section (fault KPI + trend + Top10 + raw)
# ============================================================
st.subheader("📊 异常统计（用于复盘）")

if faults_in_range is None or faults_in_range.empty:
    st.info("所选时间范围内没有异常记录（或异常记录为空）。控制塔状态主要来自设备台账的“设备状态”。")
else:
    dff = faults_in_range.copy()
    total_events = int(len(dff))
    unique_devices = int(dff["设备id"].nunique()) if "设备id" in dff.columns else 0

    stop_mask = dff["是否停机"].astype(str).str.contains("是", na=False)
    stop_df = dff[stop_mask].copy()
    downtime_hours_sum = float(pd.to_numeric(stop_df["停机小时"], errors="coerce").fillna(0).sum())

    if len(stop_df) > 0:
        complete_cnt = int(((stop_df["_start_dt"].notna()) & (stop_df["_end_dt"].notna())).sum())
        completeness = complete_cnt / len(stop_df)
    else:
        completeness = 1.0

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("本期异常次数", f"{total_events}")
    k2.metric("本期停机总时长（小时）", f"{downtime_hours_sum:.1f}")
    k3.metric("涉及设备数", f"{unique_devices}")
    k4.metric("停机记录完整率", f"{completeness*100:.0f}%")

    st.divider()

    cA, cB = st.columns([2, 1])
    daily = dff.groupby(dff["日期"].dt.date).size().reset_index(name="异常次数")
    daily.columns = ["日期", "异常次数"]
    fig_trend = px.line(daily, x="日期", y="异常次数", markers=True, title="异常趋势（按天）")
    cA.plotly_chart(fig_trend, use_container_width=True)

    sys_cnt = dff.groupby("系统").size().reset_index(name="异常次数").sort_values("异常次数", ascending=False)
    fig_sys = px.bar(sys_cnt, x="系统", y="异常次数", title="系统分布（异常次数）")
    cB.plotly_chart(fig_sys, use_container_width=True)

    st.divider()

    grp = dff.groupby(["设备id", "设备名称", "系统"], dropna=False).agg(
        异常次数=("日期", "count"),
        停机总时长小时=("停机小时", lambda x: float(pd.to_numeric(x, errors="coerce").fillna(0).sum())),
        最近一次异常=("日期", "max"),
    ).reset_index()

    grp["最近一次异常"] = pd.to_datetime(grp["最近一次异常"], errors="coerce").dt.strftime("%Y-%m-%d")
    top10 = grp.sort_values(["异常次数", "停机总时长小时"], ascending=[False, False]).head(10).copy()
    top10.insert(0, "排名", range(1, len(top10) + 1))

    st.subheader("🔝 Top 10 异常设备")
    st.dataframe(top10, use_container_width=True, hide_index=True)

    if show_raw:
        st.subheader("📄 原始明细（过滤后）")
        preferred = [
            "日期", "系统", "设备id", "设备名称", "异常类别",
            "异常描述", "是否停机", "_start_dt", "_end_dt",
            "停机小时", "处理措施", "图片路径/链接", "记录人"
        ]
        cols = [c for c in preferred if c in dff.columns]
        rest = [c for c in dff.columns if c not in cols and not c.startswith("_")]
        st.dataframe(dff[cols + rest], use_container_width=True, hide_index=True)