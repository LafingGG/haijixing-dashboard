# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sqlite3
import tempfile
from typing import Dict, Any, Tuple

import pandas as pd
import streamlit as st

from etl.parse_excel import parse_workbook
from etl.load_to_db import load_xlsx_to_db
from utils.config import load_thresholds, save_thresholds, Thresholds
from utils.paths import get_db_path
from utils.definitions import DEFINITIONS_VERSION


DB_PATH = get_db_path()


# -------------------------
# Data access (read-only)
# -------------------------
@st.cache_data(ttl=60)
def load_data(db_path: str) -> pd.DataFrame:
    if not os.path.exists(db_path):
        return pd.DataFrame()
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query(
        "SELECT * FROM fact_daily_ops ORDER BY date",
        conn,
        parse_dates=["date"],
    )
    conn.close()
    return df


def get_db_stats(db_path: str) -> Tuple[int, str | None]:
    """Step4: 校验入库是否真的生效（行数 + 最新日期）"""
    if not os.path.exists(db_path):
        return 0, None
    conn = sqlite3.connect(db_path)
    cnt = conn.execute("SELECT COUNT(*) FROM fact_daily_ops").fetchone()[0]
    latest = conn.execute("SELECT MAX(date) FROM fact_daily_ops").fetchone()[0]
    conn.close()
    return int(cnt), latest


# -------------------------
# Import report (quality)
# -------------------------
def build_import_report(df_new: pd.DataFrame) -> Dict[str, Any]:
    """
    根据 parse_workbook() 的结果生成导入质量报告。
    """
    report: Dict[str, Any] = {}

    df = df_new.copy().sort_values("date")
    report["rows"] = int(len(df))
    report["date_min"] = df["date"].min().date() if len(df) else None
    report["date_max"] = df["date"].max().date() if len(df) else None

    # 重复日期检查
    dup = df["date"].dt.date[df["date"].dt.date.duplicated(keep=False)]
    report["dup_dates"] = sorted(list(set(dup.tolist()))) if len(dup) else []

    # 缺失率（Top 10）
    miss = (df.isna().mean() * 100).sort_values(ascending=False)
    report["missing_top"] = miss.head(10)

    # 派生指标（注意除零）
    df["slag_rate"] = df["slag_total_ton"] / df["incoming_ton"].replace(0, pd.NA)
    df["water_intensity"] = df["water_m3"] / df["incoming_ton"].replace(0, pd.NA)

    th = load_thresholds()
    TH_SALGRATE = th.slag_rate_high
    TH_WATER_INT = th.water_intensity_high

    issues = []
    for _, r in df.iterrows():
        day = r["date"].date()

        # 来料异常
        if pd.isna(r["incoming_ton"]) or r["incoming_ton"] <= 0:
            issues.append((day, "来料吨为空/<=0"))
            continue

        # 出渣异常
        if pd.isna(r["slag_total_ton"]) or r["slag_total_ton"] <= 0:
            issues.append((day, "出渣合计为空/<=0"))
        else:
            if pd.notna(r["slag_rate"]) and r["slag_rate"] > TH_SALGRATE:
                issues.append((day, f"出渣率偏高（>{TH_SALGRATE}）"))

        # 用水异常
        if pd.isna(r["water_m3"]):
            issues.append((day, "用水量为空"))
        elif r["water_m3"] < 0:
            issues.append((day, "用水量为负"))
        else:
            if pd.notna(r["water_intensity"]) and r["water_intensity"] > TH_WATER_INT:
                issues.append((day, f"水耗强度偏高（>{TH_WATER_INT} m³/吨）"))

    if issues:
        df_issues = pd.DataFrame(issues, columns=["date", "issue"])
        df_issues = (
            df_issues.groupby("date")["issue"]
            .apply(lambda x: "；".join(sorted(set(x))))
            .reset_index()
        )
    else:
        df_issues = pd.DataFrame(columns=["date", "issue"])

    report["issues_df"] = df_issues
    return report


def import_excel_to_db(uploaded_file, db_path: str) -> Tuple[int, str, Dict[str, Any]]:
    """
    上传 Excel -> 解析 -> 质量报告 -> 入库（upsert） -> 返回 (rows, db_path, report)
    """
    # 1) 保存到临时文件（parse_workbook / ETL 都需要路径）
    with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
        tmp.write(uploaded_file.getbuffer())
        tmp_path = tmp.name

    try:
        # 2) 先解析做质量报告
        df_new = parse_workbook(tmp_path)
        if df_new.empty:
            raise ValueError("没有解析到数据：请检查 sheet/表头行是否符合当前解析规则。")

        report = build_import_report(df_new)

        # 3) 再调用 ETL 入库（统一用同一个 db_path）
        n, p = load_xlsx_to_db(tmp_path, db_path)

        return n, p, report

    finally:
        try:
            os.remove(tmp_path)
        except Exception:
            pass


# -------------------------
# UI
# -------------------------
st.set_page_config(page_title="海吉星果蔬项目 | 本地运营看板", layout="wide")
st.title("海吉星果蔬项目 · 本地运营看板")

# 可选：在 sidebar 显示当前 DB 路径（便于排障，后续你可删掉）
with st.sidebar:
    st.markdown("### 🗄️ 数据库")
    st.caption(f"DB_PATH: `{DB_PATH}`")
    st.caption(f"exists: `{os.path.exists(DB_PATH)}`")
    if os.path.exists(DB_PATH):
        st.caption(f"size: `{os.path.getsize(DB_PATH)} bytes`")

st.subheader("导入 Excel（上传后自动入库）")
uploaded = st.file_uploader("选择你的月度运营 Excel（.xlsx）", type=["xlsx"])

# Step4：入库前先显示当前 DB 状态（不依赖缓存）
try:
    if os.path.exists(DB_PATH):
        cnt0, latest0 = get_db_stats(DB_PATH)
        st.caption(f"当前库：行数 {cnt0}，最新日期 {latest0}")
except Exception:
    # 如果表还没建，先不显示
    pass

if uploaded is not None:
    if st.button("开始导入到数据库", type="primary"):
        try:
            with st.spinner("正在解析 → 生成质量报告 → 入库..."):
                n, p, report = import_excel_to_db(uploaded, DB_PATH)

            st.success(f"导入完成：{n} 行 → {p}")
            st.success(f"解析覆盖：{report['rows']} 天，{report['date_min']} ~ {report['date_max']}")

            if report["dup_dates"]:
                st.warning(
                    "检测到重复日期（可能重复录入）："
                    + ", ".join(map(str, report["dup_dates"][:10]))
                    + (" ..." if len(report["dup_dates"]) > 10 else "")
                )

            with st.expander("查看导入质量报告", expanded=True):
                c1, c2, c3 = st.columns(3)
                c1.metric("解析天数", report["rows"])
                c2.metric("开始日期", str(report["date_min"]))
                c3.metric("结束日期", str(report["date_max"]))

                st.markdown("**字段缺失率 Top 10（%）**")
                st.dataframe(report["missing_top"].to_frame("missing_%"), use_container_width=True)

                st.markdown("**异常日清单**")
                st.dataframe(report["issues_df"], use_container_width=True, hide_index=True)

                csv_issues = report["issues_df"].to_csv(index=False).encode("utf-8-sig")
                st.download_button(
                    "下载异常日清单 CSV",
                    data=csv_issues,
                    file_name=f"import_issues_{DEFINITIONS_VERSION}.csv",
                    mime="text/csv",
                )

            # Step4：入库后校验
            cnt1, latest1 = get_db_stats(DB_PATH)
            st.info(f"入库校验：当前行数 {cnt1}，最新日期 {latest1}")

            # 关键：清缓存并刷新，让其它页面立刻读到新数据
            st.cache_data.clear()
            st.rerun()

        except Exception as e:
            st.error(f"导入失败：{e}")


# 阈值设置
th = load_thresholds()
with st.expander("⚙️ 阈值设置（报警规则）", expanded=False):
    c1, c2, c3 = st.columns(3)
    with c1:
        slag_rate_high = st.number_input("出渣率阈值(吨/吨)", value=float(th.slag_rate_high), step=0.05)
        elec_intensity_high = st.number_input("电耗强度阈值(kWh/吨)", value=float(th.elec_intensity_high), step=5.0)
    with c2:
        water_intensity_high = st.number_input("水耗强度阈值(m³/吨)", value=float(th.water_intensity_high), step=0.05)
        water_m3_high = st.number_input("用水量绝对值阈值(m³)", value=float(th.water_m3_high), step=100.0)
    with c3:
        daily_elec_kwh_high = st.number_input("每日电耗绝对值阈值(kWh)", value=float(th.daily_elec_kwh_high), step=100.0)

    if st.button("保存阈值", type="primary"):
        save_thresholds(Thresholds(
            slag_rate_high=slag_rate_high,
            water_intensity_high=water_intensity_high,
            elec_intensity_high=elec_intensity_high,
            water_m3_high=water_m3_high,
            daily_elec_kwh_high=daily_elec_kwh_high,
        ))
        st.success("阈值已保存，刷新后生效。")
        st.rerun()


st.divider()

# 主数据加载与导航
df = load_data(DB_PATH)

if df.empty:
    st.warning("还没有数据。请上传 Excel 导入数据。")
    st.stop()

st.caption(f"数据范围：{df['date'].min().date()} ~ {df['date'].max().date()}（共 {len(df)} 天）")

st.page_link("pages/1_总览.py", label="➡️ 打开：总览", icon="📊")
st.page_link("pages/2_物料平衡.py", label="➡️ 打开：物料平衡", icon="🧪")
st.page_link("pages/3_水电能耗.py", label="➡️ 打开：水电能耗", icon="⚡")
st.page_link("pages/5_数据质量.py", label="➡️ 打开：数据质量", icon="🧹")

st.divider()
st.subheader("快速预览（最近 14 天）")
st.dataframe(df.tail(14), use_container_width=True, hide_index=True)