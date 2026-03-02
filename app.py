# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sqlite3
import pandas as pd
import streamlit as st
import tempfile
from etl.parse_excel import parse_workbook
from utils.config import load_thresholds, save_thresholds, Thresholds

DB_PATH = os.path.join(os.path.dirname(__file__), "db", "ops.sqlite")

from utils.definitions import DEFINITIONS_VERSION

@st.cache_data(ttl=5)

def upsert_df_to_sqlite(db_path: str, df: pd.DataFrame) -> int:
    """把 parse_workbook() 的结果 upsert 到 SQLite。返回写入行数。"""
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    create_sql = """
    CREATE TABLE IF NOT EXISTS fact_daily_ops (
        date TEXT PRIMARY KEY,
        incoming_trips REAL,
        incoming_ton REAL,
        slag_trips REAL,
        slag_ton REAL,
        slag_total_ton REAL,
        slurry_m3 REAL,
        water_meter_m3 REAL,
        water_m3 REAL,
        elec_meter_x1e3kwh REAL,
        elec_meter_kwh REAL,
        proj_flow_m3 REAL,
        to_wwtp_m3 REAL,
        wwtp_flow_m3 REAL,
        arrive_wwtp_m3 REAL,
        source_sheet TEXT
    );
    """

    df2 = df.copy()
    df2["date"] = df2["date"].dt.strftime("%Y-%m-%d")

    cols = list(df2.columns)
    placeholders = ",".join(["?"] * len(cols))
    col_list = ",".join(cols)
    update_list = ",".join([f"{c}=excluded.{c}" for c in cols if c != "date"])

    sql = f"""
    INSERT INTO fact_daily_ops ({col_list})
    VALUES ({placeholders})
    ON CONFLICT(date) DO UPDATE SET {update_list};
    """

    conn = sqlite3.connect(db_path)
    conn.execute(create_sql)
    conn.executemany(sql, df2.itertuples(index=False, name=None))
    conn.commit()
    conn.close()

    return len(df2)

def build_import_report(df_new: pd.DataFrame) -> dict:
    """
    根据 parse_workbook() 的结果生成导入质量报告（不依赖改 ETL）。
    返回 dict，便于页面展示。
    """
    report = {}

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

    # 派生指标
    df["slag_rate"] = df["slag_total_ton"] / df["incoming_ton"]
    df["water_intensity"] = df["water_m3"] / df["incoming_ton"]

    from utils.config import load_thresholds
    th = load_thresholds()
    TH_SALGRATE = th.slag_rate_high
    TH_WATER_INT = th.water_intensity_high

    # 异常规则（你可按实际再加）
    issues = []
    for _, r in df.iterrows():
        day = r["date"].date()

        # 来料异常
        if pd.isna(r["incoming_ton"]) or r["incoming_ton"] <= 0:
            issues.append((day, "来料吨为空/<=0"))
            continue  # 没来料时后面的强度就不算了

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

    # 聚合异常
    if issues:
        df_issues = pd.DataFrame(issues, columns=["date", "issue"])
        df_issues = df_issues.groupby("date")["issue"].apply(lambda x: "；".join(sorted(set(x)))).reset_index()
    else:
        df_issues = pd.DataFrame(columns=["date", "issue"])

    report["issues_df"] = df_issues
    return report

def load_data() -> pd.DataFrame:
    if not os.path.exists(DB_PATH):
        return pd.DataFrame()
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT * FROM fact_daily_ops ORDER BY date", conn, parse_dates=["date"])
    conn.close()
    return df


st.set_page_config(page_title="海吉星果蔬项目 | 本地运营看板", layout="wide")
st.title("海吉星果蔬项目 · 本地运营看板")

st.subheader("导入 Excel（上传后自动入库）")

uploaded = st.file_uploader(
    "选择你的月度运营 Excel（.xlsx）",
    type=["xlsx"],
    accept_multiple_files=False,
)

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


if uploaded is not None:
    with st.spinner("正在解析并导入到本地数据库…"):
        # 写入临时文件（parse_workbook 需要文件路径）
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
            tmp.write(uploaded.getbuffer())
            tmp_path = tmp.name

        try:
            df_new = parse_workbook(tmp_path)
            if df_new.empty:
                st.error("没有解析到数据：请检查 sheet/表头行是否符合当前解析规则。")
            else:
                # ✅ 先生成报告并展示
                report = build_import_report(df_new)

                st.success(
                    f"解析成功：{report['rows']} 天，覆盖 {report['date_min']} ~ {report['date_max']}"
                )

                if report["dup_dates"]:
                    st.warning(f"检测到重复日期（可能重复录入）：{', '.join(map(str, report['dup_dates'][:10]))}"
                            + (" ..." if len(report["dup_dates"]) > 10 else ""))

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
                # ✅ 再写入数据库
                n = upsert_df_to_sqlite(DB_PATH, df_new)

                st.cache_data.clear()
                st.success(f"导入完成：写入/更新 {n} 行数据。")
                st.rerun()
        finally:
            try:
                os.remove(tmp_path)
            except Exception:
                pass

st.divider()

df = load_data()

if df.empty:
    st.warning("还没有数据。请先运行：`python -m etl.load_to_db \"你的excel路径.xlsx\"` 导入数据。")
    st.stop()

st.caption(f"数据范围：{df['date'].min().date()} ~ {df['date'].max().date()}（共 {len(df)} 天）")

st.page_link("pages/1_总览.py", label="➡️ 打开：总览", icon="📊")
st.page_link("pages/2_物料平衡.py", label="➡️ 打开：物料平衡", icon="🧪")
st.page_link("pages/3_水电能耗.py", label="➡️ 打开：水电能耗", icon="⚡")
##st.page_link("pages/4_去水厂核对.py", label="➡️ 打开：去水厂核对", icon="🏭")
st.page_link("pages/5_数据质量.py", label="➡️ 打开：数据质量", icon="🧹")

st.divider()
st.subheader("快速预览（最近 14 天）")
df14 = df.tail(14).copy()
st.dataframe(df14, use_container_width=True, hide_index=True)
