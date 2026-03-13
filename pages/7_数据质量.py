# -*- coding: utf-8 -*-
from __future__ import annotations

import pandas as pd
import streamlit as st

st.set_page_config(page_title="数据质量", page_icon="🧪", layout="wide")

from utils.bootstrap import bootstrap_page
from utils.paths import get_db_path
from utils.snapshot import get_active_snapshot_id
from utils.data_access import load_daily_ops_data

bootstrap_page("数据质量")

st.title("🧪 数据质量")
st.caption("用于检查当前快照下运行数据的完整性、连续性和异常值。")

db_path = get_db_path()
snapshot_id = get_active_snapshot_id(db_path)
df = load_daily_ops_data(db_path, snapshot_id=snapshot_id)

if df.empty:
    st.warning("当前快照下暂无运行数据。")
    st.stop()

df = df.copy()
df["date"] = pd.to_datetime(df["date"], errors="coerce")
df = df[df["date"].notna()].sort_values("date")

st.markdown("### 基本情况")
c1, c2, c3, c4 = st.columns(4)
c1.metric("记录数", len(df))
c2.metric("开始日期", str(df["date"].min().date()))
c3.metric("结束日期", str(df["date"].max().date()))
c4.metric("覆盖天数", df["date"].nunique())

st.markdown("### 缺失值检查")
key_cols = [c for c in ["incoming_ton", "slag_ton", "slag_total_ton", "water_m3", "elec_meter_kwh"] if c in df.columns]
missing_rows = []
for col in key_cols:
    missing_rows.append({
        "字段": col,
        "缺失数": int(df[col].isna().sum()),
        "缺失率": df[col].isna().mean(),
    })
missing_df = pd.DataFrame(missing_rows)
st.dataframe(missing_df, use_container_width=True, hide_index=True)

st.markdown("### 日期连续性检查")
full_range = pd.date_range(df["date"].min(), df["date"].max(), freq="D")
missing_dates = sorted(set(full_range.date) - set(df["date"].dt.date))
if missing_dates:
    st.warning(f"存在 {len(missing_dates)} 个缺失日期。")
    st.dataframe(pd.DataFrame({"缺失日期": missing_dates}), use_container_width=True, hide_index=True)
else:
    st.success("日期连续性正常。")

st.markdown("### 异常值检查")
issues = []

if "incoming_ton" in df.columns:
    bad = df[df["incoming_ton"] < 0]
    if not bad.empty:
        issues.append(("incoming_ton < 0", bad[["date", "incoming_ton"]]))

if "slag_total_ton" in df.columns:
    bad = df[df["slag_total_ton"] < 0]
    if not bad.empty:
        issues.append(("slag_total_ton < 0", bad[["date", "slag_total_ton"]]))

if "water_m3" in df.columns:
    bad = df[df["water_m3"] < 0]
    if not bad.empty:
        issues.append(("water_m3 < 0", bad[["date", "water_m3"]]))

if "incoming_ton" in df.columns and "slag_total_ton" in df.columns:
    tmp = df.copy()
    tmp["slag_rate"] = tmp["slag_total_ton"] / tmp["incoming_ton"]
    bad = tmp[(tmp["incoming_ton"] > 0) & (tmp["slag_rate"] > 1.0)]
    if not bad.empty:
        issues.append(("slag_rate > 1", bad[["date", "incoming_ton", "slag_total_ton", "slag_rate"]]))

if not issues:
    st.success("未发现明显异常值。")
else:
    for title, bad_df in issues:
        st.error(title)
        st.dataframe(bad_df, use_container_width=True, hide_index=True)

st.markdown("### 原始数据预览")
st.dataframe(df.sort_values("date", ascending=False), use_container_width=True, hide_index=True)