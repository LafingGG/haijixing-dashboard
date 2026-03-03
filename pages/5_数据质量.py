# -*- coding: utf-8 -*-
from __future__ import annotations

import os, sqlite3
import pandas as pd
import streamlit as st

from utils.definitions import DEFINITIONS_MD, DEFINITIONS_VERSION

from utils.paths import get_db_path
DB_PATH = get_db_path()


import os
import streamlit as st
import sqlite3
import pandas as pd

from utils.paths import get_db_path

DB_PATH = get_db_path()

# st.sidebar.markdown("### 🔎 Debug")
# st.sidebar.caption(f"DB_PATH: `{DB_PATH}`")
# st.sidebar.caption(f"exists: `{os.path.exists(DB_PATH)}`")
# if os.path.exists(DB_PATH):
#     st.sidebar.caption(f"size: `{os.path.getsize(DB_PATH)} bytes`")

# @st.cache_data(ttl=300)
# def _debug_read_one_row(db_path: str):
#     conn = sqlite3.connect(db_path)
#     df = pd.read_sql_query("SELECT * FROM fact_daily_ops ORDER BY date LIMIT 1", conn, parse_dates=["date"])
#     conn.close()
#     return list(df.columns), df.to_dict(orient="records")[0]

# if os.path.exists(DB_PATH):
#     try:
#         cols, row0 = _debug_read_one_row(DB_PATH)
#         st.sidebar.caption(f"cols: `{len(cols)}`")
#         with st.sidebar.expander("columns"):
#             st.write(cols)
#         with st.sidebar.expander("row[0]"):
#             st.write(row0)
#     except Exception as e:
#         st.sidebar.error("Read DB failed:")
#         st.sidebar.exception(e)
#         st.stop()
# else:
#     st.sidebar.error("DB file not found. Stop.")
#     st.stop()

@st.cache_data(ttl=5)
def load_data() -> pd.DataFrame:
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT * FROM fact_daily_ops ORDER BY date", conn, parse_dates=["date"])
    conn.close()
    return df

st.set_page_config(page_title="数据质量 | 海吉星果蔬项目", layout="wide")
st.title("数据质量")

with st.expander("📌口径说明", expanded=False):
    st.markdown(DEFINITIONS_MD)

df = load_data()
if df.empty:
    st.warning("数据库为空。请先导入 Excel。")
    st.stop()

required_cols = ["date", "incoming_ton", "slag_total_ton", "water_m3"]
missing_cols = [c for c in required_cols if c not in df.columns]
if missing_cols:
    st.error(f"数据缺少必要字段：{missing_cols}")
    st.stop()

st.subheader("字段缺失率（全量）")
miss = (df.isna().mean() * 100).sort_values(ascending=False).to_frame("缺失率%")
st.dataframe(miss, use_container_width=True)

st.divider()
n = st.slider("查看最近 N 天", min_value=7, max_value=60, value=14, step=1)
dfn = df.sort_values("date").tail(n).copy()

st.subheader(f"最近 {n} 天缺失项")
miss_n = (dfn.isna().mean() * 100).sort_values(ascending=False).to_frame("缺失率%")
st.dataframe(miss_n, use_container_width=True)

st.subheader("疑似异常（简单规则）")
checks = []
for _, r in dfn.iterrows():
    issues = []
    if pd.isna(r["incoming_ton"]) or r["incoming_ton"] <= 0:
        issues.append("来料吨为空/<=0")
    if pd.isna(r["slag_total_ton"]) or r["slag_total_ton"] <= 0:
        issues.append("出渣合计为空/<=0")
    if pd.isna(r["water_m3"]):
        issues.append("用水量为空")
    if issues:
        checks.append({"date": r["date"].date(), "issues": "；".join(issues)})

issues_df = pd.DataFrame(checks)
st.dataframe(issues_df, use_container_width=True, hide_index=True)

csv = issues_df.to_csv(index=False).encode("utf-8-sig")
st.download_button(
    f"下载最近{n}天异常清单 CSV",
    data=csv,
    file_name=f"dq_issues_last{n}_{DEFINITIONS_VERSION}.csv",
    mime="text/csv",
)