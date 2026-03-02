# -*- coding: utf-8 -*-
from __future__ import annotations

import os, sqlite3
import numpy as np
import pandas as pd
import streamlit as st
import plotly.express as px

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "db", "ops.sqlite")

@st.cache_data(ttl=5)
def load_data() -> pd.DataFrame:
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT * FROM fact_daily_ops ORDER BY date", conn, parse_dates=["date"])
    conn.close()
    return df

st.set_page_config(page_title="物料平衡 | 海吉星果蔬项目", layout="wide")
st.title("物料平衡")


from utils.definitions import DEFINITIONS_MD, DEFINITIONS_VERSION

with st.expander("📌口径说明", expanded=False):
    st.markdown(DEFINITIONS_MD)

df = load_data()
if df.empty:
    st.warning("数据库为空。请先导入 Excel。")
    st.stop()

min_d, max_d = df["date"].min(), df["date"].max()
start, end = st.slider("选择区间", min_value=min_d.date(), max_value=max_d.date(),
                       value=(min_d.date(), max_d.date()))
dfr = df[(df["date"].dt.date>=start)&(df["date"].dt.date<=end)].copy()

incoming = float(dfr["incoming_ton"].sum(skipna=True))
slag = float(dfr["slag_total_ton"].sum(skipna=True))
rate = np.nan if incoming==0 else slag/incoming

k1,k2,k3 = st.columns(3)
k1.metric("来料(吨)", f"{incoming:,.0f}")
k2.metric("出渣合计(吨)", f"{slag:,.0f}")
k3.metric("出渣率(吨/吨)", "-" if np.isnan(rate) else f"{rate:.3f}")

st.divider()
st.subheader("出渣率（日）")

valid_in = dfr["incoming_ton"].where(dfr["incoming_ton"] > 0)
dfr["slag_rate"] = dfr["slag_total_ton"] / valid_in

fig = px.line(dfr, x="date", y="slag_rate", title="出渣率(日)")
st.plotly_chart(fig, use_container_width=True)

st.subheader("出渣率分布")
hist_df = dfr.dropna(subset=["slag_rate"])
hist_df = hist_df[hist_df["slag_rate"].between(0, 2)]
fig = px.histogram(hist_df, x="slag_rate", nbins=30, title="出渣率分布")
st.plotly_chart(fig, use_container_width=True)

st.subheader("异常日（出渣率过高）")
threshold = st.number_input("阈值（出渣率 > 阈值 列为异常）", value=0.75, step=0.05, min_value=0.0, max_value=2.0)
abn = (
    dfr.dropna(subset=["slag_rate"])
       .loc[dfr["slag_rate"] > threshold, ["date","incoming_ton","slag_total_ton","slag_rate"]]
       .sort_values("slag_rate", ascending=False)
)
st.dataframe(abn, use_container_width=True, hide_index=True)
