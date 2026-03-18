# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sqlite3

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

st.set_page_config(page_title="总览 | 海吉星果蔬项目", layout="wide")

from utils.bootstrap import bootstrap_page
from utils.config import get_bucket_to_ton
from utils.cost_store import build_cost_dashboard_dataset
from utils.data_access import (
    add_daily_electricity,
    load_daily_ops_data,
    safe_div,
)
from utils.definitions import DEFINITIONS_MD, DEFINITIONS_VERSION
from utils.device_summary import get_home_device_status
from utils.ops_kpi import get_latest_ops_kpis, classify_data_freshness
from utils.paths import get_db_path
from utils.sidebar_filters import render_global_sidebar_by_df
from utils.snapshot import get_active_snapshot_id


DB_PATH = get_db_path()
user = bootstrap_page(DB_PATH)
ACTIVE_SNAPSHOT_ID = get_active_snapshot_id(DB_PATH)
BUCKET_TO_TON = get_bucket_to_ton()


def _as_bool(v) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return False
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}


DEBUG = _as_bool(st.secrets.get("DEBUG", False))


def polish_fig(fig, title: str | None = None, height: int = 360):
    if title:
        fig.update_layout(title=title)
    fig.update_layout(
        height=height,
        hovermode="x unified",
        margin=dict(l=10, r=10, t=50, b=10),
        legend_title_text="",
    )
    fig.update_xaxes(showgrid=True, gridcolor="rgba(255,255,255,0.06)")
    fig.update_yaxes(showgrid=True, gridcolor="rgba(255,255,255,0.06)")
    return fig


def pct_change(cur, prev):
    if prev is None or pd.isna(prev) or prev == 0:
        return np.nan
    if cur is None or pd.isna(cur):
        return np.nan
    return (cur - prev) / prev


def month_range(df: pd.DataFrame, target_period: pd.Period):
    if df.empty:
        return df.copy()
    mask = df["date"].dt.to_period("M") == target_period
    return df.loc[mask].copy()


@st.cache_data(ttl=60)
def get_prev_month_cost_kpi(db_path: str):
    data = build_cost_dashboard_dataset(db_path)
    monthly = data["monthly_with_ops"]

    if monthly.empty:
        return None

    monthly = monthly.copy()
    monthly["analysis_month"] = monthly["analysis_month"].astype(str)
    monthly = monthly.sort_values("analysis_month")

    prev_month = (pd.Timestamp.today().to_period("M") - 1).strftime("%Y-%m")
    hit = monthly[monthly["analysis_month"] == prev_month]

    row = hit.iloc[-1] if not hit.empty else monthly.iloc[-1]

    return {
        "month": row["analysis_month"],
        "total_cost": row["total_cost"],
        "ton_cost": row["ton_cost"],
        "incoming_ton": row["incoming_ton"],
    }


def _sum_col(df: pd.DataFrame, col: str) -> float:
    if col not in df.columns:
        return 0.0
    return float(pd.to_numeric(df[col], errors="coerce").sum(skipna=True))


def _latest_non_empty_value(df: pd.DataFrame, col: str):
    if col not in df.columns or df.empty:
        return np.nan
    tmp = df[["date", col]].copy()
    tmp[col] = pd.to_numeric(tmp[col], errors="coerce")
    tmp = tmp.dropna(subset=[col])
    if tmp.empty:
        return np.nan
    return tmp.sort_values("date").iloc[-1][col]


def _build_summary(df: pd.DataFrame) -> dict:
    incoming_ton = _sum_col(df, "incoming_ton")
    incoming_trips = _sum_col(df, "incoming_trips")
    slag_total_ton = _sum_col(df, "slag_total_ton")
    water_m3 = _sum_col(df, "water_m3")
    daily_elec_kwh = _sum_col(df, "daily_elec_kwh")
    incoming_bucket_count = _sum_col(df, "incoming_bucket_count")
    centrifuge_feed_m3 = _sum_col(df, "centrifuge_feed_m3")
    line1_feed_bucket_count = _sum_col(df, "line1_feed_bucket_count")
    line2_feed_bucket_count = _sum_col(df, "line2_feed_bucket_count")
    compress_bucket_count = _sum_col(df, "compress_bucket_count")

    return {
        "incoming_ton": incoming_ton,
        "incoming_trips": incoming_trips,
        "slag_total_ton": slag_total_ton,
        "water_m3": water_m3,
        "daily_elec_kwh": daily_elec_kwh,
        "incoming_bucket_count": incoming_bucket_count,
        "centrifuge_feed_m3": centrifuge_feed_m3,
        "line1_feed_bucket_count": line1_feed_bucket_count,
        "line2_feed_bucket_count": line2_feed_bucket_count,
        "compress_bucket_count": compress_bucket_count,
        "slag_rate": safe_div(slag_total_ton, incoming_ton),
        "water_intensity": safe_div(water_m3, incoming_ton),
        "elec_intensity": safe_div(daily_elec_kwh, incoming_ton),
        "slurry_per_ton": safe_div(centrifuge_feed_m3, incoming_ton),
        "avg_incoming_ton_per_day": safe_div(incoming_ton, len(df)),
    }


def _get_status_issues(dfr: pd.DataFrame) -> list[str]:
    issues: list[str] = []
    if dfr.empty:
        return ["当前区间无数据"]

    if "incoming_ton" in dfr.columns and dfr["incoming_ton"].isna().any():
        issues.append("来料吨存在缺失")
    if "slag_total_ton" in dfr.columns and dfr["slag_total_ton"].isna().any():
        issues.append("出渣合计存在缺失")
    if "water_m3" in dfr.columns and dfr["water_m3"].isna().any():
        issues.append("用水量存在缺失")
    if "daily_elec_kwh" in dfr.columns and dfr["daily_elec_kwh"].notna().sum() == 0:
        issues.append("电表抄表点不足，无法生成每日电耗")
    if "centrifuge_feed_m3" in dfr.columns and dfr["centrifuge_feed_m3"].fillna(0).sum() == 0:
        issues.append("离心机真实进料量尚未形成连续数据")
    return issues


def _render_status_banner(dfr: pd.DataFrame, start_date, end_date):
    issues = _get_status_issues(dfr)
    if len(issues) == 0:
        level = "ok"
    elif len(issues) <= 2:
        level = "warn"
    else:
        level = "danger"

    badge = {"ok": "🟢 STABLE", "warn": "🟠 WATCH", "danger": "🔴 ALERT"}[level]
    grad = {
        "ok": "linear-gradient(90deg, rgba(34,197,94,0.18), rgba(59,130,246,0.10))",
        "warn": "linear-gradient(90deg, rgba(245,158,11,0.18), rgba(59,130,246,0.10))",
        "danger": "linear-gradient(90deg, rgba(239,68,68,0.18), rgba(59,130,246,0.10))",
    }[level]

    st.markdown(
        f"""
<div style="
  padding:14px 16px;
  border-radius:18px;
  background:{grad};
  border:1px solid rgba(255,255,255,0.10);
  margin: 8px 0 14px 0;">
  <div style="display:flex;align-items:center;justify-content:space-between;gap:12px;flex-wrap:wrap;">
    <div style="font-size:14px;letter-spacing:0.08em;opacity:0.92;font-weight:800;">{badge}</div>
    <div style="opacity:0.78;">区间：{start_date} ~ {end_date} ｜ 共 {len(dfr)} 天</div>
  </div>
  <div style="margin-top:8px;opacity:0.88;">
    {"✅ 当前区间未发现明显缺失/异常信号" if len(issues) == 0 else "；".join(issues)}
  </div>
</div>
""",
        unsafe_allow_html=True,
    )


if DEBUG:
    st.sidebar.markdown("### 🔎 Debug")
    st.sidebar.caption(f"DB_PATH: `{DB_PATH}`")
    st.sidebar.caption(f"exists: `{os.path.exists(DB_PATH)}`")
    if os.path.exists(DB_PATH):
        st.sidebar.caption(f"size: `{os.path.getsize(DB_PATH)} bytes`")

    @st.cache_data(ttl=300)
    def _debug_read_one_row(db_path: str):
        conn = sqlite3.connect(db_path)
        df = pd.read_sql_query(
            "SELECT * FROM fact_daily_ops WHERE snapshot_id=? ORDER BY date LIMIT 1",
            conn,
            params=(ACTIVE_SNAPSHOT_ID,),
            parse_dates=["date"],
        )
        conn.close()
        row0 = df.to_dict(orient="records")[0] if not df.empty else {}
        return list(df.columns), row0

    if os.path.exists(DB_PATH):
        try:
            cols, row0 = _debug_read_one_row(DB_PATH)
            st.sidebar.caption(f"cols: `{len(cols)}`")
            with st.sidebar.expander("columns"):
                st.write(cols)
            with st.sidebar.expander("row[0]"):
                st.write(row0)
        except Exception as e:
            st.sidebar.error("Read DB failed:")
            st.sidebar.exception(e)
            st.stop()


st.title("总览")

st.markdown(
    """
<style>
h1, h2, h3, h4, h5, h6 {
  color: rgba(255,255,255,0.96) !important;
  text-shadow: 0 0 10px rgba(255,255,255,0.06);
}

div[data-testid="stHeader"] *,
div[data-testid="stMarkdownContainer"] h1,
div[data-testid="stMarkdownContainer"] h2,
div[data-testid="stMarkdownContainer"] h3 {
  color: rgba(255,255,255,0.96) !important;
}

section[data-testid="stSidebar"] * {
  color: rgba(255,255,255,0.92);
}

.block-container:before {
  opacity: 0.12 !important;
}
</style>
""",
    unsafe_allow_html=True,
)

df = load_daily_ops_data(DB_PATH, ACTIVE_SNAPSHOT_ID)
if df.empty:
    st.warning("数据库为空。请先导入 Excel。")
    st.stop()

df["date"] = pd.to_datetime(df["date"], errors="coerce")
df = df[df["date"].notna()].copy()
df = df.sort_values("date")
df = add_daily_electricity(df)

start_date, end_date, date_meta = render_global_sidebar_by_df(df, date_col="date")
mask = (df["date"].dt.date >= start_date) & (df["date"].dt.date <= end_date)
dfr = df.loc[mask].copy()

min_d, max_d = df["date"].min(), df["date"].max()

st.markdown(
    f"""
<div style="
  padding:16px 18px;
  border-radius:18px;
  background: linear-gradient(90deg, rgba(34,197,94,0.18), rgba(59,130,246,0.10));
  border: 1px solid rgba(255,255,255,0.08);
  margin-bottom: 12px;">
  <div style="font-size:20px;font-weight:800;">海吉星果蔬项目 · 运营驾驶舱</div>
  <div style="opacity:0.78;margin-top:4px;">
    数据范围：{min_d.date()} ~ {max_d.date()} ｜ 当前筛选：{date_meta['label']} ｜ 桶重口径：1 桶≈{BUCKET_TO_TON:.2f} 吨 ｜ 版本：{DEFINITIONS_VERSION}
  </div>
</div>
""",
    unsafe_allow_html=True,
)

with st.expander("📌 口径说明", expanded=False):
    st.markdown(DEFINITIONS_MD)

latest_kpi = get_latest_ops_kpis(DB_PATH)
device_info = get_home_device_status(DB_PATH)
cost_kpi = get_prev_month_cost_kpi(DB_PATH)

_render_status_banner(dfr, start_date, end_date)

if latest_kpi:
    days_lag = latest_kpi["days_lag"]
    freshness = classify_data_freshness(days_lag)
    if days_lag is None:
        freshness_text = "未知"
    else:
        freshness_text = f"{freshness}（延迟 {days_lag} 天）"
else:
    days_lag = None
    freshness_text = "未知"

summary = _build_summary(dfr)

# ========= 顶部核心 KPI =========
st.subheader("核心指标")

k1, k2, k3, k4, k5, k6 = st.columns(6)

recent_incoming = latest_kpi["incoming_ton"] if latest_kpi else np.nan
recent_slurry = latest_kpi.get("centrifuge_feed_m3", np.nan) if latest_kpi else np.nan
recent_slag_rate = latest_kpi["slag_ratio"] if latest_kpi else np.nan
recent_date = latest_kpi["date"] if latest_kpi else None

k1.metric("最近处理量", "-" if pd.isna(recent_incoming) else f"{recent_incoming:,.1f} 吨")
k2.metric("最近真实浆料", "-" if pd.isna(recent_slurry) else f"{recent_slurry:,.1f} m³")
k3.metric("最近出渣率", "-" if pd.isna(recent_slag_rate) else f"{recent_slag_rate * 100:.1f}%")
k4.metric("设备状态", device_info["label"])
k5.metric(
    "数据日期",
    recent_date.strftime("%Y-%m-%d") if recent_date is not None and pd.notna(recent_date) else "-",
)
k6.metric(
    "上月吨均成本",
    "-"
    if not cost_kpi or pd.isna(cost_kpi["ton_cost"])
    else f"{cost_kpi['ton_cost']:.1f} 元/吨",
)

st.caption(f"设备状态说明：{device_info['detail']}")
st.caption(f"数据新鲜度：{freshness_text}")

# # ========= 第二行 KPI =========
# q1, q2, q3, q4, q5, q6 = st.columns(6)
# q1.metric("区间处理量", f"{summary['incoming_ton']:,.0f} 吨")
# q2.metric("区间真实浆料", f"{summary['centrifuge_feed_m3']:,.0f} m³")
# q3.metric("区间出渣率", "-" if pd.isna(summary["slag_rate"]) else f"{summary['slag_rate'] * 100:.1f}%")
# q4.metric("平均来料", "-" if pd.isna(summary["avg_incoming_ton_per_day"]) else f"{summary['avg_incoming_ton_per_day']:.1f} 吨/天")
# q5.metric("来料桶数", f"{summary['incoming_bucket_count']:,.0f} 桶" if summary["incoming_bucket_count"] > 0 else "-")
# q6.metric("压缩箱累计", f"{summary['compress_bucket_count']:,.0f} 桶" if summary["compress_bucket_count"] > 0 else "-")

# # ========= 第二行 KPI =========
# q1, q2, q3, q4 = st.columns(4)

# q1.metric(
#     "区间出渣率",
#     "-" if pd.isna(summary["slag_rate"]) else f"{summary['slag_rate'] * 100:.1f}%"
# )

# q2.metric(
#     "平均来料",
#     "-" if pd.isna(summary["avg_incoming_ton_per_day"]) else f"{summary['avg_incoming_ton_per_day']:.1f} 吨/天"
# )

# q3.metric(
#     "来料桶数",
#     f"{summary['incoming_bucket_count']:,.0f} 桶" if summary["incoming_bucket_count"] > 0 else "-"
# )

# q4.metric(
#     "压缩箱累计",
#     f"{summary['compress_bucket_count']:,.0f} 桶" if summary["compress_bucket_count"] > 0 else "-"
# )



# ========= 异常提醒 =========
alerts = []

if (
    "compress_bucket_count" in dfr.columns
    and "line1_feed_bucket_count" in dfr.columns
    and "line2_feed_bucket_count" in dfr.columns
):
    tmp = dfr.copy()
    tmp["total_feed_bucket_count"] = (
        tmp["line1_feed_bucket_count"].fillna(0)
        + tmp["line2_feed_bucket_count"].fillna(0)
    )

    tmp = tmp[tmp["total_feed_bucket_count"] > 0].copy()

    if not tmp.empty:
        tmp["slag_rate_bucket"] = (
            tmp["compress_bucket_count"].fillna(0)
            / tmp["total_feed_bucket_count"]
        )

        latest_row = tmp.sort_values("date").iloc[-1]
        latest_rate = latest_row["slag_rate_bucket"]

        if latest_rate > 0.75:
            alerts.append(
                f"出渣率偏高：{latest_row['date'].date()} 出渣率为 {latest_rate:.1%}，可能存在破碎/筛分异常。"
            )
        elif latest_rate < 0.40:
            alerts.append(
                f"出渣率偏低：{latest_row['date'].date()} 出渣率为 {latest_rate:.1%}，可能存在分离不充分或填报异常。"
            )

if "centrifuge_feed_m3" in dfr.columns and dfr["centrifuge_feed_m3"].fillna(0).sum() == 0:
    alerts.append("当前筛选区间还没有形成连续的离心机真实进料数据。")
if "centrifuge_feed_m3" in dfr.columns and dfr["centrifuge_feed_m3"].fillna(0).sum() == 0:
    alerts.append("当前筛选区间还没有形成连续的离心机真实进料数据。")

if len(alerts) > 0:
    st.subheader("异常提醒")
    for item in alerts:
        st.warning(item)

# ========= 主视图 1：近7天处理量 / 真实浆料 =========
st.subheader("近 7 天核心趋势")

recent = df.copy()
valid_mask = (
    recent["incoming_ton"].fillna(0) > 0
) | (
    recent.get("slag_total_ton", pd.Series(index=recent.index, dtype=float)).fillna(0) > 0
) | (
    recent.get("incoming_bucket_count", pd.Series(index=recent.index, dtype=float)).fillna(0) > 0
) | (
    recent.get("centrifuge_feed_m3", pd.Series(index=recent.index, dtype=float)).fillna(0) > 0
)
recent = recent.loc[valid_mask].copy().tail(7)

if recent.empty:
    st.info("暂无近 7 天趋势数据。")
else:
    fig = go.Figure()
    fig.add_bar(
        x=recent["date"].dt.strftime("%Y-%m-%d"),
        y=recent["incoming_ton"],
        name="处理量（吨）",
    )
    if "centrifuge_feed_m3" in recent.columns:
        fig.add_scatter(
            x=recent["date"].dt.strftime("%Y-%m-%d"),
            y=recent["centrifuge_feed_m3"],
            mode="lines+markers",
            name="真实浆料（m³）",
            yaxis="y2",
        )
    fig.update_layout(
        height=400,
        margin=dict(l=10, r=10, t=40, b=10),
        xaxis=dict(title="日期"),
        yaxis=dict(title="处理量（吨）"),
        yaxis2=dict(
            title="真实浆料（m³）",
            overlaying="y",
            side="right",
        ),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    st.plotly_chart(fig, use_container_width=True)

# ========= 主视图 2：双线处理 / 出渣对比 =========
st.subheader("双线处理 / 出渣对比")

has_line_compare = (
    "line1_feed_bucket_count" in dfr.columns
    and "line2_feed_bucket_count" in dfr.columns
    and (
        dfr["line1_feed_bucket_count"].fillna(0).sum() > 0
        or dfr["line2_feed_bucket_count"].fillna(0).sum() > 0
    )
)

if has_line_compare:
    cols_needed = ["date", "line1_feed_bucket_count", "line2_feed_bucket_count"]

    if "line1_slag_bucket_count" in dfr.columns:
        cols_needed.append("line1_slag_bucket_count")
    if "line2_slag_bucket_count" in dfr.columns:
        cols_needed.append("line2_slag_bucket_count")

    line_df = dfr[cols_needed].copy()
    line_df["日期"] = line_df["date"].dt.strftime("%Y-%m-%d")

    if "line1_slag_bucket_count" not in line_df.columns:
        line_df["line1_slag_bucket_count"] = np.nan
    if "line2_slag_bucket_count" not in line_df.columns:
        line_df["line2_slag_bucket_count"] = np.nan

    # 出渣比例
    line_df["line1_slag_rate"] = (
        pd.to_numeric(line_df["line1_slag_bucket_count"], errors="coerce")
        / pd.to_numeric(line_df["line1_feed_bucket_count"], errors="coerce")
    )
    line_df["line2_slag_rate"] = (
        pd.to_numeric(line_df["line2_slag_bucket_count"], errors="coerce")
        / pd.to_numeric(line_df["line2_feed_bucket_count"], errors="coerce")
    )

    fig = go.Figure()

    # 柱形：处理桶数
    fig.add_bar(
        x=line_df["日期"],
        y=line_df["line1_feed_bucket_count"],
        name="1线打料桶数",
    )
    fig.add_bar(
        x=line_df["日期"],
        y=line_df["line2_feed_bucket_count"],
        name="2线打料桶数",
    )

    # 折线：出渣比例
    fig.add_scatter(
        x=line_df["日期"],
        y=line_df["line1_slag_rate"],
        mode="lines+markers",
        name="1线出渣比例",
        yaxis="y2",
    )
    fig.add_scatter(
        x=line_df["日期"],
        y=line_df["line2_slag_rate"],
        mode="lines+markers",
        name="2线出渣比例",
        yaxis="y2",
    )

    fig.update_layout(
        barmode="group",
        xaxis=dict(title="日期"),
        yaxis=dict(title="处理桶数"),
        yaxis2=dict(
            title="出渣比例",
            overlaying="y",
            side="right",
            tickformat=".0%",
            range=[0, 1],
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
    )

    st.plotly_chart(
        polish_fig(fig, "当前筛选区间双线处理桶数 / 出渣比例"),
        use_container_width=True
    )

        # 汇总指标（两排，每排4个）
    line1_bucket = _sum_col(dfr, "line1_feed_bucket_count")
    line2_bucket = _sum_col(dfr, "line2_feed_bucket_count")
    line1_slag_bucket = _sum_col(dfr, "line1_slag_bucket_count")
    line2_slag_bucket = _sum_col(dfr, "line2_slag_bucket_count")

    line1_runtime_hours = _sum_col(dfr, "line1_runtime_hours")
    line2_runtime_hours = _sum_col(dfr, "line2_runtime_hours")

    line1_feed_ton = line1_bucket * BUCKET_TO_TON
    line2_feed_ton = line2_bucket * BUCKET_TO_TON
    line1_slag_ton = line1_slag_bucket * BUCKET_TO_TON
    line2_slag_ton = line2_slag_bucket * BUCKET_TO_TON

    line1_tph = safe_div(line1_feed_ton, line1_runtime_hours)
    line2_tph = safe_div(line2_feed_ton, line2_runtime_hours)

    r1c1, r1c2, r1c3, r1c4 = st.columns(4)
    r1c1.metric("1线累计", f"{line1_feed_ton:,.1f} 吨" if line1_feed_ton > 0 else "-")
    r1c2.metric("1线出渣", f"{line1_slag_ton:,.1f} 吨" if line1_slag_ton > 0 else "-")
    r1c3.metric("1线开机时长", f"{line1_runtime_hours:,.1f} h" if line1_runtime_hours > 0 else "-")
    r1c4.metric("1线吨/小时", f"{line1_tph:,.2f}" if not pd.isna(line1_tph) else "-")

    r2c1, r2c2, r2c3, r2c4 = st.columns(4)
    r2c1.metric("2线累计", f"{line2_feed_ton:,.1f} 吨" if line2_feed_ton > 0 else "-")
    r2c2.metric("2线出渣", f"{line2_slag_ton:,.1f} 吨" if line2_slag_ton > 0 else "-")
    r2c3.metric("2线开机时长", f"{line2_runtime_hours:,.1f} h" if line2_runtime_hours > 0 else "-")
    r2c4.metric("2线吨/小时", f"{line2_tph:,.2f}" if not pd.isna(line2_tph) else "-")

else:
    st.info("当前区间暂无可展示的双线处理数据。")

# ========= 折叠区：环比 =========
with st.expander("📈 本月 vs 上月（环比）", expanded=False):
    cur_period = pd.Timestamp(start_date).to_period("M")
    prev_period = cur_period - 1

    df_cur = month_range(df, cur_period).copy()
    df_prev = month_range(df, prev_period).copy()
    df_cur = add_daily_electricity(df_cur)
    df_prev = add_daily_electricity(df_prev)

    cur_k = _build_summary(df_cur)
    prev_k = _build_summary(df_prev)

    t1, t2 = st.tabs(["总量环比", "效率环比"])

    with t1:
        c1, c2, c3, c4 = st.columns(4)
        delta = pct_change(cur_k["incoming_ton"], prev_k["incoming_ton"])
        c1.metric("来料(吨)", f"{cur_k['incoming_ton']:,.0f}", f"{delta * 100:.1f}%" if not np.isnan(delta) else "—")

        delta = pct_change(cur_k["centrifuge_feed_m3"], prev_k["centrifuge_feed_m3"])
        c2.metric("真实浆料(m³)", f"{cur_k['centrifuge_feed_m3']:,.0f}", f"{delta * 100:.1f}%" if not np.isnan(delta) else "—")

        delta = pct_change(cur_k["slag_total_ton"], prev_k["slag_total_ton"])
        c3.metric("出渣合计(吨)", f"{cur_k['slag_total_ton']:,.0f}", f"{delta * 100:.1f}%" if not np.isnan(delta) else "—")

        delta = pct_change(cur_k["incoming_bucket_count"], prev_k["incoming_bucket_count"])
        c4.metric("来料桶数", f"{cur_k['incoming_bucket_count']:,.0f}", f"{delta * 100:.1f}%" if not np.isnan(delta) else "—")

    with t2:
        c5, c6, c7, c8 = st.columns(4)

        delta = pct_change(cur_k["slag_rate"], prev_k["slag_rate"])
        c5.metric(
            "出渣率",
            "-" if np.isnan(cur_k["slag_rate"]) else f"{cur_k['slag_rate'] * 100:.1f}%",
            f"{delta * 100:.1f}%" if not np.isnan(delta) else "—",
        )

        delta = pct_change(cur_k["slurry_per_ton"], prev_k["slurry_per_ton"])
        c6.metric(
            "浆料强度(m³/吨)",
            "-" if np.isnan(cur_k["slurry_per_ton"]) else f"{cur_k['slurry_per_ton']:.2f}",
            f"{delta * 100:.1f}%" if not np.isnan(delta) else "—",
        )

        delta = pct_change(cur_k["water_intensity"], prev_k["water_intensity"])
        c7.metric(
            "水耗强度(m³/吨)",
            "-" if np.isnan(cur_k["water_intensity"]) else f"{cur_k['water_intensity']:.2f}",
            f"{delta * 100:.1f}%" if not np.isnan(delta) else "—",
        )

        delta = pct_change(cur_k["elec_intensity"], prev_k["elec_intensity"])
        c8.metric(
            "电耗强度(kWh/吨)",
            "-" if np.isnan(cur_k["elec_intensity"]) else f"{cur_k['elec_intensity']:.1f}",
            f"{delta * 100:.1f}%" if not np.isnan(delta) else "—",
        )

# ========= 折叠区：水电 =========
with st.expander("💧⚡ 水电分析", expanded=False):
    tab1, tab2 = st.tabs(["用水", "用电"])

    with tab1:
        tmp_w = dfr[["date", "water_m3"]].copy() if "water_m3" in dfr.columns else pd.DataFrame()
        if tmp_w.empty or tmp_w["water_m3"].notna().sum() == 0:
            st.info("当前区间暂无用水数据。")
        else:
            tmp_w["water_m3_ma7"] = tmp_w["water_m3"].rolling(7, min_periods=3).mean()
            fig = go.Figure()
            fig.add_trace(go.Bar(x=tmp_w["date"], y=tmp_w["water_m3"], name="用水量(m³)"))
            fig.add_trace(go.Scatter(x=tmp_w["date"], y=tmp_w["water_m3_ma7"], mode="lines", name="7日均线"))
            fig.update_layout(
                title="用水量（日）",
                xaxis_title="日期",
                yaxis_title="m³",
                legend_title_text="",
                hovermode="x unified",
            )
            st.plotly_chart(polish_fig(fig), use_container_width=True)

    with tab2:
        elec_valid = dfr["daily_elec_kwh"].notna().sum() if "daily_elec_kwh" in dfr.columns else 0
        if elec_valid == 0:
            st.info("当前区间电表抄表点不足，无法生成每日电耗。")
        else:
            tmp_e = dfr[["date", "daily_elec_kwh"]].copy()
            tmp_e["daily_elec_kwh_ma7"] = tmp_e["daily_elec_kwh"].rolling(7, min_periods=3).mean()

            fig = go.Figure()
            fig.add_trace(go.Bar(x=tmp_e["date"], y=tmp_e["daily_elec_kwh"], name="每日电耗(kWh)"))
            fig.add_trace(go.Scatter(x=tmp_e["date"], y=tmp_e["daily_elec_kwh_ma7"], mode="lines", name="7日均线"))
            fig.update_layout(
                title="每日电耗（均摊口径）",
                xaxis_title="日期",
                yaxis_title="kWh",
                legend_title_text="",
                hovermode="x unified",
            )
            st.plotly_chart(polish_fig(fig), use_container_width=True)

# ========= 折叠区：明细 =========
with st.expander("🔎 明细与下载", expanded=False):
    if "pick_date_filter" not in st.session_state:
        st.session_state.pick_date_filter = "（不筛选）"

    b1, b2, b3 = st.columns(3)

    if b1.button("跳到最大出渣率日", use_container_width=True):
        tmp = dfr.copy()
        if "incoming_ton" in tmp.columns and "slag_total_ton" in tmp.columns:
            tmp = tmp[(tmp["incoming_ton"].notna()) & (tmp["incoming_ton"] > 0)]
            tmp["slag_rate"] = tmp["slag_total_ton"] / tmp["incoming_ton"]
            tmp = tmp.dropna(subset=["slag_rate"])
            if not tmp.empty:
                best = tmp.sort_values("slag_rate", ascending=False).iloc[0]["date"].date()
                st.session_state.pick_date_filter = str(best)
                st.rerun()

    if b2.button("跳到最大用水量日", use_container_width=True):
        if "water_m3" in dfr.columns:
            tmp = dfr.dropna(subset=["water_m3"])
            if not tmp.empty:
                best = tmp.sort_values("water_m3", ascending=False).iloc[0]["date"].date()
                st.session_state.pick_date_filter = str(best)
                st.rerun()

    if b3.button("跳到最大用电量日", use_container_width=True):
        if "daily_elec_kwh" in dfr.columns:
            tmp = dfr.dropna(subset=["daily_elec_kwh"])
            if not tmp.empty:
                best = tmp.sort_values("daily_elec_kwh", ascending=False).iloc[0]["date"].date()
                st.session_state.pick_date_filter = str(best)
                st.rerun()

    date_options = sorted(dfr["date"].dt.date.dropna().unique().tolist())
    pick = st.selectbox(
        "选择日期（用于过滤下方明细表）",
        options=["（不筛选）"] + [str(d) for d in date_options],
        index=0,
        key="pick_date_filter",
    )

    if pick != "（不筛选）":
        pick_date = pd.to_datetime(pick).date()
        dfr_show = dfr[dfr["date"].dt.date == pick_date].copy()
    else:
        dfr_show = dfr.copy()

    st.dataframe(dfr_show, use_container_width=True, hide_index=True)

    csv = dfr_show.to_csv(index=False).encode("utf-8-sig")
    suffix = "all" if pick == "（不筛选）" else pick
    st.download_button(
        "下载当前区间 CSV",
        data=csv,
        file_name=f"ops_{start_date}_{end_date}_{suffix}_{DEFINITIONS_VERSION}.csv",
        mime="text/csv",
    )