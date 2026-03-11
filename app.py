# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sqlite3
import tempfile
from typing import Dict, Any

import pandas as pd
import streamlit as st

from typing import List

from etl.parse_excel import parse_workbook
from utils.paths import get_db_path
from utils.bootstrap import bootstrap_page
from utils.device_store import save_device_excel_for_staging
from utils.snapshot import (
    get_active_snapshot_id,
    get_staging_snapshot_id,
    publish_staging,
    rollback_to_previous_active,
    get_snapshot_stats,
    list_recent_snapshots,
    write_daily_ops_snapshot,
    get_last_publish_info,
    list_recent_publish_log,
)

DB_PATH = get_db_path()

st.set_page_config(page_title="海吉星运营驾驶舱", layout="wide")

user = bootstrap_page(DB_PATH)
is_admin = getattr(user, "role", "") == "admin"


# -------------------------
# Data access (read-only)
# -------------------------
@st.cache_data(ttl=60)
def load_data(db_path: str, snapshot_id: str) -> pd.DataFrame:
    if not os.path.exists(db_path):
        return pd.DataFrame()
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query(
        "SELECT * FROM fact_daily_ops WHERE snapshot_id=? ORDER BY date",
        conn,
        params=(snapshot_id,),
        parse_dates=["date"],
    )
    conn.close()
    return df


def build_import_report(df_new: pd.DataFrame) -> Dict[str, Any]:
    report: Dict[str, Any] = {}
    df = df_new.copy().sort_values("date")
    report["rows"] = int(len(df))
    report["date_min"] = df["date"].min().date() if len(df) else None
    report["date_max"] = df["date"].max().date() if len(df) else None

    dup = df["date"].dt.date[df["date"].dt.date.duplicated(keep=False)]
    report["dup_dates"] = sorted(list(set(dup.tolist()))) if len(dup) else []

    miss = (df.isna().mean() * 100).sort_values(ascending=False)
    report["missing_top"] = miss.head(10).round(1).to_dict()

    return report

# -------------------------
# Quality gate (v1.4-stable)
# -------------------------
def quality_gate_for_df(df_new: pd.DataFrame) -> Dict[str, Any]:
    """
    返回：是否允许写入 staging、是否允许 publish、原因说明
    """
    if df_new is None or df_new.empty:
        return {"ok_write": False, "ok_publish": False, "reasons": ["未解析到任何数据"]}

    if "date" not in df_new.columns:
        return {"ok_write": False, "ok_publish": False, "reasons": ["缺少 date 列"]}

    df = df_new.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df[df["date"].notna()].copy()
    if df.empty:
        return {"ok_write": False, "ok_publish": False, "reasons": ["date 列无法解析为日期"]}

    # 1) 重复日期（按天）
    d = df["date"].dt.date
    dup_dates = sorted(d[d.duplicated(keep=False)].unique().tolist())
    if dup_dates:
        return {
            "ok_write": False,
            "ok_publish": False,
            "reasons": [f"存在重复日期（按天）：{dup_dates[:10]}{'...' if len(dup_dates)>10 else ''}"],
        }

    # 2) 缺失率阈值（关键字段）
    # 你可以按业务重要性调整这些字段
    key_cols = ["incoming_ton", "slag_ton", "water_m3", "elec_meter_kwh"]
    present = [c for c in key_cols if c in df.columns]
    reasons = []

    # 缺失率阈值：>20% 视为不允许发布（可按需调）
    miss_threshold = 20.0
    for c in present:
        miss_pct = float(df[c].isna().mean() * 100)
        if miss_pct > miss_threshold:
            reasons.append(f"{c} 缺失率 {miss_pct:.1f}% > {miss_threshold:.0f}%")

    # ok_publish = True
    # # 写入 staging 可以放宽（允许写入，但禁止发布），这里我建议仍允许写入，方便你检查
    # ok_write = True
    
    # 写入 staging 可以放宽，方便管理员先预览；但只要存在关键质量问题，就禁止发布。
    ok_write = True
    ok_publish = len(reasons) == 0

    return {"ok_write": ok_write, "ok_publish": ok_publish, "reasons": reasons}


@st.cache_data(ttl=30)
def quality_gate_for_snapshot(db_path: str, snapshot_id: str) -> Dict[str, Any]:
    """
    对 staging 快照做发布前检查（避免 admin 重启后无法复核 df_new）。
    """
    if not snapshot_id:
        return {"ok_publish": False, "reasons": ["staging_snapshot_id 为空"]}

    if not os.path.exists(db_path):
        return {"ok_publish": False, "reasons": ["数据库不存在"]}

    conn = sqlite3.connect(db_path)
    try:
        df = pd.read_sql_query(
            "SELECT date,incoming_ton,slag_ton,water_m3,elec_meter_kwh FROM fact_daily_ops WHERE snapshot_id=?",
            conn,
            params=(snapshot_id,),
            parse_dates=["date"],
        )
    finally:
        conn.close()

    if df.empty:
        return {"ok_publish": False, "reasons": ["staging 快照没有数据"]}

    return quality_gate_for_df(df)


# -------------------------
# Header: snapshot status + last publish info
# -------------------------
active_sid = get_active_snapshot_id(DB_PATH)
staging_sid = get_staging_snapshot_id(DB_PATH) if is_admin else None

last_pub = get_last_publish_info(DB_PATH)

def _fmt_pub(pub: Dict[str, Any] | None) -> str:
    if not pub:
        return "暂无发布记录"
    # happened_at is UTC ISO Z
    return f"{pub.get('happened_at','-')}（UTC） by {pub.get('operator','-')}"

if is_admin:
    colA, colB, colC = st.columns([1.2, 1.2, 2.6])
    with colA:
        st.metric("📌 当前发布快照", active_sid[:8])
        a_stat = get_snapshot_stats(DB_PATH, active_sid)
        st.caption(f"行数：{a_stat['rows']}  日期：{a_stat['date_min']} ~ {a_stat['date_max']}")
    with colB:
        st.metric("🧪 当前草稿快照", staging_sid[:8] if staging_sid else "-")
        s_stat = get_snapshot_stats(DB_PATH, staging_sid) if staging_sid else {"rows": 0, "date_min": None, "date_max": None}
        st.caption(f"行数：{s_stat['rows']}  日期：{s_stat['date_min']} ~ {s_stat['date_max']}")
    with colC:
        st.info(f"🕒 最近更新：{_fmt_pub(last_pub)}\n\n✅ 厂长账号只看到“当前发布快照”。你可以先导入到草稿，检查无误后再发布。")
else:
    colA, colB = st.columns([1.3, 3.7])
    with colA:
        st.metric("📌 当前发布快照", active_sid[:8])
        a_stat = get_snapshot_stats(DB_PATH, active_sid)
        st.caption(f"行数：{a_stat['rows']}  日期：{a_stat['date_min']} ~ {a_stat['date_max']}")
    with colB:
        st.success(f"✅ 查看模式：仅展示已发布数据\n\n🕒 最近更新：{_fmt_pub(last_pub)}")


# -------------------------
# Admin console: import & publish + rollback
# -------------------------
if is_admin:
    with st.expander("🛠 管理后台：每日数据导入与发布", expanded=True):
        c_up1, c_up2 = st.columns([1.5, 1.2])
        with c_up1:
            up = st.file_uploader("上传运营Excel（每日一次）", type=["xlsx", "xls"], key="ops_uploader")
        with c_up2:
            dev_up = st.file_uploader("（可选）上传设备记录表", type=["xlsx", "xls"], key="dev_uploader")
            st.caption("建议同一天一起上传，发布后厂长可见")

        if up is not None:
            with st.spinner("解析Excel..."):
                tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
                tmp.write(up.getbuffer())
                tmp.close()

                df_new = parse_workbook(tmp.name)
                os.unlink(tmp.name)

            if df_new.empty:
                st.error("未解析到数据（请检查 sheet/表头是否符合预期）")
            else:
                st.success(f"解析成功：{len(df_new)} 行")
                rep = build_import_report(df_new)
                gate_df = quality_gate_for_df(df_new)
                if (not gate_df["ok_write"]) or (not gate_df["ok_publish"]):
                    # 这里给你一个清晰提示：哪些问题会影响发布
                    if gate_df["reasons"]:
                        st.warning("⚠️ 质量闸门提示：\n- " + "\n- ".join(gate_df["reasons"]))

                c1, c2 = st.columns([1.2, 1.8])
                with c1:
                    st.markdown("#### 导入质量检查")
                    st.write(rep)
                with c2:
                    st.markdown("#### 预览（前 20 行）")
                    st.dataframe(df_new.head(20), use_container_width=True)

                if st.button(
                    "写入草稿快照（staging）",
                    type="primary",
                    disabled=not gate_df["ok_write"],
                ):
                    sid = write_daily_ops_snapshot(DB_PATH, df_new, created_by=user.username, filename=up.name)
                    if dev_up is not None:
                        try:
                            save_device_excel_for_staging(DB_PATH, dev_up.getbuffer())
                            st.info("设备记录表已绑定到当前草稿快照")
                        except Exception as e:
                            st.warning(f"设备记录表保存失败：{e}")
                    st.success(f"已写入草稿快照：{sid}")
                    st.cache_data.clear()
                    st.rerun()

        st.divider()

        cpub, crollback = st.columns([1.2, 1.0])

        with cpub:
            st.divider()
            if staging_sid and active_sid != staging_sid:
                gate_snap = quality_gate_for_snapshot(DB_PATH, staging_sid)

                if gate_snap["reasons"]:
                    st.warning("⚠️ 发布前质量检查：\n- " + "\n- ".join(gate_snap["reasons"]))

                if st.button(
                    "🚀 发布草稿到厂长可见（publish）",
                    type="primary",
                    disabled=not gate_snap["ok_publish"],
                ):
                    new_active = publish_staging(DB_PATH, published_by=user.username)
                    st.success(f"发布成功：active_snapshot_id = {new_active}")
                    st.cache_data.clear()
                    st.rerun()
            else:
                st.success("当前草稿与发布一致：无需发布。")

        with crollback:
            if st.button("⏪ 回滚到上一版发布", type="secondary"):
                try:
                    new_active = rollback_to_previous_active(DB_PATH, operator=user.username)
                    st.success(f"回滚成功：active_snapshot_id = {new_active}")
                    st.cache_data.clear()
                    st.rerun()
                except Exception as e:
                    st.error(f"回滚失败：{e}")

        st.divider()
        st.markdown("#### 最近快照记录（snapshots）")
        st.dataframe(list_recent_snapshots(DB_PATH, limit=20), use_container_width=True)

        st.markdown("#### 发布/回滚日志（publish_log）")
        st.dataframe(list_recent_publish_log(DB_PATH, limit=30), use_container_width=True)
else:
    st.caption("你当前为查看者账号：仅可查看已发布数据。")


# -------------------------
# Main: show basic stats quickly
# -------------------------
st.divider()
st.markdown("## 📊 当前发布数据概览（active snapshot）")

df = load_data(DB_PATH, active_sid)
if df.empty:
    st.warning("当前数据库没有数据。请管理员先导入并发布。")
else:
    st.dataframe(df.tail(30), use_container_width=True)