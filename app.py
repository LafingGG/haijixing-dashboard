# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sqlite3
import tempfile
from typing import Any, Dict, Optional

import pandas as pd
import streamlit as st

from etl.parse_excel import parse_workbook
from etl.parse_purchase_excel import parse_purchase_workbook
from utils.bootstrap import bootstrap_page
from utils.cost_store import (
    ensure_cost_schema,
    get_latest_cost_import_info,
    replace_purchase_cost_batch,
)
from utils.device_store import save_device_excel_for_staging
from utils.paths import get_db_path
from utils.snapshot import (
    get_active_snapshot_id,
    get_last_publish_info,
    get_snapshot_stats,
    get_staging_snapshot_id,
    list_recent_publish_log,
    list_recent_snapshots,
    publish_staging,
    rollback_to_previous_active,
    write_daily_ops_snapshot,
)

DB_PATH = get_db_path()

st.set_page_config(page_title="海吉星运营驾驶舱", layout="wide")


# ============================================================
# Bootstrap
# ============================================================
user = bootstrap_page(DB_PATH)
is_admin = getattr(user, "role", "") == "admin"

# 成本表 schema 单独补一下，避免首页读取成本导入信息时报错
ensure_cost_schema(DB_PATH)


# ============================================================
# Data access
# ============================================================
@st.cache_data(ttl=60)
def load_ops_data(db_path: str, snapshot_id: str) -> pd.DataFrame:
    if not os.path.exists(db_path):
        return pd.DataFrame()

    conn = sqlite3.connect(db_path)
    try:
        df = pd.read_sql_query(
            """
            SELECT *
            FROM fact_daily_ops
            WHERE snapshot_id=?
            ORDER BY date
            """,
            conn,
            params=(snapshot_id,),
            parse_dates=["date"],
        )
        return df
    finally:
        conn.close()


@st.cache_data(ttl=30)
def load_latest_cost_import_info(db_path: str) -> Optional[Dict[str, Any]]:
    return get_latest_cost_import_info(db_path)


def build_import_report(df_new: pd.DataFrame) -> Dict[str, Any]:
    report: Dict[str, Any] = {}
    df = df_new.copy()

    if "date" in df.columns:
        df = df.sort_values("date")
        report["rows"] = int(len(df))
        report["date_min"] = df["date"].min().date() if len(df) else None
        report["date_max"] = df["date"].max().date() if len(df) else None

        dup = df["date"].dt.date[df["date"].dt.date.duplicated(keep=False)]
        report["dup_dates"] = sorted(list(set(dup.tolist()))) if len(dup) else []
    else:
        report["rows"] = int(len(df))

    miss = (df.isna().mean() * 100).sort_values(ascending=False)
    report["missing_top"] = miss.head(10).round(1).to_dict()

    return report


def build_cost_import_report(df_new: pd.DataFrame) -> Dict[str, Any]:
    report: Dict[str, Any] = {}
    df = df_new.copy()

    report["rows"] = int(len(df))

    if "expense_date" in df.columns:
        dt = pd.to_datetime(df["expense_date"], errors="coerce")
        report["expense_date_min"] = dt.min().date() if dt.notna().any() else None
        report["expense_date_max"] = dt.max().date() if dt.notna().any() else None

    if "analysis_month" in df.columns:
        months = sorted(
            {
                str(x).strip()
                for x in df["analysis_month"].dropna().astype(str).tolist()
                if str(x).strip()
            }
        )
        report["analysis_month_min"] = months[0] if months else None
        report["analysis_month_max"] = months[-1] if months else None

    if "amount" in df.columns:
        report["amount_sum"] = round(float(pd.to_numeric(df["amount"], errors="coerce").fillna(0).sum()), 2)

    for c in ["category_name", "level1_name", "payee"]:
        if c in df.columns:
            report[f"{c}_top"] = (
                df[c].fillna("（空）").astype(str).value_counts().head(8).to_dict()
            )

    miss = (df.isna().mean() * 100).sort_values(ascending=False)
    report["missing_top"] = miss.head(10).round(1).to_dict()

    return report


# ============================================================
# Helper
# ============================================================
def parse_uploaded_excel(uploaded_file, parser_func, suffix: str = ".xlsx") -> pd.DataFrame:
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
    try:
        tmp.write(uploaded_file.getbuffer())
        tmp.close()
        return parser_func(tmp.name)
    finally:
        try:
            os.unlink(tmp.name)
        except Exception:
            pass


def fmt_publish_info(pub: Optional[Dict[str, Any]]) -> str:
    if not pub:
        return "暂无发布记录"
    return f"{pub.get('happened_at', '-')}（UTC） by {pub.get('operator', '-')}"


def fmt_cost_import_info(info: Optional[Dict[str, Any]]) -> str:
    if not info:
        return "暂无成本导入记录"
    return (
        f"{info.get('imported_at', '-')}（UTC） by {info.get('imported_by', '-')}"
        f"｜文件：{info.get('source_file', '-')}"
        f"｜行数：{info.get('rows_written', '-')}"
    )


# ============================================================
# Quality gate (ops only)
# ============================================================
def quality_gate_for_df(df_new: pd.DataFrame) -> Dict[str, Any]:
    """
    返回：
    - ok_write: 是否允许写入 staging
    - ok_publish: 是否允许 publish
    - reasons: 原因说明
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

    d = df["date"].dt.date
    dup_dates = sorted(d[d.duplicated(keep=False)].unique().tolist())
    if dup_dates:
        return {
            "ok_write": False,
            "ok_publish": False,
            "reasons": [f"存在重复日期（按天）：{dup_dates[:10]}{'...' if len(dup_dates) > 10 else ''}"],
        }

    # 这里只检查核心业务字段，不再检查 water_m3 / elec_meter_kwh
    key_cols = ["incoming_ton", "slag_ton"]
    present = [c for c in key_cols if c in df.columns]
    reasons = []

    miss_threshold = 20.0
    for c in present:
        miss_pct = float(df[c].isna().mean() * 100)
        if miss_pct > miss_threshold:
            reasons.append(f"{c} 缺失率 {miss_pct:.1f}% > {miss_threshold:.0f}%")

    ok_write = True
    ok_publish = len(reasons) == 0

    return {"ok_write": ok_write, "ok_publish": ok_publish, "reasons": reasons}

@st.cache_data(ttl=30)
def quality_gate_for_snapshot(db_path: str, snapshot_id: str) -> Dict[str, Any]:
    if not snapshot_id:
        return {"ok_publish": False, "reasons": ["staging_snapshot_id 为空"]}

    if not os.path.exists(db_path):
        return {"ok_publish": False, "reasons": ["数据库不存在"]}

    conn = sqlite3.connect(db_path)
    try:
        df = pd.read_sql_query(
            """
            SELECT date, incoming_ton, slag_ton
            FROM fact_daily_ops
            WHERE snapshot_id=?
            """,
            conn,
            params=(snapshot_id,),
            parse_dates=["date"],
        )
    finally:
        conn.close()

    if df.empty:
        return {"ok_publish": False, "reasons": ["staging 快照没有数据"]}

    return quality_gate_for_df(df)

# ============================================================
# Current status
# ============================================================
active_sid = get_active_snapshot_id(DB_PATH)
staging_sid = get_staging_snapshot_id(DB_PATH) if is_admin else None

last_pub = get_last_publish_info(DB_PATH)
latest_cost_import = load_latest_cost_import_info(DB_PATH)

active_stat = get_snapshot_stats(DB_PATH, active_sid)
staging_stat = (
    get_snapshot_stats(DB_PATH, staging_sid)
    if is_admin and staging_sid
    else {"rows": 0, "date_min": None, "date_max": None}
)


# ============================================================
# Header
# ============================================================
st.title("海吉星运营驾驶舱")

if is_admin:
    c1, c2, c3 = st.columns([1.15, 1.15, 1.7])
    with c1:
        st.metric("📌 当前发布快照", active_sid[:8])
        st.caption(f"行数：{active_stat['rows']}  日期：{active_stat['date_min']} ~ {active_stat['date_max']}")
    with c2:
        st.metric("🧪 当前草稿快照", staging_sid[:8] if staging_sid else "-")
        st.caption(f"行数：{staging_stat['rows']}  日期：{staging_stat['date_min']} ~ {staging_stat['date_max']}")
    with c3:
        st.info(
            f"🕒 运营最近发布：{fmt_publish_info(last_pub)}\n\n"
            f"💰 成本最近导入：{fmt_cost_import_info(latest_cost_import)}"
        )
else:
    c1, c2 = st.columns([1.2, 2.8])
    with c1:
        st.metric("📌 当前发布快照", active_sid[:8])
        st.caption(f"行数：{active_stat['rows']}  日期：{active_stat['date_min']} ~ {active_stat['date_max']}")
    with c2:
        st.success(
            f"✅ 查看模式：仅展示已发布运营数据\n\n"
            f"🕒 运营最近发布：{fmt_publish_info(last_pub)}\n\n"
            f"💰 成本最近导入：{fmt_cost_import_info(latest_cost_import)}"
        )


# ============================================================
# Admin console
# ============================================================
if is_admin:
    # --------------------------------------------------------
    # A. 运营数据导入与发布
    # --------------------------------------------------------
    with st.expander("🛠 管理后台：每日运营数据导入与发布", expanded=True):
        c_up1, c_up2 = st.columns([1.5, 1.2])

        with c_up1:
            up = st.file_uploader("上传运营 Excel（每日一次）", type=["xlsx", "xls"], key="ops_uploader")
        with c_up2:
            dev_up = st.file_uploader("（可选）上传设备记录表", type=["xlsx", "xls"], key="dev_uploader")
            st.caption("建议同一天一起上传；发布后厂长可见。")

        if up is not None:
            with st.spinner("解析运营 Excel..."):
                try:
                    df_new = parse_uploaded_excel(up, parse_workbook, suffix=".xlsx")
                except Exception as e:
                    df_new = pd.DataFrame()
                    st.error(f"运营 Excel 解析失败：{e}")

            if df_new is not None and not df_new.empty:
                st.success(f"运营数据解析成功：{len(df_new)} 行")

                rep = build_import_report(df_new)
                gate_df = quality_gate_for_df(df_new)

                if gate_df["reasons"]:
                    if gate_df["ok_publish"]:
                        st.info("质量检查提示：\n- " + "\n- ".join(gate_df["reasons"]))
                    else:
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
                    key="write_ops_staging_btn",
                ):
                    try:
                        sid = write_daily_ops_snapshot(
                            DB_PATH,
                            df_new,
                            created_by=user.username,
                            filename=up.name,
                        )

                        if dev_up is not None:
                            try:
                                save_device_excel_for_staging(DB_PATH, dev_up.getbuffer())
                                st.info("设备记录表已绑定到当前草稿快照")
                            except Exception as e:
                                st.warning(f"设备记录表保存失败：{e}")

                        st.success(f"已写入草稿快照：{sid}")
                        st.cache_data.clear()
                        st.rerun()
                    except Exception as e:
                        st.error(f"写入草稿失败：{e}")
            elif up is not None:
                st.error("未解析到运营数据，请检查 sheet / 表头格式。")

        st.divider()

        cpub, crollback = st.columns([1.2, 1.0])

        with cpub:
            if staging_sid and active_sid != staging_sid:
                gate_snap = quality_gate_for_snapshot(DB_PATH, staging_sid)

                if gate_snap["reasons"]:
                    st.warning("⚠️ 发布前质量检查：\n- " + "\n- ".join(gate_snap["reasons"]))

                if st.button(
                    "🚀 发布草稿到厂长可见（publish）",
                    type="primary",
                    disabled=not gate_snap["ok_publish"],
                    key="publish_ops_btn",
                ):
                    try:
                        new_active = publish_staging(DB_PATH, published_by=user.username)
                        st.success(f"发布成功：active_snapshot_id = {new_active}")
                        st.cache_data.clear()
                        st.rerun()
                    except Exception as e:
                        st.error(f"发布失败：{e}")
            else:
                st.success("当前草稿与发布一致：无需发布。")

        with crollback:
            if st.button("⏪ 回滚到上一版发布", type="secondary", key="rollback_ops_btn"):
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

        st.markdown("#### 发布 / 回滚日志（publish_log）")
        st.dataframe(list_recent_publish_log(DB_PATH, limit=30), use_container_width=True)

    # --------------------------------------------------------
    # B. 成本数据导入（即时生效）
    # --------------------------------------------------------
    with st.expander("💰 管理后台：成本 Excel 导入（即时生效）", expanded=True):
        st.caption(
            "这里沿用当前成本模块逻辑：导入后会直接替换当前成本数据，并立即在成本分析页面生效。"
        )

        cost_up = st.file_uploader(
            "上传采购 / 运营费用 Excel",
            type=["xlsx", "xls"],
            key="cost_uploader",
        )

        latest_cost_import = load_latest_cost_import_info(DB_PATH)
        if latest_cost_import:
            st.info(
                "当前成本版本："
                f"{latest_cost_import.get('imported_at', '-')}"
                f"｜导入人：{latest_cost_import.get('imported_by', '-')}"
                f"｜文件：{latest_cost_import.get('source_file', '-')}"
                f"｜行数：{latest_cost_import.get('rows_written', '-')}"
            )
        else:
            st.info("当前还没有成本导入记录。")

        if cost_up is not None:
            with st.spinner("解析成本 Excel..."):
                try:
                    cost_df = parse_uploaded_excel(cost_up, parse_purchase_workbook, suffix=".xlsx")
                except Exception as e:
                    cost_df = pd.DataFrame()
                    st.error(f"成本 Excel 解析失败：{e}")

            if cost_df is not None and not cost_df.empty:
                st.success(f"成本数据解析成功：{len(cost_df)} 行")

                rep = build_cost_import_report(cost_df)
                c1, c2 = st.columns([1.2, 1.8])

                with c1:
                    st.markdown("#### 成本导入检查")
                    st.write(rep)

                with c2:
                    st.markdown("#### 预览（前 30 行）")
                    st.dataframe(cost_df.head(30), use_container_width=True)

                if st.button(
                    "📥 导入并替换当前成本数据",
                    type="primary",
                    key="replace_cost_batch_btn",
                ):
                    try:
                        result = replace_purchase_cost_batch(
                            DB_PATH,
                            cost_df,
                            imported_by=user.username,
                            source_file=cost_up.name,
                        )
                        st.success(
                            "成本导入成功："
                            f"batch_id={result.get('batch_id', '-')}, "
                            f"采购明细 {result.get('purchase_rows', 0)} 行, "
                            f"运营成本 {result.get('operational_rows', 0)} 行"
                        )
                        st.cache_data.clear()
                        st.rerun()
                    except Exception as e:
                        st.error(f"成本导入失败：{e}")
            elif cost_up is not None:
                st.error("未解析到成本数据，请检查 sheet / 表头格式。")
else:
    st.caption("你当前为查看者账号：仅可查看已发布运营数据及版本信息。")


# ============================================================
# Main preview
# ============================================================
st.divider()
st.markdown("## 📊 当前发布运营数据概览（active snapshot）")

df = load_ops_data(DB_PATH, active_sid)
if df.empty:
    st.warning("当前数据库没有运营数据，请管理员先导入并发布。")
else:
    c1, c2, c3 = st.columns(3)

    date_min = df["date"].min().date() if "date" in df.columns and not df.empty else None
    date_max = df["date"].max().date() if "date" in df.columns and not df.empty else None

    incoming_total = (
        float(pd.to_numeric(df["incoming_ton"], errors="coerce").fillna(0).sum())
        if "incoming_ton" in df.columns else 0.0
    )
    slag_total = (
        float(pd.to_numeric(df["slag_ton"], errors="coerce").fillna(0).sum())
        if "slag_ton" in df.columns else 0.0
    )

    with c1:
        st.metric("记录天数", len(df))
        st.caption(f"{date_min} ~ {date_max}")
    with c2:
        st.metric("累计进料量（吨）", f"{incoming_total:,.1f}")
    with c3:
        st.metric("累计出渣量（吨）", f"{slag_total:,.1f}")

    st.dataframe(df.tail(30), use_container_width=True)