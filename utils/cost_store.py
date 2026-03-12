# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import os
import sqlite3
import uuid
from datetime import datetime
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd

from utils.snapshot import ensure_snapshot_schema, get_active_snapshot_id


def _utcnow_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


CATEGORY_SEED = [
    ("materials", "材料采购", "材料费用", "材料采购", 10, 0, 1, "药剂、辅料、包材、低值易耗品等"),
    ("repair", "维修费用", "维修费用", "维修费用", 20, 0, 1, "设备维修、配件更换、抢修服务"),
    ("energy_cost", "能源费用", "能源费用", "能源费用", 25, 0, 1, "电费、水费、燃气等能源费用"),
    ("slag", "固渣处理费", "运营费用", "固渣处理费", 30, 0, 1, "固渣外运、处置、堆肥原料等"),
    ("carbon_source", "碳源处理费", "运营费用", "碳源处理费", 40, 0, 1, "水厂、碳源、发酵液、浆料处理相关"),
    ("transport", "运输费", "运输费用", "运输费", 50, 0, 1, "短倒、外运、物流、车辆运输"),
    ("admin", "行政费用", "行政/后勤费用", "行政费用", 60, 0, 1, "办公、通信、杂费、行政采购"),
    ("dormitory", "宿舍后勤", "行政/后勤费用", "宿舍后勤", 70, 0, 1, "住宿、保洁、食堂、生活保障"),
    ("hospitality", "招待交通", "招待交通", "招待交通", 80, 0, 1, "接待、差旅、市内交通"),
    ("labor", "人工费用", "人工费用", "人工费用", 90, 1, 1, "预留接口，后续正式接入"),
    ("other", "其他", "其他费用", "其他", 999, 0, 1, "暂时无法归类的支出"),
]

COST_IMPORT_LOG_SQL = """
CREATE TABLE IF NOT EXISTS cost_import_log (
    import_id TEXT PRIMARY KEY,
    batch_id TEXT NOT NULL,
    imported_at TEXT NOT NULL,
    imported_by TEXT,
    source_file TEXT,
    source_sheet TEXT,
    rows_written INTEGER,
    status TEXT NOT NULL,
    error_message TEXT
);
"""

DIM_COST_CATEGORY_SQL = """
CREATE TABLE IF NOT EXISTS dim_cost_category (
    category_code TEXT PRIMARY KEY,
    category_name TEXT NOT NULL UNIQUE,
    level1_name TEXT NOT NULL,
    display_name TEXT NOT NULL,
    sort_order INTEGER NOT NULL DEFAULT 999,
    is_reserved INTEGER NOT NULL DEFAULT 0,
    is_active INTEGER NOT NULL DEFAULT 1,
    note TEXT
);
"""

FACT_PURCHASE_EXPENSE_SQL = """
CREATE TABLE IF NOT EXISTS fact_purchase_expense (
    expense_id TEXT PRIMARY KEY,
    batch_id TEXT NOT NULL,
    imported_at TEXT NOT NULL,
    imported_by TEXT,
    source_file TEXT,
    source_sheet TEXT,
    source_row_no INTEGER,
    expense_date TEXT NOT NULL,
    expense_month TEXT NOT NULL,
    item_name TEXT,
    payee TEXT,
    amount REAL NOT NULL,
    category_name TEXT,
    category_code TEXT,
    level1_name TEXT,
    remark TEXT,
    service_date TEXT,
    service_month TEXT,
    analysis_month TEXT,
    date_source TEXT,
    service_dates_json TEXT,
    service_months_json TEXT,
    raw_json TEXT,
    row_hash TEXT UNIQUE
);
"""

FACT_OPERATIONAL_COST_SQL = """
CREATE TABLE IF NOT EXISTS fact_operational_cost (
    cost_id TEXT PRIMARY KEY,
    source_type TEXT NOT NULL,
    source_row_hash TEXT,
    batch_id TEXT NOT NULL,
    expense_date TEXT,
    payment_month TEXT,
    service_date TEXT,
    analysis_month TEXT NOT NULL,
    amount REAL NOT NULL,
    allocated_ratio REAL,
    category_code TEXT,
    category_name TEXT,
    level1_name TEXT,
    payee TEXT,
    item_name TEXT,
    remark TEXT,
    source_file TEXT,
    date_source TEXT
);
"""


def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {r[1] for r in rows}


def _ensure_columns(conn: sqlite3.Connection, table_name: str, columns: Dict[str, str]) -> None:
    existing = _table_columns(conn, table_name)
    for col, col_type in columns.items():
        if col not in existing:
            conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {col} {col_type}")


def ensure_cost_schema(db_path: str) -> None:
    if not db_path or not str(db_path).strip():
        raise ValueError("数据库路径为空，请检查 get_db_path() 的返回值。")

    db_dir = os.path.dirname(db_path)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(COST_IMPORT_LOG_SQL)
        conn.execute(DIM_COST_CATEGORY_SQL)
        conn.execute(FACT_PURCHASE_EXPENSE_SQL)
        conn.execute(FACT_OPERATIONAL_COST_SQL)

        _ensure_columns(conn, "fact_purchase_expense", {
            "service_date": "TEXT",
            "service_month": "TEXT",
            "analysis_month": "TEXT",
            "date_source": "TEXT",
            "service_dates_json": "TEXT",
            "service_months_json": "TEXT",
        })
        _ensure_columns(conn, "fact_operational_cost", {
            "expense_date": "TEXT",
            "payment_month": "TEXT",
            "service_date": "TEXT",
            "analysis_month": "TEXT",
            "allocated_ratio": "REAL",
            "date_source": "TEXT",
        })

        conn.executemany(
            """
            INSERT INTO dim_cost_category(
                category_code, category_name, level1_name, display_name,
                sort_order, is_reserved, is_active, note
            ) VALUES(?,?,?,?,?,?,?,?)
            ON CONFLICT(category_code) DO UPDATE SET
                category_name=excluded.category_name,
                level1_name=excluded.level1_name,
                display_name=excluded.display_name,
                sort_order=excluded.sort_order,
                is_reserved=excluded.is_reserved,
                is_active=excluded.is_active,
                note=excluded.note
            """,
            CATEGORY_SEED,
        )
        conn.commit()
    finally:
        conn.close()


def _latest_batch_id(conn: sqlite3.Connection) -> Optional[str]:
    row = conn.execute(
        "SELECT batch_id FROM cost_import_log WHERE status='success' ORDER BY imported_at DESC LIMIT 1"
    ).fetchone()
    return row[0] if row else None


def _build_allocations(row: Dict[str, Any]) -> list[dict]:
    amount = float(row["amount"])
    service_dates_json = row.get("service_dates_json")
    service_dates = []
    try:
        service_dates = json.loads(service_dates_json) if service_dates_json else []
    except Exception:
        service_dates = []

    if service_dates:
        months = {}
        for ds in service_dates:
            m = str(ds)[:7]
            months[m] = months.get(m, 0) + 1
        total = sum(months.values()) or 1
        out = []
        for m, cnt in sorted(months.items()):
            out.append({
                "analysis_month": m,
                "service_date": None,
                "amount": amount * cnt / total,
                "allocated_ratio": cnt / total,
            })
        return out

    analysis_month = row.get("analysis_month") or row.get("expense_month")
    return [{
        "analysis_month": analysis_month,
        "service_date": row.get("service_date"),
        "amount": amount,
        "allocated_ratio": 1.0,
    }]


def replace_purchase_cost_batch(
    db_path: str,
    df: pd.DataFrame,
    imported_by: str = "admin",
    source_file: str = "",
) -> Dict[str, Any]:
    ensure_snapshot_schema(db_path)
    ensure_cost_schema(db_path)
    if df is None or df.empty:
        raise ValueError("未解析到任何采购费用数据")

    batch_id = str(uuid.uuid4())
    import_id = str(uuid.uuid4())
    imported_at = _utcnow_iso()

    conn = sqlite3.connect(db_path)
    try:
        old_batch = _latest_batch_id(conn)
        conn.execute("DELETE FROM fact_operational_cost")
        conn.execute("DELETE FROM fact_purchase_expense")

        purchase_rows = []
        op_rows = []

        for row in df.to_dict(orient="records"):
            expense_id = str(uuid.uuid4())
            purchase_rows.append(
                (
                    expense_id,
                    batch_id,
                    imported_at,
                    imported_by,
                    source_file,
                    row.get("source_sheet"),
                    int(row.get("source_row_no")) if pd.notna(row.get("source_row_no")) else None,
                    row["expense_date"],
                    row["expense_month"],
                    row.get("item_name"),
                    row.get("payee"),
                    float(row["amount"]),
                    row.get("category_name"),
                    row.get("category_code"),
                    row.get("level1_name"),
                    row.get("remark"),
                    row.get("service_date"),
                    row.get("service_month"),
                    row.get("analysis_month"),
                    row.get("date_source"),
                    row.get("service_dates_json"),
                    row.get("service_months_json"),
                    row.get("raw_json"),
                    row.get("row_hash"),
                )
            )

            for alloc in _build_allocations(row):
                op_rows.append(
                    (
                        str(uuid.uuid4()),
                        "purchase",
                        row.get("row_hash"),
                        batch_id,
                        row["expense_date"],
                        row["expense_month"],
                        alloc.get("service_date"),
                        alloc["analysis_month"],
                        float(alloc["amount"]),
                        float(alloc.get("allocated_ratio") or 1.0),
                        row.get("category_code"),
                        row.get("category_name"),
                        row.get("level1_name"),
                        row.get("payee"),
                        row.get("item_name"),
                        row.get("remark"),
                        source_file,
                        row.get("date_source"),
                    )
                )

        conn.executemany(
            """
            INSERT INTO fact_purchase_expense(
                expense_id, batch_id, imported_at, imported_by, source_file, source_sheet, source_row_no,
                expense_date, expense_month, item_name, payee, amount, category_name, category_code,
                level1_name, remark, service_date, service_month, analysis_month, date_source,
                service_dates_json, service_months_json, raw_json, row_hash
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            purchase_rows,
        )

        conn.executemany(
            """
            INSERT INTO fact_operational_cost(
                cost_id, source_type, source_row_hash, batch_id, expense_date, payment_month, service_date,
                analysis_month, amount, allocated_ratio, category_code, category_name, level1_name,
                payee, item_name, remark, source_file, date_source
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            op_rows,
        )

        conn.execute(
            """
            INSERT INTO cost_import_log(import_id, batch_id, imported_at, imported_by, source_file, source_sheet, rows_written, status, error_message)
            VALUES(?,?,?,?,?,?,?,?,?)
            """,
            (import_id, batch_id, imported_at, imported_by, source_file, None, int(len(df)), "success", None),
        )
        conn.commit()
        return {
            "batch_id": batch_id,
            "replaced_batch_id": old_batch,
            "rows_written": int(len(df)),
            "imported_at": imported_at,
        }
    except Exception as e:
        conn.rollback()
        try:
            conn.execute(
                """
                INSERT INTO cost_import_log(import_id, batch_id, imported_at, imported_by, source_file, source_sheet, rows_written, status, error_message)
                VALUES(?,?,?,?,?,?,?,?,?)
                """,
                (import_id, batch_id, imported_at, imported_by, source_file, None, 0, "failed", str(e)),
            )
            conn.commit()
        except Exception:
            pass
        raise
    finally:
        conn.close()


def get_latest_cost_import_info(db_path: str) -> Optional[Dict[str, Any]]:
    ensure_cost_schema(db_path)
    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            """
            SELECT imported_at, imported_by, source_file, rows_written, batch_id
            FROM cost_import_log
            WHERE status='success'
            ORDER BY imported_at DESC
            LIMIT 1
            """
        ).fetchone()
        if not row:
            return None
        return {
            "imported_at": row[0],
            "imported_by": row[1],
            "source_file": row[2],
            "rows_written": row[3],
            "batch_id": row[4],
        }
    finally:
        conn.close()


def load_operational_cost_data(db_path: str) -> pd.DataFrame:
    ensure_cost_schema(db_path)
    conn = sqlite3.connect(db_path)
    try:
        return pd.read_sql_query(
            """
            SELECT expense_date, payment_month, service_date, analysis_month, amount, allocated_ratio,
                   category_code, category_name, level1_name, payee, item_name, remark, source_file,
                   date_source, batch_id
            FROM fact_operational_cost
            ORDER BY analysis_month, rowid
            """,
            conn,
        )
    finally:
        conn.close()


def load_purchase_expense_data(db_path: str) -> pd.DataFrame:
    ensure_cost_schema(db_path)
    conn = sqlite3.connect(db_path)
    try:
        return pd.read_sql_query(
            """
            SELECT expense_date, expense_month, item_name, payee, amount, category_name, category_code,
                   level1_name, remark, source_sheet, service_date, service_month, analysis_month,
                   date_source, service_dates_json, service_months_json, batch_id
            FROM fact_purchase_expense
            ORDER BY expense_date, rowid
            """,
            conn,
        )
    finally:
        conn.close()


def load_monthly_incoming_ton(db_path: str) -> pd.DataFrame:
    ensure_snapshot_schema(db_path)
    sid = get_active_snapshot_id(db_path)
    conn = sqlite3.connect(db_path)
    try:
        return pd.read_sql_query(
            """
            SELECT substr(date,1,7) AS analysis_month, SUM(COALESCE(incoming_ton,0)) AS incoming_ton
            FROM fact_daily_ops
            WHERE snapshot_id=?
            GROUP BY substr(date,1,7)
            ORDER BY analysis_month
            """,
            conn,
            params=(sid,),
        )
    finally:
        conn.close()


def build_cost_dashboard_dataset(db_path: str) -> Dict[str, pd.DataFrame]:
    op_df = load_operational_cost_data(db_path)
    purchase_df = load_purchase_expense_data(db_path)

    if op_df.empty:
        return {
            "cost_df": op_df,
            "purchase_df": purchase_df,
            "monthly": pd.DataFrame(),
            "category_monthly": pd.DataFrame(),
            "level1_monthly": pd.DataFrame(),
            "monthly_with_ops": pd.DataFrame(),
        }

    monthly = op_df.groupby("analysis_month", as_index=False).agg(total_cost=("amount", "sum"))
    category_monthly = op_df.groupby(["analysis_month", "category_name"], as_index=False).agg(total_cost=("amount", "sum"))
    level1_monthly = op_df.groupby(["analysis_month", "level1_name"], as_index=False).agg(total_cost=("amount", "sum"))

    ops_monthly = load_monthly_incoming_ton(db_path)
    monthly_with_ops = monthly.merge(ops_monthly, on="analysis_month", how="left")
    monthly_with_ops["ton_cost"] = np.where(
        monthly_with_ops["incoming_ton"].fillna(0) > 0,
        monthly_with_ops["total_cost"] / monthly_with_ops["incoming_ton"],
        np.nan,
    )

    return {
        "cost_df": op_df,
        "purchase_df": purchase_df,
        "monthly": monthly,
        "category_monthly": category_monthly,
        "level1_monthly": level1_monthly,
        "monthly_with_ops": monthly_with_ops,
    }
