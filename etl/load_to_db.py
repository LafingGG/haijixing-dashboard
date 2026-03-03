# -*- coding: utf-8 -*-
from __future__ import annotations

import sys
import os
import sqlite3
import pandas as pd

from etl.parse_excel import parse_workbook


DEFAULT_DB_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "db", "ops.sqlite"))

CREATE_SQL = """
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


def upsert_df(conn: sqlite3.Connection, df: pd.DataFrame) -> None:
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

    conn.executemany(sql, df2.itertuples(index=False, name=None))
    conn.commit()


def load_xlsx_to_db(xlsx_path: str, db_path: str) -> tuple[int, str]:
    """Parse Excel and upsert into sqlite db. Returns (rows, db_path)."""
    if not os.path.exists(xlsx_path):
        raise FileNotFoundError(f"找不到文件: {xlsx_path}")

    df = parse_workbook(xlsx_path)
    if df.empty:
        raise ValueError("未解析到数据（请检查 sheet/表头是否符合预期）")

    db_path = os.path.abspath(db_path)
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    conn = sqlite3.connect(db_path)
    conn.execute(CREATE_SQL)
    upsert_df(conn, df)
    conn.close()

    return len(df), db_path


def main():
    if len(sys.argv) < 2:
        print('用法: python -m etl.load_to_db "你的excel路径.xlsx" [--db /path/to/db.sqlite]')
        sys.exit(1)

    xlsx_path = sys.argv[1]
    db_path = DEFAULT_DB_PATH

    # optional: allow overriding db path
    if "--db" in sys.argv:
        i = sys.argv.index("--db")
        if i + 1 < len(sys.argv):
            db_path = sys.argv[i + 1]

    try:
        n, p = load_xlsx_to_db(xlsx_path, db_path)
        print(f"导入完成：{n} 行 -> {p}")
    except Exception as e:
        print(str(e))
        sys.exit(2)


if __name__ == "__main__":
    main()