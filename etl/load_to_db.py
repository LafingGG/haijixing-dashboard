# -*- coding: utf-8 -*-
from __future__ import annotations

import sys
import os
import sqlite3
import pandas as pd

from etl.parse_excel import parse_workbook
from utils.snapshot import ensure_snapshot_schema, write_daily_ops_snapshot


DEFAULT_DB_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "project.db"))


def load_xlsx_to_db_snapshot(xlsx_path: str, db_path: str, created_by: str = "admin") -> tuple[str, int, str]:
    """
    Parse Excel and write into a NEW staging snapshot.
    Returns (snapshot_id, rows, db_path).
    """
    if not os.path.exists(xlsx_path):
        raise FileNotFoundError(f"找不到文件: {xlsx_path}")

    df = parse_workbook(xlsx_path)
    if df.empty:
        raise ValueError("未解析到数据（请检查 sheet/表头是否符合预期）")

    db_path = os.path.abspath(db_path)
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    ensure_snapshot_schema(db_path)
    sid = write_daily_ops_snapshot(db_path=db_path, df=df, created_by=created_by, filename=os.path.basename(xlsx_path))
    return sid, len(df), db_path


def main():
    if len(sys.argv) < 2:
        print('用法: python -m etl.load_to_db "你的excel路径.xlsx" [--db /path/to/db.sqlite] [--by username]')
        sys.exit(1)

    xlsx_path = sys.argv[1]
    db_path = DEFAULT_DB_PATH
    created_by = "cli"

    # optional: allow overriding db path
    if "--db" in sys.argv:
        i = sys.argv.index("--db")
        if i + 1 < len(sys.argv):
            db_path = sys.argv[i + 1]

    if "--by" in sys.argv:
        i = sys.argv.index("--by")
        if i + 1 < len(sys.argv):
            created_by = sys.argv[i + 1]

    try:
        sid, n, p = load_xlsx_to_db_snapshot(xlsx_path, db_path, created_by=created_by)
        print(f"导入完成：{n} 行 -> {p} (staging snapshot: {sid})")
        print("提示：需要发布才能让查看者看到最新数据。")
    except Exception as e:
        print(str(e))
        sys.exit(2)


if __name__ == "__main__":
    main()
