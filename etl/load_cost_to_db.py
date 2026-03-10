# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sys

from etl.parse_purchase_excel import parse_purchase_workbook
from utils.cost_store import ensure_cost_schema, replace_purchase_cost_batch
from utils.paths import get_db_path

DEFAULT_DB_PATH = get_db_path()


def load_purchase_xlsx_to_db(xlsx_path: str, db_path: str, imported_by: str = "admin") -> dict:
    if not os.path.exists(xlsx_path):
        raise FileNotFoundError(f"找不到文件: {xlsx_path}")
    ensure_cost_schema(db_path)
    df = parse_purchase_workbook(xlsx_path)
    if df.empty:
        raise ValueError("未解析到采购费用数据（请检查 sheet 名称、表头字段或表格结构）")
    return replace_purchase_cost_batch(db_path, df, imported_by=imported_by, source_file=os.path.basename(xlsx_path))


def main():
    if len(sys.argv) < 2:
        print('用法: python -m etl.load_cost_to_db "采购表路径.xlsx" [--db /path/to/db.sqlite] [--by username]')
        sys.exit(1)

    xlsx_path = sys.argv[1]
    db_path = DEFAULT_DB_PATH
    imported_by = "cli"

    if "--db" in sys.argv:
        i = sys.argv.index("--db")
        if i + 1 < len(sys.argv):
            db_path = sys.argv[i + 1]
    if "--by" in sys.argv:
        i = sys.argv.index("--by")
        if i + 1 < len(sys.argv):
            imported_by = sys.argv[i + 1]

    try:
        info = load_purchase_xlsx_to_db(xlsx_path, db_path, imported_by=imported_by)
        print(f"采购费用导入完成：{info['rows_written']} 行 -> {db_path}")
        print(f"batch_id: {info['batch_id']}")
    except Exception as e:
        print(str(e))
        sys.exit(2)


if __name__ == "__main__":
    main()
