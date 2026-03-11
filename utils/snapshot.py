# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sqlite3
import uuid
from datetime import datetime
from typing import Optional, Tuple, Dict, Any

import pandas as pd


def _utcnow_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


SNAPSHOT_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS snapshots (
    snapshot_id TEXT PRIMARY KEY,
    created_at TEXT NOT NULL,
    created_by TEXT,
    status TEXT NOT NULL,          -- staging / published / archived
    note TEXT
);
"""

SETTINGS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS app_settings (
    key TEXT PRIMARY KEY,
    value TEXT
);
"""

IMPORT_LOG_SQL = """
CREATE TABLE IF NOT EXISTS etl_import_log (
    import_id TEXT PRIMARY KEY,
    snapshot_id TEXT NOT NULL,
    uploaded_at TEXT NOT NULL,
    uploaded_by TEXT,
    filename TEXT,
    table_name TEXT,
    date_min TEXT,
    date_max TEXT,
    rows_written INTEGER,
    status TEXT NOT NULL,          -- success / failed
    error_message TEXT
);
"""

# ✅ v1.4-stable: publish log for traceability + rollback support
PUBLISH_LOG_SQL = """
CREATE TABLE IF NOT EXISTS publish_log (
    log_id TEXT PRIMARY KEY,
    action TEXT NOT NULL,                 -- publish / rollback
    happened_at TEXT NOT NULL,
    operator TEXT,
    active_snapshot_id TEXT,
    staging_snapshot_id TEXT,
    prev_active_snapshot_id TEXT,
    ops_filename TEXT,
    ops_rows INTEGER,
    date_min TEXT,
    date_max TEXT,
    device_attached INTEGER DEFAULT 0,
    note TEXT
);
"""


def ensure_snapshot_schema(db_path: str) -> None:
    """
    Ensure snapshot tables exist and migrate fact_daily_ops to v2 schema:
    - old: fact_daily_ops(date PRIMARY KEY, ...)
    - new: fact_daily_ops(snapshot_id, date, ..., PRIMARY KEY(snapshot_id, date))

    v1.4-stable:
    - ensure publish_log exists
    - keep migration robust for old DBs
    """
    if not db_path or not str(db_path).strip():
        raise ValueError("数据库路径为空，请检查 get_db_path() 的返回值。")

    db_dir = os.path.dirname(db_path)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(SNAPSHOT_TABLE_SQL)
        conn.execute(SETTINGS_TABLE_SQL)
        conn.execute(IMPORT_LOG_SQL)
        conn.execute(PUBLISH_LOG_SQL)
        conn.commit()

        # detect current fact_daily_ops schema
        cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='fact_daily_ops'")
        exists = cur.fetchone() is not None
        if not exists:
            _create_fact_daily_ops_v2(conn)
            conn.execute("ALTER TABLE fact_daily_ops_v2 RENAME TO fact_daily_ops")
            _ensure_settings(conn)
            conn.commit()
            return

        cols = [r[1] for r in conn.execute("PRAGMA table_info(fact_daily_ops)").fetchall()]
        if "snapshot_id" in cols:
            # already v2
            _ensure_settings(conn)
            conn.commit()
            return

        # migrate old table -> v2
        legacy_snapshot = f"legacy-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
        _create_fact_daily_ops_v2(conn)

        old_df = pd.read_sql_query("SELECT * FROM fact_daily_ops", conn)
        if not old_df.empty:
            old_df.insert(0, "snapshot_id", legacy_snapshot)
            _write_fact_daily_ops(conn, old_df, conflict_update=True)

        conn.execute("ALTER TABLE fact_daily_ops RENAME TO fact_daily_ops_old")
        conn.execute("ALTER TABLE fact_daily_ops_v2 RENAME TO fact_daily_ops")
        conn.execute("DROP TABLE fact_daily_ops_old")

        _insert_snapshot(conn, legacy_snapshot, created_by="system", status="published", note="auto-migrated from v1")
        _set_setting(conn, "active_snapshot_id", legacy_snapshot)
        _set_setting(conn, "staging_snapshot_id", legacy_snapshot)

        # v1.4: write a baseline publish log so "最近更新"不为空
        _write_publish_log(
            conn,
            action="publish",
            operator="system",
            active_snapshot_id=legacy_snapshot,
            staging_snapshot_id=legacy_snapshot,
            prev_active_snapshot_id=None,
            ops_filename=None,
            ops_rows=len(old_df) if isinstance(old_df, pd.DataFrame) else None,
            date_min=None,
            date_max=None,
            device_attached=0,
            note="auto-migrated baseline",
        )

        conn.commit()
    finally:
        conn.close()


# ----------------------------
# settings helpers
# ----------------------------
def _ensure_settings(conn: sqlite3.Connection) -> None:
    active = _get_setting(conn, "active_snapshot_id")
    staging = _get_setting(conn, "staging_snapshot_id")

    if not active:
        row = conn.execute(
            "SELECT snapshot_id FROM snapshots WHERE status='published' ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        if row:
            _set_setting(conn, "active_snapshot_id", row[0])
        else:
            sid = create_snapshot(conn, created_by="system", status="published", note="init")
            _set_setting(conn, "active_snapshot_id", sid)
            _set_setting(conn, "staging_snapshot_id", sid)

    if not staging:
        _set_setting(conn, "staging_snapshot_id", _get_setting(conn, "active_snapshot_id"))


def _get_setting(conn: sqlite3.Connection, key: str) -> Optional[str]:
    row = conn.execute("SELECT value FROM app_settings WHERE key=?", (key,)).fetchone()
    return row[0] if row else None


def _set_setting(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        "INSERT INTO app_settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value),
    )


def get_active_snapshot_id(db_path: str) -> str:
    ensure_snapshot_schema(db_path)
    conn = sqlite3.connect(db_path)
    try:
        _ensure_settings(conn)
        conn.commit()
        sid = _get_setting(conn, "active_snapshot_id")
        if not sid:
            raise RuntimeError("active_snapshot_id 未初始化")
        return sid
    finally:
        conn.close()


def get_staging_snapshot_id(db_path: str) -> Optional[str]:
    ensure_snapshot_schema(db_path)
    conn = sqlite3.connect(db_path)
    try:
        _ensure_settings(conn)
        conn.commit()
        return _get_setting(conn, "staging_snapshot_id")
    finally:
        conn.close()


# ----------------------------
# snapshots CRUD
# ----------------------------
def create_snapshot(conn: sqlite3.Connection, created_by: str, status: str = "staging", note: str = "") -> str:
    sid = str(uuid.uuid4())
    _insert_snapshot(conn, sid, created_by=created_by, status=status, note=note)
    return sid


def _insert_snapshot(conn: sqlite3.Connection, snapshot_id: str, created_by: str, status: str, note: str = "") -> None:
    conn.execute(
        "INSERT INTO snapshots(snapshot_id, created_at, created_by, status, note) VALUES(?,?,?,?,?)",
        (snapshot_id, _utcnow_iso(), created_by, status, note),
    )


# ----------------------------
# v1.4: publish log helpers
# ----------------------------
def _get_latest_import_for_snapshot(conn: sqlite3.Connection, snapshot_id: str) -> Dict[str, Any]:
    """
    Returns latest etl_import_log row for this snapshot_id (fact_daily_ops).
    """
    row = conn.execute(
        """
        SELECT filename, date_min, date_max, rows_written, uploaded_at, uploaded_by
        FROM etl_import_log
        WHERE snapshot_id=? AND table_name='fact_daily_ops' AND status='success'
        ORDER BY uploaded_at DESC
        LIMIT 1
        """,
        (snapshot_id,),
    ).fetchone()

    if not row:
        return {"filename": None, "date_min": None, "date_max": None, "rows_written": None}

    return {"filename": row[0], "date_min": row[1], "date_max": row[2], "rows_written": row[3]}


def _device_attached_for_snapshot(db_path: str, snapshot_id: str) -> int:
    """
    device file convention: <data_dir>/device_snapshots/device_<snapshot_id>.xlsx
    """
    data_dir = os.path.dirname(db_path)
    p = os.path.join(data_dir, "device_snapshots", f"device_{snapshot_id}.xlsx")
    return 1 if os.path.exists(p) else 0


def _write_publish_log(
    conn: sqlite3.Connection,
    action: str,
    operator: str,
    active_snapshot_id: Optional[str],
    staging_snapshot_id: Optional[str],
    prev_active_snapshot_id: Optional[str],
    ops_filename: Optional[str],
    ops_rows: Optional[int],
    date_min: Optional[str],
    date_max: Optional[str],
    device_attached: int,
    note: str = "",
) -> str:
    log_id = str(uuid.uuid4())
    conn.execute(
        """
        INSERT INTO publish_log(
            log_id, action, happened_at, operator,
            active_snapshot_id, staging_snapshot_id, prev_active_snapshot_id,
            ops_filename, ops_rows, date_min, date_max, device_attached, note
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            log_id,
            action,
            _utcnow_iso(),
            operator,
            active_snapshot_id,
            staging_snapshot_id,
            prev_active_snapshot_id,
            ops_filename,
            ops_rows,
            date_min,
            date_max,
            int(device_attached),
            note,
        ),
    )
    return log_id


def get_last_publish_info(db_path: str) -> Optional[Dict[str, Any]]:
    ensure_snapshot_schema(db_path)
    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            """
            SELECT action, happened_at, operator, active_snapshot_id, prev_active_snapshot_id,
                   ops_filename, ops_rows, date_min, date_max, device_attached, note
            FROM publish_log
            ORDER BY happened_at DESC
            LIMIT 1
            """
        ).fetchone()
        if not row:
            return None
        return {
            "action": row[0],
            "happened_at": row[1],
            "operator": row[2],
            "active_snapshot_id": row[3],
            "prev_active_snapshot_id": row[4],
            "ops_filename": row[5],
            "ops_rows": row[6],
            "date_min": row[7],
            "date_max": row[8],
            "device_attached": row[9],
            "note": row[10],
        }
    finally:
        conn.close()


def list_recent_publish_log(db_path: str, limit: int = 20) -> pd.DataFrame:
    ensure_snapshot_schema(db_path)
    conn = sqlite3.connect(db_path)
    try:
        df = pd.read_sql_query(
            """
            SELECT happened_at, action, operator, active_snapshot_id, prev_active_snapshot_id,
                   ops_filename, ops_rows, date_min, date_max, device_attached, note
            FROM publish_log
            ORDER BY happened_at DESC
            LIMIT ?
            """,
            conn,
            params=(int(limit),),
        )
        return df
    finally:
        conn.close()


# ----------------------------
# publish / rollback
# ----------------------------
def publish_staging(db_path: str, published_by: str = "admin") -> str:
    """
    active_snapshot_id <- staging_snapshot_id

    v1.4:
    - write publish_log
    - archive previous active (keep record)
    """
    ensure_snapshot_schema(db_path)
    conn = sqlite3.connect(db_path)
    try:
        _ensure_settings(conn)
        staging = _get_setting(conn, "staging_snapshot_id")
        if not staging:
            raise RuntimeError("没有 staging 快照可发布")

        active_prev = _get_setting(conn, "active_snapshot_id")

        # archive previous active
        if active_prev and active_prev != staging:
            conn.execute(
                "UPDATE snapshots SET status='archived' WHERE snapshot_id=? AND status!='archived'",
                (active_prev,),
            )

        # mark staging as published
        conn.execute("UPDATE snapshots SET status='published' WHERE snapshot_id=?", (staging,))
        _set_setting(conn, "active_snapshot_id", staging)

        # publish log (best-effort from import log)
        meta = _get_latest_import_for_snapshot(conn, staging)
        device_attached = _device_attached_for_snapshot(db_path, staging)

        _write_publish_log(
            conn,
            action="publish",
            operator=published_by,
            active_snapshot_id=staging,
            staging_snapshot_id=staging,
            prev_active_snapshot_id=active_prev,
            ops_filename=meta.get("filename"),
            ops_rows=meta.get("rows_written"),
            date_min=meta.get("date_min"),
            date_max=meta.get("date_max"),
            device_attached=device_attached,
            note="publish staging -> active",
        )

        conn.commit()
        return staging
    finally:
        conn.close()


def rollback_to_previous_active(db_path: str, operator: str = "admin") -> str:
    """
    v1.4:
    Rollback active_snapshot_id to prev_active_snapshot_id from latest publish_log.
    """
    ensure_snapshot_schema(db_path)
    conn = sqlite3.connect(db_path)
    try:
        _ensure_settings(conn)
        row = conn.execute(
            """
            SELECT prev_active_snapshot_id, active_snapshot_id
            FROM publish_log
            WHERE action='publish'
            ORDER BY happened_at DESC
            LIMIT 1
            """
        ).fetchone()

        if not row or not row[0]:
            raise RuntimeError("没有可回滚的上一版（publish_log 中未找到 prev_active_snapshot_id）")

        prev_active = row[0]
        cur_active = _get_setting(conn, "active_snapshot_id")

        if prev_active == cur_active:
            return cur_active or prev_active

        # set active back
        _set_setting(conn, "active_snapshot_id", prev_active)

        # status bookkeeping (optional)
        conn.execute("UPDATE snapshots SET status='published' WHERE snapshot_id=?", (prev_active,))
        if cur_active:
            conn.execute(
                "UPDATE snapshots SET status='archived' WHERE snapshot_id=? AND status!='archived'",
                (cur_active,),
            )

        meta = _get_latest_import_for_snapshot(conn, prev_active)
        device_attached = _device_attached_for_snapshot(db_path, prev_active)

        _write_publish_log(
            conn,
            action="rollback",
            operator=operator,
            active_snapshot_id=prev_active,
            staging_snapshot_id=_get_setting(conn, "staging_snapshot_id"),
            prev_active_snapshot_id=cur_active,
            ops_filename=meta.get("filename"),
            ops_rows=meta.get("rows_written"),
            date_min=meta.get("date_min"),
            date_max=meta.get("date_max"),
            device_attached=device_attached,
            note="rollback active to previous published snapshot",
        )

        conn.commit()
        return prev_active
    finally:
        conn.close()


# ----------------------------
# ETL write
# ----------------------------
def write_daily_ops_snapshot(db_path: str, df: pd.DataFrame, created_by: str, filename: str = "") -> str:
    """
    Create a new staging snapshot and write df into it.
    Returns snapshot_id.
    """
    ensure_snapshot_schema(db_path)
    conn = sqlite3.connect(db_path)
    try:
        _ensure_settings(conn)
        conn.commit()

        sid = create_snapshot(conn, created_by=created_by, status="staging", note="daily ops import")

        cols = [r[1] for r in conn.execute("PRAGMA table_info(fact_daily_ops)").fetchall()]
        if "snapshot_id" not in cols:
            raise RuntimeError("fact_daily_ops 未升级为快照结构，请先运行 ensure_snapshot_schema()")

        df2 = df.copy()
        df2.insert(0, "snapshot_id", sid)

        rows = _write_fact_daily_ops(conn, df2, conflict_update=True)
        _set_setting(conn, "staging_snapshot_id", sid)

        import_id = str(uuid.uuid4())
        date_min = pd.to_datetime(df["date"]).min()
        date_max = pd.to_datetime(df["date"]).max()
        conn.execute(
            """INSERT INTO etl_import_log(import_id, snapshot_id, uploaded_at, uploaded_by, filename, table_name,
                                          date_min, date_max, rows_written, status, error_message)
               VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
            (
                import_id,
                sid,
                _utcnow_iso(),
                created_by,
                filename,
                "fact_daily_ops",
                date_min.strftime("%Y-%m-%d") if pd.notna(date_min) else None,
                date_max.strftime("%Y-%m-%d") if pd.notna(date_max) else None,
                int(rows),
                "success",
                None,
            ),
        )
        conn.commit()
        return sid
    except Exception as e:
        # best-effort log
        try:
            import_id = str(uuid.uuid4())
            conn.execute(
                """INSERT INTO etl_import_log(import_id, snapshot_id, uploaded_at, uploaded_by, filename, table_name,
                                              status, error_message)
                   VALUES(?,?,?,?,?,?,?,?)""",
                (import_id, "unknown", _utcnow_iso(), created_by, filename, "fact_daily_ops", "failed", str(e)),
            )
            conn.commit()
        except Exception:
            pass
        raise
    finally:
        conn.close()


# ----------------------------
# stats / listing
# ----------------------------
def get_snapshot_stats(db_path: str, snapshot_id: str) -> Dict[str, Any]:
    ensure_snapshot_schema(db_path)
    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT COUNT(*) FROM fact_daily_ops WHERE snapshot_id=?",
            (snapshot_id,),
        ).fetchone()
        rows = int(row[0]) if row else 0

        row2 = conn.execute(
            "SELECT MIN(date), MAX(date) FROM fact_daily_ops WHERE snapshot_id=?",
            (snapshot_id,),
        ).fetchone()
        date_min, date_max = (row2[0], row2[1]) if row2 else (None, None)

        return {"rows": rows, "date_min": date_min, "date_max": date_max}
    finally:
        conn.close()


def list_recent_snapshots(db_path: str, limit: int = 20) -> pd.DataFrame:
    ensure_snapshot_schema(db_path)
    conn = sqlite3.connect(db_path)
    try:
        df = pd.read_sql_query(
            """
            SELECT snapshot_id, created_at, created_by, status, note
            FROM snapshots
            ORDER BY created_at DESC
            LIMIT ?
            """,
            conn,
            params=(int(limit),),
        )
        return df
    finally:
        conn.close()


# ----------------------------
# internal: fact_daily_ops v2
# ----------------------------
def _create_fact_daily_ops_v2(conn: sqlite3.Connection) -> None:
    # v2 schema: snapshot_id + date composite key
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fact_daily_ops_v2 (
            snapshot_id TEXT NOT NULL,
            date TEXT NOT NULL,
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
            source_sheet TEXT,
            PRIMARY KEY(snapshot_id, date)
        );
        """
    )


def _write_fact_daily_ops(conn: sqlite3.Connection, df: pd.DataFrame, conflict_update: bool = True) -> int:
    if df is None or df.empty:
        return 0

    # normalize columns to match table
    cols = [r[1] for r in conn.execute("PRAGMA table_info(fact_daily_ops_v2)").fetchall()]
    if not cols:
        # if already renamed to fact_daily_ops, fallback to that
        cols = [r[1] for r in conn.execute("PRAGMA table_info(fact_daily_ops)").fetchall()]

    df2 = df.copy()
    # ensure date is text YYYY-MM-DD
    if "date" in df2.columns:
        df2["date"] = pd.to_datetime(df2["date"], errors="coerce").dt.strftime("%Y-%m-%d")

    # keep only known cols
    keep = [c for c in cols if c in df2.columns]
    df2 = df2[keep]

    table_name = "fact_daily_ops_v2"
    # if v2 already swapped, insert into fact_daily_ops
    if conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='fact_daily_ops'").fetchone():
        # detect whether fact_daily_ops already has snapshot_id
        cols2 = [r[1] for r in conn.execute("PRAGMA table_info(fact_daily_ops)").fetchall()]
        if "snapshot_id" in cols2:
            table_name = "fact_daily_ops"

    placeholders = ",".join(["?"] * len(keep))
    collist = ",".join(keep)

    if conflict_update:
        # build update set excluding PK columns
        pk = {"snapshot_id", "date"}
        set_cols = [c for c in keep if c not in pk]
        set_sql = ",".join([f"{c}=excluded.{c}" for c in set_cols]) if set_cols else ""
        sql = f"INSERT INTO {table_name}({collist}) VALUES({placeholders})"
        if set_sql:
            sql += f" ON CONFLICT(snapshot_id,date) DO UPDATE SET {set_sql}"
    else:
        sql = f"INSERT OR IGNORE INTO {table_name}({collist}) VALUES({placeholders})"

    conn.executemany(sql, df2.itertuples(index=False, name=None))
    return int(len(df2))