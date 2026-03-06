# -*- coding: utf-8 -*-
from __future__ import annotations

import os
from typing import Optional

from utils.snapshot import get_active_snapshot_id, get_staging_snapshot_id, ensure_snapshot_schema


def _data_dir_from_db(db_path: str) -> str:
    # db_path = <project>/data/project.db
    return os.path.dirname(db_path)


def _device_dir(db_path: str) -> str:
    return os.path.join(_data_dir_from_db(db_path), "device_snapshots")


def get_device_snapshot_file(db_path: str, snapshot_id: str) -> str:
    return os.path.join(_device_dir(db_path), f"device_{snapshot_id}.xlsx")


def save_device_excel_for_staging(db_path: str, content_bytes: bytes) -> str:
    """
    Save uploaded device excel under current staging snapshot_id.
    Returns saved file path.
    """
    ensure_snapshot_schema(db_path)
    sid = get_staging_snapshot_id(db_path)
    os.makedirs(_device_dir(db_path), exist_ok=True)
    p = get_device_snapshot_file(db_path, sid)
    with open(p, "wb") as f:
        f.write(content_bytes)
    return p


def get_published_device_excel_path(db_path: str) -> Optional[str]:
    ensure_snapshot_schema(db_path)
    sid = get_active_snapshot_id(db_path)
    p = get_device_snapshot_file(db_path, sid)
    return p if os.path.exists(p) else None
