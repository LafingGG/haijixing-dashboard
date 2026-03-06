# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sqlite3
import pandas as pd
import streamlit as st

from utils.definitions import DEFINITIONS_MD, DEFINITIONS_VERSION
from utils.paths import get_db_path
from utils.bootstrap import bootstrap_page
from utils.snapshot import get_active_snapshot_id
from utils.guards import require_admin

DB_PATH = get_db_path()
user = bootstrap_page(DB_PATH)
require_admin(user)  # ✅ v1.4-stable: viewer 直接拦截

ACTIVE_SNAPSHOT_ID = get_active_snapshot_id(DB_PATH)


def _as_bool(v) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return False
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}


st.set_page_config(page_title="数据质量", layout="wide")
st.title("🧪 数据质量")
st.caption(f"Definitions: {DEFINITIONS_VERSION}")

# ---- 以下保留你原来的页面逻辑（如果你原文件后面还有内容，请保持不变） ----
# 这里我不重写你现有的统计细节，避免破坏现有行为。
# 你把 require_admin(user) 加进去后，厂长就进不来了。
#
# 如果你希望我把本页也做成“更稳定的异常兜底”，把你原文件剩余部分也贴出来，我再给你整页可替换版。