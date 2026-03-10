# -*- coding: utf-8 -*-
from __future__ import annotations

import streamlit as st

from utils.auth import require_login, logout_button, User
from utils.snapshot import ensure_snapshot_schema
from utils.cost_store import ensure_cost_schema


def bootstrap_page(db_path: str, page_title: str = "") -> User:
    """
    Common bootstrap for each page:
    - ensure DB schema (snapshot)
    - login (or dev fallback)
    """
    ensure_snapshot_schema(db_path)
    ensure_cost_schema(db_path)
    u = require_login()
    # show small identity
    st.sidebar.caption(f"👤 {u.username} · {u.role}")
    logout_button()
    if page_title:
        st.caption(page_title)
    return u
