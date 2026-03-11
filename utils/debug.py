# -*- coding: utf-8 -*-
from __future__ import annotations

import os
from typing import Optional

import pandas as pd
import streamlit as st
from streamlit.errors import StreamlitSecretNotFoundError


def as_bool(v) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return False
    if isinstance(v, (int, float)):
        return bool(v)
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}


def get_debug_flag() -> bool:
    try:
        return as_bool(st.secrets.get("DEBUG", False))
    except StreamlitSecretNotFoundError:
        return False


def render_debug_sidebar(db_path: str, snapshot_id: Optional[str], preview_df: Optional[pd.DataFrame] = None) -> None:
    if not get_debug_flag():
        return

    st.sidebar.markdown("### 🔎 Debug")
    st.sidebar.caption(f"DB_PATH: `{db_path}`")
    st.sidebar.caption(f"exists: `{os.path.exists(db_path)}`")
    if os.path.exists(db_path):
        st.sidebar.caption(f"size: `{os.path.getsize(db_path)} bytes`")
    if snapshot_id:
        st.sidebar.caption(f"snapshot: `{snapshot_id[:8]}`")

    if preview_df is not None and not preview_df.empty:
        with st.sidebar.expander("preview columns"):
            st.write(list(preview_df.columns))
        with st.sidebar.expander("preview row[0]"):
            st.write(preview_df.head(1).to_dict(orient="records")[0])