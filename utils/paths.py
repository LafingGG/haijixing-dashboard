# utils/paths.py
import os
import streamlit as st

def get_db_path() -> str:
    """
    Single source of truth for DB path.
    Priority:
      1) Streamlit secrets (cloud)
      2) env var
      3) repo default
    """
    # 1) Streamlit Cloud secrets
    if hasattr(st, "secrets") and "DB_PATH" in st.secrets:
        return st.secrets["DB_PATH"]

    # 2) environment variable
    env = os.getenv("DB_PATH")
    if env:
        return env

    # 3) default in repo
    return "data/project.db"