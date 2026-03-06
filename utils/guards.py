# -*- coding: utf-8 -*-
from __future__ import annotations

import streamlit as st


def require_admin(user) -> None:
    if getattr(user, "role", "") != "admin":
        st.warning("你没有权限查看本页面。")
        st.stop()