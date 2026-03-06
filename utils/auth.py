# -*- coding: utf-8 -*-
from __future__ import annotations

import hmac
from dataclasses import dataclass
from typing import Optional, Dict, Any

import streamlit as st
from streamlit.errors import StreamlitSecretNotFoundError


# ✅ 版本标记：用于确认运行的就是这个文件
AUTH_FILE_VERSION = "2026-03-05-auth-enforced-login-v1"


@dataclass(frozen=True)
class User:
    username: str
    role: str  # "admin" | "viewer"


# ----------------------------
# secrets helpers
# ----------------------------
def _secrets_get(path: str, default=None):
    """
    Read nested secrets from st.secrets safely.

    NOTE:
    - st.secrets is NOT a dict; it's a Secrets object.
    - Use __getitem__ (cur[part]) for access.
    """
    try:
        cur: Any = st.secrets
        for part in path.split("."):
            try:
                cur = cur[part]
            except Exception:
                return default
        return cur
    except StreamlitSecretNotFoundError:
        return default


def _get_users_from_secrets() -> Dict[str, Dict[str, str]]:
    users = _secrets_get("USERS", default={})
    return dict(users) if users else {}


def is_auth_enabled() -> bool:
    v = _secrets_get("AUTH.ENABLED", default=False)
    if isinstance(v, bool):
        return v
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}


def _verify_password(input_pwd: str, stored_pwd: str) -> bool:
    if stored_pwd is None:
        return False
    return hmac.compare_digest(str(input_pwd), str(stored_pwd))


# ----------------------------
# session helpers
# ----------------------------
def get_current_user() -> Optional[User]:
    u = st.session_state.get("_user")
    return u if isinstance(u, User) else None


def _invalidate_session_user_if_needed() -> None:
    """
    ✅ 核心修复：
    如果 AUTH 已启用且 USERS 已配置，则 session 里的 _user 必须：
    - username 存在于 USERS
    - role 与 USERS 中一致（防止 stale role）
    否则清掉 _user，让系统回到登录框。
    """
    enabled = is_auth_enabled()
    users = _get_users_from_secrets()

    if not enabled or not users:
        return  # dev 模式不做校验

    u = get_current_user()
    if not u:
        return

    info = users.get(u.username)
    if not info:
        # session 里残留了 fallback 的 local_admin 等账号
        st.session_state.pop("_user", None)
        st.session_state["_force_login"] = True
        return

    # role 不一致也视为无效（避免改了 secrets 后仍沿用旧 role）
    role_cfg = info.get("role", "viewer")
    if u.role != role_cfg:
        st.session_state.pop("_user", None)
        st.session_state["_force_login"] = True
        return


def logout_button() -> None:
    """
    Robust logout:
    - clear user session
    - clear login input cache
    - force showing login UI on next run
    """
    if st.sidebar.button("退出登录"):
        st.session_state.pop("_user", None)
        st.session_state.pop("_login_username", None)
        st.session_state.pop("_login_password", None)
        st.session_state["_force_login"] = True
        st.rerun()


# ----------------------------
# UI
# ----------------------------
def login_widget() -> Optional[User]:
    # 侧边栏显示版本标记（方便你确认生效）
    st.sidebar.caption(f"auth: {AUTH_FILE_VERSION}")

    enabled = is_auth_enabled()
    users = _get_users_from_secrets()

    # If user explicitly clicked logout, force showing login UI once.
    if st.session_state.get("_force_login"):
        st.session_state["_force_login"] = False
        enabled = True

    # Dev fallback only when auth not enabled or no users configured
    if (not enabled) or (not users):
        st.sidebar.markdown("### 🔐 登录（开发模式）")
        st.sidebar.info(
            "当前未启用 AUTH 或未配置 USERS，已自动以管理员身份进入。\n\n"
            "如需启用登录：请在 `.streamlit/secrets.toml` 配置 `[AUTH] ENABLED=true` 和 `[USERS.xxx]`。"
        )
        u = User(username="local_admin", role="admin")
        st.session_state["_user"] = u
        return u

    # Normal login
    st.sidebar.markdown("### 🔐 登录")
    username = st.sidebar.text_input("账号", key="_login_username")
    password = st.sidebar.text_input("密码", type="password", key="_login_password")

    if st.sidebar.button("登录"):
        info = users.get(username)
        if not info:
            st.sidebar.error("账号不存在")
            return None
        if not _verify_password(password, info.get("password", "")):
            st.sidebar.error("密码错误")
            return None

        role = info.get("role", "viewer")
        u = User(username=username, role=role)
        st.session_state["_user"] = u
        st.session_state.pop("_login_password", None)
        st.rerun()

    return get_current_user()


def require_login() -> User:
    # ✅ 先做“session user 有效性校验”，避免一直卡在 fallback 的 local_admin
    _invalidate_session_user_if_needed()

    u = get_current_user()
    if u:
        return u

    u2 = login_widget()
    if u2:
        return u2
    st.stop()


def require_role(role: str) -> User:
    u = require_login()
    if u.role != role:
        st.error("无权限访问该功能")
        st.stop()
    return u