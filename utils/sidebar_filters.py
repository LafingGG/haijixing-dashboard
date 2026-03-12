# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import date
from typing import Dict, List, Tuple

import pandas as pd
import streamlit as st


def _clamp_date(d: date, min_date: date, max_date: date) -> date:
    return min(max(d, min_date), max_date)


def _month_to_range(month_str: str, min_date: date, max_date: date) -> Tuple[date, date]:
    p = pd.Period(month_str)
    start = _clamp_date(p.start_time.date(), min_date, max_date)
    end = _clamp_date(p.end_time.date(), min_date, max_date)
    return start, end


def _get_month_from_date(d: date) -> str:
    return pd.Timestamp(d).to_period("M").strftime("%Y-%m")


def _infer_month_pick(start_date: date, end_date: date, months: List[str]) -> str:
    start_month = pd.Timestamp(start_date).to_period("M")
    end_month = pd.Timestamp(end_date).to_period("M")

    if start_month != end_month:
        return "自定义"

    month_str = start_month.strftime("%Y-%m")
    month_start = start_month.start_time.date()
    month_end = start_month.end_time.date()

    if start_date == month_start and end_date == month_end and month_str in months:
        return month_str

    return "自定义"


def _ensure_state(min_date: date, max_date: date) -> None:
    if "global_start_date" not in st.session_state:
        st.session_state.global_start_date = min_date

    if "global_end_date" not in st.session_state:
        st.session_state.global_end_date = max_date

    st.session_state.global_start_date = _clamp_date(
        st.session_state.global_start_date, min_date, max_date
    )
    st.session_state.global_end_date = _clamp_date(
        st.session_state.global_end_date, min_date, max_date
    )

    if st.session_state.global_start_date > st.session_state.global_end_date:
        st.session_state.global_start_date, st.session_state.global_end_date = (
            st.session_state.global_end_date,
            st.session_state.global_start_date,
        )


def _apply_month(month_str: str, min_date: date, max_date: date) -> None:
    start, end = _month_to_range(month_str, min_date, max_date)
    st.session_state.global_start_date = start
    st.session_state.global_end_date = end


def render_global_sidebar_by_df(
    df: pd.DataFrame,
    date_col: str = "date",
    title: str = "海吉星运营驾驶舱",
    show_data_hint: bool = True,
) -> Tuple[date, date, Dict[str, str]]:
    """
    基于页面 dataframe 渲染统一侧边栏时间控件。

    返回:
        start_date, end_date, meta
    """
    if df is None or df.empty or date_col not in df.columns:
        st.sidebar.markdown(f"## {title}")
        st.sidebar.markdown("---")
        st.sidebar.warning("当前页面暂无可用日期数据")
        today = date.today()
        meta = {
            "label": f"{today.isoformat()} ~ {today.isoformat()}",
            "month_pick": "自定义",
            "days": "0",
        }
        return today, today, meta

    tmp = df.copy()
    tmp[date_col] = pd.to_datetime(tmp[date_col], errors="coerce")
    tmp = tmp.dropna(subset=[date_col]).sort_values(date_col)

    if tmp.empty:
        st.sidebar.markdown(f"## {title}")
        st.sidebar.markdown("---")
        st.sidebar.warning("当前页面暂无有效日期数据")
        today = date.today()
        meta = {
            "label": f"{today.isoformat()} ~ {today.isoformat()}",
            "month_pick": "自定义",
            "days": "0",
        }
        return today, today, meta

    min_date = tmp[date_col].min().date()
    max_date = tmp[date_col].max().date()
    months = sorted(tmp[date_col].dt.to_period("M").astype(str).unique().tolist())

    _ensure_state(min_date, max_date)

    inferred_month_pick = _infer_month_pick(
        st.session_state.global_start_date,
        st.session_state.global_end_date,
        months,
    )

    valid_options = ["自定义"] + months

    # 这个 key 只给 selectbox 当 widget key 用
    # 每轮在 widget 创建前，根据当前日期区间决定默认显示值
    st.session_state["global_month_pick_widget"] = (
        inferred_month_pick if inferred_month_pick in valid_options else "自定义"
    )

    def _on_month_change():
        picked = st.session_state["global_month_pick_widget"]
        if picked != "自定义":
            _apply_month(picked, min_date, max_date)

    st.sidebar.markdown(f"## {title}")
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 📅 时间范围")

    st.sidebar.selectbox(
        "快捷月份",
        options=valid_options,
        key="global_month_pick_widget",
        on_change=_on_month_change,
    )

    current_month = _get_month_from_date(st.session_state.global_start_date)
    display_month_pick = _infer_month_pick(
        st.session_state.global_start_date,
        st.session_state.global_end_date,
        months,
    )
    st.sidebar.caption(f"当前月份：{current_month} ｜ 模式：{display_month_pick}")

    c1, c2 = st.sidebar.columns(2)

    with c1:
        if st.button("◀ 上一月", use_container_width=True, key="global_prev_month_btn"):
            if current_month in months:
                idx = months.index(current_month)
                if idx > 0:
                    prev_month = months[idx - 1]
                    _apply_month(prev_month, min_date, max_date)
                    st.rerun()

    with c2:
        if st.button("下一月 ▶", use_container_width=True, key="global_next_month_btn"):
            if current_month in months:
                idx = months.index(current_month)
                if idx < len(months) - 1:
                    next_month = months[idx + 1]
                    _apply_month(next_month, min_date, max_date)
                    st.rerun()

    st.sidebar.divider()

    start_date = st.sidebar.date_input(
        "开始日期",
        value=st.session_state.global_start_date,
        min_value=min_date,
        max_value=max_date,
        key="global_start_date",
    )

    end_date = st.sidebar.date_input(
        "结束日期",
        value=st.session_state.global_end_date,
        min_value=min_date,
        max_value=max_date,
        key="global_end_date",
    )

    if start_date > end_date:
        start_date, end_date = end_date, start_date
        st.session_state.global_start_date = start_date
        st.session_state.global_end_date = end_date

    display_month_pick = _infer_month_pick(start_date, end_date, months)
    days = (pd.Timestamp(end_date) - pd.Timestamp(start_date)).days + 1

    st.sidebar.caption(f"当前区间：{start_date.isoformat()} ~ {end_date.isoformat()}")
    st.sidebar.caption(f"覆盖天数：{days} 天")
    st.sidebar.caption(f"数据边界：{min_date.isoformat()} ~ {max_date.isoformat()}")

    if show_data_hint:
        st.sidebar.markdown("---")
        st.sidebar.markdown("### ℹ️ 数据说明")
        st.sidebar.caption("生产数据通常会滞后 2–3 天")
        st.sidebar.caption("采购费用与大额付款可能跨月入账")

    meta = {
        "label": f"{start_date.isoformat()} ~ {end_date.isoformat()}",
        "month_pick": display_month_pick,
        "days": str(days),
        "data_min": min_date.isoformat(),
        "data_max": max_date.isoformat(),
    }
    return start_date, end_date, meta