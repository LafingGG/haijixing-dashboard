# utils/config.py
from __future__ import annotations
import os
from dataclasses import dataclass, asdict
import yaml

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
CONFIG_PATH = os.path.join(ROOT_DIR, "config.yaml")


@dataclass
class Thresholds:
    slag_rate_high: float = 0.75
    water_intensity_high: float = 0.50
    elec_intensity_high: float = 80.0
    water_m3_high: float = 999999.0
    daily_elec_kwh_high: float = 999999.0


def load_thresholds() -> Thresholds:
    if not os.path.exists(CONFIG_PATH):
        return Thresholds()
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    t = (data.get("thresholds") or {})
    base = Thresholds()
    # 只覆盖存在的字段，防止配置缺字段报错
    for k in asdict(base).keys():
        if k in t and t[k] is not None:
            setattr(base, k, float(t[k]))
    return base


def save_thresholds(th: Thresholds) -> None:
    data = {"thresholds": asdict(th)}
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, allow_unicode=True, sort_keys=False)