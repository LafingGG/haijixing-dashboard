# utils/config.py
from __future__ import annotations

import os
from dataclasses import dataclass, asdict

import yaml

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
CONFIG_PATH = os.path.join(ROOT_DIR, "config.yaml")
DEFAULT_BUCKET_TO_TON = 0.2


@dataclass
class Thresholds:
    slag_rate_high: float = 0.75
    water_intensity_high: float = 0.50
    elec_intensity_high: float = 80.0
    water_m3_high: float = 999999.0
    daily_elec_kwh_high: float = 999999.0


@dataclass
class OpsSettings:
    bucket_to_ton: float = DEFAULT_BUCKET_TO_TON


def _load_yaml() -> dict:
    if not os.path.exists(CONFIG_PATH):
        return {}
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def load_thresholds() -> Thresholds:
    data = _load_yaml()
    t = (data.get("thresholds") or {})
    base = Thresholds()
    for k in asdict(base).keys():
        if k in t and t[k] is not None:
            setattr(base, k, float(t[k]))
    return base


def save_thresholds(th: Thresholds) -> None:
    data = _load_yaml()
    data["thresholds"] = asdict(th)
    if "ops" not in data:
        data["ops"] = {"bucket_to_ton": DEFAULT_BUCKET_TO_TON}
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, allow_unicode=True, sort_keys=False)


def load_ops_settings() -> OpsSettings:
    data = _load_yaml()
    ops = data.get("ops") or {}
    bucket_to_ton = ops.get("bucket_to_ton", DEFAULT_BUCKET_TO_TON)
    try:
        bucket_to_ton = float(bucket_to_ton)
    except Exception:
        bucket_to_ton = DEFAULT_BUCKET_TO_TON
    if bucket_to_ton <= 0:
        bucket_to_ton = DEFAULT_BUCKET_TO_TON
    return OpsSettings(bucket_to_ton=bucket_to_ton)


def get_bucket_to_ton() -> float:
    return load_ops_settings().bucket_to_ton
