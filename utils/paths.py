# -*- coding: utf-8 -*-
from __future__ import annotations

import os


def get_project_root() -> str:
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def get_data_dir() -> str:
    data_dir = os.environ.get("HX_DATA_DIR", "").strip()
    if data_dir:
        os.makedirs(data_dir, exist_ok=True)
        return data_dir

    default_dir = os.path.join(get_project_root(), "data")
    os.makedirs(default_dir, exist_ok=True)
    return default_dir


def get_db_path() -> str:
    env_path = os.environ.get("HX_DB_PATH", "").strip()
    if env_path:
        db_dir = os.path.dirname(env_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)
        return env_path

    return os.path.join(get_data_dir(), "project.db")
