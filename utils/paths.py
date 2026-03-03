# utils/paths.py
import os

def get_db_path():
    return os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "project.db")