# infrastructure/filesystem/app_paths.py
from __future__ import annotations

import sys
from pathlib import Path


def get_app_base_dir() -> Path:
    if getattr(sys, "frozen", False) and hasattr(sys, "executable"):
        return Path(sys.executable).resolve().parent
    return Path(sys.argv[0]).resolve().parent
