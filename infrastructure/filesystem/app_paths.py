# infrastructure/filesystem/app_paths.py
from __future__ import annotations
from pathlib import Path
import sys

def get_app_base_dir() -> Path:
    """
    Devuelve la carpeta base de la app:
      - si está empaquetada (PyInstaller): carpeta del exe
      - si no: carpeta del entrypoint
    """
    if getattr(sys, "frozen", False) and hasattr(sys, "executable"):
        return Path(sys.executable).resolve().parent
    return Path(sys.argv[0]).resolve().parent
