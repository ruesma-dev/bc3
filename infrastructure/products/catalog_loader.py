# infrastructure/products/catalog_loader.py
from __future__ import annotations
from pathlib import Path
from typing import List, Dict
import pandas as pd


def load_catalog(excel_path: Path) -> List[Dict[str, str]]:
    """
    Lee Excel de catálogo (col0=código, col1=descripción completa).
    Devuelve lista de dicts [{"code": "...", "desc": "..."}].
    """
    if not excel_path.exists():
        raise FileNotFoundError(excel_path)
    df = pd.read_excel(excel_path, engine="openpyxl")
    if df.shape[1] < 2:
        raise ValueError("El Excel debe tener al menos 2 columnas: código y descripción.")
    df = df.iloc[:, :2].copy()
    df.columns = ["code", "desc"]
    df["code"] = df["code"].astype(str).str.strip()
    df["desc"] = df["desc"].astype(str).str.strip()
    rows = []
    for _, r in df.iterrows():
        code = r["code"]
        desc = r["desc"]
        if code:
            rows.append({"code": code[:20], "desc": desc})
    if not rows:
        raise ValueError("Catálogo vacío tras limpieza.")
    return rows
