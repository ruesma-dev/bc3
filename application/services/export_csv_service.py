# application/services/export_csv_service.py
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from application.services.build_tree_service import Node


def _flatten(node: Node, acc: List[Dict[str, Any]]) -> None:
    acc.append(
        {
            "tipo": node.kind,
            "codigo": node.code,
            "descripcion_corta": node.description,
            "descripcion_larga": node.long_desc or "",
            "unidad": node.unidad or "",
            "precio": node.precio if node.precio is not None else "",
            "cantidad_pres": node.can_pres if node.can_pres is not None else "",
            "importe_pres": node.imp_pres if node.imp_pres is not None else "",
            "hijos": ",".join(child.code for child in node.children)
            if node.children
            else "",
            "mediciones": "⏎".join(node.measurements),
        }
    )
    for child in node.children:
        _flatten(child, acc)


def export_to_csv(roots: List[Node], csv_path: Path, sep: str = ";") -> None:
    rows: List[Dict[str, Any]] = []
    for root in roots:
        _flatten(root, rows)

    df = pd.DataFrame(
        rows,
        columns=[
            "tipo",
            "codigo",
            "descripcion_corta",
            "descripcion_larga",
            "unidad",
            "precio",
            "cantidad_pres",
            "importe_pres",
            "hijos",
            "mediciones",
        ],
    )
    df.to_csv(csv_path, sep=sep, index=False, encoding="utf-8")
