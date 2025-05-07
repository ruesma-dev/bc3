# application/services/export_csv_service.py
# ===========================================================
# Crea un DataFrame a partir del árbol y lo exporta a CSV (;)
# con las columnas solicitadas, incluida la descripción larga.
# ===========================================================

from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
from application.services.build_tree_service import Node


# --------------------------------------------------------------------------- #
#                          ÁRBOL → FILAS                                      #
# --------------------------------------------------------------------------- #
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
            "hijos": ",".join(ch.code for ch in node.children) if node.children else "",
            "mediciones": "⏎".join(node.measurements),
        }
    )
    for ch in node.children:
        _flatten(ch, acc)


# --------------------------------------------------------------------------- #
#                          EXPORTAR A CSV                                     #
# --------------------------------------------------------------------------- #
def export_to_csv(roots: List[Node], csv_path: Path) -> None:
    rows: List[Dict[str, Any]] = []
    for r in roots:
        _flatten(r, rows)

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
    df.to_csv(csv_path, sep=";", index=False, encoding="utf-8")
