# application/services/audit_service.py
from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, List, Optional

import json
import pandas as pd
from bc3_lib.domain.node import Node
from application.services.product_selection_service import ProductMatch


def _index_nodes_by_code(roots: List[Node]) -> Dict[str, Node]:
    idx: Dict[str, Node] = {}
    def dfs(n: Node) -> None:
        if n.code not in idx:
            idx[n.code] = n
            for ch in n.children:
                dfs(ch)
    for r in roots:
        dfs(r)
    return idx


@dataclass
class AuditRow:
    node_code_old: str
    node_kind: str
    descripcion_corta: str
    descripcion_larga: str
    unidad: str
    selected_product_code: str
    confidence: float
    reason: str


def export_product_matches_audit(
    roots: List[Node],
    matches: List[ProductMatch],
    json_path: Path,
    csv_path: Optional[Path] = None,
) -> None:
    """
    Genera un JSON (y opcionalmente CSV) con la decisión de producto por descompuesto.
    Estructura por fila:
      {
        "node_code_old": "...",
        "node_kind": "des_mat|des_mo|des_maq|partida|...",
        "descripcion_corta": "...",
        "descripcion_larga": "...",
        "unidad": "UD|M|M2|...",
        "selected_product_code": "...",
        "confidence": 0.0..1.0,
        "reason": "texto (llm|fallback|...)"
      }
    """
    nodes = _index_nodes_by_code(roots)
    rows: List[AuditRow] = []

    for m in matches:
        n = nodes.get(m.node_code_old)
        if not n:  # si ya se renombró en memoria, puede no estar: lo ignoramos
            continue
        rows.append(
            AuditRow(
                node_code_old=m.node_code_old,
                node_kind=n.kind or "",
                descripcion_corta=n.description or "",
                descripcion_larga=n.long_desc or "",
                unidad=n.unidad or "",
                selected_product_code=m.product_code,
                confidence=float(m.confidence),
                reason=m.reason or "",
            )
        )

    # JSON bonito
    json_path.parent.mkdir(parents=True, exist_ok=True)
    with json_path.open("w", encoding="utf-8") as f:
        json.dump([asdict(r) for r in rows], f, ensure_ascii=False, indent=2)

    # CSV opcional
    if csv_path:
        df = pd.DataFrame([asdict(r) for r in rows])
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(csv_path, index=False, sep=";", encoding="utf-8")
