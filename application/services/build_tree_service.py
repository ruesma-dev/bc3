# application/services/build_tree_service.py
"""
Lee un fichero BC3 y construye el árbol:
tipo, código, descripción, unidad, precio, cantidad, importe,
hijos directos, mediciones.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional
import re

_NUM = re.compile(r"^-?\d+(?:[.,]\d+)?$")


@dataclass
class Node:
    code: str
    description: str
    kind: str
    unidad: Optional[str] = None
    precio: Optional[float] = None
    can_pres: Optional[float] = None
    imp_pres: Optional[float] = None
    measurements: List[str] = field(default_factory=list)
    children: List["Node"] = field(default_factory=list)

    # helpers
    def add_child(self, child: "Node") -> None:
        self.children.append(child)

    def compute_total(self) -> None:
        if self.imp_pres is None and self.precio is not None and self.can_pres is not None:
            self.imp_pres = self.precio * self.can_pres
        for ch in self.children:
            ch.compute_total()


# --------------------------------------------------------------------------- #
#                             PARSEO DEL FICHERO                              #
# --------------------------------------------------------------------------- #
def _kind(code: str, tipo: str) -> str:
    if "##" in code:
        return "Titulo"
    if "#" in code:
        return "capítulo"
    mapa = {"0": "partida", "1": "descompuesto_mo", "2": "descompuesto_maq", "3": "descompuesto_mat"}
    return mapa.get(tipo, "otro")


def _num(txt: str) -> Optional[float]:
    return float(txt.replace(",", ".")) if txt and _NUM.match(txt) else None


def build_tree(path: Path) -> List[Node]:
    nodes, parents, qty, meas = {}, {}, {}, {}

    with path.open(encoding="latin-1", errors="ignore") as fh:
        for raw in fh:
            tag = raw[:2]

            # ---------------- CONCEPTOS --------------------------------------
            if tag == "~C":
                _, rest = raw.split("|", 1)
                parts = rest.rstrip("\n").split("|")
                if len(parts) < 6:
                    continue
                code, unidad, desc, pres, _, tipo = parts[:6]
                print(parts, tipo)
                nodes[code] = Node(
                    code=code,
                    description=desc,
                    kind=_kind(code, tipo),
                    unidad=unidad or None,
                    precio=_num(pres),
                )

            # -------------- RELACIONES (~D) ----------------------------------
            elif tag == "~D":
                _, rest = raw.split("|", 1)
                parent_code, child_part = rest.split("|", 1)
                chunks = child_part.rstrip("|\n").split("\\")
                for i in range(0, len(chunks), 3):
                    child_code = chunks[i].strip()
                    if not child_code:
                        continue
                    canp = chunks[i + 2] if i + 2 < len(chunks) else ""
                    parents[child_code] = parent_code
                    if _NUM.match(canp):
                        qty[child_code] = float(canp.replace(",", "."))

            # -------------- MEDICIONES (~M) ----------------------------------
            elif tag == "~M":
                body = raw.split("|", 2)[1]
                code = body.split("\\", 1)[1].split("|", 1)[0]
                meas.setdefault(code, []).append(raw.rstrip())

    # Enlazar jerarquía y asignar datos
    for child_code, parent_code in parents.items():
        if child_code in nodes and parent_code in nodes:
            nodes[parent_code].add_child(nodes[child_code])

    for code, node in nodes.items():
        if code in qty:
            node.can_pres = qty[code]
        if code in meas:
            node.measurements = meas[code]
        node.compute_total()

    roots = [n for c, n in nodes.items() if c not in parents]
    return sorted(roots, key=lambda n: n.code)
