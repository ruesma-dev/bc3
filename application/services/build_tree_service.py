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
def _kind_from_code(code: str) -> str:
    if code.endswith("#"):
        return "capítulo"
    if " DESC" in code:
        return "descompuesto"
    if "." in code:
        _, tail = code.split(".", 1)
        return "subcapítulo" if tail.lstrip("0") == "" else "partida"
    return "otro"


def _num(txt: str) -> Optional[float]:
    return float(txt.replace(",", ".")) if txt and _NUM.match(txt) else None


def build_tree(path: Path) -> List[Node]:
    nodes: Dict[str, Node] = {}
    parents: Dict[str, str] = {}
    qty: Dict[str, float] = {}
    meas: Dict[str, List[str]] = {}

    with path.open(encoding="latin-1", errors="ignore") as fh:
        for raw in fh:
            tag = raw[:2]

            # ------------------ CONCEPTOS (~C) --------------------------------
            if tag == "~C":
                _, rest = raw.split("|", 1)
                code, unidad, desc, pres, *_ = rest.rstrip("|\n").split("|")
                nodes[code] = Node(
                    code=code,
                    description=desc,
                    kind=_kind_from_code(code),
                    unidad=unidad or None,
                    precio=_num(pres),
                )

            # ------------------ RELACIONES (~D) -------------------------------
            # -------------------- RELACIONES (~D) CORREGIDO -----------------------------
            elif tag == "~D":
                # Ej.: ~D|01.002|01.002 DESC\1\690.6\|
                _, rest = raw.split("|", 1)  # rest = "01.002|01.002 DESC\1\690.6\|"
                parent_code, child_part = rest.split("|", 1)  # "01.002", "01.002 DESC\1\690.6\|"
                child_part = child_part.rstrip("|\n")  # quita el | final
                chunks = child_part.split("\\")  # ['01.002 DESC','1','690.6','']

                for i in range(0, len(chunks), 3):  # grupo = hijo, coef1, CanPres
                    child_code = chunks[i].strip()
                    if not child_code:
                        continue
                    coef1 = chunks[i + 1] if i + 1 < len(chunks) else ""
                    canp = chunks[i + 2] if i + 2 < len(chunks) else ""

                    parents[child_code] = parent_code  # --> lista de hijos directa

                    if _NUM.match(canp):  # Cantidad presupuestada correcta
                        qty[child_code] = float(canp.replace(",", "."))


            # ------------------ MEDICIONES (~M) -------------------------------
            elif tag == "~M":
                # form: ~M|<algo\código>|...  → el código va detrás de la 1.ª '\'
                body = raw.split("|", 2)[1]  # C001\01.001|...
                parts = body.split("\\", 1)
                if len(parts) >= 2:
                    code = parts[1].split("|", 1)[0]
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
