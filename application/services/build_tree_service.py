# application/services/build_tree_service.py
# ===========================================================
# interface_adapters/controllers/etl_controller.py llama a:
#    roots = build_tree(<ruta bc3>)
# para obtener la jerarquía.
#
# Este módulo:
#  • Lee el BC3 (copia modificada)                     (~C, ~D, ~M, ~T …)
#  • Construye un árbol de Node con:
#       - tipo (supercapítulo, capítulo, partida, des_mo, des_maq, des_mat)
#       - código
#       - descripción corta  (~C)
#       - descripción larga  (~T  1ª línea)
#       - unidad, precio, cantidad, importe
#       - hijos directos, mediciones
# ===========================================================

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

import re
from utils.text_sanitize import clean_text

_NUM = re.compile(r"^-?\d+(?:[.,]\d+)?$")


# --------------------------------------------------------------------------- #
#                                DATACLASS                                    #
# --------------------------------------------------------------------------- #
@dataclass
class Node:
    code: str
    description: str                        # texto corto (Conceptos)
    long_desc: Optional[str] = None         # texto largo (primera línea ~T)
    kind: str = ""                          # supercapítulo | capítulo | …
    unidad: Optional[str] = None
    precio: Optional[float] = None
    can_pres: Optional[float] = None
    imp_pres: Optional[float] = None
    measurements: List[str] = field(default_factory=list)
    children: List["Node"] = field(default_factory=list)

    def add_child(self, child: "Node") -> None:
        self.children.append(child)

    def compute_total(self) -> None:
        if self.imp_pres is None and self.precio is not None and self.can_pres is not None:
            self.imp_pres = self.precio * self.can_pres
        for ch in self.children:
            ch.compute_total()


# --------------------------------------------------------------------------- #
#                        FUNCIONES AUXILIARES                                 #
# --------------------------------------------------------------------------- #
def _kind_from_code_and_t(code: str, tipo: str) -> str:
    if "##" in code:
        return "supercapítulo"
    if "#" in code:
        return "capítulo"
    mapping = {"0": "partida", "1": "des_mo", "2": "des_maq", "3": "des_mat"}
    return mapping.get(tipo, "otro")


def _num(txt: str) -> Optional[float]:
    return float(txt.replace(",", ".")) if txt and _NUM.match(txt) else None


# --------------------------------------------------------------------------- #
#                             PARSER PRINCIPAL                                #
# --------------------------------------------------------------------------- #
def build_tree(bc3_path: Path) -> List[Node]:
    """Devuelve una lista de raíces Node a partir del fichero BC3."""
    nodes: Dict[str, Node] = {}
    parents: Dict[str, str] = {}
    qty_map: Dict[str, float] = {}
    imp_map: Dict[str, float] = {}
    tipo_tmp: Dict[str, str] = {}          # guarda T antes de crear Node
    meas_map: Dict[str, List[str]] = {}

    with bc3_path.open(encoding="latin-1", errors="ignore") as fh:
        for raw in fh:
            tag = raw[:2]

            # ----------------------- CONCEPTOS (~C) --------------------------
            if tag == "~C":
                _, rest = raw.split("|", 1)
                parts = rest.rstrip("\n").split("|")
                if len(parts) < 6:
                    continue
                code, unidad, desc, pres, _, tipo = parts[:6]
                tipo_tmp[code] = tipo
                nodes[code] = Node(
                    code=code,
                    description=clean_text(desc),
                    kind=_kind_from_code_and_t(code, tipo),
                    unidad=unidad or None,
                    precio=_num(pres),
                )

            # ----------------------- TEXTOS (~T) -----------------------------
            elif tag == "~T":
                # ~T|codigo|texto largo ...
                _, rest = raw.split("|", 1)
                code, long_txt = rest.rstrip("\n").split("|", 1)
                if code in nodes and nodes[code].long_desc is None:
                    nodes[code].long_desc = clean_text(long_txt)

            # ----------------------- RELACIONES (~D) -------------------------
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
                        qty_map[child_code] = float(canp.replace(",", "."))

            # ----------------------- MEDICIONES (~M) -------------------------
            elif tag == "~M":
                # ~M|algo\codigo|... resto
                body = raw.split("|", 2)[1]
                if "\\" in body:
                    code = body.split("\\", 1)[1].split("|", 1)[0]
                    meas_map.setdefault(code, []).append(raw.rstrip())

    # -------------- Enlazar jerarquía + asignar cantidades -------------------
    for child, parent in parents.items():
        if child in nodes and parent in nodes:
            nodes[parent].add_child(nodes[child])

    for code, node in nodes.items():
        if code in qty_map:
            node.can_pres = qty_map[code]
        if code in imp_map:
            node.imp_pres = imp_map[code]
        if code in meas_map:
            node.measurements = meas_map[code]
        node.compute_total()     # calcula importe si falta

    # -------------- Raíces = nodos que jamás son hijos -----------------------
    roots = [n for c, n in nodes.items() if c not in parents]
    return sorted(roots, key=lambda n: n.code)
