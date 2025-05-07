# application/services/build_tree_service.py
from __future__ import annotations
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional
from collections import defaultdict
import re
from utils.text_sanitize import clean_text

_NUM = re.compile(r"^-?\d+(?:[.,]\d+)?$")


@dataclass
class Node:
    code: str
    description: str
    long_desc: str | None = None
    kind: str = ""
    unidad: str | None = None
    precio: float | None = None
    can_pres: float | None = None
    imp_pres: float | None = None
    measurements: List[str] = field(default_factory=list)
    children: List["Node"] = field(default_factory=list)

    def add_child(self, child: "Node") -> None:
        self.children.append(child)

    def compute_total(self) -> None:
        if self.imp_pres is None and self.precio is not None and self.can_pres is not None:
            self.imp_pres = self.precio * self.can_pres
        for ch in self.children:
            ch.compute_total()


def _kind(code: str, t: str) -> str:
    if "##" in code:
        return "supercapítulo"
    if "#" in code:
        return "capítulo"
    return {"0": "partida", "1": "des_mo", "2": "des_maq", "3": "des_mat"}.get(t, "otro")


def _num(v: str) -> float | None:
    return float(v.replace(",", ".")) if v and _NUM.match(v) else None


# ------------------ PASADA EXTRA ------------------------------------------- #
def _convert_lonely_des(nodes: Dict[str, Node]) -> None:
    parent_of = {ch.code: p.code for p in nodes.values() for ch in p.children}
    roots = [n for n in nodes.values() if n.code not in parent_of]

    def dfs(n: Node):
        for ch in n.children:
            dfs(ch)

        hermanos = n.children
        if not hermanos or not any(h.kind == "partida" for h in hermanos):
            return

        for des in list(hermanos):
            if des.kind.startswith("des_") and not des.children:
                # ---- convertir des a partida (mantiene sus valores) ----------
                des.kind = "partida"
                if des.can_pres is None:
                    des.can_pres = 1

                # ---- crear clon .1 con todo = 1 ------------------------------
                clone_code = (des.code + ".1")[:20]
                clone = Node(
                    code=clone_code,
                    description=des.description,
                    long_desc=des.long_desc,
                    kind="des_mat",
                    unidad=des.unidad,
                    precio=1.0,
                    can_pres=1.0,
                    imp_pres=1.0,
                )
                clone.compute_total()
                des.add_child(clone)

    for r in roots:
        dfs(r)


# --------------------------------------------------------------------------- #
#                 Re‑escribir modificaciones en la copia BC3                 #
# --------------------------------------------------------------------------- #
def _rewrite_bc3(path: Path, nodes: Dict[str, Node]) -> None:
    """
    Para cada descompuesto convertido en partida:
      • Reemplaza su línea ~C  →  T = 0   (mismo nº de columnas, misma unidad…)
      • Añade el clon .1 con unidad, y con precio = 1, fecha = 1, T = 3
      • Añade la ~D partida → clon  (\1\1)
    """
    lines = path.read_text("latin-1", errors="ignore").splitlines(keepends=True)
    out: list[str] = []
    processed_clones: set[str] = set()

    for ln in lines:
        if ln.startswith("~C|"):
            _, rest = ln.split("|", 1)
            parts = rest.rstrip("\n").split("|")

            # Aseguramos al menos 6 campos (código, unidad, desc, pres, fecha, tipo)
            while len(parts) < 6:
                parts.append("")

            code = parts[0]
            node = nodes.get(code)

            if (
                node
                and node.kind == "partida"
                and node.children
                and node.children[0].code.endswith(".1")
            ):
                # --------- línea del nodo convertido a partida ---------------
                parts[5] = "0"                   # tipo -> partida
                out.append("~C|" + "|".join(parts) + "|\n")

                # --------- línea del clon (.1) --------------------------------
                clone = node.children[0]
                if clone.code not in processed_clones:
                    clone_parts = parts.copy()
                    clone_parts[0] = clone.code       # nuevo código
                    clone_parts[3] = "1"               # precio = 1
                    clone_parts[4] = "1"               # fecha = 1
                    clone_parts[5] = "3"               # tipo material
                    out.append("~C|" + "|".join(clone_parts) + "|\n")
                    # relación D partida -> clon  (coef 1, cantidad 1)
                    out.append(f"~D|{node.code}|{clone.code}\\1\\1\\1\\|\n")
                    processed_clones.add(clone.code)
                continue

        out.append(ln)

    path.write_text("".join(out), "latin-1", errors="ignore")


# ------------------------ PARSER PRINCIPAL --------------------------------- #
def build_tree(bc3_path: Path) -> List[Node]:
    nodes: Dict[str, Node] = {}
    parents: Dict[str, str] = {}
    qty_map: Dict[str, float] = {}
    meas_map: Dict[str, List[str]] = defaultdict(list)

    with bc3_path.open("r", encoding="latin-1", errors="ignore") as fh:
        for raw in fh:
            tag = raw[:2]

            if tag == "~C":
                _, rest = raw.split("|", 1)
                code, unidad, desc, pres, _, t = rest.rstrip("\n").split("|")[:6]
                nodes[code] = Node(
                    code=code,
                    description=clean_text(desc),
                    kind=_kind(code, t),
                    unidad=unidad or None,
                    precio=_num(pres),
                )

            elif tag == "~T":
                _, rest = raw.split("|", 1)
                code, txt = rest.rstrip("\n").split("|", 1)
                if code in nodes and nodes[code].long_desc is None:
                    nodes[code].long_desc = clean_text(txt)

            elif tag == "~D":
                _, rest = raw.split("|", 1)
                parent_code, child_part = rest.split("|", 1)
                chunks = child_part.rstrip("|\n").split("\\")
                for i in range(0, len(chunks), 3):
                    child_code = chunks[i].strip()
                    if child_code:
                        parents[child_code] = parent_code
                        canp = chunks[i + 2] if i + 2 < len(chunks) else ""
                        if _NUM.match(canp):
                            qty_map[child_code] = float(canp.replace(",", "."))

            elif tag == "~M":
                body = raw.split("|", 2)[1]
                if "\\" in body:
                    code = body.split("\\", 1)[1].split("|", 1)[0]
                    meas_map[code].append(raw.rstrip())

    # enlazar jerarquía
    for ch, p in parents.items():
        if ch in nodes and p in nodes:
            nodes[p].add_child(nodes[ch])

    # datos numéricos
    for c, n in nodes.items():
        if c in qty_map:
            n.can_pres = qty_map[c]
        if c in meas_map:
            n.measurements = meas_map[c]
        n.compute_total()

    # pasada extra
    _convert_lonely_des(nodes)

    # actualizar BC3
    _rewrite_bc3(bc3_path, nodes)

    # raíces
    child_codes = {c.code for n in nodes.values() for c in n.children}
    roots = [n for n in nodes.values() if n.code not in child_codes]
    return sorted(roots, key=lambda n: n.code)
