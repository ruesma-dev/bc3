# application/services/build_tree_service.py
from __future__ import annotations
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List
from collections import defaultdict
import re

from utils.text_sanitize import clean_text

_NUM = re.compile(r"^-?\d+(?:[.,]\d+)?$")


# --------------------------------------------------------------------------- #
#                                 Node                                        #
# --------------------------------------------------------------------------- #
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


# --------------------------------------------------------------------------- #
#                       helpers                                               #
# --------------------------------------------------------------------------- #
def _kind(code: str, t: str) -> str:
    if "##" in code:
        return "supercapítulo"
    if "#" in code:
        return "capítulo"
    return {"0": "partida", "1": "des_mo", "2": "des_maq", "3": "des_mat"}.get(t, "otro")


def _num(v: str) -> float | None:
    return float(v.replace(",", ".")) if v and _NUM.match(v) else None


# --------------------------------------------------------------------------- #
#         PASADA EXTRA – des huérfano  y  partida sin hijos                   #
# --------------------------------------------------------------------------- #
def _add_missing_clones(nodes: Dict[str, "Node"]) -> None:
    parent_of = {ch.code: p.code for p in nodes.values() for ch in p.children}
    roots = [n for n in nodes.values() if n.code not in parent_of]

    def dfs(n: Node):
        for ch in n.children:
            dfs(ch)

        # --- regla A: partidas ya presentes ---------------------------------
        if n.kind == "partida" and not n.children:
            _create_clone(n)

        # --- regla B: descompuestos huérfanos junto a partidas -------------
        bros = n.children
        if bros and any(b.kind == "partida" for b in bros):
            for des in bros:
                if des.kind.startswith("des_") and not des.children:
                    des.kind = "partida"
                    des.can_pres = des.can_pres or 1
                    _create_clone(des)

    def _create_clone(parent: Node):
        clone_code = (parent.code + ".1")[:20]
        if any(c.code == clone_code for c in parent.children):
            return
        clone = Node(
            code=clone_code,
            description=parent.description,
            long_desc=parent.long_desc,
            kind="des_mat",
            unidad=parent.unidad,
            precio=1.0,
            can_pres=1.0,
            imp_pres=1.0,
        )
        clone.compute_total()
        parent.add_child(clone)

    for r in roots:
        dfs(r)


# --------------------------------------------------------------------------- #
#           Re‑escribir modificaciones en la copia BC3                       #
# --------------------------------------------------------------------------- #
def _rewrite_bc3(path: Path, nodes: Dict[str, Node]) -> None:
    lines = path.read_text("latin-1", errors="ignore").splitlines(keepends=True)
    out: list[str] = []
    done: set[str] = set()

    for ln in lines:
        if ln.startswith("~C|"):
            _, rest = ln.split("|", 1)
            parts = rest.rstrip("\n").split("|")
            while len(parts) < 6:
                parts.append("")

            code = parts[0]
            node = nodes.get(code)

            # nodos partida (convertidos o ya partidas) que tienen clon .1
            if node and node.kind == "partida" and any(c.code.endswith(".1") for c in node.children):
                parts[5] = "0"              # tipo partida
                out.append("~C|" + "|".join(parts) + "|\n")

                for clone in node.children:
                    if clone.code not in done and clone.code.endswith(".1"):
                        clone_parts = parts.copy()
                        clone_parts[0] = clone.code
                        clone_parts[3] = "1"   # precio 1
                        clone_parts[4] = "1"   # fecha 1
                        clone_parts[5] = "3"   # material
                        out.append("~C|" + "|".join(clone_parts) + "|\n")
                        out.append(f"~D|{node.code}|{clone.code}\\1\\1\\1\\|\n")
                        done.add(clone.code)
                continue

        out.append(ln)

    path.write_text("".join(out), "latin-1", errors="ignore")


# --------------------------------------------------------------------------- #
#                           PARSER PRINCIPAL                                  #
# --------------------------------------------------------------------------- #
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

    # jerarquía
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

    # pasada extra (descompuestos huérfanos y partidas sin hijos)
    _add_missing_clones(nodes)

    # escribir cambios en BC3
    _rewrite_bc3(bc3_path, nodes)

    # raíces
    child_codes = {c.code for n in nodes.values() for c in n.children}
    roots = [n for n in nodes.values() if n.code not in child_codes]
    return sorted(roots, key=lambda n: n.code)
