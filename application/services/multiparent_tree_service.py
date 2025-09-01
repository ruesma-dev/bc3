# application/services/multiparent_tree_service.py
from __future__ import annotations
from collections import defaultdict
from pathlib import Path
from typing import Dict, List
import re

from bc3_lib.domain.node import Node
from bc3_lib.infra.reader import iter_registers
from bc3_lib.utils.text_sanitize import clean_text  # misma firma que en la lib

_NUM_RE = re.compile(r"^-?\d+(?:[.,]\d+)?$")


def _kind(code: str, t_flag: str) -> str:
    if "##" in code:
        return "supercapitulo"
    if "#" in code:
        return "capitulo"
    return {"0": "partida", "1": "des_mo", "2": "des_maq", "3": "des_mat"}.get(t_flag, "otro")


def _to_float(txt: str) -> float | None:
    return float(txt.replace(",", ".")) if txt and _NUM_RE.match(txt) else None


def build_tree_multiparent(bc3_path: Path) -> List[Node]:
    """
    Igual que bc3_lib.infra.reader.build_tree, pero permite que el MISMO código hijo
    cuelgue de VARIOS padres (no colapsa a un único padre).
    """
    nodes: Dict[str, Node] = {}
    edges: List[tuple[str, str]] = []         # (parent, child) → ¡todas las apariciones!
    qty_map: Dict[str, float] = {}
    meas_map: Dict[str, List[str]] = defaultdict(list)

    for reg in iter_registers(bc3_path):
        if reg.tag == "~C":
            fields = (reg.fields + [""] * 6)[:6]
            code, unit, desc, price, _unused, tflag = fields
            nodes[code] = Node(
                code=code,
                description=clean_text(desc),
                kind=_kind(code, tflag),
                unidad=unit or None,
                precio=_to_float(price),
            )

        elif reg.tag == "~T":
            code, txt = (reg.fields + [""])[:2]
            if code in nodes and nodes[code].long_desc is None:
                nodes[code].long_desc = clean_text(txt)

        elif reg.tag == "~D":
            parent = reg.fields[0]
            child_part = "|".join(reg.fields[1:])
            chunks = child_part.rstrip("|").split("\\")
            for i in range(0, len(chunks), 3):
                child = chunks[i].strip()
                if not child:
                    continue
                edges.append((parent, child))
                qty_raw = chunks[i + 2] if i + 2 < len(chunks) else ""
                if _NUM_RE.match(qty_raw):
                    qty_map[child] = float(qty_raw.replace(",", "."))

        elif reg.tag == "~M":
            body = "|".join(reg.fields[1:])
            if "\\" in body:
                code = body.split("\\", 1)[1].split("|", 1)[0]
                meas_map[code].append(reg.raw)

    # Construir jerarquía con TODAS las aristas
    for parent, child in edges:
        if parent in nodes and child in nodes:
            nodes[parent].add_child(nodes[child])

    # Completar cantidades/mediciones + totales
    for code, node in nodes.items():
        if code in qty_map:
            node.can_pres = qty_map[code]
        if code in meas_map:
            node.measurements = meas_map[code]
        node.compute_total()

    # Raíces: nodos que NUNCA aparecen como hijo (ojo, con multi-padre pueden quedar igual)
    child_codes = {c.code for n in nodes.values() for c in n.children}
    roots = [n for n in nodes.values() if n.code not in child_codes]
    return sorted(roots, key=lambda n: n.code)
