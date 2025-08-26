# application/services/missing_clones_service.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Set

from bc3_lib.domain.node import Node


def _index_nodes(roots: Iterable[Node]) -> Dict[str, Node]:
    idx: Dict[str, Node] = {}

    def dfs(n: Node) -> None:
        if n.code not in idx:
            idx[n.code] = n
            for ch in n.children:
                dfs(ch)

    for r in roots:
        dfs(r)
    return idx


def _create_clone(parent: Node) -> Node:
    clone_code = (parent.code + ".1")[:20]
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
    parent.children.append(clone)
    return clone


def add_missing_clones(roots: List[Node]) -> Set[str]:
    """
    Regla A: si una partida no tiene hijos -> crea clon '.1' material.
    Regla B: si hay descompuestos huérfanos junto a partidas -> promover a partida y clonar '.1'.
    Devuelve el conjunto de códigos clon creados (terminados en '.1').
    """
    created: Set[str] = set()

    def dfs(n: Node) -> None:
        for ch in n.children:
            dfs(ch)

        # A) partidas sin hijos -> clon
        if n.kind == "partida" and not n.children:
            c = _create_clone(n)
            created.add(c.code)

        # B) hermanos: si hay partida entre hermanos y descompuesto sin hijos -> promover + clon
        bros = n.children
        if bros and any(b.kind == "partida" for b in bros):
            for d in bros:
                if d.kind.startswith("des_") and not d.children:
                    d.kind = "partida"
                    d.can_pres = d.can_pres or 1.0
                    c = _create_clone(d)
                    created.add(c.code)

    for r in roots:
        dfs(r)

    # Recalcular importes
    for r in roots:
        r.compute_total()

    return created


def rewrite_bc3_with_clones(path: Path, roots: List[Node]) -> None:
    """
    Inserta en el BC3:
      • ~C del clon '.1' con T=3, precio=1 y unidad=la del padre
      • ~D que conecta partida_original -> clon '.1' con cantidad=1
    Solo si existen clones creados.
    """
    nodes = _index_nodes(roots)
    has_clones = any(
        n.kind == "partida" and any(c.code.endswith(".1") for c in n.children)
        for n in nodes.values()
    )
    if not has_clones:
        return

    tmp = path.with_suffix(path.suffix + ".tmp")
    done: Set[str] = set()

    with path.open("r", encoding="latin-1", errors="ignore") as fin, \
         tmp.open("w", encoding="latin-1", errors="ignore") as fout:

        for raw in fin:
            line = raw

            if raw.startswith("~C|"):
                _, rest = raw.split("|", 1)
                parts = rest.rstrip("\n").split("|")
                while len(parts) < 6:
                    parts.append("")

                code = parts[0]
                node = nodes.get(code)

                # Si es partida y tiene clon '.1' entre sus hijos
                if node and node.kind == "partida" and any(c.code.endswith(".1") for c in node.children):
                    # Forzamos T=0 de la partida (por claridad de tipo)
                    parts[5] = "0"
                    fout.write("~C|" + "|".join(parts) + "|\n")

                    # Añadimos cada clon una sola vez
                    for clone in node.children:
                        if clone.code.endswith(".1") and clone.code not in done:
                            clone_parts = parts.copy()
                            clone_parts[0] = clone.code
                            clone_parts[3] = "1"  # precio
                            clone_parts[4] = "1"  # fecha/campo libre
                            clone_parts[5] = "3"  # material
                            fout.write("~C|" + "|".join(clone_parts) + "|\n")
                            fout.write(f"~D|{node.code}|{clone.code}\\1\\1\\1|\n")
                            done.add(clone.code)
                    continue

            # Default: escribimos la línea tal cual
            fout.write(line)

    tmp.replace(path)
