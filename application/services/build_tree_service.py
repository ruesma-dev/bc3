# application/services/build_tree_service.py
from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List

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
        if (
            self.imp_pres is None
            and self.precio is not None
            and self.can_pres is not None
        ):
            self.imp_pres = self.precio * self.can_pres
        for child in self.children:
            child.compute_total()


def _kind(code: str, type_code: str) -> str:
    if "##" in code:
        return "supercapítulo"
    if "#" in code:
        return "capítulo"
    return {
        "0": "partida",
        "1": "des_mo",
        "2": "des_maq",
        "3": "des_mat",
    }.get(type_code, "otro")


def _num(value: str) -> float | None:
    return float(value.replace(",", ".")) if value and _NUM.match(value) else None


def _fmt_price_str(value: float | None) -> str:
    if value is None:
        return ""
    return f"{value:.15g}".replace(",", ".")


def _add_missing_clones(nodes: Dict[str, "Node"]) -> None:
    parent_of = {child.code: parent.code for parent in nodes.values() for child in parent.children}
    roots = [node for node in nodes.values() if node.code not in parent_of]

    def dfs(node: Node) -> None:
        for child in node.children:
            dfs(child)

        if node.kind == "partida" and not node.children:
            _create_clone(node)

        siblings = node.children
        if siblings and any(sibling.kind == "partida" for sibling in siblings):
            for des in siblings:
                if des.kind.startswith("des_") and not des.children:
                    des.kind = "partida"
                    des.can_pres = des.can_pres or 1
                    _create_clone(des)

    def _create_clone(parent: Node) -> None:
        clone_code = (parent.code + ".1")[:20]
        if any(child.code == clone_code for child in parent.children):
            return
        clone_price = parent.precio if parent.precio is not None else 1.0
        clone = Node(
            code=clone_code,
            description=parent.description,
            long_desc=parent.long_desc,
            kind="des_mat",
            unidad=parent.unidad,
            precio=clone_price,
            can_pres=1.0,
            imp_pres=None,
        )
        clone.compute_total()
        parent.add_child(clone)

    for root in roots:
        dfs(root)


def _rewrite_bc3(path: Path, nodes: Dict[str, Node]) -> None:
    lines = path.read_text("latin-1", errors="ignore").splitlines(keepends=True)
    existing_c_codes: set[str] = set()
    for raw in lines:
        if raw.startswith("~C|"):
            try:
                _, rest = raw.split("|", 1)
                code = rest.split("|", 1)[0]
                existing_c_codes.add(code)
            except Exception:
                pass

    out: list[str] = []
    done: set[str] = set()

    for line in lines:
        if line.startswith("~C|"):
            _, rest = line.split("|", 1)
            parts = rest.rstrip("\n").split("|")
            while len(parts) < 6:
                parts.append("")

            code = parts[0]
            node = nodes.get(code)

            if node and node.kind == "partida" and any(
                child.code.endswith(".1")
                for child in node.children
            ):
                parts[5] = "0"
                out.append("~C|" + "|".join(parts) + "|\n")

                for clone in node.children:
                    if not clone.code.endswith(".1"):
                        continue
                    if clone.code in done:
                        continue
                    if clone.code in existing_c_codes:
                        done.add(clone.code)
                        continue

                    clone_parts = parts.copy()
                    clone_parts[0] = clone.code

                    parent_price_str = parts[3] if len(parts) > 3 else ""
                    if parent_price_str and parent_price_str.strip():
                        clone_price_str = parent_price_str.strip()
                    else:
                        clone_price_str = _fmt_price_str(clone.precio) or "1"
                    clone_parts[3] = clone_price_str

                    parent_date_str = parts[4] if len(parts) > 4 else ""
                    clone_parts[4] = (
                        parent_date_str.strip() if parent_date_str else "1"
                    )
                    clone_parts[5] = "3"

                    out.append("~C|" + "|".join(clone_parts) + "|\n")
                    out.append(f"~D|{node.code}|{clone.code}\\1\\1\\1|\n")
                    done.add(clone.code)
                continue

        out.append(line)

    path.write_text("".join(out), "latin-1", errors="ignore")


def build_tree(bc3_path: Path) -> List[Node]:
    nodes: Dict[str, Node] = {}
    parent_children: Dict[str, List[str]] = defaultdict(list)
    qty_map: Dict[str, float] = {}
    meas_map: Dict[str, List[str]] = defaultdict(list)

    with bc3_path.open("r", encoding="latin-1", errors="ignore") as fh:
        for raw in fh:
            tag = raw[:2]

            if tag == "~C":
                _, rest = raw.split("|", 1)
                parts = rest.rstrip("\n").split("|")
                while len(parts) < 6:
                    parts.append("")
                code, unidad, desc, pres, _, type_code = parts[:6]

                desc_clean = clean_text(desc)
                if (
                    type_code in {"0", "1", "2", "3"}
                    and "#" not in code
                    and not desc_clean.strip()
                ):
                    desc_clean = code

                nodes[code] = Node(
                    code=code,
                    description=desc_clean,
                    kind=_kind(code, type_code),
                    unidad=unidad or None,
                    precio=_num(pres),
                )

            elif tag == "~T":
                _, rest = raw.split("|", 1)
                try:
                    code, txt = rest.rstrip("\n").split("|", 1)
                except ValueError:
                    code, txt = rest.rstrip("\n"), ""
                if code in nodes and nodes[code].long_desc is None:
                    nodes[code].long_desc = clean_text(txt)

            elif tag == "~D":
                _, rest = raw.split("|", 1)
                parent_code, child_part = rest.split("|", 1)
                chunks = child_part.rstrip("|\n").split("\\")

                for i in range(0, len(chunks), 3):
                    child_code = chunks[i].strip()
                    if child_code:
                        parent_children[parent_code].append(child_code)
                        canp = chunks[i + 2] if i + 2 < len(chunks) else ""
                        if _NUM.match(canp):
                            qty_map[child_code] = float(
                                canp.replace(",", ".")
                            )

            elif tag == "~M":
                body = raw.split("|", 2)[1]
                if "\\" in body:
                    code = body.split("\\", 1)[1].split("|", 1)[0]
                    meas_map[code].append(raw.rstrip())

    for parent, children in parent_children.items():
        for child_code in children:
            if parent in nodes and child_code in nodes:
                nodes[parent].add_child(nodes[child_code])

    for code, node in nodes.items():
        if code in qty_map:
            node.can_pres = qty_map[code]
        if code in meas_map:
            node.measurements = meas_map[code]
        node.compute_total()

    _add_missing_clones(nodes)
    _rewrite_bc3(bc3_path, nodes)

    child_codes = {child.code for node in nodes.values() for child in node.children}
    roots = [node for node in nodes.values() if node.code not in child_codes]
    return sorted(roots, key=lambda node: node.code)
