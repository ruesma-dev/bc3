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


def _fmt_price_str(value: float | None) -> str:
    """Devuelve un str sin notación científica y con punto decimal si aplica."""
    if value is None:
        return ""
    s = f"{value:.15g}"
    return s.replace(",", ".")


# --------------------------------------------------------------------------- #
#         PASADA EXTRA – des huérfano  y  partida sin hijos                   #
# --------------------------------------------------------------------------- #
def _add_missing_clones(nodes: Dict[str, "Node"]) -> None:
    """
    Reglas:
      A) Partida sin hijos → crear clon .1 SIEMPRE,
         y el precio del clon = precio de la partida (si existe).
      B) Descompuestos huérfanos junto a partidas → elevar a partida y clonar .1.
    """
    parent_of = {ch.code: p.code for p in nodes.values() for ch in p.children}
    roots = [n for n in nodes.values() if n.code not in parent_of]

    def dfs(n: Node):
        for ch in n.children:
            dfs(ch)

        # --- regla A: partida sin hijos -> clonar siempre -------------------
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
        clone_price = parent.precio if (parent.precio is not None) else 1.0
        clone = Node(
            code=clone_code,
            description=parent.description,
            long_desc=parent.long_desc,
            kind="des_mat",
            unidad=parent.unidad,
            precio=clone_price,      # precio del clon = precio de la partida
            can_pres=1.0,
            imp_pres=None,
        )
        clone.compute_total()
        parent.add_child(clone)

    for r in roots:
        dfs(r)


# --------------------------------------------------------------------------- #
#           Re-escribir modificaciones en la copia BC3                       #
# --------------------------------------------------------------------------- #
def _rewrite_bc3(path: Path, nodes: Dict[str, Node]) -> None:
    """
    Inserta en el BC3 los clones '.1' de partidas:
      - Mantiene 'tipo' de la partida en 0
      - Crea un ~C del clon con:
          * precio = precio del padre (si disponible) o precio del Node clon
          * fecha  = fecha del padre (si disponible)
          * tipo   = 3 (material)
      - Crea el ~D padre→clon con can=1, coef=1, etc.
      - Evita duplicar: no inserta el clon si ya existe un ~C con ese código.
    """
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
                # Forzamos a tipo partida (0) y escribimos el ~C original
                parts[5] = "0"
                out.append("~C|" + "|".join(parts) + "|\n")

                # Para cada clon .1 que aún no hayamos escrito:
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
                    clone_parts[4] = parent_date_str.strip() if parent_date_str else "1"

                    clone_parts[5] = "3"

                    out.append("~C|" + "|".join(clone_parts) + "|\n")
                    out.append(f"~D|{node.code}|{clone.code}\\1\\1\\1|\n")
                    done.add(clone.code)
                continue

        out.append(ln)

    path.write_text("".join(out), "latin-1", errors="ignore")


# --------------------------------------------------------------------------- #
#                           PARSER PRINCIPAL                                  #
# --------------------------------------------------------------------------- #
def build_tree(bc3_path: Path) -> List[Node]:
    """
    Construye el árbol lógico a partir de un BC3 ya normalizado.

    Cambios clave respecto a la versión anterior:
      - Soporta que un MISMO código hijo cuelgue de VARIOS padres:
        en vez de parents[child] = parent guardamos todas las relaciones
        parent_children[parent].append(child).
      - Esto evita que partidas como P5.15.07 se queden “sin hijos” cuando
        comparten descompuesto con otras partidas (P5.15.08, etc.).
    """
    nodes: Dict[str, Node] = {}
    parent_children: Dict[str, List[str]] = defaultdict(list)
    qty_map: Dict[str, float] = {}
    meas_map: Dict[str, List[str]] = defaultdict(list)

    line_no = 0  # para debug

    with bc3_path.open("r", encoding="latin-1", errors="ignore") as fh:
        for raw in fh:
            line_no += 1
            tag = raw[:2]

            if tag == "~C":
                _, rest = raw.split("|", 1)
                parts = rest.rstrip("\n").split("|")
                while len(parts) < 6:
                    parts.append("")
                code, unidad, desc, pres, _, t = parts[:6]

                desc_clean = clean_text(desc)
                if (t in {"0", "1", "2", "3"}) and ("#" not in code) and not desc_clean.strip():
                    desc_clean = code

                nodes[code] = Node(
                    code=code,
                    description=desc_clean,
                    kind=_kind(code, t),
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
                            qty_map[child_code] = float(canp.replace(",", "."))

            elif tag == "~M":
                body = raw.split("|", 2)[1]
                if "\\" in body:
                    code = body.split("\\", 1)[1].split("|", 1)[0]
                    meas_map[code].append(raw.rstrip())

    print(
        "[BT DEBUG] build_tree() – leídos:",
        f"nodes={len(nodes)}, parent_children={len(parent_children)},",
        f"qty_map={len(qty_map)}, meas_map={len(meas_map)}",
    )

    # DEBUG específico para el caso P5.14.03.06 DESC. y P5.15.07 / P5.15.08
    suspect_child = "P5.14.03.06 DESC."
    if suspect_child in parent_children:
        print(f"[BT DEBUG] padres de {suspect_child!r}: {parent_children[suspect_child]!r}")
    else:
        print(f"[BT DEBUG] {suspect_child!r} no aparece como hijo en ningún ~D")

    # ---------------------- jerarquía con DEBUG ------------------------------
    missing_links = 0
    for parent, children in parent_children.items():
        for ch in children:
            has_parent = parent in nodes
            has_child = ch in nodes
            if has_parent and has_child:
                nodes[parent].add_child(nodes[ch])
            else:
                missing_links += 1
                if missing_links <= 50:
                    print(
                        "[BT DEBUG] enlace padre-hijo NO creado:",
                        f"parent={parent!r} (en nodes={has_parent})",
                        f"child={ch!r} (en nodes={has_child})",
                    )

    if missing_links:
        print(f"[BT DEBUG] enlaces omitidos por falta de nodo padre/hijo: {missing_links}")

    # ---------------------- datos numéricos ---------------------------------
    for c, n in nodes.items():
        if c in qty_map:
            n.can_pres = qty_map[c]
        if c in meas_map:
            n.measurements = meas_map[c]
        n.compute_total()

    # ---------------------- pasada extra (clones) ---------------------------
    _add_missing_clones(nodes)

    # ---------------------- escribir cambios en BC3 -------------------------
    _rewrite_bc3(bc3_path, nodes)

    # ---------------------- raíces -----------------------------------------
    child_codes = {c.code for n in nodes.values() for c in n.children}
    roots = [n for n in nodes.values() if n.code not in child_codes]
    return sorted(roots, key=lambda n: n.code)
