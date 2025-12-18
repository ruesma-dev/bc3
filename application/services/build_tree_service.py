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

    Ojo: aquí ya asumimos que, previamente, se han “limpiado” las partidas
    cuyas descomposiciones tienen rendimiento 0 en todos sus hijos (es decir,
    dichas partidas ya no tienen hijos en nodes[*].children).
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
def _rewrite_bc3(
    path: Path,
    nodes: Dict[str, Node],
    parents_all_zero: set[str] | None = None,
) -> None:
    """
    Inserta en el BC3 los clones '.1' de partidas y limpia descompuestos
    “muertos” (rendimiento 0):

      - Para partidas con clones '.1':
          * Mantiene 'tipo' de la partida en 0
          * Crea un ~C del clon con:
              · precio = precio del padre (si disponible) o precio del Node clon
              · fecha  = fecha del padre (si disponible)
              · tipo   = 3 (material)
          * Crea el ~D padre→clon con can=1, coef=1, etc.
          * Evita duplicar: no inserta el clon si ya existe un ~C con ese código.

      - Para partidas en parents_all_zero:
          * Se eliminan sus ~D originales (descompuestos de rendimiento 0).
          * Si además tienen clon .1, quedará solo el nuevo ~D padre→clon.
    """
    parents_all_zero = parents_all_zero or set()

    # Leemos y, ANTES de tocar nada, indexamos los códigos ~C ya presentes
    lines = path.read_text("latin-1", errors="ignore").splitlines(keepends=True)
    existing_c_codes: set[str] = set()
    for raw in lines:
        if raw.startswith("~C|"):
            try:
                _, rest = raw.split("|", 1)
                code = rest.split("|", 1)[0]
                existing_c_codes.add(code)
            except Exception:
                # línea malformada: la dejamos pasar
                pass

    out: list[str] = []
    done: set[str] = set()

    for ln in lines:
        # 1) Filtrado de ~D para partidas con todos los descompuestos a 0
        if ln.startswith("~D|") and parents_all_zero:
            try:
                _, rest = ln.split("|", 1)
                parent_code = rest.split("|", 1)[0]
            except ValueError:
                parent_code = ""
            if parent_code in parents_all_zero:
                # No escribimos este ~D original: se considerará “sin descompuesto”
                # y, si tiene clon .1, se añadirá un nuevo ~D padre→clon más abajo.
                print(
                    f"[BT DEBUG] _rewrite_bc3(): eliminando ~D de partida "
                    f"con rendimiento 0: {parent_code}"
                )
                continue

        # 2) Lógica de clones '.1' sobre ~C
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
                    # EVITAR DUPLICADOS ENTRE EJECUCIONES:
                    if clone.code in existing_c_codes:
                        # Ya existe un ~C con ese código en el fichero → NO lo reinsertamos
                        done.add(clone.code)
                        continue

                    clone_parts = parts.copy()
                    clone_parts[0] = clone.code

                    # --- precio del clon -------------------------------
                    parent_price_str = parts[3] if len(parts) > 3 else ""
                    if parent_price_str and parent_price_str.strip():
                        clone_price_str = parent_price_str.strip()
                    else:
                        clone_price_str = _fmt_price_str(clone.precio) or "1"
                    clone_parts[3] = clone_price_str

                    # --- fecha del clon = fecha del padre (si existe) ---
                    parent_date_str = parts[4] if len(parts) > 4 else ""
                    clone_parts[4] = parent_date_str.strip() if parent_date_str else "1"

                    # --- tipo del clon = material -----------------------
                    clone_parts[5] = "3"

                    out.append("~C|" + "|".join(clone_parts) + "|\n")
                    out.append(f"~D|{node.code}|{clone.code}\\1\\1\\1|\n")
                    done.add(clone.code)
                continue

        # 3) Resto de líneas sin cambios
        out.append(ln)

    path.write_text("".join(out), "latin-1", errors="ignore")


# --------------------------------------------------------------------------- #
#                           PARSER PRINCIPAL                                  #
# --------------------------------------------------------------------------- #
def build_tree(bc3_path: Path) -> List[Node]:
    nodes: Dict[str, Node] = {}
    # padre -> lista de códigos hijo
    children_map: Dict[str, List[str]] = defaultdict(list)
    # (padre, hijo) -> rendimiento (cantidad en ~D)
    edge_qty_map: Dict[tuple[str, str], float] = {}
    # mapa “global” por código hijo (lo usamos para can_pres del Node)
    qty_map: Dict[str, float] = {}
    meas_map: Dict[str, List[str]] = defaultdict(list)

    line_no = 0  # para debug

    with bc3_path.open("r", encoding="latin-1", errors="ignore") as fh:
        for raw in fh:
            line_no += 1
            tag = raw[:2]

            if tag == "~C":
                _, rest = raw.split("|", 1)
                # Aseguramos al menos 6 campos para evitar IndexError en BC3 raros
                parts = rest.rstrip("\n").split("|")
                while len(parts) < 6:
                    parts.append("")
                code, unidad, desc, pres, _, t = parts[:6]

                # Fallback: si T∈{0,1,2,3}, no es estructural (sin '#') y la
                # descripción corta está vacía, usar el código como descripción.
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
                # Por seguridad, permitimos que no haya '|'
                try:
                    code, txt = rest.rstrip("\n").split("|", 1)
                except ValueError:
                    code, txt = rest.rstrip("\n"), ""
                if code in nodes and nodes[code].long_desc is None:
                    nodes[code].long_desc = clean_text(txt)

            elif tag == "~D":
                # ~D|PADRE|hijo\coef\cant\hijo2\coef2\cant2\...|
                _, rest = raw.split("|", 1)
                parent_code, child_part = rest.split("|", 1)
                parent_code = parent_code.strip()
                chunks = child_part.rstrip("|\n").split("\\")

                for i in range(0, len(chunks), 3):
                    child_code = (chunks[i] if i < len(chunks) else "").strip()
                    if not child_code:
                        continue

                    children_map[parent_code].append(child_code)

                    qty_str = chunks[i + 2] if i + 2 < len(chunks) else ""
                    if _NUM.match(qty_str):
                        q = float(qty_str.replace(",", "."))
                        edge_qty_map[(parent_code, child_code)] = q
                        # Para can_pres, acumulamos (suma de rendimientos donde aparezca)
                        qty_map[child_code] = qty_map.get(child_code, 0.0) + q

            elif tag == "~M":
                body = raw.split("|", 2)[1]
                if "\\" in body:
                    code = body.split("\\", 1)[1].split("|", 1)[0]
                    meas_map[code].append(raw.rstrip())

    # Resumen inicial de lo leído
    print(
        "[BT DEBUG] build_tree() – leídos:",
        f"nodes={len(nodes)}, padres_con_D={len(children_map)},",
        f"qty_map={len(qty_map)}, meas_map={len(meas_map)}",
    )

    # ---------------------- detectar partidas con descompuestos “a 0” ------- #
    parents_all_zero: set[str] = set()
    for parent_code, child_codes in children_map.items():
        parent_node = nodes.get(parent_code)
        if not parent_node or parent_node.kind != "partida":
            continue
        if not child_codes:
            continue

        all_zero = True
        for child_code in child_codes:
            q = edge_qty_map.get((parent_code, child_code))
            # “nulo o cero”: tratamos None, "", etc. como 0
            q_val = float(q) if q is not None else 0.0
            if abs(q_val) > 1e-9:
                all_zero = False
                break

        if all_zero:
            parents_all_zero.add(parent_code)

    if parents_all_zero:
        print(
            "[BT DEBUG] Partidas con TODOS los descompuestos a rendimiento 0:",
            ", ".join(sorted(parents_all_zero)),
        )

    # ---------------------- jerarquía padre-hijo ---------------------------- #
    missing_links = 0
    for parent_code, child_codes in children_map.items():
        parent_node = nodes.get(parent_code)
        if not parent_node:
            continue

        for child_code in child_codes:
            # Si la partida está en parents_all_zero, IGNORAMOS sus hijos:
            # se tratará como “sin descompuesto” y entrará en la lógica de clon .1
            if parent_code in parents_all_zero:
                continue

            child_node = nodes.get(child_code)
            has_child = child_node is not None

            if has_child:
                parent_node.add_child(child_node)
            else:
                missing_links += 1
                if missing_links <= 50:
                    print(
                        "[BT DEBUG] enlace padre-hijo NO creado:",
                        f"parent={parent_code!r} (en nodes=True)",
                        f"child={child_code!r} (en nodes=False)",
                    )

    if missing_links:
        print(f"[BT DEBUG] enlaces omitidos por falta de nodo hijo: {missing_links}")

    # ---------------------- datos numéricos --------------------------------- #
    for c, n in nodes.items():
        if c in qty_map:
            n.can_pres = qty_map[c]
        if c in meas_map:
            n.measurements = meas_map[c]
        n.compute_total()

    # ---------------------- pasada extra (clones) --------------------------- #
    # Importante: en este punto, las partidas de parents_all_zero ya NO tienen
    # hijos en n.children, así que se comportan exactamente como “partidas sin
    # descompuesto original” y se les generará el clon .1.
    _add_missing_clones(nodes)

    # ---------------------- escribir cambios en BC3 ------------------------- #
    _rewrite_bc3(bc3_path, nodes, parents_all_zero)

    # ---------------------- raíces ----------------------------------------- #
    child_codes = {c.code for n in nodes.values() for c in n.children}
    roots = [n for n in nodes.values() if n.code not in child_codes]
    return sorted(roots, key=lambda n: n.code)

