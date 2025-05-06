# infrastructure/bc3/bc3_modifier.py
from __future__ import annotations
from pathlib import Path
import re
from collections import defaultdict
from utils.text_sanitize import clean_text

MAX_CODE_LEN = 20


def _short(code: str) -> str:
    return code[:MAX_CODE_LEN]


# --------------------------------------------------------------------------- #
#                    PASADA 1 –  code_map  /  tipo_map  /  children_map       #
# --------------------------------------------------------------------------- #
def _collect_info(src: Path):
    code_map: dict[str, str] = {}
    tipo_map: dict[str, str] = {}
    children_map: dict[str, list[str]] = defaultdict(list)

    with src.open("r", encoding="latin-1", errors="ignore") as fh:
        for line in fh:
            if line.startswith("~C|"):
                _, rest = line.split("|", 1)
                code, *_rest, tipo = rest.rstrip("\n").split("|")[:6]

                tipo_map[code] = tipo
                if len(code) > MAX_CODE_LEN:
                    code_map[code] = _short(code)

            elif line.startswith("~D|"):
                _, rest = line.split("|", 1)
                parent_code, child_part = rest.split("|", 1)
                chunks = child_part.rstrip("|\n").split("\\")
                for i in range(0, len(chunks), 3):
                    child_code = chunks[i].strip()
                    if child_code:
                        children_map[parent_code].append(child_code)

    return code_map, tipo_map, children_map


# --------------------------------------------------------------------------- #
#        PASADA 1‑bis – calcular qué códigos se fuerzan a material            #
# --------------------------------------------------------------------------- #
def _compute_force_material(tipo_map: dict[str, str],
                            children_map: dict[str, list[str]]) -> set[str]:
    """
    Devuelve el conjunto de códigos que deben terminar con T=3 (material):
      • descendientes (todos los niveles) de cualquier nodo
        con T=0 y sin '#'
    """
    force_mat: set[str] = set()

    def dfs(code: str):
        for ch in children_map.get(code, []):
            if ch not in force_mat:
                force_mat.add(ch)
                dfs(ch)

    for code, tipo in tipo_map.items():
        if tipo == "0" and "#" not in code:      # partida real
            dfs(code)
    return force_mat


# --------------------------------------------------------------------------- #
#                      PASADA 2 –  reescritura del BC3                        #
# --------------------------------------------------------------------------- #
def convert_to_material(src: Path, dst: Path) -> None:
    if not src.exists():
        raise FileNotFoundError(src)

    code_map, tipo_map, children_map = _collect_info(src)
    force_mat = _compute_force_material(tipo_map, children_map)

    dst.parent.mkdir(parents=True, exist_ok=True)

    # patrón global para sustituir códigos antes de \ o |
    repl_pattern = re.compile(
        r"(" + "|".join(re.escape(k) for k in code_map.keys()) + r")(?=[\\|])"
    ) if code_map else None

    with src.open("r", encoding="latin-1", errors="ignore") as fin, \
         dst.open("w", encoding="latin-1", errors="ignore") as fout:

        for raw in fin:
            line = raw

            # ---------------------------  ~C  --------------------------------
            if raw.startswith("~C|"):
                head, rest = raw.split("|", 1)
                parts = rest.rstrip("\n").split("|")
                if len(parts) >= 6:
                    code, unidad, desc, pres, *_r, tipo = parts[:6]

                    # truncado de código
                    if code in code_map:
                        parts[0] = code_map[code]
                        code = parts[0]

                    # regla de descompuestos originales (T 1/2/3)
                    if tipo in {"1", "2", "3"}:
                        parts[5] = "3"
                        tipo = "3"

                    # NUEVA regla: si está en la rama de una partida ⇒ material
                    if code in force_mat:
                        parts[5] = "3"
                        tipo = "3"

                    # unidad vacía en partida / descompuesto
                    if tipo in {"0", "1", "2", "3"} and not unidad.strip():
                        parts[1] = "UD"

                    # limpieza de descripción
                    parts[2] = clean_text(parts[2])

                    line = f"{head}|{'|'.join(parts)}|\n"

            # ------------------ resto de líneas ( ~D, ~M, … ) -----------------
            else:
                if repl_pattern:
                    line = repl_pattern.sub(
                        lambda m: code_map[m.group(1)], raw.rstrip("\n")
                    ) + "\n"

            # limpieza final de tildes y símbolos
            fout.write(clean_text(line))
