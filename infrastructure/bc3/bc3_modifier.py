# infrastructure/bc3/bc3_modifier.py
from __future__ import annotations
from pathlib import Path
import re
from collections import defaultdict
from utils.text_sanitize import clean_text

MAX_CODE_LEN = 20


def _short(code: str) -> str:
    return code[:MAX_CODE_LEN]


def _to_float(s: str) -> float | None:
    s = (s or "").strip()
    if not s:
        return None
    try:
        # BC3 puede traer coma o punto
        return float(s.replace(",", "."))
    except Exception:
        return None


def _fmt_num(value: float) -> str:
    # Evita notación científica y respeta punto decimal
    return f"{value:.15g}".replace(",", ".")


# --------------------------------------------------------------------------- #
# PASADA 1 – code_map / tipo_map / children_map / price_map / meas_pair_map   #
# --------------------------------------------------------------------------- #
def _collect_info(src: Path):
    """
    Devuelve:
      - code_map: códigos largos -> truncados (≤ MAX_CODE_LEN)
      - tipo_map: tipo ORIGINAL por código (0/1/2/3/…)
      - children_map: hijos por padre a partir de ~D
      - price_map: precio ORIGINAL (texto) por código (desde ~C)
      - meas_pair_map: suma de mediciones por (padre, hijo) desde ~M
                       (clave = (parent_code, child_code), valor float)
    """
    code_map: dict[str, str] = {}
    tipo_map: dict[str, str] = {}
    children_map: dict[str, list[str]] = defaultdict(list)
    price_map: dict[str, str] = {}
    meas_pair_map: dict[tuple[str, str], float] = defaultdict(float)

    with src.open("r", encoding="latin-1", errors="ignore") as fh:
        for raw in fh:
            if raw.startswith("~C|"):
                _, rest = raw.split("|", 1)
                parts = rest.rstrip("\n").split("|")
                if len(parts) >= 6:
                    code = parts[0]
                    tipo = parts[5]
                    tipo_map[code] = tipo
                    price_map[code] = parts[3] if len(parts) > 3 else ""
                    if len(code) > MAX_CODE_LEN:
                        code_map[code] = _short(code)

            elif raw.startswith("~D|"):
                _, rest = raw.split("|", 1)
                parent_code, child_part = rest.split("|", 1)
                chunks = child_part.rstrip("|\n").split("\\")
                for i in range(0, len(chunks), 3):
                    child_code = (chunks[i] if i < len(chunks) else "").strip()
                    if child_code:
                        children_map[parent_code].append(child_code)

            elif raw.startswith("~M|"):
                # Formato típico: ~M|<PADRE>\<HIJO>|<meta>|<cantidad>|...
                parts = raw.rstrip("\n").split("|")
                if len(parts) >= 4:
                    pair = parts[1]
                    qty_str = parts[3]
                    if "\\" in pair:
                        parent, child = pair.split("\\", 1)
                        qty = _to_float(qty_str)
                        if qty is not None:
                            meas_pair_map[(parent, child)] += qty

    return code_map, tipo_map, children_map, price_map, meas_pair_map


# --------------------------------------------------------------------------- #
# PASADA 1-bis – calcular qué códigos se fuerzan a material                   #
# --------------------------------------------------------------------------- #
def _compute_force_material(tipo_map: dict[str, str],
                            children_map: dict[str, list[str]]) -> set[str]:
    """
    Devuelve el conjunto de códigos que deben terminar con T=3 (material):
    • todos los descendientes (cualquier nivel) de un nodo con T=0 y sin '#'
    """
    force_mat: set[str] = set()

    def dfs(code: str):
        for ch in children_map.get(code, []):
            if ch not in force_mat:
                force_mat.add(ch)
                dfs(ch)

    for code, tipo in tipo_map.items():
        if tipo == "0" and "#" not in code:  # partida real (no estructural)
            dfs(code)
    return force_mat


# --------------------------------------------------------------------------- #
# PASADA 2 – reescritura del BC3                                              #
# --------------------------------------------------------------------------- #
def convert_to_material(src: Path, dst: Path) -> None:
    if not src.exists():
        raise FileNotFoundError(src)

    code_map, tipo_map, children_map, price_map, meas_pair_map = _collect_info(src)
    force_mat = _compute_force_material(tipo_map, children_map)

    dst.parent.mkdir(parents=True, exist_ok=True)

    # Patrón global para sustituir códigos largos por truncados en líneas genéricas
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
                    # code | unidad | desc | precio | fecha | tipo
                    code = parts[0]
                    unidad = parts[1] if len(parts) > 1 else ""
                    desc = parts[2] if len(parts) > 2 else ""
                    pres = parts[3] if len(parts) > 3 else ""
                    tipo = parts[5]

                    # truncado de código (si venía largo)
                    if code in code_map:
                        parts[0] = code_map[code]
                        code = parts[0]

                    # ¿estructural? (cap./supercap.: contienen '#')
                    is_structural = "#" in code

                    # descompuestos originales (T 1/2/3) -> material
                    if tipo in {"1", "2", "3"}:
                        parts[5] = "3"
                        tipo = "3"

                    # forzar rama bajo partidas reales a material (también si era T=0)
                    if code in force_mat:
                        parts[5] = "3"
                        tipo = "3"

                    # Si el T ORIGINAL era 0 y ahora es 3, reponer SIEMPRE el precio original
                    orig_tipo = tipo_map.get(parts[0], tipo_map.get(code, tipo))
                    if orig_tipo == "0" and tipo == "3":
                        parts[3] = price_map.get(parts[0], price_map.get(code, pres))

                    # unidad vacía en partida / descompuesto (NO aplicar a estructurales)
                    if (tipo in {"0", "1", "2", "3"}) and (not is_structural) and not (unidad or "").strip():
                        parts[1] = "UD"

                    # descripción: si vacía y no estructural, usar el código
                    desc_clean = clean_text(desc)
                    if (tipo in {"0", "1", "2", "3"}) and (not is_structural) and not desc_clean.strip():
                        parts[2] = parts[0]
                    else:
                        parts[2] = desc_clean

                    line = f"{head}|{'|'.join(parts)}|\n"

            # ---------------------------  ~D  --------------------------------
            elif raw.startswith("~D|"):
                # Modificamos cantidades 0 -> medición cuando el hijo era T=0
                # y lo estamos convirtiendo a material (está en force_mat).
                _, rest = raw.split("|", 1)
                parent_code, child_part = rest.split("|", 1)

                # Guardamos parent original y su posible truncado
                parent_code_out = code_map.get(parent_code, parent_code)

                chunks = child_part.rstrip("|\n").split("\\")
                new_chunks: list[str] = []

                for i in range(0, len(chunks), 3):
                    child_code = (chunks[i] if i < len(chunks) else "").strip()
                    coef = chunks[i + 1] if i + 1 < len(chunks) else ""
                    qty = chunks[i + 2] if i + 2 < len(chunks) else ""

                    if not child_code:
                        continue

                    # Si el hijo era partida y lo convertimos a material
                    # y la cantidad viene 0, sustituimos por la medición del par (padre, hijo).
                    if tipo_map.get(child_code) == "0" and (child_code in force_mat):
                        qty_is_zero = (qty.strip() in {"", "0", "0.0", "0.00", "0,0", "0,00"})
                        if qty_is_zero:
                            meas = meas_pair_map.get((parent_code, child_code))
                            if meas is not None and meas > 0:
                                qty = _fmt_num(meas)

                    # Aplicamos truncado de código si procede
                    child_code_out = code_map.get(child_code, child_code)

                    new_chunks.extend([child_code_out, coef, qty])

                # Reconstruimos el ~D (manteniendo barra final antes del '|')
                rebuilt = "\\".join(new_chunks) + "\\"
                line = f"~D|{parent_code_out}|{rebuilt}|\n"

            # ------------------ resto de líneas ( ~M, etc. ) ------------------
            else:
                if repl_pattern:
                    line = repl_pattern.sub(
                        lambda m: code_map[m.group(1)], raw.rstrip("\n")
                    ) + "\n"

            # limpieza final
            fout.write(clean_text(line))
