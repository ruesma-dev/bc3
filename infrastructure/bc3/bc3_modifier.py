# infrastructure/bc3/bc3_modifier.py
from __future__ import annotations
from pathlib import Path
import re
from collections import defaultdict
from utils.text_sanitize import clean_text

# --- constantes ---
MAX_CODE_LEN = 20
DEFAULT_UNIT = "ud"  # unidad por defecto si falta o es inválida


def _short(code: str) -> str:
    return code[:MAX_CODE_LEN]


def _to_float(s: str) -> float | None:
    s = (s or "").strip()
    if not s:
        return None
    try:
        return float(s.replace(",", "."))
    except Exception:
        return None


def _fmt_num(value: float | None) -> str:
    if value is None:
        return ""
    # Evita notación científica y usa punto
    return f"{value:.15g}".replace(",", ".")


def _unit_normalized(unidad_raw: str) -> str:
    """
    Limpia y valida la unidad. Si queda vacía o es “rara”, devuelve DEFAULT_UNIT.
    Regla de validez:
      - tras limpiar con clean_text, si hay al menos un alfanumérico -> OK
      - o si la unidad es exactamente '%' -> OK
      - en otro caso -> DEFAULT_UNIT
    """
    u = clean_text(unidad_raw or "").strip()
    if not u:
        return DEFAULT_UNIT
    if any(ch.isalnum() for ch in u):
        return u
    if u == "%":  # p.ej. descuentos que usan porcentaje como unidad
        return u
    return DEFAULT_UNIT


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
                # ~M|<PADRE>\<HIJO>|<meta>|<cantidad>|...
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


# ---------------------- limpieza de basura RTF en ~T ------------------------ #
_RTF_GARBAGE_PATTERNS = [
    r"^\s*w\d{2,}_[^\s()|]*",          # p.ej. w12240_paperh15840_...
    r"\(?_rtf_ansi[^\)|]*\)?",         # (_rtf_ansi... )
    r"\(?_fonttbl[^\)|]*\)?",          # (_fonttbl... )
    r"\(?_colortbl[^\)|]*\)?",         # (_colortbl... )
    r"\(?_sectd[^\)|]*\)?",            # (_sectd... )
    r"\(?_header_[^\)|]*\)?",          # (_header_... )
    r"\(?_footer_[^\)|]*\)?",          # (_footer_... )
    r"\(?_plain_pard[^\)|]*\)?",       # (_plain_pard... )
]
_RTF_GARBAGE_RE = re.compile("|".join(_RTF_GARBAGE_PATTERNS), flags=re.IGNORECASE)


def _strip_rtf_artifacts(txt: str) -> str:
    """
    Elimina artefactos RTF habituales que a veces aparecen pegados en ~T.
    No toca separadores del formato; eso lo controla el flujo de escritura.
    """
    out = txt
    # elimina repetidamente mientras siga encontrando basura
    for _ in range(5):
        new = _RTF_GARBAGE_RE.sub(" ", out)
        if new == out:
            break
        out = new
    # colapsa espacios/puntos repetidos y limpia bordes
    out = re.sub(r"[.]{3,}", "..", out)
    out = re.sub(r"\s{2,}", " ", out).strip()
    return out


# --------------------------------------------------------------------------- #
# PASADA 2 – reescritura del BC3                                              #
# --------------------------------------------------------------------------- #
def convert_to_material(src: Path, dst: Path) -> None:
    """
    Normaliza el BC3:
      - Trunca códigos largos (≤20).
      - Fuerza T=3 en descompuestos originales (1/2/3) y en toda la sub-rama
        bajo partidas reales (T=0 sin '#').
      - **Asegura unidad válida en TODOS los descompuestos (originales y convertidos)**:
        si tras limpiar no es válida → 'ud'.
      - Si una partida T=0 pasa a T=3, conserva su precio original.
      - En ~D, si la cantidad del hijo (que era T=0 y pasa a material) viene 0,
        la sustituye por la medición ~M del par (padre, hijo), si existe.
      - Limpia descripciones (tildes, no imprimibles) manteniendo ~ | \.
      - Preserva el nº exacto de barras '\' justo antes del '|' en cada ~D.
      - En ~T, elimina artefactos RTF comunes y sanea texto (sin reemplazar '|' por '.').
    """
    if not src.exists():
        raise FileNotFoundError(src)

    code_map, tipo_map, children_map, price_map, meas_pair_map = _collect_info(src)
    force_mat = _compute_force_material(tipo_map, children_map)

    # Conjunto de todos los códigos que aparecen como hijo en ~D
    all_children: set[str] = {ch for lst in children_map.values() for ch in lst}

    dst.parent.mkdir(parents=True, exist_ok=True)

    # Sustitución genérica de códigos largos (no ~C, ~D, ~T)
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
                # Aseguramos al menos 6 campos: code, unidad, desc, pres, fecha, tipo
                while len(parts) < 6:
                    parts.append("")

                code = parts[0]
                unidad = parts[1]
                desc = parts[2]
                pres = parts[3]
                tipo = parts[5]

                # Truncado de código si venía largo
                if code in code_map:
                    parts[0] = code_map[code]
                    code = parts[0]

                # Descompuestos originales (1/2/3) → material
                if tipo in {"1", "2", "3"}:
                    parts[5] = "3"
                    tipo = "3"

                # Rama bajo partidas reales ⇒ material (y conservar precio si venía de T=0)
                if code in force_mat:
                    orig_tipo = tipo_map.get(parts[0], tipo_map.get(code, tipo))
                    if orig_tipo == "0":
                        parts[3] = price_map.get(parts[0], price_map.get(code, pres))
                    parts[5] = "3"
                    tipo = "3"

                # === NORMALIZACIÓN DE UNIDAD PARA TODOS LOS DESCOMPUESTOS ===
                # Consideramos descompuesto si:
                #   - tipo efectivo es 3, o
                #   - aparece como hijo en algún ~D, o
                #   - está en la rama forzada a material
                is_desc = (tipo == "3") or (code in all_children) or (code in force_mat)
                if is_desc:
                    parts[1] = _unit_normalized(unidad)

                # Limpiamos SOLO la descripción (no tocamos precio/fecha/tipo aquí)
                parts[2] = clean_text(desc)

                line = f"{head}|{'|'.join(parts)}|\n"

            # ---------------------------  ~D  --------------------------------
            elif raw.startswith("~D|"):
                _, rest = raw.split("|", 1)
                parent_code, child_part = rest.split("|", 1)

                # Preservar exactamente las barras finales antes de '|'
                body_no_nl = child_part.rstrip("\n")
                tail_bs_match = re.search(r"(\\+)\|\s*$", body_no_nl)
                tail_bslashes = tail_bs_match.group(1) if tail_bs_match else "\\"

                # Quitar solo el '|' final (no las barras)
                if body_no_nl.endswith("|"):
                    body_no_nl = body_no_nl[:-1]

                chunks = body_no_nl.split("\\")
                new_chunks: list[str] = []

                i = 0
                while i < len(chunks):
                    child_code = (chunks[i] if i < len(chunks) else "").strip()
                    coef = chunks[i + 1] if i + 1 < len(chunks) else ""
                    qty = chunks[i + 2] if i + 2 < len(chunks) else ""
                    i += 3

                    if not child_code:
                        continue

                    # Si el hijo era T=0 y lo convertimos a material, y qty==0 → usar medición
                    if tipo_map.get(child_code) == "0" and (child_code in force_mat):
                        qty_is_zero = (qty.strip() in {"", "0", "0.0", "0.00", "0,0", "0,00"})
                        if qty_is_zero:
                            meas = meas_pair_map.get((parent_code, child_code))
                            if (meas is not None) and (meas > 0):
                                qty = _fmt_num(meas)

                    # Aplicar truncado a hijo si procede
                    child_code_out = code_map.get(child_code, child_code)
                    new_chunks.extend([child_code_out, coef, qty])

                rebuilt = "\\".join(new_chunks) + tail_bslashes
                parent_code_out = code_map.get(parent_code, parent_code)
                line = f"~D|{parent_code_out}|{rebuilt}|\n"

            # ---------------------------  ~T  --------------------------------
            elif raw.startswith("~T|"):
                # ~T|<code>|<texto_largo>|
                try:
                    _tag, rest = raw.split("|", 1)
                    code, txt = (rest.rstrip("\n").split("|", 1) + [""])[:2]
                    # aplicar truncado de código si estaba largo
                    code_out = code_map.get(code, code)
                    # 1) limpieza “RTF” específica
                    txt = _strip_rtf_artifacts(txt)
                    # 2) limpieza general (NO sustituimos '|' por '.')
                    txt_out = clean_text(txt)
                    line = f"~T|{code_out}|{txt_out}|\n"
                except Exception:
                    line = clean_text(raw.rstrip("\n")) + "\n"

            # ------------------ resto de líneas ( ~M, etc. ) ------------------
            else:
                if repl_pattern:
                    line = repl_pattern.sub(
                        lambda m: code_map[m.group(1)], raw.rstrip("\n")
                    ) + "\n"

            # Escritura con limpieza (respeta ~ | \)
            fout.write(clean_text(line))
