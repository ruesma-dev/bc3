# infrastructure/bc3/bc3_modifier.py
from __future__ import annotations
from pathlib import Path
import re
from collections import defaultdict
from utils.text_sanitize import clean_text

# --- constantes ---
MAX_CODE_LEN = 20
DEFAULT_UNIT = "UD"  # unidad por defecto si falta o es inválida


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


# -------------------------- Unificación de unidades ------------------------- #
# Objetivo: solo 13 unidades canónicas (siempre en MAYÚSCULAS):
# %, CM, H, KG, T, L, M, M2, M3, PA, PLANTA, UD, VIV
_CANON_UNITS = {
    "%", "CM", "H", "KG", "T", "L", "M", "M2", "M3", "PA", "PLANTA", "UD", "VIV"
}

# Sinónimos/variantes -> canónica (ya sin puntos/espacios y en MAYÚSCULAS)
# Nota: abajo, en _unit_unify, se normaliza la clave eliminando '.', ' ', '-', '_', '·', 'º'
_UNIT_SYNONYM_MAP: dict[str, str] = {
    # Porcentaje
    "%": "%", "PORCENTAJE": "%",

    # Centímetro
    "CM": "CM", "CMS": "CM", "CENTIMETRO": "CM", "CENTIMETROS": "CM",
    "CENTIMETER": "CM", "CENTIMETERS": "CM",

    # Hora
    "H": "H", "HR": "H", "HRS": "H", "HS": "H",
    "HORA": "H", "HORAS": "H",

    # Kilogramo
    "KG": "KG", "KGS": "KG", "KILO": "KG", "KILOS": "KG",
    "KILOGRAMO": "KG", "KILOGRAMOS": "KG",

    # Tonelada
    "T": "T", "TN": "T", "TON": "T", "TM": "T",
    "TONELADA": "T", "TONELADAS": "T",

    # Litro
    "L": "L", "LT": "L", "LTS": "L", "LITRO": "L", "LITROS": "L",

    # Metro (longitud)
    "M": "M", "METRO": "M", "METROS": "M",
    "ML": "M", "MLINEAL": "M", "METROLINEAL": "M", "METROSLINEALES": "M",

    # Metro cuadrado
    "M2": "M2", "METROCUADRADO": "M2", "METROSCUADRADOS": "M2",
    "METROSCUADRADO": "M2", "M2CUADRADOS": "M2",

    # Metro cúbico
    "M3": "M3", "METROCUBICO": "M3", "METROSCUBICOS": "M3",
    "METROSCUBICO": "M3", "M3CUBICOS": "M3",

    # Partida alzada
    "PA": "PA", "P.A": "PA", "PAA": "PA",
    "PARTIDAALZADA": "PA", "PARTIDASALZADA": "PA",

    # Planta
    "PLANTA": "PLANTA", "PLANT": "PLANTA", "PLANTAS": "PLANTA",

    # Unidad
    "UD": "UD", "U": "UD", "UN": "UD", "UNID": "UD", "UNIDS": "UD",
    "UNIDAD": "UD", "UNIDADES": "UD", "PIEZA": "UD", "PIEZAS": "UD",
    "PZA": "UD", "PZAS": "UD",

    # Vivienda
    "VIV": "VIV", "VIVIENDA": "VIV", "VIVIENDAS": "VIV",
}

def _unit_unify(u_raw: str) -> str:
    """
    Unifica variantes comunes de unidades a una de las 13 formas canónicas.
    - Limpia con clean_text
    - Normaliza a mayúsculas
    - Elimina puntos, espacios y signos '·', '-', '_' y 'º'
    - Convierte '²'->'2', '³'->'3' y 'M^2'/'M^3' -> 'M2'/'M3'
    - Usa _UNIT_SYNONYM_MAP; si no hay match y hay texto alfanumérico, devuelve el limpio;
      al final _unit_normalized decidirá si cae a DEFAULT_UNIT.
    """
    if not u_raw:
        return ""

    u = clean_text(u_raw).strip()
    if not u:
        return ""

    # Atajos
    if u == "%":
        return "%"

    # Normalización fuerte para clave
    key = (
        u.upper()
         .replace("·", "")
         .replace(".", "")
         .replace(" ", "")
         .replace("-", "")
         .replace("_", "")
         .replace("º", "")
    )
    # Potencias y superíndices
    key = key.replace("^2", "2").replace("^3", "3")
    key = key.replace("²", "2").replace("³", "3")

    # Normalizaciones específicas M.2 / M^2 / M² -> M2  (y análogo para M3)
    if key in {"M2", "M02"}:
        key = "M2"
    if key in {"M3", "M03"}:
        key = "M3"

    # Sinónimos → canónica
    canon = _UNIT_SYNONYM_MAP.get(key)
    if canon:
        return canon

    # Si no mapeamos pero parece una de las formas M2/M3 por patrón, fállelas a canónica
    if re.fullmatch(r"M2", key):
        return "M2"
    if re.fullmatch(r"M3", key):
        return "M3"
    if re.fullmatch(r"M", key):
        return "M"

    # Si no hay mapeo, devolver el texto limpio en MAYÚSCULAS (lo validamos luego)
    return u.upper()


def _unit_normalized(unidad_raw: str) -> str:
    """
    Limpia, **unifica** y valida la unidad.
    - Siempre devuelve MAYÚSCULAS.
    - Si tras unificar no pertenece al conjunto canónico pero tiene alfanumérico,
      se usa DEFAULT_UNIT ('UD').
    - '%' se respeta.
    """
    u = _unit_unify(unidad_raw or "")
    if not u:
        return DEFAULT_UNIT
    if u == "%":
        return "%"""
    u = u.upper()
    if any(ch.isalnum() for ch in u):
        if u in _CANON_UNITS:
            return u
        # cae a la unidad por defecto si no está en el universo canónico
        return DEFAULT_UNIT
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
    for _ in range(5):
        new = _RTF_GARBAGE_RE.sub(" ", out)
        if new == out:
            break
        out = new
    out = re.sub(r"[.]{3,}", "..", out)
    out = re.sub(r"\s{2,}", " ", out).strip()
    return out


# --------------------------------------------------------------------------- #
# PASADA 2 – reescritura del BC3 (interposición de CD# bajo supercapítulo)    #
# --------------------------------------------------------------------------- #
def convert_to_material(src: Path, dst: Path) -> None:
    """
    Normaliza el BC3 y **intercala CD# bajo el supercapítulo '##'**:
      - Detecta el primer ~C cuyo código termina en '##' (p.ej. '0##') y,
        cuando procesa su ~D, lo reescribe a 'CD#' y traslada sus hijos
        originales a un nuevo '~D|CD#|...|'. Inserta '~C|CD#|' si no existía.
      - Trunca códigos largos (≤20).
      - Fuerza T=3 en descompuestos originales (1/2/3) y en sub-rama bajo T=0.
        Si una partida T=0 pasa a T=3, conserva su precio original.
      - **Asegura y UNIFICA la unidad** en PARTIDAS (T=0 sin '#') y DESCOMPUESTOS.
      - En ~D, si qty del hijo (ex T=0 → material) es 0, usa medición ~M.
      - Limpia descripciones manteniendo separadores ~ | \.
      - Preserva el nº exacto de '\' justo antes del '|' en cada ~D (en las reescrituras normales).
      - En ~T, limpia artefactos RTF (sin reemplazar '|').
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

    # Intercalación de CD# bajo el supercapítulo
    need_cd_parent = ("CD#" not in tipo_map)
    super_root: str | None = None           # p.ej. '0##'
    super_d_rewritten = False               # si ya reescribimos su ~D
    cd_concept_written = False              # si ya insertamos ~C|CD#|

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

                # Si aún no hemos detectado super_root, el primero que acabe en '##'
                if super_root is None and code.endswith("##"):
                    super_root = code

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

                # === UNIDAD para PARTIDAS y DESCOMPUESTOS ===
                is_desc = (tipo == "3") or (code in all_children) or (code in force_mat)
                is_partida = (tipo == "0") and ("#" not in code)
                if is_desc or is_partida:
                    parts[1] = _unit_normalized(unidad)

                # Limpiamos SOLO la descripción
                parts[2] = clean_text(desc)

                line = f"{head}|{'|'.join(parts)}|\n"
                fout.write(clean_text(line))
                continue

            # ---------------------------  ~D  --------------------------------
            if raw.startswith("~D|"):
                # Analizamos el ~D original para posible intercalación CD#
                _tag, rest = raw.split("|", 1)
                parent_code, child_part = rest.split("|", 1)

                body_no_nl = child_part.rstrip("\n")
                # nº exacto de '\' antes de '|' para reescrituras normales
                tail_bs_match = re.search(r"(\\+)\|\s*$", body_no_nl)
                tail_bslashes = tail_bs_match.group(1) if tail_bs_match else "\\"
                if body_no_nl.endswith("|"):
                    body_no_nl = body_no_nl[:-1]

                chunks = body_no_nl.split("\\")
                triplets: list[str] = []
                i = 0
                while i < len(chunks):
                    child_code = (chunks[i] if i < len(chunks) else "").strip()
                    coef = chunks[i + 1] if i + 1 < len(chunks) else ""
                    qty  = chunks[i + 2] if i + 2 < len(chunks) else ""
                    i += 3

                    if not child_code:
                        continue

                    # Si el hijo era T=0 y lo convertimos a material, y qty==0 → medición
                    if tipo_map.get(child_code) == "0" and (child_code in force_mat):
                        qty_is_zero = (qty.strip() in {"", "0", "0.0", "0.00", "0,0", "0,00"})
                        if qty_is_zero:
                            meas = meas_pair_map.get((parent_code, child_code))
                            if (meas is not None) and (meas > 0):
                                qty = _fmt_num(meas)

                    # Truncado de código de hijo si procede
                    child_code_out = code_map.get(child_code, child_code)
                    triplets.extend([child_code_out, coef, qty])

                # ¿Es el ~D del supercapítulo? Intercala CD#
                if need_cd_parent and (super_root is not None) and (parent_code == super_root) and not super_d_rewritten:
                    # 1) Reescribimos el ~D del supercapítulo para que SOLO cuelgue CD#
                    #    (sin barras invertidas extra al final)
                    fout.write(f"~D|{super_root}|CD#\\1\\1\\1|\n")

                    # 2) Insertamos ~C|CD# si no existía
                    if not cd_concept_written:
                        fout.write("~C|CD#||COSTE DIRECTO|||0|\n")
                        cd_concept_written = True

                    # 3) Colgamos bajo CD# todos los hijos originales del supercapítulo
                    rebuilt_children = "\\".join(triplets)  # NO añadimos tail_bslashes: salida limpia
                    fout.write(f"~D|CD#|{rebuilt_children}|\n")

                    super_d_rewritten = True
                    continue  # ya hemos escrito lo necesario para este ~D

                # Reescritura normal de ~D
                rebuilt = "\\".join(triplets) + tail_bslashes
                parent_code_out = code_map.get(parent_code, parent_code)
                line = f"~D|{parent_code_out}|{rebuilt}|\n"
                fout.write(clean_text(line))
                continue

            # ---------------------------  ~T  --------------------------------
            if raw.startswith("~T|"):
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
                fout.write(clean_text(line))
                continue

            # ------------------ resto de líneas ( ~M, etc. ) ------------------
            if repl_pattern:
                line = repl_pattern.sub(
                    lambda m: code_map[m.group(1)], raw.rstrip("\n")
                ) + "\n"

            # Escritura con limpieza (respeta ~ | \)
            fout.write(clean_text(line))

        # Fallback: si detectamos super_root pero no vimos su ~D (caso raro),
        # insertamos la estructura CD# al final usando children_map de la pasada 1.
        if need_cd_parent and (super_root is not None) and not super_d_rewritten:
            orig_children = children_map.get(super_root, [])
            triplets = []
            for c in orig_children:
                c_out = code_map.get(c, c)
                # sin conocer coef/qty originales, colgamos 1\1 (estándar)
                triplets.extend([c_out, "1", "1"])
            fout.write(f"~D|{super_root}|CD#\\1\\1\\1|\n")
            if not cd_concept_written:
                fout.write("~C|CD#||COSTE DIRECTO|||0|\n")
            fout.write(f"~D|CD#|{'\\'.join(triplets)}|\n")
