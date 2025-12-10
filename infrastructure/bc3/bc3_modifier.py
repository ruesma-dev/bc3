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

def _shorten_code_unique(code: str,
                         used: dict[str, str]) -> str:
    """
    Devuelve un código BC3 <= MAX_CODE_LEN sin colisiones.
    `used` es un dict short_code -> original_code.
    """
    # Si ya es corto, lo dejamos tal cual
    if len(code) <= MAX_CODE_LEN:
        # Si hay duplicado exacto, asumimos que el BC3 original
        # no tiene códigos repetidos; no tocamos nada.
        return code

    # 1) Intento naive: primeras MAX_CODE_LEN letras
    base = code[:MAX_CODE_LEN]
    if base not in used:
        used[base] = code
        return base

    # 2) Intento con último carácter: 19 primeras + último
    alt = code[:MAX_CODE_LEN - 1] + code[-1]
    if alt not in used:
        used[alt] = code
        return alt

    # 3) Fallback general: sufijos numéricos
    i = 1
    while True:
        suffix = f"#{i}"
        cutoff = MAX_CODE_LEN - len(suffix)
        candidate = code[:cutoff] + suffix
        if candidate not in used:
            used[candidate] = code
            return candidate
        i += 1

def _format_d_triplets(parent_code_out: str, triplets: list[str]) -> str:
    """
    Construye una línea ~D en formato canónico BC3:

        ~D|PADRE|c1\k1\q1\c2\k2\q2\...\|

    Es decir:
      - un separador '\' entre cada campo,
      - y exactamente un '\' antes del '|' final.
    """
    if triplets:
        body = "\\".join(triplets) + "\\"
    else:
        body = ""
    return f"~D|{parent_code_out}|{body}|\n"

def _ensure_d_trailing_backslash(line: str) -> str:
    """
    Si la línea es ~D, garantiza que acabe en '\\|':
      - si no hay barra antes del último '|', la añade
      - si hay varias barras, las reduce a una
    No toca líneas que no sean ~D ni las que no terminen en '|'.
    """
    if not line.startswith("~D|"):
        return line

    raw = line.rstrip("\n")
    if not raw.endswith("|"):
        print("[BC3 DEBUG] ~D sin '|' final, no se toca:", raw)
        return line

    before = raw
    # Quitamos el '|' final, normalizamos las '\' finales y volvemos a añadir '|'
    raw_no_tail = raw[:-1]
    raw_no_tail = raw_no_tail.rstrip("\\") + "\\"
    after = raw_no_tail + "|"

    if before != after:
        print(
            "[BC3 DEBUG] Fix ~D '\\\\' final:\n"
            "   IN :", before, "\n"
            "   OUT:", after
        )

    return after + "\n"




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
      - code_map: códigos largos -> truncados ÚNICOS (≤ MAX_CODE_LEN)
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

    # short_code -> original_code (para evitar colisiones al recortar)
    used_short: dict[str, str] = {}

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

                    # Construimos code_map solo para códigos LARGOS
                    if len(code) > MAX_CODE_LEN:
                        # Evitar recalcular para el mismo código original
                        if code not in code_map:
                            short = _shorten_code_unique(code, used_short)
                            code_map[code] = short
                    else:
                        # Registrar también los códigos cortos tal cual,
                        # para que ningún código largo se recorte a un valor
                        # ya existente en el BC3 original.
                        if code not in used_short:
                            used_short[code] = code

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
    Normaliza el BC3 y **intercala CD# bajo el supercapítulo '##'**.
    (Docstring original, sin cambios funcionales, solo añadidos prints
    y corrección del orden clean_text → _ensure_d_trailing_backslash
    en las ~D).
    """
    if not src.exists():
        raise FileNotFoundError(src)

    print("[BC3 DEBUG] convert_to_material() SRC=", src, "DST=", dst)

    code_map, tipo_map, children_map, price_map, meas_pair_map = _collect_info(src)
    force_mat = _compute_force_material(tipo_map, children_map)

    print(
        "[BC3 DEBUG]  tipo_map:", len(tipo_map),
        "children_map:", len(children_map),
        "price_map:", len(price_map),
        "meas_pair_map:", len(meas_pair_map),
        "force_mat:", len(force_mat)
    )

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
                while len(parts) < 6:
                    parts.append("")  # code, unidad, desc, pres, fecha, tipo

                orig_code = parts[0]
                unidad = parts[1]
                desc = parts[2]
                pres = parts[3]
                tipo = parts[5]

                # detectar supercapítulo sobre el código ORIGINAL
                if super_root is None and orig_code.endswith("##"):
                    super_root = orig_code
                    print("[BC3 DEBUG]  Detectado super_root:", super_root)

                # aplicar truncado SOLO para el código de salida
                if orig_code in code_map:
                    code_out = code_map[orig_code]
                else:
                    code_out = orig_code
                parts[0] = code_out
                code = code_out  # alias local para salida (por si lo necesitas)

                # Descompuestos originales (1/2/3) → material
                if tipo in {"1", "2", "3"}:
                    parts[5] = "3"
                    tipo = "3"

                # Rama bajo partidas reales ⇒ material
                if orig_code in force_mat:
                    orig_tipo = tipo_map.get(orig_code, tipo)
                    if orig_tipo == "0":
                        parts[3] = price_map.get(orig_code, pres)
                    parts[5] = "3"
                    tipo = "3"

                # === UNIDAD para PARTIDAS y DESCOMPUESTOS ===
                is_desc = (tipo == "3") or (orig_code in all_children) or (orig_code in force_mat)
                is_partida = (tipo == "0") and ("#" not in orig_code)
                if is_desc or is_partida:
                    parts[1] = _unit_normalized(unidad)

                # limpiar descripción corta
                parts[2] = clean_text(desc)

                line = f"{head}|{'|'.join(parts)}|\n"
                line = clean_text(line)
                fout.write(line)
                continue

            # ---------------------------  ~D  --------------------------------
            if raw.startswith("~D|"):
                print("[BC3 DEBUG] Leyendo ~D original:", raw.rstrip("\n"))

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

                print("[BC3 DEBUG]   parent_code:", parent_code, "triplets:", triplets)

                # ¿Es el ~D del supercapítulo? Intercala CD#
                if (
                    need_cd_parent
                    and (super_root is not None)
                    and (parent_code == super_root)
                    and not super_d_rewritten
                ):
                    # 1) Reescribimos el ~D del supercapítulo para que SOLO cuelgue CD#
                    line_super = f"~D|{super_root}|CD#\\1\\1\\1|\n"
                    print("[BC3 DEBUG]   ~D super_root antes limpieza:", line_super.rstrip("\n"))
                    line_super = clean_text(line_super)
                    # Aseguramos barra final DESPUÉS de limpiar
                    line_super = _ensure_d_trailing_backslash(line_super)
                    print("[BC3 DEBUG]   ~D super_root final:", line_super.rstrip("\n"))
                    fout.write(line_super)

                    # 2) Insertamos ~C|CD# si no existía
                    if not cd_concept_written:
                        fout.write("~C|CD#||COSTE DIRECTO|||0|\n")
                        cd_concept_written = True

                    # 3) Colgamos bajo CD# todos los hijos originales del supercapítulo
                    children_body = "\\".join(triplets)
                    line_cd = f"~D|CD#|{children_body}|\n"
                    print("[BC3 DEBUG]   ~D CD# antes limpieza:", line_cd.rstrip("\n"))
                    line_cd = clean_text(line_cd)
                    line_cd = _ensure_d_trailing_backslash(line_cd)
                    print("[BC3 DEBUG]   ~D CD# final:", line_cd.rstrip("\n"))
                    fout.write(line_cd)

                    super_d_rewritten = True
                    continue  # ya hemos escrito lo necesario para este ~D

                # Reescritura normal de ~D
                rebuilt = "\\".join(triplets) + tail_bslashes
                parent_code_out = code_map.get(parent_code, parent_code)
                line = f"~D|{parent_code_out}|{rebuilt}|\n"
                print("[BC3 DEBUG]   ~D normal antes limpieza:", line.rstrip("\n"))
                line = clean_text(line)
                # OJO: ahora aseguramos barra final DESPUÉS de clean_text
                line = _ensure_d_trailing_backslash(line)
                print("[BC3 DEBUG]   ~D normal final:", line.rstrip("\n"))
                fout.write(line)
                continue

            # ---------------------------  ~T  --------------------------------
            if raw.startswith("~T|"):
                try:
                    _tag, rest = raw.split("|", 1)
                    code, txt = (rest.rstrip("\n").split("|", 1) + [""])[:2]
                    code_out = code_map.get(code, code)
                    txt = _strip_rtf_artifacts(txt)
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

            fout.write(clean_text(line))

        # Fallback: si detectamos super_root pero no vimos su ~D (caso raro),
        if need_cd_parent and (super_root is not None) and not super_d_rewritten:
            orig_children = children_map.get(super_root, [])
            triplets = []
            for c in orig_children:
                c_out = code_map.get(c, c)
                triplets.extend([c_out, "1", "1"])

            line_super = f"~D|{super_root}|CD#\\1\\1\\1|\n"
            print("[BC3 DEBUG]   [fallback] ~D super_root antes limpieza:", line_super.rstrip("\n"))
            line_super = clean_text(line_super)
            line_super = _ensure_d_trailing_backslash(line_super)
            print("[BC3 DEBUG]   [fallback] ~D super_root final:", line_super.rstrip("\n"))
            fout.write(line_super)

            if not cd_concept_written:
                fout.write("~C|CD#||COSTE DIRECTO|||0|\n")

            children_body = "\\".join(triplets)
            line_cd = f"~D|CD#|{children_body}|\n"
            print("[BC3 DEBUG]   [fallback] ~D CD# antes limpieza:", line_cd.rstrip("\n"))
            line_cd = clean_text(line_cd)
            line_cd = _ensure_d_trailing_backslash(line_cd)
            print("[BC3 DEBUG]   [fallback] ~D CD# final:", line_cd.rstrip("\n"))
            fout.write(line_cd)
