# infrastructure/bc3/bc3_modifier.py
from __future__ import annotations

import re
from collections import defaultdict
from pathlib import Path

from utils.text_sanitize import clean_text

MAX_CODE_LEN = 20
DEFAULT_UNIT = "UD"


def _short(code: str) -> str:
    return code[:MAX_CODE_LEN]


def _to_float(value: str) -> float | None:
    value = (value or "").strip()
    if not value:
        return None
    try:
        return float(value.replace(",", "."))
    except Exception:
        return None


def _fmt_num(value: float | None) -> str:
    if value is None:
        return ""
    return f"{value:.15g}".replace(",", ".")


def _shorten_code_unique(code: str, used: dict[str, str]) -> str:
    if len(code) <= MAX_CODE_LEN:
        return code

    base = code[:MAX_CODE_LEN]
    if base not in used:
        used[base] = code
        return base

    alt = code[: MAX_CODE_LEN - 1] + code[-1]
    if alt not in used:
        used[alt] = code
        return alt

    i = 1
    while True:
        suffix = f"#{i}"
        cutoff = MAX_CODE_LEN - len(suffix)
        candidate = code[:cutoff] + suffix
        if candidate not in used:
            used[candidate] = code
            return candidate
        i += 1


def _ensure_d_trailing_backslash(line: str) -> str:
    if not line.startswith("~D|"):
        return line

    raw = line.rstrip("\n")
    if not raw.endswith("|"):
        return line

    raw_no_tail = raw[:-1]
    raw_no_tail = raw_no_tail.rstrip("\\") + "\\"
    after = raw_no_tail + "|"
    return after + "\n"


_CANON_UNITS = {
    "%",
    "CM",
    "H",
    "KG",
    "T",
    "L",
    "M",
    "M2",
    "M3",
    "PA",
    "PLANTA",
    "UD",
    "VIV",
}

_UNIT_SYNONYM_MAP: dict[str, str] = {
    "%": "%",
    "PORCENTAJE": "%",
    "CM": "CM",
    "CMS": "CM",
    "CENTIMETRO": "CM",
    "CENTIMETROS": "CM",
    "H": "H",
    "HR": "H",
    "HRS": "H",
    "HS": "H",
    "HORA": "H",
    "HORAS": "H",
    "KG": "KG",
    "KGS": "KG",
    "KILO": "KG",
    "KILOS": "KG",
    "KILOGRAMO": "KG",
    "KILOGRAMOS": "KG",
    "T": "T",
    "TN": "T",
    "TON": "T",
    "TM": "T",
    "TONELADA": "T",
    "TONELADAS": "T",
    "L": "L",
    "LT": "L",
    "LTS": "L",
    "LITRO": "L",
    "LITROS": "L",
    "M": "M",
    "METRO": "M",
    "METROS": "M",
    "ML": "M",
    "MLINEAL": "M",
    "METROLINEAL": "M",
    "METROSLINEALES": "M",
    "M2": "M2",
    "METROCUADRADO": "M2",
    "METROSCUADRADOS": "M2",
    "METROSCUADRADO": "M2",
    "M3": "M3",
    "METROCUBICO": "M3",
    "METROSCUBICOS": "M3",
    "METROSCUBICO": "M3",
    "PA": "PA",
    "PARTIDAALZADA": "PA",
    "PARTIDASALZADA": "PA",
    "PLANTA": "PLANTA",
    "PLANT": "PLANTA",
    "PLANTAS": "PLANTA",
    "UD": "UD",
    "U": "UD",
    "UN": "UD",
    "UNID": "UD",
    "UNIDS": "UD",
    "UNIDAD": "UD",
    "UNIDADES": "UD",
    "PIEZA": "UD",
    "PIEZAS": "UD",
    "PZA": "UD",
    "PZAS": "UD",
    "VIV": "VIV",
    "VIVIENDA": "VIV",
    "VIVIENDAS": "VIV",
}


def _unit_unify(unit_raw: str) -> str:
    if not unit_raw:
        return ""

    unit = clean_text(unit_raw).strip()
    if not unit:
        return ""

    if unit == "%":
        return "%"

    key = (
        unit.upper()
        .replace("·", "")
        .replace(".", "")
        .replace(" ", "")
        .replace("-", "")
        .replace("_", "")
        .replace("º", "")
    )
    key = key.replace("^2", "2").replace("^3", "3")
    key = key.replace("²", "2").replace("³", "3")

    if key in {"M2", "M02"}:
        key = "M2"
    if key in {"M3", "M03"}:
        key = "M3"

    canon = _UNIT_SYNONYM_MAP.get(key)
    if canon:
        return canon

    if re.fullmatch(r"M2", key):
        return "M2"
    if re.fullmatch(r"M3", key):
        return "M3"
    if re.fullmatch(r"M", key):
        return "M"

    return unit.upper()


def _unit_normalized(unit_raw: str) -> str:
    unit = _unit_unify(unit_raw or "")
    if not unit:
        return DEFAULT_UNIT
    if unit == "%":
        return "%"
    unit = unit.upper()
    if any(ch.isalnum() for ch in unit):
        if unit in _CANON_UNITS:
            return unit
        return DEFAULT_UNIT
    return DEFAULT_UNIT


def _collect_info(src: Path):
    code_map: dict[str, str] = {}
    tipo_map: dict[str, str] = {}
    children_map: dict[str, list[str]] = defaultdict(list)
    price_map: dict[str, str] = {}
    meas_pair_map: dict[tuple[str, str], float] = defaultdict(float)

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

                    if len(code) > MAX_CODE_LEN:
                        if code not in code_map:
                            short = _shorten_code_unique(code, used_short)
                            code_map[code] = short
                    else:
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


def _compute_force_material(
    tipo_map: dict[str, str],
    children_map: dict[str, list[str]],
) -> set[str]:
    force_mat: set[str] = set()

    def dfs(code: str) -> None:
        for child in children_map.get(code, []):
            if child not in force_mat:
                force_mat.add(child)
                dfs(child)

    for code, tipo in tipo_map.items():
        if tipo == "0" and "#" not in code:
            dfs(code)
    return force_mat


_RTF_GARBAGE_PATTERNS = [
    r"^\s*w\d{2,}_[^\s()|]*",
    r"\(?_rtf_ansi[^\)|]*\)?",
    r"\(?_fonttbl[^\)|]*\)?",
    r"\(?_colortbl[^\)|]*\)?",
    r"\(?_sectd[^\)|]*\)?",
    r"\(?_header_[^\)|]*\)?",
    r"\(?_footer_[^\)|]*\)?",
    r"\(?_plain_pard[^\)|]*\)?",
]
_RTF_GARBAGE_RE = re.compile("|".join(_RTF_GARBAGE_PATTERNS), flags=re.IGNORECASE)


def _strip_rtf_artifacts(txt: str) -> str:
    out = txt
    for _ in range(5):
        new = _RTF_GARBAGE_RE.sub(" ", out)
        if new == out:
            break
        out = new
    out = re.sub(r"[.]{3,}", "..", out)
    out = re.sub(r"\s{2,}", " ", out).strip()
    return out


def convert_to_material(src: Path, dst: Path) -> None:
    if not src.exists():
        raise FileNotFoundError(src)

    code_map, tipo_map, children_map, price_map, meas_pair_map = _collect_info(src)
    force_mat = _compute_force_material(tipo_map, children_map)

    all_children: set[str] = {
        child for children in children_map.values() for child in children
    }

    dst.parent.mkdir(parents=True, exist_ok=True)

    repl_pattern = (
        re.compile(
            r"("
            + "|".join(re.escape(key) for key in code_map.keys())
            + r")(?=[\\|])"
        )
        if code_map
        else None
    )

    need_cd_parent = "CD#" not in tipo_map
    super_root: str | None = None
    super_d_rewritten = False
    cd_concept_written = False

    with src.open("r", encoding="latin-1", errors="ignore") as fin, dst.open(
        "w",
        encoding="latin-1",
        errors="ignore",
    ) as fout:
        for raw in fin:
            line = raw

            if raw.startswith("~C|"):
                head, rest = raw.split("|", 1)
                parts = rest.rstrip("\n").split("|")
                while len(parts) < 6:
                    parts.append("")

                orig_code = parts[0]
                unidad = parts[1]
                desc = parts[2]
                pres = parts[3]
                tipo = parts[5]

                if super_root is None and orig_code.endswith("##"):
                    super_root = orig_code

                code_out = code_map.get(orig_code, orig_code)
                parts[0] = code_out

                if tipo in {"1", "2", "3"}:
                    parts[5] = "3"
                    tipo = "3"

                if orig_code in force_mat:
                    orig_tipo = tipo_map.get(orig_code, tipo)
                    if orig_tipo == "0":
                        parts[3] = price_map.get(orig_code, pres)
                    parts[5] = "3"
                    tipo = "3"

                is_desc = (
                    tipo == "3"
                    or orig_code in all_children
                    or orig_code in force_mat
                )
                is_partida = tipo == "0" and "#" not in orig_code
                if is_desc or is_partida:
                    parts[1] = _unit_normalized(unidad)

                parts[2] = clean_text(desc)

                line = f"{head}|{'|'.join(parts)}|\n"
                line = clean_text(line)
                fout.write(line)
                continue

            if raw.startswith("~D|"):
                _tag, rest = raw.split("|", 1)
                parent_code, child_part = rest.split("|", 1)

                body_no_nl = child_part.rstrip("\n")
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
                    qty = chunks[i + 2] if i + 2 < len(chunks) else ""
                    i += 3

                    if not child_code:
                        continue

                    if (
                        tipo_map.get(child_code) == "0"
                        and child_code in force_mat
                    ):
                        qty_is_zero = qty.strip() in {
                            "",
                            "0",
                            "0.0",
                            "0.00",
                            "0,0",
                            "0,00",
                        }
                        if qty_is_zero:
                            meas = meas_pair_map.get((parent_code, child_code))
                            if (meas is not None) and (meas > 0):
                                qty = _fmt_num(meas)

                    child_code_out = code_map.get(child_code, child_code)
                    triplets.extend([child_code_out, coef, qty])

                if (
                    need_cd_parent
                    and (super_root is not None)
                    and (parent_code == super_root)
                    and not super_d_rewritten
                ):
                    line_super = f"~D|{super_root}|CD#\\1\\1\\1|\n"
                    line_super = clean_text(line_super)
                    line_super = _ensure_d_trailing_backslash(line_super)
                    fout.write(line_super)

                    if not cd_concept_written:
                        fout.write("~C|CD#||COSTE DIRECTO|||0|\n")
                        cd_concept_written = True

                    children_body = "\\".join(triplets)
                    line_cd = f"~D|CD#|{children_body}|\n"
                    line_cd = clean_text(line_cd)
                    line_cd = _ensure_d_trailing_backslash(line_cd)
                    fout.write(line_cd)

                    super_d_rewritten = True
                    continue

                rebuilt = "\\".join(triplets) + tail_bslashes
                parent_code_out = code_map.get(parent_code, parent_code)
                line = f"~D|{parent_code_out}|{rebuilt}|\n"
                line = clean_text(line)
                line = _ensure_d_trailing_backslash(line)
                fout.write(line)
                continue

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

            if repl_pattern:
                line = repl_pattern.sub(
                    lambda match: code_map[match.group(1)],
                    raw.rstrip("\n"),
                ) + "\n"

            fout.write(clean_text(line))

        if need_cd_parent and (super_root is not None) and not super_d_rewritten:
            orig_children = children_map.get(super_root, [])
            triplets = []
            for child in orig_children:
                child_out = code_map.get(child, child)
                triplets.extend([child_out, "1", "1"])

            line_super = f"~D|{super_root}|CD#\\1\\1\\1|\n"
            line_super = clean_text(line_super)
            line_super = _ensure_d_trailing_backslash(line_super)
            fout.write(line_super)

            if not cd_concept_written:
                fout.write("~C|CD#||COSTE DIRECTO|||0|\n")

            children_body = "\\".join(triplets)
            line_cd = f"~D|CD#|{children_body}|\n"
            line_cd = clean_text(line_cd)
            line_cd = _ensure_d_trailing_backslash(line_cd)
            fout.write(line_cd)
