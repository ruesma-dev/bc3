# infrastructure/bc3/bc3_modifier.py
from __future__ import annotations
from pathlib import Path
import re
from utils.text_sanitize import clean_text

MAX_CODE_LEN = 20


def _short(code: str) -> str:
    """Trunca el código a un máximo de 20 caracteres."""
    return code[:MAX_CODE_LEN]


# --------------------------------------------------------------------------- #
#                         PASADA 1 – construir code_map                       #
# --------------------------------------------------------------------------- #
def _collect_code_map(src: Path) -> dict[str, str]:
    """
    Lee solo las líneas ~C y devuelve {codigo_original: codigo_truncado}
    """
    code_map: dict[str, str] = {}
    with src.open("r", encoding="latin-1", errors="ignore") as fh:
        for line in fh:
            if line.startswith("~C|"):
                _, rest = line.split("|", 1)
                code = rest.split("|", 1)[0]
                if len(code) > MAX_CODE_LEN:
                    code_map[code] = _short(code)
    return code_map


# --------------------------------------------------------------------------- #
#                         PASADA 2 – reescribir BC3                           #
# --------------------------------------------------------------------------- #
def convert_to_material(src: Path, dst: Path) -> None:
    """
    Genera `dst` aplicando:
      • Truncado de códigos (>20) → 20 car. (en todas las líneas)
      • Descompuestos (T 1/2/3)   → T = 3 (Material)
      • Unidad vacía en partida/descompuesto → 'UD'
      • Limpieza de tildes y caracteres 'raros'
    """
    if not src.exists():
        raise FileNotFoundError(src)

    code_map = _collect_code_map(src)          # primera pasada
    dst.parent.mkdir(parents=True, exist_ok=True)

    # patrón para encontrar cualquier código a sustituir delante de \ o |
    if code_map:
        pipeslash_pattern = re.compile(
            r"(" + "|".join(re.escape(k) for k in code_map.keys()) + r")(?=[\\|])"
        )

    with src.open("r", encoding="latin-1", errors="ignore") as fin, \
         dst.open("w", encoding="latin-1", errors="ignore") as fout:

        for raw in fin:
            line = raw

            # ------------------- procesar línea ~C ---------------------------
            if raw.startswith("~C|"):
                head, rest = raw.split("|", 1)
                parts = rest.rstrip("\n").split("|")
                if len(parts) >= 6:
                    code, unidad, desc, pres, *_rest, tipo = parts[:6]

                    # 1) truncado de código (si corresponde)
                    if code in code_map:
                        parts[0] = code_map[code]

                    # 2) descompuestos 1/2/3 → material
                    if tipo in {"1", "2", "3"}:
                        parts[5] = "3"

                    # 3) unidad vacía en partida/descompuesto
                    if tipo in {"0", "1", "2", "3"} and not unidad.strip():
                        parts[1] = "UD"

                    # 4) limpieza de descripción
                    parts[2] = clean_text(parts[2])

                    line = f"{head}|{'|'.join(parts)}|\n"

            # ------------------- procesar resto de líneas --------------------
            if code_map and not raw.startswith("~C|"):
                line = pipeslash_pattern.sub(
                    lambda m: code_map[m.group(1)], raw.rstrip("\n")
                ) + "\n"

            # limpieza final (quita tildes y símbolos “raros”)
            fout.write(clean_text(line))
