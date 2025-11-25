# utils/bc3_postclean.py
from __future__ import annotations

from pathlib import Path
import re


# Colapsa barras verticales de cola: "||||" -> "|"
_TRAIL_PIPES = re.compile(r"\|+\s*$")


def _collapse_trailing_pipes(line: str) -> str:
    if line.endswith("\n"):
        body, nl = line[:-1], "\n"
    else:
        body, nl = line, ""
    body = _TRAIL_PIPES.sub("|", body)
    return body + nl


def normalize_trailing_pipes(path: Path) -> None:
    """
    Normaliza el archivo BC3 'path' para que:
      - Cualquier exceso de '|' al final de cada línea quede en una sola '|'.
      - No toca nada más (contenido interno intacto).
    """
    text = path.read_text("latin-1", errors="ignore")
    out_lines = []
    for ln in text.splitlines(keepends=True):
        out_lines.append(_collapse_trailing_pipes(ln))
    path.write_text("".join(out_lines), "latin-1", errors="ignore")
