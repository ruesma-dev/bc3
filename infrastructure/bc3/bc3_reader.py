# infrastructure/bc3/bc3_reader.py
import re
from pathlib import Path
from typing import Iterator

BC3_SECTION_REGEX = re.compile(r"^(?P<section>[A-Z]{3});")

def iter_bc3_lines(path: Path) -> Iterator[str]:
    """Devuelve cada línea del BC3, sin \n, ignorando vacías y comentarios."""
    with path.open(encoding="latin-1") as fh:
        for line in fh:
            line = line.rstrip("\n")
            if not line or line.startswith("/*"):
                continue
            yield line

def read_bc3(path: Path):
    """
    Yields tuples (section, line_content) where section is e.g. 'CAB', 'SUB', 'PAR', 'DEC', etc.
    """
    for line in iter_bc3_lines(path):
        match = BC3_SECTION_REGEX.match(line)
        if not match:
            continue
        section = match.group("section")
        yield section, line[len(section) + 1 :]  # remove 'SEC;'

def print_raw_bc3(path: Path) -> None:
    """
    Imprime en consola el contenido íntegro del fichero BC3 tal cual.
    """
    with path.open(mode="r", encoding="latin‑1", errors="ignore") as fh:
        for line in fh:
            # Mostramos las líneas exactamente como están en el fichero
            print(line.rstrip("\n"))
