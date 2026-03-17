# infrastructure/bc3/bc3_reader.py
from __future__ import annotations

import re
from pathlib import Path
from typing import Iterator

BC3_SECTION_REGEX = re.compile(r"^(?P<section>[A-Z]{3});")


def iter_bc3_lines(path: Path) -> Iterator[str]:
    with path.open(encoding="latin-1") as fh:
        for line in fh:
            line = line.rstrip("\n")
            if not line or line.startswith("/*"):
                continue
            yield line


def read_bc3(path: Path):
    for line in iter_bc3_lines(path):
        match = BC3_SECTION_REGEX.match(line)
        if not match:
            continue
        section = match.group("section")
        yield section, line[len(section) + 1 :]


def print_raw_bc3(path: Path) -> None:
    with path.open(mode="r", encoding="latin-1", errors="ignore") as fh:
        for line in fh:
            print(line.rstrip("\n"))
