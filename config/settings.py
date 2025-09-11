# config/settings.py
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    # Si no está instalado python-dotenv, seguimos con variables del entorno
    pass


@dataclass(frozen=True)
class Settings:
    # Rutas y ficheros
    input_dir: Path = Path(os.getenv("INPUT_DIR", "input"))
    output_dir: Path = Path(os.getenv("OUTPUT_DIR", "output"))
    input_filename: str = os.getenv("INPUT_FILE_NAME", "presupuesto.bc3")
    csv_filename: str = os.getenv("CSV_FILENAME", "presupuesto_tree.csv")
    encoding: str = os.getenv("BC3_ENCODING", "latin-1")

    # Reglas
    max_code_len: int = int(os.getenv("MAX_CODE_LEN", "20"))
    force_material: bool = os.getenv("FORCE_MATERIAL", "true").lower() == "true"
    fill_unit_ud: bool = os.getenv("FILL_UNIT_UD", "true").lower() == "true"
    create_clones: bool = os.getenv("CREATE_CLONES", "true").lower() == "true"
    rewrite_bc3: bool = os.getenv("REWRITE_BC3", "true").lower() == "true"

    # Salida
    csv_sep: str = os.getenv("CSV_SEPARATOR", ";")

    # Logging
    log_level: str = os.getenv("LOG_LEVEL", "INFO")
