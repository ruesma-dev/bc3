# config/settings.py
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

try:
    from dotenv import load_dotenv  # type: ignore

    load_dotenv()
except Exception:
    pass


def _env_bool(name: str, default: str = "false") -> bool:
    value = os.getenv(name, default)
    return str(value).strip().lower() in {"1", "true", "yes", "y", "si", "sí"}


def _env_int(name: str, default: str) -> int:
    raw = (os.getenv(name, default) or default).strip()
    try:
        return int(raw)
    except Exception:
        return int(default)


@dataclass(frozen=True)
class Settings:
    input_dir: Path = Path(os.getenv("INPUT_DIR", "input"))
    output_dir: Path = Path(os.getenv("OUTPUT_DIR", "output"))
    input_filename: str = os.getenv("INPUT_FILE_NAME", "presupuesto.bc3")
    csv_filename: str = os.getenv("CSV_FILENAME", "presupuesto_tree.csv")
    encoding: str = os.getenv("BC3_ENCODING", "latin-1")

    max_code_len: int = _env_int("MAX_CODE_LEN", "20")
    force_material: bool = _env_bool("FORCE_MATERIAL", "true")
    fill_unit_ud: bool = _env_bool("FILL_UNIT_UD", "true")
    create_clones: bool = _env_bool("CREATE_CLONES", "true")
    rewrite_bc3: bool = _env_bool("REWRITE_BC3", "true")

    csv_sep: str = os.getenv("CSV_SEPARATOR", ";")
    log_level: str = os.getenv("LOG_LEVEL", "INFO")

    phase2_dump_ocr_io: bool = _env_bool("PHASE2_DUMP_OCR_IO", "false")
    phase2_dump_ocr_dir: str = (os.getenv("PHASE2_DUMP_OCR_DIR", "") or "").strip()
    phase2_dump_ocr_mode: str = (
        os.getenv("PHASE2_DUMP_OCR_MODE", "all") or "all"
    ).strip().lower()

    # Variables legacy mantenidas por compatibilidad. Ya no se usan en la llamada principal.
    ocr_service_root: str = (os.getenv("OCR_SERVICE_ROOT", "") or "").strip()
    ocr_service_python: str = (os.getenv("OCR_SERVICE_PYTHON", "") or "").strip()
    ocr_service_module: str = (
        os.getenv(
            "OCR_SERVICE_MODULE",
            "interface_adapters.cli.bc3_classify_stdin",
        )
        or ""
    ).strip()
    ocr_service_timeout_s: int = _env_int("OCR_SERVICE_TIMEOUT_S", "240")

    bc3_classify_prompt_key: str = (
        os.getenv("BC3_CLASSIFY_PROMPT_KEY", "bc3_clasificador_es") or ""
    ).strip()
    bc3_catalog_sheet: str = (os.getenv("BC3_CATALOG_SHEET", "") or "").strip()
