# config/settings.py

from __future__ import annotations
import os
from pathlib import Path
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv(usecwd=True), override=False)

# CSV
CSV_DEFAULT_PATH: Path = Path("output/presupuesto_tree.csv")
CSV_SEP: str = ";"
CSV_ENCODING: str = "utf-8"

# Catálogo (xlsx/csv/yaml)
PRODUCTS_PATH: Path = Path("config/productos_etl.xlsx")

# Gemini
GEMINI_MODEL_NAME: str = os.getenv("GEMINI_MODEL_NAME", "gemini-2.5-flash-lite")
GEMINI_API_KEY: str | None = os.getenv("GEMINI_API_KEY")

# Cuota y robustez
GEMINI_RPM: int = int(os.getenv("GEMINI_RPM", "10"))           # acorde al modelo
GEMINI_MAX_RETRIES: int = int(os.getenv("GEMINI_MAX_RETRIES", "2"))
GEMINI_ON_429: str = os.getenv("GEMINI_ON_429", "fallback")    # wait|fallback
GEMINI_MIN_CONFIDENCE: float = float(os.getenv("GEMINI_MIN_CONFIDENCE", "0.35"))

# Batch
GEMINI_BATCH_MODE: bool = os.getenv("GEMINI_BATCH_MODE", "true").lower() == "true"
GEMINI_BATCH_SIZE: int = int(os.getenv("GEMINI_BATCH_SIZE", "10"))       # ítems por request
PREFILTER_TOPK: int = int(os.getenv("PREFILTER_TOPK", "20"))             # candidatos por ítem

# Fallback local
USE_LOCAL_FALLBACK: bool = os.getenv("USE_LOCAL_FALLBACK", "true").lower() == "true"

# Renombrado BC3
USE_DESTRUCTIVE_RENAME: bool = os.getenv("USE_DESTRUCTIVE_RENAME", "true").lower() == "true"

# Códigos BC3
MAX_CODE_LEN: int = 20
