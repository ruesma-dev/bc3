# main_phase2.py
from __future__ import annotations
from pathlib import Path
import sys

# Carga .env si está disponible (API key, etc.)
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

from application.services.phase2_code_mapper import run_phase2
from utils.timer import timer

# --- Ruta por defecto para la FASE 2: salida de la FASE 1 ---
#    (puedes cambiarla si tu proyecto está en otro directorio)
DEFAULT_BC3_IN = Path(r"C:\Users\pgris\PycharmProjects\bc3\output\presupuesto_material.bc3")
# Ruta por defecto del catálogo (si no pasas argumento 2)
DEFAULT_CATALOG = Path("data") / "catalogo.xlsx"   # ajusta si lo tienes en otro sitio


def _usage() -> None:
    print(
        "Uso:\n"
        "  python main_phase2.py <input.bc3> <catalogo.xlsx> [output.bc3]\n\n"
        "Atajos:\n"
        "  python main_phase2.py <catalogo.xlsx>            # usa input por defecto (FASE 1)\n"
        "  python main_phase2.py                            # usa input y catálogo por defecto\n\n"
        "Por defecto, input.bc3 =\n"
        f"  {DEFAULT_BC3_IN}\n"
        "y catálogo = data\\catalogo.xlsx\n"
    )


if __name__ == "__main__":
    argc = len(sys.argv)

    if argc >= 3:
        # Caso completo: input + catálogo (+ opcional output)
        bc3_in = Path(sys.argv[1])
        catalog_xlsx = Path(sys.argv[2])
        bc3_out = Path(sys.argv[3]) if argc >= 4 else None
    elif argc == 2:
        # Solo pasas el catálogo → usamos input por defecto
        bc3_in = DEFAULT_BC3_IN
        catalog_xlsx = Path(sys.argv[1])
        bc3_out = None
    else:
        # Ningún argumento → defaults para todo
        bc3_in = DEFAULT_BC3_IN
        catalog_xlsx = DEFAULT_CATALOG
        bc3_out = None

    if not bc3_in.exists():
        print(f"ERROR: No existe el BC3 de entrada:\n  {bc3_in}\n")
        _usage()
        sys.exit(2)
    if not catalog_xlsx.exists():
        print(f"ERROR: No existe el Excel de catálogo:\n  {catalog_xlsx}\n")
        _usage()
        sys.exit(3)

    with timer("FASE 2 (clasificación + reescritura)"):
        out = run_phase2(bc3_in, catalog_xlsx, bc3_out)

    print(f"BC3 clasificado → {out.resolve()}")
