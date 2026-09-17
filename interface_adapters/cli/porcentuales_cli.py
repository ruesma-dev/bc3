# interface_adapters/cli/porcentuales_cli.py
"""F-002 · Ejecuta la pasada de porcentuales sobre un `.bc3` suelto (R22).

    python -m interface_adapters.cli.porcentuales_cli entrada.bc3 salida.bc3

No necesita el resto del ETL, ni red, ni base de datos: todo lo que la pasada
necesita está dentro del propio fichero. Deja además el informe de R20 en un
CSV para Excel (UTF-8 con BOM, `;` y coma decimal).
"""

from __future__ import annotations

import argparse
import csv
import logging
from decimal import Decimal
from pathlib import Path
from typing import Sequence

from config.settings import Settings
from infrastructure.bc3.bc3_porcentajes import (
    InformePorcentuales,
    convertir_porcentuales,
)

logger = logging.getLogger("f002.porcentuales")

INFORME_POR_DEFECTO = Path("output") / "informe_porcentuales.csv"
CABECERA_CASOS = ("padre", "codigo", "motivo", "rendimiento", "importe")


def _con_coma(valor: float) -> str:
    """Número para Excel en español: coma decimal y sin notación científica."""
    return f"{Decimal(str(valor)):f}".replace(".", ",")


def escribir_informe(informe: InformePorcentuales, destino: Path) -> Path:
    """Vuelca el informe a CSV (UTF-8 BOM, `;`, coma decimal)."""
    destino.parent.mkdir(parents=True, exist_ok=True)
    with destino.open("w", encoding="utf-8-sig", newline="") as fichero:
        escritor = csv.writer(fichero, delimiter=";")
        escritor.writerow(("metrica", "valor"))
        escritor.writerow(("descompuestos_con_porcentual", informe.descompuestos))
        escritor.writerow(("lineas_convertidas", informe.lineas_convertidas))
        escritor.writerow(("conceptos_eliminados", informe.conceptos_eliminados))
        escritor.writerow(())
        escritor.writerow(CABECERA_CASOS)
        for caso in informe.casos:
            escritor.writerow((
                caso.padre,
                caso.codigo,
                caso.motivo,
                _con_coma(caso.rendimiento),
                _con_coma(caso.importe),
            ))
    return destino


def _argumentos(argv: Sequence[str] | None) -> argparse.Namespace:
    analizador = argparse.ArgumentParser(
        prog="porcentuales_cli",
        description="Convierte los descompuestos porcentuales de un BC3 a UD "
                    "con cantidad 1, conservando el importe de Presto.",
    )
    analizador.add_argument("entrada", type=Path, help="fichero .bc3 de entrada")
    analizador.add_argument("salida", type=Path, help="fichero .bc3 de salida")
    analizador.add_argument("--informe", type=Path, default=INFORME_POR_DEFECTO,
                            help=f"CSV del informe (por defecto {INFORME_POR_DEFECTO})")
    analizador.add_argument("--sin-conversion", action="store_true",
                            help="copia el fichero sin tocarlo (bandera apagada)")
    return analizador.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    ajustes = Settings()
    logging.basicConfig(level=ajustes.log_level,
                        format="%(levelname)s %(name)s: %(message)s")
    args = _argumentos(argv)

    activo = ajustes.porcentuales_a_ud and not args.sin_conversion
    try:
        informe = convertir_porcentuales(
            args.entrada, args.salida, encoding=ajustes.encoding, activo=activo
        )
    except FileNotFoundError:
        logger.error("No existe el fichero de entrada: %s", args.entrada)
        return 1

    ruta_informe = escribir_informe(informe, args.informe)
    logger.info(
        "%s → %s | %d ~D con porcentual, %d líneas convertidas, "
        "%d conceptos eliminados, %d casos excepcionales",
        args.entrada, args.salida, informe.descompuestos,
        informe.lineas_convertidas, informe.conceptos_eliminados,
        len(informe.casos),
    )
    logger.info("Informe → %s", ruta_informe)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
