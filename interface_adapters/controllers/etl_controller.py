# interface_adapters/controllers/etl_controller.py
"""
Controlador principal (versión con Pipeline + Settings):

  0) Crea una copia del BC3 en output/ con:
        • Descompuestos T∈{1,2,3} → T=3 (Material)
        • (Opcional) Forzar T=3 en toda la rama bajo partidas reales (T=0, sin '#')
        • Truncado de códigos y normalización de texto
  1) Construye el árbol lógico sobre la COPIA modificada
  2) (Opcional) Muestra el árbol por consola
  3) (Opcional) Exporta a CSV (output/presupuesto_tree.csv)
"""

from __future__ import annotations

from dataclasses import replace

from config.settings import Settings
from application.pipeline.pipeline import Pipeline, ETLContext
from application.pipeline.steps import (
    ResolveInputStep,
    TransformBC3Step,
    BuildTreeStep,
    PrintTreeStep,
    ExportCsvStep,
)
from utils.timer import Stopwatch


def run_etl(
    input_filename: str | None = None,
    *,
    show_tree: bool = True,
    export_csv: bool = True,
    settings: Settings | None = None,
) -> None:
    """
    Ejecuta el ETL sobre un .bc3.

    Args:
        input_filename: (opcional) nombre de fichero dentro de input/.
                        Si None, se usa Settings.input_filename.
        show_tree:      imprime el árbol por consola si True.
        export_csv:     genera CSV si True.
        settings:       instancia de Settings (opcional). Si no se proporciona,
                        se crea a partir de las variables de entorno.
    """
    # 1) Cargar settings (desde .env) y permitir override del fichero de entrada
    sw = Stopwatch()
    if settings is None:
        settings = Settings()
    if input_filename:
        # Settings está congelado (frozen=True); usamos replace() para clonar con override
        settings = replace(settings, input_filename=input_filename)

    # 2) Construir pipeline según flags
    pipeline = Pipeline().add(ResolveInputStep()).add(TransformBC3Step()).add(BuildTreeStep())
    if show_tree:
        pipeline.add(PrintTreeStep())
    if export_csv:
        pipeline.add(ExportCsvStep())

    # 3) Ejecutar
    ctx = ETLContext(settings=settings)
    pipeline.run(ctx)
    print(sw.report("ETL – tiempos"))
