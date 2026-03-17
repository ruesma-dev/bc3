# interface_adapters/controllers/etl_controller.py
from __future__ import annotations

from dataclasses import replace

from application.pipeline.pipeline import ETLContext, Pipeline
from application.pipeline.steps import (
    BuildTreeStep,
    ExportCsvStep,
    PrintTreeStep,
    ResolveInputStep,
    TransformBC3Step,
)
from config.settings import Settings
from utils.timer import Stopwatch


def run_etl(
    input_filename: str | None = None,
    *,
    show_tree: bool = True,
    export_csv: bool = True,
) -> None:
    sw = Stopwatch()
    settings = Settings()
    if input_filename:
        settings = replace(settings, input_filename=input_filename)

    pipeline = Pipeline().add(ResolveInputStep()).add(TransformBC3Step()).add(BuildTreeStep())
    if show_tree:
        pipeline.add(PrintTreeStep())
    if export_csv:
        pipeline.add(ExportCsvStep())

    ctx = ETLContext(settings=settings)
    pipeline.run(ctx)
    print(sw.report("ETL – tiempos"))
