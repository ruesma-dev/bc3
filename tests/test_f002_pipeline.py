# tests/test_f002_pipeline.py
"""F-002 · El encaje de la pasada en el pipeline del ETL.

La pasada de porcentuales corre ANTES de `convert_to_material` y le entrega su
salida: `TransformBC3Step` lee `ctx.preprocessed_path or ctx.original_path`.

Aquí no se toca `input/`: la entrada son las fixtures y la salida el `tmp_path`
del test. `convert_to_material` se sustituye por un espía para comprobar
exactamente con qué ruta se le llama.
"""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

from application.pipeline import steps as modulo_steps
from application.pipeline.pipeline import ETLContext
from application.pipeline.steps import ConvertirPorcentualesStep, TransformBC3Step
from config.settings import Settings

FIXTURES = Path(__file__).parent / "fixtures"


def _contexto(tmp_path, **cambios) -> ETLContext:
    ajustes = replace(
        Settings(),
        input_dir=FIXTURES,
        input_filename="f002_cadena.bc3",
        output_dir=tmp_path,
        **cambios,
    )
    ctx = ETLContext(settings=ajustes)
    ctx.original_path = FIXTURES / "f002_cadena.bc3"
    return ctx


def _espiar_convert_to_material(monkeypatch) -> list[Path]:
    recibidas: list[Path] = []

    def espia(src, dst, **kwargs):
        recibidas.append(Path(src))
        Path(dst).write_bytes(Path(src).read_bytes())

    monkeypatch.setattr(modulo_steps, "convert_to_material", espia)
    return recibidas


def test_f002_el_step_deja_la_ruta_preprocesada_en_el_contexto(tmp_path):
    ctx = _contexto(tmp_path)
    ConvertirPorcentualesStep().run(ctx)
    assert ctx.preprocessed_path is not None
    assert ctx.preprocessed_path.exists()
    assert ctx.preprocessed_path.parent == tmp_path
    # La entrada no se toca y la salida ya trae los clones.
    assert ctx.original_path.read_bytes() == (FIXTURES / "f002_cadena.bc3").read_bytes()
    texto = ctx.preprocessed_path.read_bytes().decode("latin-1")
    assert "~C|43.15.P1|UD|" in texto
    assert "~C|%SUB25|" not in texto


def test_f002_transform_usa_la_ruta_preprocesada_cuando_existe(tmp_path, monkeypatch):
    recibidas = _espiar_convert_to_material(monkeypatch)
    ctx = _contexto(tmp_path)
    ConvertirPorcentualesStep().run(ctx)
    TransformBC3Step().run(ctx)
    assert recibidas == [ctx.preprocessed_path]


def test_f002_transform_usa_la_original_si_no_hubo_pasada_previa(tmp_path, monkeypatch):
    recibidas = _espiar_convert_to_material(monkeypatch)
    ctx = _contexto(tmp_path)
    TransformBC3Step().run(ctx)
    assert recibidas == [ctx.original_path]


def test_f002_r21_con_la_bandera_apagada_el_step_no_preprocesa_nada(tmp_path):
    ctx = _contexto(tmp_path, porcentuales_a_ud=False)
    ConvertirPorcentualesStep().run(ctx)
    assert ctx.preprocessed_path is None
    assert list(tmp_path.iterdir()) == []
