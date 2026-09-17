# tests/test_mutacion_informe_base.py
"""El comando de la cabecera no reproduce nada si se calla contra qué base midió.

Pasó de verdad el 2026-09-06. La rama de una feature nació de la rama de otra
feature, la campaña se lanzó con `--base feature/F-009-cierre-sigrid` y el
informe imprimió `python -m harness.mutacion --feature F-012 --workers 8`.
Copiado tal cual, ese comando mide **otro alcance**: el diff contra `dev` de
una rama que nace de otra feature arrastra las líneas de la feature madre, así
que ni el número de mutantes ni la lista de supervivientes son comparables.

Es el mismo agujero que ya se tapó con `--workers` el 2026-09-02: la cabecera
del informe es la única memoria de cómo se midió, porque la línea de comando se
la lleva el scrollback.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from harness import mutacion
from harness.alcance import Alcance
from harness.mutacion import (
    BASE_POR_DEFECTO,
    InformeMutacion,
    Mutante,
    comando_de,
    escribir_informe,
)

#: Una rama de feature que nace de otra feature, que es el caso que rompió.
BASE_HEREDADA = "feature/F-009-cierre-sigrid"

ALCANCE = Alcance(
    feature="F-000", origen="rama", ref_diff=("dev", "rama"), lineas={"codigo.py": {2}}
)


def mutante(linea: int) -> Mutante:
    return Mutante(
        fichero="codigo.py",
        linea=linea,
        col=0,
        original="==",
        mutado="!=",
        operador="comparacion",
    )


def informe(**campos: object) -> InformeMutacion:
    campos_base: dict[str, object] = {
        "feature": "F-000",
        "alcance": ALCANCE,
        "generados": 3,
        "muertos": 3,
        "mutantes_evaluados": [mutante(2), mutante(3), mutante(4)],
        "segundos": 120.0,
    }
    campos_base.update(campos)
    return InformeMutacion(**campos_base)  # type: ignore[arg-type]


def escrito(informe_: InformeMutacion, tmp_path: Path) -> str:
    destino = tmp_path / "mutacion_F-000.md"
    escribir_informe(informe_, destino)
    return destino.read_text(encoding="utf-8")


# --- Contra qué base se midió -----------------------------------------------


def test_el_comando_cita_la_base_cuando_no_es_la_de_siempre() -> None:
    """Sin `--base`, copiar el comando mide otro alcance y nadie se entera."""
    assert comando_de(informe(base=BASE_HEREDADA)) == (
        f"python -m harness.mutacion --feature F-000 --base {BASE_HEREDADA}"
    )


def test_la_cabecera_del_informe_lleva_esa_base(tmp_path: Path) -> None:
    texto = escrito(informe(base=BASE_HEREDADA), tmp_path)

    assert f"--feature F-000 --base {BASE_HEREDADA}" in texto


def test_con_la_base_de_siempre_el_comando_no_la_repite() -> None:
    """`--base dev` es lo que hace el comando sin flags: escribirlo es ruido."""
    assert comando_de(informe(base=BASE_POR_DEFECTO)) == (
        "python -m harness.mutacion --feature F-000"
    )


def test_sin_base_conocida_el_comando_no_se_inventa_ninguna() -> None:
    """Un informe viejo, o uno fabricado a mano, no gana una base falsa."""
    assert "--base" not in comando_de(informe())


def test_la_base_convive_con_los_workers() -> None:
    """Las dos hacen falta a la vez: alcance distinto y reloj distinto."""
    assert comando_de(informe(base=BASE_HEREDADA, workers=8)) == (
        f"python -m harness.mutacion --feature F-000 --base {BASE_HEREDADA} "
        "--workers 8"
    )


# --- Y el dato llega ahí desde la línea de comando --------------------------


def _campania_falsa(medido: InformeMutacion):
    def falsa(alcance, ejecutor, **kwargs):
        return medido

    return falsa


def test_main_registra_en_el_informe_la_base_con_la_que_se_lanzo(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """De nada sirve el campo si `main` no lo rellena: esto cierra el circuito."""
    medido = informe()
    monkeypatch.setattr(mutacion, "alcance_de_feature", lambda *a, **k: ALCANCE)
    monkeypatch.setattr(mutacion, "ejecutar_campania", _campania_falsa(medido))

    destino = tmp_path / "mutacion_F-000.md"
    mutacion.main(
        [
            "--feature",
            "F-000",
            "--raiz",
            str(tmp_path),
            "--base",
            BASE_HEREDADA,
            "--salida",
            str(destino),
        ],
        ejecutor=object(),
    )

    assert medido.base == BASE_HEREDADA
    assert f"--base {BASE_HEREDADA}" in destino.read_text(encoding="utf-8")
