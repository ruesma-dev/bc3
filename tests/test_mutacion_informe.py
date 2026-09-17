# tests/test_mutacion_informe.py
"""Repetir una campaña no puede borrar el análisis de los supervivientes.

Una campaña de mutación se relanza muchas veces —tras un merge, tras tocar un
test, tras actualizar el arnés— y hasta ahora cada relanzamiento sobrescribía
el informe con la plantilla vacía. Con ella se perdía el análisis de cada
superviviente, que es evidencia que `CHECKPOINTS.md` (C4 bis) exige y que a
veces el humano ya ha dado por buena. Pasó de verdad el 2026-08-18.
"""

from __future__ import annotations

from pathlib import Path

from harness.alcance import Alcance
from harness.mutacion import (
    AVISO_REPUESTO,
    CABECERA_PENDIENTE,
    InformeMutacion,
    Mutante,
    analisis_escritos,
    escribir_informe,
)

ANALISIS = (
    "#### Análisis (completado por el implementer)\n"
    "\n"
    "> Por qué ningún test lo caza: solo cambia el color de una cabecera.\n"
    "> Decisión: **mutante equivalente, justificado**."
)


def _informe(supervivientes: list[Mutante]) -> InformeMutacion:
    alcance = Alcance(
        feature="F-999",
        origen="rama",
        ref_diff=("dev", "feature/F-999-prueba"),
        lineas={"main.py": {10, 20}},
    )
    return InformeMutacion(
        feature="F-999",
        alcance=alcance,
        generados=2,
        muertos=1,
        supervivientes=supervivientes,
        mutantes_evaluados=list(supervivientes),
    )


def _mutante(linea: int = 10, mutado: str = "b = False") -> Mutante:
    return Mutante(
        fichero="main.py",
        linea=linea,
        col=0,
        original="b = True",
        mutado=mutado,
        operador="booleano",
    )


def test_sin_informe_previo_el_analisis_queda_pendiente(tmp_path: Path) -> None:
    ruta = tmp_path / "mutacion_F-999.md"
    escribir_informe(_informe([_mutante()]), ruta)
    assert CABECERA_PENDIENTE in ruta.read_text(encoding="utf-8")


def test_el_analisis_escrito_sobrevive_a_repetir_la_campania(tmp_path: Path) -> None:
    ruta = tmp_path / "mutacion_F-999.md"
    escribir_informe(_informe([_mutante()]), ruta)
    ruta.write_text(
        ruta.read_text(encoding="utf-8").replace(
            CABECERA_PENDIENTE
            + "\n"
            + "\n"
            + "> Por qué ningún test lo caza: PENDIENTE.\n"
            + "> Decisión: ¿test nuevo o mutante equivalente justificado?",
            ANALISIS,
        ),
        encoding="utf-8",
    )

    escribir_informe(_informe([_mutante()]), ruta)

    texto = ruta.read_text(encoding="utf-8")
    assert "mutante equivalente, justificado" in texto
    assert CABECERA_PENDIENTE not in texto
    assert AVISO_REPUESTO in texto, "el reviewer tiene que ver que el análisis se trajo"


def test_el_analisis_sigue_al_mutante_aunque_la_linea_se_mueva(tmp_path: Path) -> None:
    """La línea baila con cualquier import nuevo; el mutante es el mismo."""
    ruta = tmp_path / "mutacion_F-999.md"
    escribir_informe(_informe([_mutante(linea=10)]), ruta)
    ruta.write_text(
        ruta.read_text(encoding="utf-8").replace(CABECERA_PENDIENTE, ANALISIS.splitlines()[0]),
        encoding="utf-8",
    )

    escribir_informe(_informe([_mutante(linea=42)]), ruta)

    texto = ruta.read_text(encoding="utf-8")
    assert "`main.py:42`" in texto
    assert "#### Análisis (completado por el implementer)" in texto


def test_un_mutante_distinto_no_hereda_el_analisis_de_otro(tmp_path: Path) -> None:
    ruta = tmp_path / "mutacion_F-999.md"
    escribir_informe(_informe([_mutante()]), ruta)
    ruta.write_text(
        ruta.read_text(encoding="utf-8").replace(CABECERA_PENDIENTE, ANALISIS.splitlines()[0]),
        encoding="utf-8",
    )

    escribir_informe(_informe([_mutante(mutado="b = None")]), ruta)

    assert CABECERA_PENDIENTE in ruta.read_text(encoding="utf-8")


def test_la_plantilla_vacia_no_cuenta_como_analisis() -> None:
    texto = (
        "### 1. `main.py:10` [booleano]\n"
        "\n"
        "- Original: `b = True`\n"
        "- Mutado:   `b = False`\n"
        "\n"
        f"{CABECERA_PENDIENTE}\n"
        "\n"
        "> Por qué ningún test lo caza: PENDIENTE.\n"
    )
    assert analisis_escritos(texto) == {}


def test_dos_supervivientes_con_la_misma_clave_y_analisis_distintos_se_descartan() -> None:
    """Reponer el análisis equivocado sería peor que no reponer ninguno."""
    seccion = (
        "### {n}. `main.py:{linea}` [booleano]\n"
        "\n"
        "- Original: `b = True`\n"
        "- Mutado:   `b = False`\n"
        "\n"
        "#### Análisis (completado por el implementer)\n"
        "\n"
        "> {texto}\n"
    )
    texto = seccion.format(n=1, linea=10, texto="es equivalente") + seccion.format(
        n=2, linea=99, texto="es un hueco real"
    )
    assert analisis_escritos(texto) == {}
