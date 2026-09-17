# tests/test_mutacion_alcance_por_ficheros.py
"""Alcance declarado a mano con `--ficheros`.

El alcance solo se sabía calcular desde un diff de git. Una campaña sobre un
módulo tal como está HOY no tiene diff que la describa: el sujeto no es «lo que
cambió», son ficheros enteros. `--ficheros` es esa puerta, y estos tests fijan
sus dos mitades: que el alcance salga entero y declarado en el informe (R15), y
que una lista mala aborte con código 2 y sin mutar ni escribir nada (R16).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from harness.alcance import Alcance, alcance_de_ficheros
from harness.mutacion import (
    InformeMutacion,
    _analizar_argumentos,
    escribir_informe,
    main,
)

FICHERO_REAL = "harness/rigor.py"
ESTE_TEST = "tests/test_mutacion_alcance_por_ficheros.py"


# --- R15: el alcance son los ficheros ENTEROS -------------------------------


def test_el_alcance_incluye_todas_las_lineas_del_fichero() -> None:
    total = len(Path(FICHERO_REAL).read_text(encoding="utf-8").splitlines())

    alcance = alcance_de_ficheros([FICHERO_REAL], "F-999")

    assert alcance.ficheros() == [FICHERO_REAL]
    assert alcance.lineas[FICHERO_REAL] == set(range(1, total + 1))
    assert alcance.total_lineas() == total


def test_el_alcance_admite_varios_ficheros_y_los_normaliza() -> None:
    alcance = alcance_de_ficheros(["harness\\rigor.py", "harness/alcance.py"], "F-999")

    assert alcance.ficheros() == ["harness/alcance.py", "harness/rigor.py"]


def test_el_origen_es_ficheros_y_la_ref_dice_que_no_hay_diff() -> None:
    alcance = alcance_de_ficheros([FICHERO_REAL], "F-999")

    assert alcance.feature == "F-999"
    assert alcance.origen == "ficheros"
    assert alcance.ref_diff[0] == "(sin diff)"
    assert len(alcance.ref_diff[1]) == 40, "la segunda ref debe ser el SHA de HEAD"


def test_el_informe_declara_el_alcance_como_declarado_en_la_orden(
    tmp_path: Path,
) -> None:
    """R15 exige esta frase literal: sin diff que enseñar, se dice por qué."""
    alcance = Alcance(
        feature="F-999",
        origen="ficheros",
        ref_diff=("(sin diff)", "0" * 40),
        lineas={FICHERO_REAL: {1, 2, 3}},
    )
    ruta = tmp_path / "informe.md"

    escribir_informe(InformeMutacion(feature="F-999", alcance=alcance), ruta)

    assert (
        "Origen del diff: **ficheros** (alcance declarado en la orden)."
        in ruta.read_text(encoding="utf-8")
    )


def test_el_informe_de_un_alcance_calculado_sigue_enseniando_las_refs(
    tmp_path: Path,
) -> None:
    """La rama nueva no puede comerse el caso normal: con diff, se ven las refs."""
    alcance = Alcance(
        feature="F-999",
        origen="rama",
        ref_diff=("base123", "rama456"),
        lineas={FICHERO_REAL: {1}},
    )
    ruta = tmp_path / "informe.md"

    escribir_informe(InformeMutacion(feature="F-999", alcance=alcance), ruta)

    assert (
        "Origen del diff: **rama** (`base123` .. `rama456`)."
        in ruta.read_text(encoding="utf-8")
    )


# --- R15: el CLI cablea `--ficheros` al alcance -----------------------------


def test_el_cli_acepta_ficheros_separados_por_coma() -> None:
    opciones = _analizar_argumentos(["--feature", "F-999", "--ficheros", "a.py,b.py"])

    assert opciones.ficheros == "a.py,b.py"


def test_con_ficheros_la_campania_muta_ese_alcance_y_no_el_del_diff(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Cableado de `--ficheros` en `main`, sin lanzar ninguna suite.

    La campaña se sustituye por un doble que solo apunta qué alcance recibió:
    lo que se comprueba es la decisión del CLI, no el mutador.
    """
    recibidos: list[Alcance] = []

    def _campania_falsa(alcance: Alcance, *_args: object, **_kwargs: object):
        recibidos.append(alcance)
        # `generados` no puede ser 0: desde la 1.7.2 una campaña sin ni un
        # mutante aborta con código 3 sin escribir informe. Lo que este test
        # comprueba es QUÉ alcance recibió la campaña, no su recuento.
        return InformeMutacion(feature=alcance.feature, alcance=alcance, generados=5)

    def _diff_prohibido(*_args: object, **_kwargs: object) -> Alcance:
        raise AssertionError("con --ficheros no se calcula el alcance desde el diff")

    monkeypatch.setattr("harness.mutacion.ejecutar_campania", _campania_falsa)
    monkeypatch.setattr("harness.mutacion.alcance_de_feature", _diff_prohibido)

    codigo = main(
        [
            "--feature",
            "F-999",
            "--ficheros",
            FICHERO_REAL,
            "--workers",
            "1",
            "--salida",
            str(tmp_path / "informe.md"),
        ]
    )

    assert codigo == 0
    assert len(recibidos) == 1
    assert recibidos[0].origen == "ficheros"
    assert recibidos[0].ficheros() == [FICHERO_REAL]


# --- R16: una ruta mala aborta sin mutar nada -------------------------------


def test_una_ruta_inexistente_aborta_con_mensaje_explicito() -> None:
    with pytest.raises(SystemExit) as parada:
        alcance_de_ficheros(["harness/no_existe_de_nada.py"], "F-999")

    assert "harness/no_existe_de_nada.py" in str(parada.value)
    assert "no existe" in str(parada.value)


def test_una_ruta_que_no_es_produccion_aborta() -> None:
    with pytest.raises(SystemExit) as parada:
        alcance_de_ficheros([ESTE_TEST], "F-999")

    assert "no es código de producción" in str(parada.value)


def test_una_lista_vacia_aborta() -> None:
    with pytest.raises(SystemExit) as parada:
        alcance_de_ficheros([], "F-999")

    assert "--ficheros" in str(parada.value)


def test_el_cli_devuelve_2_y_no_llega_a_mutar(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _campania_prohibida(*_args: object, **_kwargs: object):
        raise AssertionError("no se puede mutar nada con un alcance rechazado")

    monkeypatch.setattr("harness.mutacion.ejecutar_campania", _campania_prohibida)

    assert main(["--feature", "F-999", "--ficheros", "harness/no_existe.py"]) == 2
    assert main(["--feature", "F-999", "--ficheros", "docs/CONVENTIONS.md"]) == 2


def test_ficheros_solo_con_separadores_aborta_desde_el_cli(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """La guarda de lista vacía, por el camino por el que se llega de verdad.

    `test_una_lista_vacia_aborta` cubre la llamada directa con `[]`, pero desde
    `main` la lista NUNCA es `[]`: `split(",")` devuelve siempre al menos un
    elemento y las entradas en blanco se descartan una a una. Con `--ficheros
    ","` la campaña terminaba en **exit 0** escribiendo un informe de **0
    mutantes**, que es justo la campaña vacía que `CHECKPOINTS.md` manda mirar
    con lupa. Lo que importa no es cómo llegue la lista: es que no quede ningún
    fichero que mutar.
    """

    def _campania_prohibida(*_args: object, **_kwargs: object):
        raise AssertionError("no se puede lanzar una campaña sin nada que mutar")

    monkeypatch.setattr("harness.mutacion.ejecutar_campania", _campania_prohibida)

    assert main(["--feature", "F-999", "--ficheros", ","]) == 2
    assert main(["--feature", "F-999", "--ficheros", ",,,"]) == 2
    assert main(["--feature", "F-999", "--ficheros", "   "]) == 2


def test_el_aborto_por_alcance_vacio_no_escribe_informe(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Un informe con un cero que nadie ha medido es peor que no tener informe."""

    def _campania_prohibida(*_args: object, **_kwargs: object):
        raise AssertionError("no se puede lanzar una campaña sin nada que mutar")

    monkeypatch.setattr("harness.mutacion.ejecutar_campania", _campania_prohibida)
    destino = tmp_path / "no_deberia_existir.md"

    codigo = main(["--feature", "F-999", "--ficheros", ",,", "--salida", str(destino)])

    assert codigo == 2
    assert not destino.exists()
