# tests/test_alcance_excluidos.py
"""El arnés no es código de producción de la feature que lo usa.

`harness.alcance` decide qué líneas entran en el alcance de una feature, y de
ahí salen las dos puertas objetivas del arnés: la de cobertura de líneas
cambiadas y la campaña de mutación. Si el propio `harness/` entra en ese
conjunto, tocar el arnés desde la rama de una feature hace que **la campaña se
mute a sí misma** y que la puerta de cobertura mida la herramienta en vez del
producto. El arnés es utillaje, igual que `tests` o `docs`.

Este fichero fija además la regla que el módulo lleva documentada desde el
principio y que hasta hoy no sostenía ningún test: la exclusión se compara por
SEGMENTO de ruta a cualquier profundidad, no como prefijo de la raíz ni contra
el nombre del fichero.
"""

from __future__ import annotations

import pytest

from harness.alcance import (
    DIRECTORIOS_EXCLUIDOS,
    DIRECTORIOS_EXCLUIDOS_AL_DECLARAR,
    alcance_de_ficheros,
    es_produccion,
    filtrar_produccion,
)


def test_harness_esta_entre_los_directorios_excluidos() -> None:
    """El utillaje del arnés no es código mutable de ninguna feature."""
    assert "harness" in DIRECTORIOS_EXCLUIDOS


@pytest.mark.parametrize(
    "ruta",
    [
        "harness/mutacion.py",
        "harness/mutacion_paralela.py",
        "harness/alcance.py",
        "harness\\alcance.py",  # separador de Windows
        "services/api/harness/parche.py",  # a cualquier profundidad
    ],
)
def test_ningun_fichero_del_arnes_es_produccion(ruta: str) -> None:
    assert es_produccion(ruta) is False


@pytest.mark.parametrize(
    "ruta",
    [
        "tests/test_algo.py",
        "specs/F-001-algo/plan.py",
        "progress/apunte.py",
        "docs/ejemplo.py",
        "services/api/tests/test_algo.py",
    ],
)
def test_las_exclusiones_anteriores_siguen_en_pie(ruta: str) -> None:
    """Añadir `harness` no puede haberse llevado por delante lo que ya excluía."""
    assert es_produccion(ruta) is False


@pytest.mark.parametrize(
    "ruta",
    [
        "services/api/entrada.py",
        "services/api/domain/modelos.py",
        "app/harness.py",  # el NOMBRE del fichero no cuenta: es código
        "app/docs.py",
        "harnesses/util.py",  # segmento completo, no prefijo
        "mi_harness/util.py",
    ],
)
def test_el_codigo_del_proyecto_sigue_siendo_produccion(ruta: str) -> None:
    assert es_produccion(ruta) is True


def test_filtrar_produccion_saca_el_arnes_del_mapa_de_lineas() -> None:
    """La puerta de cobertura y la campaña comen de aquí: el filtro es la frontera."""
    lineas = {
        "harness/mutacion_paralela.py": {10, 11},
        "services/api/entrada.py": {704},
        "tests/test_algo.py": {1},
    }
    assert filtrar_produccion(lineas) == {
        "services/api/entrada.py": {704}
    }


# --- La excepción: el alcance que declara una persona ------------------------


def test_al_declarar_ficheros_a_mano_el_arnes_si_se_puede_mutar() -> None:
    """`--ficheros` existe justamente para medir la maquinaria del arnés.

    La exclusión de `harness` protege el alcance AUTOMÁTICO, el que sale del
    diff y que nadie eligió. Cuando alguien escribe la lista a mano no hay nada
    de lo que protegerle, y en el repositorio del propio arnés esa maquinaria es
    el producto: sin esta excepción, el arnés no podría medirse nunca a sí mismo.
    """
    assert "harness" not in DIRECTORIOS_EXCLUIDOS_AL_DECLARAR
    assert es_produccion("harness/mutacion.py", DIRECTORIOS_EXCLUIDOS_AL_DECLARAR)


@pytest.mark.parametrize("ruta", ["tests/test_algo.py", "docs/ejemplo.py"])
def test_declarar_a_mano_no_levanta_las_demas_exclusiones(ruta: str) -> None:
    """Mutar un test o un documento sigue sin significar nada, lo pida quien lo pida."""
    assert es_produccion(ruta, DIRECTORIOS_EXCLUIDOS_AL_DECLARAR) is False


def test_alcance_de_ficheros_acepta_un_modulo_del_arnes() -> None:
    """La comprobación de punta a punta: la puerta por la que entra de verdad."""
    alcance = alcance_de_ficheros(["harness/rigor.py"], "F-999")

    assert alcance.ficheros() == ["harness/rigor.py"]
    assert alcance.total_lineas() > 0


def test_alcance_de_ficheros_sigue_rechazando_un_test() -> None:
    with pytest.raises(SystemExit):
        alcance_de_ficheros(["tests/test_alcance_excluidos.py"], "F-999")
