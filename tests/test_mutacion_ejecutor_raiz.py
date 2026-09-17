# tests/test_mutacion_ejecutor_raiz.py
"""La campaña puede juzgar lo que no pertenece a ningún servicio.

Hasta hoy, un fichero de `harness/` no cae en ningún servicio de
`harness/servicios.json`, así que `ejecutor_para` lo juzgaba con `python -m
pytest` SIN ruta desde la raíz. Como la raíz no tiene configuración de pytest,
esa invocación recoge `services/**/tests` y muere en la recolección: hasta la
1.6.0 daba un falso verde silencioso (todos «muertos») y desde la 1.6.0 aborta
la campaña. En ambos casos, `harness/` no se puede medir.

El arreglo es la ruta acotada `tests`, exactamente la que `harness/init.sh` ya
usa en su sección 7 para la suite de la raíz.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from harness import mutacion
from harness.mutacion import ARGUMENTOS_LINEA_BASE, EjecutorPytest, ejecutor_para
from harness.servicios import Servicio

SERVICIOS = [
    Servicio(nombre="sv1", ruta="services/uno", lenguaje="python"),
    Servicio(nombre="infra", ruta="infra", lenguaje="otro"),
]


class _EspiaSubprocess:
    """Anota con qué orden se habría lanzado la suite, sin lanzarla."""

    def __init__(self) -> None:
        self.ordenes: list[list[str]] = []

    def __call__(self, orden, **_kwargs):
        self.ordenes.append([str(parte) for parte in orden])
        return subprocess.CompletedProcess(orden, 0, b"", b"")


@pytest.fixture
def espia(monkeypatch: pytest.MonkeyPatch) -> _EspiaSubprocess:
    doble = _EspiaSubprocess()
    monkeypatch.setattr(mutacion.subprocess, "run", doble)
    return doble


def _raiz_con_tests(tmp_path: Path) -> str:
    (tmp_path / "tests").mkdir()
    return str(tmp_path)


def test_lo_que_no_es_de_un_servicio_se_acota_a_tests(tmp_path: Path) -> None:
    """R1: con `<raiz>/tests`, el ejecutor de la raíz acota ahí la recolección."""
    raiz = _raiz_con_tests(tmp_path)

    ejecutor = ejecutor_para("harness/mutacion.py", SERVICIOS, raiz=raiz)

    assert ejecutor.ruta == "tests"


def test_un_py_de_un_servicio_de_otro_lenguaje_tambien_se_acota(
    tmp_path: Path,
) -> None:
    """R1: un `.py` suelto dentro de un servicio que no es Python cae aquí igual."""
    raiz = _raiz_con_tests(tmp_path)

    ejecutor = ejecutor_para("infra/despliegue.py", SERVICIOS, raiz=raiz)

    assert ejecutor.ruta == "tests"


def test_sin_directorio_tests_la_invocacion_es_la_de_siempre(
    tmp_path: Path,
) -> None:
    """R2: sin `<raiz>/tests` no hay nada que acotar y no se inventa una ruta."""
    ejecutor = ejecutor_para("harness/mutacion.py", SERVICIOS, raiz=str(tmp_path))

    assert ejecutor.ruta is None


def test_el_fichero_de_un_servicio_python_no_lleva_ruta(tmp_path: Path) -> None:
    """La suite de un servicio ya está acotada por su propio directorio."""
    raiz = _raiz_con_tests(tmp_path)

    ejecutor = ejecutor_para("services/uno/app/flujo.py", SERVICIOS, raiz=raiz)

    assert ejecutor.ruta is None


def test_la_ruta_acotada_llega_a_la_orden_de_pytest(
    tmp_path: Path, espia: _EspiaSubprocess
) -> None:
    """R1: la ruta no es decorativa; va en la orden con la que corre la suite."""
    EjecutorPytest(raiz=str(tmp_path), ruta="tests").correr(10)

    assert espia.ordenes[-1][-1] == "tests"


def test_la_linea_base_corre_con_la_misma_ruta_acotada(
    tmp_path: Path, espia: _EspiaSubprocess
) -> None:
    """R3: si la base no se acota igual, aborta la campaña antes de empezar."""
    EjecutorPytest(raiz=str(tmp_path), ruta="tests").linea_base(10)

    orden = espia.ordenes[-1]
    assert orden[-1] == "tests"
    for argumento in ARGUMENTOS_LINEA_BASE:
        assert argumento in orden


def test_sin_ruta_la_orden_no_gana_argumentos(
    tmp_path: Path, espia: _EspiaSubprocess
) -> None:
    """R2/R3: sin ruta, la orden es exactamente la de siempre."""
    ejecutor = EjecutorPytest(raiz=str(tmp_path))
    ejecutor.correr(10)

    assert espia.ordenes[-1] == [ejecutor.ejecutable, "-m", "pytest", *ejecutor.argumentos]


def test_la_identidad_distingue_dos_rutas_acotadas(tmp_path: Path) -> None:
    """R4: misma raíz y mismo intérprete, distinta suite: no se deduplican."""
    con_ruta = EjecutorPytest(raiz=str(tmp_path), ruta="tests")
    sin_ruta = EjecutorPytest(raiz=str(tmp_path))

    assert con_ruta.identidad() != sin_ruta.identidad()


def test_dos_ejecutores_identicos_siguen_compartiendo_identidad(
    tmp_path: Path,
) -> None:
    """R4 no puede romper la deduplicación que ahorra líneas base repetidas."""
    uno = EjecutorPytest(raiz=str(tmp_path), ruta="tests")
    otro = EjecutorPytest(raiz=str(tmp_path), ruta="tests")

    assert uno.identidad() == otro.identidad()


def test_los_implicados_no_funden_la_raiz_acotada_con_la_libre(
    tmp_path: Path,
) -> None:
    """R4: la línea base de uno NO vale por la del otro; se corren las dos."""
    mutantes = [
        mutacion.Mutante(
            fichero=fichero,
            linea=1,
            col=0,
            original="a == b",
            mutado="a != b",
            operador="comparacion",
        )
        for fichero in ("harness/uno.py", "harness/dos.py")
    ]
    ejecutores = {
        "harness/uno.py": EjecutorPytest(raiz=str(tmp_path), ruta="tests"),
        "harness/dos.py": EjecutorPytest(raiz=str(tmp_path)),
    }

    implicados = mutacion.ejecutores_implicados(
        mutantes, None, lambda fichero: ejecutores[fichero]
    )

    assert len(implicados) == 2
