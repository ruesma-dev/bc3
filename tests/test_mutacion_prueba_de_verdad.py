# tests/test_mutacion_prueba_de_verdad.py
"""La prueba de verdad: reproducir el defecto, no describirlo.

`test_mutacion_linea_base.py` comprueba el MENSAJE de aborto con dobles de
test. Eso no basta, y la razón está escrita en el propio encargo: hasta la
1.5.2 la limitación estaba **documentada** en la docstring de
`harness/mutacion_paralela.py` y nada la comprobaba. Un aviso en un docstring
no es una salvaguarda; un test con un doble tampoco demuestra que el escenario
real acabe donde se cree.

Aquí se monta un repositorio git de juguete, con su suite de verdad y pytest de
verdad, y se mide la MISMA campaña con el comportamiento de antes y con el de
ahora. Dos escenarios, los dos vistos en producción:

A · **Paralelo, fichero no versionado** (datamart-seg-anual, 2026-08-19). Un
   test depende de un fichero que `.gitignore` excluye. Dentro del `git
   worktree` que crea cada worker ese fichero no existe, la suite arranca roja
   y TODO mutante sale «muerto».

B · **Serie, recolección rota** (albaranes, 2026-08-19). Un fichero que no
   pertenece a ningún servicio lo juzga `ejecutor_para` con `python -m pytest`
   **sin ruta**; sin configuración de pytest en la raíz, esa invocación recoge
   lo que no debe y revienta en la recolección haga lo que haga el mutante.
   Exit 2, que hasta la 1.5.2 se contaba como MUERTO. Este escenario NO
   necesita worktrees: afecta también a las campañas en serie, y por eso el
   alcance del daño es mayor de lo que decía el encargo.

C · **Bytecode rancio** (descubierto AQUÍ, montando este fichero: el escenario
   A daba 3 supervivientes o 2 según el segundo en que cayera). CPython
   reutiliza un `.pyc` cuando el fuente conserva tamaño y mtime en segundos
   enteros, y dos mutantes consecutivos del mismo fichero cumplen las dos cosas
   con facilidad. El segundo se juzga con el bytecode del primero.

Los escenarios se miden por partida doble: CONTROL (lo que la campaña debería
decir), ANTES (lo que decía la 1.5.2) y DESPUÉS (lo que dice la 1.6.0). La
comparación es la evidencia; sin ella no se puede afirmar que esté arreglado.
"""

from __future__ import annotations

import os
import subprocess
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import pytest
from harness import mutacion_paralela
from harness.alcance import Alcance
from harness.mutacion import (
    MUERTO,
    PYTEST_OK,
    PYTEST_SIN_TESTS,
    SUPERVIVIENTE,
    TIMEOUT,
    BaseRota,
    EjecutorPytest,
    _escribir,
    _leer,
    ejecutar_campania,
)
from harness.mutacion_paralela import ejecutar_campania_paralela

#: Segundos por mutante. La suite de juguete tarda menos de un segundo; con
#: este margen un cuelgue se nota como timeout en vez de colgar la suite real.
TIMEOUT_S = 90

#: Código bajo mutación. `descuento` la cubren los tests; `etiqueta` no la
#: cubre nadie, a propósito: sus mutantes SOBREVIVEN, y ese superviviente es lo
#: que el defecto hacía desaparecer.
APP = '''# app.py
def descuento(importe, ratio):
    if importe > 100:
        return importe * ratio
    return importe


def etiqueta(cantidad):
    if cantidad > 5:
        return "muchos"
    return "pocos"
'''

#: Suite que depende de un fichero NO versionado, como el `.env` de un
#: proyecto real. En el árbol principal existe; dentro de un worktree, no.
TEST_APP = '''# test_app.py
from pathlib import Path

import app

RAIZ = Path(__file__).resolve().parent


def test_hay_datos_locales():
    assert (RAIZ / "datos.local").is_file()


def test_descuento_aplica_el_ratio_por_encima_del_umbral():
    assert app.descuento(200, 0.5) == 100.0


def test_por_debajo_del_umbral_no_hay_descuento():
    assert app.descuento(100, 0.5) == 100
'''

#: Fichero de test que ni siquiera se puede recoger: pytest sale con código 2
#: sin ejecutar nada. Es el escenario B en su forma mínima.
TEST_IRRECOLECTABLE = '''# tests/test_irrecolectable.py
import modulo_que_no_existe  # noqa: F401


def test_nunca_llega_a_correr():
    assert True
'''

#: Líneas de `app.py` que entran en el alcance de la campaña: las cuatro que
#: contienen operadores mutables.
LINEAS_EN_ALCANCE = {3, 4, 8, 9}


def _git(raiz: Path, *args: str) -> None:
    subprocess.run(["git", "-C", str(raiz), *args], check=True, capture_output=True)


def _repositorio(destino: Path, con_suite_rota: bool = False) -> Path:
    """Repositorio de juguete: código, suite, `.gitignore` y un commit."""
    destino.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        ["git", "init", "-q", "-b", "dev", str(destino)], check=True, capture_output=True
    )
    _git(destino, "config", "user.email", "arnes@ruesma.es")
    _git(destino, "config", "user.name", "Arnes")

    (destino / "app.py").write_text(APP, encoding="utf-8", newline="")
    (destino / "test_app.py").write_text(TEST_APP, encoding="utf-8", newline="")
    # El fichero del que depende un test queda FUERA de git, igual que un
    # `.env`. Ignorado, además, para que el árbol se vea limpio: si saliera
    # como `??`, la campaña paralela abortaría por otro motivo y el escenario
    # que se quiere reproducir no llegaría a darse.
    (destino / ".gitignore").write_text("datos.local\n", encoding="utf-8")
    if con_suite_rota:
        (destino / "tests").mkdir()
        (destino / "tests" / "test_irrecolectable.py").write_text(
            TEST_IRRECOLECTABLE, encoding="utf-8"
        )
    _git(destino, "add", "-A")
    _git(destino, "commit", "-q", "-m", "inicial")

    (destino / "datos.local").write_text("dato local\n", encoding="utf-8")
    return destino


def _alcance() -> Alcance:
    return Alcance(
        feature="F-999",
        origen="rama",
        ref_diff=("dev", "feature/F-999-juguete"),
        lineas={"app.py": set(LINEAS_EN_ALCANCE)},
    )


@contextmanager
def arnes_1_5_2() -> Iterator[None]:
    """Devuelve el mutador al comportamiento de la 1.5.2, para medir el ANTES.

    Deshace exactamente lo que la 1.6.0 añade en este camino y nada más:

    1. el veredicto vuelve a ser binario —cualquier suite que no acabe en
       verde es un mutante muerto, sin preguntarse por qué—;
    2. la campaña deja de correr la línea base.

    Hace falta porque el código de hoy **se niega** a producir el número falso:
    sin volver atrás no hay ANTES con el que comparar, y una comparación contra
    un recuerdo no es una comparación.
    """
    ejecutar_original = EjecutorPytest.ejecutar
    campania_original = mutacion_paralela.ejecutar_campania

    def ejecutar_binario(self: EjecutorPytest, timeout_s: int) -> str:
        resultado = self.correr(timeout_s)
        if resultado.expirado:
            return TIMEOUT
        if resultado.codigo in (PYTEST_OK, PYTEST_SIN_TESTS):
            return SUPERVIVIENTE
        return MUERTO

    def campania_sin_linea_base(*args: object, **kwargs: object) -> object:
        kwargs["comprobar_base"] = False
        return campania_original(*args, **kwargs)  # type: ignore[arg-type]

    EjecutorPytest.ejecutar = ejecutar_binario  # type: ignore[method-assign]
    mutacion_paralela.ejecutar_campania = campania_sin_linea_base  # type: ignore[assignment]
    try:
        yield
    finally:
        EjecutorPytest.ejecutar = ejecutar_original  # type: ignore[method-assign]
        mutacion_paralela.ejecutar_campania = campania_original  # type: ignore[assignment]


def _entorno_que_escribe_bytecode() -> dict[str, str]:
    """Entorno para los subprocesos que TIENEN que dejar `.pyc` en disco.

    El escenario C necesita que el intérprete hijo escriba bytecode; si no lo
    escribe, no hay `.pyc` rancio que reutilizar y el test no prueba nada. Y
    quién lo escribe no lo decide el test: lo decide **quien invoca a pytest**,
    porque `PYTHONDONTWRITEBYTECODE` se hereda.

    Eso reventó el 2026-08-26 en `datamart-seg-anual` de la peor manera
    posible: la campaña de mutación lanza cada evaluación con
    `PYTHONDONTWRITEBYTECODE=1` —es el arreglo de la 1.6.3, el que evita
    envenenar el árbol—, y la línea base que mide antes de mutar corre la suite
    del proyecto, este test incluido. Resultado: línea base SIEMPRE en rojo y
    campaña abortada sin informe. El test que vigila el bytecode no sobrevivía
    a la propia campaña del arnés.

    Por eso el entorno del hijo se construye aquí y no se hereda tal cual: se
    parte de `os.environ` —el subproceso necesita `PATH` y compañía— y se
    quitan las dos variables que deciden dónde y si se escribe bytecode.
    Quitarlas es lo contrario de debilitar el test: es lo que garantiza que
    haya `.pyc` que comprobar en cualquier entorno.
    """
    entorno = dict(os.environ)
    entorno.pop("PYTHONDONTWRITEBYTECODE", None)
    # Si apunta a otro árbol, el `__pycache__` no aparece junto al fuente y el
    # `glob` sale vacío igual que con la anterior.
    entorno.pop("PYTHONPYCACHEPREFIX", None)
    return entorno


def _sin_mutar(repositorio: Path) -> None:
    """El árbol quedó como estaba: ni un mutante escrito en disco."""
    assert _leer(repositorio / "app.py") == APP, (
        "la campaña dejó app.py MUTADO: es el defecto que arregla la pieza 3"
    )


# --- Escenario A · paralelo con un fichero no versionado --------------------


def test_A_control_en_serie_hay_supervivientes(tmp_path: Path) -> None:
    """Lo que la campaña DEBE decir: `etiqueta` no la comprueba nadie.

    En el árbol principal `datos.local` existe, así que la base está verde y
    los números salen bien. Este es el `--workers 1` del encargo: el que se
    demostró bueno.
    """
    repositorio = _repositorio(tmp_path / "juguete")

    informe = ejecutar_campania(
        _alcance(),
        EjecutorPytest(raiz=str(repositorio), ejecutable=sys.executable),
        timeout_s=TIMEOUT_S,
        raiz=str(repositorio),
    )

    assert informe.generados == 5
    assert len(informe.supervivientes) == 3, [
        m.descripcion() for m in informe.supervivientes
    ]
    assert informe.muertos == 2
    assert informe.fiable
    _sin_mutar(repositorio)


def test_A_antes_la_campania_paralela_daba_cero_supervivientes_falsos(
    tmp_path: Path,
) -> None:
    """El defecto, reproducido: mismo árbol, cero supervivientes.

    Los tres supervivientes que el control acaba de demostrar desaparecen. No
    porque ningún test los cace —ninguno lo hace—, sino porque la suite del
    worker ya estaba roja: le falta `datos.local`, que no está versionado.
    """
    repositorio = _repositorio(tmp_path / "juguete")

    with arnes_1_5_2():
        informe = ejecutar_campania_paralela(
            _alcance(),
            [],
            timeout_s=TIMEOUT_S,
            raiz=str(repositorio),
            workers=2,
        )

    assert informe.generados == 5
    assert informe.muertos == 5, "el ANTES daba TODOS los mutantes por muertos"
    assert informe.supervivientes == [], "el cero falso, medido"
    # Y lo peor: el informe no tenía ni una pega que ponerle.
    assert informe.fiable
    _sin_mutar(repositorio)


def test_A_despues_la_campania_paralela_aborta_y_dice_por_que(tmp_path: Path) -> None:
    """El arreglo: la línea base del worktree no está verde, así que no se
    juzga a nadie y el mensaje nombra el test que falla."""
    repositorio = _repositorio(tmp_path / "juguete")

    with pytest.raises(BaseRota) as caido:
        ejecutar_campania_paralela(
            _alcance(),
            [],
            timeout_s=TIMEOUT_S,
            raiz=str(repositorio),
            workers=2,
        )

    mensaje = str(caido.value)
    assert "LÍNEA BASE EN ROJO" in mensaje
    assert "test_app.py::test_hay_datos_locales" in mensaje
    assert "no existen dentro de un git worktree" in mensaje
    assert "--workers 1" in mensaje
    _sin_mutar(repositorio)


# --- Escenario B · serie con la recolección rota ----------------------------


def test_B_antes_una_recoleccion_rota_mataba_a_todo_el_mundo(tmp_path: Path) -> None:
    """El tercer caso, en serie y sin worktrees de por medio.

    `python -m pytest` sin ruta revienta en la recolección (exit 2) haga lo que
    haga el mutante. La 1.5.2 traducía cualquier salida distinta de 0 y 5 a
    MUERTO, así que la campaña entera salía «sin supervivientes» en segundos
    sin que un solo test juzgara nada. Pasó con los 19 mutantes de F-034 en
    albaranes y con los 61 de F-012, todos sobre ficheros de `harness/`.
    """
    repositorio = _repositorio(tmp_path / "juguete", con_suite_rota=True)

    with arnes_1_5_2():
        informe = ejecutar_campania(
            _alcance(),
            EjecutorPytest(raiz=str(repositorio), ejecutable=sys.executable),
            timeout_s=TIMEOUT_S,
            raiz=str(repositorio),
            comprobar_base=False,
        )

    assert informe.muertos == 5
    assert informe.supervivientes == []
    _sin_mutar(repositorio)


def test_C_un_mutante_nunca_se_juzga_con_el_bytecode_del_anterior(
    tmp_path: Path,
) -> None:
    """El tercer camino hacia un muerto que nadie mató: el `.pyc` rancio.

    Este test salió de una discrepancia real entre dos ejecuciones de
    `test_A_control...`: la misma campaña sobre el mismo repositorio daba 3
    supervivientes o 2 según el segundo en que cayera. El que desaparecía se
    contaba MUERTO sin que ningún test lo tocara, porque CPython reutilizaba el
    bytecode del mutante anterior —mismo tamaño de fuente, mismo mtime en
    segundos enteros—.

    Aquí se fuerza esa coincidencia con `os.utime` en vez de esperar a que el
    reloj la regale, que es lo que la hace reproducible. Y los dos subprocesos
    parten del entorno que fija `_entorno_que_escribe_bytecode()`, no del que
    traiga quien invoque a pytest: ver allí por qué.
    """
    entorno = _entorno_que_escribe_bytecode()

    modulo = tmp_path / "m.py"
    _escribir(modulo, "VALOR = 1\n")
    subprocess.run(
        [sys.executable, "-c", "import m"],
        cwd=tmp_path,
        check=True,
        capture_output=True,
        env=entorno,
    )
    assert list((tmp_path / "__pycache__").glob("m.*.pyc")), (
        "el intérprete no dejó bytecode: este test no estaría probando nada"
    )
    antes = modulo.stat()

    _escribir(modulo, "VALOR = 9\n")  # mismo tamaño EXACTO que el anterior
    os.utime(modulo, (antes.st_atime, antes.st_mtime))  # y mismo mtime

    salida = subprocess.run(
        [sys.executable, "-c", "import m; print(m.VALOR)"],
        cwd=tmp_path,
        check=True,
        capture_output=True,
        text=True,
        env=entorno,
    ).stdout.strip()

    assert salida == "9", (
        "se ejecutó el bytecode rancio y no el fuente que hay en disco: un "
        "mutante juzgado así sale muerto sin que ningún test lo cace"
    )


def test_C_regresion_el_escenario_del_bytecode_aguanta_la_variable_puesta() -> None:
    """El escenario C, corrido con `PYTHONDONTWRITEBYTECODE=1` puesta.

    Esta es la regresión del defecto de la 1.7.6, y se ejecuta de la única
    forma que demuestra algo: relanzando el test C en un subproceso con la
    variable envenenada, exactamente como se la pone la campaña de mutación al
    medir su línea base. Si vuelve a salir en rojo, es que alguien ha dejado el
    escenario C a merced del entorno que herede.

    No vale con comprobar el código fuente ni con marcar el test como `skip`
    cuando la variable esté puesta: saltárselo apagaría justo el test que
    protege el arreglo de la 1.6.3, y entonces la campaña arrancaría verde
    sobre un árbol envenenado sin que nadie lo notara.
    """
    raiz = Path(__file__).resolve().parents[1]
    fichero = Path(__file__).resolve().relative_to(raiz).as_posix()
    caso = test_C_un_mutante_nunca_se_juzga_con_el_bytecode_del_anterior.__name__

    completado = subprocess.run(
        # `-m pytest` y no el ejecutable: es lo que mete la raíz en `sys.path`
        # y lo que hace que `from harness import ...` importe aquí dentro.
        # `-p no:cacheprovider` para no ensuciar `.pytest_cache` del anidado.
        [
            sys.executable,
            "-m",
            "pytest",
            f"{fichero}::{caso}",
            "-q",
            "-p",
            "no:cacheprovider",
        ],
        cwd=raiz,
        env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
        capture_output=True,
        text=True,
        timeout=TIMEOUT_S,
        check=False,  # el veredicto lo da el assert, con la salida delante
    )

    assert completado.returncode == 0, (
        "el escenario C falla con PYTHONDONTWRITEBYTECODE puesta, que es como "
        "lo corre la campaña de mutación al medir la línea base: la campaña "
        "aborta sin escribir informe y ninguna feature se puede cerrar.\n"
        f"{completado.stdout}\n{completado.stderr}"
    )


def test_B_despues_una_recoleccion_rota_aborta_la_campania(tmp_path: Path) -> None:
    """El mismo árbol, con la 1.6.0: no se cuenta ni un muerto."""
    repositorio = _repositorio(tmp_path / "juguete", con_suite_rota=True)

    with pytest.raises(BaseRota) as caido:
        ejecutar_campania(
            _alcance(),
            EjecutorPytest(raiz=str(repositorio), ejecutable=sys.executable),
            timeout_s=TIMEOUT_S,
            raiz=str(repositorio),
        )

    mensaje = str(caido.value)
    assert "LÍNEA BASE EN ROJO" in mensaje
    # Que el aborto NOMBRE lo que falla es la mitad del arreglo: «código 2» a
    # secas manda al humano a adivinar, y adivinar cuesta una tarde.
    assert "tests/test_irrecolectable.py" in mensaje
    _sin_mutar(repositorio)
