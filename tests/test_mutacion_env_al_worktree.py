# tests/test_mutacion_env_al_worktree.py
"""Un worktree no tiene lo que no está versionado, y el `.env` es justo eso.

La campaña paralela reparte los mutantes entre `git worktree` desechables. Un
worktree solo contiene lo que hay en `HEAD`, así que el fichero de entorno local
—no versionado por definición— no aparece por ninguna parte. En un proyecto
cuya configuración se valida al importar, la suite del worker ni arranca: la
línea base sale ROJA sin mutar nada y la campaña aborta con `BaseRota`.

Medido en un proyecto real el 2026-08-26, dentro de un worktree recién creado
desde HEAD: **25 tests rojos, 23 de ellos por la misma causa** —falta el fichero
de entorno, la configuración no valida y el CLI devuelve salida vacía—. Con la
campaña en serie costando ~18 h y la paralela ~4,6 h, la trampa dejaba
inservible el único modo que hace viable la campaña.

El arreglo NO copia el fichero al worktree. Copiarlo escribiría la
configuración local en el temp del sistema, fuera del repositorio y de su
`.gitignore`, y una campaña muerta a machetazos la dejaría ahí. Las variables se
vuelcan al `os.environ` del coordinador antes de crear ningún worktree y los
workers las heredan, porque cada suite se lanza con `env={**os.environ, ...}`.

Ningún test de este fichero toca red ni base de datos: repositorios de juguete
bajo `tmp_path` y ejecutores dobles.
"""

from __future__ import annotations

import os
import subprocess
from collections.abc import Iterator
from pathlib import Path

import pytest

from harness.alcance import Alcance
from harness.mutacion import MUERTO, ResultadoSuite
from harness.mutacion_paralela import (
    FICHERO_ENTORNO,
    ejecutar_campania_paralela,
    parsear_variables,
    volcar_variables,
)

#: Nombre inventado para estos tests. No es de nadie: si estuviera en el entorno
#: real de la máquina, los tests de precedencia probarían lo contrario de lo que
#: dicen probar.
CLAVE = "ARNES_VARIABLE_DE_PRUEBA"

VALOR_DEL_FICHERO = "valor-del-fichero"


@pytest.fixture
def entorno_restaurado() -> Iterator[None]:
    """Devuelve `os.environ` a su estado original al acabar el test.

    `monkeypatch` restaura lo que ÉL toca; las claves que `volcar_variables`
    añade por su cuenta no las conoce, y se quedarían pegadas al proceso de
    pytest contaminando los tests siguientes. Se restaura con `clear` + `update`
    y no reemplazando el objeto, para que cada alta y cada baja pase por
    `putenv`/`unsetenv` y los subprocesos vean lo mismo que este proceso.
    """
    copia = dict(os.environ)
    yield
    os.environ.clear()
    os.environ.update(copia)


def _escribir_env(carpeta: Path, *lineas: str) -> None:
    carpeta.mkdir(parents=True, exist_ok=True)
    (carpeta / FICHERO_ENTORNO).write_text("\n".join(lineas) + "\n", encoding="utf-8")


# --- Lo que el fichero pone, y lo que el entorno ya tenía --------------------


def test_una_variable_del_fichero_llega_al_entorno(
    tmp_path: Path, entorno_restaurado: None
) -> None:
    _escribir_env(tmp_path, f"{CLAVE}={VALOR_DEL_FICHERO}")

    anadidas = volcar_variables(str(tmp_path))

    assert os.environ[CLAVE] == VALOR_DEL_FICHERO
    assert anadidas == [CLAVE]


def test_una_variable_ya_presente_en_el_entorno_NO_se_pisa(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, entorno_restaurado: None
) -> None:
    """El entorno real manda: es la precedencia que espera quien exporta a mano.

    Quien pone una variable en su terminal para una tanda concreta lo hace para
    que valga durante esa tanda. Si el fichero la pisara, el arnés cambiaría en
    silencio la configuración con la que corre la suite.
    """
    monkeypatch.setenv(CLAVE, "el-que-ya-estaba")
    _escribir_env(tmp_path, f"{CLAVE}={VALOR_DEL_FICHERO}")

    anadidas = volcar_variables(str(tmp_path))

    assert os.environ[CLAVE] == "el-que-ya-estaba"
    assert anadidas == [], "una clave que ya estaba no se cuenta como añadida"


def test_una_variable_vacia_en_el_entorno_tampoco_se_pisa(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, entorno_restaurado: None
) -> None:
    """Cadena vacía es un valor, no una ausencia: `in os.environ`, no `if`."""
    monkeypatch.setenv(CLAVE, "")
    _escribir_env(tmp_path, f"{CLAVE}={VALOR_DEL_FICHERO}")

    volcar_variables(str(tmp_path))

    assert os.environ[CLAVE] == ""


def test_solo_se_vuelca_lo_que_faltaba(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, entorno_restaurado: None
) -> None:
    monkeypatch.setenv(f"{CLAVE}_UNO", "el-que-ya-estaba")
    _escribir_env(
        tmp_path,
        f"{CLAVE}_UNO=del-fichero",
        f"{CLAVE}_DOS=del-fichero",
        f"{CLAVE}_TRES=del-fichero",
    )

    anadidas = volcar_variables(str(tmp_path))

    assert anadidas == [f"{CLAVE}_DOS", f"{CLAVE}_TRES"]
    assert os.environ[f"{CLAVE}_UNO"] == "el-que-ya-estaba"
    assert os.environ[f"{CLAVE}_DOS"] == "del-fichero"


# --- Sin fichero no pasa nada ------------------------------------------------


def test_sin_fichero_de_entorno_la_campania_sigue_como_siempre(
    tmp_path: Path, entorno_restaurado: None
) -> None:
    """Ni excepción ni ruido: la inmensa mayoría de proyectos no tiene `.env`."""
    assert volcar_variables(str(tmp_path)) == []


def test_un_fichero_de_entorno_ilegible_tampoco_rompe_nada(
    tmp_path: Path, entorno_restaurado: None
) -> None:
    """Un directorio con ese nombre es la forma barata de provocar el `OSError`.

    Da igual el motivo —permisos, un enlace roto, un directorio—: volcar
    variables es una comodidad, y ninguna comodidad puede tumbar una campaña de
    horas. La línea base sigue estando ahí para delatar lo que falte.
    """
    (tmp_path / FICHERO_ENTORNO).mkdir()

    assert volcar_variables(str(tmp_path)) == []


# --- El parseo, línea a línea ------------------------------------------------


@pytest.mark.parametrize(
    ("linea", "esperado"),
    [
        ("CLAVE=valor", {"CLAVE": "valor"}),
        ("  CLAVE  =  valor  ", {"CLAVE": "valor"}),
        ("export CLAVE=valor", {"CLAVE": "valor"}),
        ("export   CLAVE=valor", {"CLAVE": "valor"}),
        ('CLAVE="valor con espacios"', {"CLAVE": "valor con espacios"}),
        ("CLAVE='valor'", {"CLAVE": "valor"}),
        ('CLAVE="', {"CLAVE": '"'}),
        ("CLAVE=", {"CLAVE": ""}),
        # Las comillas que envuelven un valor VACIO tambien se quitan: son dos
        # caracteres y el valor de dentro es la cadena vacia. Lo caza la campana
        # de mutacion de F-047 en datamart-seg-anual: con `len(valor) > 2` en
        # vez de `>= 2`, un `CLAVE=""` acababa valiendo literalmente `""`, dos
        # comillas, y esa variable se exportaba asi al entorno del worker.
        ('CLAVE=""', {"CLAVE": ""}),
        ("CLAVE=''", {"CLAVE": ""}),
        ("CLAVE=uno=dos", {"CLAVE": "uno=dos"}),
        ("CLAVE=contra#seña", {"CLAVE": "contra#seña"}),
        ("# CLAVE=valor", {}),
        ("#CLAVE=valor", {}),
        ("   # comentario indentado", {}),
        ("", {}),
        ("      ", {}),
        ("CLAVE", {}),
        ("=valor", {}),
        ("esto no es una asignación", {}),
        ("CLAVE CON ESPACIOS=valor", {}),
    ],
    ids=lambda caso: repr(caso),
)
def test_el_parseo_entiende_las_lineas_raras(
    linea: str, esperado: dict[str, str]
) -> None:
    """Un `#` DENTRO de un valor no es un comentario: puede ser una contraseña.

    Por eso solo se descartan los comentarios de línea entera. Y una comilla
    suelta se queda tal cual: solo se quitan si envuelven el valor entero.
    """
    assert parsear_variables(linea) == esperado


def test_un_fichero_entero_se_parsea_en_orden_saltandose_la_paja() -> None:
    texto = (
        "# configuración local, no versionada\n"
        "\n"
        "PRIMERA=uno\n"
        "   \n"
        "export SEGUNDA = 'dos'\n"
        "# TERCERA=no\n"
        'CUARTA="cuatro"\n'
    )

    assert parsear_variables(texto) == {
        "PRIMERA": "uno",
        "SEGUNDA": "dos",
        "CUARTA": "cuatro",
    }


def test_la_ultima_definicion_de_una_clave_manda() -> None:
    """Como en cualquier fichero de este tipo: lo de abajo pisa a lo de arriba."""
    assert parsear_variables("CLAVE=uno\nCLAVE=dos\n") == {"CLAVE": "dos"}


# --- La garantía de seguridad: el fichero NO viaja al worktree ---------------

FUENTE = "def clasifica(a, b):\n    if a == b:\n        return a > b\n    return None\n"


class EjecutorDoble:
    """Ejecutor de un worker: mata todo mutante y apunta qué veía al hacerlo."""

    def __init__(self, raiz: str, visitas: list[dict[str, object]]) -> None:
        self.raiz = raiz
        self._visitas = visitas

    def linea_base(self, _timeout_s: int) -> ResultadoSuite:
        self._anotar()
        return ResultadoSuite(codigo=0)

    def ejecutar(self, _timeout_s: int) -> str:
        self._anotar()
        return MUERTO

    def _anotar(self) -> None:
        self._visitas.append(
            {
                "raiz": self.raiz,
                "fichero_en_disco": (Path(self.raiz) / FICHERO_ENTORNO).exists(),
                "variable": os.environ.get(CLAVE),
            }
        )


@pytest.fixture
def repo(tmp_path: Path) -> tuple[Alcance, str]:
    """Repositorio de juguete con código mutable y un fichero de entorno local.

    El `.gitignore` no es decoración: sin él, el fichero no versionado saldría
    en `git status --porcelain` y la campaña paralela se negaría a arrancar por
    árbol sucio antes de llegar a lo que este test mide.
    """

    def _git(*args: str) -> None:
        subprocess.run(
            ["git", "-C", str(tmp_path), *args], capture_output=True, check=True
        )

    (tmp_path / "codigo.py").write_text(FUENTE, encoding="utf-8")
    (tmp_path / ".gitignore").write_text(f"{FICHERO_ENTORNO}\n", encoding="utf-8")
    _escribir_env(tmp_path, f"{CLAVE}={VALOR_DEL_FICHERO}")
    _git("init", "-q")
    _git("config", "user.email", "arnes@ejemplo.invalid")
    _git("config", "user.name", "Arnes")
    _git("add", "codigo.py", ".gitignore")
    _git("commit", "-q", "-m", "base")
    alcance = Alcance(
        feature="F-000",
        origen="rama",
        ref_diff=("dev", "feature/x"),
        lineas={"codigo.py": {2, 3}},
    )
    return (alcance, str(tmp_path))


@pytest.fixture
def visitas(
    repo: tuple[Alcance, str], entorno_restaurado: None
) -> list[dict[str, object]]:
    """Una campaña paralela DE VERDAD, con dos worktrees, y lo que vio cada worker.

    Se corre una sola vez y la comprueban varios tests: crear worktrees cuesta,
    y lo que se mira son tres propiedades del mismo hecho.
    """
    alcance, raiz = repo
    anotadas: list[dict[str, object]] = []

    informe = ejecutar_campania_paralela(
        alcance,
        servicios=[],
        timeout_s=5,
        raiz=raiz,
        workers=2,
        fabrica=lambda _fichero, raiz_worker: EjecutorDoble(raiz_worker, anotadas),
    )

    assert informe.workers == 2, (
        "sin dos worktrees de verdad este escenario no prueba nada"
    )
    return anotadas


def test_ningun_worktree_recibe_una_copia_del_fichero_de_entorno(
    visitas: list[dict[str, object]], repo: tuple[Alcance, str]
) -> None:
    """LA garantía: los secretos no se escriben en el temp del sistema.

    Copiar el fichero al worktree sería la solución de una línea, y es la que se
    descartó: el temp está fuera del repositorio y de su `.gitignore`, y una
    campaña que muera a machetazos dejaría ahí la configuración local. Si alguien
    «arregla» el arranque de los workers copiando el fichero, este test se pone
    rojo.
    """
    _, raiz = repo
    con_fichero = [visita["raiz"] for visita in visitas if visita["fichero_en_disco"]]

    assert con_fichero == [], (
        f"el fichero de entorno ha aparecido dentro de un worktree: {con_fichero}"
    )
    assert (Path(raiz) / FICHERO_ENTORNO).is_file(), (
        "el del árbol principal no se toca: ni se copia ni se mueve"
    )


def test_los_workers_juzgan_en_worktrees_y_no_en_el_arbol_principal(
    visitas: list[dict[str, object]], repo: tuple[Alcance, str]
) -> None:
    _, raiz = repo
    raices = {visita["raiz"] for visita in visitas}

    assert len(raices) == 2, f"se esperaban dos worktrees distintos, hay {raices}"
    assert raiz not in raices


def test_la_variable_del_arbol_principal_esta_puesta_mientras_corren_los_workers(
    visitas: list[dict[str, object]],
) -> None:
    """Lo que hereda el subproceso de cada suite es este `os.environ`."""
    assert {visita["variable"] for visita in visitas} == {VALOR_DEL_FICHERO}


def test_el_volcado_ocurre_ANTES_de_crear_ningun_worktree(
    repo: tuple[Alcance, str],
    entorno_restaurado: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Volcar después no serviría: la línea base corre nada más nacer el worktree.

    Cada worker mide su línea base dentro de su worktree antes de juzgar a nadie,
    así que si las variables llegaran tarde la campaña ya habría abortado con
    `BaseRota`.
    """
    from harness import mutacion_paralela

    alcance, raiz = repo
    al_crear: list[str | None] = []
    original = mutacion_paralela.Worktrees

    class WorktreesEspia(original):  # type: ignore[valid-type, misc]
        def __enter__(self) -> list[str]:
            al_crear.append(os.environ.get(CLAVE))
            return super().__enter__()

    monkeypatch.setattr(mutacion_paralela, "Worktrees", WorktreesEspia)

    ejecutar_campania_paralela(
        alcance,
        servicios=[],
        timeout_s=5,
        raiz=raiz,
        workers=2,
        fabrica=lambda _fichero, raiz_worker: EjecutorDoble(raiz_worker, []),
    )

    assert al_crear == [VALOR_DEL_FICHERO]
