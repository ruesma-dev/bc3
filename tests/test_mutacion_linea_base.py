# tests/test_mutacion_linea_base.py
"""Una campaña de mutación no puede contar muertos sobre una suite ya rota.

El 2026-08-19, en `datamart-seg-anual`, la misma feature y el mismo árbol
dieron `108 generados, 108 muertos, 0 supervivientes` en modo paralelo y
`108, 106, 2 supervivientes` con `--workers 1`. Los dos supervivientes eran
`bold=True -> bold=False` en dos `click.secho`; ningún test menciona `bold`,
así que no podían morir: el cero era falso.

La causa: dentro de un `git worktree` la suite del proyecto ya arrancaba roja
—faltaba `.env`, que no está versionado— y `EjecutorPytest.ejecutar` contaba
CUALQUIER suite roja como mutante muerto. La ironía es que la docstring de
`harness/mutacion_paralela.py` ya predecía este fallo entre sus «limitaciones
conocidas»: estaba escrito y nada lo comprobaba.

Estos tests son ese «nada» convertido en algo.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest
from harness.alcance import Alcance
from harness.mutacion import (
    BASE_ROTA,
    INDETERMINADO,
    MUERTO,
    PYTEST_FALLOS,
    PYTEST_OK,
    PYTEST_SIN_TESTS,
    SUPERVIVIENTE,
    TIMEOUT,
    ArbolSucio,
    BaseRota,
    Centinela,
    EjecutorPytest,
    Mutante,
    ResultadoSuite,
    _leer,
    ejecutar_campania,
    ficheros_con_cambios,
    guardia_arbol_limpio,
    restaurar_desde_centinela,
)

FUENTE = "# app.py\ndef bandera():\n    return True\n"


# --- Dobles de test ---------------------------------------------------------


class EjecutorFalso:
    """Ejecutor con el guion escrito: qué devuelve la suite y qué la base.

    Reproduce la interfaz que `ejecutar_campania` espera de un ejecutor real
    (`raiz`, `identidad`, `ejecutar`, `linea_base`) sin lanzar ni un proceso.
    """

    def __init__(
        self,
        veredictos: list[str] | None = None,
        bases: list[ResultadoSuite] | None = None,
        raiz: str = ".",
    ) -> None:
        self.raiz = raiz
        self._veredictos = list(veredictos or [])
        self._bases = list(bases or [])
        self.veredicto_por_defecto = MUERTO
        self.base_por_defecto = ResultadoSuite(codigo=PYTEST_OK)
        self.corridas_base = 0

    def identidad(self) -> tuple[str, str]:
        return (self.raiz, "falso")

    def ejecutar(self, timeout_s: int) -> str:  # noqa: ARG002
        if self._veredictos:
            return self._veredictos.pop(0)
        return self.veredicto_por_defecto

    def linea_base(self, timeout_s: int) -> ResultadoSuite:  # noqa: ARG002
        self.corridas_base += 1
        if self._bases:
            return self._bases.pop(0)
        return self.base_por_defecto


@pytest.fixture
def arbol(tmp_path: Path) -> Path:
    """Repositorio git de un solo fichero, commiteado y limpio."""
    subprocess.run(["git", "init", "-q", "-b", "dev", str(tmp_path)], check=True)
    subprocess.run(
        ["git", "-C", str(tmp_path), "config", "user.email", "arnes@ruesma.es"], check=True
    )
    subprocess.run(
        ["git", "-C", str(tmp_path), "config", "user.name", "Arnes"], check=True
    )
    (tmp_path / "app.py").write_text(FUENTE, encoding="utf-8", newline="")
    subprocess.run(["git", "-C", str(tmp_path), "add", "-A"], check=True)
    subprocess.run(
        ["git", "-C", str(tmp_path), "commit", "-q", "-m", "inicial"], check=True
    )
    return tmp_path


def _alcance() -> Alcance:
    return Alcance(
        feature="F-999",
        origen="rama",
        ref_diff=("dev", "feature/F-999-prueba"),
        lineas={"app.py": {3}},
    )


# --- 1. La línea base aborta la campaña en vez de contar muertos ------------


def test_una_base_roja_aborta_en_vez_de_contar_muertos(arbol: Path) -> None:
    """El defecto exacto del 2026-08-19, en su forma mínima."""
    ejecutor = EjecutorFalso(bases=[ResultadoSuite(codigo=PYTEST_FALLOS)])
    ejecutor.veredicto_por_defecto = MUERTO  # como pasaba: todo «muerto»

    with pytest.raises(BaseRota):
        ejecutar_campania(_alcance(), ejecutor, raiz=str(arbol))


def test_el_aborto_nombra_los_tests_que_fallan_y_las_causas(arbol: Path) -> None:
    """Un aborto sin nombres obliga a adivinar, y adivinar cuesta una tarde."""
    salida = (
        "FAILED tests/test_env.py::test_hay_env - AssertionError\n"
        "FAILED tests/test_rama.py::test_rama_actual - AssertionError\n"
    )
    ejecutor = EjecutorFalso(
        bases=[ResultadoSuite(codigo=PYTEST_FALLOS, salida=salida)]
    )

    with pytest.raises(BaseRota) as caido:
        ejecutar_campania(_alcance(), ejecutor, raiz=str(arbol))

    mensaje = str(caido.value)
    assert "tests/test_env.py::test_hay_env" in mensaje
    assert "tests/test_rama.py::test_rama_actual" in mensaje
    assert "no existen dentro de un git worktree" in mensaje  # ficheros no versionados
    assert "detached HEAD" in mensaje
    assert "editable" in mensaje
    assert "--workers 1" in mensaje


def test_la_base_se_comprueba_antes_de_tocar_un_solo_fichero(arbol: Path) -> None:
    ejecutor = EjecutorFalso(bases=[ResultadoSuite(codigo=PYTEST_FALLOS)])

    with pytest.raises(BaseRota):
        ejecutar_campania(_alcance(), ejecutor, raiz=str(arbol))

    assert _leer(arbol / "app.py") == FUENTE


def test_una_base_sin_tests_que_recoger_no_es_una_base_rota(arbol: Path) -> None:
    """No hay suite que romper: la campaña ya responde con supervivientes."""
    ejecutor = EjecutorFalso(
        bases=[ResultadoSuite(codigo=PYTEST_SIN_TESTS)], veredictos=[SUPERVIVIENTE]
    )

    informe = ejecutar_campania(_alcance(), ejecutor, raiz=str(arbol))

    assert len(informe.supervivientes) == 1


def test_con_la_base_verde_la_campania_cuenta_como_siempre(arbol: Path) -> None:
    ejecutor = EjecutorFalso(veredictos=[SUPERVIVIENTE])

    informe = ejecutar_campania(_alcance(), ejecutor, raiz=str(arbol))

    assert informe.muertos == 0
    assert len(informe.supervivientes) == 1
    assert informe.fiable


def test_la_base_solo_se_corre_una_vez_por_suite_distinta(arbol: Path) -> None:
    """En un monorepo, varios ficheros del mismo servicio comparten línea base."""
    unico = EjecutorFalso()
    alcance = Alcance(
        feature="F-999",
        origen="rama",
        ref_diff=("dev", "rama"),
        lineas={"app.py": {3}},
    )
    ejecutar_campania(
        alcance, unico, raiz=str(arbol), ejecutor_de=lambda _fichero: unico
    )

    # Una al arrancar y una al cerrar: ni una más por mutante.
    assert unico.corridas_base == 2


# --- 2. El veredicto deja de ser binario ------------------------------------


def test_un_mutante_sobre_base_rota_no_cuenta_como_muerto(arbol: Path) -> None:
    """La base estaba verde al arrancar y se rompe a mitad de campaña."""
    ejecutor = EjecutorFalso(
        # base al arrancar: verde. Luego la suite se rompe por su cuenta.
        bases=[
            ResultadoSuite(codigo=PYTEST_OK),  # arranque
            ResultadoSuite(codigo=PYTEST_FALLOS),  # confirmación del indeterminado
            ResultadoSuite(codigo=PYTEST_FALLOS),  # cierre
        ],
        veredictos=[INDETERMINADO],
    )

    informe = ejecutar_campania(_alcance(), ejecutor, raiz=str(arbol))

    assert informe.muertos == 0, "una base rota no mata a nadie"
    assert len(informe.base_rota) == 1
    assert not informe.fiable
    assert informe.aviso_base is not None


def test_si_la_base_sigue_verde_el_mutante_si_esta_muerto(arbol: Path) -> None:
    """Un mutante puede reventar la recolección: eso es una muerte legítima."""
    ejecutor = EjecutorFalso(
        bases=[ResultadoSuite(codigo=PYTEST_OK)] * 3, veredictos=[INDETERMINADO]
    )

    informe = ejecutar_campania(_alcance(), ejecutor, raiz=str(arbol))

    assert informe.muertos == 1
    assert informe.base_rota == []
    assert informe.fiable


def test_la_base_rota_al_cerrar_invalida_la_campania_entera(arbol: Path) -> None:
    ejecutor = EjecutorFalso(
        bases=[ResultadoSuite(codigo=PYTEST_OK), ResultadoSuite(codigo=PYTEST_FALLOS)],
        veredictos=[MUERTO],
    )

    informe = ejecutar_campania(_alcance(), ejecutor, raiz=str(arbol))

    assert informe.muertos == 1  # el número está, pero...
    assert not informe.fiable  # ...el informe dice que no vale
    assert "NO valen para cerrar una feature" in (informe.aviso_base or "")


def test_el_ejecutor_traduce_los_codigos_de_pytest_uno_a_uno() -> None:
    """Solo el 1 —«han fallado tests»— significa que el mutante pudo morir."""
    ejecutor = EjecutorPytest()
    traducciones = {
        PYTEST_OK: SUPERVIVIENTE,
        PYTEST_SIN_TESTS: SUPERVIVIENTE,
        PYTEST_FALLOS: MUERTO,
        2: INDETERMINADO,  # recolección interrumpida
        3: INDETERMINADO,  # error interno de pytest
        4: INDETERMINADO,  # mal uso de la línea de órdenes
    }
    for codigo, esperado in traducciones.items():
        ejecutor.correr = lambda _t, _a=None, _c=codigo: ResultadoSuite(codigo=_c)  # type: ignore[method-assign]
        assert ejecutor.ejecutar(1) == esperado, f"código {codigo}"

    ejecutor.correr = lambda _t, _a=None: ResultadoSuite(codigo=-1, expirado=True)  # type: ignore[method-assign]
    assert ejecutor.ejecutar(1) == TIMEOUT


def test_el_resultado_extrae_los_tests_caidos_sin_repetirlos() -> None:
    resultado = ResultadoSuite(
        codigo=PYTEST_FALLOS,
        salida=(
            "FAILED tests/a.py::test_uno - X\n"
            "ERROR tests/b.py::test_dos\n"
            "FAILED tests/a.py::test_uno - X\n"
        ),
    )
    assert resultado.fallidos() == ["tests/a.py::test_uno", "tests/b.py::test_dos"]


# --- 3. Restauración a prueba de muerte -------------------------------------


def test_una_interrupcion_deja_el_arbol_restaurado(arbol: Path) -> None:
    """Ctrl-C a mitad de un mutante: el fichero vuelve a como estaba."""

    class EjecutorQueRevienta(EjecutorFalso):
        def ejecutar(self, timeout_s: int) -> str:
            raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        ejecutar_campania(_alcance(), EjecutorQueRevienta(), raiz=str(arbol))

    assert _leer(arbol / "app.py") == FUENTE


def test_si_no_puede_restaurar_queda_el_centinela_con_el_fichero_tocado(
    arbol: Path,
) -> None:
    """El caso brutal: el proceso muere sin desenrollar la pila.

    Se simula lo único que se puede simular sin matar el intérprete: que el
    mutante llegó a aplicarse y nadie llegó a soltarlo. Lo que se exige es que
    el centinela deje escrito QUÉ fichero quedó tocado y en qué línea, que es la
    diferencia entre un rastro y commitear un mutante creyendo que es código.
    """
    centinela = Centinela(raiz=str(arbol), feature="F-999", modo="serie")
    centinela.abrir()
    mutante = Mutante(
        fichero="app.py",
        linea=3,
        col=11,
        original="    return True",
        mutado="    return False",
        operador="booleano",
    )
    (arbol / "app.py").write_text(
        FUENTE.replace("return True", "return False"), encoding="utf-8", newline=""
    )
    centinela.aplicar(arbol / "app.py", mutante, FUENTE)

    datos = Centinela.leer(str(arbol))
    assert datos is not None
    assert datos["muta_arbol_principal"] is True
    assert datos["aplicados"][0]["fichero"] == "app.py"
    assert datos["aplicados"][0]["linea"] == 3
    assert "booleano" in datos["aplicados"][0]["descripcion"]

    # Y desde ese rastro se puede deshacer el destrozo.
    restaurados, irrecuperables = restaurar_desde_centinela(str(arbol))
    assert irrecuperables == []
    assert len(restaurados) == 1
    assert _leer(arbol / "app.py") == FUENTE
    assert Centinela.leer(str(arbol)) is None


def test_una_campania_que_termina_bien_no_deja_centinela(arbol: Path) -> None:
    centinela = Centinela(raiz=str(arbol), feature="F-999", modo="serie")
    centinela.abrir()
    assert Centinela.leer(str(arbol)) is not None

    ejecutar_campania(
        _alcance(), EjecutorFalso(veredictos=[MUERTO]), raiz=str(arbol), centinela=centinela
    )
    centinela.cerrar()

    assert Centinela.leer(str(arbol)) is None


def test_un_arbol_sucio_no_deja_arrancar_la_campania(arbol: Path) -> None:
    """Un mutante en el diff es indistinguible del trabajo de un humano."""
    (arbol / "app.py").write_text(FUENTE + "# a medias\n", encoding="utf-8", newline="")

    with pytest.raises(ArbolSucio) as caido:
        ejecutar_campania(_alcance(), EjecutorFalso(), raiz=str(arbol))

    assert "app.py" in str(caido.value)
    assert "commitea" in str(caido.value).lower()


def test_la_guardia_solo_mira_los_ficheros_que_va_a_mutar(arbol: Path) -> None:
    """Un informe a medias en `progress/` no tiene por qué frenar la campaña."""
    (arbol / "otro.txt").write_text("borrador\n", encoding="utf-8")

    guardia_arbol_limpio(str(arbol), ["app.py"])  # no lanza

    assert ficheros_con_cambios(str(arbol), ["app.py"]) == []


def test_sin_git_la_guardia_aborta_en_vez_de_seguir_a_ciegas(tmp_path: Path) -> None:
    """No saber si un fichero tiene trabajo dentro es motivo para no tocarlo."""
    (tmp_path / "app.py").write_text(FUENTE, encoding="utf-8")

    with pytest.raises(ArbolSucio):
        ficheros_con_cambios(str(tmp_path), ["app.py"])


# --- Interacción con el informe ---------------------------------------------


def test_el_informe_de_una_campania_no_fiable_lo_dice_lo_primero(
    arbol: Path, tmp_path: Path
) -> None:
    from harness.mutacion import escribir_informe

    ejecutor = EjecutorFalso(
        bases=[ResultadoSuite(codigo=PYTEST_OK), ResultadoSuite(codigo=PYTEST_FALLOS)],
        veredictos=[MUERTO],
    )
    informe = ejecutar_campania(_alcance(), ejecutor, raiz=str(arbol))

    destino = tmp_path / "informe.md"
    escribir_informe(informe, destino)
    texto = destino.read_text(encoding="utf-8")

    assert "CAMPAÑA NO VÁLIDA" in texto
    assert texto.index("CAMPAÑA NO VÁLIDA") < texto.index("## Totales")


def test_los_mutantes_sin_veredicto_salen_en_el_informe(
    arbol: Path, tmp_path: Path
) -> None:
    from harness.mutacion import escribir_informe

    informe = ejecutar_campania(
        _alcance(),
        EjecutorFalso(
            bases=[
                ResultadoSuite(codigo=PYTEST_OK),
                ResultadoSuite(codigo=PYTEST_FALLOS),
                ResultadoSuite(codigo=PYTEST_OK),
            ],
            veredictos=[INDETERMINADO],
        ),
        raiz=str(arbol),
    )
    destino = tmp_path / "informe.md"
    escribir_informe(informe, destino)
    texto = destino.read_text(encoding="utf-8")

    assert "| Sin veredicto (base rota) | 1 |" in texto
    assert "Sin veredicto: la suite estaba rota por su cuenta" in texto
    assert BASE_ROTA == "base_rota"  # el veredicto tiene nombre estable
