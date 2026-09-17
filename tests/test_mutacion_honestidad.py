# tests/test_mutacion_honestidad.py
"""F-040 · R11–R17 (+ R20, R21): la campaña deja de mentir sobre lo que midió.

Dos mentiras distintas, las dos vistas en campo el 2026-08-21:

- **D3.** Una línea base que EXPIRA no es una base rota. El arnés decía «La
  base se rompió… arregla la suite y repite la campaña» sobre una suite
  impecable que solo se había quedado sin tiempo, y mandó al humano a buscar un
  fallo que no existía.
- **D4.** Una campaña con CERO mutantes salía en verde, escribiendo un informe
  que se lee como «nada que arreglar». Van tres puertas distintas por las que
  ha entrado el mismo fallo, así que la guarda va en el embudo (`main`), que es
  por donde pasan todas.

Ningún test de este fichero ejecuta una suite real: todos usan ejecutores
dobles que devuelven el `ResultadoSuite` que se les pide.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from harness.alcance import Alcance
from harness.mutacion import (
    InformeMutacion,
    Mutante,
    ResultadoSuite,
    _base_rota_al_final,
    escribir_informe,
    main,
)


class EjecutorDoble:
    """Ejecutor que devuelve los resultados de línea base que se le dicten."""

    def __init__(self, *resultados: ResultadoSuite) -> None:
        self._resultados = list(resultados)
        self.llamadas = 0
        self.raiz = "raiz_doble"

    def linea_base(self, _timeout_s: int) -> ResultadoSuite:
        self.llamadas += 1
        indice = min(self.llamadas - 1, len(self._resultados) - 1)
        return self._resultados[indice]

    def ejecutar(self, _timeout_s: int) -> str:  # pragma: no cover - no se usa
        raise AssertionError("estos tests no juzgan mutantes")


VERDE = ResultadoSuite(codigo=0, salida="")
EXPIRADA = ResultadoSuite(codigo=-1, salida="", expirado=True)
FALLIDA = ResultadoSuite(
    codigo=1, salida="FAILED tests/test_uno.py::test_a\nFAILED tests/test_dos.py::test_b\n"
)


def _mutante(linea: int = 42) -> Mutante:
    return Mutante(
        fichero="harness/mutacion.py",
        linea=linea,
        col=0,
        original="a == b",
        mutado="a != b",
        operador="comparacion",
        longitud=2,
        sustituto="!=",
    )


def _alcance(feature: str = "F-040") -> Alcance:
    return Alcance(
        feature=feature,
        origen="rama",
        ref_diff=("dev", "feature/x"),
        lineas={"harness/mutacion.py": {1, 2, 3}},
    )


# --- R21 y R12: la base que estaba verde y acaba ROJA ------------------------


def test_una_base_verde_al_cerrar_no_produce_ningun_aviso() -> None:
    doble = EjecutorDoble(VERDE)

    assert _base_rota_al_final([("wk_0", doble)], 120) is None


def test_una_base_fallida_nombra_los_tests_caidos() -> None:
    """R12: el aviso de siempre, cuando la suite falla DE VERDAD."""
    aviso = _base_rota_al_final([("wk_0", EjecutorDoble(FALLIDA))], 120)

    assert aviso is not None
    assert "tests/test_uno.py::test_a" in aviso
    assert "tests/test_dos.py::test_b" in aviso
    assert "Arregla la suite" in aviso, (
        "una base realmente rota SÍ se arregla arreglando la suite"
    )


def test_un_ejecutor_sin_linea_base_no_cuenta() -> None:
    """Un doble sin `linea_base` no puede desmentir nada: se salta."""

    class SinBase:
        raiz = "sin_base"

    assert _base_rota_al_final([("sin", SinBase())], 120) is None


# --- R11: una base que EXPIRA no es una base rota ---------------------------


def test_una_base_expirada_dice_que_la_suite_NO_fallo() -> None:
    """El mensaje que mandó al humano a arreglar una suite impecable.

    La suite no falló: se quedó sin tiempo. Decirle «arregla la suite» a quien
    tiene la suite bien es peor que no decir nada, porque le hace perder la
    tarde buscando lo que no hay.
    """
    aviso = _base_rota_al_final([("wk_0", EjecutorDoble(EXPIRADA))], 240)

    assert aviso is not None
    assert "no falló" in aviso or "NO falló" in aviso
    assert "Arregla la suite" not in aviso, (
        "R11: expirar no es fallar; mandar a arreglar la suite es la mentira "
        "que esta feature quita"
    )


def test_la_base_expirada_dice_QUE_HACER_y_con_cuanto_tiempo() -> None:
    aviso = _base_rota_al_final([("wk_0", EjecutorDoble(EXPIRADA))], 240)

    assert aviso is not None
    assert "240" in aviso, "el aviso tiene que decir cuánto tiempo se concedió"
    assert "workers" in aviso.lower(), "la acción es bajar workers…"
    assert "suelo" in aviso.lower(), "…o subir el suelo del timeout configurado"
    assert "timeout_por_mutante_s" in aviso, "…y nombrar la clave que se sube"


def test_los_dos_avisos_no_son_el_mismo_texto() -> None:
    expirada = _base_rota_al_final([("wk_0", EjecutorDoble(EXPIRADA))], 240)
    fallida = _base_rota_al_final([("wk_0", EjecutorDoble(FALLIDA))], 240)

    assert expirada != fallida


def test_los_dos_avisos_invalidan_el_informe_igual(tmp_path: Path) -> None:
    """R13: cambia el texto, no la consecuencia. `fiable` sigue siendo falso."""
    for aviso in (
        _base_rota_al_final([("wk_0", EjecutorDoble(EXPIRADA))], 240),
        _base_rota_al_final([("wk_0", EjecutorDoble(FALLIDA))], 240),
    ):
        informe = InformeMutacion(feature="F-040", alcance=_alcance())
        informe.aviso_base = aviso
        ruta = tmp_path / "informe.md"

        assert informe.fiable is False
        escribir_informe(informe, ruta)
        assert "⚠ CAMPAÑA NO VÁLIDA" in ruta.read_text(encoding="utf-8")


# --- R20: la sección `## Timeouts`, sin el prefijo duplicado ----------------


def test_la_seccion_de_timeouts_nombra_fichero_y_linea_UNA_vez(
    tmp_path: Path,
) -> None:
    """`Mutante.descripcion()` ya lleva `fichero:linea` dentro.

    Anteponérselo daba `- \\`a.py:3\\` a.py:3 [op] x -> y`. No revienta, pero
    nadie lo había leído nunca: la sección entera estaba sin test.
    """
    informe = InformeMutacion(feature="F-040", alcance=_alcance())
    informe.timeouts = [_mutante(42)]
    ruta = tmp_path / "informe.md"

    escribir_informe(informe, ruta)

    fila = next(
        linea
        for linea in ruta.read_text(encoding="utf-8").splitlines()
        if linea.startswith("- `harness/mutacion.py:42`")
    )
    assert fila.count("harness/mutacion.py:42") == 1, (
        f"fichero:línea duplicado en la fila de timeouts: {fila!r}"
    )
    assert "[comparacion]" in fila
    assert "a == b -> a != b" in fila


def test_la_seccion_de_timeouts_solo_existe_si_hay_timeouts(
    tmp_path: Path,
) -> None:
    informe = InformeMutacion(feature="F-040", alcance=_alcance())
    ruta = tmp_path / "informe.md"

    escribir_informe(informe, ruta)

    assert "## Timeouts" not in ruta.read_text(encoding="utf-8")


def test_hay_una_fila_por_mutante_expirado(tmp_path: Path) -> None:
    informe = InformeMutacion(feature="F-040", alcance=_alcance())
    informe.timeouts = [_mutante(10), _mutante(20), _mutante(30)]
    ruta = tmp_path / "informe.md"

    escribir_informe(informe, ruta)

    texto = ruta.read_text(encoding="utf-8")
    assert "## Timeouts" in texto
    filas = [linea for linea in texto.splitlines() if linea.startswith("- `harness/")]
    assert len(filas) == 3


# --- R14, R15, R16: cero mutantes NO puede salir en verde -------------------

#: Código con el que sale una campaña que no ha juzgado nada. Es el 3 de
#: `CampaniaAbortada`, distinto del 1 de «hay supervivientes» a propósito: «hay
#: supervivientes» es un resultado, «no se ha medido nada» no lo es.
NO_SE_MIDIO_NADA = 3


def _informe_de(alcance: Alcance, generados: int) -> InformeMutacion:
    return InformeMutacion(feature=alcance.feature, alcance=alcance, generados=generados)


@pytest.fixture
def sin_campania(monkeypatch: pytest.MonkeyPatch) -> None:
    """Prohíbe arrancar la campaña: la guarda tiene que morder ANTES (R16)."""

    def _prohibida(*_args: object, **_kwargs: object):
        raise AssertionError(
            "con el alcance vacío no se arranca ni una línea base: el aborto va "
            "antes de tocar nada"
        )

    monkeypatch.setattr("harness.mutacion.ejecutar_campania", _prohibida)
    monkeypatch.setattr(
        "harness.mutacion_paralela.ejecutar_campania_paralela", _prohibida
    )


def test_un_alcance_sin_lineas_aborta_con_3_antes_de_la_linea_base(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, sin_campania: None
) -> None:
    """Hoy imprime «Sin líneas de producción en el alcance» y SIGUE.

    Seguir es lo que produce el informe de cero mutantes que se lee como «todo
    bien». R16: se aborta ahí mismo, sin correr ninguna suite.
    """
    vacio = Alcance(feature="F-040", origen="rama", ref_diff=("dev", "x"), lineas={})
    monkeypatch.setattr(
        "harness.mutacion.alcance_de_feature", lambda *_a, **_k: vacio
    )
    destino = tmp_path / "no_deberia_existir.md"

    codigo = main(["--feature", "F-040", "--salida", str(destino)])

    assert codigo == NO_SE_MIDIO_NADA
    assert not destino.exists(), "una campaña que no midió nada no escribe informe"


def test_el_aborto_por_alcance_vacio_explica_por_que(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    sin_campania: None,
) -> None:
    vacio = Alcance(feature="F-040", origen="rama", ref_diff=("dev", "x"), lineas={})
    monkeypatch.setattr(
        "harness.mutacion.alcance_de_feature", lambda *_a, **_k: vacio
    )

    main(["--feature", "F-040", "--salida", str(tmp_path / "x.md")])

    salida = capsys.readouterr()
    texto = salida.out + salida.err
    assert "no se ha juzgado" in texto.lower() or "no se ha medido" in texto.lower()
    assert "alcance" in texto.lower()


def test_un_alcance_con_ficheros_pero_sin_ninguna_linea_tambien_aborta(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, sin_campania: None
) -> None:
    """`{"a.py": set()}` no es un alcance: es un diccionario con un fichero."""
    hueco = Alcance(
        feature="F-040",
        origen="rama",
        ref_diff=("dev", "x"),
        lineas={"harness/mutacion.py": set()},
    )
    monkeypatch.setattr(
        "harness.mutacion.alcance_de_feature", lambda *_a, **_k: hueco
    )

    assert main(["--feature", "F-040", "--salida", str(tmp_path / "x.md")]) == (
        NO_SE_MIDIO_NADA
    )


@pytest.mark.parametrize(
    "via", ["feature", "ficheros"], ids=["por --feature", "por --ficheros"]
)
def test_cero_mutantes_generados_aborta_con_3_por_cualquier_via(
    via: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """R15: la guarda vive en el EMBUDO, no en cada constructor de alcance.

    Van tres puertas por las que ha entrado el mismo fallo —invocación sin
    ruta, `--ficheros ","` y `--feature` sobre una rama ya mergeada—. Taparlas
    una a una garantiza una cuarta; `main` es el único punto por el que pasan
    todas, presentes y futuras.
    """
    con_lineas = _alcance()
    monkeypatch.setattr(
        "harness.mutacion.alcance_de_feature", lambda *_a, **_k: con_lineas
    )
    monkeypatch.setattr(
        "harness.mutacion.alcance_de_ficheros", lambda *_a, **_k: con_lineas
    )
    monkeypatch.setattr(
        "harness.mutacion.ejecutar_campania",
        lambda alcance, *_a, **_k: _informe_de(alcance, generados=0),
    )
    destino = tmp_path / "no_deberia_existir.md"
    orden = ["--feature", "F-040", "--workers", "1", "--salida", str(destino)]
    if via == "ficheros":
        orden += ["--ficheros", "harness/mutacion.py"]

    codigo = main(orden)

    assert codigo == NO_SE_MIDIO_NADA
    assert not destino.exists()


def test_el_aborto_por_cero_mutantes_dice_que_no_se_juzgo_nada(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    con_lineas = _alcance()
    monkeypatch.setattr(
        "harness.mutacion.alcance_de_feature", lambda *_a, **_k: con_lineas
    )
    monkeypatch.setattr(
        "harness.mutacion.ejecutar_campania",
        lambda alcance, *_a, **_k: _informe_de(alcance, generados=0),
    )

    main(["--feature", "F-040", "--workers", "1", "--salida", str(tmp_path / "x.md")])

    salida = capsys.readouterr()
    texto = (salida.out + salida.err).lower()
    assert "0 mutantes" in texto or "cero mutantes" in texto
    assert "mutable" in texto, (
        "el mensaje tiene que decir el porqué: alcance vacío o sin código mutable"
    )


def test_una_campania_con_mutantes_sigue_escribiendo_su_informe(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """La guarda nueva no puede comerse el caso normal."""
    con_lineas = _alcance()
    monkeypatch.setattr(
        "harness.mutacion.alcance_de_feature", lambda *_a, **_k: con_lineas
    )
    monkeypatch.setattr(
        "harness.mutacion.ejecutar_campania",
        lambda alcance, *_a, **_k: _informe_de(alcance, generados=7),
    )
    destino = tmp_path / "informe.md"

    codigo = main(["--feature", "F-040", "--workers", "1", "--salida", str(destino)])

    assert codigo == 0
    assert destino.is_file()
    assert "| Mutantes generados | 7 |" in destino.read_text(encoding="utf-8")


# --- R17: la guarda de entrada de `alcance_de_ficheros` se queda ------------


def test_la_guarda_de_alcance_de_ficheros_sigue_existiendo(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, sin_campania: None
) -> None:
    """R14 es la red final, NO la sustituta de la guarda de entrada.

    Las dos hacen falta y dicen cosas distintas: `alcance_de_ficheros` sabe QUÉ
    ruta sobra y por qué (código 2, error de uso); `main` solo sabe que no
    quedó nada que juzgar (código 3). Quedarse con la segunda perdería el
    diagnóstico que ahorra la tarde.
    """
    destino = tmp_path / "no_deberia_existir.md"

    assert main(["--feature", "F-040", "--ficheros", ",", "--salida", str(destino)]) == 2
    assert (
        main(
            [
                "--feature",
                "F-040",
                "--ficheros",
                "docs/CONVENTIONS.md",
                "--salida",
                str(destino),
            ]
        )
        == 2
    )
    assert not destino.exists()


def test_el_mensaje_de_la_guarda_de_entrada_nombra_la_ruta_que_sobra(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], sin_campania: None
) -> None:
    main(
        [
            "--feature",
            "F-040",
            "--ficheros",
            "docs/CONVENTIONS.md",
            "--salida",
            str(tmp_path / "x.md"),
        ]
    )

    assert "docs/CONVENTIONS.md" in capsys.readouterr().err


# --- R27: CHECKPOINTS.md tiene que reconocer el factor de workers ----------


def test_rm2_avisa_de_que_el_tiempo_total_se_divide_entre_workers() -> None:
    """La regla de coherencia estaba escrita para campañas EN SERIE.

    Aplicada tal cual a una campaña paralela, un «Tiempo total» tres veces
    menor que `mutantes × media` parece una campaña inventada cuando es
    exactamente lo que tiene que salir con tres workers.
    """
    crudo = (
        Path("CHECKPOINTS.md")
        .read_text(encoding="utf-8")
        .split("**RM2 ·", 1)[1]
        .split("- [ ] **RM5", 1)[0]
    )
    # El documento va envuelto a 79 columnas: una frase parte en dos líneas y
    # buscarla literal fallaría por un salto de línea, no por lo que dice.
    bloque = " ".join(crudo.split())

    assert "worker" in bloque.lower(), "RM2 no menciona los workers"
    assert "media × W" in bloque, (
        "RM2 tiene que dar la corrección: el coste real por mutante es media × W"
    )
    assert "ya viene dividida" in bloque, (
        "RM2 tiene que explicar POR QUÉ: la media es tiempo de pared entre "
        "mutantes, así que el paralelismo ya está descontado"
    )
    assert "Timeout efectivo" in bloque, (
        "RM2 tiene que mandar leer el timeout efectivo antes de juzgar tiempos"
    )
    assert "no son comparables" in bloque or "no comparables" in bloque
