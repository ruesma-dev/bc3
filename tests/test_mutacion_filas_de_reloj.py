# tests/test_mutacion_filas_de_reloj.py
"""Las filas del informe de mutación que dependen del RELOJ.

Dos campañas equivalentes escriben el mismo informe salvo en los tiempos. Qué
filas son «tiempo» se mantenía a mano dentro de un test de paridad
serie/paralelo, y nadie lo extendió cuando una versión posterior añadió una
fila más: el test quedó flaky durante semanas. Aquí se declara una sola vez,
junto a quien escribe el informe (`FILAS_DE_RELOJ`), y se comprueba de la única
manera que caza una fila NUEVA: escribiendo el mismo informe con dos relojes
distintos y exigiendo que las líneas comparables coincidan.

`lineas_comparables` es además la forma canónica de comparar dos informes de
mutación ignorando el reloj: quien necesite comparar campañas la usa en vez de
escribirse su propio filtro, que es como nació el problema.
"""

from __future__ import annotations

from pathlib import Path

from harness.alcance import Alcance
from harness.mutacion import (
    FILAS_DE_RELOJ,
    InformeMutacion,
    Mutante,
    escribir_informe,
    lineas_comparables,
)


def _alcance() -> Alcance:
    return Alcance(
        feature="F-999",
        origen="rama",
        ref_diff=("base", "rama"),
        lineas={"harness/mutacion.py": {10, 11, 12}},
    )


def _mutante() -> Mutante:
    return Mutante(
        fichero="harness/mutacion.py",
        linea=11,
        col=4,
        original="a + b",
        mutado="a - b",
        operador="aritmetico",
    )


def _informe(**cambios: object) -> InformeMutacion:
    """Un informe completo, con supervivientes y muestreo, listo para comparar."""
    datos: dict[str, object] = {
        "feature": "F-999",
        "alcance": _alcance(),
        "generados": 40,
        "muertos": 18,
        "supervivientes": [_mutante()],
        "mutantes_evaluados": [_mutante()] * 20,
        "segundos": 100.0,
        "muestreado": True,
        "max_mutantes": 20,
        "semilla": 20260820,
        "sha_head": "0123456789abcdef0123456789abcdef01234567",
        "segundos_linea_base": {"raiz": 50.0},
        "nivel": "estandar",
    }
    datos.update(cambios)
    return InformeMutacion(**datos)  # type: ignore[arg-type]


def _escribir(informe: InformeMutacion, ruta: Path) -> list[str]:
    escribir_informe(informe, ruta)
    return lineas_comparables(ruta.read_text(encoding="utf-8"))


# --- R3: la constante y la función existen y son lo que dicen ---------------


def test_filas_de_reloj_es_una_tupla_de_prefijos_no_vacia() -> None:
    assert isinstance(FILAS_DE_RELOJ, tuple)
    assert FILAS_DE_RELOJ
    assert all(isinstance(prefijo, str) and prefijo for prefijo in FILAS_DE_RELOJ)


def test_lineas_comparables_descarta_las_filas_declaradas(tmp_path: Path) -> None:
    texto = tmp_path / "informe.md"
    escribir_informe(_informe(), texto)
    comparables = lineas_comparables(texto.read_text(encoding="utf-8"))

    assert not any(
        linea.startswith(FILAS_DE_RELOJ) for linea in comparables
    ), "quedó en las comparables una fila declarada como fila de reloj"
    assert not any(linea.startswith("<!-- ") for linea in comparables)
    assert "## Totales" in comparables, "se llevó por delante filas que no son reloj"


# --- R4 y R5: dos relojes distintos, el mismo informe comparable ------------


def test_dos_informes_que_solo_difieren_en_tiempos_son_comparables_iguales(
    tmp_path: Path,
) -> None:
    """El test que caza una fila de reloj NUEVA sin declarar (R5).

    No revisa la constante: escribe el mismo informe con dos relojes distintos.
    Cualquier fila que salga del reloj y no esté en `FILAS_DE_RELOJ` hace que
    estas dos listas dejen de coincidir, que es exactamente el flake conocido.
    """
    rapido = _escribir(
        _informe(segundos=3.0, segundos_linea_base={"raiz": 1.0}),
        tmp_path / "rapido.md",
    )
    lento = _escribir(
        _informe(segundos=987.6, segundos_linea_base={"raiz": 543.2}),
        tmp_path / "lento.md",
    )

    assert rapido == lento


def test_todo_prefijo_declarado_sigue_apareciendo_en_el_informe(
    tmp_path: Path,
) -> None:
    """Un prefijo que ya no casa con ninguna fila no protege nada.

    Si alguien renombra `| Tiempo total`, la constante deja de filtrarla y el
    test anterior se vuelve flaky otra vez. Esto lo caza en el acto.
    """
    ruta = tmp_path / "informe.md"
    escribir_informe(_informe(), ruta)
    lineas = ruta.read_text(encoding="utf-8").splitlines()

    for prefijo in FILAS_DE_RELOJ:
        assert any(
            linea.startswith(prefijo) for linea in lineas
        ), f"el prefijo {prefijo!r} ya no casa con ninguna fila del informe"


def test_una_fila_de_reloj_sin_declarar_rompe_la_comparacion() -> None:
    """La guarda de R5, demostrada: sin declarar, la fila nueva se ve.

    Se simula la fila que alguien añade al informe sin declararla y se comprueba
    que `lineas_comparables` NO la esconde.
    """
    fila_sin_declarar = "| Fila nueva del reloj (s) | {} |"
    rapido = lineas_comparables("## Totales\n" + fila_sin_declarar.format("1.0"))
    lento = lineas_comparables("## Totales\n" + fila_sin_declarar.format("9.0"))

    assert rapido != lento


# --- R6: una diferencia de verdad se sigue viendo ---------------------------


def test_las_diferencias_que_no_son_reloj_siguen_apareciendo(tmp_path: Path) -> None:
    referencia = _escribir(_informe(), tmp_path / "referencia.md")

    otro_alcance = Alcance(
        feature="F-999",
        origen="ficheros",
        ref_diff=("(sin diff)", "abc1234"),
        lineas={"harness/rigor.py": {1, 2}},
    )
    otro_superviviente = Mutante(
        fichero="harness/rigor.py",
        linea=2,
        col=0,
        original="x > 0",
        mutado="x >= 0",
        operador="comparacion",
    )
    variantes = {
        "alcance": _informe(alcance=otro_alcance),
        "totales": _informe(generados=41, muertos=19),
        "sha": _informe(sha_head="f" * 40),
        "muestreo": _informe(muestreado=False),
        "superviviente": _informe(supervivientes=[otro_superviviente]),
    }

    for nombre, variante in variantes.items():
        distinto = _escribir(variante, tmp_path / f"{nombre}.md")
        assert distinto != referencia, f"la diferencia de {nombre} quedó escondida"
