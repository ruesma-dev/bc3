# tests/test_mutacion_muestreo_por_nivel.py
"""Cuánto cuesta una campaña lo decide el nivel de rigor.

Una campaña completa sobre una feature grande evalúa decenas de mutantes y
obliga a analizar por escrito cada superviviente, que es lo caro. El tope y la
semilla dejan de ser algo que hay que acordarse de teclear y pasan a vivir en
`harness/rigor.json`, por nivel: `estandar` muestreado y reproducible,
`critico` sin tope.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from harness import mutacion
from harness.alcance import Alcance
from harness.mutacion import (
    InformeMutacion,
    _muestreo_configurado,
    resolver_muestreo,
)
from harness.rigor import cargar_rigor, max_mutantes_nivel, semilla_nivel

RIGOR = {
    "nivel_por_defecto": "estandar",
    "niveles": {
        "documental": {"fase_red": False, "cobertura": False, "mutacion": False},
        "estandar": {
            "fase_red": True,
            "cobertura": True,
            "mutacion": True,
            "max_mutantes": 20,
            "semilla": 20260820,
        },
        "critico": {
            "fase_red": True,
            "cobertura": True,
            "mutacion": True,
            "max_mutantes": None,
            "semilla": None,
        },
    },
}


def test_el_nivel_estandar_declara_tope_y_semilla_en_rigor_json() -> None:
    assert max_mutantes_nivel("estandar", RIGOR) == 20
    assert semilla_nivel("estandar", RIGOR) == 20260820


def test_el_nivel_critico_no_tiene_tope_en_rigor_json() -> None:
    """`None` es «sin tope»: en `critico` se evalúa todo lo generado."""
    assert max_mutantes_nivel("critico", RIGOR) is None
    assert semilla_nivel("critico", RIGOR) is None


def test_un_nivel_sin_las_claves_de_rigor_no_revienta() -> None:
    """Las claves son OPCIONALES: un `rigor.json` viejo sigue funcionando."""
    assert max_mutantes_nivel("documental", RIGOR) is None
    assert semilla_nivel("documental", RIGOR) is None


def test_un_nivel_que_no_existe_en_rigor_json_no_impone_tope() -> None:
    assert max_mutantes_nivel("inventado", RIGOR) is None
    assert semilla_nivel("inventado", RIGOR) is None


def test_un_tope_absurdo_del_rigor_json_se_trata_como_ausencia() -> None:
    """Misma doctrina que `workers_mutacion`: mejor el defecto que una campaña rara."""
    malos = {
        "niveles": {
            "cero": {"max_mutantes": 0},
            "negativo": {"max_mutantes": -3},
            "booleano": {"max_mutantes": True},
            "texto": {"max_mutantes": "20"},
        }
    }
    for nivel in malos["niveles"]:
        assert max_mutantes_nivel(nivel, malos) is None, nivel


def test_una_semilla_de_rigor_no_entera_se_trata_como_ausencia() -> None:
    malos = {"niveles": {"booleana": {"semilla": True}, "texto": {"semilla": "x"}}}
    for nivel in malos["niveles"]:
        assert semilla_nivel(nivel, malos) is None, nivel


def test_la_semilla_cero_es_una_semilla_valida_en_rigor_json() -> None:
    """A diferencia del tope, `0` no significa nada especial en una semilla."""
    assert semilla_nivel("cero", {"niveles": {"cero": {"semilla": 0}}}) == 0


def test_el_rigor_json_del_repositorio_declara_el_tope_de_estandar() -> None:
    """El tope y la semilla viven en el fichero, no cableados en el código."""
    rigor = cargar_rigor()

    assert max_mutantes_nivel("estandar", rigor) == 20
    assert semilla_nivel("estandar", rigor) == 20260820, "semilla fija: reproducible"
    assert max_mutantes_nivel("critico", rigor) is None, "critico se mide entero"


def test_el_nivel_por_defecto_del_rigor_json_es_estandar() -> None:
    """R8: quien no declara `rigor` ya no arrastra el nivel más caro."""
    assert cargar_rigor()["nivel_por_defecto"] == "estandar"


# --- R6, R7: precedencia de la orden sobre el nivel --------------------------


def test_lo_pedido_a_mano_gana_al_nivel() -> None:
    assert resolver_muestreo(5, 7, 20, 20260820) == (5, 7)


def test_sin_nada_pedido_manda_el_nivel() -> None:
    assert resolver_muestreo(None, None, 20, 20260820) == (20, 20260820)


def test_max_mutantes_cero_significa_sin_tope() -> None:
    """La única forma de anular desde la orden el tope que impone un nivel."""
    assert resolver_muestreo(0, None, 20, 20260820) == (None, 20260820)


def test_un_tope_negativo_tampoco_es_un_tope() -> None:
    assert resolver_muestreo(-1, None, 20, None) == (None, None)


def test_sin_nivel_ni_peticion_la_campania_es_completa() -> None:
    """Un `rigor.json` sin las claves nuevas mide todo, como hasta hoy."""
    assert resolver_muestreo(None, None, None, None) == (None, None)


def test_la_semilla_pedida_gana_sin_arrastrar_el_tope() -> None:
    assert resolver_muestreo(None, 1, 20, 20260820) == (20, 1)


def test_el_muestreo_configurado_sale_del_nivel_de_la_feature() -> None:
    maximo, semilla, nivel = _muestreo_configurado("F-038")

    assert (maximo, semilla, nivel) == (20, 20260820, "estandar")


def test_una_feature_sin_ficha_cae_en_estandar_no_en_critico() -> None:
    """R8: omitir el nivel ya no arrastra la campaña completa."""
    assert _muestreo_configurado("F-999-no-existe") == (20, 20260820, "estandar")


def test_una_configuracion_ilegible_no_impone_muestreo(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Misma red de seguridad que `_timeout_configurado`: se degrada, no se cae."""
    monkeypatch.chdir(tmp_path)

    assert _muestreo_configurado("F-038") == (None, None, None)


# --- R6, R7 vistos desde el CLI ---------------------------------------------


def _campania_espia(capturado: dict):
    def falsa(alcance, ejecutor, **kwargs):
        capturado.update(kwargs)
        return InformeMutacion(feature=alcance.feature, alcance=alcance)

    return falsa


def _preparar_cli(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capturado: dict
) -> list[str]:
    alcance = Alcance(
        feature="F-038",
        origen="rama",
        ref_diff=("dev", "feature/F-038-coste-del-ciclo-sdd"),
        lineas={"harness/mutacion.py": {1}},
    )
    monkeypatch.setattr(mutacion, "alcance_de_feature", lambda *a, **k: alcance)
    monkeypatch.setattr(mutacion, "ejecutar_campania", _campania_espia(capturado))
    return [
        "--feature",
        "F-038",
        "--raiz",
        str(tmp_path),
        "--salida",
        str(tmp_path / "informe.md"),
    ]


def test_el_cli_sin_banderas_aplica_el_muestreo_del_nivel(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    capturado: dict = {}

    mutacion.main(_preparar_cli(tmp_path, monkeypatch, capturado), ejecutor=object())

    assert capturado["max_mutantes"] == 20
    assert capturado["semilla"] == 20260820
    # Y se dice por pantalla: quien lanza la campaña tiene que ver que va
    # muestreada, o leerá 20 mutantes creyendo que son todos.
    assert "Muestreo: hasta 20 mutantes, semilla 20260820" in capsys.readouterr().out


def test_una_campania_completa_no_anuncia_muestreo(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Sin tope no hay muestreo que anunciar: anunciarlo sería mentir."""
    capturado: dict = {}
    argv = _preparar_cli(tmp_path, monkeypatch, capturado) + ["--max-mutantes", "0"]

    mutacion.main(argv, ejecutor=object())

    assert "Muestreo:" not in capsys.readouterr().out


def test_el_cli_con_max_mutantes_cero_anula_el_tope_del_nivel(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    capturado: dict = {}
    argv = _preparar_cli(tmp_path, monkeypatch, capturado) + ["--max-mutantes", "0"]

    mutacion.main(argv, ejecutor=object())

    assert capturado["max_mutantes"] is None, "0 = sin tope, no «cero mutantes»"


def test_un_tope_de_un_solo_mutante_es_un_tope_legitimo() -> None:
    """La frontera del `> 0`: pedir 1 mutante no puede leerse como «sin tope»."""
    assert resolver_muestreo(1, None, 20, 20260820) == (1, 20260820)


def test_un_nivel_de_rigor_puede_declarar_un_solo_mutante() -> None:
    """Misma frontera en el lector: `1` es un tope, no un valor absurdo."""
    assert max_mutantes_nivel("minimo", {"niveles": {"minimo": {"max_mutantes": 1}}}) == 1
