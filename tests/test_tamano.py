# tests/test_tamano.py
"""Los topes de tamaño del papeleo de una feature.

Cada línea de spec se paga tres veces: la escribe el spec-author, la lee el
implementer y la relee el reviewer. El arnés no decía nada del tamaño y por eso
los agentes escribían cuanto se les ocurría. Los topes viven en
`harness/rigor.json` —nunca cableados en el código— y los mide
`python -m harness.tamano`.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from harness.rigor import cargar_rigor, topes_tamano
from harness.tamano import ETIQUETA, main, medir, slug_de_feature


def test_los_topes_viven_en_el_rigor_json_y_no_en_el_codigo() -> None:
    rigor = {"tamano": {"requirements": 150, "design": 250, "impl": 220, "review": 140}}

    assert topes_tamano(rigor) == {
        "requirements": 150,
        "design": 250,
        "impl": 220,
        "review": 140,
    }


def test_un_rigor_json_sin_bloque_tamano_no_declara_topes() -> None:
    """Sin bloque, la puerta se declara N/A: un arnés viejo no se rompe."""
    assert topes_tamano({"niveles": {}}) == {}


def test_el_doc_del_bloque_de_rigor_no_es_un_tope() -> None:
    """`$doc` es documentación del propio JSON, como en el resto del fichero."""
    assert topes_tamano({"tamano": {"$doc": "explicación", "impl": 150}}) == {"impl": 150}


def test_un_tope_absurdo_del_rigor_json_se_descarta() -> None:
    rigor = {"tamano": {"impl": 0, "review": -1, "design": True, "requirements": "120"}}

    assert topes_tamano(rigor) == {}


def test_el_rigor_json_del_repositorio_declara_los_cuatro_topes() -> None:
    """150 / 250 / 220 / 140: los cuatro ficheros que se pagan por triplicado."""
    topes = topes_tamano(cargar_rigor())

    assert topes == {"requirements": 150, "design": 250, "impl": 220, "review": 140}


# --- R14, R16: la medición ---------------------------------------------------

TOPES = {"requirements": 150, "design": 250, "impl": 220, "review": 140}


def _repositorio(tmp_path: Path, ficheros: dict[str, int]) -> Path:
    """Repositorio de juguete con `ficheros` = {ruta relativa: nº de líneas}."""
    (tmp_path / "harness").mkdir(parents=True, exist_ok=True)
    (tmp_path / "harness" / "rigor.json").write_text(
        json.dumps(
            {
                "nivel_por_defecto": "estandar",
                "niveles": {
                    "estandar": {
                        "fase_red": True,
                        "cobertura": True,
                        "mutacion": True,
                    }
                },
                "tamano": TOPES,
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "harness" / "features.json").write_text(
        json.dumps(
            {"features": [{"id": "F-100", "branch": "feature/F-100-de-juguete"}]}
        ),
        encoding="utf-8",
    )
    for ruta, cuantas in ficheros.items():
        destino = tmp_path / ruta
        destino.parent.mkdir(parents=True, exist_ok=True)
        destino.write_text("línea\n" * cuantas, encoding="utf-8")
    return tmp_path


def test_solo_se_nombra_lo_que_excede_su_tope(tmp_path: Path) -> None:
    raiz = _repositorio(
        tmp_path,
        {
            "specs/F-100-de-juguete/requirements.md": 151,
            "specs/F-100-de-juguete/design.md": 250,
            "progress/impl_F-100.md": 221,
        },
    )

    excesos = medir("F-100", "F-100-de-juguete", TOPES, raiz=str(raiz))

    assert [(e.clave, e.lineas, e.tope) for e in excesos] == [
        ("requirements", 151, 150),
        ("impl", 221, 220),
    ]


def test_el_tope_exacto_no_es_un_exceso(tmp_path: Path) -> None:
    """150 líneas es «dentro»: el tope es el límite, no el primero que sobra."""
    raiz = _repositorio(tmp_path, {"specs/F-100-de-juguete/requirements.md": 150})

    assert medir("F-100", "F-100-de-juguete", TOPES, raiz=str(raiz)) == []


def test_los_ficheros_que_no_existen_no_se_miden(tmp_path: Path) -> None:
    """El informe de review no existe mientras el implementer trabaja."""
    raiz = _repositorio(tmp_path, {})

    assert medir("F-100", "F-100-de-juguete", TOPES, raiz=str(raiz)) == []


def test_sin_slug_no_se_miden_las_specs(tmp_path: Path) -> None:
    raiz = _repositorio(
        tmp_path,
        {
            "specs/F-100-de-juguete/requirements.md": 300,
            "progress/impl_F-100.md": 221,
        },
    )

    excesos = medir("F-100", None, TOPES, raiz=str(raiz))

    assert [e.clave for e in excesos] == ["impl"]


def test_el_slug_sale_de_la_rama_declarada_en_features_json(
    tmp_path: Path,
) -> None:
    raiz = _repositorio(tmp_path, {})

    assert slug_de_feature("F-100", raiz=str(raiz)) == "F-100-de-juguete"


def test_sin_ficha_el_slug_sale_del_directorio_de_la_spec(
    tmp_path: Path,
) -> None:
    raiz = _repositorio(tmp_path, {"specs/F-200-otra/requirements.md": 1})

    assert slug_de_feature("F-200", raiz=str(raiz)) == "F-200-otra"


def test_dos_directorios_de_spec_no_dejan_adivinar_el_slug(
    tmp_path: Path,
) -> None:
    raiz = _repositorio(
        tmp_path,
        {"specs/F-300-una/requirements.md": 1, "specs/F-300-otra/requirements.md": 1},
    )

    assert slug_de_feature("F-300", raiz=str(raiz)) is None


# --- R14: el CLI -------------------------------------------------------------


def test_el_cli_sale_con_cero_cuando_todo_cabe(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    raiz = _repositorio(tmp_path, {"progress/impl_F-100.md": 220})

    assert main(["--feature", "F-100", "--raiz", str(raiz)]) == 0
    assert ETIQUETA in capsys.readouterr().out


def test_el_cli_sale_con_uno_nombrando_fichero_lineas_y_tope(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    raiz = _repositorio(tmp_path, {"progress/impl_F-100.md": 242})

    assert main(["--feature", "F-100", "--raiz", str(raiz)]) == 1
    salida = capsys.readouterr()
    assert "progress/impl_F-100.md: 242 líneas > tope 220" in salida.out + salida.err


def test_sin_topes_declarados_el_cli_no_bloquea_y_dice_por_que(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Código 2: la puerta se declara N/A con su motivo, no en rojo."""
    raiz = _repositorio(tmp_path, {})
    (raiz / "harness" / "rigor.json").write_text("{}", encoding="utf-8")

    assert main(["--feature", "F-100", "--raiz", str(raiz)]) == 2
    salida = capsys.readouterr()
    assert "N/A" in salida.out + salida.err


def test_el_cli_mide_una_sola_feature(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Las 12 specs viejas exceden hoy: medirlas dejaría init.sh en rojo eterno."""
    raiz = _repositorio(
        tmp_path,
        {
            "progress/impl_F-100.md": 10,
            "specs/F-019-vieja/requirements.md": 333,
            "progress/impl_F-019.md": 1189,
        },
    )

    assert main(["--feature", "F-100", "--raiz", str(raiz)]) == 0
    assert "F-019" not in capsys.readouterr().out


# --- R15: la puerta dentro del portero --------------------------------------


def test_init_sh_mide_el_tamano_de_la_feature_en_curso() -> None:
    """La puerta vive en el portero, no en la buena voluntad de quien pasa."""
    portero = Path("harness/init.sh").read_text(encoding="utf-8", errors="replace")

    assert "7 quater" in portero, "la sección declarada en el diseño"
    assert "python -m harness.tamano" in portero or "harness.tamano" in portero
    assert "PUERTA TAMAÑO" in portero


def test_init_sh_declara_na_con_motivo_cuando_no_hay_feature() -> None:
    """N/A a secas es un checkbox vacío: el motivo se imprime."""
    portero = Path("harness/init.sh").read_text(encoding="utf-8", errors="replace")
    seccion = portero.split("7 quater", 1)[1].split("--- 8.", 1)[0]

    assert "PUERTA TAMAÑO: N/A" in seccion
    assert "feature_de_rama" in seccion, "la feature en curso se resuelve como en 7b"


def test_un_tope_de_una_linea_es_valido_aunque_sea_absurdo() -> None:
    """La frontera del `< 1`: lo que se descarta es el cero, no el uno."""
    assert topes_tamano({"tamano": {"impl": 1}}) == {"impl": 1}


def test_el_slug_es_todo_lo_que_sigue_a_la_primera_barra(
    tmp_path: Path,
) -> None:
    """Una rama con más de una barra no puede recortarse por en medio."""
    raiz = _repositorio(tmp_path, {})
    (raiz / "harness" / "features.json").write_text(
        json.dumps({"features": [{"id": "F-100", "branch": "feature/exp/F-100-largo"}]}),
        encoding="utf-8",
    )

    assert slug_de_feature("F-100", raiz=str(raiz)) == "exp/F-100-largo"


def test_el_cli_resume_cada_fichero_que_ha_medido(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Sin el resumen, la puerta en verde no dice QUÉ midió: podría no medir nada."""
    raiz = _repositorio(
        tmp_path,
        {
            "specs/F-100-de-juguete/requirements.md": 90,
            "progress/impl_F-100.md": 220,
        },
    )

    assert main(["--feature", "F-100", "--raiz", str(raiz)]) == 0
    salida = capsys.readouterr().out
    assert "requirements 90/150" in salida
    assert "impl 220/220" in salida


def test_sin_papeleo_todavia_el_cli_lo_dice_en_vez_de_callar(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    raiz = _repositorio(tmp_path, {})

    assert main(["--feature", "F-100", "--raiz", str(raiz)]) == 0
    assert "ningún fichero de papeleo todavía" in capsys.readouterr().out


# --- La puerta de init.sh no mide papeleo ya cerrado ------------------------


def test_la_puerta_de_init_descarta_las_features_done() -> None:
    """Una feature `done` es papeleo CERRADO: medirlo no cambia nada de lo que
    se vaya a escribir y deja el portero en rojo permanente. Sin esta regla la
    amnistía de «lo viejo no se mide» se cae justo en la rama base, en cuanto
    una feature cerrada declara esa rama como suya: pasó con F-015 de
    `porcentajes`, hecha directamente en `dev`, cuyo review de 546 líneas
    —anterior a los topes— dejaba `dev` en rojo para siempre."""
    portero = Path("harness/init.sh").read_text(encoding="utf-8", errors="replace")
    bloque = portero.split("7 quater", 1)[1].split("--- 8.", 1)[0]

    assert 'ficha.get("status") == "done"' in bloque
    assert "ficha = None" in bloque


def test_sin_feature_el_cli_no_adivina_cual_medir() -> None:
    """`--feature` es OBLIGATORIA, y sin ella argparse aborta con codigo 2.

    Lo destapo la campana de mutacion de F-047 en datamart-seg-anual: con
    `required=False` la opcion llegaria a `None` y la puerta mediria «la feature
    None», es decir, ningun fichero, y saldria en verde sin haber comprobado
    nada. Un tope que no mide nada es peor que no tenerlo, porque parece que
    mide.
    """
    import pytest as _pytest

    from harness.tamano import main as _main

    with _pytest.raises(SystemExit) as salida:
        _main([])

    assert salida.value.code == 2
