# tests/test_mutacion_huecos.py
"""F-040 · R18–R26: los ocho huecos que nadie estaba comprobando.

Cada uno de estos requisitos nació de mirar dentro de la maquinaria de
mutación y encontrar una rama sin test detrás. No comparten tema: comparten
que estaban descubiertas, y que una rama descubierta de la herramienta que
mide la calidad de los tests es exactamente el peor sitio donde tenerla.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from harness.mutacion import RUTA_CENTINELA, _analizar_argumentos, _modo_restaurar
from harness.mutacion import main as mutacion_main
from harness.rigor import main as rigor_main
from harness.rigor import timeout_mutacion, validar_features

#: Configuración de rigor mínima y válida con la que se prueba el CLI sin
#: depender del `harness/rigor.json` real, que cambia con el proyecto.
RIGOR_MINIMO = {
    "nivel_por_defecto": "estandar",
    "cobertura": {"umbral_lineas_cambiadas": 80},
    "niveles": {
        "estandar": {"fase_red": True, "cobertura": True, "mutacion": True},
        "documental": {"fase_red": False, "cobertura": False, "mutacion": False},
    },
}


def _escribir_json(ruta: Path, datos: object) -> str:
    ruta.write_text(json.dumps(datos, ensure_ascii=False), encoding="utf-8")
    return str(ruta)


# --- R22: el timeout configurado, validado de verdad ------------------------


def test_un_timeout_entero_positivo_se_acepta() -> None:
    assert timeout_mutacion({"mutacion": {"timeout_por_mutante_s": 120}}) == 120


def test_un_booleano_no_es_un_timeout() -> None:
    """El defecto de campo: `isinstance(True, int)` es cierto y `True > 0`.

    Con `"timeout_por_mutante_s": true` la campaña concedía **1 segundo** por
    mutante y salía entera en «timeout», sin que nada avisara de que el valor
    configurado no era un número.
    """
    with pytest.raises(ValueError) as error:
        timeout_mutacion({"mutacion": {"timeout_por_mutante_s": True}})

    assert "True" in str(error.value), (
        "el mensaje tiene que enseñar el valor que se rechaza"
    )
    assert "no vale" in str(error.value)


@pytest.mark.parametrize("valor", [0, -1, -600, 1.5, "120", [], {}])
def test_un_valor_que_no_es_entero_positivo_se_rechaza(valor: object) -> None:
    with pytest.raises(ValueError) as error:
        timeout_mutacion({"mutacion": {"timeout_por_mutante_s": valor}})

    assert "no vale" in str(error.value)


@pytest.mark.parametrize(
    "rigor",
    [
        {},
        {"mutacion": {}},
        # `null` explícito cuenta como ausencia, igual que en `workers` y
        # `max_mutantes`: declarar la clave sin valor es no declararla.
        {"mutacion": {"timeout_por_mutante_s": None}},
    ],
)
def test_la_clave_ausente_dice_que_FALTA_y_no_que_no_vale(
    rigor: dict,
) -> None:
    """«Falta la clave» y «el valor no vale» son dos averías distintas.

    Hasta hoy las dos daban el mismo mensaje —«Falta 'mutacion...'»— y quien lo
    leía se ponía a buscar una clave que estaba delante de sus ojos.
    """
    with pytest.raises(ValueError) as error:
        timeout_mutacion(rigor)

    assert "Falta" in str(error.value)
    assert "no vale" not in str(error.value)


def test_los_dos_mensajes_no_son_el_mismo() -> None:
    with pytest.raises(ValueError) as ausente:
        timeout_mutacion({"mutacion": {}})
    with pytest.raises(ValueError) as invalido:
        timeout_mutacion({"mutacion": {"timeout_por_mutante_s": 0}})

    assert str(ausente.value) != str(invalido.value)


# --- R23: `--timeout 0` deja de colarse -------------------------------------


@pytest.mark.parametrize("valor", ["0", "-5"])
def test_un_timeout_no_positivo_sale_con_codigo_2(valor: str) -> None:
    """Hoy `--timeout 0` es falsy y cae en silencio al timeout configurado.

    Quien lo escribe cree haber pedido algo y recibe otra cosa: el silencio
    esconde el error en vez de corregirlo.
    """
    with pytest.raises(SystemExit) as parada:
        _analizar_argumentos(["--feature", "F-040", "--timeout", valor])

    assert parada.value.code == 2


@pytest.mark.parametrize("valor", ["0", "-3"])
def test_unos_workers_no_positivos_salen_con_codigo_2(valor: str) -> None:
    """Misma guarda, mismo motivo: `--workers 0` no es una campaña de nada."""
    with pytest.raises(SystemExit) as parada:
        _analizar_argumentos(["--feature", "F-040", "--workers", valor])

    assert parada.value.code == 2


def test_los_valores_legitimos_siguen_pasando() -> None:
    opciones = _analizar_argumentos(
        ["--feature", "F-040", "--timeout", "600", "--workers", "3"]
    )

    assert opciones.timeout == 600
    assert opciones.workers == 3
    # 1 es el valor legítimo más pequeño: la campaña en serie de toda la vida.
    assert _analizar_argumentos(["--feature", "F-040", "--workers", "1"]).workers == 1
    assert _analizar_argumentos(["--feature", "F-040", "--timeout", "1"]).timeout == 1


def test_sin_flags_no_se_valida_nada() -> None:
    opciones = _analizar_argumentos(["--feature", "F-040"])

    assert opciones.timeout is None
    assert opciones.workers is None


# --- R25: un nivel inexistente hace salir al validador con código 1 ----------


def test_una_ficha_con_nivel_inexistente_sale_con_codigo_1(
    tmp_path: Path,
) -> None:
    """El código de salida ES el contrato: `harness/init.sh` solo mira eso.

    Que la validación IMPRIMA el error no sirve de nada si devuelve 0: el
    portero da la configuración por buena y sigue.
    """
    config = _escribir_json(tmp_path / "rigor.json", RIGOR_MINIMO)
    features = _escribir_json(
        tmp_path / "features.json",
        {"features": [{"id": "F-999", "rigor": "marciano"}]},
    )

    assert rigor_main(["--config", config, "--features", features]) == 1


def test_el_error_nombra_la_ficha_y_los_niveles_validos(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config = _escribir_json(tmp_path / "rigor.json", RIGOR_MINIMO)
    features = _escribir_json(
        tmp_path / "features.json",
        {"features": [{"id": "F-999", "rigor": "marciano"}]},
    )

    rigor_main(["--config", config, "--features", features])

    error = capsys.readouterr().err
    assert "F-999" in error
    assert "marciano" in error
    assert "documental" in error and "estandar" in error


def test_un_inventario_correcto_sale_con_codigo_0(tmp_path: Path) -> None:
    """La rama nueva no puede comerse el caso normal."""
    config = _escribir_json(tmp_path / "rigor.json", RIGOR_MINIMO)
    features = _escribir_json(
        tmp_path / "features.json",
        {"features": [{"id": "F-001", "rigor": "estandar"}, {"id": "F-002"}]},
    )

    assert rigor_main(["--config", config, "--features", features]) == 0


def test_una_configuracion_ilegible_tambien_sale_con_codigo_1(
    tmp_path: Path,
) -> None:
    features = _escribir_json(tmp_path / "features.json", {"features": []})

    assert (
        rigor_main(
            ["--config", str(tmp_path / "no_existe.json"), "--features", features]
        )
        == 1
    )


# --- R26: las dos ramas de «feature sin rigor o con rigor nulo» --------------


def test_una_ficha_SIN_la_clave_rigor_no_es_un_error() -> None:
    """Rama 1 de la guarda: `"rigor" not in feature`.

    No declarar nivel es legítimo —se aplica `nivel_por_defecto`—, y hoy nada lo
    comprobaba porque las 40 fichas del inventario real declaran su rigor.
    """
    assert validar_features([{"id": "F-100"}], RIGOR_MINIMO) == []


def test_una_ficha_con_rigor_NULO_no_es_un_error() -> None:
    """Rama 2 de la guarda: `feature["rigor"] is None`.

    Es la mitad que el mutante `or` -> `and` deja viva si solo se prueba la
    primera: con `and`, una ficha sin la clave revienta con `KeyError` y una
    ficha con `null` se cuela sin validar.
    """
    assert validar_features([{"id": "F-101", "rigor": None}], RIGOR_MINIMO) == []


def test_las_dos_ramas_conviven_en_el_mismo_inventario() -> None:
    fichas = [
        {"id": "F-100"},
        {"id": "F-101", "rigor": None},
        {"id": "F-102", "rigor": "estandar"},
    ]

    assert validar_features(fichas, RIGOR_MINIMO) == []


def test_un_rigor_declarado_y_malo_si_es_un_error() -> None:
    """La guarda no puede tragárselo todo: lo declarado se valida."""
    errores = validar_features(
        [{"id": "F-100"}, {"id": "F-103", "rigor": "marciano"}], RIGOR_MINIMO
    )

    assert len(errores) == 1
    assert "F-103" in errores[0]


@pytest.mark.parametrize("declarado", [123, [], {}, False])
def test_un_rigor_declarado_que_ni_siquiera_es_texto_es_un_error(
    declarado: object,
) -> None:
    errores = validar_features([{"id": "F-104", "rigor": declarado}], RIGOR_MINIMO)

    assert len(errores) == 1


# --- R24: la retirada de worktrees cuando `git worktree remove` FALLA -------


def test_si_remove_falla_se_borra_el_directorio_y_luego_se_purga(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """El camino de rescate de Windows, con un git que falla de verdad.

    Hasta hoy solo se probaba con un git que funciona: la rama del `if codigo
    != 0` —la que existe porque en Windows un proceso rezagado deja un fichero
    abierto— no la ejercitaba nadie. Y el ORDEN importa: `prune` solo retira el
    registro de un worktree que YA no está en disco, así que purgar antes de
    borrar no desregistra nada.
    """
    import shutil as _shutil

    from harness import mutacion_paralela

    eventos: list[tuple[str, str]] = []

    def _git_que_no_sabe_borrar(_raiz: str, *args: str) -> tuple[int, str]:
        eventos.append(("git", " ".join(args)))
        if args[:2] == ("worktree", "remove"):
            return (128, "fatal: 'wk_0' contains modified or untracked files")
        return (0, "")

    def _rmtree_falso(ruta: str, **_kwargs: object) -> None:
        eventos.append(("rmtree", str(ruta)))

    monkeypatch.setattr(mutacion_paralela, "_git", _git_que_no_sabe_borrar)
    monkeypatch.setattr(_shutil, "rmtree", _rmtree_falso)

    worktrees = mutacion_paralela.Worktrees(".", 0)
    worktrees.rutas = ["/tmp/mutacion_F-040/wk_0"]
    worktrees._retirar()

    assert ("rmtree", "/tmp/mutacion_F-040/wk_0") in eventos, (
        "con `remove` en fallo hay que borrar el directorio a mano"
    )
    orden = [nombre for nombre, _ in eventos]
    borrado = orden.index("rmtree")
    purgas = [
        indice
        for indice, (nombre, args) in enumerate(eventos)
        if nombre == "git" and args == "worktree prune"
    ]
    assert purgas, "tras borrar a mano hay que desregistrar el worktree con prune"
    assert purgas[-1] > borrado, (
        "el `prune` va DESPUÉS del `rmtree`: purgar antes no desregistra nada, "
        "porque el worktree todavía está en disco"
    )
    assert worktrees.rutas == [], "la lista de worktrees queda vacía pase lo que pase"


def test_si_remove_funciona_no_se_borra_a_mano_ni_se_purga(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """El camino normal no puede pagar el precio del de rescate."""
    import shutil as _shutil

    from harness import mutacion_paralela

    eventos: list[tuple[str, str]] = []

    def _git_que_borra_bien(_raiz: str, *args: str) -> tuple[int, str]:
        eventos.append(("git", " ".join(args)))
        return (0, "")

    def _rmtree_falso(ruta: str, **_kwargs: object) -> None:
        eventos.append(("rmtree", str(ruta)))

    monkeypatch.setattr(mutacion_paralela, "_git", _git_que_borra_bien)
    monkeypatch.setattr(_shutil, "rmtree", _rmtree_falso)

    worktrees = mutacion_paralela.Worktrees(".", 0)
    worktrees.rutas = ["/tmp/mutacion_F-040/wk_0"]
    worktrees._retirar()

    assert ("rmtree", "/tmp/mutacion_F-040/wk_0") not in eventos
    assert ("git", "worktree prune") not in eventos
    assert eventos == [("git", "worktree remove --force /tmp/mutacion_F-040/wk_0")]


# --- R18 y R19: no se mide encima de un mutante viejo -----------------------

def _plantar_centinela(raiz: Path, aplicados: list[dict]) -> Path:
    """Deja en `raiz` el centinela que habría dejado una campaña muerta."""
    ruta = raiz / RUTA_CENTINELA
    ruta.parent.mkdir(parents=True, exist_ok=True)
    ruta.write_text(
        json.dumps(
            {
                "feature": "F-039",
                "modo": "serie",
                "pid": 4242,
                "inicio": "2026-08-20 23:59:59",
                "raiz": raiz.as_posix(),
                "muta_arbol_principal": True,
                "aplicados": aplicados,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return ruta


def _mutado(raiz: Path, recuperable: bool) -> dict:
    """Un fichero con un mutante escrito, con o sin datos para deshacerlo."""
    fichero = raiz / "codigo.py"
    fichero.write_text("VALOR = 2\n", encoding="utf-8")
    entrada = {
        "fichero": "codigo.py",
        "ruta": fichero.resolve().as_posix(),
        "linea": 1,
        "descripcion": "codigo.py:1 [entero] VALOR = 1 -> VALOR = 2",
        "linea_original": "VALOR = 1" if recuperable else None,
        "en_arbol_principal": True,
    }
    if not recuperable:
        # Sin `linea_original` solo queda `git checkout`, y `tmp_path` no es un
        # repositorio: ahí es donde la restauración se declara irrecuperable.
        entrada["linea_original"] = None
    return entrada


def test_restaurar_sale_con_0_cuando_no_hay_nada_que_restaurar(
    tmp_path: Path,
) -> None:
    assert _modo_restaurar(str(tmp_path)) == 0


def test_restaurar_sale_con_0_cuando_lo_restaura_todo(tmp_path: Path) -> None:
    _plantar_centinela(tmp_path, [_mutado(tmp_path, recuperable=True)])

    assert _modo_restaurar(str(tmp_path)) == 0
    assert (tmp_path / "codigo.py").read_text(encoding="utf-8").startswith("VALOR = 1")
    assert not (tmp_path / RUTA_CENTINELA).exists(), "el centinela se retira"


def test_restaurar_sale_con_2_si_algo_queda_irrecuperable(
    tmp_path: Path,
) -> None:
    _plantar_centinela(tmp_path, [_mutado(tmp_path, recuperable=False)])

    assert _modo_restaurar(str(tmp_path)) == 2


def test_el_cli_propaga_el_codigo_de_restaurar(tmp_path: Path) -> None:
    _plantar_centinela(tmp_path, [_mutado(tmp_path, recuperable=False)])

    assert mutacion_main(["--restaurar", "--raiz", str(tmp_path)]) == 2


def test_un_centinela_irrecuperable_aborta_con_3_sin_empezar(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Medir encima de un mutante viejo es medir el mutante.

    Hasta hoy `main` llamaba a `_modo_restaurar` y TIRABA su código de salida:
    si la restauración no podía deshacer algo, la campaña arrancaba igual sobre
    un árbol que ya estaba mutado, y todos sus veredictos hablaban de un código
    que nadie había escrito.
    """

    def _prohibido(*_args: object, **_kwargs: object):
        raise AssertionError(
            "con un mutante viejo sin deshacer no se calcula alcance ni se muta"
        )

    monkeypatch.setattr("harness.mutacion.alcance_de_feature", _prohibido)
    monkeypatch.setattr("harness.mutacion.ejecutar_campania", _prohibido)
    _plantar_centinela(tmp_path, [_mutado(tmp_path, recuperable=False)])

    assert mutacion_main(["--feature", "F-040", "--raiz", str(tmp_path)]) == 3


def test_el_aborto_dice_que_el_arbol_sigue_mutado(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(
        "harness.mutacion.alcance_de_feature",
        lambda *_a, **_k: (_ for _ in ()).throw(AssertionError("no se llega aquí")),
    )
    _plantar_centinela(tmp_path, [_mutado(tmp_path, recuperable=False)])

    mutacion_main(["--feature", "F-040", "--raiz", str(tmp_path)])

    error = capsys.readouterr().err
    assert "codigo.py" in error, "hay que nombrar el fichero que sigue mutado"
    assert "mutante" in error.lower()


def test_un_centinela_que_SI_se_restaura_deja_seguir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """La guarda nueva no puede bloquear el caso que ya funcionaba.

    Un centinela restaurable es la situación normal tras un Ctrl-C: se deshace
    y la campaña continúa.
    """
    llegadas: list[str] = []

    def _alcance_espia(*_args: object, **_kwargs: object):
        llegadas.append("alcance")
        raise SystemExit("hasta aquí basta: la guarda dejó pasar")

    monkeypatch.setattr("harness.mutacion.alcance_de_feature", _alcance_espia)
    _plantar_centinela(tmp_path, [_mutado(tmp_path, recuperable=True)])

    assert mutacion_main(["--feature", "F-040", "--raiz", str(tmp_path)]) == 2
    assert llegadas == ["alcance"]
