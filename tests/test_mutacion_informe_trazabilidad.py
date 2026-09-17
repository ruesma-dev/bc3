# tests/test_mutacion_informe_trazabilidad.py
"""El informe de mutación trae los datos que si no hay que pedir a mano.

Las dos reglas que más tokens habrían ahorrado en F-034 (RM1 y RM2) exigen dos
datos que hasta hoy no estaban en el informe:

- Contra qué commit se midió. La rama de F-034 creció de 56 a 1.057 líneas
  DESPUÉS de medir, y el informe seguía pareciendo válido: ~200.000 tokens de
  primer rechazo.
- Cuánto tarda una suite limpia ahí. Se declararon 18 mutantes en 111 s cuando
  la realidad eran 63 minutos; con la línea base y la media por mutante
  impresas, la incoherencia se ve leyendo, sin reejecutar nada (~350.000
  tokens de segundo rechazo).
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

import pytest

from harness.alcance import Alcance
from harness.mutacion import (
    PYTEST_FALLOS,
    PYTEST_OK,
    BaseRota,
    InformeMutacion,
    Mutante,
    ResultadoSuite,
    comprobar_linea_base,
    ejecutar_campania,
    escribir_informe,
    sha_de_head,
)
from harness.mutacion_paralela import fusionar

FUENTE = "# app.py\ndef bandera():\n    return True\n"


class _EjecutorFalso:
    """Interfaz mínima que `ejecutar_campania` espera, sin lanzar procesos."""

    def __init__(self, raiz: str = ".", base: ResultadoSuite | None = None) -> None:
        self.raiz = raiz
        self.base = base or ResultadoSuite(codigo=PYTEST_OK)

    def identidad(self) -> tuple[str, str]:
        return (self.raiz, "falso")

    def ejecutar(self, timeout_s: int) -> str:
        return "muerto"

    def linea_base(self, timeout_s: int) -> ResultadoSuite:
        return self.base


class _SinLineaBase:
    """Doble que no sabe correr una suite: no puede aportar un tiempo."""

    raiz = "."

    def ejecutar(self, timeout_s: int) -> str:
        return "muerto"


@pytest.fixture
def arbol(tmp_path: Path) -> Path:
    """Repositorio git de un solo fichero, commiteado y limpio."""
    subprocess.run(["git", "init", "-q", "-b", "dev", str(tmp_path)], check=True)
    subprocess.run(
        ["git", "-C", str(tmp_path), "config", "user.email", "arnes@ruesma.es"], check=True
    )
    subprocess.run(["git", "-C", str(tmp_path), "config", "user.name", "Arnes"], check=True)
    (tmp_path / "app.py").write_text(FUENTE, encoding="utf-8", newline="")
    subprocess.run(["git", "-C", str(tmp_path), "add", "-A"], check=True)
    subprocess.run(["git", "-C", str(tmp_path), "commit", "-q", "-m", "inicial"], check=True)
    return tmp_path


def _alcance() -> Alcance:
    return Alcance(
        feature="F-999",
        origen="rama",
        ref_diff=("dev", "feature/F-999-prueba"),
        lineas={"app.py": {3}},
    )


# --- R11: la línea base deja su tiempo medido -------------------------------


def test_comprobar_linea_base_devuelve_los_segundos_de_cada_ejecutor() -> None:
    tiempos = comprobar_linea_base(
        [("raiz", _EjecutorFalso()), ("servicio", _EjecutorFalso(raiz="s"))], 10
    )

    assert sorted(tiempos) == ["raiz", "servicio"]
    assert all(segundos >= 0.0 for segundos in tiempos.values())


def test_un_ejecutor_sin_linea_base_no_inventa_un_tiempo() -> None:
    """Mejor `n/d` que un cero: un cero se suma y se lee como medición."""
    assert comprobar_linea_base([("doble", _SinLineaBase())], 10) == {}


def test_una_base_roja_sigue_abortando_la_campania() -> None:
    """El tiempo es un dato de más; el veredicto de la base no cambia."""
    with pytest.raises(BaseRota):
        comprobar_linea_base(
            [("raiz", _EjecutorFalso(base=ResultadoSuite(codigo=PYTEST_FALLOS)))], 10
        )


def test_la_campania_guarda_el_tiempo_de_la_base_en_su_informe(
    arbol: Path,
) -> None:
    informe = ejecutar_campania(
        _alcance(), _EjecutorFalso(raiz=str(arbol)), raiz=str(arbol)
    )

    assert list(informe.segundos_linea_base) == [Path(arbol).as_posix()]


# --- R10: contra qué commit se midió ----------------------------------------


def test_la_campania_declara_el_sha_completo_de_head(arbol: Path) -> None:
    esperado = subprocess.run(
        ["git", "-C", str(arbol), "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()

    informe = ejecutar_campania(
        _alcance(), _EjecutorFalso(raiz=str(arbol)), raiz=str(arbol)
    )

    assert informe.sha_head == esperado
    assert re.fullmatch(r"[0-9a-f]{40}", informe.sha_head or ""), "completo, no abreviado"


def test_fuera_de_un_repositorio_no_se_inventa_un_sha(tmp_path: Path) -> None:
    assert sha_de_head(str(tmp_path)) is None


# --- R12: la campaña paralela propaga lo que midieron sus workers -----------


def test_la_agregacion_propaga_sha_y_tiempos_de_los_workers() -> None:
    alcance = _alcance()
    parciales = [
        InformeMutacion(
            feature="F-999",
            alcance=alcance,
            sha_head="a" * 40,
            segundos_linea_base={"wk_0": 12.0},
        ),
        InformeMutacion(
            feature="F-999",
            alcance=alcance,
            sha_head="a" * 40,
            segundos_linea_base={"wk_1": 13.0},
        ),
    ]

    informe = fusionar(alcance, parciales, generados=2, segundos=30.0)

    assert informe.sha_head == "a" * 40
    assert informe.segundos_linea_base == {"wk_0": 12.0, "wk_1": 13.0}


def test_sin_parciales_no_hay_dato_que_propagar() -> None:
    """El caso de R12: el informe tendrá que imprimir `n/d`, no un cero."""
    informe = fusionar(_alcance(), [], generados=0, segundos=0.0)

    assert informe.sha_head is None
    assert informe.segundos_linea_base == {}


# --- R9, R10, R11, R12: lo que se lee en el informe -------------------------


def _informe_escrito(tmp_path: Path, **campos) -> str:
    alcance = _alcance()
    mutante = Mutante(
        fichero="app.py",
        linea=3,
        col=11,
        original="return True",
        mutado="return False",
        operador="booleano",
    )
    informe = InformeMutacion(
        feature="F-999",
        alcance=alcance,
        generados=campos.pop("generados", 1),
        muertos=1,
        mutantes_evaluados=[mutante],
        segundos=60.0,
        **campos,
    )
    ruta = tmp_path / "mutacion_F-999.md"
    escribir_informe(informe, ruta)
    return ruta.read_text(encoding="utf-8")


def test_el_informe_declara_el_sha_completo_contra_el_que_se_midio(
    tmp_path: Path,
) -> None:
    texto = _informe_escrito(tmp_path, sha_head="b" * 40)

    assert f"| SHA de HEAD medido | `{'b' * 40}` |" in texto


def test_el_informe_declara_la_linea_base_y_la_media_por_mutante(
    tmp_path: Path,
) -> None:
    """Con estos dos números, RM2 se comprueba leyendo, sin reejecutar nada."""
    texto = _informe_escrito(tmp_path, segundos_linea_base={"services/sv2": 118.4})

    assert "| Línea base (s) — `services/sv2` | 118.4 |" in texto
    assert "| Media por mutante evaluado (s) | 60.0 |" in texto


def test_sin_datos_el_informe_dice_n_d_y_no_omite_la_fila(
    tmp_path: Path,
) -> None:
    """Un cero se lee como medición; una fila ausente se lee como descuido."""
    texto = _informe_escrito(tmp_path)

    assert "| SHA de HEAD medido | n/d |" in texto
    assert "| Línea base (s) | n/d |" in texto
    assert "| Línea base (s) | 0" not in texto


def test_sin_mutantes_evaluados_la_media_tambien_es_n_d(
    tmp_path: Path,
) -> None:
    alcance = _alcance()
    ruta = tmp_path / "vacio.md"
    escribir_informe(InformeMutacion(feature="F-999", alcance=alcance), ruta)

    assert "| Media por mutante evaluado (s) | n/d |" in ruta.read_text(encoding="utf-8")


def test_una_campania_muestreada_declara_cuantos_semilla_y_nivel(
    tmp_path: Path,
) -> None:
    texto = _informe_escrito(
        tmp_path,
        generados=61,
        muestreado=True,
        max_mutantes=20,
        semilla=20260820,
        nivel="estandar",
    )

    assert "Muestreo | sí — 1 de 61 mutantes, semilla `20260820`, nivel `estandar`" in texto


def test_una_campania_completa_lo_sigue_diciendo_asi(tmp_path: Path) -> None:
    assert "| Muestreo | no: campaña completa |" in _informe_escrito(tmp_path)
