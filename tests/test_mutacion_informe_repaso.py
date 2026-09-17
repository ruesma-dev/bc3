# tests/test_mutacion_informe_repaso.py
"""El informe tiene que decir qué pasó con los mutantes en `timeout`.

Desde que la campaña paralela los repasa en serie, «0 timeouts» ya no distingue
tres situaciones muy distintas: una campaña sin incidencias, una que las tuvo y
las resolvió repasando, y una campaña en serie que no repasa porque no le hace
falta. Sin decirlo por escrito, quien lee el informe no puede saber en cuál está.

Y la cabecera cita ahora el comando ENTERO, `--workers` incluido: sin ese dato
el «Tiempo total» no se puede interpretar ni reproducir. Pasó de verdad el
2026-09-02, reconstruyendo una campaña de la que solo quedaba el informe.
"""

from __future__ import annotations

from pathlib import Path

from harness.alcance import Alcance
from harness.mutacion import InformeMutacion, Mutante, escribir_informe

ALCANCE = Alcance(
    feature="F-000", origen="rama", ref_diff=("dev", "rama"), lineas={"codigo.py": {2}}
)


def mutante(linea: int) -> Mutante:
    return Mutante(
        fichero="codigo.py",
        linea=linea,
        col=0,
        original="==",
        mutado="!=",
        operador="comparacion",
    )


def informe(**campos: object) -> InformeMutacion:
    base: dict[str, object] = {
        "feature": "F-000",
        "alcance": ALCANCE,
        "generados": 3,
        "muertos": 3,
        "mutantes_evaluados": [mutante(2), mutante(3), mutante(4)],
        "segundos": 120.0,
    }
    base.update(campos)
    return InformeMutacion(**base)  # type: ignore[arg-type]


def escrito(informe_: InformeMutacion, tmp_path: Path) -> str:
    destino = tmp_path / "mutacion_F-000.md"
    escribir_informe(informe_, destino)
    return destino.read_text(encoding="utf-8")


def fila(texto: str, prefijo: str = "Timeouts repasados") -> str:
    return next(linea for linea in texto.splitlines() if prefijo in linea)


# --- El comando que reproduce la campaña ------------------------------------


def test_la_cabecera_cita_el_comando_con_sus_workers(tmp_path: Path) -> None:
    """Medio comando no reproduce nada: con 1 worker y con 16 no es lo mismo."""
    texto = escrito(informe(workers=8), tmp_path)

    assert "--feature F-000 --workers 8" in texto


def test_sin_saber_los_workers_la_cabecera_no_se_inventa_ninguno(
    tmp_path: Path,
) -> None:
    texto = escrito(informe(), tmp_path)

    assert "--feature F-000`" in texto
    assert "--workers" not in texto.split("## Alcance", 1)[0]


# --- Qué pasó con los timeouts ----------------------------------------------


def test_el_informe_dice_cuantos_se_repasaron_y_a_cuantos_les_saco_veredicto(
    tmp_path: Path,
) -> None:
    texto = escrito(
        informe(muertos=2, timeouts=[mutante(4)], timeouts_repasados=3), tmp_path
    )

    assert "3" in fila(texto), "no se ve cuántos se repasaron"
    assert "2" in fila(texto), "no se ve a cuántos les sacó veredicto"


def test_sin_un_solo_timeout_la_fila_del_repaso_lo_dice(tmp_path: Path) -> None:
    """«0 repasados» a secas se confunde con un arnés que no repasa."""
    assert "ningún mutante agotó el reloj" in fila(escrito(informe(), tmp_path))


def test_una_campania_en_serie_explica_por_que_no_repasa(tmp_path: Path) -> None:
    """En serie el reloj ya midió al mutante a solas: repasar no aportaría nada."""
    texto = escrito(
        informe(muertos=2, timeouts=[mutante(4)], timeouts_repasados=0, workers=1),
        tmp_path,
    )

    assert "serie" in fila(texto)


def test_la_seccion_de_timeouts_avisa_de_que_sobrevivieron_al_repaso(
    tmp_path: Path,
) -> None:
    """Tras el repaso, un timeout ya no tiene la contención como excusa."""
    texto = escrito(
        informe(muertos=2, timeouts=[mutante(4)], timeouts_repasados=3), tmp_path
    )

    cuerpo = texto.split("## Timeouts", 1)[1].lower()
    assert "repas" in cuerpo, "no se dice que estos ya pasaron por el repaso"
    assert "serie" in cuerpo, "no se dice que el repaso fue sin concurrencia"


def test_los_repasados_que_siguen_colgados_se_cuentan_aparte(
    tmp_path: Path,
) -> None:
    informe_ = informe(muertos=1, timeouts=[mutante(3), mutante(4)], timeouts_repasados=3)

    assert informe_.timeouts_resueltos == 1
    assert "2 en timeout todavía" in fila(escrito(informe_, tmp_path))
