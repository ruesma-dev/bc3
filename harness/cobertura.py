# harness/cobertura.py
"""Puerta de cobertura de las LÍNEAS CAMBIADAS por la feature en curso.

No mide la cobertura global del repositorio —esa es deuda histórica, se ve
pero no bloquea—, sino la de lo que esta feature acaba de escribir. Cruza el
JSON de `coverage` con el mismo alcance de diff que usa la mutación: una sola
fuente de verdad para el «qué cambió».

La puerta decide ella misma si aplica. No aplica en la rama de integración, ni
cuando no hay líneas Python de producción cambiadas, ni cuando el nivel de
rigor de la feature no exige cobertura: en esos casos se declara N/A **con el
motivo escrito**, que es distinto de aprobar en silencio.
"""

from __future__ import annotations

import argparse
import ast
import importlib.util
import json
import sys
from pathlib import Path

from harness.alcance import alcance_de_feature, ejecutar_git
from harness.rigor import (
    RUTA_FEATURES,
    RUTA_RIGOR,
    cargar_features,
    cargar_rigor,
    exige,
    feature_de_rama,
    nivel_de_feature,
    umbral_cobertura,
)
from harness.servicios import Servicio, cargar_servicios

ETIQUETA = "PUERTA COBERTURA"
MENSAJE_INSTALACION = (
    "falta la medición de cobertura y la puerta aplica: "
    "pip install -r requirements-dev.txt"
)


def hay_coverage() -> bool:
    """¿Está instalada la herramienta de medición?"""
    return importlib.util.find_spec("coverage") is not None


def rama_actual(raiz: str = ".") -> str:
    return ejecutar_git(["branch", "--show-current"], raiz=raiz).strip()


def lineas_ejecutables(fuente: str) -> set[int]:
    """Líneas con sentencia de un fuente Python.

    Sirve para los ficheros que NO aparecen en el informe de coverage: un
    módulo nuevo que ningún test importa no puede salir gratis por no haberse
    medido.
    """
    try:
        arbol = ast.parse(fuente)
    except SyntaxError:
        return set()
    return {
        nodo.lineno
        for nodo in ast.walk(arbol)
        if isinstance(nodo, ast.stmt) and nodo.lineno
    }


def _indexar(cov: dict) -> dict[str, dict]:
    """Normaliza las rutas del JSON de coverage (separadores del sistema)."""
    return {
        ruta.replace("\\", "/"): datos for ruta, datos in cov.get("files", {}).items()
    }


def fusionar_coberturas(
    cov_raiz: dict | None, coberturas_servicios: list[tuple[str, dict]]
) -> dict:
    """Une el informe de la raíz con el de cada servicio, re-prefijando rutas.

    Función pura. Un servicio ejecuta su suite desde SU directorio, así que su
    `coverage.json` numera las rutas respecto a él (`app/flujo.py`), mientras
    que el alcance las numera respecto a la raíz del repositorio
    (`services/email/app/flujo.py`). Sin re-prefijar, ningún fichero de
    servicio casaría con el alcance y todos saldrían «no medidos».
    """
    fusionado: dict[str, dict] = dict(_indexar(cov_raiz or {}))
    for ruta_servicio, cobertura_servicio in coberturas_servicios:
        prefijo = ruta_servicio.replace("\\", "/").strip("/")
        for ruta, datos in _indexar(cobertura_servicio).items():
            fusionado[f"{prefijo}/{ruta}" if prefijo else ruta] = datos
    return {"files": fusionado}


def cobertura_lineas_cambiadas(
    cov: dict, lineas: dict[str, set[int]], raiz: str = "."
) -> tuple[int, int]:
    """Devuelve (líneas cubiertas, líneas ejecutables) dentro del alcance."""
    medidos = _indexar(cov)
    cubiertas = 0
    totales = 0

    for fichero, numeros in lineas.items():
        clave = fichero.replace("\\", "/")
        datos = medidos.get(clave)
        if datos is None:
            ruta = Path(raiz) / clave
            if not ruta.is_file():
                continue
            ejecutables = lineas_ejecutables(ruta.read_text(encoding="utf-8"))
            totales += len(numeros & ejecutables)
            continue
        ejecutadas = set(datos.get("executed_lines", []))
        ausentes = set(datos.get("missing_lines", []))
        relevantes = numeros & (ejecutadas | ausentes)
        totales += len(relevantes)
        cubiertas += len(relevantes & ejecutadas)

    return cubiertas, totales


def _leer_cobertura(ruta: Path) -> dict | None:
    """Lee un informe de coverage; `None` si no existe, error si no es JSON."""
    if not ruta.is_file():
        return None
    try:
        return json.loads(ruta.read_text(encoding="utf-8"))
    except ValueError as error:
        raise ValueError(f"{ruta.as_posix()} no es JSON válido: {error}") from error


def coberturas_de_servicios(
    servicios: list[Servicio], raiz: str, nombre_informe: str
) -> list[tuple[str, dict]]:
    """Informes de coverage de los servicios Python que tengan uno.

    Un servicio sin informe no es un error aquí: sus ficheros cambiados caen en
    el mecanismo de «no medido» y cuentan como no cubiertos (que es la verdad).
    """
    encontradas: list[tuple[str, dict]] = []
    for servicio in servicios:
        if servicio.lenguaje != "python":
            continue
        datos = _leer_cobertura(Path(raiz) / servicio.ruta / nombre_informe)
        if datos is not None:
            encontradas.append((servicio.ruta, datos))
    return encontradas


def _na(motivo: str) -> int:
    print(f"{ETIQUETA}: N/A ({motivo})")
    return 0


def _ko(motivo: str) -> int:
    print(f"{ETIQUETA}: {motivo}", file=sys.stderr)
    return 1


def main(argv: list[str] | None = None) -> int:
    """Puerta de cobertura: 0 si pasa o no aplica, 1 si falla."""
    analizador = argparse.ArgumentParser(
        prog="python -m harness.cobertura",
        description="Comprueba la cobertura de las líneas cambiadas por la feature.",
    )
    analizador.add_argument("--base", default="dev", help="Rama de integración")
    analizador.add_argument("--config", default=str(RUTA_RIGOR))
    analizador.add_argument("--features", default=str(RUTA_FEATURES))
    analizador.add_argument("--cov", default="coverage.json")
    analizador.add_argument("--raiz", default=".")
    opciones = analizador.parse_args(argv)

    try:
        rigor = cargar_rigor(opciones.config)
    except ValueError as error:
        return _ko(str(error))

    rama = rama_actual(opciones.raiz)
    if not rama or rama in (opciones.base, "main", "master"):
        return _na(f"rama {rama or '(detached)'}: solo aplica en ramas de feature")

    feature = feature_de_rama(rama, cargar_features(opciones.features))
    if feature is None:
        return _na(f"la rama {rama} no corresponde a ninguna feature declarada")

    nivel = nivel_de_feature(feature, rigor)
    if not exige(nivel, "cobertura", rigor):
        return _na(f"{feature.get('id')} es de nivel {nivel}: no exige cobertura")

    try:
        alcance = alcance_de_feature(
            feature.get("id", ""),
            base=opciones.base,
            rama=rama,
            raiz=opciones.raiz,
        )
    except SystemExit as parada:
        return _na(f"no se pudo calcular el alcance: {parada}")

    if not alcance.lineas:
        return _na(
            f"{feature.get('id')} no cambia líneas Python de producción frente a "
            f"{opciones.base}"
        )

    if not hay_coverage():
        return _ko(MENSAJE_INSTALACION)

    ruta_cov = Path(opciones.cov)
    try:
        servicios = cargar_servicios(raiz=opciones.raiz)
        cov_raiz = _leer_cobertura(ruta_cov)
        por_servicio = coberturas_de_servicios(servicios, opciones.raiz, ruta_cov.name)
    except ValueError as error:
        return _ko(str(error))

    if cov_raiz is None and not por_servicio:
        return _ko(MENSAJE_INSTALACION)
    # Sin servicios declarados esto es exactamente el informe de la raíz: el
    # repositorio de un solo proyecto sigue el camino de siempre.
    datos = fusionar_coberturas(cov_raiz, por_servicio) if servicios else cov_raiz or {}

    cubiertas, totales = cobertura_lineas_cambiadas(datos, alcance.lineas, opciones.raiz)
    if totales == 0:
        return _na(
            f"{feature.get('id')}: las líneas cambiadas no contienen sentencias "
            "ejecutables"
        )

    umbral = umbral_cobertura(rigor)
    porcentaje = 100.0 * cubiertas / totales
    resumen = (
        f"{porcentaje:.1f}% de {totales} líneas cambiadas cubiertas "
        f"({cubiertas}/{totales}, umbral {umbral}%, nivel {nivel})"
    )
    if porcentaje + 1e-9 < umbral:
        return _ko(resumen)
    print(f"{ETIQUETA}: {resumen}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
