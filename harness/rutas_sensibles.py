# harness/rutas_sensibles.py
"""Rutas sensibles: zonas del repositorio que exigen una verificación extra.

Hay ficheros cuyo diff no lo cubren los tests unitarios por bien escritos que
estén: un prompt de IA, un schema que la IA rellena, el cliente que habla con
el proveedor, la red determinista que decide un precio. Cambiarlos puede dejar
la suite en verde y el sistema roto.

Este módulo es el mecanismo GENÉRICO: se declaran a mano en
`harness/rutas_sensibles.json` una o más verificaciones, cada una con sus rutas
(glob), el comando que las ejecuta, el informe que dejan y qué líneas debe
contener ese informe para valerse. La puerta compara el diff de la feature en
curso contra esas rutas y comprueba **el informe**; nunca ejecuta la
verificación —sería caro y convertiría el portero en la factura del mes—.

Dos reglas heredadas de `harness/servicios.py`:

- **Sin fichero, no cambia nada.** La ausencia es la configuración del caso
  mayoritario y el arnés sigue el camino de siempre.
- **Con fichero roto, el arnés falla.** Degradar en silencio a «sin puerta»
  dejaría zonas enteras sin vigilar mientras el portero imprime que todo va
  bien, que es la peor de las salidas posibles.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from fnmatch import fnmatch
from pathlib import Path

from harness.alcance import (
    EjecutorGit,
    git_en,
    parsear_diff,
    rama_de_feature,
    resolver_refs,
)

#: Ruta (relativa a la raíz del repositorio) de la declaración.
RUTA_DECLARACION = Path("harness/rutas_sensibles.json")

#: Qué pasa cuando la verificación no está hecha: `bloqueo` tumba el arnés,
#: `aviso` solo lo dice. Una verificación nueva suele arrancar en `aviso` y
#: subirse a `bloqueo` cuando su evidencia ya es fiable.
EXIGENCIAS: tuple[str, ...] = ("bloqueo", "aviso")

#: Código de salida de `--puerta` cuando la exigencia es `aviso` y falta la
#: evidencia. `init.sh` lo traduce a [AVISO]; 0 es [OK] y 1 es [KO].
CODIGO_AVISO = 3


class ErrorDeclaracion(ValueError):
    """La declaración de rutas sensibles no se puede usar tal como está."""


@dataclass(frozen=True)
class RutaSensible:
    """Un patrón de rutas y POR QUÉ tocarlas obliga a verificar algo más."""

    patron: str
    motivo: str


@dataclass(frozen=True)
class Coincidencia:
    """Un fichero del diff que ha caído dentro de una ruta declarada."""

    fichero: str
    patron: str
    motivo: str


@dataclass(frozen=True)
class Verificacion:
    """Una verificación extra declarada: qué protege y con qué se demuestra."""

    nombre: str
    comando: str
    informe: str
    exigencia: str
    rutas: tuple[RutaSensible, ...]
    exige_lineas: tuple[str, ...] = ()

    def comando_para(self, feature: str) -> str:
        return self.comando.replace("{feature}", feature)

    def informe_para(self, feature: str) -> str:
        return self.informe.replace("{feature}", feature)


@dataclass(frozen=True)
class ResultadoPuerta:
    """Veredicto de la puerta: qué imprimir y con qué código salir."""

    codigo: int
    mensaje: str


# --- Carga y validación de la declaración -----------------------------------


def _texto(entrada: dict, clave: str, nombre: str) -> str:
    valor = entrada.get(clave)
    if not isinstance(valor, str) or not valor.strip():
        raise ErrorDeclaracion(
            f"verificación '{nombre}': el campo '{clave}' debe ser un texto no "
            f"vacío, y es {valor!r}"
        )
    return valor


def _verificacion_de_entrada(entrada: object) -> Verificacion:
    if not isinstance(entrada, dict):
        raise ErrorDeclaracion(
            f"cada verificación debe ser un objeto JSON con 'nombre', 'comando', "
            f"'informe', 'exigencia' y 'rutas', y hay una que es {entrada!r}"
        )

    nombre = entrada.get("nombre")
    if not isinstance(nombre, str) or not nombre.strip():
        raise ErrorDeclaracion(f"hay una verificación sin 'nombre' válido: {entrada!r}")

    exigencia = entrada.get("exigencia")
    if exigencia not in EXIGENCIAS:
        raise ErrorDeclaracion(
            f"verificación '{nombre}': 'exigencia' debe ser uno de "
            f"{list(EXIGENCIAS)}, y es {exigencia!r}"
        )

    declaradas = entrada.get("rutas")
    if not isinstance(declaradas, list) or not declaradas:
        raise ErrorDeclaracion(
            f"verificación '{nombre}': 'rutas' debe ser una lista con al menos "
            f"una entrada. Una verificación sin rutas no protege nada."
        )

    rutas = []
    for ruta in declaradas:
        if not isinstance(ruta, dict):
            raise ErrorDeclaracion(
                f"verificación '{nombre}': cada ruta debe ser un objeto con "
                f"'patron' y 'motivo', y hay una que es {ruta!r}"
            )
        rutas.append(
            RutaSensible(
                patron=_texto(ruta, "patron", nombre),
                motivo=_texto(ruta, "motivo", nombre),
            )
        )

    exige = entrada.get("exige_lineas", [])
    if not isinstance(exige, list) or any(not isinstance(x, str) for x in exige):
        raise ErrorDeclaracion(
            f"verificación '{nombre}': 'exige_lineas' debe ser una lista de textos"
        )

    return Verificacion(
        nombre=nombre,
        comando=_texto(entrada, "comando", nombre),
        informe=_texto(entrada, "informe", nombre),
        exigencia=exigencia,
        rutas=tuple(rutas),
        exige_lineas=tuple(exige),
    )


def cargar_declaracion(
    ruta: Path | str = RUTA_DECLARACION, raiz: str = "."
) -> list[Verificacion]:
    """Carga la declaración; lista vacía si no existe (R21), error si está rota."""
    fichero = Path(ruta)
    if not fichero.is_absolute():
        fichero = Path(raiz) / fichero
    if not fichero.is_file():
        return []

    try:
        datos = json.loads(fichero.read_text(encoding="utf-8"))
    except ValueError as error:
        raise ErrorDeclaracion(
            f"{fichero.name} no es JSON válido: {error}"
        ) from error

    declaradas = datos.get("verificaciones") if isinstance(datos, dict) else None
    if not isinstance(declaradas, list):
        raise ErrorDeclaracion(
            f"{fichero.name}: falta la lista 'verificaciones' (un objeto JSON con "
            f"la clave 'verificaciones')"
        )

    verificaciones = [_verificacion_de_entrada(entrada) for entrada in declaradas]
    vistos: set[str] = set()
    for verificacion in verificaciones:
        if verificacion.nombre in vistos:
            raise ErrorDeclaracion(
                f"nombre de verificación duplicado en la declaración: "
                f"'{verificacion.nombre}'"
            )
        vistos.add(verificacion.nombre)
    return verificaciones


def validar(verificaciones: list[Verificacion], raiz: str = ".") -> None:
    """Comprueba que cada patrón declarado case con al menos un fichero real.

    Un patrón muerto —el servicio se renombró, el fichero se borró— hace que el
    arnés diga que protege algo que ya no existe. Vale más que salte aquí.
    """
    base = Path(raiz)
    existentes = [
        ruta.relative_to(base).as_posix()
        for ruta in base.rglob("*")
        if ruta.is_file() and not _ignorado(ruta.relative_to(base).as_posix())
    ]
    for verificacion in verificaciones:
        for ruta in verificacion.rutas:
            if not any(_casa(fichero, ruta.patron) for fichero in existentes):
                raise ErrorDeclaracion(
                    f"verificación '{verificacion.nombre}': el patrón "
                    f"'{ruta.patron}' no casa con ningún fichero del "
                    f"repositorio. Una declaración muerta es un aviso falso de "
                    f"protección: corrígela o bórrala."
                )


_DIRECTORIOS_IGNORADOS = ("/.git/", "/.venv/", "/__pycache__/", "/node_modules/")


def _ignorado(ruta: str) -> bool:
    camino = f"/{ruta}"
    return any(directorio in camino for directorio in _DIRECTORIOS_IGNORADOS)


# --- Cotejo del diff contra las rutas declaradas ----------------------------


def _normalizar(ruta: str) -> str:
    return ruta.replace("\\", "/").strip("/")


def _casa(fichero: str, patron: str) -> bool:
    """`fnmatch` con `**` entendido como «y todo lo que cuelgue de ahí»."""
    normalizado = _normalizar(fichero)
    patron = _normalizar(patron)
    if fnmatch(normalizado, patron):
        return True
    if patron.endswith("/**") and fnmatch(normalizado, patron[:-3]):
        return True
    return patron.endswith("**") and normalizado.startswith(patron[:-2])


def ficheros_tocados(
    feature_id: str,
    base: str = "dev",
    rama: str | None = None,
    raiz: str = ".",
    git: EjecutorGit | None = None,
) -> list[str]:
    """Ficheros que toca la feature, TODOS: aquí un YAML cuenta igual que un .py.

    Reutiliza el cálculo de `harness.alcance` (resolución de referencias y
    parseo del diff) para no tener dos ideas distintas de «qué cambió», pero
    sin su filtro de solo-Python.
    """
    git = git or git_en(raiz)
    if rama is None:
        rama = rama_de_feature(feature_id, str(Path(raiz) / "harness/features.json"))
    ref_a, ref_b, _ = resolver_refs(feature_id, rama, base, git=git)
    return sorted(parsear_diff(git(["diff", ref_a, ref_b])))


def rutas_tocadas(
    verificacion: Verificacion, ficheros: list[str]
) -> list[Coincidencia]:
    """Qué ficheros del diff caen dentro de las rutas de esta verificación."""
    coincidencias = []
    for fichero in ficheros:
        for ruta in verificacion.rutas:
            if _casa(fichero, ruta.patron):
                coincidencias.append(
                    Coincidencia(
                        fichero=_normalizar(fichero),
                        patron=ruta.patron,
                        motivo=ruta.motivo,
                    )
                )
                break
    return coincidencias


def informe_incumple(texto: str, exige_lineas: tuple[str, ...]) -> list[str]:
    """Qué exigencias declaradas NO cumple el informe."""
    return [linea for linea in exige_lineas if linea not in texto]


# --- La puerta --------------------------------------------------------------


def evaluar_puerta(
    verificaciones: list[Verificacion],
    *,
    feature: str,
    rama: str,
    base: str = "dev",
    raiz: Path | str = ".",
    git: EjecutorGit | None = None,
) -> ResultadoPuerta:
    """Decide si la feature en curso ha hecho sus verificaciones extra.

    NO ejecuta ninguna verificación: solo mira el diff y lee informes (R26).
    """
    raiz = Path(raiz)
    if not verificaciones:
        return ResultadoPuerta(0, "")
    if not feature or not rama:
        return ResultadoPuerta(
            0,
            "PUERTA RUTAS SENSIBLES: N/A (sin feature en curso con rama: no hay "
            "diff que cotejar)",
        )

    ficheros = ficheros_tocados(feature, base, rama, str(raiz), git)
    lineas: list[str] = []
    peor = 0

    for verificacion in verificaciones:
        coincidencias = rutas_tocadas(verificacion, ficheros)
        if not coincidencias:
            lineas.append(
                f"PUERTA RUTAS SENSIBLES [{verificacion.nombre}]: N/A "
                f"({feature} no toca ninguna ruta sensible declarada)"
            )
            continue

        ruta_informe = raiz / verificacion.informe_para(feature)
        if ruta_informe.is_file():
            faltan = informe_incumple(
                ruta_informe.read_text(encoding="utf-8"), verificacion.exige_lineas
            )
        else:
            faltan = [f"no existe {verificacion.informe_para(feature)}"]

        detalle = "\n".join(
            f"      - {c.fichero} ({c.motivo})" for c in sorted(set(coincidencias), key=lambda c: c.fichero)
        )
        if not faltan:
            lineas.append(
                f"PUERTA RUTAS SENSIBLES [{verificacion.nombre}]: "
                f"{len(coincidencias)} ruta(s) tocada(s) y "
                f"{verificacion.informe_para(feature)} lo respalda"
            )
            continue

        peor = max(peor, 1 if verificacion.exigencia == "bloqueo" else CODIGO_AVISO)
        etiqueta = "FALTA" if verificacion.exigencia == "bloqueo" else "aviso: falta"
        lineas.append(
            f"PUERTA RUTAS SENSIBLES [{verificacion.nombre}]: {etiqueta} la "
            f"evidencia de {len(coincidencias)} ruta(s) sensible(s) tocada(s):\n"
            f"{detalle}\n"
            f"      Sin cumplir: {', '.join(faltan)}\n"
            f"      Lánzalo con: {verificacion.comando_para(feature)}"
        )

    if peor == CODIGO_AVISO:
        return ResultadoPuerta(CODIGO_AVISO, "\n".join(lineas))
    return ResultadoPuerta(peor, "\n".join(lineas))


# --- CLI --------------------------------------------------------------------


def _feature_en_curso(raiz: Path) -> tuple[str, str]:
    """La feature `in_progress` del inventario y su rama; vacío si no hay."""
    fichero = raiz / "harness/features.json"
    if not fichero.is_file():
        return ("", "")
    try:
        datos = json.loads(fichero.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return ("", "")
    for feature in datos.get("features", []):
        if feature.get("status") == "in_progress":
            return (feature.get("id", ""), feature.get("branch", "") or "")
    return ("", "")


def main(argv: list[str] | None = None) -> int:
    """`--validar` (la declaración es sana) o `--puerta` (¿hay evidencia?)."""
    analizador = argparse.ArgumentParser(
        prog="python -m harness.rutas_sensibles",
        description="Rutas sensibles del repositorio y su verificación extra.",
    )
    analizador.add_argument("--validar", action="store_true")
    analizador.add_argument("--puerta", action="store_true")
    analizador.add_argument("--base", default="dev")
    analizador.add_argument("--declaracion", default=str(RUTA_DECLARACION))
    analizador.add_argument("--raiz", default=".")
    opciones = analizador.parse_args(argv)

    raiz = Path(opciones.raiz)
    try:
        verificaciones = cargar_declaracion(opciones.declaracion, opciones.raiz)
        if verificaciones and opciones.validar:
            validar(verificaciones, opciones.raiz)
    except ErrorDeclaracion as error:
        print(str(error), file=sys.stderr)
        return 1

    if not verificaciones:
        if opciones.validar:
            print("    sin harness/rutas_sensibles.json: no hay puerta que aplicar")
        return 0

    if opciones.validar:
        total = sum(len(v.rutas) for v in verificaciones)
        print(
            f"    {len(verificaciones)} verificación(es), {total} ruta(s) "
            f"sensible(s) declaradas: "
            f"{', '.join(f'{v.nombre} ({v.exigencia})' for v in verificaciones)}"
        )
        return 0

    feature, rama = _feature_en_curso(raiz)
    resultado = evaluar_puerta(
        verificaciones,
        feature=feature,
        rama=rama,
        base=opciones.base,
        raiz=raiz,
    )
    if resultado.mensaje:
        print(resultado.mensaje)
    return resultado.codigo


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
