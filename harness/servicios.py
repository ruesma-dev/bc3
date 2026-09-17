# harness/servicios.py
"""Servicios de un monorepo: qué zonas del repositorio se validan por separado.

Un repositorio puede alojar varios servicios en subcarpetas, cada uno con su
entorno virtual, sus tests y hasta otro lenguaje. Este módulo lee la
declaración **opcional** `harness/servicios.json` y responde a las tres
preguntas que necesitan el portero, la puerta de cobertura y la campaña de
mutación: qué servicios hay, a cuál pertenece un fichero y con qué intérprete
se ejecuta su suite.

Regla de oro: **sin `harness/servicios.json` no cambia nada**. La ausencia del
fichero es la configuración del caso mayoritario —un solo proyecto en la
raíz—, y en ese caso `cargar_servicios()` devuelve una lista vacía y el resto
del arnés sigue exactamente el camino de siempre.

La otra cara: si el fichero existe pero está roto, el arnés **falla**. Degradar
en silencio a mono-proyecto dejaría servicios enteros sin comprobar mientras el
portero imprime que todo va bien, que es la peor de las salidas posibles.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path

#: Ruta (relativa a la raíz del repositorio) de la declaración de servicios.
RUTA_SERVICIOS = Path("harness/servicios.json")

#: Lenguajes que la declaración reconoce. `otro` significa «no es Python»: sus
#: comprobaciones de Python se degradan con aviso.
LENGUAJES: tuple[str, ...] = ("python", "otro")

#: Separador de la salida `--shell`. No puede aparecer dentro de un campo.
SEPARADOR = "|"

#: Rutas relativas donde vive el intérprete dentro de un entorno virtual.
INTERPRETES_DEL_VENV: tuple[str, ...] = ("Scripts/python.exe", "bin/python")


@dataclass(frozen=True)
class Servicio:
    """Un servicio declarado del monorepo."""

    nombre: str
    ruta: str
    lenguaje: str
    venv: str | None = None
    comando_tests: str | None = None


# --- Carga y validación -----------------------------------------------------


def _normalizar(ruta: str) -> str:
    """Separadores a barras y sin barra final: una ruta, una sola escritura."""
    return ruta.replace("\\", "/").strip("/")


def _texto(entrada: dict, clave: str, nombre: str, obligatorio: bool) -> str | None:
    """Lee un campo de texto de la declaración, o falla diciendo cuál falla."""
    valor = entrada.get(clave)
    if valor is None and not obligatorio:
        return None
    if not isinstance(valor, str) or not valor.strip():
        raise ValueError(
            f"servicio '{nombre}': el campo '{clave}' debe ser un texto no vacío, "
            f"y es {valor!r}"
        )
    if SEPARADOR in valor:
        raise ValueError(
            f"servicio '{nombre}': el campo '{clave}' no puede contener "
            f"'{SEPARADOR}', que es el separador de la salida --shell"
        )
    return valor


def _servicio_de_entrada(entrada: object) -> Servicio:
    """Convierte una entrada de la declaración en un `Servicio` validado."""
    if not isinstance(entrada, dict):
        raise ValueError(
            f"cada servicio debe ser un objeto JSON con 'nombre', 'ruta' y "
            f"'lenguaje', y hay uno que es {entrada!r}"
        )

    nombre = entrada.get("nombre")
    if not isinstance(nombre, str) or not nombre.strip():
        raise ValueError(f"hay un servicio sin 'nombre' válido: {entrada!r}")

    lenguaje = entrada.get("lenguaje")
    if lenguaje not in LENGUAJES:
        raise ValueError(
            f"servicio '{nombre}': lenguaje {lenguaje!r} desconocido; "
            f"usa uno de {list(LENGUAJES)}"
        )

    return Servicio(
        nombre=nombre,
        ruta=_normalizar(str(_texto(entrada, "ruta", nombre, obligatorio=True))),
        lenguaje=lenguaje,
        venv=_texto(entrada, "venv", nombre, obligatorio=False),
        comando_tests=_texto(entrada, "comando_tests", nombre, obligatorio=False),
    )


def _comprobar_conjunto(servicios: list[Servicio], raiz: str) -> None:
    """Valida lo que solo se ve mirando todos los servicios a la vez."""
    vistos_nombre: set[str] = set()
    vistas_ruta: list[str] = []

    for servicio in servicios:
        if servicio.nombre in vistos_nombre:
            raise ValueError(f"nombre duplicado en la declaración: '{servicio.nombre}'")
        vistos_nombre.add(servicio.nombre)

        for anterior in vistas_ruta:
            if anterior == servicio.ruta:
                raise ValueError(
                    f"ruta duplicada en la declaración: '{servicio.ruta}'"
                )
            if anterior.startswith(f"{servicio.ruta}/") or servicio.ruta.startswith(
                f"{anterior}/"
            ):
                raise ValueError(
                    f"la ruta '{servicio.ruta}' se solapa con '{anterior}': un "
                    f"fichero no puede pertenecer a dos servicios"
                )
        vistas_ruta.append(servicio.ruta)

        if not (Path(raiz) / servicio.ruta).is_dir():
            raise ValueError(
                f"servicio '{servicio.nombre}': la ruta '{servicio.ruta}' no existe "
                f"en el repositorio"
            )


def cargar_servicios(
    ruta: Path | str = RUTA_SERVICIOS, raiz: str = "."
) -> list[Servicio]:
    """Carga la declaración de servicios; lista vacía si no hay ninguna.

    `ruta` relativa se resuelve bajo `raiz`, para que una campaña lanzada sobre
    un worktree lea la declaración de ESE árbol y no la del directorio actual.
    """
    fichero = Path(ruta)
    if not fichero.is_absolute():
        fichero = Path(raiz) / fichero
    if not fichero.is_file():
        return []

    try:
        datos = json.loads(fichero.read_text(encoding="utf-8"))
    except ValueError as error:
        raise ValueError(
            f"{fichero.as_posix()} no es JSON válido: {error}"
        ) from error

    declarados = datos.get("servicios") if isinstance(datos, dict) else None
    if not isinstance(declarados, list):
        raise ValueError(
            f"{fichero.as_posix()}: falta la lista 'servicios' (un objeto JSON con "
            f"la clave 'servicios')"
        )

    servicios = [_servicio_de_entrada(entrada) for entrada in declarados]
    _comprobar_conjunto(servicios, raiz)
    return servicios


# --- Consultas --------------------------------------------------------------


def servicio_de_ruta(ruta: str, servicios: list[Servicio]) -> Servicio | None:
    """Servicio al que pertenece `ruta`, por prefijo más largo; `None` si a ninguno."""
    normalizada = _normalizar(ruta)
    candidatos = [
        servicio
        for servicio in servicios
        if normalizada == servicio.ruta or normalizada.startswith(f"{servicio.ruta}/")
    ]
    if not candidatos:
        return None
    return max(candidatos, key=lambda servicio: len(servicio.ruta))


def interprete(servicio: Servicio, raiz: str = ".") -> str:
    """Intérprete Python con el que se ejecuta la suite de un servicio.

    Con `venv` declarado, el de ESE entorno, en ruta absoluta y con barras (Git
    Bash no ejecuta rutas con contrabarras). Sin `venv`, el que corre el arnés.
    Un `venv` declarado que no existe es un error: caer al intérprete global
    probaría el servicio con dependencias que no son las suyas.
    """
    if servicio.venv is None:
        return sys.executable

    base = Path(raiz) / servicio.venv
    for relativo in INTERPRETES_DEL_VENV:
        candidato = base / relativo
        if candidato.is_file():
            return candidato.resolve().as_posix()

    raise ValueError(
        f"servicio '{servicio.nombre}': el venv declarado '{servicio.venv}' no "
        f"contiene intérprete ({' ni '.join(INTERPRETES_DEL_VENV)}). No se usa el "
        f"intérprete global en su lugar: probaría con las dependencias equivocadas"
    )


def tiene_tests(servicio: Servicio, raiz: str = ".") -> bool:
    """¿El servicio tiene directorio de tests propio?"""
    return (Path(raiz) / servicio.ruta / "tests").is_dir()


def linea_shell(servicio: Servicio, raiz: str = ".") -> str:
    """Una línea `nombre|ruta|lenguaje|interprete|comando_tests` para `init.sh`.

    El intérprete va **ya resuelto**: el bucle del portero no tiene que adivinar
    dónde vive el ejecutable de un venv en cada sistema operativo. En un
    servicio que no es Python el campo va vacío.
    """
    ruta_interprete = (
        Path(interprete(servicio, raiz)).as_posix()
        if servicio.lenguaje == "python"
        else ""
    )
    return SEPARADOR.join(
        [
            servicio.nombre,
            servicio.ruta,
            servicio.lenguaje,
            ruta_interprete,
            servicio.comando_tests or "",
        ]
    )


# --- CLI --------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    """`--validar` (0 si la declaración es sana) o `--shell` (una línea por servicio)."""
    analizador = argparse.ArgumentParser(
        prog="python -m harness.servicios",
        description="Declaración de servicios del monorepo (opcional).",
    )
    analizador.add_argument("--validar", action="store_true")
    analizador.add_argument(
        "--shell", action="store_true", help="salida parseable: un servicio por línea"
    )
    analizador.add_argument("--ruta", default=str(RUTA_SERVICIOS))
    analizador.add_argument("--raiz", default=".")
    opciones = analizador.parse_args(argv)

    try:
        servicios = cargar_servicios(opciones.ruta, opciones.raiz)
        lineas = [linea_shell(servicio, opciones.raiz) for servicio in servicios]
    except ValueError as error:
        print(str(error), file=sys.stderr)
        return 1

    if opciones.shell:
        for linea in lineas:
            print(linea)
        return 0

    if not servicios:
        print("    sin harness/servicios.json: proyecto único en la raíz")
        return 0
    print(
        f"    {len(servicios)} servicio(s): "
        f"{', '.join(f'{s.nombre} ({s.lenguaje})' for s in servicios)}"
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
