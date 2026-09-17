# harness/alcance.py
"""Alcance de una feature: qué líneas de código de producción toca.

El alcance NO se mantiene a mano: sale del diff de git entre la base de
integración y la rama de la feature (o, si la rama ya no existe, entre el
primer padre de su commit de merge y el propio merge).

Lo usan por igual la campaña de mutación (`harness.mutacion`) y la puerta de
cobertura (`harness.cobertura`): una sola fuente de verdad para el «qué
cambió».
"""

from __future__ import annotations

import json
import re
import subprocess
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

#: Directorios cuyo contenido nunca es código de producción mutable. Se
#: comparan como SEGMENTO de ruta a cualquier profundidad, no como prefijo de
#: la raíz: en un repositorio con servicios en subcarpetas, los tests de un
#: servicio son tests igual que los de la raíz.
#:
#: `harness` está aquí por la misma razón que `tests`: es utillaje, no producto.
#: Sin excluirlo, tocar el arnés desde la rama de una feature mete sus propias
#: líneas en el alcance, y entonces la campaña **se muta a sí misma** y la
#: puerta de cobertura mide la herramienta en vez de lo que la herramienta
#: vigila.
DIRECTORIOS_EXCLUIDOS: tuple[str, ...] = (
    "tests",
    "specs",
    "progress",
    "docs",
    "harness",
)

#: Exclusiones que aplican cuando el alcance lo DECLARA una persona con
#: `--ficheros`. `harness` NO está, a propósito: la exclusión de arriba protege
#: el alcance AUTOMÁTICO —el que sale del diff, que nadie eligió y que arrastra
#: el arnés sin querer en cuanto una rama lo toca—, y aquí no hay nada de lo que
#: proteger a quien escribe la lista. Medir si la maquinaria del arnés está
#: protegida por sus tests es, literalmente, la razón por la que `--ficheros`
#: existe (ver `alcance_de_ficheros`), y en el repositorio del propio arnés esa
#: maquinaria es el producto, no el utillaje.
DIRECTORIOS_EXCLUIDOS_AL_DECLARAR: tuple[str, ...] = (
    "tests",
    "specs",
    "progress",
    "docs",
)

#: Ruta por defecto del inventario de features del arnés.
RUTA_FEATURES = "harness/features.json"

_CABECERA_HUNK = re.compile(r"^@@ -\d+(?:,\d+)? \+(\d+)(?:,\d+)? @@")

EjecutorGit = Callable[[list[str]], str]


# --- Ejecución de git -------------------------------------------------------


def ejecutar_git(args: list[str], raiz: str = ".") -> str:
    """Ejecuta `git` en `raiz` y devuelve su salida estándar.

    Un código de salida distinto de cero devuelve cadena vacía: quien llama
    decide qué significa (por ejemplo, que una rama no existe).
    """
    proceso = subprocess.run(
        ["git", "-C", raiz, *args],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    if proceso.returncode != 0:
        return ""
    return proceso.stdout or ""


def git_en(raiz: str = ".") -> EjecutorGit:
    """Devuelve un ejecutor de git atado a un directorio de trabajo."""

    def _git(args: list[str]) -> str:
        return ejecutar_git(args, raiz=raiz)

    return _git


# --- Parseo del diff --------------------------------------------------------


def parsear_diff(texto: str) -> dict[str, set[int]]:
    """Extrae del diff unificado las líneas añadidas o cambiadas por fichero.

    Función pura: no toca git ni el disco. Las líneas se numeran sobre la
    versión NUEVA del fichero, que es la que se va a mutar o medir. Un fichero
    nuevo entra entero (todas sus líneas son añadidas). Un fichero borrado no
    entra: ya no hay nada que mutar.
    """
    resultado: dict[str, set[int]] = {}
    ruta: str | None = None
    numero = 0

    for linea in texto.splitlines():
        if linea.startswith("+++ "):
            destino = linea[4:].strip()
            if destino == "/dev/null":
                ruta = None
            else:
                ruta = destino[2:] if destino.startswith(("a/", "b/")) else destino
                resultado.setdefault(ruta, set())
            continue
        if linea.startswith("--- "):
            continue
        if linea.startswith("@@"):
            coincidencia = _CABECERA_HUNK.match(linea)
            numero = int(coincidencia.group(1)) if coincidencia else 0
            continue
        if ruta is None or numero == 0:
            continue
        if linea.startswith("\\"):  # "\ No newline at end of file"
            continue
        if linea.startswith("+"):
            resultado[ruta].add(numero)
            numero += 1
        elif linea.startswith("-"):
            continue  # línea eliminada: no avanza la numeración del fichero nuevo
        else:  # línea de contexto (o ruido entre ficheros, que el próximo @@ corrige)
            numero += 1

    return {ruta: lineas for ruta, lineas in resultado.items() if lineas}


def es_produccion(
    ruta: str, excluidos: tuple[str, ...] = DIRECTORIOS_EXCLUIDOS
) -> bool:
    """¿Es `ruta` código de producción susceptible de mutarse o medirse?

    Solo Python, y con ningún directorio excluido en su camino: basta con que
    uno de `excluidos` aparezca como un segmento completo, esté en la raíz o
    dentro de un servicio del monorepo. El nombre del propio fichero no cuenta
    (`app/docs.py` es código), ni vale como prefijo (`harnesses/util.py` también
    lo es).

    `excluidos` se pasa a mano en un solo sitio: el alcance declarado con
    `--ficheros`, que sí puede nombrar el arnés. Ver
    `DIRECTORIOS_EXCLUIDOS_AL_DECLARAR`.
    """
    normalizada = ruta.replace("\\", "/")
    if not normalizada.endswith(".py"):
        return False
    directorios = normalizada.split("/")[:-1]
    return not any(nombre in excluidos for nombre in directorios)


def filtrar_produccion(lineas: dict[str, set[int]]) -> dict[str, set[int]]:
    """Deja solo los ficheros de producción del mapa de líneas."""
    return {ruta: nums for ruta, nums in lineas.items() if es_produccion(ruta)}


# --- Resolución de referencias ----------------------------------------------


def resolver_refs(
    feature_id: str,
    rama: str | None,
    base: str = "dev",
    git: EjecutorGit | None = None,
) -> tuple[str, str, str]:
    """Decide entre qué dos referencias se calcula el diff de la feature.

    Orden: (1) la rama existe → base común con `base` frente a la rama;
    (2) la rama ya no existe → commit de merge localizado por su mensaje, y el
    diff va del primer padre al propio merge; (3) ni una cosa ni otra →
    `SystemExit` explícito, sin mutar ni medir nada.

    Devuelve `(ref_a, ref_b, origen)` con `origen` en {"rama", "merge"}.
    """
    git = git or git_en()

    if rama and git(["rev-parse", "--verify", "--quiet", rama]).strip():
        base_comun = git(["merge-base", base, rama]).strip() or base
        return (base_comun, rama, "rama")

    merge = git(
        ["log", "--merges", "--grep", feature_id, "-n", "1", "--format=%H", base]
    ).strip()
    if merge:
        return (f"{merge}^1", merge, "merge")

    raise SystemExit(
        f"No se puede calcular el alcance de {feature_id}: no existe la rama "
        f"{rama or '(sin declarar)'} ni un commit de merge que la mencione en "
        f"{base}. Abortado sin tocar nada."
    )


def rama_de_feature(feature_id: str, ruta_features: str = RUTA_FEATURES) -> str | None:
    """Lee del inventario del arnés la rama declarada para una feature."""
    fichero = Path(ruta_features)
    if not fichero.is_file():
        return None
    try:
        datos = json.loads(fichero.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    for feature in datos.get("features", []):
        if feature.get("id") == feature_id:
            return feature.get("branch")
    return None


# --- Alcance ----------------------------------------------------------------


@dataclass
class Alcance:
    """Líneas de producción que toca una feature."""

    feature: str
    origen: str
    ref_diff: tuple[str, str]
    lineas: dict[str, set[int]] = field(default_factory=dict)

    def ficheros(self) -> list[str]:
        return sorted(self.lineas)

    def total_lineas(self) -> int:
        return sum(len(nums) for nums in self.lineas.values())

    def descripcion(self) -> str:
        return (
            f"{self.feature}: {len(self.lineas)} fichero(s), "
            f"{self.total_lineas()} línea(s) de producción "
            f"(origen {self.origen}, {self.ref_diff[0]}..{self.ref_diff[1]})"
        )


#: Primera referencia del `ref_diff` de un alcance declarado a mano: no hay
#: diff detrás, y decirlo es más honesto que inventar una base de comparación.
SIN_DIFF = "(sin diff)"

#: Valor de `Alcance.origen` cuando el alcance no sale de un diff sino de la
#: orden. Lo mira el informe para no imprimir dos refs que no comparan nada.
ORIGEN_FICHEROS = "ficheros"


def alcance_de_ficheros(rutas: list[str], feature_id: str, raiz: str = ".") -> Alcance:
    """Alcance declarado a mano: los ficheros indicados, ENTEROS.

    Existe porque hay campañas cuyo sujeto no es «lo que cambió» sino un módulo
    entero —medir si la maquinaria del arnés está protegida por sus tests, por
    ejemplo—, y para eso no hay diff que sirva: el de la rama numeraría líneas
    ajenas y el de la feature original apunta a un código que ya no existe.

    `origen="ficheros"` y `ref_diff=(SIN_DIFF, <sha de HEAD>)`, para que el
    informe pueda decir contra qué commit se midió. Aborta con `SystemExit` —sin
    tocar nada— si una ruta no existe, si `es_produccion` la rechaza —mutar lo
    que no es código de producción da supervivientes que no significan nada— o
    si, ya filtradas las entradas en blanco, no queda ni un fichero que mutar.
    """
    base = Path(raiz)
    lineas: dict[str, set[int]] = {}
    for ruta in rutas:
        normalizada = ruta.replace("\\", "/").strip()
        if not normalizada:
            continue
        fichero = base / normalizada
        if not fichero.is_file():
            raise SystemExit(
                f"--ficheros: {normalizada} no existe en {base.as_posix()}. "
                "Abortado sin tocar nada."
            )
        if not es_produccion(normalizada, DIRECTORIOS_EXCLUIDOS_AL_DECLARAR):
            raise SystemExit(
                f"--ficheros: {normalizada} no es código de producción "
                "(solo .py fuera de "
                f"{', '.join(DIRECTORIOS_EXCLUIDOS_AL_DECLARAR)}). "
                "Abortado sin tocar nada."
            )
        total = len(fichero.read_text(encoding="utf-8").splitlines())
        lineas[normalizada] = set(range(1, total + 1))

    # DESPUÉS del filtrado, no antes: desde el CLI la lista nunca llega vacía
    # —`split(",")` devuelve siempre al menos un elemento— y las entradas en
    # blanco se descartan una a una, así que `--ficheros ","` se colaba hasta
    # el final y escribía un informe de CERO mutantes. Una campaña vacía que
    # sale con éxito es peor que un aborto: se lee como «nada que mutar, todo
    # bien». Lo que importa no es cómo venga la lista, sino que quede algo.
    if not lineas:
        raise SystemExit(
            "--ficheros no deja ninguna ruta que mutar "
            f"({rutas!r}): no hay nada que medir. Abortado sin tocar nada."
        )

    sha = ejecutar_git(["rev-parse", "HEAD"], raiz=raiz).strip()
    return Alcance(
        feature=feature_id,
        origen=ORIGEN_FICHEROS,
        ref_diff=(SIN_DIFF, sha),
        lineas=lineas,
    )


def alcance_de_feature(
    feature_id: str,
    base: str = "dev",
    rama: str | None = None,
    raiz: str = ".",
    git: EjecutorGit | None = None,
) -> Alcance:
    """Calcula el alcance de una feature desde el diff de git."""
    git = git or git_en(raiz)
    if rama is None:
        rama = rama_de_feature(feature_id, str(Path(raiz) / RUTA_FEATURES))

    ref_a, ref_b, origen = resolver_refs(feature_id, rama, base, git=git)
    texto = git(["diff", ref_a, ref_b])
    return Alcance(
        feature=feature_id,
        origen=origen,
        ref_diff=(ref_a, ref_b),
        lineas=filtrar_produccion(parsear_diff(texto)),
    )
