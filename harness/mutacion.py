# harness/mutacion.py
"""Mutador mínimo del arnés: comprueba que los tests son de verdad.

Muta ÚNICAMENTE las líneas de producción que toca una feature (el alcance lo
calcula `harness.alcance` desde el diff de git), ejecuta la suite por cada
mutante y cuenta cuántos sobreviven. Un mutante superviviente es una línea que
ningún test comprueba de verdad.

Tres garantías que sostienen los números que imprime (1.5.3):

1. **Línea base.** Antes de juzgar a nadie, la suite corre SIN mutar nada, en
   el mismo sitio y con el mismo intérprete con que se va a juzgar. Si no está
   verde, la campaña se aborta: sobre una base roja todo mutante sale «muerto»
   y el cero de supervivientes es mentira. Pasó de verdad el 2026-08-19.
2. **El veredicto no es binario.** Una suite que se rompe por su cuenta —error
   de recolección, error interno, mal uso— no dice nada del mutante. Se
   comprueba la base ahí mismo y, si está rota, el mutante NO cuenta como
   muerto.
3. **Restauración a prueba de muerte.** `try/finally`, manejador de SIGINT y
   SIGTERM, y un centinela en disco que deja escrito qué mutante está aplicado
   ahora mismo, para que un `kill` a machetazos deje rastro en vez de un
   mutante disfrazado de código.

Todo con biblioteca estándar (`ast` + `subprocess`): la herramienta tiene que
poder instalarse tal cual en cualquier repositorio, incluido Windows.
"""

from __future__ import annotations

import argparse
import ast
import json
import math
import os
import random
import re
import signal
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from harness.alcance import (
    ORIGEN_FICHEROS,
    Alcance,
    alcance_de_feature,
    alcance_de_ficheros,
)
from harness.rigor import (
    RUTA_RIGOR,
    cargar_features,
    cargar_rigor,
    max_mutantes_nivel,
    nivel_de_feature,
    semilla_nivel,
    timeout_mutacion,
    workers_mutacion,
)
from harness.servicios import Servicio, cargar_servicios, interprete, servicio_de_ruta

Posicion = tuple[int, int]

#: Veredictos posibles de la suite frente a un mutante.
MUERTO = "muerto"
SUPERVIVIENTE = "superviviente"
TIMEOUT = "timeout"

#: La suite no llegó a juzgar: se rompió por su cuenta (error de recolección,
#: error interno, mal uso). Es un veredicto INTERNO: `ejecutar_campania` lo
#: resuelve comprobando la línea base ahí mismo, y sale de ahí como `MUERTO`
#: (la base está verde: fue el mutante quien la rompió) o como `BASE_ROTA`.
INDETERMINADO = "indeterminado"

#: Mutante juzgado sobre una base que ya estaba rota. No cuenta como muerto ni
#: como superviviente: no se sabe nada de él, y decir «muerto» era el defecto
#: que la 1.5.3 arregla.
BASE_ROTA = "base_rota"

#: SUELO de segundos por mutante si nadie configura otra cosa. Suelo y no
#: techo: el timeout efectivo se deriva de la línea base medida (R1/R7).
TIMEOUT_POR_DEFECTO = 120

#: Rama de integración contra la que se calcula el alcance si nadie dice otra.
#: Vive aquí, y no suelta dentro del `argparse`, porque `comando_de` necesita
#: saber cuál es el valor de siempre para no repetirlo en el comando.
BASE_POR_DEFECTO = "dev"

#: Cuánto se multiplica el peor tiempo de línea base MEDIDO para obtener el
#: timeout de un mutante. Va en el código y no en `rigor.json` a propósito
#: (decisión del humano del 2026-08-21): es un parámetro del mecanismo, no de la
#: máquina, y meterlo en configuración reabre la puerta a cablear valores
#: locales en un arnés que viaja a cinco proyectos.
#:
#: Dos, no diez: el margen tiene que absorber el ruido de una suite que ya se
#: midió con los W workers compitiendo, no tapar un mutante que cuelga.
MARGEN_TIMEOUT = 2.0

#: Por cuánto se multiplica el suelo para darle su timeout a la LÍNEA BASE.
#: Huevo y gallina: la base necesita un reloj para poder medirse, y no puede
#: usar uno derivado de sí misma. Se le da uno holgado y aparte, y es defendible
#: porque se paga **una vez por worker**, no una por mutante. Si ni con 10
#: minutos cabe la suite limpia, el problema ya no es el reloj.
FACTOR_HOLGURA_BASE = 5

#: Tope del número de workers CALCULADO por defecto. No limita lo que se pida a
#: mano con `--workers` ni lo declarado en `rigor.json` (R10).
#:
#: Fue 16 hasta que alguien lo midió, y ese 16 hacía la campaña paralela
#: inutilizable por defecto: el 2026-08-21, en una máquina de 22 núcleos, la
#: suite limpia tardó 51 s en reposo, 97,5 s con UN worker y 119-122 s con
#: TRES, contra un timeout configurado de 120 s. Con 16 no habría cabido ni
#: una línea base.
#:
#: El único punto medido y verde son 3 workers, y ya ahí la suite sube un 25 %.
#: 4 es un paso sobre lo medido, no un salto; subirlo es una decisión CON
#: DATOS, y `--workers N` sigue sin límite para quien quiera producirlos.
TOPE_WORKERS = 4

#: Códigos de salida de pytest que este módulo distingue. Solo el 1 —«han
#: fallado tests»— significa que el mutante pudo ser cazado; el 2 (recolección
#: interrumpida), el 3 (error interno) y el 4 (mal uso) significan que la suite
#: ni siquiera llegó a juzgar, y contarlos como muerte era parte del defecto.
PYTEST_OK = 0
PYTEST_FALLOS = 1

#: Código con el que pytest avisa de que no ha recogido NINGÚN test. No es un
#: fallo de la suite: es que no hay suite. Contarlo como mutante muerto daría
#: por cazado lo que nadie comprueba, justo lo que esta herramienta destapa.
PYTEST_SIN_TESTS = 5

#: Argumentos con los que se corre la LÍNEA BASE (la suite sin mutar nada).
#: Sin `-x` a propósito: el mensaje de aborto tiene que nombrar TODOS los tests
#: que fallan, no solo el primero, o arreglar la base es un juego de adivinar.
#: `-rfE` fuerza el resumen «FAILED ...» y «ERROR ...» aunque `--tb=no` calle
#: las trazas. La `E` no sobra: una suite que muere en la RECOLECCIÓN no tiene
#: ni un FAILED que enseñar, y ése es justo el caso más frecuente en modo serie
#: —pytest invocado sin ruta sobre una raíz sin configuración—. Sin ella el
#: aborto decía «código 2» a secas y mandaba a adivinar.
ARGUMENTOS_LINEA_BASE: tuple[str, ...] = (
    "-q", "--tb=no", "-rfE", "-p", "no:cacheprovider",
)

#: Fichero donde una campaña en curso deja escrito qué mutante tiene aplicado.
#: Vive bajo `.arnes_cache/`, que el bloque gestionado del `.gitignore` ya
#: ignora: es estado de una ejecución, no algo que se versione.
RUTA_CENTINELA = Path(".arnes_cache/mutacion_en_curso.json")

#: Líneas del resumen de pytest que nombran un test caído.
_LINEA_FALLIDA = re.compile(r"^(?:FAILED|ERROR)\s+(\S+)", re.MULTILINE)

#: Con qué empiezan las líneas de eco de la línea base. La campaña paralela las
#: reconoce por aquí para NO contarlas en su `[i/n]` de progreso.
MARCA_LINEA_BASE = "[base] "

#: (símbolo original, símbolo mutado) por tipo de nodo del árbol sintáctico.
#:
#: `is` / `is not` entran aquí porque en Python son LA guarda de ausencia
#: (`x is None`), y sin ellas la campaña quedaba ciega justo en el patrón que
#: ha provocado los defectos más caros. Límite conocido y aceptado: la
#: sustitución busca la cadena literal, así que un `is  not` con espaciado no
#: canónico —o partido entre dos líneas— NO genera mutante (no falla:
#: simplemente no hay candidato). En un repositorio formateado con ruff/black
#: ese espaciado no existe, y sostener una expresión regular por él obligaría a
#: cambiar el contrato de `_Candidato` a cambio de nada.
COMPARACIONES: dict[type, tuple[str, str]] = {
    ast.Eq: ("==", "!="),
    ast.NotEq: ("!=", "=="),
    ast.Lt: ("<", "<="),
    ast.LtE: ("<=", "<"),
    ast.Gt: (">", ">="),
    ast.GtE: (">=", ">"),
    ast.Is: ("is", "is not"),
    ast.IsNot: ("is not", "is"),
}
ARITMETICOS: dict[type, tuple[str, str]] = {
    ast.Add: ("+", "-"),
    ast.Sub: ("-", "+"),
    ast.Mult: ("*", "//"),
    ast.FloorDiv: ("//", "*"),
}
LOGICOS: dict[type, tuple[str, str]] = {
    ast.And: ("and", "or"),
    ast.Or: ("or", "and"),
}


@dataclass(frozen=True)
class Mutante:
    """Un solo cambio en una sola línea de un solo fichero."""

    fichero: str
    linea: int
    col: int
    original: str
    mutado: str
    operador: str
    longitud: int = 0
    sustituto: str = ""

    def descripcion(self) -> str:
        return f"{self.fichero}:{self.linea} [{self.operador}] {self.original} -> {self.mutado}"


@dataclass(frozen=True)
class _Candidato:
    """Sitio del código donde se puede aplicar una mutación."""

    ini: Posicion
    fin: Posicion
    buscar: str
    nuevo: str
    operador: str
    hasta: Posicion | None = None


def _inicio(nodo: ast.AST) -> Posicion:
    return (nodo.lineno, nodo.col_offset)  # type: ignore[attr-defined]


def _final(nodo: ast.AST) -> Posicion:
    return (nodo.end_lineno, nodo.end_col_offset)  # type: ignore[attr-defined]


def _texto_del_span(brutas: list[str], ini: Posicion, fin: Posicion) -> str:
    """Devuelve el texto exacto entre dos posiciones de una misma línea."""
    if ini[0] != fin[0]:
        return ""
    return brutas[ini[0] - 1].encode("utf-8")[ini[1] : fin[1]].decode("utf-8", "replace")


def _candidatos(nodo: ast.AST, brutas: list[str]) -> Iterator[_Candidato]:
    """Enumera las mutaciones aplicables a un nodo del árbol sintáctico."""
    if isinstance(nodo, ast.Compare):
        izquierdas = [nodo.left, *nodo.comparators[:-1]]
        for operador, izquierda, derecha in zip(
            nodo.ops, izquierdas, nodo.comparators, strict=True
        ):
            simbolos = COMPARACIONES.get(type(operador))
            if simbolos:
                yield _Candidato(
                    _final(izquierda), _inicio(derecha), *simbolos, "comparacion"
                )

    elif isinstance(nodo, ast.BinOp):
        simbolos = ARITMETICOS.get(type(nodo.op))
        if simbolos:
            yield _Candidato(
                _final(nodo.left), _inicio(nodo.right), *simbolos, "aritmetico"
            )

    elif isinstance(nodo, ast.AugAssign):
        simbolos = ARITMETICOS.get(type(nodo.op))
        if simbolos:
            yield _Candidato(
                _final(nodo.target), _inicio(nodo.value), *simbolos, "aritmetico"
            )

    elif isinstance(nodo, ast.BoolOp):
        simbolos = LOGICOS.get(type(nodo.op))
        if simbolos:
            for izquierda, derecha in zip(nodo.values, nodo.values[1:], strict=False):
                yield _Candidato(_final(izquierda), _inicio(derecha), *simbolos, "logico")

    elif isinstance(nodo, ast.UnaryOp) and isinstance(nodo.op, ast.Not):
        # Se elimina el `not` y el hueco hasta su operando, para no dejar
        # espacios de más en la línea mutada.
        yield _Candidato(
            _inicio(nodo),
            _inicio(nodo.operand),
            "not",
            "",
            "not",
            hasta=_inicio(nodo.operand),
        )

    elif isinstance(nodo, ast.Constant):
        if isinstance(nodo.value, bool):  # `bool` es subclase de `int`: va primero
            texto = _texto_del_span(brutas, _inicio(nodo), _final(nodo))
            if texto in ("True", "False"):
                yield _Candidato(
                    _inicio(nodo),
                    _final(nodo),
                    texto,
                    "False" if texto == "True" else "True",
                    "booleano",
                )
        elif isinstance(nodo.value, int):
            texto = _texto_del_span(brutas, _inicio(nodo), _final(nodo))
            if texto:
                yield _Candidato(
                    _inicio(nodo), _final(nodo), texto, str(nodo.value + 1), "entero"
                )


#: Bytes que cuentan como «parte de una palabra» al delimitar un token
#: alfabético. Los >= 0x80 entran porque una letra acentuada de un comentario
#: ocupa dos bytes en UTF-8 y sigue siendo parte de la palabra.
_PARTE_DE_PALABRA = re.compile(rb"[A-Za-z0-9_\x80-\xff]")


def _es_palabra(objetivo: bytes) -> bool:
    """¿El token a buscar se escribe con letras (`is`, `is not`, `and`, `not`)?

    Solo estos se delimitan. Los símbolos (`==`, `+`, `<=`) NO: `x==y` es
    legal, el carácter anterior a `==` es una letra y exigirle delimitación
    dejaría de mutarlo.
    """
    return bool(
        objetivo
        and _PARTE_DE_PALABRA.match(objetivo[:1])
        and _PARTE_DE_PALABRA.match(objetivo[-1:])
    )


def _delimitado(bruta: bytes, ini: int, fin: int) -> bool:
    """¿La coincidencia `[ini, fin)` es un token entero y no parte de una palabra?

    `is` cabe dentro de «análisis», y el hueco entre los dos operandos de una
    comparación puede contener un comentario cuando la condición va entre
    paréntesis. Sin esta comprobación el mutante caía dentro del comentario:
    equivalente por construcción, superviviente eterno y ruido en el informe.
    """
    anterior = bruta[ini - 1 : ini] if ini > 0 else b""
    siguiente = bruta[fin : fin + 1]
    return not (_PARTE_DE_PALABRA.match(anterior) or _PARTE_DE_PALABRA.match(siguiente))


def _localizar(brutas: list[str], candidato: _Candidato) -> Posicion | None:
    """Encuentra el token a mutar dentro del hueco entre dos operandos.

    Devuelve `(línea, columna en bytes)` de la primera coincidencia **válida**:
    para un token alfabético, la primera que sea una palabra entera. Se busca
    línea a línea porque un token de operador nunca está partido entre dos
    líneas —y por eso un `is not` con salto de línea en medio no da mutante,
    límite declarado junto a `COMPARACIONES`—.
    """
    objetivo = candidato.buscar.encode("utf-8")
    if not objetivo:
        return None
    exigir_palabra = _es_palabra(objetivo)
    (linea_ini, col_ini), (linea_fin, col_fin) = candidato.ini, candidato.fin
    for numero in range(linea_ini, linea_fin + 1):
        if numero > len(brutas):
            break
        bruta = brutas[numero - 1].encode("utf-8")
        desde = col_ini if numero == linea_ini else 0
        hasta = col_fin if numero == linea_fin else len(bruta)
        posicion = bruta.find(objetivo, desde, hasta)
        while posicion != -1:
            if not exigir_palabra or _delimitado(
                bruta, posicion, posicion + len(objetivo)
            ):
                return (numero, posicion)
            posicion = bruta.find(objetivo, posicion + 1, hasta)
    return None


def generar_mutantes(fuente: str, lineas: set[int], fichero: str) -> list[Mutante]:
    """Genera un mutante por cada mutación posible en las líneas del alcance.

    Cada mutante es UN SOLO cambio. Las líneas fuera del alcance no se tocan:
    la herramienta nunca muta el repositorio entero.
    """
    try:
        arbol = ast.parse(fuente)
    except SyntaxError:
        return []

    brutas = fuente.split("\n")
    mutantes: list[Mutante] = []
    vistos: set[tuple[int, int, str]] = set()

    for nodo in ast.walk(arbol):
        for candidato in _candidatos(nodo, brutas):
            localizado = _localizar(brutas, candidato)
            if localizado is None:
                continue
            numero, col = localizado
            if numero not in lineas:
                continue
            if (numero, col, candidato.operador) in vistos:
                continue
            vistos.add((numero, col, candidato.operador))

            longitud = len(candidato.buscar.encode("utf-8"))
            if candidato.hasta is not None and candidato.hasta[0] == numero:
                longitud = max(longitud, candidato.hasta[1] - col)

            bruta = brutas[numero - 1].encode("utf-8")
            nueva = (
                bruta[:col] + candidato.nuevo.encode("utf-8") + bruta[col + longitud :]
            ).decode("utf-8", "replace")
            mutantes.append(
                Mutante(
                    fichero=fichero,
                    linea=numero,
                    col=col,
                    original=brutas[numero - 1].strip(),
                    mutado=nueva.strip(),
                    operador=candidato.operador,
                    longitud=longitud,
                    sustituto=candidato.nuevo,
                )
            )

    return sorted(mutantes, key=lambda m: (m.fichero, m.linea, m.col, m.operador))


def aplicar_mutante(fuente: str, mutante: Mutante) -> str:
    """Devuelve el fuente con el mutante aplicado (sin tocar el disco)."""
    brutas = fuente.split("\n")
    bruta = brutas[mutante.linea - 1].encode("utf-8")
    brutas[mutante.linea - 1] = (
        bruta[: mutante.col]
        + mutante.sustituto.encode("utf-8")
        + bruta[mutante.col + mutante.longitud :]
    ).decode("utf-8", "replace")
    return "\n".join(brutas)


# --- Ejecución de la suite --------------------------------------------------


def _leer(ruta: Path) -> str:
    """Lee preservando los saltos de línea tal cual están en disco."""
    with open(ruta, encoding="utf-8", newline="") as fichero:
        return fichero.read()


def _purgar_bytecode(ruta: Path) -> None:
    """Borra el `.pyc` de `ruta`, si lo hay.

    CPython da por válido un bytecode cuando el fuente conserva el mismo tamaño
    y el mismo mtime **truncado a segundos enteros**. Dos mutantes consecutivos
    del mismo fichero cumplen las dos cosas más veces de las que parece: se
    escriben en el mismo segundo y muchas mutaciones cambian el mismo número de
    bytes (`*`->`//` y `>`->`>=` añaden uno cada una). Cuando coinciden, la
    suite del segundo mutante ejecuta el bytecode del PRIMERO y su veredicto no
    dice nada del código que hay en disco.

    Medido aquí el 2026-08-19: la misma campaña sobre el mismo repositorio de
    juguete daba 3 supervivientes o 2 según el segundo en que cayera, y el que
    desaparecía salía contado como MUERTO. Es el mismo pecado que arregla esta
    versión —un muerto que nadie mató—, por otra puerta.
    """
    cache = ruta.parent / "__pycache__"
    if not cache.is_dir():
        return
    for compilado in cache.glob(f"{ruta.stem}.*.pyc"):
        try:
            compilado.unlink()
        except OSError:
            # Windows: otro proceso puede tenerlo abierto. No es fatal —la
            # siguiente escritura vuelve a intentarlo— y perder la campaña por
            # no poder borrar una caché sería peor.
            pass


def _escribir(ruta: Path, texto: str) -> None:
    """Escribe un fuente sin traducir saltos de línea, y tira su bytecode.

    Sin traducir, porque restaurar debe dejar el fichero idéntico. Y tirando el
    `.pyc`, porque lo que se escribe aquí se va a ejecutar acto seguido: ver
    `_purgar_bytecode`.
    """
    with open(ruta, "w", encoding="utf-8", newline="") as fichero:
        fichero.write(texto)
    _purgar_bytecode(ruta)


@dataclass(frozen=True)
class ResultadoSuite:
    """Lo que devolvió una ejecución de la suite, sin interpretar todavía.

    Separar el HECHO (código de salida y salida de texto) de su INTERPRETACIÓN
    (¿mutante muerto?, ¿base rota?) es lo que permite que la misma ejecución
    sirva para juzgar un mutante y para juzgar la línea base.
    """

    codigo: int
    salida: str = ""
    expirado: bool = False

    @property
    def verde(self) -> bool:
        """¿La suite terminó sin fallos? Sin tests que recoger cuenta como sí.

        No hay suite que romper: la campaña ya trata ese caso declarando
        supervivientes, que es la respuesta honesta a «nadie comprueba esto».
        """
        return not self.expirado and self.codigo in (PYTEST_OK, PYTEST_SIN_TESTS)

    @property
    def sin_tests(self) -> bool:
        return not self.expirado and self.codigo == PYTEST_SIN_TESTS

    def fallidos(self) -> list[str]:
        """Tests que la suite nombra como caídos, en orden y sin repetir."""
        vistos: list[str] = []
        for nombre in _LINEA_FALLIDA.findall(self.salida):
            if nombre not in vistos:
                vistos.append(nombre)
        return vistos


class CampaniaAbortada(RuntimeError):
    """La campaña no puede dar un número honesto y para sin escribir informe.

    Abortar es la única salida decente: escribir un informe con un cero de
    supervivientes que nadie ha medido es peor que no medir, porque el reviewer
    lo da por bueno.
    """


class BaseRota(CampaniaAbortada):
    """La suite falla SIN mutar nada: ningún veredicto de esta campaña valdría."""


class ArbolSucio(CampaniaAbortada):
    """Hay trabajo sin commitear en ficheros que la campaña va a mutar."""


class EjecutorPytest:
    """Lanza la suite en un proceso aparte y traduce el resultado.

    Que la suite falle CON tests caídos significa que los tests cazan el mutante
    (muerto). Que pase —o que no haya ningún test que recoger— significa que el
    mutante sobrevive: nadie comprobaba esa línea. Y que pytest ni llegue a
    ejecutar (recolección interrumpida, error interno, mal uso) no significa
    nada del mutante: eso sale como `INDETERMINADO` y lo resuelve quien llama
    comprobando la línea base.

    `raiz` es el directorio desde el que se lanza la suite y `ejecutable`, el
    intérprete con el que se lanza. En un monorepo, los de cada servicio.

    `ruta` acota la RECOLECCIÓN dentro de esa raíz. Sin ella, `python -m pytest`
    a secas recoge todo lo que cuelgue del directorio: en un monorepo eso
    arrastra `services/**/tests` y muere en la recolección con el intérprete
    equivocado, así que ningún mutante llega a juzgarse (R1–R3). Es la
    misma ruta explícita que `harness/init.sh` usa para la suite de la raíz.
    """

    def __init__(
        self,
        raiz: str = ".",
        argumentos: list[str] | None = None,
        ejecutable: str | None = None,
        ruta: str | None = None,
    ) -> None:
        self.raiz = raiz
        self.argumentos = argumentos or ["-x", "-q", "--tb=no", "-p", "no:cacheprovider"]
        self.ejecutable = ejecutable or sys.executable
        self.ruta = ruta

    def identidad(self) -> tuple[str, str, str]:
        """Con qué intérprete, desde dónde y sobre qué ruta juzga. Dos
        ejecutores con la misma identidad ejecutan exactamente la misma suite:
        la línea base de uno vale por la del otro y no se corre dos veces.

        La ruta entra en la identidad porque dos ejecutores que comparten raíz e
        intérprete pero acotan distinto NO corren la misma suite: darles la
        misma línea base sería medir una y dar por buena la otra."""
        return (str(Path(self.raiz).resolve()), self.ejecutable, self.ruta or "")

    def correr(self, timeout_s: int, argumentos: tuple[str, ...] | None = None) -> ResultadoSuite:
        """Ejecuta la suite y devuelve el hecho crudo, sin interpretarlo.

        La ruta acotada va SIEMPRE al final, tanto aquí como en la línea base:
        una base que recoge más tests que los mutantes no protege nada.
        """
        pedidos = list(argumentos or self.argumentos)
        if self.ruta is not None:
            pedidos.append(self.ruta)
        try:
            proceso = subprocess.run(
                [self.ejecutable, "-m", "pytest", *pedidos],
                cwd=self.raiz,
                capture_output=True,
                timeout=timeout_s,
                check=False,
                # SIN BYTECODE, y no es un detalle de rendimiento. La suite se
                # lanza contra un fichero MUTADO: si el subproceso escribe
                # `__pycache__`, en el disco queda un `.pyc` compilado desde el
                # código mutado. Al restaurar el `.py` original, CPython valida
                # la caché por (tamaño, mtime) del fuente, y la restauración
                # deja los dos iguales —mismo texto, mismo segundo—, así que el
                # `.pyc` mutado se sigue dando por bueno. El árbol queda
                # envenenado: la campaña mide contra un código que ya no está y
                # la suite del proyecto puede pasar o fallar sin que nadie haya
                # tocado nada. El síntoma que lo delata es una campaña
                # absurdamente rápida. Pasó de verdad el 2026-08-19, y costó
                # una review entera averiguar de dónde salía.
                env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
            )
        except subprocess.TimeoutExpired:
            return ResultadoSuite(codigo=-1, salida="", expirado=True)
        salida = (proceso.stdout or b"").decode("utf-8", "replace") + (
            proceso.stderr or b""
        ).decode("utf-8", "replace")
        return ResultadoSuite(codigo=proceso.returncode, salida=salida)

    def linea_base(self, timeout_s: int) -> ResultadoSuite:
        """Corre la suite SIN mutar nada, nombrando todos los tests que caen."""
        return self.correr(timeout_s, ARGUMENTOS_LINEA_BASE)

    def ejecutar(self, timeout_s: int) -> str:
        resultado = self.correr(timeout_s)
        if resultado.expirado:
            return TIMEOUT
        if resultado.verde:
            return SUPERVIVIENTE
        if resultado.codigo == PYTEST_FALLOS:
            return MUERTO
        return INDETERMINADO


# --- Línea base: nunca contar muertos sobre una suite que ya fallaba ---------


#: Causas conocidas de una línea base roja que SOLO se rompe en modo paralelo.
#: Se enumeran en el mensaje de aborto porque el arreglo es distinto en cada
#: caso y adivinarlo desde «3 tests fallan» cuesta una tarde.
CAUSAS_CONOCIDAS = (
    (
        "ficheros NO versionados que la suite necesita (.env, datos locales, "
        "fixtures generadas): no existen dentro de un git worktree"
    ),
    (
        "detached HEAD: el worktree no está en ninguna rama, así que un test "
        "que lea `git branch --show-current` recibe cadena vacía"
    ),
    (
        "instalación editable apuntando al árbol principal: la suite del worker "
        "importaría el código de fuera del worktree, sin mutar"
    ),
)

#: Causas que NO tienen nada que ver con el paralelismo y muerden igual con
#: `--workers 1`. La primera se descubrió el 2026-08-19: un fichero de
#: `harness/` no pertenece a ningún servicio, así que `ejecutor_para` lo juzga
#: con `python -m pytest` SIN ruta desde la raíz; sin configuración de pytest
#: ahí, esa invocación recoge las suites de todos los servicios y muere en la
#: recolección en menos de un segundo, haga lo que haga el mutante. Dos
#: campañas enteras, de 19 y de 61 mutantes sobre ficheros de `harness/`,
#: salieron «muertas» sin que un solo test las juzgara.
CAUSAS_EN_CUALQUIER_MODO = (
    (
        "la suite se invoca SIN ruta —`python -m pytest` desde la raíz— porque "
        "el fichero mutado no cae en ningún servicio de harness/servicios.json: "
        "si la raíz no tiene configuración de pytest (testpaths, rootdir), esa "
        "invocación recoge lo que no debe y muere en la recolección"
    ),
    (
        "la suite necesita un servicio externo (base de datos, cola, API) que "
        "en esta máquina no está levantado"
    ),
)


def timeout_derivado(
    suelo: int, tiempos_base: dict[str, float], margen: float = MARGEN_TIMEOUT
) -> int:
    """Segundos por mutante derivados de la línea base ya medida (R1).

    `max(suelo, ceil(peor_tiempo × margen))`. Sede ÚNICA de la fórmula.

    Se deriva de la línea base y no de una medición aparte porque
    `comprobar_linea_base` ya corre la suite limpia **dentro de cada worktree y
    con los W workers compitiendo**, que es justo la contención que hay que
    medir, y ya devuelve los segundos: la medición correcta estaba hecha y se
    tiraba. Usarla no cuesta ni un segundo extra y absorbe máquina, workers y
    tamaño de suite sin ninguna fórmula que adivine.

    Manda el PEOR de los tiempos: un timeout que solo le vale al worker más
    rápido no le vale a nadie. Se redondea hacia arriba porque regalar el
    segundo que faltaba justo en el peor caso convierte una campaña válida en
    una tanda de «timeout». Sin ninguna medición —ejecutores dobles, campaña sin
    línea base— no hay nada de lo que derivar y se devuelve el suelo tal cual.
    """
    if not tiempos_base:
        return suelo
    return max(suelo, math.ceil(max(tiempos_base.values()) * margen))


def timeout_de_linea_base(suelo: int) -> int:
    """Segundos que se le conceden a la suite SIN mutar nada (R2).

    Ver `FACTOR_HOLGURA_BASE`: es holgado a propósito y su coste está acotado
    porque se paga una vez por worker.
    """
    return suelo * FACTOR_HOLGURA_BASE


def mensaje_base_rota(etiqueta: str, resultado: ResultadoSuite) -> str:
    """Mensaje accionable: qué falla, dónde, por qué suele fallar y qué hacer."""
    fallidos = resultado.fallidos()
    detalle = (
        "\n".join(f"    - {nombre}" for nombre in fallidos)
        if fallidos
        else f"    (pytest salió con código {resultado.codigo} sin nombrar tests)"
    )
    causas = "\n".join(f"    - {causa}" for causa in CAUSAS_CONOCIDAS)
    siempre = "\n".join(f"    - {causa}" for causa in CAUSAS_EN_CUALQUIER_MODO)
    return (
        f"LÍNEA BASE EN ROJO en {etiqueta}: la suite falla SIN mutar nada.\n"
        "Campaña abortada sin escribir informe: sobre una base roja TODO "
        "mutante saldría «muerto» y el cero de supervivientes sería falso.\n"
        "\n"
        "  Tests que fallan sin mutar:\n"
        f"{detalle}\n"
        "\n"
        "  Causas conocidas SOLO del modo paralelo (cada worker corre en un\n"
        "  `git worktree` desechable creado desde HEAD):\n"
        f"{causas}\n"
        "\n"
        "  Causas que muerden en CUALQUIER modo, también con --workers 1:\n"
        f"{siempre}\n"
        "\n"
        "  Arregla la base. Si la tuya es de las primeras, --workers 1 la "
        "esquiva: en\n"
        "  serie la suite corre sobre el propio árbol y no hay worktree que "
        "valga."
    )


def mensaje_base_expirada_al_arrancar(
    etiqueta: str, timeout_s: int, workers: int | None
) -> str:
    """Aviso de una línea base de ARRANQUE que se quedó sin tiempo (R5).

    «Sube el timeout» no es una acción: con W workers compitiendo, la respuesta
    casi siempre es bajar W, y el valor que se sube tiene nombre y fichero. Sin
    esos dos datos el mensaje manda a adivinar, y adivinar en esta maquinaria ha
    costado ya varias tardes.
    """
    cuantos = f"{workers} worker(s)" if workers is not None else "los workers en juego"
    return (
        f"LÍNEA BASE SIN TERMINAR en {etiqueta}: la suite SIN MUTAR agotó los "
        f"{timeout_s} s que se le concedieron (el suelo "
        f"'mutacion.timeout_por_mutante_s' multiplicado por la holgura de "
        f"{FACTOR_HOLGURA_BASE}), con {cuantos} compitiendo por la máquina.\n"
        "  Ningún veredicto de esta campaña valdría: si la suite LIMPIA ya no "
        "cabe en el reloj, todo mutante saldría «timeout».\n"
        "  Qué hacer: baja los workers (--workers N; cada worker arranca una "
        "suite entera, no un hilo) o sube el suelo "
        "'mutacion.timeout_por_mutante_s' de harness/rigor.json. Si ni con esa "
        "holgura cabe la suite limpia, entonces sí: acótala."
    )


def comprobar_linea_base(
    implicados: list[tuple[str, object]],
    timeout_s: int,
    eco: Callable[[str], None] | None = None,
    workers: int | None = None,
) -> dict[str, float]:
    """Corre la suite sin mutar en cada sitio donde se va a juzgar.

    `implicados` son los pares `(etiqueta, ejecutor)` que van a dictar los
    veredictos. Un ejecutor sin `linea_base` —un doble de test— se salta con
    aviso: no se puede comprobar lo que no sabe correr una suite.

    Lanza `BaseRota` en cuanto uno falla, sin haber tocado un solo fichero.

    Devuelve los SEGUNDOS que tardó la suite limpia en cada sitio (R11). Ese
    número es el patrón con el que se lee todo lo demás: una campaña que declara
    veinte mutantes en menos de lo que tarda UNA suite limpia no ha juzgado a
    nadie, y eso se ve leyendo el informe en vez de reejecutándolo. Un ejecutor
    que no sabe correr la suite no aporta entrada: mejor `n/d` que un cero, que
    se lee como medición.
    """
    tiempos: dict[str, float] = {}
    for etiqueta, ejecutor in implicados:
        correr_base = getattr(ejecutor, "linea_base", None)
        if correr_base is None:
            if eco is not None:
                eco(f"{MARCA_LINEA_BASE}{etiqueta}: ejecutor sin línea base, no se comprueba")
            continue
        arranque = time.monotonic()
        resultado = correr_base(timeout_s)
        tiempos[etiqueta] = time.monotonic() - arranque
        if resultado.expirado:
            raise BaseRota(
                mensaje_base_expirada_al_arrancar(etiqueta, timeout_s, workers)
            )
        if not resultado.verde:
            raise BaseRota(mensaje_base_rota(etiqueta, resultado))
        if eco is not None:
            estado = "sin tests que recoger" if resultado.sin_tests else "en verde"
            eco(
                f"{MARCA_LINEA_BASE}{etiqueta}: {estado} "
                f"({tiempos[etiqueta]:.1f} s)"
            )
    return tiempos


def ejecutores_implicados(
    mutantes: list[Mutante],
    ejecutor: object,
    ejecutor_de: Callable[[str], object] | None,
) -> list[tuple[str, object]]:
    """Ejecutores distintos que van a juzgar esta tanda de mutantes.

    Se deduplica por identidad —(directorio, intérprete)— para no correr N
    veces la misma suite en un monorepo donde varios ficheros del alcance caen
    en el mismo servicio.
    """
    if ejecutor_de is None:
        return [(_etiqueta_de(ejecutor), ejecutor)]
    elegidos: dict[object, tuple[str, object]] = {}
    for mutante in mutantes:
        candidato = ejecutor_de(mutante.fichero)
        identidad = getattr(candidato, "identidad", None)
        clave = identidad() if identidad is not None else id(candidato)
        elegidos.setdefault(clave, (_etiqueta_de(candidato), candidato))
    return list(elegidos.values())


def _etiqueta_de(ejecutor: object) -> str:
    """Cómo se nombra un ejecutor en los mensajes: por el directorio que corre."""
    raiz = getattr(ejecutor, "raiz", None)
    return Path(str(raiz)).as_posix() if raiz is not None else repr(ejecutor)


def _ruta_raiz(raiz: str) -> str | None:
    """Ruta con la que se acota la suite de la RAÍZ, o `None` si no la hay.

    `tests` es la convención del arnés —`harness/init.sh` ya invoca así la
    suite de la raíz— y acotarla es lo que permite juzgar un fichero que no
    pertenece a ningún servicio. Un repositorio que ponga su suite en otro sitio
    no mejora (R2): se invoca sin ruta, exactamente como hasta hoy.
    """
    return "tests" if (Path(raiz) / "tests").is_dir() else None


def ejecutor_para(
    fichero: str,
    servicios: list[Servicio],
    raiz: str = ".",
    raiz_venvs: str | None = None,
) -> EjecutorPytest:
    """Ejecutor con el que se juzga un mutante, según a qué servicio pertenece.

    Un fichero de un servicio Python se juzga con la suite de ESE servicio, en
    su directorio y con su intérprete. Todo lo demás —código de la raíz, o un
    `.py` suelto dentro de un servicio de otro lenguaje— con la suite de la
    raíz, como en un repositorio de un solo proyecto.

    `raiz_venvs` separa DÓNDE se ejecuta la suite de DÓNDE vive el entorno
    virtual: la campaña paralela lanza los tests dentro de un `git worktree`
    —que no trae venvs, porque no están versionados— con el intérprete del
    árbol principal. Sin él, ambas cosas son `raiz` y el comportamiento es
    exactamente el de siempre.
    """
    servicio = servicio_de_ruta(fichero, servicios)
    if servicio is None or servicio.lenguaje != "python":
        return EjecutorPytest(raiz=raiz, ruta=_ruta_raiz(raiz))
    return EjecutorPytest(
        raiz=str(Path(raiz) / servicio.ruta),
        ejecutable=interprete(servicio, raiz_venvs or raiz),
    )


# --- Centinela: qué mutante está aplicado AHORA MISMO ------------------------


@dataclass(frozen=True)
class Aplicado:
    """Un mutante escrito en disco en este instante."""

    fichero: str
    ruta: str
    linea: int
    descripcion: str
    linea_original: str
    en_arbol_principal: bool


class Centinela:
    """Deja escrito en disco qué mutante está aplicado, y en qué fichero.

    El `try/finally` de la campaña restaura el árbol en todo lo que se puede
    prever: excepción, timeout, Ctrl-C. Lo que NO cubre es un `kill` que mata el
    proceso sin desenrollar la pila —pasó el 2026-08-19 y dejaron el árbol con
    un `==` convertido en `!=` dentro de la puerta que protege `build_mart`—. El
    riesgo real de eso no es perder la campaña: es **commitear un mutante
    creyendo que es código**.

    Este centinela es la red de la red: un JSON pequeño que se reescribe al
    aplicar y al soltar cada mutante, y que se borra al terminar bien. Si queda
    ahí, algo murió a medias y lo dice con nombre y línea. Lo leen dos: la
    siguiente campaña (que se ofrece a restaurar) y `harness/init.sh`, para no
    dar por válido un veredicto medido sobre un mutante.

    Es seguro entre hilos: los workers de la campaña paralela comparten uno.
    """

    def __init__(
        self, raiz: str = ".", feature: str = "", modo: str = "serie", pid: int | None = None
    ) -> None:
        self.raiz = Path(raiz).resolve()
        self.ruta = self.raiz / RUTA_CENTINELA
        self.feature = feature
        self.modo = modo
        self.pid = os.getpid() if pid is None else pid
        self.inicio = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self._aplicados: dict[str, Aplicado] = {}
        self._cerrojo = threading.Lock()

    # -- escritura ----------------------------------------------------------

    def _volcar(self) -> None:
        """Reescribe el fichero entero: es pequeño y así nunca queda a medias."""
        aplicados = list(self._aplicados.values())
        datos = {
            "feature": self.feature,
            "modo": self.modo,
            "pid": self.pid,
            "inicio": self.inicio,
            "raiz": self.raiz.as_posix(),
            "muta_arbol_principal": any(a.en_arbol_principal for a in aplicados),
            "aplicados": [vars(a) for a in aplicados],
        }
        self.ruta.parent.mkdir(parents=True, exist_ok=True)
        self.ruta.write_text(
            json.dumps(datos, ensure_ascii=False, indent=2), encoding="utf-8"
        )

    def abrir(self) -> None:
        with self._cerrojo:
            self._volcar()

    def aplicar(self, ruta: Path, mutante: Mutante, fuente_original: str) -> None:
        """Anota que `ruta` acaba de quedar mutada, con cómo era su línea."""
        brutas = fuente_original.split("\n")
        original = brutas[mutante.linea - 1] if mutante.linea <= len(brutas) else ""
        absoluta = Path(ruta).resolve()
        with self._cerrojo:
            self._aplicados[absoluta.as_posix()] = Aplicado(
                fichero=mutante.fichero,
                ruta=absoluta.as_posix(),
                linea=mutante.linea,
                descripcion=mutante.descripcion(),
                linea_original=original,
                en_arbol_principal=_dentro_de(absoluta, self.raiz),
            )
            self._volcar()

    def soltar(self, ruta: Path) -> None:
        """Anota que `ruta` ya está restaurada."""
        with self._cerrojo:
            self._aplicados.pop(Path(ruta).resolve().as_posix(), None)
            self._volcar()

    def cerrar(self) -> None:
        """Retira el centinela: la campaña terminó y el árbol está limpio."""
        with self._cerrojo:
            self._aplicados.clear()
            self.ruta.unlink(missing_ok=True)

    # -- lectura y recuperación ---------------------------------------------

    @staticmethod
    def leer(raiz: str = ".") -> dict | None:
        """Contenido del centinela de `raiz`, o `None` si no hay campaña anotada."""
        ruta = Path(raiz).resolve() / RUTA_CENTINELA
        if not ruta.is_file():
            return None
        try:
            return json.loads(ruta.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            # Un centinela ilegible es igual de sospechoso que uno legible: hubo
            # una campaña y nadie la cerró. Se devuelve lo mínimo para avisar.
            return {"feature": "", "aplicados": [], "muta_arbol_principal": True,
                    "ilegible": True}


def _dentro_de(ruta: Path, directorio: Path) -> bool:
    """¿`ruta` cuelga de `directorio`? (sin `is_relative_to`, que exige 3.9+)."""
    try:
        Path(ruta).resolve().relative_to(Path(directorio).resolve())
    except ValueError:
        return False
    return True


def restaurar_desde_centinela(raiz: str = ".") -> tuple[list[str], list[str]]:
    """Deshace los mutantes que dejó aplicados una campaña muerta a medias.

    Devuelve `(restaurados, irrecuperables)`. Se reescribe LA LÍNEA mutada con
    el texto original que el propio centinela guardó, que es lo único que la
    campaña llegó a cambiar; solo si esos datos no sirven se recurre a `git
    checkout`, que reescribe el fichero entero y, con `core.autocrlf` de por
    medio, puede cambiarle los finales de línea por el camino. Lo que no se pueda
    arreglar se devuelve para que lo vea un humano: dejarlo en silencio sería lo
    peor.
    """
    datos = Centinela.leer(raiz)
    if datos is None:
        return ([], [])
    restaurados: list[str] = []
    irrecuperables: list[str] = []
    for aplicado in datos.get("aplicados", []):
        ruta = Path(str(aplicado.get("ruta", "")))
        if not ruta.is_file():
            continue  # el worktree ya no está: no hay nada que restaurar
        original = aplicado.get("linea_original")
        numero = int(aplicado.get("linea", 0) or 0)
        brutas = _leer(ruta).split("\n") if original is not None else []
        if original is not None and 0 < numero <= len(brutas):
            brutas[numero - 1] = original
            _escribir(ruta, "\n".join(brutas))
            restaurados.append(ruta.as_posix())
            continue
        codigo, _ = _git_en(str(ruta.parent), "checkout", "--", str(ruta))
        if codigo == 0:
            restaurados.append(ruta.as_posix())
        else:
            irrecuperables.append(ruta.as_posix())
    Path(raiz).resolve().joinpath(RUTA_CENTINELA).unlink(missing_ok=True)
    return (restaurados, irrecuperables)


# --- Restauración a prueba de señales ----------------------------------------


@contextmanager
def restauracion_ante_senales(restaurar: Callable[[], None]) -> Iterator[None]:
    """Ejecuta `restaurar` también cuando llegan SIGINT o SIGTERM.

    `try/finally` no cubre una señal que termina el proceso: SIGTERM lo mata sin
    desenrollar la pila y el árbol se queda mutado. Aquí se atiende la señal, se
    restaura y se convierte en la excepción que el `finally` de siempre sabe
    tratar (`KeyboardInterrupt` para SIGINT, `SystemExit` para SIGTERM).

    Solo el hilo principal puede registrar manejadores: llamado desde un worker
    esto no hace nada, a propósito y sin ruido. En la campaña paralela el
    manejador lo pone el coordinador, que sí es el hilo principal, y restaura
    los worktrees de todos leyendo el centinela compartido.
    """
    anteriores: dict[int, object] = {}

    def manejador(numero: int, _marco: object) -> None:
        restaurar()
        if numero == getattr(signal, "SIGINT", None):
            raise KeyboardInterrupt
        raise SystemExit(128 + numero)

    for nombre in ("SIGINT", "SIGTERM"):
        numero = getattr(signal, nombre, None)
        if numero is None:
            continue
        try:
            anteriores[int(numero)] = signal.signal(numero, manejador)
        except (ValueError, OSError):
            pass  # hilo secundario, o plataforma que no permite ese manejador
    try:
        yield
    finally:
        for numero, anterior in anteriores.items():
            try:
                signal.signal(numero, anterior)  # type: ignore[arg-type]
            except (ValueError, OSError, TypeError):
                pass


# --- Guardia del árbol de trabajo --------------------------------------------


def _git_en(raiz: str, *args: str) -> tuple[int, str]:
    """git en `raiz`, devolviendo `(código, salida)`.

    No se reutiliza `harness.alcance.ejecutar_git`: aquel devuelve cadena vacía
    cuando git falla, y aquí distinguir «no hay cambios» de «git ha fallado»
    decide si la campaña arranca o no.
    """
    proceso = subprocess.run(
        ["git", "-C", str(raiz), *args],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    return proceso.returncode, (proceso.stdout or "") + (proceso.stderr or "")


def sha_de_head(raiz: str = ".") -> str | None:
    """SHA COMPLETO de `HEAD` en `raiz`, o `None` si git no puede responder.

    Completo y no abreviado a propósito: es lo que permite comprobar sin
    ambigüedad que el alcance medido y el alcance revisado son el mismo commit.
    Fuera de un repositorio devuelve `None`, y el informe imprime `n/d`: nunca
    un SHA inventado.
    """
    codigo, salida = _git_en(raiz, "rev-parse", "HEAD")
    if codigo != 0:
        return None
    sha = salida.strip()
    return sha or None


def ficheros_con_cambios(raiz: str, ficheros: list[str]) -> list[str]:
    """De los ficheros dados, cuáles tienen cambios sin commitear.

    Lanza `ArbolSucio` si git no puede responder: no saber si un fichero que
    vamos a sobrescribir tiene trabajo dentro es exactamente el caso en que no
    hay que tocarlo.
    """
    if not ficheros:
        return []
    codigo, salida = _git_en(raiz, "status", "--porcelain", "--", *ficheros)
    if codigo != 0:
        raise ArbolSucio(
            f"No se pudo comprobar el estado del árbol en {raiz} (git salió con "
            f"{codigo}). La campaña sobrescribe ficheros de producción para "
            "mutarlos y no arranca sin saber si tienen trabajo sin commitear.\n"
            f"    {salida.strip()}"
        )
    sucios: list[str] = []
    for linea in salida.splitlines():
        if len(linea) < 4:
            continue
        ruta = linea[3:].strip().strip('"')
        if " -> " in ruta:  # renombrado: interesa el destino
            ruta = ruta.split(" -> ", 1)[1].strip().strip('"')
        if ruta not in sucios:
            sucios.append(ruta)
    return sucios


def guardia_arbol_limpio(raiz: str, ficheros: list[str]) -> None:
    """Se niega a arrancar si algún fichero a mutar tiene cambios sin commitear.

    Dos motivos, los dos vividos: (1) al restaurar, la campaña reescribe el
    fichero con lo que leyó al empezar, así que un cambio a medias que llegue
    después se pierde; (2) si la campaña muere a machetazos, el mutante que deje
    escrito es indistinguible de trabajo real y acaba commiteado.
    """
    sucios = ficheros_con_cambios(raiz, ficheros)
    if not sucios:
        return
    listado = "\n".join(f"    - {ruta}" for ruta in sucios)
    raise ArbolSucio(
        "Hay cambios sin commitear en ficheros que esta campaña va a MUTAR:\n"
        f"{listado}\n"
        "\n"
        "  La campaña los sobrescribe y los restaura a como estaban al empezar, "
        "así que\n"
        "  cualquier edición posterior se perdería; y si la campaña muere a "
        "medias, el\n"
        "  mutante que quede en disco es indistinguible de tu trabajo. "
        "Commitea (o guarda\n"
        "  en un stash) y vuelve a lanzarla."
    )


# --- Campaña ----------------------------------------------------------------


@dataclass
class InformeMutacion:
    """Resultado de una campaña de mutación."""

    feature: str
    alcance: Alcance
    generados: int = 0
    muertos: int = 0
    supervivientes: list[Mutante] = field(default_factory=list)
    timeouts: list[Mutante] = field(default_factory=list)
    mutantes_evaluados: list[Mutante] = field(default_factory=list)
    segundos: float = 0.0
    muestreado: bool = False
    max_mutantes: int | None = None
    semilla: int | None = None
    #: Mutantes juzgados sobre una base que ya estaba rota. Ni muertos ni
    #: supervivientes: de ellos no se sabe nada.
    base_rota: list[Mutante] = field(default_factory=list)
    #: Motivo por el que los números de este informe NO son de fiar, si lo hay.
    aviso_base: str | None = None
    #: SHA completo de HEAD contra el que se midió. Sin él, un informe sigue
    #: pareciendo válido después de que la rama crezca mil líneas (RM1/R10).
    sha_head: str | None = None
    #: Segundos que tardó la suite LIMPIA en cada ejecutor implicado (R11). Es
    #: el patrón contra el que se lee la media por mutante.
    segundos_linea_base: dict[str, float] = field(default_factory=dict)
    #: Nivel de rigor que fijó el muestreo, para que el informe diga quién lo
    #: decidió (R9). Lo rellena el CLI, que es quien lo resuelve.
    nivel: str | None = None
    #: Segundos concedidos a CADA mutante en esta campaña (R4). Ya no es el
    #: valor de `rigor.json` sino uno derivado de la línea base medida, y sin
    #: declararlo los tiempos de dos campañas dejan de ser comparables sin que
    #: nadie se entere.
    timeout_efectivo: int | None = None
    #: El SUELO del que se partió (`mutacion.timeout_por_mutante_s`), o el valor
    #: de `--timeout` si se fijó a mano.
    timeout_suelo: int | None = None
    #: ¿El timeout vino de `--timeout` en vez de derivarse? (R6)
    timeout_fijado: bool = False
    #: Workers con los que se midió. Con W workers el «Tiempo total» es
    #: aproximadamente `mutantes × media / W`, y sin este número la regla de
    #: coherencia de RM2 marcaría como sospechosa una campaña legítima (R27).
    workers: int | None = None
    #: Rama de integración contra la que se calculó el alcance. Sin ella, el
    #: comando de la cabecera mide OTRO alcance cuando la rama nace de otra
    #: feature: el diff contra `dev` arrastra las líneas de la feature madre.
    base: str | None = None
    #: Cuántos mutantes que quedaron en `timeout` se repasaron después EN SERIE.
    #: Cero significa «no hizo falta» o «esta campaña no repasa», nunca «no se
    #: hace»: el informe distingue los tres casos por escrito.
    timeouts_repasados: int = 0

    @property
    def segundos_por_mutante(self) -> float | None:
        """Media de segundos por mutante evaluado; `None` si no se evaluó ninguno."""
        if not self.mutantes_evaluados:
            return None
        return self.segundos / len(self.mutantes_evaluados)

    @property
    def evaluados(self) -> int:
        return len(self.mutantes_evaluados)

    @property
    def timeouts_resueltos(self) -> int:
        """Repasados a los que el repaso SÍ les sacó un veredicto de verdad.

        Lo que sigue en `timeouts` tras el repaso ya no tiene la contención como
        excusa: se midió a solas y aun así agotó el reloj.
        """
        return max(0, self.timeouts_repasados - len(self.timeouts))

    @property
    def fiable(self) -> bool:
        """¿Se puede cerrar una feature con estos números?"""
        return self.aviso_base is None and not self.base_rota


def ejecutar_campania(
    alcance: Alcance,
    ejecutor: object,
    timeout_s: int = TIMEOUT_POR_DEFECTO,
    raiz: str = ".",
    max_mutantes: int | None = None,
    semilla: int | None = None,
    mutantes: list[Mutante] | None = None,
    eco: Callable[[str], None] | None = None,
    ejecutor_de: Callable[[str], object] | None = None,
    centinela: Centinela | None = None,
    comprobar_arbol: bool = True,
    comprobar_base: bool = True,
    timeout_base_s: int | None = None,
    timeout_fijado: bool = False,
    workers: int = 1,
) -> InformeMutacion:
    """Muta el alcance, mutante a mutante, y cuenta cuántos sobreviven.

    Con `ejecutor_de` se elige el ejecutor fichero a fichero (un monorepo juzga
    cada mutante con la suite de su servicio); sin ella se usa `ejecutor` para
    todo, que es el caso de un repositorio de un solo proyecto.

    Antes de juzgar a nadie corre la LÍNEA BASE —la suite sin mutar nada, en el
    mismo sitio y con el mismo intérprete— y aborta con `BaseRota` si no está
    verde. Y antes de eso se niega a arrancar, con `ArbolSucio`, si algún
    fichero a mutar tiene cambios sin commitear. Las dos comprobaciones se
    pueden desactivar (`comprobar_base`, `comprobar_arbol`) para el único caso
    en que sobran: la campaña paralela, cuyo coordinador ya comprobó el árbol
    principal entero y cuyos worktrees se comprueban uno a uno igualmente.

    `timeout_s` es el SUELO por mutante, no el techo: tras medir la línea base
    se deriva de ella el timeout efectivo (`timeout_derivado`), que nunca baja
    del suelo. Con `timeout_fijado=True` —`--timeout N` en la orden— no se
    deriva nada y `timeout_s` se usa tal cual (R6). La propia línea base corre
    con su reloj aparte y más holgado, `timeout_base_s` (R2).

    Garantía dura: pase lo que pase (excepción, timeout, Ctrl-C, SIGTERM),
    ningún fichero queda mutado en el árbol de trabajo al salir de esta función.
    """
    inicio = time.monotonic()
    suelo = timeout_s
    if timeout_base_s is None:
        timeout_base_s = timeout_de_linea_base(suelo)
    base = Path(raiz)
    fuentes: dict[str, str] = {}

    for fichero in alcance.ficheros():
        ruta = base / fichero
        if ruta.is_file():
            fuentes[fichero] = _leer(ruta)

    if mutantes is None:
        mutantes = []
        for fichero, fuente in fuentes.items():
            mutantes.extend(generar_mutantes(fuente, alcance.lineas[fichero], fichero))

    informe = InformeMutacion(
        feature=alcance.feature,
        alcance=alcance,
        generados=len(mutantes),
        max_mutantes=max_mutantes,
        semilla=semilla,
        sha_head=sha_de_head(raiz),
        timeout_suelo=suelo,
        timeout_efectivo=suelo,
        timeout_fijado=timeout_fijado,
        workers=workers,
    )

    if max_mutantes is not None and len(mutantes) > max_mutantes:
        sorteo = random.Random(semilla)
        mutantes = sorted(
            sorteo.sample(mutantes, max_mutantes),
            key=lambda m: (m.fichero, m.linea, m.col, m.operador),
        )
        informe.muestreado = True

    # Nada de esto toca un solo fichero: si algo falla, falla ANTES de mutar.
    if comprobar_arbol:
        guardia_arbol_limpio(raiz, list(fuentes))
    implicados = ejecutores_implicados(list(mutantes), ejecutor, ejecutor_de)
    if comprobar_base and mutantes:
        informe.segundos_linea_base = comprobar_linea_base(
            implicados, timeout_base_s, eco, workers=workers
        )
        # Aquí es donde la campaña deja de adivinar: la suite limpia ya se ha
        # corrido en el sitio donde se va a juzgar y con los W workers
        # compitiendo, así que lo que tardó ES la medida buena. Antes ese
        # número se tiraba y el timeout salía de un fijo de `rigor.json`.
        if not timeout_fijado:
            timeout_s = timeout_derivado(suelo, informe.segundos_linea_base)
            informe.timeout_efectivo = timeout_s
            if eco is not None and informe.segundos_linea_base:
                peor = max(informe.segundos_linea_base.values())
                eco(
                    f"{MARCA_LINEA_BASE}timeout por mutante: {timeout_s} s "
                    f"= max(suelo {suelo} s, peor línea base {peor:.1f} s "
                    f"× margen {MARGEN_TIMEOUT})"
                )

    def restaurar_todo() -> None:
        """Devuelve al disco los fuentes tal y como se leyeron al empezar."""
        for fichero, fuente in fuentes.items():
            ruta = base / fichero
            if ruta.is_file() and _leer(ruta) != fuente:
                _escribir(ruta, fuente)
            if centinela is not None:
                centinela.soltar(ruta)

    try:
        with restauracion_ante_senales(restaurar_todo):
            for indice, mutante in enumerate(mutantes, start=1):
                fuente = fuentes.get(mutante.fichero)
                if fuente is None:
                    continue
                informe.mutantes_evaluados.append(mutante)
                mutada = aplicar_mutante(fuente, mutante)

                try:
                    compile(mutada, mutante.fichero, "exec")
                except (SyntaxError, ValueError):
                    informe.muertos += 1  # no compila: los tests lo cazarían siempre
                    continue

                elegido = (
                    ejecutor if ejecutor_de is None else ejecutor_de(mutante.fichero)
                )

                ruta = base / mutante.fichero
                try:
                    _escribir(ruta, mutada)
                    if centinela is not None:
                        centinela.aplicar(ruta, mutante, fuente)
                    veredicto = elegido.ejecutar(timeout_s)
                finally:
                    _escribir(ruta, fuente)
                    if centinela is not None:
                        centinela.soltar(ruta)

                if veredicto == INDETERMINADO:
                    # La suite ni llegó a juzgar. Puede ser el mutante (rompió
                    # la recolección) o puede ser la base. Cuesta una ejecución
                    # más averiguarlo, y es la diferencia entre un número y una
                    # invención.
                    veredicto = _resolver_indeterminado(elegido, timeout_s)

                if veredicto == SUPERVIVIENTE:
                    informe.supervivientes.append(mutante)
                elif veredicto == TIMEOUT:
                    informe.timeouts.append(mutante)
                elif veredicto == BASE_ROTA:
                    informe.base_rota.append(mutante)
                else:
                    informe.muertos += 1

                if eco is not None:
                    eco(
                        f"[{indice}/{len(mutantes)}] {veredicto:13} "
                        f"{mutante.descripcion()}"
                    )
    finally:
        # Red de seguridad: si algo se torció entre medias, el árbol vuelve a
        # su estado original igualmente.
        restaurar_todo()
        informe.segundos = time.monotonic() - inicio

    # La línea base del arranque protege el arranque. Ésta protege el resto: si
    # la base se rompió a mitad —alguien borró un fichero, el entorno cambió—,
    # cada «muerto» contado desde entonces es un mutante que nadie cazó.
    if comprobar_base and informe.mutantes_evaluados:
        # Con el timeout de la BASE, no con el del mutante: ésta también es una
        # suite limpia y también se corre una vez por worker (R2). Dársela con
        # el reloj corto convertiría en «base expirada» lo que solo es una
        # máquina ocupada, y eso ya es la mentira que arregla R11.
        informe.aviso_base = _base_rota_al_final(implicados, timeout_base_s)
    if informe.base_rota and informe.aviso_base is None:
        informe.aviso_base = (
            f"{len(informe.base_rota)} mutante(s) se juzgaron con la suite rota "
            "por causas ajenas a la mutación: no cuentan ni como muertos ni como "
            "supervivientes, y la campaña no cubre esas líneas."
        )

    return informe


def _resolver_indeterminado(ejecutor: object, timeout_s: int) -> str:
    """¿La suite se rompió por el mutante o ya estaba rota?

    Se corre la línea base ahí mismo. Verde: fue el mutante quien reventó la
    recolección, y eso es una muerte legítima. Roja: no se sabe nada de este
    mutante, y decir «muerto» sería inventárselo.
    """
    correr_base = getattr(ejecutor, "linea_base", None)
    if correr_base is None:
        return BASE_ROTA
    return MUERTO if correr_base(timeout_s).verde else BASE_ROTA


def mensaje_base_expirada_al_final(etiqueta: str, timeout_s: int) -> str:
    """Aviso de una línea base de cierre que se quedó SIN TIEMPO (R11).

    Expirar no es fallar. La suite no dijo que nada esté mal: no llegó a
    terminar. Confundir las dos cosas no es un matiz de redacción: el
    2026-08-21, en la primera ejecución real de la campaña paralela, el arnés
    dijo «Arregla la suite y repite la campaña» sobre una suite impecable y
    mandó al humano a buscar un fallo que no existía. Lo que hay que tocar aquí
    es el reloj —menos workers compitiendo, o más suelo—, no los tests.
    """
    return (
        f"La línea base de cierre EXPIRÓ en {etiqueta}: se agotaron los "
        f"{timeout_s} s concedidos. Ojo, la suite NO falló y NO hay ningún test "
        "roto que buscar: se quedó sin tiempo, que es otra cosa. Aun así los "
        "números de esta campaña no valen, porque no se ha podido comprobar que "
        "la base siguiera verde al terminar.\n"
        "  Qué hacer: baja los workers (--workers N: cada worker corre una suite "
        "entera y todas compiten por la misma máquina) o sube el SUELO "
        "'mutacion.timeout_por_mutante_s' de harness/rigor.json. No toques la "
        "suite."
    )


def _base_rota_al_final(implicados: list[tuple[str, object]], timeout_s: int) -> str | None:
    """Reejecuta la línea base al cerrar. Devuelve el motivo si dejó de estar verde.

    Distingue las dos formas de no estar verde, porque tienen arreglos opuestos:
    EXPIRAR es un problema de reloj (R11) y FALLAR es un problema de suite
    (R12). La consecuencia sí es la misma en los dos casos: el informe queda
    marcado como no fiable y la ejecución sale con 3 (R13).
    """
    for etiqueta, ejecutor in implicados:
        correr_base = getattr(ejecutor, "linea_base", None)
        if correr_base is None:
            continue
        resultado = correr_base(timeout_s)
        if resultado.expirado:
            return mensaje_base_expirada_al_final(etiqueta, timeout_s)
        if not resultado.verde:
            fallidos = ", ".join(resultado.fallidos()) or f"código {resultado.codigo}"
            return (
                f"La línea base estaba VERDE al empezar y ROJA al terminar en "
                f"{etiqueta} ({fallidos}). La base se rompió durante la campaña, "
                "así que los mutantes contados como «muertos» pueden no estarlo: "
                "estos números NO valen para cerrar una feature. Arregla la suite "
                "y repite la campaña."
            )
    return None


# --- Informe ----------------------------------------------------------------


#: Cabecera con la que se escribe un análisis que todavía nadie ha hecho.
CABECERA_PENDIENTE = "#### Análisis (PENDIENTE del implementer)"

#: Encabezado de la sección de un superviviente en el informe.
PATRON_SUPERVIVIENTE = re.compile(
    r"^### \d+\. `(?P<fichero>.+):(?P<linea>\d+)` \[(?P<operador>[^\]]+)\]$"
)

#: Aviso que acompaña a un análisis traído de la campaña anterior.
AVISO_REPUESTO = (
    "> _Análisis traído de la campaña anterior de esta feature: el mutante "
    "volvió a sobrevivir con el mismo operador y el mismo texto. Reléelo si el "
    "código de alrededor ha cambiado._"
)


def clave_de_mutante(fichero: str, operador: str, original: str, mutado: str) -> tuple:
    """Identidad de un superviviente entre dos campañas, SIN el número de línea.

    La línea se mueve en cuanto alguien añade un import más arriba, y aun así
    sigue siendo el mismo mutante: mismo fichero, mismo operador y mismo cambio
    de texto. Incluirla haría que el análisis se perdiera en cuanto el fichero
    respirase, que es justo lo que este mecanismo evita.
    """
    return (fichero, operador, original, mutado)


def analisis_escritos(texto: str) -> dict[tuple, str]:
    """Análisis ya redactados en un informe anterior, indexados por mutante.

    Devuelve solo los que alguien ha escrito de verdad: la plantilla vacía no
    cuenta. Si dos supervivientes comparten clave con análisis distintos, la
    clave se descarta entera —reponer el análisis equivocado sería peor que no
    reponer ninguno—.
    """
    encontrados: dict[tuple, list[str]] = {}
    lineas = texto.splitlines()
    indice = 0
    while indice < len(lineas):
        cabecera = PATRON_SUPERVIVIENTE.match(lineas[indice].strip())
        if cabecera is None:
            indice += 1
            continue
        fichero = cabecera.group("fichero")
        operador = cabecera.group("operador")
        original = mutado = None
        analisis: list[str] = []
        indice += 1
        while indice < len(lineas) and not lineas[indice].startswith("### "):
            linea = lineas[indice]
            if linea.startswith("- Original: `") and original is None:
                original = linea[len("- Original: `") : -1]
            elif linea.startswith("- Mutado:") and mutado is None:
                mutado = linea.split("`", 1)[1][:-1] if "`" in linea else None
            elif linea.startswith("#### Análisis"):
                if linea.strip() != CABECERA_PENDIENTE:
                    analisis = [linea]
                    indice += 1
                    while indice < len(lineas) and not lineas[indice].startswith(
                        ("### ", "## ")
                    ):
                        analisis.append(lineas[indice])
                        indice += 1
                    break
            indice += 1
        if analisis and original is not None and mutado is not None:
            clave = clave_de_mutante(fichero, operador, original, mutado)
            texto_analisis = "\n".join(analisis).rstrip()
            encontrados.setdefault(clave, []).append(texto_analisis)
    return {
        clave: textos[0]
        for clave, textos in encontrados.items()
        if len(set(textos)) == 1
    }


#: Prefijos de las filas del informe cuyo valor depende de CÓMO se corrió la
#: campaña —el reloj y las condiciones— y no de lo que midió. Vive aquí, al lado
#: de quien las escribe, para que quien añada una fila así la vea y la declare:
#: cuando esta lista se mantenía a mano dentro de un test de paridad
#: serie/paralelo, la fila que añadió una versión posterior se quedó fuera y
#: dejó el test flaky durante semanas. Se compara por PREFIJO a propósito:
#: `| Línea base (s) — \`etiqueta\`` lleva pegada la etiqueta del ejecutor.
#:
#: `Workers` y `Timeout efectivo` entraron después: una campaña en serie y una
#: paralela sobre el mismo commit tienen que dar informes comparables, y esas
#: dos filas no coinciden nunca. El `Suelo configurado` NO entra: sale de
#: `rigor.json`, es el mismo en las dos, y que deje de coincidir es una
#: diferencia real que hay que ver.
FILAS_DE_RELOJ: tuple[str, ...] = (
    "Generado por",
    "| Tiempo total",
    "| Línea base (s)",
    "| Media por mutante evaluado (s)",
    "| Timeout efectivo por mutante (s)",
    "| Workers",
    "| Timeouts repasados en serie",
)

#: Comentario de ruta con el que arranca todo informe. Cambia con el nombre del
#: fichero, no con lo medido, así que tampoco entra en la comparación.
_COMENTARIO_DE_RUTA = "<!-- "


def lineas_comparables(texto: str) -> list[str]:
    """Las líneas de un informe que dos campañas equivalentes deben compartir.

    Descarta las de `FILAS_DE_RELOJ` y el comentario de ruta de la cabecera. Lo
    que queda es el resultado de la medición —alcance, totales, SHA, muestreo y
    fichas de supervivientes—, que sí tiene que coincidir entre la campaña en
    serie y la paralela.
    """
    return [
        linea
        for linea in texto.splitlines()
        if not linea.startswith(FILAS_DE_RELOJ)
        and not linea.startswith(_COMENTARIO_DE_RUTA)
    ]


def comando_de(informe: InformeMutacion) -> str:
    """El comando que reproduce esta campaña: base y workers incluidos.

    Sin `--workers` el comando de la cabecera no reproduce nada: la misma
    feature medida con 1 worker y con 16 da tiempos que se diferencian en un
    factor 4 y, hasta que la campaña repasa sus timeouts, veredictos distintos.

    Y sin `--base` reproduce **otro alcance**, que es peor: cuando la rama de la
    feature nace de otra rama de feature en vez de `dev`, el diff contra `dev`
    arrastra las líneas de la feature madre, así que ni el número de mutantes ni
    la lista de supervivientes son comparables. Pasó de verdad el 2026-09-06.

    Solo se escribe lo que no es el valor de siempre: repetir `--base dev` en
    todos los informes es ruido que acaba haciendo que nadie lea la línea.
    """
    comando = f"python -m harness.mutacion --feature {informe.feature}"
    if informe.base is not None and informe.base != BASE_POR_DEFECTO:
        comando += f" --base {informe.base}"
    if informe.workers is not None:
        comando += f" --workers {informe.workers}"
    return comando


def fila_del_repaso(informe: InformeMutacion) -> str:
    """Fila de totales que cuenta qué pasó con los mutantes en `timeout`.

    Se imprime SIEMPRE, también cuando no hubo ninguno: una fila ausente se lee
    como descuido, y un `0` a secas no distingue «no hizo falta repasar» de «este
    arnés no repasa». Las tres redacciones son los tres casos reales.
    """
    if informe.timeouts_repasados:
        return (
            f"| Timeouts repasados en serie | {informe.timeouts_repasados} — "
            f"{informe.timeouts_resueltos} con veredicto tras el repaso, "
            f"{len(informe.timeouts)} en timeout todavía |"
        )
    if informe.timeouts:
        return (
            "| Timeouts repasados en serie | 0: campaña en serie, el reloj ya "
            "midió a cada mutante a solas |"
        )
    return "| Timeouts repasados en serie | 0: ningún mutante agotó el reloj |"


def escribir_informe(informe: InformeMutacion, ruta: Path) -> None:
    """Escribe el informe de la campaña en Markdown.

    Si ya había un informe de esta feature, los análisis de supervivientes que
    alguien hubiera escrito se conservan. Una campaña se repite muchas veces
    —tras un merge, tras cambiar el arnés, tras tocar un test— y sobrescribir
    ese trabajo con la plantilla vacía borra evidencia que el reviewer exige y
    que a veces el humano ya ha dado por buena.
    """
    alcance = informe.alcance
    previos = analisis_escritos(ruta.read_text(encoding="utf-8")) if ruta.exists() else {}
    lineas: list[str] = [
        f"<!-- {ruta.as_posix()} -->",
        f"# {informe.feature} · Campaña de mutación",
        "",
        f"Generado por `{comando_de(informe)}` "
        f"el {datetime.now().strftime('%Y-%m-%d %H:%M')}.",
        "",
    ]
    if not informe.fiable:
        # Lo primero que se lee, y en negrita: un informe cuyos números no valen
        # tiene que decirlo antes que nada, o el reviewer suma la tabla y cierra.
        lineas += [
            "> ## ⚠ CAMPAÑA NO VÁLIDA",
            ">",
            f"> {informe.aviso_base}",
            ">",
            "> **No cierres la feature con estos números.** Arregla la línea base "
            "y repite la campaña.",
            "",
        ]
    # Un alcance declarado a mano (`--ficheros`) no tiene diff detrás: enseñar
    # dos refs entre backticks haría creer que se comparó algo con algo.
    if alcance.origen == ORIGEN_FICHEROS:
        origen_del_alcance = (
            f"Origen del diff: **{ORIGEN_FICHEROS}** (alcance declarado en la orden)."
        )
    else:
        origen_del_alcance = (
            f"Origen del diff: **{alcance.origen}** "
            f"(`{alcance.ref_diff[0]}` .. `{alcance.ref_diff[1]}`)."
        )
    lineas += [
        "## Alcance",
        "",
        origen_del_alcance,
        "",
        "| Fichero | Líneas en alcance |",
        "|---|---|",
    ]
    for fichero in alcance.ficheros():
        lineas.append(f"| `{fichero}` | {len(alcance.lineas[fichero])} |")
    lineas += [
        f"| **Total** | **{alcance.total_lineas()}** |",
        "",
        "## Totales",
        "",
        "| Métrica | Valor |",
        "|---|---|",
        f"| Mutantes generados | {informe.generados} |",
        f"| Mutantes evaluados | {informe.evaluados} |",
        f"| Muertos | {informe.muertos} |",
        f"| Supervivientes | {len(informe.supervivientes)} |",
        f"| Timeouts | {len(informe.timeouts)} |",
        fila_del_repaso(informe),
        f"| Sin veredicto (base rota) | {len(informe.base_rota)} |",
        f"| Tiempo total | {informe.segundos:.1f} s |",
    ]
    # RM1: sin el commit medido, un informe sigue pareciendo válido después de
    # que la rama crezca mil líneas. RM2: con la línea base y la media por
    # mutante juntas, una campaña imposiblemente rápida se ve leyendo. Las tres
    # filas se imprimen SIEMPRE; lo que no se sabe se dice `n/d`, porque un cero
    # se lee como medición y una fila ausente, como descuido.
    lineas.append(
        f"| SHA de HEAD medido | `{informe.sha_head}` |"
        if informe.sha_head
        else "| SHA de HEAD medido | n/d |"
    )
    if informe.segundos_linea_base:
        for etiqueta, segundos in sorted(informe.segundos_linea_base.items()):
            lineas.append(f"| Línea base (s) — `{etiqueta}` | {segundos:.1f} |")
    else:
        lineas.append("| Línea base (s) | n/d |")
    media = informe.segundos_por_mutante
    lineas.append(
        f"| Media por mutante evaluado (s) | {media:.1f} |"
        if media is not None
        else "| Media por mutante evaluado (s) | n/d |"
    )
    # R4. El timeout por mutante ya no es el fijo de `rigor.json`: se deriva
    # de la línea base medida. Sin declararlo, los tiempos de dos
    # campañas dejan de ser comparables sin que nadie se entere, y el reviewer
    # compara peras con manzanas creyendo que compara lo mismo.
    if informe.timeout_efectivo is None:
        lineas.append("| Timeout efectivo por mutante (s) | n/d |")
    elif informe.timeout_fijado:
        lineas.append(
            f"| Timeout efectivo por mutante (s) | {informe.timeout_efectivo} "
            "— fijado a mano con `--timeout`, sin derivar |"
        )
    else:
        lineas.append(
            f"| Timeout efectivo por mutante (s) | {informe.timeout_efectivo} "
            f"— derivado de la línea base × {MARGEN_TIMEOUT} |"
        )
    lineas.append(
        f"| Suelo configurado (s) | {informe.timeout_suelo} |"
        if informe.timeout_suelo is not None
        else "| Suelo configurado (s) | n/d |"
    )
    lineas.append(
        f"| Workers | {informe.workers} |"
        if informe.workers is not None
        else "| Workers | n/d |"
    )
    if informe.muestreado:
        lineas.append(
            f"| Muestreo | sí — {informe.evaluados} de {informe.generados} "
            f"mutantes, semilla `{informe.semilla}`, nivel "
            f"`{informe.nivel or 'n/d'}` |"
        )
    else:
        lineas.append("| Muestreo | no: campaña completa |")

    lineas += ["", "## Supervivientes", ""]
    if not informe.supervivientes:
        lineas += ["Ninguno: cada mutación aplicada la cazó al menos un test.", ""]
    else:
        lineas += [
            "Cada superviviente es una línea que ningún test comprueba de verdad, "
            "o una mutación equivalente. Distinguirlo es trabajo del implementer: "
            "ningún análisis puede quedarse sin completar al cerrar la feature.",
            "",
        ]
        for numero, mutante in enumerate(informe.supervivientes, start=1):
            lineas += [
                f"### {numero}. `{mutante.fichero}:{mutante.linea}` "
                f"[{mutante.operador}]",
                "",
                f"- Original: `{mutante.original}`",
                f"- Mutado:   `{mutante.mutado}`",
                "",
            ]
            previo = previos.get(
                clave_de_mutante(
                    mutante.fichero, mutante.operador, mutante.original, mutante.mutado
                )
            )
            if previo is None:
                lineas += [
                    CABECERA_PENDIENTE,
                    "",
                    "> Por qué ningún test lo caza: PENDIENTE.",
                    "> Decisión: ¿test nuevo o mutante equivalente justificado?",
                    "",
                ]
            else:
                lineas += [previo, "", AVISO_REPUESTO, ""]

    if informe.timeouts:
        lineas += ["## Timeouts", ""]
        if informe.timeouts_repasados:
            lineas += [
                (
                    "Estos agotaron el reloj **también al repasarlos en serie**, "
                    "uno a uno y sin nadie compitiendo por la máquina: la "
                    "contención ya no los explica. Míralos como un cuelgue de "
                    "verdad, no como ruido."
                ),
                "",
            ]
        else:
            lineas += [
                (
                    "Campaña sin repaso en serie: con un solo evaluador el reloj "
                    "ya midió a cada mutante a solas, así que estos timeouts son "
                    "suyos."
                ),
                "",
            ]
        for mutante in informe.timeouts:
            # `descripcion()` ya empieza por `fichero:linea`: anteponerlo daba
            # `- \`a.py:3\` a.py:3 [op] x -> y`. Nadie lo había leído nunca
            # porque la sección entera estaba sin test (R20).
            lineas.append(
                f"- `{mutante.fichero}:{mutante.linea}` [{mutante.operador}] "
                f"{mutante.original} -> {mutante.mutado}"
            )
        lineas.append("")

    if informe.base_rota:
        lineas += [
            "## Sin veredicto: la suite estaba rota por su cuenta",
            "",
            "De estos mutantes no se sabe nada. La suite falló sin que la "
            "mutación tuviera que ver, así que **no** cuentan como muertos: esas "
            "líneas se quedan sin comprobar hasta que la base vuelva a estar "
            "verde y la campaña se repita.",
            "",
        ]
        for mutante in informe.base_rota:
            lineas.append(f"- `{mutante.fichero}:{mutante.linea}` {mutante.descripcion()}")
        lineas.append("")

    ruta.parent.mkdir(parents=True, exist_ok=True)
    ruta.write_text("\n".join(lineas) + "\n", encoding="utf-8")


# --- CLI --------------------------------------------------------------------


def _analizar_argumentos(argv: list[str] | None) -> argparse.Namespace:
    analizador = argparse.ArgumentParser(
        prog="python -m harness.mutacion",
        description=(
            "Muta las líneas de producción que toca una feature y comprueba "
            "cuántas mutaciones sobreviven a la suite de tests."
        ),
    )
    analizador.add_argument(
        "--feature", default=None, help="Identificador de la feature, p. ej. F-XXX"
    )
    analizador.add_argument(
        "--base", default=BASE_POR_DEFECTO, help="Rama de integración"
    )
    analizador.add_argument("--rama", default=None, help="Rama de la feature")
    analizador.add_argument("--raiz", default=".", help="Raíz del repositorio a mutar")
    analizador.add_argument(
        "--ficheros",
        default=None,
        help=(
            "Rutas separadas por coma que se mutan ENTERAS, en vez de calcular "
            "el alcance desde el diff de la feature. Para campañas cuyo sujeto "
            "es un módulo, no un cambio. --feature sigue haciendo falta: es "
            "quien resuelve el nivel de rigor y, con él, el muestreo."
        ),
    )
    analizador.add_argument(
        "--timeout",
        type=int,
        default=None,
        help=(
            "Segundos por mutante, ANULANDO el cálculo automático. Sin este "
            "flag el timeout se deriva de la línea base medida y "
            "'mutacion.timeout_por_mutante_s' actúa solo como suelo."
        ),
    )
    analizador.add_argument(
        "--max-mutantes",
        type=int,
        default=None,
        help=(
            "Mutantes que se evalúan como mucho. 0 = SIN TOPE (la campaña "
            "entera), que es como se anula el tope que impone un nivel de "
            "rigor. Sin este flag, el 'max_mutantes' del nivel de la feature "
            "en harness/rigor.json; si el nivel no declara ninguno, sin tope."
        ),
    )
    analizador.add_argument(
        "--semilla",
        type=int,
        default=None,
        help=(
            "Semilla del muestreo. Sin ella, la 'semilla' del nivel de rigor "
            "de la feature: dos campañas del mismo nivel eligen los mismos "
            "mutantes y son comparables."
        ),
    )
    analizador.add_argument("--salida", default=None, help="Ruta del informe")
    analizador.add_argument(
        "--workers",
        type=int,
        default=None,
        help=(
            "Evaluadores concurrentes, cada uno en su git worktree. 1 = campaña "
            "en serie sobre el propio árbol (lo de siempre). Sin este flag, "
            "'mutacion.workers' de harness/rigor.json o los núcleos menos dos."
        ),
    )
    analizador.add_argument(
        "--estado",
        action="store_true",
        help=(
            "No lanza nada: dice si hay una campaña en curso según el centinela "
            "de .arnes_cache/. Códigos: 0 no hay, 3 la hay sin tocar el árbol "
            "principal, 4 la hay CON un mutante aplicado en el árbol principal. "
            "Lo usa harness/init.sh."
        ),
    )
    analizador.add_argument(
        "--restaurar",
        action="store_true",
        help=(
            "No lanza nada: deshace los mutantes que dejó aplicados una campaña "
            "muerta a medias, según el centinela, y lo retira."
        ),
    )
    opciones = analizador.parse_args(argv)
    # R23. Hasta hoy `--timeout 0` era falsy y caía EN SILENCIO al valor
    # configurado: quien lo escribía creía haber pedido algo y recibía otra
    # cosa. Un timeout de cero no es una opción legítima, así que se rechaza con
    # el código 2 de error de uso en vez de esconderse.
    if opciones.timeout is not None and opciones.timeout <= 0:
        analizador.error(
            f"--timeout tiene que ser un entero de segundos mayor que cero, y "
            f"es {opciones.timeout}."
        )
    if opciones.workers is not None and opciones.workers < 1:
        analizador.error(
            f"--workers tiene que ser 1 o más (1 = campaña en serie), y es "
            f"{opciones.workers}."
        )
    return opciones


#: Códigos de salida de `--estado`, para que init.sh no tenga que leer el JSON.
SIN_CAMPANIA = 0
CAMPANIA_AJENA = 3
CAMPANIA_MUTANDO = 4


def _modo_estado(raiz: str) -> int:
    """Informa de si hay una campaña en curso. Ver `--estado`."""
    datos = Centinela.leer(raiz)
    if datos is None:
        print("Sin campaña de mutación en curso.")
        return SIN_CAMPANIA
    aplicados = datos.get("aplicados", [])
    detalle = "; ".join(str(a.get("descripcion", "")) for a in aplicados) or "ninguno"
    cabecera = (
        f"Campaña de mutación EN CURSO: feature {datos.get('feature') or '(?)'}, "
        f"modo {datos.get('modo') or '(?)'}, pid {datos.get('pid') or '(?)'}, "
        f"desde {datos.get('inicio') or '(?)'}. Mutante aplicado: {detalle}."
    )
    if datos.get("muta_arbol_principal"):
        print(
            f"{cabecera}\n"
            "    El ÁRBOL PRINCIPAL tiene un mutante escrito ahora mismo: lo que "
            "midas aquí\n"
            "    (compilación, tests, cobertura) mide el mutante, no tu código. "
            "Si no hay\n"
            "    ninguna campaña viva, el centinela es basura de una que murió: "
            "recupérate\n"
            "    con `python -m harness.mutacion --restaurar`."
        )
        return CAMPANIA_MUTANDO
    print(
        f"{cabecera}\n"
        "    Muta en worktrees aparte, no en este árbol. Aun así compite por CPU "
        "y se\n"
        "    romperá si commiteas mientras corre."
    )
    return CAMPANIA_AJENA


def _modo_restaurar(raiz: str) -> int:
    """Deshace lo que dejó una campaña muerta a medias. Ver `--restaurar`."""
    datos = Centinela.leer(raiz)
    if datos is None:
        print("No hay centinela: no quedó ningún mutante aplicado.")
        return 0
    restaurados, irrecuperables = restaurar_desde_centinela(raiz)
    for ruta in restaurados:
        print(f"Restaurado: {ruta}")
    for ruta in irrecuperables:
        print(f"NO SE PUDO RESTAURAR: {ruta}", file=sys.stderr)
    if not restaurados and not irrecuperables:
        print("El centinela no apuntaba a ningún fichero vivo; retirado.")
    return 2 if irrecuperables else 0


def workers_por_defecto() -> int:
    """Workers cuando nadie dice nada: `min(max(1, (núcleos - 2) // 2), tope)`.

    Los dos primeros núcleos son los de siempre: la máquina y el coordinador. El
    `// 2` es lo que cambió sobre la fórmula vieja, y cambió porque aquélla suponía
    que el cuello de botella es la CPU. No lo es: cada worker arranca un proceso
    **pytest completo** —intérprete, importaciones, recolección, E/S de disco y,
    en un monorepo, un venv por servicio—, así que el recurso escaso es la
    máquina entera y no el núcleo. Se reserva del orden de dos núcleos por
    suite.

    En 4 núcleos da 1; en 8, 3; en 22, 4 (antes daba 16, y con 16 no cabía ni
    una línea base dentro de su timeout).
    """
    return min(max(1, ((os.cpu_count() or 1) - 2) // 2), TOPE_WORKERS)


def resolver_workers(pedidos: int | None, configurados: int | None) -> int:
    """Número de workers, por precedencia: `--workers` > `rigor.json` > default."""
    if pedidos is not None:
        return pedidos
    if configurados is not None:
        return configurados
    return workers_por_defecto()


def _timeout_configurado() -> int:
    """Timeout por mutante de `harness/rigor.json`, con red de seguridad.

    Se lee la configuración del arnés que ejecuta la campaña (el directorio
    actual), no la del repositorio mutado, que puede ser un árbol antiguo sin
    arnés. Un proyecto sin configuración de rigor cae en el valor por defecto;
    lo que nunca se cablea en el código es el umbral de cobertura.
    """
    try:
        return timeout_mutacion(cargar_rigor(RUTA_RIGOR))
    except ValueError:
        return TIMEOUT_POR_DEFECTO


def _workers_configurados() -> int | None:
    """Workers declarados en `harness/rigor.json`, o `None` si no hay clave."""
    try:
        return workers_mutacion(cargar_rigor(RUTA_RIGOR))
    except ValueError:
        return None


def resolver_muestreo(
    pedido_max: int | None,
    pedido_semilla: int | None,
    nivel_max: int | None,
    nivel_semilla: int | None,
) -> tuple[int | None, int | None]:
    """Tope y semilla del muestreo: `--max-mutantes` > nivel de rigor > sin tope.

    Función pura, y sede ÚNICA de la precedencia: la campaña en serie y la
    paralela reciben ya resuelto lo mismo. `pedido_max == 0` significa **sin
    tope**, que es la única forma de anular desde la orden el tope que impone un
    nivel; un negativo se trata igual, porque «menos de un mutante» no es un
    tope que nadie quiera.

    El tope y la semilla se resuelven por separado a propósito: pedir una
    semilla distinta para reproducir algo no debe deshacer el tope del nivel.
    """
    if pedido_max is not None:
        maximo = pedido_max if pedido_max > 0 else None
    else:
        maximo = nivel_max
    semilla = pedido_semilla if pedido_semilla is not None else nivel_semilla
    return (maximo, semilla)


def _muestreo_configurado(feature: str) -> tuple[int | None, int | None, str | None]:
    """Tope, semilla y NIVEL de rigor de una feature, según `harness/rigor.json`.

    Devuelve también el nivel porque el informe tiene que declarar quién fijó la
    semilla (R9), y resolverlo aparte obligaría a releer los mismos dos ficheros.

    Ante cualquier configuración ausente o ilegible devuelve `(None, None,
    None)` —campaña completa, como hasta hoy—, igual que hacen
    `_timeout_configurado` y `_workers_configurados`: el arnés se degrada, no se
    cae, en un repositorio con configuración anterior.
    """
    try:
        rigor = cargar_rigor(RUTA_RIGOR)
        ficha = next(
            (f for f in cargar_features() if f.get("id") == feature), {}
        )
        nivel = nivel_de_feature(ficha, rigor)
        return (max_mutantes_nivel(nivel, rigor), semilla_nivel(nivel, rigor), nivel)
    except ValueError:
        return (None, None, None)


#: Código con el que sale una campaña que NO ha juzgado nada. Distinto del 1
#: de «hay supervivientes» a propósito: aquello es un resultado, esto no.
NADA_JUZGADO = 3


def mensaje_alcance_vacio(alcance: Alcance) -> str:
    """Por qué se aborta cuando el alcance no tiene ni una línea (R16)."""
    return (
        f"ALCANCE VACÍO en {alcance.feature}: ni una línea de producción que "
        f"mutar (origen {alcance.origen}, {alcance.ref_diff[0]}.."
        f"{alcance.ref_diff[1]}). No se ha juzgado NADA.\n"
        "  No se escribe informe: un fichero en progress/ con un cero que nadie "
        "ha medido es peor que no tener fichero, porque se lee como «nada que "
        "arreglar» y el reviewer lo da por bueno.\n"
        "  Causas habituales: la rama ya está mergeada y el diff contra la base "
        "sale vacío (declara el alcance a mano con --ficheros), o todo lo que "
        "cambió es documentación, tests o especificaciones."
    )


def mensaje_sin_mutantes(alcance: Alcance) -> str:
    """Por qué se aborta cuando hay líneas pero no sale ni un mutante (R14)."""
    return (
        f"CERO MUTANTES en {alcance.feature}: el alcance tiene "
        f"{alcance.total_lineas()} línea(s) de producción pero no se ha generado "
        "ni un mutante, así que no se ha juzgado NADA.\n"
        "  No se escribe informe: «0 generados, 0 supervivientes» se lee como "
        "una campaña impecable.\n"
        "  Motivo: esas líneas no llevan código mutable —imports, docstrings, "
        "declaraciones, cadenas— o el fichero no se pudo leer. Amplía el "
        "alcance o aporta la evidencia de otra forma, y dilo por escrito."
    )


def main(argv: list[str] | None = None, ejecutor: object | None = None) -> int:
    """Punto de entrada.

    Códigos: 0 sin supervivientes, 1 con ellos, 2 error de uso, 3 campaña
    abortada (línea base roja, árbol sucio o nada que juzgar). El 3 es distinto
    del 1 a propósito: «hay supervivientes» es un resultado; «no se ha medido
    nada» no.

    Las dos guardas de D4 viven AQUÍ y no en `harness.alcance` a sabiendas. El
    mismo modo de fallo —una campaña de cero que sale en verde— ha entrado ya
    por tres puertas distintas: la invocación de pytest sin ruta, `--ficheros
    ","` y `--feature` sobre una rama ya mergeada. Poner una cuarta guarda en
    el cuarto constructor de alcance es esperar a la quinta; `main` es el
    único punto por el que pasan todas las vías, incluidas
    las que todavía no existen, y además es quien escribe el informe y elige el
    código de salida. La guarda de entrada de `alcance_de_ficheros` NO se
    retira (R17): ésa sabe QUÉ ruta sobra, y ésta solo sabe que no quedó nada.
    """
    opciones = _analizar_argumentos(argv)
    # `is not None` y no `or`: aquí el 0 ya no puede llegar (R23 lo rechaza),
    # pero era justo el `or` lo que hacía que `--timeout 0` cayera en silencio
    # al valor configurado.
    timeout_fijado = opciones.timeout is not None
    timeout_s = opciones.timeout if timeout_fijado else _timeout_configurado()

    if opciones.estado:
        return _modo_estado(opciones.raiz)
    if opciones.restaurar:
        return _modo_restaurar(opciones.raiz)
    if not opciones.feature:
        print("Falta --feature (o usa --estado / --restaurar).", file=sys.stderr)
        return 2

    # Una campaña anterior que murió a machetazos pudo dejar un mutante escrito.
    # Empezar encima de él mediría el mutante viejo, no el código.
    if Centinela.leer(opciones.raiz) is not None:
        print(
            "Hay un centinela de una campaña anterior sin cerrar. Se restaura "
            "antes de empezar:",
            file=sys.stderr,
        )
        # R18: hasta hoy este código de salida se TIRABA. Si la restauración no
        # podía deshacer algo, la campaña arrancaba igual sobre un árbol que ya
        # estaba mutado y todos sus veredictos hablaban de un código que nadie
        # había escrito.
        if _modo_restaurar(opciones.raiz) != 0:
            print(
                "ABORTADA sin empezar: queda al menos un fichero con un MUTANTE "
                "de la campaña anterior escrito en disco (los nombra la línea "
                "«NO SE PUDO RESTAURAR» de aquí arriba).\n"
                "  Medir encima de un mutante viejo es medir el mutante: cada "
                "veredicto de esta campaña hablaría de un código que nadie ha "
                "escrito, y el informe no lo diría.\n"
                "  Deshazlo a mano —`git checkout -- <fichero>`— y vuelve a "
                "lanzarla.",
                file=sys.stderr,
            )
            return NADA_JUZGADO

    try:
        servicios = cargar_servicios(raiz=opciones.raiz)
    except ValueError as error:
        print(str(error), file=sys.stderr)
        return 2

    try:
        if opciones.ficheros:
            alcance = alcance_de_ficheros(
                opciones.ficheros.split(","), opciones.feature, raiz=opciones.raiz
            )
        else:
            alcance = alcance_de_feature(
                opciones.feature,
                base=opciones.base,
                rama=opciones.rama,
                raiz=opciones.raiz,
            )
    except SystemExit as parada:
        print(str(parada), file=sys.stderr)
        return 2

    print(alcance.descripcion())
    # R16: aquí, ANTES de crear worktrees o correr ninguna línea base. Hasta hoy
    # esto solo imprimía «Sin líneas de producción» y SEGUÍA, y seguir es lo que
    # producía el informe de cero mutantes que se lee como «todo bien».
    if not alcance.total_lineas():
        print(mensaje_alcance_vacio(alcance), file=sys.stderr)
        return NADA_JUZGADO

    def factoria(fichero: str) -> object:
        return ejecutor_para(fichero, servicios, opciones.raiz)

    nivel_max, nivel_semilla, nivel = _muestreo_configurado(opciones.feature)
    max_mutantes, semilla = resolver_muestreo(
        opciones.max_mutantes, opciones.semilla, nivel_max, nivel_semilla
    )
    if max_mutantes is not None:
        print(
            f"Muestreo: hasta {max_mutantes} mutantes, semilla {semilla} "
            f"(nivel {nivel or 'no resuelto'})."
        )

    workers = resolver_workers(opciones.workers, _workers_configurados())
    timeout_base_s = timeout_de_linea_base(timeout_s)
    if timeout_fijado:
        # R6: quien fija el timeout a mano tiene que saber que ha desactivado la
        # derivación, o leerá el informe creyendo que el número está medido.
        print(
            f"Timeout FIJADO a mano: {timeout_s} s por mutante. El cálculo "
            "automático a partir de la línea base queda ANULADO."
        )
    else:
        print(
            f"Timeout por mutante: se derivará de la línea base medida, con "
            f"suelo {timeout_s} s y margen {MARGEN_TIMEOUT}. La propia línea "
            f"base dispone de {timeout_base_s} s."
        )
    paralela = workers >= 2 and ejecutor is None
    centinela = Centinela(
        raiz=opciones.raiz,
        feature=opciones.feature,
        modo="paralelo" if paralela else "serie",
    )
    centinela.abrir()

    try:
        if paralela:
            # Import perezoso: `harness.mutacion_paralela` se apoya en este
            # módulo, y al revés solo aquí. Sin ciclo que gestionar.
            from harness.mutacion_paralela import ejecutar_campania_paralela

            print(f"Campaña paralela: hasta {workers} workers, uno por worktree.")
            informe = ejecutar_campania_paralela(
                alcance,
                servicios,
                timeout_s=timeout_s,
                raiz=opciones.raiz,
                workers=workers,
                max_mutantes=max_mutantes,
                semilla=semilla,
                eco=lambda linea: print(linea, flush=True),
                centinela=centinela,
                timeout_base_s=timeout_base_s,
                timeout_fijado=timeout_fijado,
            )
        else:
            informe = ejecutar_campania(
                alcance,
                ejecutor or EjecutorPytest(raiz=opciones.raiz),
                timeout_s=timeout_s,
                raiz=opciones.raiz,
                max_mutantes=max_mutantes,
                semilla=semilla,
                eco=lambda linea: print(linea, flush=True),
                ejecutor_de=factoria if servicios and ejecutor is None else None,
                centinela=centinela,
                timeout_base_s=timeout_base_s,
                timeout_fijado=timeout_fijado,
                workers=workers,
            )
    except CampaniaAbortada as error:
        # Sin informe a propósito: un fichero en `progress/` con un cero que
        # nadie ha medido es peor que no tener fichero.
        print(str(error), file=sys.stderr)
        return 3
    except ValueError as error:
        print(str(error), file=sys.stderr)
        return 2
    finally:
        centinela.cerrar()

    # R14: red final. Puede haber líneas en el alcance y aun así ni un mutante
    # (imports, docstrings, declaraciones). Ese informe no dice nada de nada.
    if informe.generados == 0:
        print(mensaje_sin_mutantes(alcance), file=sys.stderr)
        return NADA_JUZGADO

    destino = Path(opciones.salida or f"progress/mutacion_{opciones.feature}.md")
    # Quién fijó el muestreo lo sabe el CLI, no la campaña: el informe tiene que
    # decirlo para que se pueda repetir con los mismos mutantes (R9).
    informe.nivel = nivel
    # Contra qué base se midió también lo sabe el CLI: con `--ficheros` no hay
    # base que valga, y sin ella el comando de la cabecera mide otro alcance.
    informe.base = None if opciones.ficheros else opciones.base
    escribir_informe(informe, destino)
    print(
        f"{informe.evaluados} mutantes evaluados, {informe.muertos} muertos, "
        f"{len(informe.supervivientes)} supervivientes, "
        f"{len(informe.timeouts)} timeouts, {len(informe.base_rota)} sin "
        f"veredicto en {informe.segundos:.1f} s"
    )
    print(f"Informe: {destino.as_posix()}")
    if not informe.fiable:
        print(f"CAMPAÑA NO VÁLIDA: {informe.aviso_base}", file=sys.stderr)
        return 3
    return 1 if informe.supervivientes else 0


if __name__ == "__main__":  # pragma: no cover
    # Se llama al `main` del módulo IMPORTADO, no al de esta copia. Con
    # `python -m harness.mutacion` este fichero se ejecuta como `__main__`, y
    # `harness.mutacion_paralela` lo vuelve a importar como `harness.mutacion`:
    # dos copias del módulo y, por tanto, dos clases `BaseRota` distintas. Un
    # `except CampaniaAbortada` escrito aquí NO cazaría la que lanza la campaña
    # paralela, y el aborto saldría por pantalla como un traceback pelado.
    # Delegando en el módulo importado, todo el mundo usa las mismas clases.
    from harness.mutacion import main as _main

    raise SystemExit(_main())
