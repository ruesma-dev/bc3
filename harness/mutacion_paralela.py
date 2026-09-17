# harness/mutacion_paralela.py
"""Campaña de mutación repartida entre varios workers, cada uno en su worktree.

La campaña en serie de `harness.mutacion` ya hace bien lo difícil: aplica un
mutante, lanza la suite del servicio dueño del fichero y restaura con
`try/finally` más una red de seguridad final. Aquí NO se reescribe nada de eso:
el paralelismo se monta por fuera.

1. El coordinador calcula los mutantes UNA vez desde el árbol principal y
   aplica el muestreo UNA vez, antes de repartir.
2. Crea N `git worktree` desechables desde `HEAD`, en el temp del sistema.
3. Lanza N hilos; cada uno llama a `ejecutar_campania` tal cual, con
   `raiz=<su worktree>` y `mutantes=<su partición>`. El trabajo pesado (pytest)
   va en subprocesos, así que el GIL no pinta nada.
4. Fusiona los parciales en un informe idéntico al de la campaña en serie
   salvo la fecha y la fila «Tiempo total».
5. Repasa EN SERIE, sobre uno solo de los worktrees, los mutantes que quedaron
   en `timeout`, y sustituye ese no-veredicto por el de verdad.
6. `finally`: retira los worktrees pase lo que pase.

Sobre el paso 5, que es contraintuitivo hasta que se mide: con N suites
compitiendo, el reloj de un mutante mide la carga de la máquina, no al mutante.
Medido el 2026-09-02 sobre una suite de 38,7 s, con 16 workers cada pase subía a
131,6 s contra un tope de 120 s; tres campañas de la misma feature y el mismo
commit dieron 15, 27 y 0 timeouts, sobre mutantes distintos cada vez, y algún
mutante «muerto» en una salía `timeout` en la siguiente. Un `timeout` de una
campaña concurrente no es un veredicto: es un reintento pendiente.

El árbol principal no se muta NUNCA en modo paralelo, y por eso se exige que
esté limpio: los worktrees se crean desde `HEAD` y con cambios sin commitear
evaluarían un código distinto del que se ve en disco.

Riesgos propios del modo paralelo, y qué los detecta ahora (1.5.3). Los tres
tienen el mismo síntoma —la suite del worker arranca ROJA sin mutar nada, y
entonces TODO mutante sale «muerto»—, así que los caza la misma comprobación:
la **línea base** que `ejecutar_campania` corre dentro de cada worktree antes
de juzgar a nadie. Si no está verde, la campaña aborta con `BaseRota` en vez de
publicar un cero de supervivientes que nadie ha medido.

- **Instalación editable apuntando al árbol principal.** Si el venv de un
  servicio instala su paquete en modo editable contra el árbol principal, la
  suite del worker importaría el código SIN mutar. Los venvs no se copian al
  worktree: solo se reutiliza su intérprete. DETECTADO, no resuelto.
- **Suites que dependan de ficheros no versionados** (`.env`, datos locales):
  no existen dentro de un worktree. El caso más común —un `.env` con la
  configuración sin la que la suite ni arranca— queda RESUELTO desde la 1.7.7:
  el coordinador vuelca sus variables al entorno del proceso antes de crear
  ningún worktree y los workers las heredan (ver `volcar_variables`). El
  fichero NO se copia a ninguna parte. Cualquier OTRO fichero no versionado
  que la suite necesite —datos locales, certificados, un fixture generado—
  sigue sin existir en el worktree, y ahí solo hay detección.
- **Detached HEAD**: el worktree no está en ninguna rama, así que un test que
  lea `git branch --show-current` recibe cadena vacía. DETECTADO, no resuelto.

Esto no es teoría: hasta la 1.5.2 estos tres casos estaban escritos aquí como
«limitaciones conocidas» y NADA los comprobaba. El 2026-08-19, la misma
feature y el mismo árbol dieron `108 generados, 108 muertos, 0
supervivientes` en paralelo y `108, 106, 2 supervivientes` con `--workers 1`.
El cero era falso: dos `bold=True -> bold=False` que ningún test menciona.

Todo con biblioteca estándar, como el resto del arnés.
"""

from __future__ import annotations

import os
import random
import shutil
import subprocess
import tempfile
import threading
import time
from collections.abc import Callable, Iterator
from dataclasses import replace
from pathlib import Path
from types import TracebackType

from harness.alcance import Alcance
from harness.mutacion import (
    MARCA_LINEA_BASE,
    TIMEOUT_POR_DEFECTO,
    BaseRota,
    Centinela,
    EjecutorPytest,
    InformeMutacion,
    Mutante,
    ejecutar_campania,
    ejecutor_para,
    generar_mutantes,
    restauracion_ante_senales,
    restaurar_desde_centinela,
)
from harness.mutacion import (
    # privado de `harness.mutacion` a sabiendas: la campaña paralela tiene que
    # leer los fuentes EXACTAMENTE igual que la campaña en serie (sin traducir
    # saltos de línea) o los mutantes generados no serían los mismos.
    _leer as _leer_fuente,
)
from harness.servicios import Servicio, interprete, servicio_de_ruta

#: Fábrica de ejecutores: `(fichero, raíz del worker) -> ejecutor`.
Fabrica = Callable[[str, str], object]

#: Clave con la que la campaña en serie ordena sus mutantes. El informe
#: paralelo tiene que salir en ESTE orden para ser indistinguible del suyo.
Clave = tuple[str, int, int, str]


def clave_estable(mutante: Mutante) -> Clave:
    """Identidad ordenable de un mutante: `(fichero, línea, columna, operador)`."""
    return (mutante.fichero, mutante.linea, mutante.col, mutante.operador)


# --- Reparto y fusión (funciones puras) -------------------------------------


def repartir(mutantes: list[Mutante], n: int) -> list[list[Mutante]]:
    """Reparte los mutantes entre `n` workers en round-robin por índice.

    Determinista: las mismas entradas dan siempre las mismas particiones, y su
    unión es exactamente la lista recibida, sin repetidos ni omitidos. El
    round-robin sobre la lista ya ordenada equilibra el coste: mutantes del
    mismo fichero —cuya suite tarda lo mismo— se reparten entre todos.
    """
    cuantos = max(1, n)
    return [mutantes[indice::cuantos] for indice in range(cuantos)]


def fusionar(
    alcance: Alcance,
    parciales: list[InformeMutacion],
    generados: int,
    segundos: float,
    muestreado: bool = False,
    max_mutantes: int | None = None,
    semilla: int | None = None,
    workers: int | None = None,
) -> InformeMutacion:
    """Funde los informes de los workers en el informe único de la campaña.

    Los totales se suman y las listas se reordenan por la clave estable, de
    forma que el resultado no delata en qué worker cayó cada mutante. Los
    metadatos de muestreo son los del coordinador, que es quien muestreó.

    El timeout efectivo del conjunto es el MÁXIMO de los parciales: cada worker
    lo deriva de la línea base de SU worktree, y el informe tiene que declarar
    el reloj más largo que se concedió, que es el que explica el peor caso. La
    media de tres relojes distintos no es ningún reloj.
    """
    informe = InformeMutacion(
        feature=alcance.feature,
        alcance=alcance,
        generados=generados,
        muertos=sum(parcial.muertos for parcial in parciales),
        segundos=segundos,
        muestreado=muestreado,
        max_mutantes=max_mutantes,
        semilla=semilla,
    )
    for atributo in ("supervivientes", "timeouts", "mutantes_evaluados", "base_rota"):
        juntos: list[Mutante] = []
        for parcial in parciales:
            juntos.extend(getattr(parcial, atributo))
        setattr(informe, atributo, sorted(juntos, key=clave_estable))
    # Todos los workers nacen del MISMO `HEAD`, así que el primero que lo sepa
    # habla por todos; y cada uno midió su propia línea base, en su worktree,
    # así que sus tiempos se juntan en vez de pisarse (R10–R12). Sin parciales
    # —campaña sin nada que evaluar— no hay dato, y el informe imprime `n/d`.
    informe.sha_head = next(
        (parcial.sha_head for parcial in parciales if parcial.sha_head), None
    )
    for parcial in parciales:
        informe.segundos_linea_base.update(parcial.segundos_linea_base)
    informe.workers = workers
    efectivos = [
        parcial.timeout_efectivo
        for parcial in parciales
        if parcial.timeout_efectivo is not None
    ]
    informe.timeout_efectivo = max(efectivos) if efectivos else None
    suelos = [
        parcial.timeout_suelo for parcial in parciales if parcial.timeout_suelo is not None
    ]
    informe.timeout_suelo = max(suelos) if suelos else None
    informe.timeout_fijado = any(parcial.timeout_fijado for parcial in parciales)
    # Basta con que UN worker haya perdido la base para que la campaña entera
    # deje de valer: su partición no está medida y el total no cuadra.
    avisos = [parcial.aviso_base for parcial in parciales if parcial.aviso_base]
    if avisos:
        informe.aviso_base = avisos[0]
    return informe


def reemplazar_timeouts(
    informe: InformeMutacion, reintento: InformeMutacion
) -> InformeMutacion:
    """Reparte los mutantes repasados en serie según su veredicto de verdad.

    `informe` es el de la campaña paralela; `reintento`, el que devuelve volver a
    evaluar SIN concurrencia los mutantes que quedaron en `timeout`. Se cambia
    únicamente **cómo se reparten**: `generados` y `mutantes_evaluados` no se
    tocan —nadie se ha evaluado dos veces ni ha desaparecido nadie— y las listas
    se reordenan con `clave_estable`, igual que hace `fusionar`, para que el
    informe no delate en qué orden se repasó.

    Los segundos se suman: el repaso cuesta minutos reales y esconderlos falsea
    la media por mutante, que es justo la señal con la que `CHECKPOINTS.md`
    detecta una campaña que no midió lo que dice.

    Función pura: no toca disco, ni git, ni el reloj.
    """
    if not informe.timeouts:
        return informe

    corregido = replace(
        informe,
        muertos=informe.muertos + reintento.muertos,
        supervivientes=sorted(
            [*informe.supervivientes, *reintento.supervivientes], key=clave_estable
        ),
        timeouts=sorted(reintento.timeouts, key=clave_estable),
        mutantes_evaluados=list(informe.mutantes_evaluados),
        segundos=informe.segundos + reintento.segundos,
        timeouts_repasados=len(informe.timeouts),
    )
    return corregido


# --- Worktrees desechables ---------------------------------------------------


def _git(raiz: str, *args: str) -> tuple[int, str]:
    """Ejecuta git en `raiz` y devuelve `(código de salida, salida completa)`.

    No se usa `harness.alcance.ejecutar_git` a propósito: aquel devuelve cadena
    vacía cuando git falla, y aquí la diferencia entre «no hay cambios» y «git
    ha fallado» decide si se aborta la campaña.
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


#: Rutas que NO cuentan para decidir si el árbol está limpio: son estado del
#: propio arnés (caché de suites, centinela de la campaña en curso), no trabajo.
#: El bloque gestionado del `.gitignore` ya las ignora, pero un repositorio
#: puede llevar un `.gitignore` viejo y entonces la campaña se negaría a
#: arrancar por culpa del fichero que ella misma acaba de escribir.
RUTAS_DEL_ARNES: tuple[str, ...] = (".arnes_cache/",)


def arbol_limpio(raiz: str = ".") -> bool:
    """¿El árbol de trabajo está sin cambios pendientes de commitear?

    Un git que falla —no es un repositorio, no está instalado— cuenta como NO
    limpio: la campaña paralela se apoya en `HEAD`, y sin poder comprobarlo lo
    prudente es abortar.
    """
    codigo, salida = _git(raiz, "status", "--porcelain")
    if codigo != 0:
        return False
    for linea in salida.splitlines():
        ruta = linea[3:].strip().strip('"') if len(linea) > 3 else ""
        if ruta and not ruta.startswith(RUTAS_DEL_ARNES):
            return False
    return True


class Worktrees:
    """Crea N worktrees desechables desde `HEAD` y garantiza su retirada.

    Se usa como gestor de contexto: `__enter__` devuelve las rutas y `__exit__`
    las retira pase lo que pase (fin normal, excepción o `KeyboardInterrupt`).
    Viven en el temp del sistema, nunca bajo el repositorio: dentro saldrían en
    `git status`, en la recolección de pytest y en el radar del portero, y el
    peor caso imaginable —el proceso matado a machetazos— dejaría basura dentro
    del árbol de trabajo.
    """

    def __init__(self, raiz: str, cuantos: int, etiqueta: str = "mutacion") -> None:
        self.raiz = str(raiz)
        self.cuantos = max(0, cuantos)
        self.etiqueta = etiqueta
        self.rutas: list[str] = []
        self._temporal: str | None = None

    def __enter__(self) -> list[str]:
        # Retira primero los registros huérfanos que dejó una campaña muerta:
        # si no, se acumulan campaña tras campaña en `git worktree list`.
        _git(self.raiz, "worktree", "prune")
        self._temporal = tempfile.mkdtemp(prefix=f"mutacion_{self.etiqueta}_")
        try:
            for indice in range(self.cuantos):
                destino = Path(self._temporal) / f"wk_{indice}"
                codigo, salida = _git(
                    self.raiz, "worktree", "add", "--detach", str(destino), "HEAD"
                )
                if codigo != 0:
                    raise RuntimeError(
                        f"No se pudo crear el worktree {destino.as_posix()}: "
                        f"{salida.strip()}"
                    )
                self.rutas.append(str(destino))
        except BaseException:
            self._retirar()
            raise
        return self.rutas

    def __exit__(
        self,
        tipo: type[BaseException] | None,
        valor: BaseException | None,
        traza: TracebackType | None,
    ) -> bool:
        self._retirar()
        return False  # nunca traga la excepción: solo limpia

    def _retirar(self) -> None:
        """Borra los worktrees creados; lo que no se deje borrar, se desregistra."""
        for ruta in self.rutas:
            codigo, _ = _git(self.raiz, "worktree", "remove", "--force", ruta)
            if codigo != 0:
                # Windows: un proceso rezagado puede tener un fichero abierto.
                # Se borra el directorio y se desregistra después, en ese orden:
                # `prune` solo retira el registro de un worktree que ya no está.
                shutil.rmtree(ruta, ignore_errors=True)
                _git(self.raiz, "worktree", "prune")
        self.rutas = []
        if self._temporal is not None:
            shutil.rmtree(self._temporal, ignore_errors=True)
            self._temporal = None


# --- El `.env` del árbol principal, al entorno del proceso -------------------

#: Fichero de variables de entorno que el coordinador vuelca antes de crear los
#: worktrees. Es el nombre de facto en cualquier proyecto que use un fichero de
#: configuración local no versionado.
FICHERO_ENTORNO = ".env"


def parsear_variables(texto: str) -> dict[str, str]:
    """Convierte un texto `CLAVE=valor` en un diccionario de variables.

    Parseo deliberadamente mínimo y con biblioteca estándar, como el resto del
    arnés: no se añade una dependencia para leer un fichero de dos columnas.
    Lo que entiende, y nada más:

    - líneas en blanco y comentarios de línea entera (`#`): se ignoran;
    - `export CLAVE=valor`: el prefijo `export ` se descarta;
    - espacios alrededor de la clave, del `=` y del valor: se recortan;
    - un valor entero entre comillas simples o dobles: se le quitan.

    Lo que NO hace, a propósito: quitar comentarios al final de una línea con
    valor (un `#` puede ser parte de una contraseña perfectamente válida) ni
    expandir `$OTRA_VARIABLE`. Una línea sin `=`, o con una clave vacía o con
    espacios dentro, no es una asignación y se salta en silencio: el fichero es
    del proyecto y esta función no está para validarlo, sino para no reventar.
    """
    variables: dict[str, str] = {}
    for cruda in texto.splitlines():
        linea = cruda.strip()
        if not linea or linea.startswith("#"):
            continue
        if linea.startswith("export "):
            linea = linea[len("export ") :].lstrip()
        clave, separador, valor = linea.partition("=")
        clave = clave.strip()
        if not separador or not clave or any(letra.isspace() for letra in clave):
            continue
        valor = valor.strip()
        if len(valor) >= 2 and valor[0] == valor[-1] and valor[0] in ("'", '"'):
            valor = valor[1:-1]
        variables[clave] = valor
    return variables


def volcar_variables(raiz: str = ".", fichero: str = FICHERO_ENTORNO) -> list[str]:
    """Vuelca al proceso las variables del fichero de entorno del árbol principal.

    Devuelve las claves realmente añadidas, en el orden del fichero. Sin fichero
    —o si no se puede leer— devuelve la lista vacía y no pasa nada más: la
    campaña sigue exactamente como sin esta función.

    POR QUÉ existe. Un worktree solo contiene lo versionado, y un fichero de
    entorno local no lo está. En un proyecto cuya configuración se lee de ahí,
    la suite del worker ni arranca: la validación de la configuración revienta y
    la campaña aborta con `BaseRota` sin haber juzgado un solo mutante. El modo
    paralelo quedaba inservible justo donde más se nota (una campaña de horas).

    POR QUÉ ASÍ y no copiando el fichero. Copiarlo escribiría la configuración
    local —credenciales incluidas— en el temp del sistema, fuera del repositorio
    y de su `.gitignore`, y una campaña muerta a machetazos la dejaría ahí. Las
    variables viven en la memoria de este proceso y de los subprocesos que
    lanza; no se escriben en ningún disco. Los workers las heredan solos: la
    suite de cada mutante se lanza con `env={**os.environ, ...}`.

    PRECEDENCIA: lo que ya esté en el entorno MANDA y no se pisa. Es lo que
    hacen las bibliotecas de configuración al leer un fichero así, y lo que
    espera quien exporta una variable a mano para una tanda concreta.
    """
    try:
        texto = (Path(raiz) / fichero).read_text(encoding="utf-8-sig", errors="replace")
    except OSError:
        return []

    anadidas: list[str] = []
    for clave, valor in parsear_variables(texto).items():
        if clave in os.environ:
            continue
        os.environ[clave] = valor
        anadidas.append(clave)
    return anadidas


# --- Piezas del coordinador --------------------------------------------------


def fabrica_de_ejecutores(servicios: list[Servicio], raiz_venvs: str) -> Fabrica:
    """Fábrica que juzga cada fichero con la suite de SU servicio en el worker.

    La suite se ejecuta DENTRO del worktree del worker (ahí está el código
    mutado) y con el intérprete resuelto contra el árbol principal (ahí están
    los venvs, que no se versionan y por tanto no existen en un worktree).
    """

    def fabrica(fichero: str, raiz_worker: str) -> object:
        return ejecutor_para(
            fichero, servicios, raiz=raiz_worker, raiz_venvs=raiz_venvs
        )

    return fabrica


def resolver_interpretes(
    alcance: Alcance, servicios: list[Servicio], raiz: str = "."
) -> dict[str, str]:
    """Resuelve por adelantado el intérprete de cada servicio del alcance.

    Se hace ANTES de crear ningún worktree: un venv declarado que no existe es
    un `ValueError` de `harness.servicios`, y descubrirlo con dieciséis
    checkouts ya en el temp del sistema sería tirar minutos a la basura.
    """
    resueltos: dict[str, str] = {}
    for fichero in alcance.ficheros():
        servicio = servicio_de_ruta(fichero, servicios)
        if servicio is None or servicio.lenguaje != "python":
            continue
        if servicio.nombre not in resueltos:
            resueltos[servicio.nombre] = interprete(servicio, raiz)
    return resueltos


def generar_y_muestrear(
    alcance: Alcance,
    raiz: str = ".",
    max_mutantes: int | None = None,
    semilla: int | None = None,
) -> tuple[list[Mutante], int, bool]:
    """Genera los mutantes del alcance y aplica el muestreo UNA sola vez.

    Devuelve `(mutantes a evaluar, generados, muestreado)`. Reproduce paso por
    paso lo que hace `ejecutar_campania` en serie —mismo orden de ficheros,
    misma lectura sin traducir saltos de línea, mismo `random.Random(semilla)`
    sobre la misma lista— porque de esa igualdad depende que el muestreo elija
    exactamente los mismos mutantes que la campaña en serie.
    """
    base = Path(raiz)
    mutantes: list[Mutante] = []
    for fichero in alcance.ficheros():
        ruta = base / fichero
        if ruta.is_file():
            mutantes.extend(
                generar_mutantes(_leer_fuente(ruta), alcance.lineas[fichero], fichero)
            )

    generados = len(mutantes)
    if max_mutantes is not None and generados > max_mutantes:
        sorteo = random.Random(semilla)
        return (
            sorted(sorteo.sample(mutantes, max_mutantes), key=clave_estable),
            generados,
            True,
        )
    return (mutantes, generados, False)


def renumerar(linea: str, indice: int, total: int) -> str:
    """Cambia el `[i/n]` que numera un worker por el `[i/n]` de la campaña.

    Cada worker numera su propia partición; por pantalla lo que interesa es
    cuánto queda de campaña. El orden de las líneas NO es contractual: el
    informe sí.
    """
    resto = linea.split("] ", 1)[1] if linea.startswith("[") and "] " in linea else linea
    return f"[{indice}/{total}] {resto}"


def eco_del_repaso(
    eco: Callable[[str], None] | None, total: int
) -> Callable[[str], None] | None:
    """Eco del repaso en serie: numeración propia y marca `repaso` delante.

    La numeración de la campaña ya se cerró cuando el repaso empieza, así que
    seguir contando sobre ella daría un `[i/n]` fuera de rango. El repaso cuenta
    lo suyo, y la marca deja ver por pantalla que eso ya no es la campaña. Las
    líneas de la línea base pasan tal cual, como en `eco_compartido`: no son
    mutantes y numerarlas descuadraría el `[i/n]`.
    """
    if eco is None:
        return None
    hechos = 0

    def _eco(linea: str) -> None:
        nonlocal hechos
        if linea.startswith(MARCA_LINEA_BASE):
            eco(f"repaso {linea}")
            return
        hechos += 1
        eco(f"repaso {renumerar(linea, hechos, total)}")

    return _eco


class _ParticionCancelable:
    """Partición que deja de rendir mutantes en cuanto se pide cancelar.

    Se pasa tal cual como `mutantes=` a `ejecutar_campania`, que solo le pide
    `len()` e iteración. Así la cancelación cooperativa no cuesta ni una línea
    dentro de la campaña en serie, que es la parte delicada del módulo.
    """

    def __init__(self, mutantes: list[Mutante], evento: threading.Event) -> None:
        self._mutantes = mutantes
        self._evento = evento

    def __len__(self) -> int:
        return len(self._mutantes)

    def __iter__(self) -> Iterator[Mutante]:
        for mutante in self._mutantes:
            if self._evento.is_set():
                return
            yield mutante


# --- Coordinador -------------------------------------------------------------


def ejecutar_campania_paralela(
    alcance: Alcance,
    servicios: list[Servicio],
    timeout_s: int = TIMEOUT_POR_DEFECTO,
    raiz: str = ".",
    workers: int = 2,
    max_mutantes: int | None = None,
    semilla: int | None = None,
    eco: Callable[[str], None] | None = None,
    fabrica: Fabrica | None = None,
    centinela: Centinela | None = None,
    timeout_base_s: int | None = None,
    timeout_fijado: bool = False,
) -> InformeMutacion:
    """Evalúa los mutantes del alcance repartidos entre varios worktrees.

    Devuelve el mismo `InformeMutacion` que produciría la campaña en serie
    sobre el mismo commit: mismos totales y mismas listas en el mismo orden.
    Solo cambian el reloj y en qué worker cayó cada mutante, que no se cuenta.

    Lanza `ValueError` si el árbol principal tiene cambios sin commitear o si
    un servicio del alcance declara un venv sin intérprete. En ambos casos, sin
    haber creado ningún worktree ni tocado ningún fichero. Y `BaseRota` si la
    suite de algún worktree no está verde SIN mutar nada, que es como se
    manifiestan las tres trampas de la cabecera de este módulo.
    """
    inicio = time.monotonic()
    resolver_interpretes(alcance, servicios, raiz)  # R11: revienta aquí o nunca
    # ANTES de crear ningún worktree: lo que un worktree no tiene es lo no
    # versionado, y el fichero de entorno del árbol principal es justo eso. Sus
    # variables pasan a este proceso y los workers las heredan; el fichero no se
    # copia a ninguna parte. Ver `volcar_variables` para el porqué completo.
    volcar_variables(raiz)

    mutantes, generados, muestreado = generar_y_muestrear(
        alcance, raiz, max_mutantes, semilla
    )
    fabrica = fabrica or fabrica_de_ejecutores(servicios, raiz_venvs=raiz)
    efectivo = max(1, min(workers, len(mutantes)))
    total = len(mutantes)

    cerrojo = threading.Lock()
    hechos = 0

    def eco_compartido(linea: str) -> None:
        nonlocal hechos
        if eco is None:
            return
        if linea.startswith(MARCA_LINEA_BASE):
            # La línea base no es un mutante: contarla en el `[i/n]` haría que
            # el progreso pasara de n antes de empezar (`[9/5]`, visto de
            # verdad) y que el usuario dudase de todo lo demás.
            eco(linea)
            return
        with cerrojo:
            hechos += 1
            indice = hechos
        eco(renumerar(linea, indice, total))

    def correr(
        raiz_worker: str,
        particion: object,
        eco_propio: Callable[[str], None] | None = None,
        workers_declarados: int | None = None,
    ) -> InformeMutacion:
        return ejecutar_campania(
            alcance,
            EjecutorPytest(raiz=raiz_worker),
            timeout_s=timeout_s,
            raiz=raiz_worker,
            mutantes=particion,  # type: ignore[arg-type]
            eco=eco_propio or (eco_compartido if eco is not None else None),
            ejecutor_de=lambda fichero: fabrica(fichero, raiz_worker),
            centinela=centinela,
            # Cada worker deriva SU timeout de la línea base de SU worktree: es
            # ahí donde se nota la contención de los W workers, y es lo que hace
            # que el reloj se adapte a la máquina sin fórmula que lo adivine.
            timeout_base_s=timeout_base_s,
            timeout_fijado=timeout_fijado,
            workers=workers_declarados or efectivo,
            # El worktree acaba de nacer de HEAD: comprobar que está limpio
            # sería preguntarle a git lo que git acaba de hacer. Lo que sí se
            # comprueba, y aquí es donde importa, es su LÍNEA BASE.
            comprobar_arbol=False,
        )

    def informe_final(parciales: list[InformeMutacion]) -> InformeMutacion:
        return fusionar(
            alcance,
            parciales,
            generados=generados,
            segundos=time.monotonic() - inicio,
            muestreado=muestreado,
            max_mutantes=max_mutantes,
            semilla=semilla,
            workers=efectivo,
        )

    def repasar(informe: InformeMutacion, raiz_repaso: str) -> InformeMutacion:
        """Vuelve a juzgar EN SERIE los mutantes que quedaron en `timeout`.

        Con N suites peleándose por la máquina, el reloj de un mutante mide la
        contención y no al mutante: medido el 2026-09-02, una suite de 38,7 s
        pasaba a 131,6 s con 16 workers contra un tope de 120 s, y tres campañas
        de la misma feature y el mismo commit dieron 15, 27 y 0 timeouts sobre
        mutantes distintos. Aquí se juzga uno detrás de otro, sobre UN solo
        worktree —el árbol principal no se muta nunca en modo paralelo, y esa
        garantía no se rompe ni para esto—, así que lo que siga en `timeout`
        después ya sí señala un cuelgue de verdad.

        El repaso mide su propia línea base, que es lo que se busca: sin
        contención sale un timeout derivado más ajustado y honesto. Si esa línea
        base saliera rota, la campaña YA HECHA no se tira por la borda: se
        devuelve tal cual, con sus timeouts sin aclarar y su aviso.
        """
        if not informe.timeouts:
            return informe

        pendientes = list(informe.timeouts)
        if eco is not None:
            eco(
                f"Repaso en serie de {len(pendientes)} mutante(s) en timeout: "
                "sin concurrencia, el reloj mide al mutante y no a la máquina."
            )
        try:
            reintento = correr(
                raiz_repaso,
                pendientes,
                eco_propio=eco_del_repaso(eco, len(pendientes)),
                workers_declarados=1,
            )
        except BaseRota:
            if eco is not None:
                eco(
                    "Repaso abortado: la línea base del worktree del repaso no "
                    "está verde. La campaña se entrega tal cual, con sus "
                    "timeouts sin aclarar."
                )
            return informe
        return reemplazar_timeouts(informe, reintento)

    # R8: con menos de dos mutantes que evaluar, paralelizar solo cuesta. Se
    # muta in situ, como toda la vida, y no se crea ni un worktree. Como aquí sí
    # se escribe en el árbol principal, la guardia de árbol limpio vuelve a
    # aplicar: es exactamente el caso que puede dejarte un mutante en el diff.
    #
    # Aquí NO se repasan los timeouts, y es a propósito: sin concurrencia, el
    # repaso repetiría exactamente la misma medición que acaba de hacerse. Un
    # timeout de una campaña in situ ya es el veredicto que el repaso buscaba, y
    # repetirlo solo duplicaría el coste del único caso en que de verdad hay un
    # cuelgue. Por eso `timeouts_repasados` se queda en cero: nadie repasó nada.
    if efectivo < 2:
        parciales = (
            [
                ejecutar_campania(
                    alcance,
                    EjecutorPytest(raiz=raiz),
                    timeout_s=timeout_s,
                    raiz=raiz,
                    mutantes=mutantes,
                    eco=eco_compartido if eco is not None else None,
                    ejecutor_de=lambda fichero: fabrica(fichero, raiz),
                    centinela=centinela,
                    timeout_base_s=timeout_base_s,
                    timeout_fijado=timeout_fijado,
                    workers=efectivo,
                )
            ]
            if mutantes
            else []
        )
        return informe_final(parciales)

    if not arbol_limpio(raiz):
        raise ValueError(
            "El árbol principal tiene cambios sin commitear y la campaña "
            "paralela crea sus worktrees desde HEAD: evaluaría un código "
            "distinto del que ves en disco. Commitea los cambios o lanza la "
            "campaña con --workers 1."
        )

    particiones = repartir(mutantes, efectivo)
    resultados: list[InformeMutacion | None] = [None] * efectivo
    fallos: list[BaseException] = []
    evento = threading.Event()

    def trabajo(indice: int, raiz_worker: str) -> None:
        particion = _ParticionCancelable(particiones[indice], evento)
        try:
            resultados[indice] = correr(raiz_worker, particion)
        except BaseException as error:  # noqa: BLE001  (se relanza en el hilo principal)
            # Un worker reventado cancela a los demás: seguir gastando minutos
            # para dar después un informe incompleto sería lo peor de ambos.
            evento.set()
            with cerrojo:
                fallos.append(error)

    def rescate() -> None:
        """Qué hacer si al coordinador le llega una señal: cortar y restaurar.

        Los workers corren en hilos secundarios y ahí `signal.signal` no se
        puede registrar, así que el único manejador posible es éste. Deshace lo
        que el centinela diga que está aplicado —en cualquier worktree— antes de
        que el proceso muera.
        """
        evento.set()
        if centinela is not None:
            restaurar_desde_centinela(raiz)

    with restauracion_ante_senales(rescate), Worktrees(
        raiz, efectivo, etiqueta=alcance.feature
    ) as rutas:
        hilos = [
            threading.Thread(
                target=trabajo, args=(indice, ruta), name=f"mutacion-{indice}"
            )
            for indice, ruta in enumerate(rutas)
        ]
        for hilo in hilos:
            hilo.start()
        try:
            for hilo in hilos:
                hilo.join()
        except (KeyboardInterrupt, SystemExit):
            # Los workers dejan de coger mutantes; el que tenga una suite en
            # vuelo la termina o agota su timeout. La limpieza la garantiza el
            # `with` de Worktrees, pase lo que pase.
            evento.set()
            for hilo in hilos:
                hilo.join()
            raise

        if fallos:
            raise fallos[0]

        # El repaso va DENTRO del `with`: necesita un worktree vivo, y el árbol
        # principal no se muta nunca en modo paralelo. Salir por aquí retira los
        # worktrees igual que salir por cualquier otro sitio.
        informe = informe_final(
            [parcial for parcial in resultados if parcial is not None]
        )
        return repasar(informe, rutas[0])
