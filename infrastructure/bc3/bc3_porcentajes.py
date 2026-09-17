# infrastructure/bc3/bc3_porcentajes.py
"""F-002 · Pasada previa que convierte los descompuestos porcentuales a UD.

En un BC3 de Presto, un concepto porcentual (`%SUB25`, `%VID`, `%%beneficio…`)
no trae unitario: su importe lo calcula Presto como

    rendimiento × (suma de los importes de las líneas que le PRECEDEN en ese
    mismo `~D`)

El ERP de destino no entiende esa semántica, así que esta pasada materializa el
cálculo: sustituye cada tripleta porcentual por `clon\\1\\1` y emite un `~C`
propio del par (padre, línea porcentual) con unidad `UD`, tipo `3` y precio
igual al importe que Presto habría calculado.

Es una pasada `.bc3` → `.bc3` independiente: corre ANTES de
`bc3_modifier.convert_to_material` y no toca ninguna línea que no sea (a) un
`~D` con porcentuales convertibles, (b) el `~C`/`~T` de un porcentual que deja
de estar referenciado o (c) un `~M` del par convertido.

Detalle del contrato en `specs/F-002-porcentuales-a-ud/`.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from pathlib import Path

from infrastructure.bc3.bc3_modifier import (
    MAX_CODE_LEN,
    _format_d_triplets,
    _shorten_code_unique,
)

logger = logging.getLogger(__name__)

# --- constantes del dominio ------------------------------------------------ #
# Decimales del precio del clon (R6). 4 por defecto: con 2 el desvío medido
# sobre la salida real llega a 165 EUR en Siroco y 143 EUR en laguna, siempre
# al alza —ROUND_HALF_UP tiene sesgo—, y con 6 ya es cero. El mínimo es 2
# porque es lo que traen los `~C` originales (tabla en `design.md`).
DECIMALES_POR_DEFECTO = 4
DECIMALES_MINIMOS = 2
DECIMALES_MAXIMOS = 6
UNIDAD_CLON = "UD"
TIPO_CLON = "3"
MARCA_PORCENTUAL = "%"

# Motivos de las filas excepcionales del informe (R9, R9 ter, R14, R16).
MOTIVO_BASE_RECONSTRUIDA = "base_reconstruida"
MOTIVO_BASE_NO_DESPEJABLE = "base_no_despejable"
MOTIVO_BASE_INDETERMINADA = "base_indeterminada"
MOTIVO_CONCEPTO_CONSERVADO = "concepto_conservado"

# Una tripleta de `~D` tal y como viaja en el fichero: (hijo, factor, rendimiento).
Tripleta = tuple[str, str, str]


# --------------------------------------------------------------------------- #
# Informe (R20)                                                                #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class CasoPorcentual:
    """Fila excepcional del informe: un caso de R9, R14 o R16."""

    padre: str
    codigo: str
    motivo: str
    rendimiento: float
    importe: float


@dataclass
class InformePorcentuales:
    """Resumen de una pasada.

    `descompuestos` cuenta las líneas `~D` con al menos una línea porcentual
    (las examinadas por la pasada), convertidas o no.
    """

    descompuestos: int = 0
    lineas_convertidas: int = 0
    conceptos_eliminados: int = 0
    decimales: int = DECIMALES_POR_DEFECTO
    casos: list[CasoPorcentual] = field(default_factory=list)

    def anota(self, padre: str, codigo: str, motivo: str,
              rendimiento: Decimal | float, importe: Decimal | float) -> None:
        """Añade una fila excepcional. Los cinco datos son obligatorios: una
        fila del informe sin rendimiento ni importe no le sirve a nadie."""
        self.casos.append(
            CasoPorcentual(
                padre=padre,
                codigo=codigo,
                motivo=motivo,
                rendimiento=float(rendimiento),
                importe=float(importe),
            )
        )


# --------------------------------------------------------------------------- #
# Números: siempre Decimal construido desde el TEXTO del BC3                   #
# --------------------------------------------------------------------------- #
def a_decimal(texto: str | Decimal | None) -> Decimal | None:
    """Decimal desde el texto del BC3, o None si no es un número.

    Nunca pasa por `float`: el BC3 trae los números como texto y ahí es donde
    hay que quedarse para no arrastrar el error binario.
    """
    if texto is None:
        return None
    if isinstance(texto, Decimal):
        return texto
    limpio = str(texto).strip().replace(",", ".")
    if not limpio:
        return None
    try:
        return Decimal(limpio)
    except InvalidOperation:
        return None


def decimales_saneados(valor: object) -> int:
    """Decimales admitidos para el precio del clon, o el defecto con aviso.

    R6 bis: fuera de 2..6, o si no es un entero, se usa 4 y se avisa por log.
    Un `.env` trae texto, así que `"6"` vale; `2.5` no, porque un número de
    decimales fraccionario no significa nada.
    """
    try:
        decimales = int(valor)  # type: ignore[arg-type]
        if decimales != valor and str(valor) != str(decimales):
            raise ValueError(valor)
    except (TypeError, ValueError):
        logger.warning(
            "PORCENTUALES_DECIMALES=%r no es un entero: se usan %d decimales",
            valor, DECIMALES_POR_DEFECTO,
        )
        return DECIMALES_POR_DEFECTO
    if not DECIMALES_MINIMOS <= decimales <= DECIMALES_MAXIMOS:
        logger.warning(
            "PORCENTUALES_DECIMALES=%s fuera del rango %d..%d: se usan %d",
            decimales, DECIMALES_MINIMOS, DECIMALES_MAXIMOS,
            DECIMALES_POR_DEFECTO,
        )
        return DECIMALES_POR_DEFECTO
    return decimales


def redondear(valor: Decimal, decimales: int) -> Decimal:
    """Redondeo a `decimales` posiciones con ROUND_HALF_UP (R6).

    `quantize` solo mira el EXPONENTE del patrón, así que lo que importa aquí
    es el `-decimales`, no el coeficiente: se escribe como exponente para que
    se lea igual que se piensa.
    """
    return valor.quantize(Decimal(f"1e-{decimales}"), rounding=ROUND_HALF_UP)


def formatear_precio(valor: Decimal, decimales: int) -> str:
    """Precio tal y como se escribe en un `~C` (R7).

    Notación decimal con punto, sin exponentes y sin ceros de relleno a la
    derecha: un 7,41 exacto sale `7.41` aunque se redondee a 4 decimales. El
    `-0` se escribe `0`: un precio negativo de cero es ruido que algunos ERP
    leen como texto no numérico.
    """
    redondeado = redondear(valor, decimales)
    if redondeado == 0:
        return "0"
    texto = f"{redondeado:f}"
    if "." in texto:
        texto = texto.rstrip("0").rstrip(".")
    return texto


# --------------------------------------------------------------------------- #
# R1 · Detección                                                               #
# --------------------------------------------------------------------------- #
def es_porcentual(codigo: str, unidad: str) -> bool:
    """D1: el código empieza por `%` o el `~C` declara la unidad `%`.

    Las dos formas conviven en `input/`: en Siroco y El Escorial el porcentual
    se delata por el código y la unidad viene vacía; en los bancos de precios
    (`presupuesto.bc3`) la unidad es `%`.
    """
    if (codigo or "").strip().startswith(MARCA_PORCENTUAL):
        return True
    return (unidad or "").strip() == MARCA_PORCENTUAL


# --------------------------------------------------------------------------- #
# R2 · Importes (D2 y D4)                                                      #
# --------------------------------------------------------------------------- #
def importe_linea(precio: Decimal | str | None,
                  factor: str | Decimal,
                  rendimiento: str | Decimal) -> Decimal:
    """D2 · Importe de una línea normal: precio × factor × rendimiento.

    Sin precio conocido el importe es 0; ese `~D` no se convierte (R16).
    """
    p = a_decimal(precio) or Decimal(0)
    f = a_decimal(factor)
    r = a_decimal(rendimiento)
    if f is None or r is None:
        return Decimal(0)
    return p * f * r


def importe_porcentual(factor: str | Decimal,
                       rendimiento: str | Decimal,
                       base: Decimal) -> Decimal:
    """D4 · Importe de una línea porcentual: factor × rendimiento × base.

    El precio del `~C` del porcentual (el porcentaje en tanto por ciento) NO
    entra en la cuenta: el rendimiento del `~D` ya viene en tanto por uno.
    """
    f = a_decimal(factor)
    r = a_decimal(rendimiento)
    if f is None or r is None:
        return Decimal(0)
    return f * r * base


def calcular_importes(triples: Sequence[Tripleta],
                      precios: Mapping[str, Decimal | None],
                      es_pct: Callable[[str], bool],
                      *,
                      redondear_a: int | None = None,
                      base_inicial: Decimal | None = None) -> list[Decimal]:
    """Importe de cada tripleta, recorridas EN EL ORDEN DEL FICHERO (D3).

    Es la única implementación del cálculo: la usan la pasada y los tests.

    - `redondear_a=d` fija cada importe porcentual a `d` decimales y acumula en
      la base el valor YA redondeado (R6), que es el que se podrá releer en el
      fichero de salida. Es el modo con el que se calculan los precios de clon.
    - `redondear_a=None` deja el encadenado exacto: es como se mide la ENTRADA
      al comprobar el invariante de R19.
    - `base_inicial` arranca el acumulado de D3 en ese valor en vez de en 0:
      es la base reconstruida de un `~D` cuyas líneas son todas porcentuales
      (R9), que viaja al fichero como una línea propia `<padre>.P0`.
    """
    importes: list[Decimal] = []
    base = base_inicial if base_inicial is not None else Decimal(0)
    for codigo, factor, rendimiento in triples:
        if es_pct(codigo):
            importe = importe_porcentual(factor, rendimiento, base)
            if redondear_a is not None:
                importe = redondear(importe, redondear_a)
        else:
            importe = importe_linea(precios.get(codigo), factor, rendimiento)
        importes.append(importe)
        base += importe
    return importes


def triples_de_cuerpo(cuerpo: str) -> list[Tripleta]:
    """Parte el cuerpo de un `~D` en tripletas, conservando el texto original."""
    partes = cuerpo.split("\\")
    return [
        (partes[i], partes[i + 1], partes[i + 2])
        for i in range(0, len(partes) - 2, 3)
    ]


# --------------------------------------------------------------------------- #
# Plan de conversión (pasada 1)                                                #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class LineaClon:
    """Clon de una línea porcentual concreta de un `~D` concreto (D5)."""

    indice: int          # posición de la tripleta dentro del ~D
    codigo: str          # código del clon (R5)
    original: str        # código del concepto porcentual sustituido
    precio: Decimal      # importe D4 ya redondeado (R6)
    resumen: str
    fecha: str


@dataclass(frozen=True)
class LineaBase:
    """Línea de base reconstruida de un `~D` solo-porcentual (R9).

    No sustituye a ninguna tripleta de la entrada —se añade delante de las
    porcentuales—, así que no tiene ni índice ni concepto original: solo lo que
    hace falta para escribir su `~C` y su tripleta `codigo\1\1`.
    """

    codigo: str          # <padre>.P0 (R5)
    precio: Decimal      # P − Σ importes porcentuales (R9 bis)
    resumen: str
    fecha: str


@dataclass(frozen=True)
class PlanDescompuesto:
    """Lo que hay que hacerle a UNA línea `~D`.

    `base` solo existe en los `~D` de R9: es la línea `<padre>.P0` con la base
    reconstruida, que va DELANTE de las porcentuales.
    """

    numero_linea: int    # índice de la línea ~D dentro del fichero
    padre: str           # código del padre tal cual viene en el ~D
    clones: list[LineaClon]
    base: LineaBase | None = None


@dataclass
class Plan:
    """Todo lo que la pasada 2 necesita saber, decidido en la pasada 1."""

    planes: dict[int, PlanDescompuesto] = field(default_factory=dict)
    conceptos_a_eliminar: set[str] = field(default_factory=set)
    remapeo_m: dict[tuple[str, str], str] = field(default_factory=dict)
    informe: InformePorcentuales = field(default_factory=InformePorcentuales)


def _leer_lineas(src: Path, encoding: str) -> list[str]:
    """Líneas del BC3 CON su terminador original (R18: nada se reescribe solo)."""
    if not src.exists():
        raise FileNotFoundError(src)
    return src.read_bytes().decode(encoding).splitlines(keepends=True)


def _campos(linea: str) -> list[str]:
    return linea.rstrip("\r\n").split("|")


def _terminador(lineas: Sequence[str]) -> str:
    for linea in lineas:
        if linea.endswith("\r\n"):
            return "\r\n"
        if linea.endswith("\n"):
            return "\n"
    return "\r\n"


def codigo_base_de_padre(padre: str) -> str:
    """Código del padre sin la marca `#` de capítulo, que no es parte del código.

    Caso real: `~D|33.03.01#|...` de El Escorial, cuyo `~M` apunta al par
    `33.03.01\\%CC`, ya sin la marca.
    """
    return padre.removesuffix("#")


def codigo_de_clon(padre: str, ordinal: int, ocupados: dict[str, str]) -> str:
    """`<padre>.P<n>` recortado a 20 caracteres y único (R5).

    La unicidad la resuelve `_shorten_code_unique` de `bc3_modifier`, que ya
    implementa la escalera naive → 19+último → sufijo `#i`; aquí solo se le
    exige que mire también los códigos cortos (`forzar_unicidad`).
    """
    sufijo = f".P{ordinal}"
    candidato = codigo_base_de_padre(padre)[:MAX_CODE_LEN - len(sufijo)] + sufijo
    elegido = _shorten_code_unique(candidato, ocupados, forzar_unicidad=True)
    ocupados[elegido] = elegido
    return elegido


def _indices_porcentuales(triples: Sequence[Tripleta],
                          es_pct: Callable[[str], bool]) -> list[int]:
    return [i for i, (codigo, _, _) in enumerate(triples) if es_pct(codigo)]


def _producto_de_factores(triples: Sequence[Tripleta]) -> Decimal | None:
    """`Π(1 + r_i)` de un `~D` solo-porcentual, o None si no se puede despejar.

    R9 ter: un `(1 + r_i)` de 0 o negativo —un descuento del −100 % o mayor—
    hace la división imposible o absurda, así que ese `~D` se queda intacto.

    Los números ya vienen validados: `_base_es_indeterminada` corre antes y
    manda a R16 cualquier `~D` con un factor o un rendimiento ilegible, así que
    aquí `a_decimal` no puede devolver None.
    """
    producto = Decimal(1)
    for _, factor, rendimiento in triples:
        paso = Decimal(1) + a_decimal(factor) * a_decimal(rendimiento)
        if paso <= 0:
            return None
        producto *= paso
    return producto


def _base_es_indeterminada(triples: Sequence[Tripleta],
                           hasta: int,
                           precios: Mapping[str, Decimal | None],
                           es_pct: Callable[[str], bool]) -> bool:
    """R16: alguna línea previa a una porcentual no permite calcular la base."""
    for codigo, factor, rendimiento in triples[: hasta + 1]:
        if a_decimal(factor) is None or a_decimal(rendimiento) is None:
            return True
        if not es_pct(codigo) and precios.get(codigo) is None:
            return True
    return False


def planificar(src: Path,
               encoding: str = "latin-1",
               decimales: int = DECIMALES_POR_DEFECTO) -> Plan:
    """PASADA 1: lee el fichero entero y decide qué se convierte y con qué código."""
    decimales = decimales_saneados(decimales)
    lineas = _leer_lineas(Path(src), encoding)

    precios: dict[str, Decimal | None] = {}
    unidades: dict[str, str] = {}
    resumenes: dict[str, str] = {}
    fechas: dict[str, str] = {}
    # Códigos ya ocupados: los ~C y TAMBIÉN su truncado a 20 (D6), porque
    # `convert_to_material` recortará después los largos sobre ese hueco.
    ocupados: dict[str, str] = {}

    for linea in lineas:
        if not linea.startswith("~C|"):
            continue
        campos = _campos(linea)
        # `campos[1]` existe siempre: la línea empieza por "~C|".
        codigo = campos[1]
        if not codigo:
            continue
        unidades[codigo] = campos[2] if len(campos) > 2 else ""
        resumenes[codigo] = campos[3] if len(campos) > 3 else ""
        precios[codigo] = a_decimal(campos[4]) if len(campos) > 4 else None
        fechas[codigo] = campos[5] if len(campos) > 5 else ""
        ocupados[codigo] = codigo
        ocupados[codigo[:MAX_CODE_LEN]] = codigo

    def es_pct(codigo: str) -> bool:
        return es_porcentual(codigo, unidades.get(codigo, ""))

    plan = Plan()
    informe = plan.informe
    informe.decimales = decimales

    for numero, linea in enumerate(lineas):
        if not linea.startswith("~D|"):
            continue
        campos = _campos(linea)
        padre = campos[1]  # existe siempre: la línea empieza por "~D|"
        triples = triples_de_cuerpo(campos[2] if len(campos) > 2 else "")
        indices = _indices_porcentuales(triples, es_pct)
        if not indices:
            continue

        informe.descompuestos += 1
        primer_pct = triples[indices[0]]

        if _base_es_indeterminada(triples, indices[-1], precios, es_pct):
            # R16: el ~D entero se queda como está y sus conceptos % se conservan.
            informe.anota(padre, primer_pct[0], MOTIVO_BASE_INDETERMINADA,
                          a_decimal(primer_pct[2]) or 0, 0)
            logger.info("~D %s sin convertir: base indeterminada", padre)
            continue

        # R9: todas las líneas son porcentuales y el padre trae precio ≠ 0.
        base_inicial: Decimal | None = None
        precio_padre = precios.get(padre)
        if precio_padre is None:
            precio_padre = precios.get(codigo_base_de_padre(padre))
        solo_porcentuales = (len(indices) == len(triples)
                             and precio_padre not in (None, Decimal(0)))
        if solo_porcentuales:
            producto = _producto_de_factores(triples)
            if producto is None:
                # R9 ter: algún (1 + r) ≤ 0; la base no se puede despejar.
                # El rendimiento se lee sin red: para llegar aquí ya pasó por
                # `_base_es_indeterminada`, que aparta los ilegibles.
                informe.anota(padre, primer_pct[0], MOTIVO_BASE_NO_DESPEJABLE,
                              a_decimal(primer_pct[2]), 0)
                logger.info("~D %s sin convertir: base no despejable", padre)
                continue
            base_inicial = redondear(precio_padre / producto, decimales)

        importes = calcular_importes(triples, precios, es_pct,
                                     redondear_a=decimales,
                                     base_inicial=base_inicial)

        # La línea de base se reserva ANTES que las porcentuales para que se
        # quede con el `.P0` natural (R5) y encabece la familia en Presto.
        linea_base: LineaBase | None = None
        if base_inicial is not None:
            linea_base = LineaBase(
                codigo=codigo_de_clon(padre, 0, ocupados),
                precio=base_inicial,  # R9 bis lo ajusta con el residuo
                resumen=resumenes.get(padre) or padre,
                fecha=fechas.get(padre, ""),
            )

        clones: list[LineaClon] = []
        for ordinal, indice in enumerate(indices, start=1):
            hijo = triples[indice][0]
            codigo_nuevo = codigo_de_clon(padre, ordinal, ocupados)
            clones.append(
                LineaClon(
                    indice=indice,
                    codigo=codigo_nuevo,
                    original=hijo,
                    precio=importes[indice],
                    resumen=resumenes.get(hijo) or hijo,
                    fecha=fechas.get(hijo, ""),
                )
            )
            plan.remapeo_m[(codigo_base_de_padre(padre), hijo)] = codigo_nuevo
            informe.lineas_convertidas += 1

        if linea_base is not None:
            # R9 bis: el residuo de redondeo se absorbe aquí, nunca retocando
            # una línea porcentual, así que la suma del ~D es exactamente P.
            resto = precio_padre - sum((c.precio for c in clones), Decimal(0))
            linea_base = replace(linea_base, precio=redondear(resto, decimales))
            informe.anota(padre, linea_base.codigo, MOTIVO_BASE_RECONSTRUIDA,
                          producto, linea_base.precio)
            logger.info(
                "~D %s: base reconstruida %s a partir del precio del padre %s",
                padre, linea_base.precio, precio_padre,
            )

        plan.planes[numero] = PlanDescompuesto(numero, padre, clones, linea_base)

    _decidir_conceptos_a_eliminar(lineas, plan, es_pct)
    return plan


def _decidir_conceptos_a_eliminar(lineas: Sequence[str],
                                  plan: Plan,
                                  es_pct: Callable[[str], bool]) -> None:
    """R13/R14: un concepto `%` se borra solo si ya no lo referencia ningún `~D`."""
    referenciado_por: dict[str, str] = {}
    for numero, linea in enumerate(lineas):
        if not linea.startswith("~D|"):
            continue
        campos = _campos(linea)
        padre = campos[1]  # existe siempre: la línea empieza por "~D|"
        triples = triples_de_cuerpo(campos[2] if len(campos) > 2 else "")
        convertidos = ({c.indice for c in plan.planes[numero].clones}
                       if numero in plan.planes else set())
        for indice, (codigo, _, _) in enumerate(triples):
            if indice not in convertidos:
                referenciado_por.setdefault(codigo, padre)

    porcentuales = set()
    for linea in lineas:
        if not linea.startswith("~C|"):
            continue
        campos = _campos(linea)
        codigo = campos[1]  # existe siempre: la línea empieza por "~C|"
        if codigo and es_pct(codigo):
            porcentuales.add(codigo)

    for codigo in sorted(porcentuales):
        if codigo in referenciado_por:
            # R14: sigue vivo en alguna tripleta sin convertir; se conserva.
            plan.informe.anota(referenciado_por[codigo], codigo,
                               MOTIVO_CONCEPTO_CONSERVADO, 0, 0)
            logger.info("concepto %s conservado: lo referencia aún %s",
                        codigo, referenciado_por[codigo])
        else:
            plan.conceptos_a_eliminar.add(codigo)
    plan.informe.conceptos_eliminados = len(plan.conceptos_a_eliminar)


# --------------------------------------------------------------------------- #
# Reescritura (pasada 2)                                                       #
# --------------------------------------------------------------------------- #
def _terminador_de(linea: str) -> str:
    """El salto de línea que traía esa línea, para devolverlo tal cual."""
    return linea[len(linea.rstrip("\r\n")):]


def _linea_c_de_clon(clon: LineaClon | LineaBase, terminador: str,
                     decimales: int) -> str:
    """`~C` de un clon o de una línea de base: unidad UD, tipo 3 (R4)."""
    return (
        f"~C|{clon.codigo}|{UNIDAD_CLON}|{clon.resumen}|"
        f"{formatear_precio(clon.precio, decimales)}|{clon.fecha}|{TIPO_CLON}|{terminador}"
    )


def _reescribir_d(linea: str, descompuesto: PlanDescompuesto) -> str:
    """Sustituye cada tripleta porcentual por `clon\\1\\1` (R3).

    Las demás tripletas salen con su texto de entrada, sin reformatear, y el
    cierre `\\|` lo garantiza `_format_d_triplets` de `bc3_modifier` (R17).
    """
    campos = _campos(linea)
    # Si hay plan es que la pasada 1 leyó tripletas aquí: campos[2] existe.
    triples = triples_de_cuerpo(campos[2])
    por_indice = {clon.indice: clon for clon in descompuesto.clones}
    tripletas = [
        f"{por_indice[i].codigo}\\1\\1" if i in por_indice else "\\".join(triple)
        for i, triple in enumerate(triples)
    ]
    if descompuesto.base is not None:
        # R9: la base reconstruida encabeza el ~D, con factor 1 y rendimiento 1.
        tripletas.insert(0, f"{descompuesto.base.codigo}\\1\\1")
    texto = _format_d_triplets(campos[1], tripletas)
    return texto[:-1] + _terminador_de(linea)


def _remapear_m(linea: str, remapeo: Mapping[tuple[str, str], str]) -> str:
    """R15: un `~M` sobre el par `padre\\porcentual` pasa a apuntar al clon."""
    campos = _campos(linea)  # campos[1] existe: la línea empieza por "~M|"
    par = campos[1].split("\\")
    if len(par) != 2:
        return linea
    padre, hijo = par
    clon = remapeo.get((codigo_base_de_padre(padre), hijo))
    if clon is None:
        return linea
    campos[1] = f"{padre}\\{clon}"
    return "|".join(campos) + _terminador_de(linea)


def convertir_porcentuales(src: Path,
                           dst: Path,
                           *,
                           encoding: str = "latin-1",
                           activo: bool = True,
                           decimales: int = DECIMALES_POR_DEFECTO) -> InformePorcentuales:
    """PASADA 2: escribe en `dst` el BC3 con los porcentuales ya convertidos.

    Con `activo=False` copia el fichero sin tocar ni una línea (R21). Si la
    entrada no existe lanza `FileNotFoundError` y no crea la salida (R23).
    """
    src, dst = Path(src), Path(dst)
    if not src.exists():
        raise FileNotFoundError(src)

    if not activo:
        dst.parent.mkdir(parents=True, exist_ok=True)
        dst.write_bytes(src.read_bytes())
        logger.info("porcentuales_a_ud desactivada: %s copiado sin cambios", src.name)
        return InformePorcentuales()

    plan = planificar(src, encoding, decimales)
    lineas = _leer_lineas(src, encoding)
    terminador = _terminador(lineas)

    salida: list[str] = []
    borrando = False  # dentro de un registro multilínea que se está eliminando
    for numero, linea in enumerate(lineas):
        if not linea.startswith("~"):
            # Continuación del registro anterior (los ~T largos la usan).
            if not borrando:
                salida.append(linea)
            continue

        borrando = False
        if linea.startswith(("~C|", "~T|")):
            campos = _campos(linea)
            codigo = campos[1]  # existe siempre: la línea empieza por "~C|" o "~T|"
            if codigo in plan.conceptos_a_eliminar:
                # R13: el concepto porcentual ya no lo referencia nadie.
                borrando = True
                continue

        if numero in plan.planes:
            descompuesto = plan.planes[numero]
            # La base encabeza la familia, igual que en el ~D: en Presto se ve
            # `<padre>.P0` y debajo sus porcentuales.
            base = [descompuesto.base] if descompuesto.base is not None else []
            emitidos = base + list(descompuesto.clones)
            salida.extend(_linea_c_de_clon(c, terminador, plan.informe.decimales)
                          for c in emitidos)
            salida.append(_reescribir_d(linea, descompuesto))
            continue

        if linea.startswith("~M|"):
            salida.append(_remapear_m(linea, plan.remapeo_m))
            continue

        salida.append(linea)

    dst.parent.mkdir(parents=True, exist_ok=True)
    dst.write_bytes("".join(salida).encode(encoding))
    logger.info(
        "%s → %s: %d ~D con porcentual, %d líneas convertidas, %d conceptos "
        "eliminados, %d decimales",
        src.name, dst.name, plan.informe.descompuestos,
        plan.informe.lineas_convertidas, plan.informe.conceptos_eliminados,
        plan.informe.decimales,
    )
    return plan.informe
