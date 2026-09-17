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
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Callable, Iterable, Mapping, Sequence

logger = logging.getLogger(__name__)

# --- constantes del dominio ------------------------------------------------ #
CENTIMO = Decimal("0.01")
UNIDAD_CLON = "UD"
TIPO_CLON = "3"
MARCA_PORCENTUAL = "%"

# Motivos de las filas excepcionales del informe (R9, R14, R16).
MOTIVO_PRECIO_PADRE = "precio_del_padre_aplicado"
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
    casos: list[CasoPorcentual] = field(default_factory=list)

    def anota(self, padre: str, codigo: str, motivo: str,
              rendimiento: Decimal | float = 0, importe: Decimal | float = 0) -> None:
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


def redondear_centimos(valor: Decimal) -> Decimal:
    """Redondeo a 2 decimales con ROUND_HALF_UP (R6)."""
    return valor.quantize(CENTIMO, rounding=ROUND_HALF_UP)


def formatear_precio(valor: Decimal) -> str:
    """Precio tal y como se escribe en un `~C`: 2 decimales, punto, sin `E`.

    El `-0` se escribe como `0` (R7): un precio negativo de cero es ruido que
    algunos ERP leen como texto no numérico.
    """
    redondeado = redondear_centimos(valor)
    if redondeado == 0:
        return "0"
    return f"{redondeado:f}"


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
                      redondear: bool = False,
                      base_inicial: Decimal | None = None) -> list[Decimal]:
    """Importe de cada tripleta, recorridas EN EL ORDEN DEL FICHERO (D3).

    Es la única implementación del cálculo: la usan la pasada y los tests.

    - `redondear=True` fija cada importe porcentual a 2 decimales y acumula en
      la base el valor YA redondeado (R6), que es el que se podrá releer en el
      fichero de salida. Es el modo con el que se calculan los precios de clon.
    - `redondear=False` deja el encadenado exacto: es como se mide la ENTRADA
      al comprobar el invariante de R19.
    - `base_inicial` fuerza el precio de la primera línea porcentual de un
      descompuesto cuyas líneas son todas porcentuales (R9).
    """
    importes: list[Decimal] = []
    base = Decimal(0)
    primera_pct = True
    for codigo, factor, rendimiento in triples:
        if es_pct(codigo):
            if primera_pct and base_inicial is not None:
                importe = base_inicial
            else:
                importe = importe_porcentual(factor, rendimiento, base)
            if redondear:
                importe = redondear_centimos(importe)
            primera_pct = False
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


def hay_porcentual(triples: Iterable[Tripleta],
                   es_pct: Callable[[str], bool]) -> bool:
    return any(es_pct(codigo) for codigo, _, _ in triples)
