# tests/test_f002_porcentuales.py
"""F-002 · Conversión de descompuestos porcentuales a UD con cantidad 1.

Los tests llevan el número del requisito EARS de
`specs/F-002-porcentuales-a-ud/requirements.md` en el nombre. Los números que
se comparan son los que Presto muestra sobre los presupuestos reales de
`input/` (81,5364 · 71,54 · −1,46 · 16,00), escritos a mano: si el cálculo se
equivocara igual en la entrada y en la salida, el invariante de R19 no lo
vería, pero estos literales sí.

Sin red, sin BBDD y sin servicios de IA: la pasada solo lee y escribe ficheros.
"""

from __future__ import annotations

from decimal import Decimal

from infrastructure.bc3.bc3_porcentajes import (
    calcular_importes,
    es_porcentual,
    formatear_precio,
    importe_linea,
    importe_porcentual,
)

# Precios y tripletas literales de `input/`, tal y como los lee el BC3.
PRECIOS_43_15 = {
    "OPTIMIZADOR MPP": Decimal("41.18"),
    "%SUB25": Decimal("25"),
    "%SUB20": Decimal("20"),
    "%SUB10": Decimal("10"),
}
TRIPLES_43_15 = [
    ("OPTIMIZADOR MPP", "1", "1.2"),
    ("%SUB25", "1", "0.25"),
    ("%SUB20", "1", "0.2"),
    ("%SUB10", "1", "0.1"),
]

PRECIOS_07_02_05 = {
    "IMPALF": Decimal("26.93"),
    "IMPASF": Decimal("9.25"),
    "%MAVEN": Decimal("-5"),
    "impv": Decimal("8"),
}
TRIPLES_07_02_05 = [
    ("IMPALF", "1", "0"),
    ("IMPASF", "1", "0"),
    ("%MAVEN", "1", "-0.05"),
    ("impv", "1", "2"),
]


def _por_codigo(codigo: str) -> bool:
    """`es_pct` de conveniencia: decide solo por el código."""
    return es_porcentual(codigo, "")


# --------------------------------------------------------------------------- #
# R1 · Detección de línea porcentual                                           #
# --------------------------------------------------------------------------- #
def test_f002_r1_detecta_por_codigo():
    assert es_porcentual("%SUB25", "")
    assert es_porcentual("%VID", "")
    assert es_porcentual("% BATACHES", "")
    assert es_porcentual("%%beneficioseinsa", "")


def test_f002_r1_detecta_por_unidad_aunque_el_codigo_no_empiece_por_porcentaje():
    assert es_porcentual("MA0100", "%")
    assert es_porcentual("MA0100", " % ")


def test_f002_r1_detecta_el_porcentaje_tras_espacios_por_delante():
    assert es_porcentual("  %RF", "")


def test_f002_r1_una_linea_normal_no_es_porcentual():
    assert not es_porcentual("OPTIMIZADOR MPP", "")
    assert not es_porcentual("05.01.01", "m2")
    assert not es_porcentual("IMPALF", "M")
    # El porcentaje dentro del texto no cuenta: solo al principio del código.
    assert not es_porcentual("DTO%10", "ud")


# --------------------------------------------------------------------------- #
# R2 · Importes: D2 (línea normal) y D4 (línea porcentual)                     #
# --------------------------------------------------------------------------- #
def test_f002_r2_importe_de_linea_normal_es_precio_por_factor_por_rendimiento():
    assert importe_linea(Decimal("41.18"), "1", "1.2") == Decimal("49.416")
    assert importe_linea(Decimal("8"), "1", "2") == Decimal("16")
    # Sin precio conocido el importe es 0 (ese ~D no se convierte, R16).
    assert importe_linea(None, "1", "2") == Decimal("0")


def test_f002_r2_importe_porcentual_es_rendimiento_por_la_base_acumulada():
    assert importe_porcentual("1", "0.25", Decimal("49.416")) == Decimal("12.354")
    assert importe_porcentual("1", "-0.02", Decimal("73")) == Decimal("-1.46")
    # El precio del ~C del porcentual NO interviene: solo el rendimiento.
    assert importe_porcentual("1", "0.05", Decimal("0")) == Decimal("0")


def test_f002_r2_cadena_43_15_suma_el_literal_de_presto_81_5364():
    importes = calcular_importes(TRIPLES_43_15, PRECIOS_43_15, _por_codigo)
    assert importes == [
        Decimal("49.416"),
        Decimal("12.354"),
        Decimal("12.354"),
        Decimal("7.4124"),
    ]
    assert sum(importes) == Decimal("81.5364")


def test_f002_r2_cadena_43_15_redondeada_da_los_precios_de_clon_12_35_12_35_7_41():
    """R6: la base acumula el importe YA redondeado, que es el que viaja al BC3."""
    importes = calcular_importes(
        TRIPLES_43_15, PRECIOS_43_15, _por_codigo, redondear=True
    )
    assert importes[1] == Decimal("12.35")
    assert importes[2] == Decimal("12.35")
    assert importes[3] == Decimal("7.41")
    assert sum(importes) == Decimal("81.526")


def test_f002_r2_descuento_negativo_05_06_29_da_menos_1_46_y_total_71_54():
    precios = {"33.1.16.33.1BE": Decimal("73"), "%VID": Decimal("-2")}
    triples = [("33.1.16.33.1BE", "1", "1"), ("%VID", "1", "-0.02")]
    importes = calcular_importes(triples, precios, _por_codigo, redondear=True)
    assert importes == [Decimal("73"), Decimal("-1.46")]
    assert sum(importes) == Decimal("71.54")


def test_f002_r2_el_segundo_porcentual_ve_el_importe_del_primero_en_su_base():
    """Sin encadenar, %SUB20 daría 0,2 × 49,416 = 9,88 en vez de 12,35."""
    importes = calcular_importes(
        TRIPLES_43_15, PRECIOS_43_15, _por_codigo, redondear=True
    )
    assert importes[2] != (Decimal("0.2") * Decimal("49.416")).quantize(Decimal("0.01"))
    assert importes[2] == Decimal("12.35")


# --------------------------------------------------------------------------- #
# R7 · El precio -0 se escribe como 0                                          #
# --------------------------------------------------------------------------- #
def test_f002_r7_el_menos_cero_se_escribe_como_cero():
    assert formatear_precio(Decimal("-0.00")) == "0"
    assert formatear_precio(Decimal("0.00")) == "0"
    assert formatear_precio(Decimal("-0.05") * Decimal("0")) == "0"


def test_f002_r7_los_precios_no_salen_en_notacion_cientifica_ni_con_coma():
    for valor in (Decimal("1100.00"), Decimal("0.00001"), Decimal("-1.46")):
        texto = formatear_precio(valor)
        assert "E" not in texto.upper()
        assert "," not in texto


# --------------------------------------------------------------------------- #
# R11 · Los rendimientos 0 de las líneas normales se respetan                  #
# --------------------------------------------------------------------------- #
def test_f002_r11_los_rendimientos_cero_cuentan_como_importe_cero():
    importes = calcular_importes(
        TRIPLES_07_02_05, PRECIOS_07_02_05, _por_codigo, redondear=True
    )
    assert importes[0] == Decimal("0")
    assert importes[1] == Decimal("0")
    # R10: la base del porcentual es 0 porque las dos líneas previas valen 0.
    assert importes[2] == Decimal("0")
    assert importes[3] == Decimal("16")
    assert sum(importes) == Decimal("16")
    assert formatear_precio(importes[2]) == "0"
