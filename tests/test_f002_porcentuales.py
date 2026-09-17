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

from dataclasses import FrozenInstanceError, replace
from decimal import Decimal
from pathlib import Path

import pytest

from config.settings import Settings
from infrastructure.bc3.bc3_modifier import MAX_CODE_LEN, _shorten_code_unique
from infrastructure.bc3.bc3_porcentajes import (
    CasoPorcentual,
    calcular_importes,
    codigo_de_clon,
    convertir_porcentuales,
    es_porcentual,
    formatear_precio,
    importe_linea,
    importe_porcentual,
    planificar,
)
from interface_adapters.cli import porcentuales_cli as cli

FIXTURES = Path(__file__).parent / "fixtures"

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


def _clones_por_padre(plan) -> dict[str, list[str]]:
    """Códigos de clon planificados, agrupados por el padre de su `~D`."""
    agrupados: dict[str, list[str]] = {}
    for descompuesto in plan.planes.values():
        agrupados.setdefault(descompuesto.padre, []).extend(
            clon.codigo for clon in descompuesto.clones
        )
    return agrupados


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


def test_f002_r2_una_tripleta_con_numeros_ilegibles_vale_cero_y_no_rompe():
    """Basta con que UNO de los dos números no se lea para no poder multiplicar."""
    assert importe_linea(Decimal("10"), "", "2") == Decimal("0")
    assert importe_linea(Decimal("10"), "1", "no-es-un-numero") == Decimal("0")
    assert importe_porcentual("", "0.1", Decimal("100")) == Decimal("0")
    assert importe_porcentual("1", "no-es-un-numero", Decimal("100")) == Decimal("0")


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


# --------------------------------------------------------------------------- #
# R5 · Códigos de clon                                                         #
# --------------------------------------------------------------------------- #
def test_f002_r5_el_codigo_del_clon_es_el_padre_con_sufijo_p_n():
    ocupados: dict[str, str] = {}
    assert codigo_de_clon("05.06.29", 1, ocupados) == "05.06.29.P1"
    assert codigo_de_clon("43.15", 3, ocupados) == "43.15.P3"


def test_f002_r5_recorta_el_padre_hasta_caber_en_veinte_caracteres():
    """`RUESMA-C32.04.07.01` (19) + `.P1` daría 22: el recorte es real."""
    ocupados: dict[str, str] = {}
    clon = codigo_de_clon("RUESMA-C32.04.07.01", 1, ocupados)
    assert len(clon) <= MAX_CODE_LEN
    assert clon.endswith(".P1")
    assert clon == "RUESMA-C32.04.07..P1"


def test_f002_r5_dos_clones_del_mismo_padre_no_chocan_entre_si():
    ocupados: dict[str, str] = {}
    primero = codigo_de_clon("09.01.04", 1, ocupados)
    segundo = codigo_de_clon("09.01.04", 1, ocupados)
    assert primero != segundo


def test_f002_r5_esquiva_un_codigo_corto_ya_ocupado():
    ocupados = {"09.01.03.P1": "09.01.03.P1"}
    clon = codigo_de_clon("09.01.03", 1, ocupados)
    assert clon != "09.01.03.P1"
    assert len(clon) <= MAX_CODE_LEN


def test_f002_r5_el_helper_compartido_no_cambia_para_sus_llamantes_actuales():
    """`forzar_unicidad` es False por defecto: `convert_to_material` no se mueve.

    Ese es justo el compromiso del cambio en `bc3_modifier`: un código corto
    repetido se sigue devolviendo tal cual salvo que se pida lo contrario.
    """
    ocupados = {"MO0010": "MO0010"}
    assert _shorten_code_unique("MO0010", ocupados) == "MO0010"
    assert _shorten_code_unique("MO0010", ocupados, forzar_unicidad=True) != "MO0010"


def test_f002_r5_el_helper_no_apunta_en_ocupados_lo_que_devuelve_intacto():
    """Quien reserva el código es el llamante: `codigo_de_clon`, no el helper.

    Vale también para un código de 20 caracteres justos, que es el borde del
    recorte.
    """
    ocupados: dict[str, str] = {}
    veinte = "A" * MAX_CODE_LEN
    assert _shorten_code_unique(veinte, ocupados) == veinte
    assert ocupados == {}
    assert _shorten_code_unique("MO0010", ocupados, forzar_unicidad=True) == "MO0010"
    assert ocupados == {}


def test_f002_r5_planificar_evita_el_truncado_a_veinte_de_un_codigo_largo():
    """D6: `convert_to_material` recortará después; el clon reserva su hueco."""
    plan = planificar(FIXTURES / "f002_codigo_largo.bc3")
    clones = _clones_por_padre(plan)
    elegido = clones["RUESMA-C32.04.07.01"][0]
    assert elegido != "RUESMA-C32.04.07..P1"  # lo ocupa el truncado del largo
    assert len(elegido) <= MAX_CODE_LEN


def test_f002_r5_planificar_no_repite_ningun_codigo_de_clon():
    plan = planificar(FIXTURES / "f002_codigo_largo.bc3")
    todos = [c for lista in _clones_por_padre(plan).values() for c in lista]
    assert len(todos) == 3
    assert len(set(todos)) == 3
    assert all(len(c) <= MAX_CODE_LEN for c in todos)
    # El padre sin colisiones se queda con el código natural.
    assert "09.01.04.P1" in todos


# --------------------------------------------------------------------------- #
# Reescritura del fichero (R3, R4, R6, R9, R10, R12-R18, R23)                  #
# --------------------------------------------------------------------------- #
def _convertir(tmp_path, nombre: str):
    """Ejecuta la pasada sobre una fixture y devuelve (líneas, informe, ruta)."""
    destino = tmp_path / f"salida_{nombre}"
    informe = convertir_porcentuales(FIXTURES / nombre, destino)
    return leer(destino), informe, destino


def leer(ruta: Path) -> list[str]:
    return ruta.read_bytes().decode("latin-1").splitlines()


def _registro(lineas: list[str], prefijo: str) -> str:
    coincidencias = [l for l in lineas if l.startswith(prefijo)]
    assert coincidencias, f"no hay ninguna línea que empiece por {prefijo!r}"
    return coincidencias[0]


def test_f002_r3_la_tripleta_porcentual_queda_como_clon_con_factor_y_rendimiento_uno(tmp_path):
    lineas, _, _ = _convertir(tmp_path, "f002_cadena.bc3")
    assert _registro(lineas, "~D|43.15|") == (
        "~D|43.15|OPTIMIZADOR MPP\\1\\1.2\\43.15.P1\\1\\1\\43.15.P2\\1\\1\\"
        "43.15.P3\\1\\1\\|"
    )


def test_f002_r3_las_tripletas_no_porcentuales_salen_con_su_texto_original(tmp_path):
    lineas, _, _ = _convertir(tmp_path, "f002_ceros.bc3")
    assert _registro(lineas, "~D|07.02.05|") == (
        "~D|07.02.05|IMPALF\\1\\0\\IMPASF\\1\\0\\07.02.05.P1\\1\\1\\impv\\1\\2\\|"
    )


def test_f002_r4_el_clon_lleva_unidad_ud_tipo_3_y_la_fecha_y_el_resumen_del_original(tmp_path):
    lineas, _, _ = _convertir(tmp_path, "f002_negativo.bc3")
    clon = _registro(lineas, "~C|05.06.29.P1|")
    campos = clon.split("|")
    assert campos[2] == "UD"
    assert campos[3] == "Descuento vidrio sin intercalario de Pvc (WE)"
    assert campos[4] == "-1.46"
    assert campos[5] == "200220"
    assert campos[6] == "3"


def test_f002_r6_los_precios_de_clon_de_43_15_son_12_35_12_35_y_7_41(tmp_path):
    lineas, _, _ = _convertir(tmp_path, "f002_cadena.bc3")
    assert _registro(lineas, "~C|43.15.P1|").split("|")[4] == "12.35"
    assert _registro(lineas, "~C|43.15.P2|").split("|")[4] == "12.35"
    assert _registro(lineas, "~C|43.15.P3|").split("|")[4] == "7.41"


def test_f002_r9_los_cuatro_descompuestos_solo_porcentuales_toman_el_precio_del_padre(tmp_path):
    lineas, informe, _ = _convertir(tmp_path, "f002_solo_pct.bc3")
    esperado = {
        "31.04.03.01.P1": "1100.00",
        "32.03.04.32.P1": "1117.65",
        "ICV260.P1": "291.50",
        "ICV270.P1": "369.50",
    }
    for codigo, precio in esperado.items():
        assert _registro(lineas, f"~C|{codigo}|").split("|")[4] == precio
    # Las porcentuales siguientes se aplican sobre ese precio.
    assert _registro(lineas, "~C|ICV260.P2|").split("|")[4] == "44.89"
    assert _registro(lineas, "~C|ICV270.P2|").split("|")[4] == "56.90"
    padres = {c.padre for c in informe.casos if c.motivo == "precio_del_padre_aplicado"}
    assert padres == {"31.04.03.01", "32.03.04.32", "ICV260", "ICV270"}


def test_f002_r10_base_cero_con_una_linea_normal_detras_da_precio_cero(tmp_path):
    lineas, informe, _ = _convertir(tmp_path, "f002_ceros.bc3")
    assert _registro(lineas, "~C|07.02.05.P1|").split("|")[4] == "0"
    # R9 no aplica: el ~D tiene líneas no porcentuales.
    assert not [c for c in informe.casos if c.motivo == "precio_del_padre_aplicado"]


def test_f002_r12_el_porcentual_con_rendimiento_cero_se_convierte_con_precio_cero(tmp_path):
    """1000080 de lagunamodificado16julio: 0,93 + 0,11 + 0 + 0,16 = 1,20."""
    lineas, _, _ = _convertir(tmp_path, "f002_cadena.bc3")
    assert _registro(lineas, "~C|1000080.P2|").split("|")[4] == "0"
    assert _registro(lineas, "~C|1000080.P1|").split("|")[4] == "0.11"
    assert _registro(lineas, "~C|1000080.P3|").split("|")[4] == "0.16"
    descompuesto = _registro(lineas, "~D|1000080|")
    assert "%%gastosfinancieros" not in descompuesto
    precios = [
        Decimal(_registro(lineas, f"~C|1000080.P{n}|").split("|")[4] or "0")
        for n in (1, 2, 3)
    ]
    assert Decimal("0.93") + sum(precios) == Decimal("1.20")


def test_f002_r13_desaparecen_el_c_y_el_t_del_porcentual_que_ya_no_se_usa(tmp_path):
    lineas, informe, _ = _convertir(tmp_path, "f002_cadena.bc3")
    for codigo in ("%SUB25", "%SUB20", "%SUB10"):
        assert not [l for l in lineas if l.startswith(f"~C|{codigo}|")]
    assert not [l for l in lineas if l.startswith("~T|%SUB25|")]
    # El ~T era multilínea: su continuación tampoco puede quedarse suelta.
    assert "segunda línea del texto del concepto porcentual" not in lineas
    assert informe.conceptos_eliminados == 6


def test_f002_r14_el_porcentual_que_sigue_referenciado_se_conserva(tmp_path):
    lineas, informe, _ = _convertir(tmp_path, "f002_sin_precio.bc3")
    assert _registro(lineas, "~C|%RF|")
    assert informe.conceptos_eliminados == 0
    conservados = [c for c in informe.casos if c.motivo == "concepto_conservado"]
    # El informe dice QUIÉN lo sigue referenciando, que es lo que hay que mirar.
    # Un concepto conservado no tiene un rendimiento ni un importe propios: van
    # a 0 para que la fila no aparente un número que nadie ha calculado.
    assert [(c.padre, c.codigo, c.rendimiento, c.importe) for c in conservados] == [
        ("09.01.01", "%RF", 0.0, 0.0)
    ]


def test_f002_r15_el_registro_m_del_par_convertido_apunta_al_clon(tmp_path):
    lineas, _, _ = _convertir(tmp_path, "f002_unidad_pct.bc3")
    assert _registro(lineas, "~M|05.01.01\\") == (
        "~M|05.01.01\\05.01.01.P1|32\\4\\1\\2\\|66||"
    )
    # El padre del ~D venía con la marca de capítulo '#' y el ~M sin ella.
    assert _registro(lineas, "~M|33.03.01\\") == (
        "~M|33.03.01\\33.03.01.P1|32\\4\\1\\2\\|66||"
    )


def test_f002_r3_un_descompuesto_sin_la_barra_de_cierre_tambien_se_lee(tmp_path):
    """No todos los exportadores cierran el `~D` con `\\`; las tripletas son las
    mismas y la salida sí sale ya normalizada (R17)."""
    entrada = tmp_path / "sin_barra.bc3"
    entrada.write_bytes(
        ("~V|RIB Spain|FIEBDC-3/2016|Presto 19.02||ANSI||2||||\r\n"
         "~C|09.05.01|m2|PARTIDA|10.3|200220|0|\r\n"
         "~C|09.05.02|m2|PARTIDA SIN NI SIQUIERA EL PIPE|10.3|200220|0|\r\n"
         "~C|MO0010|h|Oficial|10|200220|1|\r\n"
         "~C|%RF||RECARGO FIJO|3|200220|0|\r\n"
         "~D|09.05.01|MO0010\\1\\1\\%RF\\1\\0.03|\r\n"
         "~D|09.05.02|MO0010\\1\\1\\%RF\\1\\0.03\r\n").encode("latin-1")
    )
    salida = tmp_path / "salida.bc3"
    informe = convertir_porcentuales(entrada, salida)
    assert informe.lineas_convertidas == 2
    lineas = leer(salida)
    assert _registro(lineas, "~C|09.05.01.P1|").split("|")[4] == "0.30"
    assert _registro(lineas, "~D|09.05.01|") == (
        "~D|09.05.01|MO0010\\1\\1\\09.05.01.P1\\1\\1\\|"
    )
    assert _registro(lineas, "~D|09.05.02|") == (
        "~D|09.05.02|MO0010\\1\\1\\09.05.02.P1\\1\\1\\|"
    )


def test_f002_r16_un_c_truncado_no_rompe_la_pasada_y_deja_el_d_sin_convertir(tmp_path):
    """Un `~C` con menos campos de los debidos no puede tumbar la pasada."""
    entrada = tmp_path / "truncado.bc3"
    entrada.write_bytes(
        ("~V|RIB Spain|FIEBDC-3/2016|Presto 19.02||ANSI||2||||\r\n"
         "~C|09.02.01|m2|PARTIDA|10|200220|0|\r\n"
         "~C|SOLOCODIGO\r\n"
         "~C|MEDIOTRUNCADO|ud\r\n"
         "~C|OTROTRUNCADO|ud|Concepto sin precio ni fecha\r\n"
         "~C|SINFECHA|ud|Concepto sin fecha|12\r\n"
         "~C|%RF||RECARGO FIJO|3|200220|0|\r\n"
         "~C|%OTRO||OTRO RECARGO|4|200220|0|\r\n"
         "~D|SOLOELPADRE\r\n"
         "~D|09.02.01|MEDIOTRUNCADO\\1\\1\\OTROTRUNCADO\\1\\1\\%RF\\1\\0\\|\r\n"
         "~D|09.02.03|SINPRECIO\\1\\1\\%OTRO\\1\\0.03\r\n"
         ).encode("latin-1")
    )
    salida = tmp_path / "salida.bc3"
    informe = convertir_porcentuales(entrada, salida)
    assert informe.lineas_convertidas == 0
    assert informe.descompuestos == 2
    indeterminados = [c for c in informe.casos if c.motivo == "base_indeterminada"]
    assert {c.padre for c in indeterminados} == {"09.02.01", "09.02.03"}
    # El rendimiento 0 del porcentual se anota como 0, no como otra cosa.
    assert [c.rendimiento for c in indeterminados if c.padre == "09.02.01"] == [0.0]
    # Los dos conceptos % siguen referenciados por un ~D sin convertir (R14),
    # incluido el que vive en un ~D sin la barra de cierre.
    assert informe.conceptos_eliminados == 0
    lineas = leer(salida)
    assert _registro(lineas, "~C|%RF|")
    assert _registro(lineas, "~C|%OTRO|")
    assert _registro(lineas, "~D|09.02.01|") == (
        "~D|09.02.01|MEDIOTRUNCADO\\1\\1\\OTROTRUNCADO\\1\\1\\%RF\\1\\0\\|"
    )


def test_f002_r4_el_clon_hereda_la_fecha_aunque_el_c_venga_sin_barra_final(tmp_path):
    entrada = tmp_path / "sin_barra_c.bc3"
    entrada.write_bytes(
        ("~V|RIB Spain|FIEBDC-3/2016|Presto 19.02||ANSI||2||||\r\n"
         "~C|09.09.01|m2|PARTIDA|10.3|200220|0|\r\n"
         "~C|MO0010|h|Oficial|10|200220|1|\r\n"
         "~C|%RF||RECARGO FIJO|3|190220\r\n"
         "~C|%RES||RESUMEN SIN PRECIO\r\n"
         "~D|09.09.01|MO0010\\1\\1\\%RF\\1\\0.03\\%RES\\1\\0.01\\|\r\n").encode("latin-1")
    )
    salida = tmp_path / "salida.bc3"
    convertir_porcentuales(entrada, salida)
    campos = _registro(leer(salida), "~C|09.09.01.P1|").split("|")
    assert campos[3] == "RECARGO FIJO"
    assert campos[5] == "190220"
    # El ~C del segundo porcentual solo trae cuatro campos: ni precio ni fecha.
    segundo = _registro(leer(salida), "~C|09.09.01.P2|").split("|")
    assert segundo[3] == "RESUMEN SIN PRECIO"
    assert segundo[5] == ""


def test_f002_r1_un_c_corto_basta_para_declarar_la_unidad_y_el_precio(tmp_path):
    """`~C|MAUX|%` (tres campos) ya dice que es porcentual; `~C|...|12` (cinco)
    ya dice cuánto vale la línea que hace de base."""
    entrada = tmp_path / "campos_justos.bc3"
    entrada.write_bytes(
        ("~V|RIB Spain|FIEBDC-3/2016|Presto 19.02||ANSI||2||||\r\n"
         "~C|09.12.01|m2|PARTIDA|20|200220|0|\r\n"
         "~C|BASE5|ud|Concepto de cinco campos|12\r\n"
         "~C|MAUX|%\r\n"
         "~D|09.12.01|BASE5\\1\\1\\MAUX\\1\\0.05\\|\r\n").encode("latin-1")
    )
    salida = tmp_path / "salida.bc3"
    informe = convertir_porcentuales(entrada, salida)
    assert informe.lineas_convertidas == 1
    assert _registro(leer(salida), "~C|09.12.01.P1|").split("|")[4] == "0.60"


def test_f002_r13_un_porcentual_declarado_a_medias_y_sin_uso_tambien_se_borra(tmp_path):
    entrada = tmp_path / "porcentual_suelto.bc3"
    entrada.write_bytes(
        ("~V|RIB Spain|FIEBDC-3/2016|Presto 19.02||ANSI||2||||\r\n"
         "~C|09.11.01|m2|PARTIDA|10|200220|0|\r\n"
         "~C|%SOLO\r\n"
         "~D|09.11.01|MO0010\\1\\1\\|\r\n").encode("latin-1")
    )
    salida = tmp_path / "salida.bc3"
    informe = convertir_porcentuales(entrada, salida)
    assert informe.conceptos_eliminados == 1
    assert not [l for l in leer(salida) if l.startswith("~C|%SOLO")]


def test_f002_r9_el_log_dice_que_concepto_recibe_el_precio_del_padre(tmp_path, caplog):
    """Quien lea el log tiene que poder auditar el caso más delicado (D3)."""
    with caplog.at_level("INFO", logger="infrastructure.bc3.bc3_porcentajes"):
        convertir_porcentuales(FIXTURES / "f002_solo_pct.bc3", tmp_path / "s.bc3")
    mensajes = [r.getMessage() for r in caplog.records]
    assert any("ICV260" in m and "%%jefedeobrayencayGF4%" in m and "291.50" in m
               for m in mensajes), mensajes


def test_f002_r5_un_c_a_medias_sigue_reservando_su_codigo(tmp_path):
    """Aunque solo traiga el código, ese código está ocupado: el clon lo esquiva."""
    entrada = tmp_path / "codigo_reservado.bc3"
    entrada.write_bytes(
        ("~V|RIB Spain|FIEBDC-3/2016|Presto 19.02||ANSI||2||||\r\n"
         "~C|09.08.01|m2|PARTIDA|10.3|200220|0|\r\n"
         "~C|MO0010|h|Oficial|10|200220|1|\r\n"
         "~C|%RF||RECARGO FIJO|3|200220|0|\r\n"
         "~C|09.08.01.P1\r\n"
         "~D|09.08.01|MO0010\\1\\1\\%RF\\1\\0.03\\|\r\n").encode("latin-1")
    )
    plan = planificar(entrada)
    clones = [c for lista in _clones_por_padre(plan).values() for c in lista]
    assert clones and clones[0] != "09.08.01.P1"


def test_f002_r16_basta_con_que_falle_uno_de_los_dos_numeros_de_la_tripleta(tmp_path):
    """Factor ilegible y rendimiento legible: la base tampoco se puede calcular."""
    entrada = tmp_path / "factor_vacio.bc3"
    entrada.write_bytes(
        ("~V|RIB Spain|FIEBDC-3/2016|Presto 19.02||ANSI||2||||\r\n"
         "~C|09.06.01|m2|PARTIDA|10.3|200220|0|\r\n"
         "~C|MO0010|h|Oficial|10|200220|1|\r\n"
         "~C|%RF||RECARGO FIJO|3|200220|0|\r\n"
         "~D|09.06.01|MO0010\\\\1\\%RF\\1\\0.03\\|\r\n").encode("latin-1")
    )
    salida = tmp_path / "salida.bc3"
    informe = convertir_porcentuales(entrada, salida)
    assert informe.lineas_convertidas == 0
    assert [c.motivo for c in informe.casos] == ["base_indeterminada",
                                                 "concepto_conservado"]
    assert leer(salida) == leer(entrada)


def test_f002_r16_una_linea_sin_precio_POSTERIOR_al_porcentual_no_estorba(tmp_path):
    """La base son las líneas que PRECEDEN: lo que venga detrás no la toca."""
    entrada = tmp_path / "sin_precio_detras.bc3"
    entrada.write_bytes(
        ("~V|RIB Spain|FIEBDC-3/2016|Presto 19.02||ANSI||2||||\r\n"
         "~C|09.07.01|m2|PARTIDA|10.3|200220|0|\r\n"
         "~C|MO0010|h|Oficial|10|200220|1|\r\n"
         "~C|%RF||RECARGO FIJO|3|200220|0|\r\n"
         "~D|09.07.01|MO0010\\1\\1\\%RF\\1\\0.03\\SINPRECIO\\1\\1\\|\r\n").encode("latin-1")
    )
    salida = tmp_path / "salida.bc3"
    informe = convertir_porcentuales(entrada, salida)
    assert informe.lineas_convertidas == 1
    assert not [c for c in informe.casos if c.motivo == "base_indeterminada"]
    assert _registro(leer(salida), "~C|09.07.01.P1|").split("|")[4] == "0.30"


def test_f002_las_piezas_del_plan_son_inmutables():
    """El plan se decide en la pasada 1 y la pasada 2 no puede retocarlo."""
    plan = planificar(FIXTURES / "f002_negativo.bc3")
    descompuesto = plan.planes[next(iter(plan.planes))]
    clon = descompuesto.clones[0]
    caso = CasoPorcentual("p", "c", "m", 0.0, 0.0)
    for objeto, campo, valor in ((clon, "precio", Decimal("1")),
                                 (descompuesto, "padre", "otro"),
                                 (caso, "importe", 1.0)):
        with pytest.raises(FrozenInstanceError):
            setattr(objeto, campo, valor)


def test_f002_r16_el_descompuesto_con_base_indeterminada_sale_intacto(tmp_path):
    lineas, informe, _ = _convertir(tmp_path, "f002_sin_precio.bc3")
    assert _registro(lineas, "~D|09.01.01|") == (
        "~D|09.01.01|SINPRECIO\\1\\1\\%RF\\1\\0.03\\|"
    )
    casos = [c for c in informe.casos if c.motivo == "base_indeterminada"]
    assert [(c.padre, c.codigo, c.rendimiento, c.importe) for c in casos] == [
        ("09.01.01", "%RF", 0.03, 0.0)
    ]
    # El otro ~D del mismo fichero sí se convierte.
    assert _registro(lineas, "~D|09.01.02|") == (
        "~D|09.01.02|MO0010\\1\\1\\09.01.02.P1\\1\\1\\|"
    )


def test_f002_r17_todo_descompuesto_reescrito_acaba_en_barra_y_sus_codigos_existen(tmp_path):
    """La exigencia es sobre los `~D` REESCRITOS.

    Un `~D` que la pasada deja intacto puede arrastrar referencias rotas de la
    entrada —`f002_sin_precio.bc3` trae una a propósito, es lo que dispara
    R16— y arreglarlas no es cosa de esta feature (R18).
    """
    for fixture in sorted(FIXTURES.glob("f002_*.bc3")):
        lineas, _, _ = _convertir(tmp_path, fixture.name)
        entrada = set(leer(fixture))
        codigos = {l.split("|")[1] for l in lineas if l.startswith("~C|")}
        descompuestos = [l for l in lineas if l.startswith("~D|")]
        assert descompuestos
        for linea in descompuestos:
            assert linea.endswith("\\|"), linea
            assert not linea.endswith("\\\\|"), linea
            if linea in entrada:
                continue  # no reescrito
            partes = linea.split("|")[2].split("\\")
            for i in range(0, len(partes) - 2, 3):
                assert partes[i] in codigos, f"{fixture.name}: falta ~C de {partes[i]}"


def test_f002_r18_la_salida_conserva_el_salto_de_linea_de_la_entrada(tmp_path):
    """Un BC3 con saltos LF no se devuelve convertido a CRLF, ni al revés."""
    crlf = (FIXTURES / "f002_negativo.bc3").read_bytes()
    entrada = tmp_path / "lf.bc3"
    entrada.write_bytes(crlf.replace(b"\r\n", b"\n"))
    salida = tmp_path / "salida_lf.bc3"
    convertir_porcentuales(entrada, salida)
    crudo = salida.read_bytes()
    assert b"\r\n" not in crudo
    assert b"~C|05.06.29.P1|UD|" in crudo


def test_f002_r18_el_texto_largo_de_un_concepto_vivo_no_se_pierde(tmp_path):
    """Un `~T` ocupa varias líneas físicas: solo se borran las del que se elimina."""
    entrada = tmp_path / "textos.bc3"
    entrada.write_bytes(
        ("una línea suelta antes del primer registro\r\n"
         "~V|RIB Spain|FIEBDC-3/2016|Presto 19.02||ANSI||2||||\r\n"
         "~C|09.13.01|m2|PARTIDA|10.3|200220|0|\r\n"
         "~T|09.13.01|Texto largo de la partida\r\n"
         "que sigue en otra línea física\r\n"
         "~C|MO0010|h|Oficial|10|200220|1|\r\n"
         "~C|%RF||RECARGO FIJO|3|200220|0|\r\n"
         "~T|%RF|Texto largo del recargo\r\n"
         "que también sigue en otra línea\r\n"
         "~D|09.13.01|MO0010\\1\\1\\%RF\\1\\0.03\\|\r\n").encode("latin-1")
    )
    salida = tmp_path / "salida.bc3"
    convertir_porcentuales(entrada, salida)
    lineas = leer(salida)
    # Lo que va antes del primer registro tampoco se pierde.
    assert lineas[0] == "una línea suelta antes del primer registro"
    assert "~T|09.13.01|Texto largo de la partida" in lineas
    assert "que sigue en otra línea física" in lineas
    # El del porcentual eliminado se va entero, continuación incluida (R13).
    assert not [l for l in lineas if l.startswith("~T|%RF|")]
    assert "que también sigue en otra línea" not in lineas


def test_f002_r18_un_fichero_vacio_sale_vacio(tmp_path):
    entrada = tmp_path / "vacio.bc3"
    entrada.write_bytes(b"")
    salida = tmp_path / "salida_vacio.bc3"
    informe = convertir_porcentuales(entrada, salida)
    assert salida.read_bytes() == b""
    assert informe.descompuestos == 0


def test_f002_r15_los_registros_m_ajenos_a_la_conversion_no_se_tocan(tmp_path):
    """Solo se reescribe el ~M del par convertido; los demás viajan igual."""
    entrada = tmp_path / "mediciones.bc3"
    entrada.write_bytes(
        ("~V|RIB Spain|FIEBDC-3/2016|Presto 19.02||ANSI||2||||\r\n"
         "~C|09.03.01|m2|PARTIDA|10.3|200220|0|\r\n"
         "~C|MO0010|h|Oficial|10|200220|1|\r\n"
         "~C|%RF||RECARGO FIJO|3|200220|0|\r\n"
         "~D|09.03.01|MO0010\\1\\1\\%RF\\1\\0.03\\|\r\n"
         "~M|09.03.01|2\\1\\|4901|\\medicion del padre\\1\\4901\\\\\\1\\\\\\\\\\\\|\r\n"
         "~M|09.03.01\\MO0010|32\\4\\1\\2\\|66||\r\n"
         "~M|09.03.01\\%RF|32\\4\\1\\3\\|66||\r\n").encode("latin-1")
    )
    salida = tmp_path / "salida.bc3"
    convertir_porcentuales(entrada, salida)
    lineas = leer(salida)
    assert "~M|09.03.01|2\\1\\|4901|\\medicion del padre\\1\\4901\\\\\\1\\\\\\\\\\\\|" in lineas
    assert "~M|09.03.01\\MO0010|32\\4\\1\\2\\|66||" in lineas
    assert "~M|09.03.01\\09.03.01.P1|32\\4\\1\\3\\|66||" in lineas


def test_f002_r6_la_base_acumula_el_importe_YA_redondeado(tmp_path):
    """Con la base sin redondear el segundo clon saldría 56,17 en vez de 56,18.

    Es la diferencia entre un fichero coherente consigo mismo —quien lo recalcule
    desde los precios escritos obtiene lo mismo— y uno que no lo es.
    """
    entrada = tmp_path / "acumulado.bc3"
    entrada.write_bytes(
        ("~V|RIB Spain|FIEBDC-3/2016|Presto 19.02||ANSI||2||||\r\n"
         "~C|09.15.01|ud|PARTIDA|168.53|200220|0|\r\n"
         "~C|BASE|ud|Material|100|200220|3|\r\n"
         "~C|%UNO||PRIMER RECARGO|12.345|200220|0|\r\n"
         "~C|%DOS||SEGUNDO RECARGO|50|200220|0|\r\n"
         "~D|09.15.01|BASE\\1\\1\\%UNO\\1\\0.12345\\%DOS\\1\\0.5\\|\r\n").encode("latin-1")
    )
    salida = tmp_path / "salida.bc3"
    convertir_porcentuales(entrada, salida)
    lineas = leer(salida)
    assert _registro(lineas, "~C|09.15.01.P1|").split("|")[4] == "12.35"
    assert _registro(lineas, "~C|09.15.01.P2|").split("|")[4] == "56.18"


def test_f002_r9_el_rendimiento_cero_de_la_primera_linea_se_anota_como_cero(tmp_path):
    """R9 con la primera porcentual a rendimiento 0: el clon cobra igual el
    precio del padre, y el informe anota el rendimiento tal cual era."""
    entrada = tmp_path / "r9_cero.bc3"
    entrada.write_bytes(
        ("~V|RIB Spain|FIEBDC-3/2016|Presto 19.02||ANSI||2||||\r\n"
         "~C|09.14.01|ud|PARTIDA CON PRECIO A MANO|50|200220|0|\r\n"
         "~C|%CERO||RECARGO DESACTIVADO|0|200220|0|\r\n"
         "~C|%DIEZ||RECARGO DEL 10|10|200220|0|\r\n"
         "~D|09.14.01|%CERO\\1\\0\\%DIEZ\\1\\0.1\\|\r\n").encode("latin-1")
    )
    salida = tmp_path / "salida.bc3"
    informe = convertir_porcentuales(entrada, salida)
    lineas = leer(salida)
    assert _registro(lineas, "~C|09.14.01.P1|").split("|")[4] == "50.00"
    assert _registro(lineas, "~C|09.14.01.P2|").split("|")[4] == "5.00"
    casos = [c for c in informe.casos if c.motivo == "precio_del_padre_aplicado"]
    assert [(c.codigo, c.rendimiento, c.importe) for c in casos] == [
        ("%CERO", 0.0, 50.0)
    ]


def test_f002_r9_el_precio_del_padre_se_busca_con_la_marca_de_capitulo_y_sin_ella(tmp_path):
    """El `~C` del padre puede venir con el `#` puesto (`33.04.01#`)."""
    entrada = tmp_path / "padre_capitulo.bc3"
    entrada.write_bytes(
        ("~V|RIB Spain|FIEBDC-3/2016|Presto 19.02||ANSI||2||||\r\n"
         "~C|33.04.01#||CAPITULO CON PRECIO A MANO|100|200220|0|\r\n"
         "~C|%RF||RECARGO FIJO|3|200220|0|\r\n"
         "~D|33.04.01#|%RF\\1\\0.03\\|\r\n").encode("latin-1")
    )
    salida = tmp_path / "salida.bc3"
    informe = convertir_porcentuales(entrada, salida)
    assert _registro(leer(salida), "~C|33.04.01.P1|").split("|")[4] == "100.00"
    assert [c.padre for c in informe.casos
            if c.motivo == "precio_del_padre_aplicado"] == ["33.04.01#"]


def test_f002_r9_no_aplica_si_el_padre_no_tiene_precio(tmp_path):
    """Todas las líneas porcentuales pero el padre a 0: el clon vale 0."""
    entrada = tmp_path / "padre_sin_precio.bc3"
    entrada.write_bytes(
        ("~V|RIB Spain|FIEBDC-3/2016|Presto 19.02||ANSI||2||||\r\n"
         "~C|09.04.01|m2|PARTIDA SIN PRECIO|0|200220|0|\r\n"
         "~C|%RF||RECARGO FIJO|3|200220|0|\r\n"
         "~D|09.04.01|%RF\\1\\0.03\\|\r\n").encode("latin-1")
    )
    salida = tmp_path / "salida.bc3"
    informe = convertir_porcentuales(entrada, salida)
    assert _registro(leer(salida), "~C|09.04.01.P1|").split("|")[4] == "0"
    assert not [c for c in informe.casos if c.motivo == "precio_del_padre_aplicado"]


def test_f002_r18_las_lineas_no_afectadas_salen_identicas_byte_a_byte(tmp_path):
    origen = FIXTURES / "f002_negativo.bc3"
    destino = tmp_path / "salida.bc3"
    convertir_porcentuales(origen, destino)
    entrada = origen.read_bytes().decode("latin-1").splitlines(keepends=True)
    salida = destino.read_bytes().decode("latin-1").splitlines(keepends=True)
    # Solo cambia el ~D; el resto de líneas viajan tal cual, con sus CRLF.
    intactas = [l for l in entrada if not l.startswith(("~D|", "~C|%VID|"))]
    for linea in intactas:
        assert linea in salida
    assert all(l.endswith("\r\n") for l in salida)
    assert destino.read_bytes().decode("latin-1")  # sigue siendo latin-1 legible


# --------------------------------------------------------------------------- #
# R21 · La bandera de configuración                                            #
# --------------------------------------------------------------------------- #
def test_f002_r21_la_bandera_existe_y_viene_activada_por_defecto():
    assert Settings().porcentuales_a_ud is True


def test_f002_r21_con_la_bandera_apagada_el_fichero_sale_identico_byte_a_byte(tmp_path):
    ajustes = replace(Settings(), porcentuales_a_ud=False)
    origen = FIXTURES / "f002_cadena.bc3"
    # Carpeta que todavía no existe: la pasada la crea, como hace el ETL.
    destino = tmp_path / "entregas" / "copia.bc3"
    informe = convertir_porcentuales(origen, destino,
                                     activo=ajustes.porcentuales_a_ud)
    assert destino.read_bytes() == origen.read_bytes()
    assert informe.lineas_convertidas == 0
    assert informe.conceptos_eliminados == 0
    assert informe.casos == []


def test_f002_r23_si_no_existe_la_entrada_lanza_filenotfound_y_no_crea_la_salida(tmp_path):
    destino = tmp_path / "no_deberia_existir.bc3"
    with pytest.raises(FileNotFoundError):
        convertir_porcentuales(FIXTURES / "f002_no_existe.bc3", destino)
    assert not destino.exists()


# --------------------------------------------------------------------------- #
# R22 · Ejecución sobre un fichero suelto                                      #
# --------------------------------------------------------------------------- #
def test_f002_r22_el_cli_convierte_un_fichero_suelto_y_termina_en_cero(tmp_path):
    salida = tmp_path / "entregas" / "salida.bc3"
    informe = tmp_path / "informes" / "informe.csv"
    codigo = cli.main([str(FIXTURES / "f002_cadena.bc3"), str(salida),
                       "--informe", str(informe)])
    assert codigo == 0
    assert "~C|43.15.P1|UD|" in salida.read_bytes().decode("latin-1")
    assert informe.exists()


def test_f002_r22_el_cli_escribe_el_informe_para_excel(tmp_path):
    """R20 + convenio de Ruesma: UTF-8 con BOM, ';' y coma decimal."""
    informe = tmp_path / "informe.csv"
    cli.main([str(FIXTURES / "f002_solo_pct.bc3"), str(tmp_path / "s.bc3"),
              "--informe", str(informe)])
    crudo = informe.read_bytes()
    assert crudo.startswith(b"\xef\xbb\xbf")
    texto = crudo.decode("utf-8-sig")
    assert "lineas_convertidas;6" in texto
    assert "descompuestos_con_porcentual;4" in texto
    assert "padre;codigo;motivo;rendimiento;importe" in texto
    assert "31.04.03.01;%SUB2.5;precio_del_padre_aplicado;0,025;1100,0" in texto
    filas = texto.split("padre;codigo;motivo;rendimiento;importe")[1].strip()
    for fila in filas.splitlines():
        rendimiento, importe = fila.split(";")[3:5]
        assert "." not in rendimiento and "." not in importe
        assert "E" not in rendimiento.upper() and "E" not in importe.upper()


def test_f002_r22_el_cli_avisa_y_devuelve_uno_si_falta_la_entrada(tmp_path):
    codigo = cli.main([str(FIXTURES / "f002_no_existe.bc3"),
                       str(tmp_path / "s.bc3"),
                       "--informe", str(tmp_path / "i.csv")])
    assert codigo == 1
    assert not (tmp_path / "s.bc3").exists()
    assert not (tmp_path / "i.csv").exists()


def test_f002_r22_el_cli_puede_copiar_sin_convertir(tmp_path):
    salida = tmp_path / "salida.bc3"
    codigo = cli.main([str(FIXTURES / "f002_cadena.bc3"), str(salida),
                       "--informe", str(tmp_path / "i.csv"), "--sin-conversion"])
    assert codigo == 0
    assert salida.read_bytes() == (FIXTURES / "f002_cadena.bc3").read_bytes()
