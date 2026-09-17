# tests/test_f002_invariante.py
"""F-002 · R19 · El importe de cada descompuesto no se mueve.

La comparación es ENTRADA contra SALIDA, `~D` a `~D`: nunca contra el precio
del `~C` del padre, porque hay padres con el precio puesto a mano que no cuadra
con su propio descompuesto (`170100`, `1701010`, `07.02.01a`; ver
`progress/explore_porcentuales.md` §2b) y eso haría fallar la suite por un dato
que ya venía torcido.

Quedan fuera los `~D` cuyas líneas son TODAS porcentuales: ahí R9 reconstruye
la base a propósito y el informe los lista uno a uno. A esos se les exige, en
cambio, R19 bis: el importe de su SALIDA vuelve a dar el precio del padre.

Sin red, sin BBDD y sin servicios de IA. Los ficheros de `input/` se leen en
modo lectura y la salida se escribe siempre en el `tmp_path` del test.
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from infrastructure.bc3.bc3_porcentajes import (
    DECIMALES_POR_DEFECTO,
    a_decimal,
    calcular_importes,
    convertir_porcentuales,
    es_porcentual,
    triples_de_cuerpo,
)

FIXTURES = Path(__file__).parent / "fixtures"
ENTRADAS = Path(__file__).resolve().parents[1] / "input"

# R9: los cuatro descompuestos solo-porcentuales reales de `input/`, con el
# precio `P` que su `~C` trae puesto a mano. Tras la conversión, el importe
# calculado sobre la SALIDA tiene que volver a dar exactamente ese `P`
# (R19 bis): el precio del padre YA lleva los porcentajes dentro.
SOLO_PORCENTUALES = {
    "31.04.03.01": Decimal("1100.00"),
    "32.03.04.32": Decimal("1117.65"),
    "ICV260": Decimal("291.50"),
    "ICV270": Decimal("369.50"),
}
FICHEROS_SOLO_PCT = {
    "COSTE_250128_Siroco_Rv4mlo.bc3": ("31.04.03.01", "32.03.04.32"),
    "lagunamodificado16julio.bc3": ("ICV260", "ICV270"),
}


def _campos(linea: str) -> list[str]:
    return linea.rstrip("\r\n").split("|")


def _lineas(ruta: Path) -> list[str]:
    return ruta.read_bytes().decode("latin-1").splitlines()


def _unidades_y_precios(lineas: list[str]):
    unidades: dict[str, str] = {}
    precios: dict[str, Decimal | None] = {}
    for linea in lineas:
        if not linea.startswith("~C|"):
            continue
        campos = _campos(linea)
        codigo = campos[1] if len(campos) > 1 else ""
        if not codigo:
            continue
        unidades[codigo] = campos[2] if len(campos) > 2 else ""
        precios[codigo] = a_decimal(campos[4]) if len(campos) > 4 else None
    return unidades, precios


def _descompuestos(lineas: list[str]) -> list[tuple[str, list[tuple[str, str, str]]]]:
    salida = []
    for linea in lineas:
        if not linea.startswith("~D|"):
            continue
        campos = _campos(linea)
        salida.append((campos[1], triples_de_cuerpo(campos[2] if len(campos) > 2 else "")))
    return salida


def _importe_total(triples, precios, es_pct) -> Decimal:
    return sum(calcular_importes(triples, precios, es_pct), Decimal(0))


def tolerancia_de(porcentuales: int, decimales: int) -> Decimal:
    """R19: `0,01 + nº de porcentuales × 10^(−d) / 2`.

    Con `d = 2` da la tolerancia original (0,005 por línea) y con `d = 4`
    aprieta cien veces más, que es de lo que sirve subir la precisión.
    """
    medio_paso = Decimal(1).scaleb(-decimales) / 2
    return Decimal("0.01") + medio_paso * porcentuales


def comparar_invariante(origen: Path, destino: Path,
                        decimales: int = DECIMALES_POR_DEFECTO) -> list[str]:
    """Devuelve la lista de `~D` que se salen de la tolerancia de R19."""
    entrada, salida = _lineas(origen), _lineas(destino)
    unidades_e, precios_e = _unidades_y_precios(entrada)
    unidades_s, precios_s = _unidades_y_precios(salida)

    def pct_entrada(codigo: str) -> bool:
        return es_porcentual(codigo, unidades_e.get(codigo, ""))

    def pct_salida(codigo: str) -> bool:
        return es_porcentual(codigo, unidades_s.get(codigo, ""))

    des_e, des_s = _descompuestos(entrada), _descompuestos(salida)
    assert len(des_e) == len(des_s), "la pasada no puede añadir ni quitar ~D"

    desviados: list[str] = []
    for (padre_e, triples_e), (padre_s, triples_s) in zip(des_e, des_s):
        assert padre_e == padre_s, f"~D descolocado: {padre_e} vs {padre_s}"
        porcentuales = [t for t in triples_e if pct_entrada(t[0])]
        if not porcentuales:
            continue
        if len(porcentuales) == len(triples_e):
            continue  # R9: cambia a propósito, va listado en el informe
        antes = _importe_total(triples_e, precios_e, pct_entrada)
        despues = _importe_total(triples_s, precios_s, pct_salida)
        tolerancia = tolerancia_de(len(porcentuales), decimales)
        if abs(antes - despues) > tolerancia:
            desviados.append(f"{padre_e}: {antes} -> {despues} (tol {tolerancia})")
    return desviados


# --------------------------------------------------------------------------- #
# R19 sobre las fixtures                                                       #
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("decimales", [2, 4])
@pytest.mark.parametrize("fixture", sorted(p.name for p in FIXTURES.glob("f002_*.bc3")))
def test_f002_r19_el_importe_de_cada_descompuesto_se_conserva(
    fixture, decimales, tmp_path
):
    origen = FIXTURES / fixture
    destino = tmp_path / f"{decimales}_{fixture}"
    convertir_porcentuales(origen, destino, decimales=decimales)
    assert comparar_invariante(origen, destino, decimales) == []


def test_f002_r19_los_descompuestos_solo_porcentuales_se_listan_en_el_informe(tmp_path):
    """R9 los cambia a propósito, así que el informe los tiene que nombrar."""
    informe = convertir_porcentuales(
        FIXTURES / "f002_solo_pct.bc3", tmp_path / "salida.bc3"
    )
    casos = {c.padre: c for c in informe.casos if c.motivo == "base_reconstruida"}
    assert set(casos) == set(SOLO_PORCENTUALES)
    bases = {"31.04.03.01": Decimal("1073.1707"),
             "32.03.04.32": Decimal("955.2564"),
             "ICV260": Decimal("225.9792"), "ICV270": Decimal("286.4471")}
    for padre, base in bases.items():
        assert Decimal(str(casos[padre].importe)) == base


def test_f002_r19_permutar_dos_tripletas_del_descompuesto_cambia_el_resultado():
    """El invariante no es una tautología: depende del ORDEN de las líneas."""
    precios = {"OPTIMIZADOR MPP": Decimal("41.18"), "%SUB25": Decimal("25")}

    def es_pct(codigo: str) -> bool:
        return es_porcentual(codigo, "")

    en_orden = [("OPTIMIZADOR MPP", "1", "1.2"), ("%SUB25", "1", "0.25")]
    permutado = list(reversed(en_orden))
    assert _importe_total(en_orden, precios, es_pct) == Decimal("61.77")
    assert _importe_total(permutado, precios, es_pct) == Decimal("49.416")


def test_f002_r19_permutar_las_tripletas_de_la_entrada_rompe_el_invariante(tmp_path):
    """Si la pasada ignorase el orden, este test no vería la diferencia."""
    origen = FIXTURES / "f002_cadena.bc3"
    texto = origen.read_bytes().decode("latin-1")
    permutado = texto.replace(
        "~D|43.15|OPTIMIZADOR MPP\\1\\1.2\\%SUB25\\1\\0.25\\",
        "~D|43.15|%SUB25\\1\\0.25\\OPTIMIZADOR MPP\\1\\1.2\\",
    )
    assert permutado != texto
    entrada = tmp_path / "permutado.bc3"
    entrada.write_bytes(permutado.encode("latin-1"))
    destino = tmp_path / "salida.bc3"
    convertir_porcentuales(entrada, destino)
    clon = next(l for l in _lineas(destino) if l.startswith("~C|43.15.P1|"))
    # Con el porcentual el primero su base es 0, no 49,416.
    assert _campos(clon)[4] == "0"


def test_f002_r19_la_pasada_es_idempotente(tmp_path):
    for fixture in sorted(FIXTURES.glob("f002_*.bc3")):
        primera = tmp_path / f"1_{fixture.name}"
        segunda = tmp_path / f"2_{fixture.name}"
        convertir_porcentuales(fixture, primera)
        informe = convertir_porcentuales(primera, segunda)
        assert primera.read_bytes() == segunda.read_bytes(), fixture.name
        assert informe.lineas_convertidas == 0 or fixture.name == "f002_sin_precio.bc3"


# --------------------------------------------------------------------------- #
# R19 sobre los BC3 reales de input/ (T9)                                      #
# --------------------------------------------------------------------------- #
BC3_DE_INPUT = sorted(ENTRADAS.glob("*.bc3")) if ENTRADAS.is_dir() else []


@pytest.mark.skipif(not BC3_DE_INPUT, reason="no hay BC3 en input/")
@pytest.mark.parametrize("decimales", [2, 4])
@pytest.mark.parametrize("nombre", [p.name for p in BC3_DE_INPUT])
def test_f002_r19_invariante_sobre_los_bc3_de_input(nombre, decimales, tmp_path):
    origen = ENTRADAS / nombre
    destino = tmp_path / f"{decimales}_{nombre}"
    antes = origen.read_bytes()
    convertir_porcentuales(origen, destino, decimales=decimales)
    assert comparar_invariante(origen, destino, decimales) == []
    # `input/` es de solo lectura: la pasada no puede haber escrito en él.
    assert origen.read_bytes() == antes


@pytest.mark.skipif(not BC3_DE_INPUT, reason="no hay BC3 en input/")
@pytest.mark.parametrize("nombre", [p.name for p in BC3_DE_INPUT])
def test_f002_r17_en_input_los_codigos_de_los_d_reescritos_existen(nombre, tmp_path):
    origen = ENTRADAS / nombre
    destino = tmp_path / nombre
    informe = convertir_porcentuales(origen, destino)
    entrada = set(_lineas(origen))
    salida = _lineas(destino)
    codigos = {_campos(l)[1] for l in salida if l.startswith("~C|")}
    reescritos = [l for l in salida if l.startswith("~D|") and l not in entrada]
    if not informe.lineas_convertidas:
        # `presupuesto.bc3` es un banco de precios: tiene conceptos '%' pero
        # ningún ~D los usa, así que no hay nada que reescribir.
        assert reescritos == []
        return
    assert reescritos, f"{nombre}: la pasada no reescribió ningún ~D"
    for linea in reescritos:
        # R17 se le exige a los ~D REESCRITOS: en input/ hay ~D que ya vienen
        # sin el '\\|' final (los ficheros '_limpio' de pasadas anteriores) y
        # esta feature no los arregla (R18).
        assert linea.endswith("\\|"), linea
        partes = _campos(linea)[2].split("\\")
        for i in range(0, len(partes) - 2, 3):
            assert partes[i] in codigos, f"{nombre}: falta ~C de {partes[i]}"
            assert not partes[i].startswith("%"), f"{nombre}: queda % en {linea}"


@pytest.mark.skipif(not BC3_DE_INPUT, reason="no hay BC3 en input/")
def test_f002_r9_los_cuatro_casos_reales_de_input_reconstruyen_su_base(tmp_path):
    """Siroco aporta dos y lagunamodificado16julio los otros dos."""
    esperado = {
        "COSTE_250128_Siroco_Rv4mlo.bc3": {"31.04.03.01", "32.03.04.32"},
        "lagunamodificado16julio.bc3": {"ICV260", "ICV270"},
    }
    for nombre, padres in esperado.items():
        origen = ENTRADAS / nombre
        if not origen.exists():
            pytest.skip(f"falta {nombre} en input/")
        informe = convertir_porcentuales(origen, tmp_path / nombre)
        casos = {c.padre for c in informe.casos if c.motivo == "base_reconstruida"}
        assert padres <= casos, f"{nombre}: faltan {padres - casos}"
        salida = _lineas(tmp_path / nombre)
        for padre in padres:
            # La familia entera: la línea de base .P0 y al menos una porcentual.
            base = [l for l in salida if l.startswith(f"~C|{padre}.P0|")]
            assert base, f"{nombre}: no hay línea de base de {padre}"
            assert [l for l in salida if l.startswith(f"~C|{padre}.P1|")]


@pytest.mark.skipif(not BC3_DE_INPUT, reason="no hay BC3 en input/")
@pytest.mark.parametrize("decimales", [2, 4])
@pytest.mark.parametrize("nombre", sorted(FICHEROS_SOLO_PCT))
def test_f002_r19bis_los_solo_porcentuales_de_input_suman_el_precio_del_padre(
    nombre, decimales, tmp_path
):
    """R19 bis · lo único que caza el defecto de la R9 anterior.

    Un `~D` solo-porcentual no se puede comparar entrada-contra-salida: su
    entrada calcula 0. Lo que sí se puede exigir es que el importe calculado
    sobre la SALIDA vuelva a dar el precio `P` del `~C` del padre, porque ese
    precio ya trae los porcentajes dentro. Con la regla vieja `ICV260` salía
    336,39 frente a 291,50: el beneficio cobrado dos veces.
    """
    origen = ENTRADAS / nombre
    if not origen.exists():
        pytest.skip(f"falta {nombre} en input/")
    destino = tmp_path / f"{decimales}_{nombre}"
    convertir_porcentuales(origen, destino, decimales=decimales)

    salida = _lineas(destino)
    unidades, precios = _unidades_y_precios(salida)

    def es_pct(codigo: str) -> bool:
        return es_porcentual(codigo, unidades.get(codigo, ""))

    por_padre = dict(_descompuestos(salida))
    desviados = []
    for padre in FICHEROS_SOLO_PCT[nombre]:
        esperado = SOLO_PORCENTUALES[padre]
        calculado = _importe_total(por_padre[padre], precios, es_pct)
        if abs(calculado - esperado) > Decimal("0.01"):
            desviados.append(f"{padre}: {calculado} != {esperado}")
    assert desviados == []
