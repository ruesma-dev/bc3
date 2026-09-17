# -*- coding: utf-8 -*-
"""BACKLOG.md es una proyección de features.json y tiene que estar al día.

Si estos tests fallan, no hay que tocar `BACKLOG.md`: se regenera con
`python harness/backlog.py` (o arrancando `bash harness/init.sh`).
"""

from __future__ import annotations

import io
import json
import os

from harness import backlog


def _leer_features() -> dict:
    with io.open(backlog.FEATURES, encoding="utf-8") as fh:
        return json.load(fh)


def test_backlog_md_existe_y_esta_al_dia():
    assert os.path.exists(backlog.SALIDA), (
        "falta BACKLOG.md: generalo con 'python harness/backlog.py'"
    )
    with io.open(backlog.SALIDA, encoding="utf-8") as fh:
        actual = fh.read()
    assert actual == backlog.construir(_leer_features()), (
        "BACKLOG.md no coincide con features.json: regeneralo con "
        "'python harness/backlog.py'"
    )


def test_la_generacion_es_determinista():
    datos = _leer_features()
    assert backlog.construir(datos) == backlog.construir(datos)


def test_estan_todas_las_features_con_su_estado_y_prioridad():
    datos = _leer_features()
    texto = backlog.construir(datos)
    for feature in datos["features"]:
        assert feature["id"] in texto, feature["id"]
        assert feature["title"].split("|")[0][:40] in texto, feature["id"]
    abiertas = [f for f in datos["features"] if f["status"] != "done"]
    assert "%d abiertas" % len(abiertas) in texto


def test_las_abiertas_salen_ordenadas_por_prioridad():
    datos = _leer_features()
    texto = backlog.construir(datos)
    bloque = texto.split("## Trabajo abierto", 1)[1].split("## Terminadas", 1)[0]
    ids_en_tabla = [
        linea.split("|")[1].strip()
        for linea in bloque.splitlines()
        if linea.startswith("| F-")
    ]
    prioridades = {f["id"]: f.get("priority", 999) for f in datos["features"]}
    orden = [prioridades[fid] for fid in ids_en_tabla]
    assert orden == sorted(orden), orden


def test_no_lleva_marca_de_tiempo():
    """Sin timestamp: el fichero solo cambia cuando cambia el backlog."""
    texto = backlog.construir(_leer_features())
    for palabra in ("generado el", "Generado el", "timestamp", "datetime"):
        assert palabra not in texto
