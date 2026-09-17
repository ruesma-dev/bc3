# -*- coding: utf-8 -*-
"""Proyecta `harness/features.json` en un `BACKLOG.md` legible.

`features.json` es la fuente de verdad del backlog, pero es JSON: nadie lo
lee de un vistazo y el humano acaba pidiendo por chat "dame la tabla de
features". Este generador escribe esa tabla en el repositorio, para que esté
siempre disponible sin preguntar y para que el propio historial de git
enseñe cómo evolucionó el backlog.

REGLAS DE DISEÑO (importan si lo tocas):

- `BACKLOG.md` es **generado**: nunca se edita a mano. Lo que se edita es
  `features.json`.
- La salida es **función pura de `features.json`**: no lleva fecha de
  generación ni mira el árbol de trabajo. Así el fichero solo cambia cuando
  cambia el backlog de verdad, y arrancar el portero no ensucia `git status`.
- No inventa estados ni fases: lo que no está en `features.json`, no sale.

Uso:

    python harness/backlog.py              # regenera BACKLOG.md
    python harness/backlog.py --comprobar  # no escribe; código 1 si difiere

`harness/init.sh` lo llama en cada arranque y avisa si el fichero cambió.
"""

from __future__ import annotations

import argparse
import io
import json
import os
import sys

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FEATURES = os.path.join(RAIZ, "harness", "features.json")
SALIDA = os.path.join(RAIZ, "BACKLOG.md")

_ETIQUETA_ESTADO = {
    "blocked": "bloqueada",
    "in_progress": "en curso",
    "spec_ready": "spec lista",
    "pending": "pendiente",
    "done": "terminada",
}


def _celda(texto: object) -> str:
    """Escapa lo que rompería una tabla Markdown."""
    if texto is None:
        return ""
    return str(texto).replace("|", "\\|").replace("\n", " ").strip()


def _clave_orden(feature: dict) -> tuple:
    prioridad = feature.get("priority")
    if not isinstance(prioridad, int):
        prioridad = 999
    return (prioridad, feature.get("id", ""))


def _tabla(features: list[dict], con_estado: bool = True) -> list[str]:
    if not con_estado:
        cabecera = "| # | Feature | Prioridad | Rigor |"
        separador = "|---|---|---|---|"
    else:
        cabecera = "| # | Feature | Prioridad | Estado | Rigor | Rama |"
        separador = "|---|---|---|---|---|---|"
    filas = [cabecera, separador]
    for f in features:
        if con_estado:
            filas.append(
                "| %s | %s | %s | %s | %s | %s |" % (
                    _celda(f.get("id")),
                    _celda(f.get("title")),
                    _celda(f.get("priority")),
                    _ETIQUETA_ESTADO.get(f.get("status"), _celda(f.get("status"))),
                    _celda(f.get("rigor")),
                    "`%s`" % _celda(f["branch"]) if f.get("branch") else "",
                )
            )
        else:
            filas.append(
                "| %s | %s | %s | %s |" % (
                    _celda(f.get("id")),
                    _celda(f.get("title")),
                    _celda(f.get("priority")),
                    _celda(f.get("rigor")),
                )
            )
    return filas


def construir(datos: dict) -> str:
    features = datos.get("features", [])
    # Las abiertas se listan por PRIORIDAD, que es como las lee el humano.
    # El orden por estado (blocked > in_progress > spec_ready > pending) lo
    # usa el líder para elegir la siguiente tarea, no esta tabla; aquí el
    # estado es una columna más.
    abiertas = sorted(
        (f for f in features if f.get("status") != "done"), key=_clave_orden
    )
    hechas = sorted(
        (f for f in features if f.get("status") == "done"), key=_clave_orden
    )

    lineas: list[str] = [
        "<!-- BACKLOG.md -->",
        "# Backlog",
        "",
        "**Fichero generado por `harness/backlog.py` a partir de "
        "`harness/features.json`. No lo edites a mano**: edita el JSON y "
        "vuelve a generarlo (lo hace solo `bash harness/init.sh`).",
        "",
        "Resumen: **%d features**, %d abiertas, %d terminadas."
        % (len(features), len(abiertas), len(hechas)),
        "",
    ]

    en_curso = [f["id"] for f in features if f.get("status") == "in_progress"]
    bloqueadas = [f["id"] for f in features if f.get("status") == "blocked"]
    if en_curso:
        lineas += ["En curso: **%s**." % ", ".join(en_curso), ""]
    if bloqueadas:
        lineas += ["Bloqueadas: **%s**." % ", ".join(bloqueadas), ""]

    lineas += ["## Trabajo abierto", ""]
    if abiertas:
        lineas += _tabla(abiertas)
    else:
        lineas.append("_No queda trabajo abierto._")
    lineas.append("")

    lineas += ["## Terminadas", ""]
    if hechas:
        lineas += _tabla(hechas, con_estado=False)
    else:
        lineas.append("_Todavía no hay features terminadas._")
    lineas.append("")

    lineas += ["## Detalle", ""]
    for f in abiertas + hechas:
        lineas.append("### %s · %s" % (_celda(f.get("id")), _celda(f.get("title"))))
        lineas.append("")
        meta = [
            "estado **%s**" % _ETIQUETA_ESTADO.get(f.get("status"), f.get("status")),
        ]
        if f.get("priority") is not None:
            meta.append("prioridad %s" % f["priority"])
        if f.get("rigor"):
            meta.append("rigor `%s`" % f["rigor"])
        if f.get("sdd") is not None:
            meta.append("SDD %s" % ("sí" if f["sdd"] else "no"))
        if f.get("branch"):
            meta.append("rama `%s`" % f["branch"])
        lineas.append(" · ".join(meta))
        lineas.append("")
        descripcion = (f.get("description") or "").strip()
        lineas.append(descripcion if descripcion else "_Sin descripción._")
        lineas.append("")

    return "\n".join(lineas).rstrip() + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Genera BACKLOG.md desde harness/features.json."
    )
    parser.add_argument(
        "--comprobar",
        action="store_true",
        help="no escribe; devuelve 1 si BACKLOG.md no está al día",
    )
    args = parser.parse_args(argv)

    try:
        with io.open(FEATURES, encoding="utf-8") as fh:
            datos = json.load(fh)
    except FileNotFoundError:
        print("harness/features.json no existe: nada que generar", file=sys.stderr)
        return 1
    except json.JSONDecodeError as exc:
        print("harness/features.json inválido: %s" % exc, file=sys.stderr)
        return 1

    nuevo = construir(datos)
    actual = None
    if os.path.exists(SALIDA):
        with io.open(SALIDA, encoding="utf-8") as fh:
            actual = fh.read()

    if actual == nuevo:
        return 0

    if args.comprobar:
        print("BACKLOG.md no está al día (regenera con: python harness/backlog.py)")
        return 1

    with io.open(SALIDA, "w", encoding="utf-8", newline="\n") as fh:
        fh.write(nuevo)
    print("BACKLOG.md regenerado")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
