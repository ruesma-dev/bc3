# harness/tamano.py
"""Puerta de TAMAÑO del papeleo de la feature en curso.

Cada línea de una spec se paga tres veces: la escribe el spec-author, la lee el
implementer y la relee el reviewer. El arnés no decía nada del tamaño y por eso
los agentes escribían cuanto se les ocurría —978 líneas de spec para arreglar un
script—. Aquí se mide lo que cuesta ese papeleo, contra los topes que declara
`harness/rigor.json`: en el código no hay ningún número que tocar.

Dos decisiones que explican la forma de esta herramienta:

1. **Solo se mide la feature en curso.** Las specs escritas antes de que
   existieran estos topes los exceden hoy (de 114 a 333 líneas en
   `requirements.md`), y medirlas dejaría
   el portero en rojo permanente o exigiría una lista de excepciones que
   mantener. Lo viejo queda amnistiado por construcción; una spec vieja que se
   retome y se edite pasará a medirse, que es el resultado deseado.
2. **Solo se miden los ficheros que existen.** El informe de review no existe
   mientras el implementer trabaja, y exigirlo aquí sería una puerta que se
   cierra sola.

Los topes son topes, no objetivos: lo que no cabe se resume y se enlaza al
fichero donde vive el detalle.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path

from harness.rigor import RUTA_FEATURES, RUTA_RIGOR, cargar_rigor, topes_tamano

ETIQUETA = "PUERTA TAMAÑO"

#: Qué fichero mide cada clave de topes de `harness/rigor.json`. Las dos
#: primeras necesitan el slug de la feature; las dos últimas, solo su id.
RUTAS: dict[str, str] = {
    "requirements": "specs/{slug}/requirements.md",
    "design": "specs/{slug}/design.md",
    "impl": "progress/impl_{feature}.md",
    "review": "progress/review_{feature}.md",
}


@dataclass(frozen=True)
class Exceso:
    """Un fichero de papeleo que se pasa de su tope."""

    clave: str
    ruta: str
    lineas: int
    tope: int

    def descripcion(self) -> str:
        return f"{self.ruta}: {self.lineas} líneas > tope {self.tope}"


def contar_lineas(ruta: Path) -> int:
    """Líneas del fichero, contadas como las cuenta cualquiera al abrirlo."""
    return len(ruta.read_text(encoding="utf-8", errors="replace").splitlines())


def slug_de_feature(feature: str, raiz: str = ".") -> str | None:
    """Slug de la carpeta de spec de `feature` (`F-XXX-lo-que-sea`), o `None`.

    Primero la rama declarada en `features.json`, que es la fuente de verdad del
    arnés; si no la hay, el único directorio `specs/F-XXX-*`. Con dos
    directorios candidatos no se adivina: sin slug, los dos ficheros de spec
    simplemente no se miden.
    """
    base = Path(raiz)
    ruta_features = base / RUTA_FEATURES
    if ruta_features.is_file():
        try:
            datos = json.loads(ruta_features.read_text(encoding="utf-8"))
        except ValueError:
            datos = {}
        for ficha in datos.get("features", []) if isinstance(datos, dict) else []:
            if ficha.get("id") != feature:
                continue
            rama = str(ficha.get("branch") or "")
            if "/" in rama:
                return rama.split("/", 1)[1]

    candidatos = sorted(
        directorio.name
        for directorio in (base / "specs").glob(f"{feature}-*")
        if directorio.is_dir()
    )
    return candidatos[0] if len(candidatos) == 1 else None


def medir(
    feature: str, slug: str | None, topes: dict[str, int], raiz: str = "."
) -> list[Exceso]:
    """Ficheros de papeleo de `feature` que exceden su tope, en orden de `RUTAS`.

    Solo se miden los que existen y los que tienen tope declarado. Sin `slug`,
    los dos ficheros de spec se saltan: medir `specs/None/requirements.md` no
    mediría nada y decirlo como exceso sería mentira.
    """
    base = Path(raiz)
    excesos: list[Exceso] = []
    for clave, patron in RUTAS.items():
        tope = topes.get(clave)
        if tope is None:
            continue
        if "{slug}" in patron and not slug:
            continue
        relativa = patron.format(slug=slug, feature=feature)
        ruta = base / relativa
        if not ruta.is_file():
            continue
        lineas = contar_lineas(ruta)
        if lineas > tope:
            excesos.append(Exceso(clave=clave, ruta=relativa, lineas=lineas, tope=tope))
    return excesos


def medidos(
    feature: str, slug: str | None, topes: dict[str, int], raiz: str = "."
) -> list[str]:
    """Resumen `clave N/tope` de cada fichero medido, para el mensaje de la puerta."""
    base = Path(raiz)
    resumen: list[str] = []
    for clave, patron in RUTAS.items():
        tope = topes.get(clave)
        if tope is None or ("{slug}" in patron and not slug):
            continue
        ruta = base / patron.format(slug=slug, feature=feature)
        if ruta.is_file():
            resumen.append(f"{clave} {contar_lineas(ruta)}/{tope}")
    return resumen


# --- CLI --------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    """Puerta de tamaño: 0 dentro de los topes, 1 si alguno se pasa, 2 si no aplica.

    El 2 no es un fallo: es «no hay nada que medir aquí» (sin configuración de
    rigor, o sin bloque `tamano`). `harness/init.sh` lo traduce en un aviso con
    el motivo impreso, nunca en un rojo silencioso.
    """
    analizador = argparse.ArgumentParser(
        prog="python -m harness.tamano",
        description=(
            "Comprueba que el papeleo de una feature cabe en los topes de "
            "líneas declarados en harness/rigor.json."
        ),
    )
    analizador.add_argument(
        "--feature", required=True, help="Identificador de la feature, formato F-XXX"
    )
    analizador.add_argument("--raiz", default=".", help="Raíz del repositorio")
    opciones = analizador.parse_args(argv)

    try:
        rigor = cargar_rigor(Path(opciones.raiz) / RUTA_RIGOR)
    except ValueError as error:
        print(f"{ETIQUETA}: N/A ({error})")
        return 2

    topes = topes_tamano(rigor)
    if not topes:
        print(
            f"{ETIQUETA}: N/A (harness/rigor.json no declara el bloque 'tamano': "
            "sin topes que medir)"
        )
        return 2

    slug = slug_de_feature(opciones.feature, raiz=opciones.raiz)
    excesos = medir(opciones.feature, slug, topes, raiz=opciones.raiz)
    if excesos:
        print(f"{ETIQUETA}: {opciones.feature} se pasa de los topes:", file=sys.stderr)
        for exceso in excesos:
            print(f"    {exceso.descripcion()}", file=sys.stderr)
        print(
            "    Los topes son topes: resume lo que no cabe y enlaza al fichero "
            "donde vive el detalle.",
            file=sys.stderr,
        )
        return 1

    resumen = ", ".join(medidos(opciones.feature, slug, topes, raiz=opciones.raiz))
    print(
        f"{ETIQUETA}: {opciones.feature} dentro de los topes "
        f"({resumen or 'ningún fichero de papeleo todavía'})"
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
