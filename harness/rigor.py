# harness/rigor.py
"""Niveles de rigor del arnés: qué se le exige a cada feature.

No todas las features merecen la misma vigilancia. Una que solo toca
documentación no puede requerir mutación; una que toca infraestructura
compartida en producción debe exigir más que la media. El nivel se declara en
el campo `rigor` de cada entrada de `harness/features.json` y lo que exige
cada nivel vive en `harness/rigor.json`.

Regla dura: si una feature no declara nivel, se le aplica el
`nivel_por_defecto` de `harness/rigor.json`. Ese nivel exige evidencia real
(fase RED, cobertura y mutación), así que omitir el campo no es la vía fácil
para saltarse las puertas; lo que no hace es arrastrar el modo más caro —cero
supervivientes tolerados— a features que no lo necesitan. `critico` se
declara: no se hereda por descuido.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

#: Puertas que un nivel puede exigir o no.
PUERTAS: tuple[str, ...] = ("fase_red", "cobertura", "mutacion")

RUTA_RIGOR = Path("harness/rigor.json")
RUTA_FEATURES = Path("harness/features.json")


def cargar_rigor(ruta: Path | str = RUTA_RIGOR) -> dict:
    """Carga y valida la configuración de niveles.

    Cualquier incoherencia es un `ValueError` con mensaje explícito: es
    preferible parar el arnés a trabajar con una configuración que nadie
    entiende.
    """
    ruta = Path(ruta)
    if not ruta.is_file():
        raise ValueError(f"No existe la configuración de rigor: {ruta.as_posix()}")
    try:
        datos = json.loads(ruta.read_text(encoding="utf-8"))
    except ValueError as error:
        raise ValueError(f"{ruta.as_posix()} no es JSON válido: {error}") from error

    if not isinstance(datos, dict):
        raise ValueError(f"{ruta.as_posix()} debe contener un objeto JSON")

    niveles = datos.get("niveles")
    if not isinstance(niveles, dict) or not niveles:
        raise ValueError(f"{ruta.as_posix()}: falta el objeto 'niveles'")

    for nombre, definicion in niveles.items():
        if not isinstance(definicion, dict):
            raise ValueError(f"{ruta.as_posix()}: el nivel '{nombre}' no es un objeto")
        for puerta in PUERTAS:
            if not isinstance(definicion.get(puerta), bool):
                raise ValueError(
                    f"{ruta.as_posix()}: el nivel '{nombre}' debe declarar "
                    f"'{puerta}' como true o false"
                )

    por_defecto = datos.get("nivel_por_defecto")
    if por_defecto not in niveles:
        raise ValueError(
            f"{ruta.as_posix()}: 'nivel_por_defecto' debe ser uno de "
            f"{sorted(niveles)}, y es {por_defecto!r}"
        )

    return datos


def niveles_validos(rigor: dict) -> list[str]:
    return sorted(rigor["niveles"])


def nivel_de_feature(feature: dict, rigor: dict) -> str:
    """Nivel declarado por la feature; si falta o no vale, el nivel por defecto."""
    declarado = feature.get("rigor")
    if isinstance(declarado, str) and declarado in rigor["niveles"]:
        return declarado
    return rigor["nivel_por_defecto"]


def exige(nivel: str, puerta: str, rigor: dict) -> bool:
    """¿El nivel exige esa puerta? Lo desconocido se considera exigido."""
    definicion = rigor["niveles"].get(nivel)
    if definicion is None:
        return True
    valor = definicion.get(puerta)
    if not isinstance(valor, bool):
        return True
    return valor


def supervivientes_maximos(nivel: str, rigor: dict) -> int | None:
    """Supervivientes admitidos sin justificación; `None` = los juzga el reviewer."""
    definicion = rigor["niveles"].get(nivel, {})
    valor = definicion.get("supervivientes_maximos")
    return valor if isinstance(valor, int) else None


def umbral_cobertura(rigor: dict) -> int:
    """Porcentaje mínimo de cobertura de las líneas cambiadas."""
    valor = rigor.get("cobertura", {}).get("umbral_lineas_cambiadas")
    if not isinstance(valor, int):
        raise ValueError(
            "Falta 'cobertura.umbral_lineas_cambiadas' en la configuración de "
            "rigor: el umbral vive en el fichero, no en el código."
        )
    return valor


def timeout_mutacion(rigor: dict) -> int:
    """SUELO de segundos que se le conceden a la suite por cada mutante.

    Suelo y no techo: `harness.mutacion` deriva el timeout efectivo de la línea
    base que mide antes de juzgar a nadie, y nunca concede menos que este valor.

    «Falta la clave» y «el valor no vale» son dos averías distintas y llevan
    mensajes distintos: cuando las dos decían «falta», quien leía el aviso se
    ponía a buscar una clave que tenía delante. Y un booleano NO es un timeout,
    aunque `isinstance(True, int)` diga que sí: con `true` la campaña concedía
    **1 segundo** por mutante y salía entera en «timeout».
    """
    bloque = rigor.get("mutacion", {})
    valor = bloque.get("timeout_por_mutante_s") if isinstance(bloque, dict) else None
    if valor is None:
        raise ValueError(
            "Falta 'mutacion.timeout_por_mutante_s' en la configuración de rigor."
        )
    if isinstance(valor, bool) or not isinstance(valor, int) or valor <= 0:
        raise ValueError(
            f"'mutacion.timeout_por_mutante_s' no vale: {valor!r}. Tiene que ser "
            "un entero estrictamente positivo (un booleano NO cuenta como "
            "entero aquí: 'true' daría 1 segundo por mutante)."
        )
    return valor


def workers_mutacion(rigor: dict) -> int | None:
    """Evaluadores concurrentes de la campaña de mutación, si se declaran.

    Al revés que el timeout, esta clave es OPCIONAL y su ausencia no es un
    error: `None` significa «decídelo tú», y quien llama aplica su valor por
    núcleos de la máquina. Cablear aquí un número lo haría viajar de máquina en
    máquina, que es justo lo que no queremos. Un valor absurdo (0, negativo, un
    texto) se trata como ausencia: mejor el default que una campaña rara.
    """
    valor = rigor.get("mutacion", {}).get("workers")
    if isinstance(valor, bool) or not isinstance(valor, int) or valor < 1:
        return None
    return valor


def max_mutantes_nivel(nivel: str, rigor: dict) -> int | None:
    """Mutantes que se evalúan como mucho en ese nivel; `None` = sin tope.

    Clave OPCIONAL, como `workers_mutacion`: ausencia, `null`, un booleano o un
    entero absurdo (cero o negativo) se tratan como «sin tope». Un `rigor.json`
    anterior a esta clave sigue funcionando y mide la campaña entera, que es lo
    que hacía.
    """
    valor = rigor.get("niveles", {}).get(nivel, {}).get("max_mutantes")
    if isinstance(valor, bool) or not isinstance(valor, int) or valor < 1:
        return None
    return valor


def semilla_nivel(nivel: str, rigor: dict) -> int | None:
    """Semilla del muestreo de ese nivel; `None` = la que decida quien llama.

    Fijarla en el fichero es lo que hace REPRODUCIBLE una campaña muestreada:
    dos reviewers que remidan la misma feature obtienen los mismos mutantes. A
    diferencia del tope, aquí el `0` es una semilla legítima.
    """
    valor = rigor.get("niveles", {}).get(nivel, {}).get("semilla")
    if isinstance(valor, bool) or not isinstance(valor, int):
        return None
    return valor


#: Ficheros de papeleo que tienen tope de líneas, con su clave en `rigor.json`.
CLAVES_TAMANO: tuple[str, ...] = ("requirements", "design", "impl", "review")


def topes_tamano(rigor: dict) -> dict[str, int]:
    """Topes de líneas del papeleo de una feature, o `{}` si no se declaran.

    Bloque OPCIONAL: sin él, la puerta de tamaño se declara N/A con su motivo
    en vez de romper el arnés de un proyecto con configuración anterior. Los
    valores que no son un entero positivo se descartan uno a uno —incluido el
    `$doc` del propio bloque—: un tope de cero prohibiría escribir.
    """
    bloque = rigor.get("tamano")
    if not isinstance(bloque, dict):
        return {}
    topes: dict[str, int] = {}
    for clave in CLAVES_TAMANO:
        valor = bloque.get(clave)
        if isinstance(valor, bool) or not isinstance(valor, int) or valor < 1:
            continue
        topes[clave] = valor
    return topes


# --- Inventario de features -------------------------------------------------


def cargar_features(ruta: Path | str = RUTA_FEATURES) -> list[dict]:
    ruta = Path(ruta)
    if not ruta.is_file():
        return []
    datos = json.loads(ruta.read_text(encoding="utf-8"))
    features = datos.get("features", [])
    return features if isinstance(features, list) else []


def validar_features(features: list[dict], rigor: dict) -> list[str]:
    """Devuelve los errores de los niveles declarados (lista vacía = todo bien).

    No declarar nivel NO es un error: se aplica el nivel por defecto. Declarar
    uno inexistente sí lo es: sería un rigor imaginario.
    """
    errores: list[str] = []
    for feature in features:
        if "rigor" not in feature or feature["rigor"] is None:
            continue
        declarado = feature["rigor"]
        if not isinstance(declarado, str) or declarado not in rigor["niveles"]:
            errores.append(
                f"{feature.get('id', '(sin id)')}: rigor {declarado!r} no válido; "
                f"usa uno de {niveles_validos(rigor)}"
            )
    return errores


def feature_de_rama(rama: str, features: list[dict]) -> dict | None:
    """Feature cuya rama declarada es `rama`; si no hay, la que esté en curso."""
    if rama:
        for feature in features:
            if feature.get("branch") == rama:
                return feature
    for feature in features:
        if feature.get("status") == "in_progress":
            return feature
    return None


# --- CLI --------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    """`python -m harness.rigor --validar`: 0 si todo cuadra, 1 si no."""
    analizador = argparse.ArgumentParser(
        prog="python -m harness.rigor",
        description="Valida la configuración de niveles de rigor del arnés.",
    )
    analizador.add_argument("--validar", action="store_true")
    analizador.add_argument("--config", default=str(RUTA_RIGOR))
    analizador.add_argument("--features", default=str(RUTA_FEATURES))
    opciones = analizador.parse_args(argv)

    try:
        rigor = cargar_rigor(opciones.config)
    except ValueError as error:
        print(str(error), file=sys.stderr)
        return 1

    errores = validar_features(cargar_features(opciones.features), rigor)
    if errores:
        for error in errores:
            print(error, file=sys.stderr)
        return 1

    print(
        f"    niveles: {', '.join(niveles_validos(rigor))}; por defecto "
        f"{rigor['nivel_por_defecto']}; umbral de cobertura "
        f"{umbral_cobertura(rigor)}%"
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
