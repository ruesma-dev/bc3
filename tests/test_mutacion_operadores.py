# tests/test_mutacion_operadores.py
"""El mutador tiene que mutar `is` / `is not`: la guarda de ausencia de Python.

`x is None` es LA forma de preguntar por la ausencia en Python, y era el punto
ciego del mutador: la tabla `COMPARACIONES` no la conocía. Es justo el patrón
de los dos defectos más caros vistos en `albaranes` —la red KG→TN que se
apagaba con un `derived_line is not None` y la precedencia del importe de línea
con un `importe_albaran_declarado is not None`—, así que sus campañas se
midieron ciegas en el sitio que más importaba.

Aquí se fija, además, el efecto colateral que aparece al mutar un operador
escrito con letras: `is` cabe dentro de cualquier palabra («análisis»), y el
hueco entre los dos operandos puede contener un comentario cuando la condición
va entre paréntesis. Sin delimitación de palabra, el mutante caía dentro del
comentario: equivalente por construcción, superviviente eterno y ruido en el
informe.

Todos los tests son puros: AST sobre fuentes en cadena y lectura de un fichero
del propio repositorio. Ni red, ni BBDD, ni subprocesos.
"""

from __future__ import annotations

import ast
import json

import pytest

from harness.mutacion import (
    _delimitado,
    _es_palabra,
    aplicar_mutante,
    clave_de_mutante,
    generar_mutantes,
)
from harness.rutas_sensibles import RUTA_DECLARACION

FICHERO = "modulo.py"

FUENTE_IS = (
    "def sin_valor(x):\n"
    "    if x is None:\n"
    "        return uno()\n"
    "    return dos()\n"
)

#: La guarda real de F-027 (`valuation_builder.py:1033`), palabra por palabra.
FUENTE_F027 = (
    "def construir(partida_result):\n"
    "    if partida_result.derived_line is not None:\n"
    "        return partida_result.derived_line\n"
    "    return None\n"
)

FUENTE_DOS_IS = (
    "def ambos(a, b):\n"
    "    if a is None or b is None:\n"
    "        return cero()\n"
    "    return uno()\n"
)

FUENTE_ENCADENADA = (
    "def encadenada(a, b, c):\n"
    "    if a is b is not c:\n"
    "        return cero()\n"
    "    return uno()\n"
)

#: El caso que destapa la falta de delimitación: «analisis» lleva dos veces la
#: secuencia `is` dentro, y está en el hueco entre los dos operandos.
FUENTE_COMENTARIO = (
    "def con_comentario(valor):\n"
    "    if (\n"
    "        valor  # el analisis previo\n"
    "        is None\n"
    "    ):\n"
    "        return uno()\n"
    "    return dos()\n"
)

#: El caso que destapa la falta de delimitación **por la derecha**: «isla»
#: EMPIEZA por `is`, así que el byte anterior no delata nada y lo único que
#: separa el comentario del operador de verdad es el byte SIGUIENTE.
FUENTE_COMENTARIO_ISLA = (
    "def con_comentario(valor):\n"
    "    if (\n"
    "        valor  # isla desierta\n"
    "        is None\n"
    "    ):\n"
    "        return uno()\n"
    "    return dos()\n"
)

FUENTE_SIMBOLOS = (
    "def sin_espacios(x, y, a, b):\n"
    "    if x==y:\n"
    "        return cero()\n"
    "    z=a+b\n"
    "    return z\n"
)

#: Espaciado que ningún formateador (ruff/black) deja pasar. Documentado como
#: límite conocido: no genera mutante y no puede reventar.
FUENTE_NO_CANONICA = (
    "def raro(x):\n"
    "    if x is  not None:\n"
    "        return uno()\n"
    "    return dos()\n"
)


def _mutantes(fuente: str) -> list:
    """Todos los mutantes de `fuente`, con el fichero entero en el alcance."""
    lineas = set(range(1, len(fuente.split("\n")) + 1))
    return generar_mutantes(fuente, lineas, FICHERO)


def _de_operador(fuente: str, operador: str) -> list:
    return [mutante for mutante in _mutantes(fuente) if mutante.operador == operador]


def _primera_comparacion(fuente: str) -> ast.Compare:
    return next(
        nodo for nodo in ast.walk(ast.parse(fuente)) if isinstance(nodo, ast.Compare)
    )


def test_f034_r1_muta_is_a_is_not():
    """R1: `x is None` produce el mutante `x is not None`."""
    mutantes = _de_operador(FUENTE_IS, "comparacion")

    assert len(mutantes) == 1, f"esperado 1 mutante de comparación, hay {len(mutantes)}"
    assert mutantes[0].linea == 2
    assert mutantes[0].original == "if x is None:"
    assert mutantes[0].mutado == "if x is not None:"


def test_f034_r2_muta_is_not_a_is():
    """R2: la guarda de F-027 produce el mutante que nadie generó en su día."""
    mutantes = _de_operador(FUENTE_F027, "comparacion")

    assert len(mutantes) == 1, f"esperado 1 mutante de comparación, hay {len(mutantes)}"
    assert mutantes[0].linea == 2
    assert mutantes[0].original == "if partida_result.derived_line is not None:"
    assert mutantes[0].mutado == "if partida_result.derived_line is None:"


def test_f034_r3_una_mutacion_por_operador_en_la_misma_linea():
    """R3: un mutante independiente por operador, unidos por `or` o encadenados."""
    unidos = _de_operador(FUENTE_DOS_IS, "comparacion")

    assert len(unidos) == 2, f"esperados 2 mutantes, hay {len(unidos)}"
    assert len({mutante.col for mutante in unidos}) == 2, "los dos en la misma columna"
    assert {mutante.mutado for mutante in unidos} == {
        "if a is not None or b is None:",
        "if a is None or b is not None:",
    }

    encadenados = _de_operador(FUENTE_ENCADENADA, "comparacion")

    assert len(encadenados) == 2, f"esperados 2 mutantes, hay {len(encadenados)}"
    assert {mutante.mutado for mutante in encadenados} == {
        "if a is not b is not c:",
        "if a is b is c:",
    }


def test_f034_r4_el_operador_declarado_es_comparacion():
    """R4: van etiquetados como `comparacion` y `clave_de_mutante` los distingue."""
    mutantes = _mutantes(FUENTE_IS)

    assert [mutante.operador for mutante in mutantes] == ["comparacion"]

    dos = _de_operador(FUENTE_DOS_IS, "comparacion")
    claves = {
        clave_de_mutante(
            mutante.fichero, mutante.operador, mutante.original, mutante.mutado
        )
        for mutante in dos
    }

    assert len(claves) == 2, (
        "los dos mutantes de la misma línea comparten clave: entre campañas se "
        "repondría el análisis del otro"
    )


def test_f034_r5_no_muta_dentro_de_una_palabra_del_comentario():
    """R5: la secuencia `is` de «analisis» no es un operador y no se muta."""
    mutantes = _de_operador(FUENTE_COMENTARIO, "comparacion")

    assert len(mutantes) == 1, f"esperado 1 mutante, hay {len(mutantes)}"
    assert mutantes[0].linea == 4, (
        f"el mutante cayó en la línea {mutantes[0].linea} "
        f"({mutantes[0].mutado!r}); el operador está en la 4"
    )
    assert mutantes[0].mutado == "is not None"


def test_f034_r5_bis_no_muta_una_palabra_que_EMPIEZA_por_is():
    """R5: «isla» empieza por `is` — quien lo salva es el delimitador DERECHO.

    El caso de «analisis» ya lo caza el byte ANTERIOR, así que por sí solo deja
    sin comprobar la mitad derecha de `_delimitado`. Una palabra que empieza por
    `is` obliga a mirar el byte que va DESPUÉS de la coincidencia: sin eso, el
    mutante vuelve a caer dentro del comentario.
    """
    mutantes = _de_operador(FUENTE_COMENTARIO_ISLA, "comparacion")

    assert len(mutantes) == 1, f"esperado 1 mutante, hay {len(mutantes)}"
    assert mutantes[0].linea == 4, (
        f"el mutante cayó en la línea {mutantes[0].linea} "
        f"({mutantes[0].mutado!r}); el operador está en la 4"
    )
    assert mutantes[0].mutado == "is not None"


def test_f034_r5_es_palabra_exige_que_LOS_DOS_extremos_sean_de_palabra():
    """R5/R6: `_es_palabra` decide a quién se le exige delimitación.

    Los dos extremos mandan, y cada uno por su cuenta: si bastara con el
    primero, un token que empieza por letra y termina en símbolo pasaría por
    palabra; si el segundo mirase el byte equivocado —el penúltimo en vez del
    último—, la respuesta dejaría de depender del carácter que de verdad linda
    con el operando de la derecha.
    """
    assert _es_palabra(b"is") is True
    assert _es_palabra(b"is not") is True
    assert _es_palabra(b"True") is True

    assert _es_palabra(b"") is False, "sin token no hay palabra que delimitar"
    assert _es_palabra(b"==") is False, "delimitar `==` dejaría de mutar `x==y`"

    assert _es_palabra(b"is=") is False, (
        "empieza por letra pero termina en símbolo: el ÚLTIMO byte también "
        "tiene que ser de palabra, y es el último, no el penúltimo"
    )
    assert _es_palabra(b"=is") is False, (
        "termina en letra pero empieza por símbolo: el PRIMER byte también "
        "tiene que ser de palabra"
    )


def test_f034_r5_delimitado_mira_los_dos_bytes_que_rodean_la_coincidencia():
    """R5: `_delimitado` interroga el byte anterior y el siguiente, ambos.

    El anterior se mira desde la columna 1 —no solo a partir de la 2—, y el
    siguiente es el que va justo detrás de la coincidencia. Cada uno basta por
    sí solo para rechazarla.
    """
    assert _delimitado(b"is ", 0, 2) is True, (
        "en la columna 0 no hay byte anterior que mirar: es un token entero"
    )
    assert _delimitado(b" is ", 1, 3) is True

    assert _delimitado(b"ais", 1, 3) is False, (
        "el byte anterior es de palabra: la coincidencia de la columna 1 es el "
        "final de «ais», no un operador"
    )
    assert _delimitado(b"isla", 0, 2) is False, (
        "el byte siguiente es de palabra: la coincidencia es el principio de "
        "«isla», no un operador"
    )


def test_f034_r6_los_simbolos_sin_espacios_siguen_mutando():
    """R6: delimitar palabras no puede dejar de mutar `x==y` ni `a+b`."""
    mutantes = _mutantes(FUENTE_SIMBOLOS)
    comparaciones = [m for m in mutantes if m.operador == "comparacion"]
    aritmeticos = [m for m in mutantes if m.operador == "aritmetico"]

    assert [m.mutado for m in comparaciones] == ["if x!=y:"]
    assert [m.mutado for m in aritmeticos] == ["z=a-b"]


def test_f034_r7_espaciado_no_canonico_no_genera_mutante_ni_falla():
    """R7: `is  not` es un límite declarado — cero mutantes, cero excepciones."""
    assert _de_operador(FUENTE_NO_CANONICA, "comparacion") == []


def test_f034_r8_el_mutante_compila_y_el_ast_lleva_el_operador_contrario():
    """R8: aplicado, el mutante compila y el AST lleva el operador contrario."""
    directo = _de_operador(FUENTE_IS, "comparacion")[0]
    mutada = aplicar_mutante(FUENTE_IS, directo)
    compile(mutada, FICHERO, "exec")

    assert isinstance(_primera_comparacion(mutada).ops[0], ast.IsNot)

    inverso = _de_operador(FUENTE_F027, "comparacion")[0]
    mutada_inversa = aplicar_mutante(FUENTE_F027, inverso)
    compile(mutada_inversa, FICHERO, "exec")

    assert isinstance(_primera_comparacion(mutada_inversa).ops[0], ast.Is)


def test_la_declaracion_de_rutas_sensibles_dice_cuando_sube_a_bloqueo():
    """Una exigencia que arranca en `aviso` tiene que decir cuándo deja de serlo.

    Sin esa frase, «aviso» se convierte en permanente por olvido: nadie sabe
    qué tendría que pasar para que la puerta bloquee de verdad, así que no pasa
    nunca. Se exige UNA sola, porque dos condiciones distintas en el mismo
    campo es no tener ninguna.

    Lo que la condición nombre debe estar **versionado**: en albaranes se
    condicionó a unos libros `.xlsx` que `.gitignore` excluye —existían en la
    máquina del humano y no en el repositorio—, así que la puerta dependía de
    un artefacto que quien clona no puede ver. La regla está escrita en
    `harness/rutas_sensibles.ejemplo.json`; comprobarla automáticamente exige
    saber qué artefacto consume cada verificación, y eso es de cada proyecto.

    En un repositorio sin declaración no hay puerta que comprobar: se salta.
    """
    if not RUTA_DECLARACION.exists():
        pytest.skip(f"este repositorio no declara {RUTA_DECLARACION.as_posix()}")

    exigencia = json.loads(RUTA_DECLARACION.read_text(encoding="utf-8")).get(
        "_exigencia", ""
    )
    condiciones = [frase for frase in exigencia.split(". ") if "'bloqueo'" in frase]

    assert len(condiciones) == 1, (
        f"esperada UNA frase que declare cuándo se sube a 'bloqueo', "
        f"hay {len(condiciones)} en {RUTA_DECLARACION.as_posix()}"
    )
