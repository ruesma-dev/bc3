# tests/test_mutacion_sin_bytecode.py
"""Una campaña de mutación no puede dejar bytecode envenenado en el árbol.

Pasó de verdad, y costó una review entera averiguarlo (`postventa-incidencias`,
F-010, 2026-08-19).

`EjecutorPytest` muta un fichero `.py`, lanza la suite en un subproceso y
restaura el original. Si ese subproceso escribe `__pycache__`, en el disco
queda un `.pyc` COMPILADO DESDE EL CÓDIGO MUTADO. El `.py` vuelve a su sitio
pero su `.pyc` no, y CPython invalida la caché por (tamaño, mtime) del fuente:
si la restauración deja el mismo tamaño y el mismo mtime —y lo deja, porque es
el mismo texto escrito en el mismo segundo—, el `.pyc` mutado se considera
VÁLIDO y se sigue importando.

A partir de ahí, la campaña mide contra un árbol envenenado: los mutantes
mueren o sobreviven por razones que no son las del código fuente, y la suite
del proyecto puede pasar o fallar sin que nadie haya tocado nada. El síntoma
que lo delata es una campaña absurdamente rápida.

El arreglo mínimo y suficiente es no generar el bytecode: `PYTHONDONTWRITEBYTECODE`
en el entorno del subproceso. Cuesta una línea y cierra el agujero de raíz.
"""

from __future__ import annotations

import subprocess

from harness.mutacion import EjecutorPytest


def test_la_suite_de_mutacion_se_lanza_sin_escribir_bytecode(monkeypatch):
    """El subproceso recibe `PYTHONDONTWRITEBYTECODE` y hereda el resto.

    Las dos mitades importan. Sin la variable, la campaña envenena el árbol.
    Sin heredar `os.environ`, el subproceso pierde `PATH`, `VIRTUAL_ENV` y las
    variables que la suite del proyecto necesita para arrancar, y todos los
    mutantes «mueren» por un fallo de importación: una campaña que mata el
    100 % sin haber comprobado nada.
    """
    capturado: dict[str, object] = {}

    def falso_run(orden, **opciones):
        capturado["orden"] = orden
        capturado["opciones"] = opciones
        return subprocess.CompletedProcess(orden, 1, stdout=b"", stderr=b"")

    monkeypatch.setenv("UNA_VARIABLE_DEL_ENTORNO", "valor")
    monkeypatch.setattr(subprocess, "run", falso_run)

    EjecutorPytest().correr(timeout_s=60)

    entorno = capturado["opciones"]["env"]
    assert entorno["PYTHONDONTWRITEBYTECODE"] == "1"
    assert entorno["UNA_VARIABLE_DEL_ENTORNO"] == "valor"
