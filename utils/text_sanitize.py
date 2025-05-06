# utils/text_sanitize.py
"""
Limpieza de texto para ficheros BC3

Objetivos
---------
1. Eliminar el carácter de sustitución � y cualquier control/no imprimible.
2. Quitar TODAS las tildes y diéresis (á → a, ñ → n, ü → u, …).
3. Conservar los separadores propios del formato BC3:
      ~   |   \
4. Mantener todos los ASCII imprimibles (letras, números, signos de puntuación).

La función `clean_text()` se usa justo antes de escribir cada línea
en la copia modificada del BC3.
"""

from __future__ import annotations
import unicodedata
import string

# --------------------------------------------------------------------------- #
#  Conjunto de caracteres que se permiten tal cual                            #
# --------------------------------------------------------------------------- #
_ALLOWED: set[str] = set(string.printable) | {"|", "~", "\\"}

# --------------------------------------------------------------------------- #
#  Strip accents (á -> a, ñ -> n, etc.)                                       #
# --------------------------------------------------------------------------- #
def _strip_accents(txt: str) -> str:
    """
    Devuelve `txt` sin diacríticos usando NFKD
    (separa los caracteres base de sus marcas de acento y descarta las marcas).
    """
    nfkd = unicodedata.normalize("NFKD", txt)
    return "".join(ch for ch in nfkd if not unicodedata.combining(ch))


# --------------------------------------------------------------------------- #
#  API pública                                                                #
# --------------------------------------------------------------------------- #
def clean_text(text: str) -> str:
    """
    Sanitiza `text` para su uso en líneas BC3:

    • elimina acentos y diéresis
    • elimina caracteres de sustitución (�) y controles
    • conserva ASCII imprimible y los separadores ~ | \

    Ejemplo:
        "SEGÚN ©norma\n"  ->  "SEGUN norma"
    """
    text = _strip_accents(text)

    cleaned = "".join(
        ch
        for ch in text
        if ch in _ALLOWED               # separadores y ASCII
        or ch.isalnum()                 # letras / dígitos sin acentos
        or ch.isspace()                 # espacios, tabs, saltos de línea
    )
    return cleaned
