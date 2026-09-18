# utils/text_sanitize.py
"""
Limpieza de texto para ficheros BC3

Objetivos
---------
1. Eliminar el carácter de sustitución � y cualquier control/no imprimible.
2. Quitar TODAS las tildes y diéresis (á → a, ñ → n, ü → u, …).
3. Conservar los separadores propios del formato BC3:
      ~   |   \
4. Mantener todos los ASCII imprimibles (letras, números, signos de puntuación).
5. Convertir { } a ( ) para evitar RTF colado en textos largos.

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
#  Letras que NFKD no descompone: se transliteran a mano                      #
# --------------------------------------------------------------------------- #
# NFKD deja intactas las letras con trazo o ligadas (Ø, æ, ß) y convierte el
# signo micro µ en la mu griega μ. Todas son `isalnum()`, así que se colaban
# como no-ASCII pese a que esta limpieza promete lo contrario. Sigrid no
# importa el descompuesto de un concepto cuyo resumen las lleve (F-002 R25),
# de modo que se traducen antes de quitar los diacríticos.
_TRANSLITERACIONES: dict[str, str] = {
    "Ø": "O", "ø": "o",
    "Æ": "AE", "æ": "ae",
    "Œ": "OE", "œ": "oe",
    "ß": "ss",
    "µ": "u", "μ": "u",
}


# --------------------------------------------------------------------------- #
#  Strip accents (á -> a, ñ -> n, etc.)                                       #
# --------------------------------------------------------------------------- #
def _strip_accents(txt: str) -> str:
    """
    Devuelve `txt` sin diacríticos usando NFKD
    (separa los caracteres base de sus marcas de acento y descarta las marcas).
    """
    txt = "".join(_TRANSLITERACIONES.get(ch, ch) for ch in txt)
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
    • convierte llaves { } a paréntesis ( )

    Ejemplo:
        "SEGÚN ©norma\n"  ->  "SEGUN norma"
    """
    if text is None:
        return ""

    # Mapeo de llaves a paréntesis (evita restos de RTF)
    text = (text.replace("{", "(").replace("}", ")"))

    text = _strip_accents(text)

    # `_ALLOWED` ya es todo el ASCII imprimible más los espacios, así que
    # preguntar además por `isalnum()` o `isspace()` no añadía nada salvo la
    # puerta por la que se colaban las letras no-ASCII (Ω, μ, þ): quien no esté
    # en ese conjunto se va, y lo que sale es ASCII por construcción.
    cleaned = "".join(ch for ch in text if ch in _ALLOWED)
    return cleaned
