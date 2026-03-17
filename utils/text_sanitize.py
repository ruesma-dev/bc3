# utils/text_sanitize.py
from __future__ import annotations

import re
import unicodedata

_CONTROL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
_MULTI_SPACE_RE = re.compile(r"[ \t]+")


def clean_text(value: object) -> str:
    if value is None:
        return ""

    text = str(value)
    text = unicodedata.normalize("NFKC", text)
    text = text.replace("\ufeff", "")
    text = _CONTROL_RE.sub("", text)
    text = _MULTI_SPACE_RE.sub(" ", text)
    return text.strip()
