"""Texto que no se puede codificar en UTF-8 (G-r3-1)."""

import json
from typing import Any


def without_lone_surrogates(value: Any) -> Any:
    """Quita los surrogates sueltos (`"\\ud800"` escapado en JSON: JSON válido, UTF-8 no) de un
    str o de cualquier valor JSON. Sin sustituto: un emoji partido entre dos snapshots deja el
    primero como prefijo del segundo. Un par válido ya es un solo carácter y no se toca."""
    if isinstance(value, str):
        return value.encode("utf-8", "ignore").decode()
    return json.loads(json.dumps(value, ensure_ascii=False).encode("utf-8", "ignore"))
