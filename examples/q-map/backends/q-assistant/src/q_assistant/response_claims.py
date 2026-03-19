from __future__ import annotations

import re
from typing import Any


def _response_claims_success(response_text: Any) -> bool:
    text = str(response_text or "").strip().lower()
    if not text:
        return False
    if re.search(r"\b(non|impossibile|fallit|errore|failed|cannot|unable)\b", text):
        return False
    return re.search(r"\b(completat\w*|success\w*|done|ok)\b", text) is not None


def _response_claims_operational_success(response_text: Any) -> bool:
    text = str(response_text or "").strip().lower()
    if not text:
        return False
    if re.search(r"\b(non|impossibile|fallit|errore|failed|cannot|unable)\b", text):
        return False
    markers = (
        "ho caricato",
        "caricato correttamente",
        "dati caricati",
        "layer attivo",
        "loaded dataset",
        "loaded data",
        "visualizzat",
    )
    return any(marker in text for marker in markers)


def _response_claims_centering_success(response_text: Any) -> bool:
    text = str(response_text or "").strip().lower()
    if not text:
        return False
    if re.search(r"\b(non|impossibile|fallit|errore|failed|cannot|unable)\b", text):
        return False
    markers = (
        "mappa centrata",
        "centrata sull",
        "centrato sull",
        "mappa inquadrata",
        "inquadrata sulla mappa",
        "inquadrato sulla mappa",
        "zoom impostato",
        "map centered",
        "map centred",
    )
    return any(marker in text for marker in markers)
