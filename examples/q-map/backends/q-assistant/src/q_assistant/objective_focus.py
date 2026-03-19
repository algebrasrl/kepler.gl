from __future__ import annotations

import re
import unicodedata


_OBJECTIVE_FOCUS_STOPWORDS: set[str] = {
    "con",
    "senza",
    "tra",
    "della",
    "delle",
    "degli",
    "degli",
    "dello",
    "dell",
    "dati",
    "dato",
    "nelle",
    "nella",
    "negli",
    "sulla",
    "sulle",
    "dopo",
    "prima",
    "quindi",
    "anche",
    "solo",
    "then",
    "with",
    "from",
    "into",
    "without",
    "about",
    "while",
    "under",
    "using",
    "usa",
    "usare",
    "esegui",
    "eseguire",
    "calcola",
    "calcolare",
    "mostra",
    "fai",
    "fare",
    "gestisci",
    "analizza",
    "analizzare",
    "verifica",
    "verificare",
    "valuta",
    "valutare",
    "restituisci",
    "restituire",
    "prepara",
    "preparare",
    "organizza",
    "organizzare",
    "evidenzia",
    "evidenziare",
    "distribuisci",
    "distribuire",
    "correggi",
    "correggere",
    "workflow",
    "mappa",
    "map",
    "tool",
    "tools",
    "step",
    "steps",
}
_OBJECTIVE_FOCUS_PRIORITY_MARKERS: tuple[str, ...] = (
    "ranking",
    "rank",
    "classifica",
    "preview",
    "distinct",
    "territorial",
    "amministr",
    "tematic",
    "spazial",
    "delimit",
    "priorit",
    "indicator",
    "metrica",
    "filtro",
    "materializz",
    "normalizz",
    "merge",
    "dataset",
    "caric",
    "salvat",
    "cloud",
    "load",
    "wait",
    "visibil",
    "ordine",
    "finale",
    "clip",
    "overlay",
    "intersezion",
    "perimetri",
    "copertura",
    "impatto",
    "prossimit",
    "continuit",
    "differenz",
    "comun",
    "conform",
    "limiti",
    "superament",
    "h3",
    "ring",
)


def _normalize_focus_token(token: str) -> str:
    text = str(token or "").strip().lower()
    if not text:
        return ""
    normalized = unicodedata.normalize("NFD", text)
    normalized = re.sub(r"[\u0300-\u036f]", "", normalized)
    return normalized


def _extract_objective_focus_terms(objective_text: str, *, max_terms: int = 3) -> list[str]:
    text = re.sub(r"\s+", " ", str(objective_text or "").strip().lower())
    if not text:
        return []
    tokens_raw = re.findall(r"[a-z0-9àèéìòù_\\-]{3,}", text, flags=re.IGNORECASE)
    if not tokens_raw:
        return []

    unique_tokens: list[str] = []
    seen: set[str] = set()
    for raw in tokens_raw:
        token = raw.strip().strip("_-")
        if not token:
            continue
        if token in seen:
            continue
        if token in _OBJECTIVE_FOCUS_STOPWORDS:
            continue
        if token.isdigit():
            continue
        seen.add(token)
        unique_tokens.append(token)

    prioritized: list[str] = []
    prioritized_set: set[str] = set()
    normalized_tokens: dict[str, str] = {token: _normalize_focus_token(token) for token in unique_tokens}
    for marker in _OBJECTIVE_FOCUS_PRIORITY_MARKERS:
        for token in unique_tokens:
            if token in prioritized_set:
                continue
            normalized_token = normalized_tokens.get(token) or token
            if marker in normalized_token:
                prioritized.append(token)
                prioritized_set.add(token)
                if len(prioritized) >= max_terms:
                    return prioritized

    for token in unique_tokens:
        if token in prioritized_set:
            continue
        prioritized.append(token)
        prioritized_set.add(token)
        if len(prioritized) >= max_terms:
            return prioritized

    return prioritized[:max_terms]
