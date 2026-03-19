from __future__ import annotations

import re

from .objective_focus import _normalize_focus_token


def _objective_requires_clip_stats_coverage_validation(objective_text: str) -> bool:
    text = str(objective_text or "").strip().lower()
    if not text:
        return False
    overlay_markers = (
        "intersezion",
        "intersection",
        "overlay",
        "clip",
    )
    stats_markers = (
        "percentuale area",
        "percent area",
        "conteggio",
        "count",
        "aggregaz",
        "statistic",
        "confront",
        "comparabil",
    )
    cross_geometry_markers = (
        "shape",
        "h3",
        "livelli diversi",
        "geometrie diverse",
        "cross-geometry",
        "cross geometry",
    )
    return (
        any(marker in text for marker in overlay_markers)
        and any(marker in text for marker in stats_markers)
        and any(marker in text for marker in cross_geometry_markers)
    )


def _objective_requests_dataset_discovery(objective_text: str) -> bool:
    text = str(objective_text or "").strip().lower()
    if not text:
        return False
    markers = (
        "lista dataset",
        "elenco dataset",
        "dataset disponibili",
        "quali dataset",
        "which dataset",
        "what datasets",
        "inventory",
        "inventario",
        "discovery",
        "snapshot",
        "catalog",
        "catalogo",
        "listqmapdatasets",
        "listqcumberdatasets",
        "listqcumberproviders",
    )
    return any(marker in text for marker in markers)


def _objective_requires_ranked_output(objective_text: str) -> bool:
    text = str(objective_text or "").strip().lower()
    if not text:
        return False
    markers = (
        "piu ",
        "più ",
        "meno ",
        "top ",
        "classifica",
        "ranking",
        "most ",
        "least ",
        "highest",
        "lowest",
        "superiore",
        "inferiore",
    )
    return any(marker in text for marker in markers)


def _objective_mentions_cloud_or_saved_maps(objective_text: str) -> bool:
    """Return True when the user objective explicitly references cloud, saved, or personal maps."""
    text = str(objective_text or "").strip().lower()
    if not text:
        return False
    markers = (
        "cloud", "mappa cloud", "mappe cloud", "cloud map",
        "mappa salvat", "mappe salvat", "saved map",
        "mappa personale", "mappe personali", "personal map",
        "archivio", "le mie mappe", "my map",
        "mappa esistente", "mappe esistenti",
    )
    return any(marker in text for marker in markers)


def _objective_requests_cloud_load_sequence(objective_text: str) -> bool:
    text = str(objective_text or "").strip().lower()
    if not text:
        return False
    cloud_markers = ("cloud", "mappa cloud", "mappe cloud", "cloud map")
    load_markers = ("load", "caric", "attesa", "wait", "fallback")
    return any(marker in text for marker in cloud_markers) and any(marker in text for marker in load_markers)


def _objective_requests_population_transfer_modes(objective_text: str) -> bool:
    text = str(objective_text or "").strip().lower()
    if not text:
        return False
    has_population = any(marker in text for marker in ("popolazione", "population", "abitanti", "residenti"))
    has_h3_context = any(marker in text for marker in ("h3", "griglia", "tassell"))
    has_mode_hint = any(marker in text for marker in ("standard", "proporzion", "area weighted", "discreto"))
    return has_population and has_h3_context and has_mode_hint


def _extract_objective_required_focus_phrases(objective_text: str) -> list[str]:
    text = re.sub(r"\s+", " ", str(objective_text or "").strip().lower())
    if not text:
        return []
    normalized_text = _normalize_focus_token(text)

    required: list[str] = []
    has_clip_intent = "clip" in text or "clipping" in text
    has_dissolve_intent = "dissolve" in text
    has_overlay_intent = "overlay" in text
    if has_clip_intent and has_dissolve_intent and has_overlay_intent:
        required.extend(("clip", "dissolve", "overlay"))

    if _objective_requests_cloud_load_sequence(objective_text):
        required.extend(("cloud", "map", "dataset"))
        if "retry" in text:
            required.append("retry")
        if "fallback" in text:
            required.append("fallback")
        if "timeout" in text:
            required.append("timeout")

    if _objective_requests_population_transfer_modes(objective_text):
        if "standard" in text:
            required.append("standard")
        if "proporzion" in normalized_text:
            required.append("proporzionale")
        if "discreto" in text:
            required.append("discreto")

    if _objective_requires_clip_stats_coverage_validation(objective_text):
        for phrase in ("percentuale area", "conteggio", "shape", "h3"):
            if phrase in text:
                required.append(phrase)

    if _objective_requires_ranked_output(objective_text):
        if "metrica derivata" in text or ("metrica" in text and "derivat" in text):
            required.append("metrica derivata")
        if "normalizz" in text:
            required.append("normalizzata")
        if "confront" in text:
            required.append("confronto")
        if "ex-aequo" in text or "ex aequo" in text:
            required.append("ex-aequo")

    deduped: list[str] = []
    seen: set[str] = set()
    for term in required:
        key = _normalize_focus_token(term)
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(term)
    return deduped


# ---------------------------------------------------------------------------
# Statistical / regulatory intent detectors
# ---------------------------------------------------------------------------


def _objective_requests_linear_regression(objective_text: str) -> bool:
    text = str(objective_text or "").strip().lower()
    return bool(
        re.search(r"regress", text)
        or re.search(r"pendenza|slope|intercett|r²|r-quadro|r quadro", text)
        or re.search(r"variabile (dipendente|indipendente)", text)
        or re.search(r"predett[iao]|predict", text)
        or re.search(r"residuo|residual", text)
    )


def _objective_requests_field_correlation(objective_text: str) -> bool:
    text = str(objective_text or "").strip().lower()
    if re.search(r"spaziale|spatial|lisa|moran|cluster|autocorrelaz", text):
        return False
    return bool(
        re.search(r"correlazion[ei].*camp[io]|correlat.*field", text)
        or re.search(r"matrice.*correlazion|correlation.*matrix", text)
        or re.search(r"pearson", text)
        or (re.search(r"correlazion", text) and re.search(r"tra.*e\s", text))
    )


def _objective_requests_natural_break_classification(objective_text: str) -> bool:
    text = str(objective_text or "").strip().lower()
    return bool(
        re.search(r"classific.*grupp.*natural|natural.break|jenks|ckmeans", text)
        or re.search(r"classifica.*in\s+\d+\s+grupp", text)
        or re.search(r"categorizz.*grupp.*natural", text)
        or re.search(r"\d+\s+grupp.*natural", text)
    )


def _objective_requests_regulatory_compliance(objective_text: str) -> bool:
    text = str(objective_text or "").strip().lower()
    return bool(
        re.search(r"155/2010|normativ|limiti?\s+di\s+legge", text)
        or re.search(r"superament[io]|exceedance", text)
        or re.search(r"conforme|compliance|conform", text)
        or re.search(r"soglie?\s+(normativ|di\s+legge|regolamentar)", text)
        or re.search(r"who.*guideline|linee\s+guida\s+oms", text)
    )


def _objective_requests_exposure_assessment(objective_text: str) -> bool:
    text = str(objective_text or "").strip().lower()
    return bool(
        re.search(r"esposizion.*popolazion|population.*exposure", text)
        or re.search(r"quant[ie].*persone.*espost|abitanti.*espost", text)
        or re.search(r"exposure.*assessment|valutazion.*esposizion", text)
        or re.search(r"popolazion.*raggio|buffer.*popolazion", text)
    )


def _objective_requests_spatial_interpolation(objective_text: str) -> bool:
    text = str(objective_text or "").strip().lower()
    return bool(
        re.search(r"interpola|interpolat", text)
        or re.search(r"idw|inverse.distance", text)
        or re.search(r"superficie continua|continuous surface", text)
        or re.search(r"stima.*spaziale.*tra.*stazion|spatial.*estimat", text)
    )


def _objective_requests_regulatory_listing(objective_text: str) -> bool:
    text = str(objective_text or "").strip().lower()
    if _objective_requests_regulatory_compliance(text):
        if re.search(r"carica|load|verifica|check|stazion|misura", text):
            return False
    return bool(
        re.search(r"limiti?\s+di\s+legge.*qualit[àa]|qualit[àa].*limiti?\s+di\s+legge", text)
        or re.search(r"soglie?\s+normativ.*inquinant|inquinant.*soglie?\s+normativ", text)
        or re.search(r"elenca.*limiti|list.*threshold|quali\s+sono.*limiti", text)
        or re.search(r"155/2010.*elenc|elenc.*155/2010", text)
    )
