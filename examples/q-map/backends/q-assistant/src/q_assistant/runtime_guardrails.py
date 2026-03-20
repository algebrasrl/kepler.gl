from __future__ import annotations

from dataclasses import dataclass
import json
import re
from typing import Any, Callable

from .runtime_loop_limit_rules import (
    apply_runtime_loop_rule_decision as _apply_runtime_loop_rule_decision,
    build_admin_level_validation_failure_decision as _build_admin_level_validation_failure_decision,
    build_admin_superlative_layer_order_decision as _build_admin_superlative_layer_order_decision,
    build_clarification_required_decision as _build_clarification_required_decision,
    build_cloud_tools_require_explicit_request_decision as _build_cloud_tools_require_explicit_request_decision,
    build_post_discovery_force_query_decision as _build_post_discovery_force_query_decision,
    build_post_filter_force_fit_decision as _build_post_filter_force_fit_decision,
    build_cloud_load_no_redundant_fallback_decision as _build_cloud_load_no_redundant_fallback_decision,
    build_cloud_no_validated_fallback_decision as _build_cloud_no_validated_fallback_decision,
    build_dataset_snapshot_reuse_decision as _build_dataset_snapshot_reuse_decision,
    build_dataset_not_found_recovery_decision as _build_dataset_not_found_recovery_decision,
    build_error_retry_guardrail_decision as _build_error_retry_guardrail_decision,
    build_identical_tool_args_failure_decision as _build_identical_tool_args_failure_decision,
    build_identical_tool_args_success_decision as _build_identical_tool_args_success_decision,
    build_low_distinct_color_recovery_decision as _build_low_distinct_color_recovery_decision,
    build_missing_field_color_recovery_decision as _build_missing_field_color_recovery_decision,
    build_post_create_validation_decision as _build_post_create_validation_decision,
    build_tool_call_finalize_decision as _build_tool_call_finalize_decision,
    build_turn_state_discovery_decision as _build_turn_state_discovery_decision,
    build_zero_match_must_acknowledge_decision as _build_zero_match_must_acknowledge_decision,
    build_force_statistical_tool_routing_decision as _build_force_statistical_tool_routing_decision,
)
from .runtime_guardrail_candidate_rules import (
    build_boundary_clip_required_candidate as _build_boundary_clip_required_candidate,
    build_centering_fit_candidate as _build_centering_fit_candidate,
    build_clip_stats_clip_required_candidate as _build_clip_stats_clip_required_candidate,
    build_cloud_load_requires_wait_validation_candidate as _build_cloud_load_requires_wait_validation_candidate,
    build_map_display_requires_fit_candidate as _build_map_display_requires_fit_candidate,
    build_metric_field_missing_recovery_candidate as _build_metric_field_missing_recovery_candidate,
    build_perimeter_overlay_coverage_candidate as _build_perimeter_overlay_coverage_candidate,
    build_post_count_isolate_final_candidate as _build_post_count_isolate_final_candidate,
    build_post_create_wait_candidate as _build_post_create_wait_candidate,
    build_post_wait_count_candidate as _build_post_wait_count_candidate,
    build_save_cached_dataset_before_wait_candidate as _build_save_cached_dataset_before_wait_candidate,
    build_turn_state_discovery_recovery_candidate as _build_turn_state_discovery_recovery_candidate,
    build_zonal_freeze_fallback_candidate as _build_zonal_freeze_fallback_candidate,
)
from .runtime_workflow_state import build_runtime_workflow_state


_LOW_DISTINCT_COLOR_FAILURE_BLOCKED_RECOMPUTE_TOOLS = {
    "listQCumberProviders",
    "listQCumberDatasets",
    "queryQCumberTerritorialUnits",
    "queryQCumberDataset",
    "queryQCumberDatasetSpatial",
    "zonalStatsByAdmin",
    "spatialJoinByPredicate",
    "clipQMapDatasetByGeometry",
    "clipDatasetByBoundary",
    "overlayDifference",
    "bufferAndSummarize",
    "nearestFeatureJoin",
    "aggregateDatasetToH3",
    "joinQMapDatasetsOnH3",
}
_RUNTIME_RESPONSE_MODE_PREFIX = "[RUNTIME_RESPONSE_MODE]"


@dataclass(frozen=True)
class RuntimeToolLoopLimitBindings:
    _extract_recent_tool_results_since_last_user: Callable[..., list[dict[str, Any]]]
    _extract_prompt_from_messages: Callable[[Any], str]
    _extract_request_tool_names: Callable[[dict[str, Any]], list[str]]
    _should_prune_qcumber_discovery_for_bridge: Callable[..., bool]
    _should_prune_territorial_query_for_thematic_objective: Callable[..., bool]
    _should_prune_qcumber_provider_listing_without_discovery: Callable[..., bool]
    _should_prune_fit_without_map_focus: Callable[..., bool]
    _has_assistant_text_since_last_user: Callable[[dict[str, Any]], bool]
    _runtime_failure_error_class: Callable[[dict[str, Any]], str]
    _select_identical_tool_args_failure_circuit_breaker: Callable[..., dict[str, Any] | None]
    _select_identical_tool_args_success_guardrail: Callable[..., dict[str, Any] | None]
    _compact_signature_for_trace: Callable[[Any], str]
    _is_turn_state_discovery_failure: Callable[[Any], bool]
    _objective_requests_dataset_discovery: Callable[[str], bool]
    _objective_requests_cloud_load_sequence: Callable[[str], bool]
    _objective_mentions_cloud_or_saved_maps: Callable[[str], bool]
    _objective_requires_ranked_output: Callable[[str], bool]
    _objective_targets_admin_units: Callable[[str], bool]
    _objective_requests_linear_regression: Callable[[str], bool]
    _objective_requests_field_correlation: Callable[[str], bool]
    _objective_requests_natural_break_classification: Callable[[str], bool]
    _objective_requests_regulatory_compliance: Callable[[str], bool]
    _objective_requests_regulatory_listing: Callable[[str], bool]
    _objective_requests_exposure_assessment: Callable[[str], bool]
    _objective_requests_spatial_interpolation: Callable[[str], bool]
    _latest_successful_tool_index: Callable[[list[dict[str, Any]], set[str]], int]
    _is_low_distinct_color_failure: Callable[[Any], bool]
    _is_metric_field_not_found_failure: Callable[[Any], bool]
    _classify_runtime_error_kind: Callable[[Any], str]
    _runtime_error_retry_policy: Callable[[str], dict[str, Any]]
    _DATASET_CREATE_OR_UPDATE_TOOLS: set[str]
    _STYLE_EXECUTION_TOOLS: set[str]
    _VISIBILITY_ISOLATION_TOOLS: set[str]
    _POST_CREATE_VALIDATION_DEFERRED_TOOLS: set[str]
    _RUNTIME_GUARDRAIL_PREFIX: str
    _RUNTIME_NEXT_STEP_PREFIX: str
    _QCUMBER_DISCOVERY_TOOLS: set[str]
    _QCUMBER_PROVIDER_SCOPED_TOOLS: set[str]
    _DISCOVERY_TOOLS: set[str]
    _TOOL_CALL_WORKFLOW_HARD_CAP: int
    _TOOL_ONLY_NO_TEXT_WATCHDOG_MIN_CALLS: int
    _ERROR_CLASS_MAX_RETRIES: int


def objective_requests_charts(objective_text: str) -> bool:
    text = str(objective_text or "").strip().lower()
    if not text:
        return False
    markers = ("grafic", "chart", "plot", "visualizz")
    return any(marker in text for marker in markers)


def objective_requests_panel_navigation(objective_text: str) -> bool:
    text = str(objective_text or "").strip().lower()
    if not text:
        return False
    explicit_markers = (
        "apri il pannello",
        "apri pannello",
        "apri il tab",
        "apri la tab",
        "cambia pannello",
        "cambia tab",
        "switch panel",
        "switch tab",
        "open panel",
        "open the panel",
        "open tab",
        "side-panel",
        "side panel",
    )
    if any(marker in text for marker in explicit_markers):
        return True

    panel_terms = (
        "pannello",
        "panel",
        "tab",
        "scheda",
        "layers",
        "layer",
        "filters",
        "filter",
        "interaction",
        "interazione",
        "profile",
        "profilo",
        "operations",
        "operazioni",
    )
    action_terms = (
        "apri",
        "aprire",
        "aperto",
        "open",
        "switch",
        "cambia",
        "passa",
        "vai",
        "show",
        "mostra",
    )
    return any(term in text for term in panel_terms) and any(term in text for term in action_terms)


def objective_requests_coloring(objective_text: str) -> bool:
    text = str(objective_text or "").strip().lower()
    if not text:
        return False
    markers = (
        "colora",
        "colora",
        "color",
        "colore",
        "colorazione",
        "choropleth",
        "scala colori",
        "palette",
    )
    return any(marker in text for marker in markers)


def objective_requests_map_centering(objective_text: str) -> bool:
    text = str(objective_text or "").strip().lower()
    if not text:
        return False
    markers = (
        "centra",
        "centrata",
        "centrato",
        "centrare",
        "inquadra",
        "inquadr",
        "zoom",
        "fit",
        "focus",
        "center",
        "centre",
    )
    return any(marker in text for marker in markers)


def objective_requests_map_display(objective_text: str) -> bool:
    text = str(objective_text or "").strip().lower()
    if not text:
        return False
    explicit_markers = (
        "mostra sulla mappa",
        "mostrami sulla mappa",
        "mostrami direttamente il risultato sulla mappa",
        "visualizza sulla mappa",
        "in primo piano sulla mappa",
        "portalo in primo piano sulla mappa",
        "portala in primo piano sulla mappa",
        "display on map",
        "show on map",
        "show it on the map",
        "bring it to the front on the map",
    )
    if any(marker in text for marker in explicit_markers):
        return True
    compact = re.sub(r"\s+", " ", text)
    explicit_display_patterns = (
        r"\bmostr\w*[^.]{0,60}\bsu(?:lla)?\s+mappa\b",
        r"\bvisualizz\w*[^.]{0,60}\bsu(?:lla)?\s+mappa\b",
        r"\bporta\w*[^.]{0,60}\bprimo\s+piano[^.]{0,40}\bsu(?:lla)?\s+mappa\b",
        r"\bshow\b[^.]{0,60}\bon\s+(?:the\s+)?map\b",
        r"\bdisplay\b[^.]{0,60}\bon\s+(?:the\s+)?map\b",
    )
    if any(re.search(pattern, compact) for pattern in explicit_display_patterns):
        return True
    # Standalone display verbs at the start of the objective imply map display
    # in a map application context (e.g. "mostra aree contaminate …").
    leading_display_patterns = (
        r"^mostr(?:a|ami|i|are)\b",
        r"^visualizz(?:a|ami|are)\b",
        r"^fai\s+vedere\b",
        r"^show\b",
        r"^display\b",
    )
    return any(re.search(pattern, compact) for pattern in leading_display_patterns)


def objective_requests_provider_discovery(objective_text: str) -> bool:
    text = str(objective_text or "").strip().lower()
    if not text:
        return False
    markers = (
        "provider",
        "fornitore",
        "fornitori",
        "sorgente",
        "sorgenti",
        "source",
        "catalogo provider",
        "catalog provider",
    )
    return any(marker in text for marker in markers)


def objective_requests_normalized_metric(objective_text: str) -> bool:
    text = str(objective_text or "").strip().lower()
    if not text:
        return False
    markers = (
        "normalizz",
        "normaliz",
        "normalized",
        "normalised",
        "per capita",
        "pro capite",
        "per 100k",
        "per 100.000",
        "per 100000",
        "/100k",
        "/100000",
        "100k abit",
        "100000 abit",
        "100k inhabit",
        "percent",
        "percentuale",
        "percentage",
        "pct",
        "%",
    )
    return any(marker in text for marker in markers)


def is_likely_normalized_metric_field(field_name: Any) -> bool:
    text = str(field_name or "").strip().lower()
    if not text:
        return False
    markers = (
        "per_100k",
        "per100k",
        "per_100000",
        "per100000",
        "per_capita",
        "percapita",
        "pro_capite",
        "normalized",
        "normaliz",
        "_pct",
        "pct_",
        "_percent",
        "percentage",
        "_ratio",
        "_rate",
    )
    return any(marker in text for marker in markers)


def summarize_runtime_tool_policy(
    initial_tool_names: list[str] | tuple[str, ...],
    final_tool_names: list[str] | tuple[str, ...],
    *,
    max_pruned_tools: int = 4,
    max_chars: int = 240,
) -> str:
    initial = sorted({str(name or "").strip() for name in (initial_tool_names or []) if str(name or "").strip()})
    final = sorted({str(name or "").strip() for name in (final_tool_names or []) if str(name or "").strip()})
    base_count = len(initial) if initial else len(final)
    if base_count <= 0:
        return "source=backend;tools=none"

    initial_set = set(initial)
    final_set = set(final)
    pruned = sorted(initial_set - final_set)
    summary = f"source=backend;allowed={len(final_set)}/{base_count}"
    if pruned:
        shown = pruned[: max(1, int(max_pruned_tools or 1))]
        suffix = ""
        if len(pruned) > len(shown):
            suffix = f",+{len(pruned) - len(shown)}"
        summary = f"{summary};pruned={','.join(shown)}{suffix}"
    else:
        summary = f"{summary};pruned=none"
    return summary[: max(48, int(max_chars or 240))]


def prune_repeated_discovery_tools(
    payload: dict[str, Any],
    *,
    extract_request_tool_results: Callable[..., list[dict[str, Any]]],
    has_repeated_discovery_loop: Callable[[list[dict[str, Any]]], bool],
    extract_request_tool_names: Callable[[dict[str, Any]], list[str]],
    discovery_loop_progress_tools: set[str],
    discovery_loop_prune_tools: set[str],
) -> dict[str, Any]:
    """
    If the recent tool trail indicates a discovery-only loop, remove redundant
    discovery tools from the advertised schema so the model is forced to progress.
    """
    outgoing = dict(payload or {})
    tools = outgoing.get("tools")
    if not isinstance(tools, list) or not tools:
        return outgoing

    results = extract_request_tool_results(outgoing, max_items=48)
    if not has_repeated_discovery_loop(results):
        return outgoing

    tool_names = set(extract_request_tool_names(outgoing))
    if not (discovery_loop_progress_tools & tool_names):
        return outgoing

    pruned_tools: list[dict[str, Any]] = []
    removed = 0
    for tool in tools:
        if not isinstance(tool, dict):
            continue
        fn = tool.get("function")
        name = str(fn.get("name") or "").strip() if isinstance(fn, dict) else ""
        if name in discovery_loop_prune_tools:
            removed += 1
            continue
        pruned_tools.append(tool)

    if removed:
        outgoing["tools"] = pruned_tools
    return outgoing


def prune_forbidden_qmap_runtime_tools(
    payload: dict[str, Any],
    *,
    forbidden_qmap_runtime_tools: set[str],
) -> dict[str, Any]:
    outgoing = dict(payload or {})
    tools = outgoing.get("tools")
    if not isinstance(tools, list) or not tools:
        return outgoing

    filtered: list[dict[str, Any]] = []
    removed = 0
    for tool in tools:
        if not isinstance(tool, dict):
            continue
        fn = tool.get("function")
        name = str(fn.get("name") or "").strip() if isinstance(fn, dict) else ""
        if name in forbidden_qmap_runtime_tools:
            removed += 1
            continue
        filtered.append(tool)

    if removed:
        outgoing["tools"] = filtered
    return outgoing


def _is_chart_execution_tool_name(tool_name: str) -> bool:
    name = str(tool_name or "").strip()
    if not name:
        return False
    lowered = name.lower()
    if lowered == "listqmapcharttools":
        return False
    return (
        lowered.endswith("charttool")
        or lowered in {"histogramtool", "categorybarstool", "summarizeqmaptimeseries"}
    )


def prune_open_panel_only_chart_navigation(
    payload: dict[str, Any],
    *,
    extract_prompt_from_messages: Callable[[Any], str],
) -> dict[str, Any]:
    outgoing = dict(payload or {})
    tools = outgoing.get("tools")
    if not isinstance(tools, list) or not tools:
        return outgoing

    objective_text = extract_prompt_from_messages(outgoing.get("messages"))

    tool_names = []
    for tool in tools:
        if not isinstance(tool, dict):
            continue
        fn = tool.get("function")
        name = str(fn.get("name") or "").strip() if isinstance(fn, dict) else ""
        if name:
            tool_names.append(name)

    if "openQMapPanel" not in tool_names:
        return outgoing

    has_non_panel_tool = any(name != "openQMapPanel" for name in tool_names)
    if not has_non_panel_tool:
        return outgoing
    if objective_requests_panel_navigation(objective_text):
        return outgoing

    filtered: list[dict[str, Any]] = []
    removed = 0
    for tool in tools:
        if not isinstance(tool, dict):
            continue
        fn = tool.get("function")
        name = str(fn.get("name") or "").strip() if isinstance(fn, dict) else ""
        if name == "openQMapPanel":
            removed += 1
            continue
        filtered.append(tool)

    if removed:
        outgoing["tools"] = filtered
    return outgoing


def prune_uninformative_chart_tools_for_ranking(
    payload: dict[str, Any],
    *,
    extract_prompt_from_messages: Callable[[Any], str],
    objective_requires_ranked_output: Callable[[str], bool],
    objective_explicit_category_distribution: Callable[[str], bool],
) -> dict[str, Any]:
    outgoing = dict(payload or {})
    tools = outgoing.get("tools")
    if not isinstance(tools, list) or not tools:
        return outgoing

    objective_text = extract_prompt_from_messages(outgoing.get("messages"))
    if not objective_requires_ranked_output(objective_text):
        return outgoing
    if objective_explicit_category_distribution(objective_text):
        return outgoing

    filtered: list[dict[str, Any]] = []
    removed = 0
    for tool in tools:
        if not isinstance(tool, dict):
            continue
        fn = tool.get("function")
        name = str(fn.get("name") or "").strip() if isinstance(fn, dict) else ""
        if name == "categoryBarsTool":
            removed += 1
            continue
        filtered.append(tool)
    if removed:
        outgoing["tools"] = filtered
    return outgoing


def prune_sampling_preview_tools_for_superlatives(
    payload: dict[str, Any],
    *,
    extract_prompt_from_messages: Callable[[Any], str],
    objective_requires_ranked_output: Callable[[str], bool],
    extract_request_tool_names: Callable[[dict[str, Any]], list[str]],
    extract_request_tool_results: Callable[..., list[dict[str, Any]]],
    latest_successful_tool_index: Callable[[list[dict[str, Any]], set[str]], int],
    is_metric_field_not_found_failure: Callable[[Any], bool],
) -> dict[str, Any]:
    outgoing = dict(payload or {})
    tools = outgoing.get("tools")
    if not isinstance(tools, list) or not tools:
        return outgoing

    objective_text = extract_prompt_from_messages(outgoing.get("messages"))
    if not objective_requires_ranked_output(objective_text):
        return outgoing

    tool_names = set(extract_request_tool_names(outgoing))
    if "rankQMapDatasetRows" not in tool_names:
        return outgoing

    results = extract_request_tool_results(outgoing, max_items=48)
    rank_idx = latest_successful_tool_index(results, {"rankQMapDatasetRows"})
    has_metric_missing_failure = any(
        str(row.get("toolName") or "").strip() == "rankQMapDatasetRows"
        and row.get("success") is False
        and is_metric_field_not_found_failure(row.get("details"))
        for row in results
    )
    should_prune_preview = rank_idx >= 0 or not has_metric_missing_failure
    if not should_prune_preview:
        return outgoing

    filtered: list[dict[str, Any]] = []
    removed = 0
    for tool in tools:
        if not isinstance(tool, dict):
            continue
        fn = tool.get("function")
        name = str(fn.get("name") or "").strip() if isinstance(fn, dict) else ""
        if name == "previewQMapDatasetRows":
            removed += 1
            continue
        filtered.append(tool)
    if removed:
        outgoing["tools"] = filtered
    return outgoing


def prune_population_style_tools_for_unresolved_value_coloring(
    payload: dict[str, Any],
    *,
    extract_prompt_from_messages: Callable[[Any], str],
    objective_requests_coloring: Callable[[str], bool],
    objective_explicit_population_metric: Callable[[str], bool],
    extract_request_tool_results: Callable[..., list[dict[str, Any]]],
    has_unresolved_zonal_ui_freeze_failure: Callable[[list[dict[str, Any]]], bool],
    extract_assistant_tool_calls: Callable[[Any], list[dict[str, Any]]],
    has_recent_forest_clc_query_call: Callable[[list[dict[str, Any]]], bool],
) -> dict[str, Any]:
    outgoing = dict(payload or {})
    tools = outgoing.get("tools")
    if not isinstance(tools, list) or not tools:
        return outgoing

    objective_text = extract_prompt_from_messages(outgoing.get("messages"))
    if not objective_requests_coloring(objective_text):
        return outgoing
    if objective_explicit_population_metric(objective_text):
        return outgoing

    results = extract_request_tool_results(outgoing)
    if not has_unresolved_zonal_ui_freeze_failure(results):
        return outgoing

    assistant_calls = extract_assistant_tool_calls(outgoing.get("messages"))
    if not has_recent_forest_clc_query_call(assistant_calls):
        return outgoing

    filtered: list[dict[str, Any]] = []
    removed = 0
    for tool in tools:
        if not isinstance(tool, dict):
            continue
        fn = tool.get("function")
        name = str(fn.get("name") or "").strip() if isinstance(fn, dict) else ""
        if name == "applyQMapStylePreset":
            removed += 1
            continue
        filtered.append(tool)
    if removed:
        outgoing["tools"] = filtered
    return outgoing


def prune_heavy_recompute_tools_after_low_distinct_color_failure(
    payload: dict[str, Any],
    *,
    extract_prompt_from_messages: Callable[[Any], str],
    objective_requests_coloring: Callable[[str], bool],
    extract_request_tool_results: Callable[..., list[dict[str, Any]]],
    is_low_distinct_color_failure: Callable[[Any], bool],
) -> dict[str, Any]:
    outgoing = dict(payload or {})
    tools = outgoing.get("tools")
    if not isinstance(tools, list) or not tools:
        return outgoing

    objective_text = extract_prompt_from_messages(outgoing.get("messages"))
    if not objective_requests_coloring(objective_text):
        return outgoing

    results = extract_request_tool_results(outgoing)
    if not results:
        return outgoing

    last_color_failure_low_distinct_idx = -1
    for idx in range(len(results) - 1, -1, -1):
        row = results[idx]
        if str(row.get("toolName") or "").strip() != "setQMapLayerColorByField":
            continue
        if row.get("success") is False and is_low_distinct_color_failure(row.get("details")):
            last_color_failure_low_distinct_idx = idx
        break

    if last_color_failure_low_distinct_idx < 0:
        return outgoing

    blocked = _LOW_DISTINCT_COLOR_FAILURE_BLOCKED_RECOMPUTE_TOOLS
    filtered: list[dict[str, Any]] = []
    removed = 0
    for tool in tools:
        if not isinstance(tool, dict):
            continue
        fn = tool.get("function")
        name = str(fn.get("name") or "").strip() if isinstance(fn, dict) else ""
        if name in blocked:
            removed += 1
            continue
        filtered.append(tool)

    if removed:
        outgoing["tools"] = filtered
    return outgoing


def append_runtime_guidance_lines(payload: dict[str, Any], guidance_lines: list[str]) -> dict[str, Any]:
    if not guidance_lines:
        return dict(payload or {})

    outgoing = dict(payload or {})
    messages = outgoing.get("messages")
    if not isinstance(messages, list):
        return outgoing

    block = "\n".join(line for line in guidance_lines if str(line or "").strip()).strip()
    if not block:
        return outgoing

    cleaned_messages: list[dict[str, Any]] = []
    for raw_msg in messages:
        if isinstance(raw_msg, dict):
            cleaned_messages.append(dict(raw_msg))

    for idx, msg in enumerate(cleaned_messages):
        if str(msg.get("role") or "").strip().lower() != "system":
            continue
        content = str(msg.get("content") or "").strip()
        updated = dict(msg)
        updated["content"] = "\n".join(part for part in (content, block) if part).strip()
        cleaned_messages[idx] = updated
        outgoing["messages"] = cleaned_messages
        return outgoing

    outgoing["messages"] = [{"role": "system", "content": block}, *cleaned_messages]
    return outgoing


def enforce_runtime_tool_loop_limits(
    payload: dict[str, Any],
    *,
    bindings: RuntimeToolLoopLimitBindings,
) -> dict[str, Any]:
    _extract_recent_tool_results_since_last_user = bindings._extract_recent_tool_results_since_last_user
    _extract_prompt_from_messages = bindings._extract_prompt_from_messages
    _extract_request_tool_names = bindings._extract_request_tool_names
    _should_prune_qcumber_discovery_for_bridge = bindings._should_prune_qcumber_discovery_for_bridge
    _should_prune_territorial_query_for_thematic_objective = (
        bindings._should_prune_territorial_query_for_thematic_objective
    )
    _should_prune_qcumber_provider_listing_without_discovery = (
        bindings._should_prune_qcumber_provider_listing_without_discovery
    )
    _should_prune_fit_without_map_focus = bindings._should_prune_fit_without_map_focus
    _has_assistant_text_since_last_user = bindings._has_assistant_text_since_last_user
    _runtime_failure_error_class = bindings._runtime_failure_error_class
    _select_identical_tool_args_failure_circuit_breaker = (
        bindings._select_identical_tool_args_failure_circuit_breaker
    )
    _select_identical_tool_args_success_guardrail = bindings._select_identical_tool_args_success_guardrail
    _compact_signature_for_trace = bindings._compact_signature_for_trace
    _is_turn_state_discovery_failure = bindings._is_turn_state_discovery_failure
    _objective_requests_dataset_discovery = bindings._objective_requests_dataset_discovery
    _objective_requests_cloud_load_sequence = bindings._objective_requests_cloud_load_sequence
    _objective_mentions_cloud_or_saved_maps = bindings._objective_mentions_cloud_or_saved_maps
    _objective_requires_ranked_output = bindings._objective_requires_ranked_output
    _objective_targets_admin_units = bindings._objective_targets_admin_units
    _objective_requests_linear_regression = bindings._objective_requests_linear_regression
    _objective_requests_field_correlation = bindings._objective_requests_field_correlation
    _objective_requests_natural_break_classification = bindings._objective_requests_natural_break_classification
    _objective_requests_regulatory_compliance = bindings._objective_requests_regulatory_compliance
    _objective_requests_regulatory_listing = bindings._objective_requests_regulatory_listing
    _objective_requests_exposure_assessment = bindings._objective_requests_exposure_assessment
    _objective_requests_spatial_interpolation = bindings._objective_requests_spatial_interpolation
    _latest_successful_tool_index = bindings._latest_successful_tool_index
    _is_low_distinct_color_failure = bindings._is_low_distinct_color_failure
    _is_metric_field_not_found_failure = bindings._is_metric_field_not_found_failure
    _classify_runtime_error_kind = bindings._classify_runtime_error_kind
    _runtime_error_retry_policy = bindings._runtime_error_retry_policy
    _DATASET_CREATE_OR_UPDATE_TOOLS = bindings._DATASET_CREATE_OR_UPDATE_TOOLS
    _STYLE_EXECUTION_TOOLS = bindings._STYLE_EXECUTION_TOOLS
    _VISIBILITY_ISOLATION_TOOLS = bindings._VISIBILITY_ISOLATION_TOOLS
    _POST_CREATE_VALIDATION_DEFERRED_TOOLS = bindings._POST_CREATE_VALIDATION_DEFERRED_TOOLS
    _RUNTIME_GUARDRAIL_PREFIX = bindings._RUNTIME_GUARDRAIL_PREFIX
    _RUNTIME_NEXT_STEP_PREFIX = bindings._RUNTIME_NEXT_STEP_PREFIX
    _QCUMBER_DISCOVERY_TOOLS = bindings._QCUMBER_DISCOVERY_TOOLS
    _QCUMBER_PROVIDER_SCOPED_TOOLS = bindings._QCUMBER_PROVIDER_SCOPED_TOOLS
    _DISCOVERY_TOOLS = bindings._DISCOVERY_TOOLS
    _TOOL_CALL_WORKFLOW_HARD_CAP = bindings._TOOL_CALL_WORKFLOW_HARD_CAP
    _TOOL_ONLY_NO_TEXT_WATCHDOG_MIN_CALLS = bindings._TOOL_ONLY_NO_TEXT_WATCHDOG_MIN_CALLS
    _ERROR_CLASS_MAX_RETRIES = bindings._ERROR_CLASS_MAX_RETRIES

    outgoing = dict(payload or {})
    tools = outgoing.get("tools")
    if not isinstance(tools, list):
        return outgoing

    results = _extract_recent_tool_results_since_last_user(outgoing, max_items=192)
    objective_text = _extract_prompt_from_messages(outgoing.get("messages"))
    request_tool_names = set(_extract_request_tool_names(outgoing))
    workflow_state = build_runtime_workflow_state(
        results=results,
        objective_text=objective_text,
        objective_targets_admin_units=_objective_targets_admin_units,
        objective_requests_map_display=objective_requests_map_display,
        objective_requires_ranked_output=_objective_requires_ranked_output,
    )
    remove_tool_names: set[str] = set()
    guidance_lines: list[str] = []
    forced_tool_choice_name = ""

    if _should_prune_qcumber_discovery_for_bridge(
        objective_text=objective_text,
        request_tool_names=request_tool_names,
        results=results,
    ):
        discovery_tools_to_remove = request_tool_names.intersection(_QCUMBER_DISCOVERY_TOOLS)
        if discovery_tools_to_remove:
            remove_tool_names.update(discovery_tools_to_remove)
            guidance_lines.extend(
                [
                    (
                        f"{_RUNTIME_GUARDRAIL_PREFIX} Selected rule `bridge_no_default_qcumber_discovery` "
                        "(bridge objective without explicit discovery request)."
                    ),
                    (
                        f"{_RUNTIME_GUARDRAIL_PREFIX} Avoid default q-cumber discovery calls in load/save bridge workflows. "
                        "Use loadData/saveDataToMap first and keep discovery only for explicit inventory requests or recovery."
                    ),
                    (
                        f"{_RUNTIME_NEXT_STEP_PREFIX} Execute one bridge step (loadData or saveDataToMap), "
                        "then validate with waitForQMapDataset/countQMapRows."
                    ),
                ]
            )

    if _should_prune_territorial_query_for_thematic_objective(
        objective_text=objective_text,
        request_tool_names=request_tool_names,
        results=results,
    ):
        remove_tool_names.add("queryQCumberTerritorialUnits")
        guidance_lines.extend(
            [
                (
                    f"{_RUNTIME_GUARDRAIL_PREFIX} Selected rule `thematic_spatial_prefer_non_territorial_query` "
                    "(recent routing metadata marks the active dataset path as non-administrative)."
                ),
                (
                    f"{_RUNTIME_GUARDRAIL_PREFIX} Avoid `queryQCumberTerritorialUnits` when backend routing metadata "
                    "indicates non-administrative query flow. Prefer queryQCumberDatasetSpatial/queryQCumberDataset."
                ),
                (
                    f"{_RUNTIME_NEXT_STEP_PREFIX} Execute one metadata-aligned query step with queryQCumberDatasetSpatial "
                    "(or queryQCumberDataset when spatial prefilter is unavailable), then continue with ranking/inspection."
                ),
            ]
        )

    if _should_prune_qcumber_provider_listing_without_discovery(
        objective_text=objective_text,
        request_tool_names=request_tool_names,
        results=results,
    ):
        remove_tool_names.add("listQCumberProviders")
        guidance_lines.extend(
            [
                (
                    f"{_RUNTIME_GUARDRAIL_PREFIX} Selected rule `provider_listing_not_required_for_current_objective` "
                    "(dataset/query path available without explicit provider inventory intent)."
                ),
                (
                    f"{_RUNTIME_GUARDRAIL_PREFIX} Avoid default provider listing when discovery is not explicitly requested. "
                    "Use listQCumberDatasets/query tools directly and keep provider inventory only for explicit source-selection requests."
                ),
                (
                    f"{_RUNTIME_NEXT_STEP_PREFIX} Continue with listQCumberDatasets/queryQCumber* and proceed to the first operational step."
                ),
            ]
        )

    latest_invalid_provider_failure_idx = -1
    latest_invalid_provider_failure_tool = ""
    for idx in range(len(results) - 1, -1, -1):
        row = results[idx]
        tool_name = str(row.get("toolName") or "").strip()
        if row.get("success") is not False or tool_name not in _QCUMBER_PROVIDER_SCOPED_TOOLS:
            continue
        if _classify_runtime_error_kind(row.get("details")) != "invalid_provider_id":
            continue
        latest_invalid_provider_failure_idx = idx
        latest_invalid_provider_failure_tool = tool_name
        break

    if latest_invalid_provider_failure_idx >= 0:
        provider_catalog_recovered = any(
            row.get("success") is True and str(row.get("toolName") or "").strip() == "listQCumberProviders"
            for row in results[latest_invalid_provider_failure_idx + 1 :]
        )
        if not provider_catalog_recovered and "listQCumberProviders" in request_tool_names:
            blocked_tools = request_tool_names.intersection(_QCUMBER_PROVIDER_SCOPED_TOOLS)
            if blocked_tools:
                remove_tool_names.update(blocked_tools)
            forced_tool_choice_name = "listQCumberProviders"
            guidance_lines.extend(
                [
                    (
                        f"{_RUNTIME_GUARDRAIL_PREFIX} Selected rule `provider_recovery_requires_explicit_listing` "
                        f"(last failure={latest_invalid_provider_failure_tool or 'qcumber'}:invalid_provider_id)."
                    ),
                    (
                        f"{_RUNTIME_GUARDRAIL_PREFIX} A qcumber step failed with an invalid providerId. "
                        "Do not continue with dataset/help/query calls on guessed provider identifiers."
                    ),
                    (
                        f"{_RUNTIME_NEXT_STEP_PREFIX} Call listQCumberProviders now, choose an explicit providerId from the "
                        "returned catalog, then continue with listQCumberDatasets/help/query."
                    ),
                ]
            )

    qcumber_dataset_scoped_tools = _QCUMBER_PROVIDER_SCOPED_TOOLS.difference({"listQCumberDatasets"})
    latest_invalid_dataset_failure_idx = -1
    latest_invalid_dataset_failure_tool = ""
    for idx in range(len(results) - 1, -1, -1):
        row = results[idx]
        tool_name = str(row.get("toolName") or "").strip()
        if row.get("success") is not False or tool_name not in qcumber_dataset_scoped_tools:
            continue
        if _classify_runtime_error_kind(row.get("details")) != "invalid_dataset_id":
            continue
        latest_invalid_dataset_failure_idx = idx
        latest_invalid_dataset_failure_tool = tool_name
        break

    if latest_invalid_dataset_failure_idx >= 0:
        dataset_catalog_recovered = any(
            row.get("success") is True and str(row.get("toolName") or "").strip() == "listQCumberDatasets"
            for row in results[latest_invalid_dataset_failure_idx + 1 :]
        )
        if not dataset_catalog_recovered and "listQCumberDatasets" in request_tool_names:
            blocked_tools = request_tool_names.intersection(qcumber_dataset_scoped_tools)
            if blocked_tools:
                remove_tool_names.update(blocked_tools)
            forced_tool_choice_name = "listQCumberDatasets"
            guidance_lines.extend(
                [
                    (
                        f"{_RUNTIME_GUARDRAIL_PREFIX} Selected rule `dataset_recovery_requires_explicit_listing` "
                        f"(last failure={latest_invalid_dataset_failure_tool or 'qcumber'}:invalid_dataset_id)."
                    ),
                    (
                        f"{_RUNTIME_GUARDRAIL_PREFIX} A qcumber step failed with an invalid datasetId. "
                        "Do not continue with help/query calls on guessed dataset identifiers."
                    ),
                    (
                        f"{_RUNTIME_NEXT_STEP_PREFIX} Call listQCumberDatasets now, choose an explicit datasetId from the "
                        "returned catalog, then continue with getQCumberDatasetHelp/query tools."
                    ),
                ]
            )

    # ─── Post-discovery state-machine transition ──────────────────────────
    # After discovery (listProviders + listDatasets succeeded), force the
    # model to proceed with a query tool instead of stopping. This is the
    # deterministic transition: discovery → query.
    post_discovery = _build_post_discovery_force_query_decision(
        results=results,
        request_tool_names=request_tool_names,
        runtime_guardrail_prefix=_RUNTIME_GUARDRAIL_PREFIX,
        runtime_next_step_prefix=_RUNTIME_NEXT_STEP_PREFIX,
    )
    if post_discovery.forced_tool_choice_name:
        forced_tool_choice_name = post_discovery.forced_tool_choice_name
    if post_discovery.guidance_lines:
        guidance_lines.extend(post_discovery.guidance_lines)

    # ─── Post-help incomplete coverage ─────────────────────────────────
    # REMOVED: forcing help for ALL discovered datasets wasted tool-call
    # budget and confused the model into using wrong query tools (e.g.
    # queryQCumberDataset instead of queryQCumberTerritorialUnits).
    # The correct fix for missing metadata is argsSchema in tool contracts
    # (schema-first approach) + the existing post_discovery_force_query
    # guardrail that ensures at least one help call happens.

    # ─── Statistical/regulatory tool routing ──────────────────────────────
    stat_routing = _build_force_statistical_tool_routing_decision(
        results=results,
        request_tool_names=request_tool_names,
        objective_text=objective_text,
        objective_requests_linear_regression=_objective_requests_linear_regression,
        objective_requests_field_correlation=_objective_requests_field_correlation,
        objective_requests_natural_break_classification=_objective_requests_natural_break_classification,
        objective_requests_regulatory_compliance=_objective_requests_regulatory_compliance,
        objective_requests_regulatory_listing=_objective_requests_regulatory_listing,
        objective_requests_exposure_assessment=_objective_requests_exposure_assessment,
        objective_requests_spatial_interpolation=_objective_requests_spatial_interpolation,
        runtime_guardrail_prefix=_RUNTIME_GUARDRAIL_PREFIX,
        runtime_next_step_prefix=_RUNTIME_NEXT_STEP_PREFIX,
    )
    if stat_routing.forced_tool_choice_name:
        forced_tool_choice_name = stat_routing.forced_tool_choice_name
    if stat_routing.guidance_lines:
        guidance_lines.extend(stat_routing.guidance_lines)

    if _should_prune_fit_without_map_focus(
        objective_text=objective_text,
        request_tool_names=request_tool_names,
        results=results,
    ):
        remove_tool_names.add("fitQMapToDataset")
        guidance_lines.extend(
            [
                (
                    f"{_RUNTIME_GUARDRAIL_PREFIX} Selected rule `fit_requires_explicit_map_focus` "
                    "(no centering/display objective detected)."
                ),
                (
                    f"{_RUNTIME_GUARDRAIL_PREFIX} Avoid fitQMapToDataset as a default step when the user did not request map centering/display."
                ),
                (
                    f"{_RUNTIME_NEXT_STEP_PREFIX} Complete the requested analysis/validation first; run fitQMapToDataset only when map focus is explicitly requested."
                    ),
                ]
            )

    forced_tool_choice_name, _ = _apply_runtime_loop_rule_decision(
        remove_tool_names=remove_tool_names,
        guidance_lines=guidance_lines,
        forced_tool_choice_name=forced_tool_choice_name,
        force_finalize_without_tools=False,
        decision=_build_post_create_validation_decision(
            results=results,
            request_tool_names=request_tool_names,
            latest_successful_tool_index=_latest_successful_tool_index,
            dataset_create_or_update_tools=_DATASET_CREATE_OR_UPDATE_TOOLS,
            post_create_validation_deferred_tools=_POST_CREATE_VALIDATION_DEFERRED_TOOLS,
            runtime_guardrail_prefix=_RUNTIME_GUARDRAIL_PREFIX,
            runtime_next_step_prefix=_RUNTIME_NEXT_STEP_PREFIX,
        ),
    )

    forced_tool_choice_name, _ = _apply_runtime_loop_rule_decision(
        remove_tool_names=remove_tool_names,
        guidance_lines=guidance_lines,
        forced_tool_choice_name=forced_tool_choice_name,
        force_finalize_without_tools=False,
        decision=_build_dataset_not_found_recovery_decision(
            results=results,
            request_tool_names=request_tool_names,
            classify_runtime_error_kind=_classify_runtime_error_kind,
            runtime_guardrail_prefix=_RUNTIME_GUARDRAIL_PREFIX,
            runtime_next_step_prefix=_RUNTIME_NEXT_STEP_PREFIX,
        ),
    )

    forced_tool_choice_name, _ = _apply_runtime_loop_rule_decision(
        remove_tool_names=remove_tool_names,
        guidance_lines=guidance_lines,
        forced_tool_choice_name=forced_tool_choice_name,
        force_finalize_without_tools=False,
        decision=_build_missing_field_color_recovery_decision(
            results=results,
            request_tool_names=request_tool_names,
            current_forced_tool_choice_name=forced_tool_choice_name,
            style_execution_tools=_STYLE_EXECUTION_TOOLS,
            blocked_recompute_tools=_LOW_DISTINCT_COLOR_FAILURE_BLOCKED_RECOMPUTE_TOOLS,
            is_metric_field_not_found_failure=_is_metric_field_not_found_failure,
            runtime_guardrail_prefix=_RUNTIME_GUARDRAIL_PREFIX,
            runtime_next_step_prefix=_RUNTIME_NEXT_STEP_PREFIX,
        ),
    )

    forced_tool_choice_name, _ = _apply_runtime_loop_rule_decision(
        remove_tool_names=remove_tool_names,
        guidance_lines=guidance_lines,
        forced_tool_choice_name=forced_tool_choice_name,
        force_finalize_without_tools=False,
        decision=_build_low_distinct_color_recovery_decision(
            results=results,
            request_tool_names=request_tool_names,
            current_forced_tool_choice_name=forced_tool_choice_name,
            style_execution_tools=_STYLE_EXECUTION_TOOLS,
            blocked_recompute_tools=_LOW_DISTINCT_COLOR_FAILURE_BLOCKED_RECOMPUTE_TOOLS,
            is_low_distinct_color_failure=_is_low_distinct_color_failure,
            runtime_guardrail_prefix=_RUNTIME_GUARDRAIL_PREFIX,
            runtime_next_step_prefix=_RUNTIME_NEXT_STEP_PREFIX,
        ),
    )

    forced_tool_choice_name, _ = _apply_runtime_loop_rule_decision(
        remove_tool_names=remove_tool_names,
        guidance_lines=guidance_lines,
        forced_tool_choice_name=forced_tool_choice_name,
        force_finalize_without_tools=False,
        decision=_build_admin_superlative_layer_order_decision(
            results=results,
            request_tool_names=request_tool_names,
            admin_superlative_map_workflow=workflow_state.admin_superlative_map_workflow,
            latest_successful_tool_index=_latest_successful_tool_index,
            runtime_guardrail_prefix=_RUNTIME_GUARDRAIL_PREFIX,
            runtime_next_step_prefix=_RUNTIME_NEXT_STEP_PREFIX,
        ),
    )

    forced_tool_choice_name, _ = _apply_runtime_loop_rule_decision(
        remove_tool_names=remove_tool_names,
        guidance_lines=guidance_lines,
        forced_tool_choice_name=forced_tool_choice_name,
        force_finalize_without_tools=False,
        decision=_build_post_filter_force_fit_decision(
            results=results,
            request_tool_names=request_tool_names,
            admin_superlative_map_workflow=workflow_state.admin_superlative_map_workflow,
            latest_successful_tool_index=_latest_successful_tool_index,
            runtime_guardrail_prefix=_RUNTIME_GUARDRAIL_PREFIX,
            runtime_next_step_prefix=_RUNTIME_NEXT_STEP_PREFIX,
        ),
    )

    if not results:
        if remove_tool_names:
            filtered: list[dict[str, Any]] = []
            for tool in tools:
                if not isinstance(tool, dict):
                    continue
                fn = tool.get("function")
                name = str(fn.get("name") or "").strip() if isinstance(fn, dict) else ""
                if name in remove_tool_names:
                    continue
                filtered.append(tool)
            outgoing["tools"] = filtered
            tool_choice = outgoing.get("tool_choice")
            if isinstance(tool_choice, dict):
                choice_name = ""
                fn = tool_choice.get("function")
                if isinstance(fn, dict):
                    choice_name = str(fn.get("name") or "").strip()
                if choice_name in remove_tool_names:
                    outgoing["tool_choice"] = "auto"
        if guidance_lines:
            outgoing = append_runtime_guidance_lines(outgoing, guidance_lines)
        return outgoing

    tool_call_count = len(results)
    has_assistant_text = _has_assistant_text_since_last_user(outgoing)
    force_finalize_without_tools = False

    forced_tool_choice_name, force_finalize_without_tools = _apply_runtime_loop_rule_decision(
        remove_tool_names=remove_tool_names,
        guidance_lines=guidance_lines,
        forced_tool_choice_name=forced_tool_choice_name,
        force_finalize_without_tools=force_finalize_without_tools,
        decision=_build_tool_call_finalize_decision(
            tool_call_count=tool_call_count,
            has_assistant_text=has_assistant_text,
            tool_call_workflow_hard_cap=_TOOL_CALL_WORKFLOW_HARD_CAP,
            tool_only_no_text_watchdog_min_calls=_TOOL_ONLY_NO_TEXT_WATCHDOG_MIN_CALLS,
            runtime_guardrail_prefix=_RUNTIME_GUARDRAIL_PREFIX,
            runtime_next_step_prefix=_RUNTIME_NEXT_STEP_PREFIX,
        ),
    )

    forced_tool_choice_name, force_finalize_without_tools = _apply_runtime_loop_rule_decision(
        remove_tool_names=remove_tool_names,
        guidance_lines=guidance_lines,
        forced_tool_choice_name=forced_tool_choice_name,
        force_finalize_without_tools=force_finalize_without_tools,
        decision=_build_clarification_required_decision(
            results=results,
            classify_runtime_error_kind=_classify_runtime_error_kind,
            discovery_tools=_DISCOVERY_TOOLS,
            runtime_guardrail_prefix=_RUNTIME_GUARDRAIL_PREFIX,
            runtime_next_step_prefix=_RUNTIME_NEXT_STEP_PREFIX,
            runtime_response_mode_prefix=_RUNTIME_RESPONSE_MODE_PREFIX,
        ),
    )

    forced_tool_choice_name, force_finalize_without_tools = _apply_runtime_loop_rule_decision(
        remove_tool_names=remove_tool_names,
        guidance_lines=guidance_lines,
        forced_tool_choice_name=forced_tool_choice_name,
        force_finalize_without_tools=force_finalize_without_tools,
        decision=_build_admin_level_validation_failure_decision(
            results=results,
            classify_runtime_error_kind=_classify_runtime_error_kind,
            discovery_tools=_DISCOVERY_TOOLS,
            runtime_guardrail_prefix=_RUNTIME_GUARDRAIL_PREFIX,
            runtime_next_step_prefix=_RUNTIME_NEXT_STEP_PREFIX,
            runtime_response_mode_prefix=_RUNTIME_RESPONSE_MODE_PREFIX,
        ),
    )

    forced_tool_choice_name, force_finalize_without_tools = _apply_runtime_loop_rule_decision(
        remove_tool_names=remove_tool_names,
        guidance_lines=guidance_lines,
        forced_tool_choice_name=forced_tool_choice_name,
        force_finalize_without_tools=force_finalize_without_tools,
        decision=_build_error_retry_guardrail_decision(
            results=results,
            runtime_failure_error_class=_runtime_failure_error_class,
            runtime_error_retry_policy=_runtime_error_retry_policy,
            error_class_max_retries=_ERROR_CLASS_MAX_RETRIES,
            runtime_guardrail_prefix=_RUNTIME_GUARDRAIL_PREFIX,
            runtime_next_step_prefix=_RUNTIME_NEXT_STEP_PREFIX,
        ),
    )

    forced_tool_choice_name, force_finalize_without_tools = _apply_runtime_loop_rule_decision(
        remove_tool_names=remove_tool_names,
        guidance_lines=guidance_lines,
        forced_tool_choice_name=forced_tool_choice_name,
        force_finalize_without_tools=force_finalize_without_tools,
        decision=_build_identical_tool_args_failure_decision(
            outgoing=outgoing,
            results=results,
            select_identical_tool_args_failure_circuit_breaker=_select_identical_tool_args_failure_circuit_breaker,
            compact_signature_for_trace=_compact_signature_for_trace,
            runtime_guardrail_prefix=_RUNTIME_GUARDRAIL_PREFIX,
            runtime_next_step_prefix=_RUNTIME_NEXT_STEP_PREFIX,
        ),
    )

    forced_tool_choice_name, force_finalize_without_tools = _apply_runtime_loop_rule_decision(
        remove_tool_names=remove_tool_names,
        guidance_lines=guidance_lines,
        forced_tool_choice_name=forced_tool_choice_name,
        force_finalize_without_tools=force_finalize_without_tools,
        decision=_build_identical_tool_args_success_decision(
            outgoing=outgoing,
            results=results,
            select_identical_tool_args_success_guardrail=_select_identical_tool_args_success_guardrail,
            compact_signature_for_trace=_compact_signature_for_trace,
            runtime_guardrail_prefix=_RUNTIME_GUARDRAIL_PREFIX,
            runtime_next_step_prefix=_RUNTIME_NEXT_STEP_PREFIX,
        ),
    )

    forced_tool_choice_name, force_finalize_without_tools = _apply_runtime_loop_rule_decision(
        remove_tool_names=remove_tool_names,
        guidance_lines=guidance_lines,
        forced_tool_choice_name=forced_tool_choice_name,
        force_finalize_without_tools=force_finalize_without_tools,
        decision=_build_turn_state_discovery_decision(
            outgoing=outgoing,
            results=results,
            extract_request_tool_names=_extract_request_tool_names,
            is_turn_state_discovery_failure=_is_turn_state_discovery_failure,
            runtime_guardrail_prefix=_RUNTIME_GUARDRAIL_PREFIX,
            runtime_next_step_prefix=_RUNTIME_NEXT_STEP_PREFIX,
        ),
    )

    forced_tool_choice_name, force_finalize_without_tools = _apply_runtime_loop_rule_decision(
        remove_tool_names=remove_tool_names,
        guidance_lines=guidance_lines,
        forced_tool_choice_name=forced_tool_choice_name,
        force_finalize_without_tools=force_finalize_without_tools,
        decision=_build_dataset_snapshot_reuse_decision(
            results=results,
            request_tool_names=request_tool_names,
            objective_text=objective_text,
            objective_requests_dataset_discovery=_objective_requests_dataset_discovery,
            discovery_tools=_DISCOVERY_TOOLS,
            is_turn_state_discovery_failure=_is_turn_state_discovery_failure,
            classify_runtime_error_kind=_classify_runtime_error_kind,
            runtime_guardrail_prefix=_RUNTIME_GUARDRAIL_PREFIX,
            runtime_next_step_prefix=_RUNTIME_NEXT_STEP_PREFIX,
        ),
    )

    forced_tool_choice_name, force_finalize_without_tools = _apply_runtime_loop_rule_decision(
        remove_tool_names=remove_tool_names,
        guidance_lines=guidance_lines,
        forced_tool_choice_name=forced_tool_choice_name,
        force_finalize_without_tools=force_finalize_without_tools,
        decision=_build_cloud_tools_require_explicit_request_decision(
            request_tool_names=request_tool_names,
            objective_mentions_cloud_or_saved_maps=_objective_mentions_cloud_or_saved_maps,
            objective_text=objective_text,
            runtime_guardrail_prefix=_RUNTIME_GUARDRAIL_PREFIX,
            runtime_next_step_prefix=_RUNTIME_NEXT_STEP_PREFIX,
        ),
    )

    forced_tool_choice_name, force_finalize_without_tools = _apply_runtime_loop_rule_decision(
        remove_tool_names=remove_tool_names,
        guidance_lines=guidance_lines,
        forced_tool_choice_name=forced_tool_choice_name,
        force_finalize_without_tools=force_finalize_without_tools,
        decision=_build_cloud_load_no_redundant_fallback_decision(
            results=results,
            request_tool_names=request_tool_names,
            objective_text=objective_text,
            objective_requests_cloud_load_sequence=_objective_requests_cloud_load_sequence,
            latest_successful_tool_index=_latest_successful_tool_index,
            runtime_guardrail_prefix=_RUNTIME_GUARDRAIL_PREFIX,
            runtime_next_step_prefix=_RUNTIME_NEXT_STEP_PREFIX,
        ),
    )

    forced_tool_choice_name, force_finalize_without_tools = _apply_runtime_loop_rule_decision(
        remove_tool_names=remove_tool_names,
        guidance_lines=guidance_lines,
        forced_tool_choice_name=forced_tool_choice_name,
        force_finalize_without_tools=force_finalize_without_tools,
        decision=_build_cloud_no_validated_fallback_decision(
            results=results,
            objective_text=objective_text,
            objective_requests_cloud_load_sequence=_objective_requests_cloud_load_sequence,
            classify_runtime_error_kind=_classify_runtime_error_kind,
            runtime_guardrail_prefix=_RUNTIME_GUARDRAIL_PREFIX,
            runtime_next_step_prefix=_RUNTIME_NEXT_STEP_PREFIX,
            runtime_response_mode_prefix=_RUNTIME_RESPONSE_MODE_PREFIX,
        ),
    )

    # ─── Zero-match finalization ──────────────────────────────────────────
    # When ALL filtered queries returned 0 rows, force the model to
    # acknowledge "no data found" and stop — prevents hallucinated counts.
    forced_tool_choice_name, force_finalize_without_tools = _apply_runtime_loop_rule_decision(
        remove_tool_names=remove_tool_names,
        guidance_lines=guidance_lines,
        forced_tool_choice_name=forced_tool_choice_name,
        force_finalize_without_tools=force_finalize_without_tools,
        decision=_build_zero_match_must_acknowledge_decision(
            results=results,
            request_tool_names=request_tool_names,
            runtime_guardrail_prefix=_RUNTIME_GUARDRAIL_PREFIX,
            runtime_next_step_prefix=_RUNTIME_NEXT_STEP_PREFIX,
            runtime_response_mode_prefix=_RUNTIME_RESPONSE_MODE_PREFIX,
        ),
    )

    if force_finalize_without_tools:
        outgoing["tools"] = []
        outgoing["tool_choice"] = "none"
    elif remove_tool_names:
        filtered: list[dict[str, Any]] = []
        removed_count = 0
        for tool in tools:
            if not isinstance(tool, dict):
                continue
            fn = tool.get("function")
            name = str(fn.get("name") or "").strip() if isinstance(fn, dict) else ""
            if name in remove_tool_names:
                removed_count += 1
                continue
            filtered.append(tool)
        if removed_count > 0:
            outgoing["tools"] = filtered
            tool_choice = outgoing.get("tool_choice")
            if isinstance(tool_choice, dict):
                choice_name = ""
                fn = tool_choice.get("function")
                if isinstance(fn, dict):
                    choice_name = str(fn.get("name") or "").strip()
                if choice_name in remove_tool_names:
                    outgoing["tool_choice"] = "auto"

    if forced_tool_choice_name and forced_tool_choice_name not in remove_tool_names:
        remaining_tool_names = set(_extract_request_tool_names(outgoing))
        if forced_tool_choice_name in remaining_tool_names:
            outgoing["tool_choice"] = {"type": "function", "function": {"name": forced_tool_choice_name}}

    if guidance_lines:
        outgoing = append_runtime_guidance_lines(outgoing, guidance_lines)
    return outgoing


def _inject_runtime_guardrail_message(
    payload: dict[str, Any],
    *,
    bindings: dict[str, Any],
) -> dict[str, Any]:
    _DATASET_CREATE_OR_UPDATE_TOOLS = bindings["_DATASET_CREATE_OR_UPDATE_TOOLS"]
    _H3_BOUNDARY_MATERIALIZATION_TOOLS = bindings["_H3_BOUNDARY_MATERIALIZATION_TOOLS"]
    _H3_CLIP_TOOLS = bindings["_H3_CLIP_TOOLS"]
    _OVERLAY_EXECUTION_TOOLS = bindings["_OVERLAY_EXECUTION_TOOLS"]
    _REMOTE_CACHE_DATASET_TOOLS = bindings["_REMOTE_CACHE_DATASET_TOOLS"]
    _RUNTIME_GUARDRAIL_PREFIX = bindings["_RUNTIME_GUARDRAIL_PREFIX"]
    _RUNTIME_NEXT_STEP_PREFIX = bindings["_RUNTIME_NEXT_STEP_PREFIX"]
    _STYLE_EXECUTION_TOOLS = bindings["_STYLE_EXECUTION_TOOLS"]
    _VISIBILITY_ISOLATION_TOOLS = bindings["_VISIBILITY_ISOLATION_TOOLS"]
    _build_dataset_hint = bindings["_build_dataset_hint"]
    _build_source_dataset_hint = bindings["_build_source_dataset_hint"]
    _classify_runtime_error_kind = bindings["_classify_runtime_error_kind"]
    _extract_assistant_tool_calls = bindings["_extract_assistant_tool_calls"]
    _extract_dataset_name_from_call = bindings["_extract_dataset_name_from_call"]
    _extract_dataset_ref_from_call = bindings["_extract_dataset_ref_from_call"]
    _extract_metric_output_field_from_call = bindings["_extract_metric_output_field_from_call"]
    _extract_missing_metric_field = bindings["_extract_missing_metric_field"]
    _extract_prompt_from_messages = bindings["_extract_prompt_from_messages"]
    _extract_request_tool_names = bindings["_extract_request_tool_names"]
    _extract_request_tool_results = bindings["_extract_request_tool_results"]
    _extract_zonal_values_count = bindings["_extract_zonal_values_count"]
    _find_related_tool_call = bindings["_find_related_tool_call"]
    _has_low_distinct_color_failure = bindings["_has_low_distinct_color_failure"]
    _has_recent_forest_clc_query_call = bindings["_has_recent_forest_clc_query_call"]
    _has_repeated_discovery_loop = bindings["_has_repeated_discovery_loop"]
    _has_successful_save_data_to_map_for_dataset = bindings["_has_successful_save_data_to_map_for_dataset"]
    _has_successful_tool_after_index = bindings["_has_successful_tool_after_index"]
    _has_unresolved_zonal_ui_freeze_failure = bindings["_has_unresolved_zonal_ui_freeze_failure"]
    _infer_h3_resolution_from_text = bindings["_infer_h3_resolution_from_text"]
    _is_low_distinct_color_failure = bindings["_is_low_distinct_color_failure"]
    _is_metric_field_not_found_failure = bindings["_is_metric_field_not_found_failure"]
    _is_preview_head_sample_details = bindings["_is_preview_head_sample_details"]
    _is_turn_state_discovery_failure = bindings["_is_turn_state_discovery_failure"]
    _is_uninformative_category_name_chart_call = bindings["_is_uninformative_category_name_chart_call"]
    _is_uninformative_category_name_chart_result = bindings["_is_uninformative_category_name_chart_result"]
    _is_zonal_ui_freeze_failure = bindings["_is_zonal_ui_freeze_failure"]
    _latest_failed_tool_index = bindings["_latest_failed_tool_index"]
    _latest_successful_tool_for_dataset = bindings["_latest_successful_tool_for_dataset"]
    _latest_successful_tool_index = bindings["_latest_successful_tool_index"]
    _needs_boundary_clip_guardrail = bindings["_needs_boundary_clip_guardrail"]
    _needs_cross_geometry_clip_guardrail = bindings["_needs_cross_geometry_clip_guardrail"]
    _needs_overlay_coverage_guardrail = bindings["_needs_overlay_coverage_guardrail"]
    _next_successful_tool_index = bindings["_next_successful_tool_index"]
    _objective_explicit_population_metric = bindings["_objective_explicit_population_metric"]
    _objective_prefers_visibility_isolation = bindings["_objective_prefers_visibility_isolation"]
    _objective_requests_cloud_load_sequence = bindings["_objective_requests_cloud_load_sequence"]
    _objective_requests_dataset_discovery = bindings["_objective_requests_dataset_discovery"]
    _objective_requires_ranked_output = bindings["_objective_requires_ranked_output"]
    _objective_targets_admin_units = bindings["_objective_targets_admin_units"]
    _objective_targets_forest_metric = bindings["_objective_targets_forest_metric"]
    _objective_targets_problem_metric = bindings["_objective_targets_problem_metric"]
    _replace_or_append_h3_resolution_suffix = bindings["_replace_or_append_h3_resolution_suffix"]
    _resolve_dataset_hint_from_result = bindings["_resolve_dataset_hint_from_result"]
    """
    Inject short runtime guardrails derived from recent tool outcomes.
    This adds hard reminders for post-create validation and avoids looping
    repeated color calls with identical args after low-distinct failures.
    """
    outgoing = dict(payload or {})
    messages = outgoing.get("messages")
    if not isinstance(messages, list):
        return outgoing

    tools_available = set(_extract_request_tool_names(outgoing))
    results = _extract_request_tool_results(outgoing, max_items=48)
    assistant_calls = _extract_assistant_tool_calls(messages)
    guidance_lines: list[str] = []
    objective_text = _extract_prompt_from_messages(messages)
    workflow_state = build_runtime_workflow_state(
        results=results,
        objective_text=objective_text,
        objective_targets_admin_units=_objective_targets_admin_units,
        objective_requests_map_display=objective_requests_map_display,
        objective_requires_ranked_output=_objective_requires_ranked_output,
    )

    if results:
        last_idx = len(results) - 1
        last_result = results[last_idx]
        last_tool = str(last_result.get("toolName") or "").strip()
        last_success = last_result.get("success") is True
        related_call = _find_related_tool_call(last_result, assistant_calls)
        dataset_name, dataset_ref, dataset_hint = _resolve_dataset_hint_from_result(last_result, assistant_calls)
        candidates: list[dict[str, Any]] = []
        has_recent_forest_context = _objective_targets_forest_metric(objective_text) or _has_recent_forest_clc_query_call(
            assistant_calls
        )
        has_unresolved_zonal_freeze = _has_unresolved_zonal_ui_freeze_failure(results)

        post_create_wait_candidate = _build_post_create_wait_candidate(
            last_success=last_success,
            last_tool=last_tool,
            dataset_hint=dataset_hint,
            tools_available=tools_available,
            dataset_create_or_update_tools=_DATASET_CREATE_OR_UPDATE_TOOLS,
        )
        if post_create_wait_candidate:
            candidates.append(post_create_wait_candidate)

        post_wait_count_candidate = _build_post_wait_count_candidate(
            results=results,
            last_idx=last_idx,
            last_success=last_success,
            last_tool=last_tool,
            dataset_name=dataset_name,
            dataset_ref=dataset_ref,
            dataset_hint=dataset_hint,
            tools_available=tools_available,
            dataset_create_or_update_tools=_DATASET_CREATE_OR_UPDATE_TOOLS,
            resolve_dataset_hint_from_result=_resolve_dataset_hint_from_result,
            assistant_calls=assistant_calls,
        )
        if post_wait_count_candidate:
            candidates.append(post_wait_count_candidate)

        cloud_load_wait_candidate = _build_cloud_load_requires_wait_validation_candidate(
            results=results,
            objective_text=objective_text,
            tools_available=tools_available,
            objective_requests_cloud_load_sequence=_objective_requests_cloud_load_sequence,
            latest_successful_tool_index=_latest_successful_tool_index,
            next_successful_tool_index=_next_successful_tool_index,
            resolve_dataset_hint_from_result=_resolve_dataset_hint_from_result,
            assistant_calls=assistant_calls,
        )
        if cloud_load_wait_candidate:
            candidates.append(cloud_load_wait_candidate)

        post_count_isolate_candidate = _build_post_count_isolate_final_candidate(
            results=results,
            last_idx=last_idx,
            last_success=last_success,
            last_tool=last_tool,
            dataset_name=dataset_name,
            dataset_ref=dataset_ref,
            dataset_hint=dataset_hint,
            tools_available=tools_available,
            objective_text=objective_text,
            objective_prefers_visibility_isolation=_objective_prefers_visibility_isolation,
            dataset_create_or_update_tools=_DATASET_CREATE_OR_UPDATE_TOOLS,
            visibility_isolation_tools=_VISIBILITY_ISOLATION_TOOLS,
            resolve_dataset_hint_from_result=_resolve_dataset_hint_from_result,
            assistant_calls=assistant_calls,
        )
        if post_count_isolate_candidate:
            candidates.append(post_count_isolate_candidate)

        center_dataset_hint = dataset_hint
        if not center_dataset_hint:
            for prev_idx in range(last_idx, -1, -1):
                prev = results[prev_idx]
                if prev.get("success") is not True:
                    continue
                _, _, hint = _resolve_dataset_hint_from_result(prev, assistant_calls)
                if hint:
                    center_dataset_hint = hint
                    break

        map_display_fit_candidate = _build_map_display_requires_fit_candidate(
            results=results,
            objective_text=objective_text,
            tools_available=tools_available,
            center_dataset_hint=center_dataset_hint,
            objective_requests_map_display=objective_requests_map_display,
            latest_successful_tool_index=_latest_successful_tool_index,
            dataset_create_or_update_tools=_DATASET_CREATE_OR_UPDATE_TOOLS,
        )
        if map_display_fit_candidate:
            candidates.append(map_display_fit_candidate)

        centering_fit_candidate = _build_centering_fit_candidate(
            results=results,
            objective_text=objective_text,
            tools_available=tools_available,
            center_dataset_hint=center_dataset_hint,
            objective_requests_map_centering=objective_requests_map_centering,
            latest_successful_tool_index=_latest_successful_tool_index,
            latest_failed_tool_index=_latest_failed_tool_index,
            dataset_create_or_update_tools=_DATASET_CREATE_OR_UPDATE_TOOLS,
        )
        if centering_fit_candidate:
            candidates.append(centering_fit_candidate)

        turn_state_recovery_candidate = _build_turn_state_discovery_recovery_candidate(
            last_result=last_result,
            is_turn_state_discovery_failure=_is_turn_state_discovery_failure,
        )
        if turn_state_recovery_candidate:
            candidates.append(turn_state_recovery_candidate)

        metric_field_missing_candidate = _build_metric_field_missing_recovery_candidate(
            results=results,
            last_idx=last_idx,
            last_result=last_result,
            last_tool=last_tool,
            dataset_hint=dataset_hint,
            objective_text=objective_text,
            is_metric_field_not_found_failure=_is_metric_field_not_found_failure,
            extract_missing_metric_field=_extract_missing_metric_field,
            find_related_tool_call=_find_related_tool_call,
            assistant_calls=assistant_calls,
            extract_metric_output_field_from_call=_extract_metric_output_field_from_call,
            resolve_dataset_hint_from_result=_resolve_dataset_hint_from_result,
            dataset_create_or_update_tools=_DATASET_CREATE_OR_UPDATE_TOOLS,
            objective_targets_problem_metric=_objective_targets_problem_metric,
            objective_explicit_population_metric=_objective_explicit_population_metric,
        )
        if metric_field_missing_candidate:
            candidates.append(metric_field_missing_candidate)

        save_cached_dataset_candidate = _build_save_cached_dataset_before_wait_candidate(
            results=results,
            last_idx=last_idx,
            last_result=last_result,
            last_tool=last_tool,
            dataset_name=dataset_name,
            related_call=related_call,
            tools_available=tools_available,
            classify_runtime_error_kind=_classify_runtime_error_kind,
            extract_dataset_name_from_call=_extract_dataset_name_from_call,
            latest_successful_tool_for_dataset=_latest_successful_tool_for_dataset,
            assistant_calls=assistant_calls,
            remote_cache_dataset_tools=_REMOTE_CACHE_DATASET_TOOLS,
            has_successful_save_data_to_map_for_dataset=_has_successful_save_data_to_map_for_dataset,
        )
        if save_cached_dataset_candidate:
            candidates.append(save_cached_dataset_candidate)

        zonal_freeze_candidate = _build_zonal_freeze_fallback_candidate(
            last_result=last_result,
            last_tool=last_tool,
            related_call=related_call,
            dataset_name=dataset_name,
            tools_available=tools_available,
            is_zonal_ui_freeze_failure=_is_zonal_ui_freeze_failure,
            infer_h3_resolution_from_text=_infer_h3_resolution_from_text,
            extract_zonal_values_count=_extract_zonal_values_count,
            replace_or_append_h3_resolution_suffix=_replace_or_append_h3_resolution_suffix,
        )
        if zonal_freeze_candidate:
            candidates.append(zonal_freeze_candidate)

        if (
            objective_requests_coloring(objective_text)
            and not _objective_explicit_population_metric(objective_text)
            and has_unresolved_zonal_freeze
            and has_recent_forest_context
        ):
            candidates.append(
                {
                    "ruleId": "coloring_after_unresolved_forest_metric_forbidden",
                    "score": 174,
                    "guardrail": (
                        "A prior forest-value workflow failed (zonalStatsByAdmin UI-freeze) and no resolved value metric is available yet. "
                        "Do not switch to population/name fallback styling."
                    ),
                    "next": (
                        "Do not use applyQMapStylePreset/population fallback and do not claim value-based coloring success. "
                        "Either report explicit analytical limitation, or continue only with a validated metric-producing workflow first."
                    ),
                }
            )

        if (
            last_tool == "setQMapLayerColorByField"
            and last_success
            and objective_requests_coloring(objective_text)
            and objective_requests_normalized_metric(objective_text)
        ):
            color_field = ""
            color_dataset_ref = _extract_dataset_ref_from_call(related_call)
            color_dataset_name = _extract_dataset_name_from_call(related_call)
            if isinstance(related_call, dict):
                args = related_call.get("args")
                if isinstance(args, dict):
                    raw_field = args.get("fieldName")
                    if isinstance(raw_field, str) and raw_field.strip():
                        color_field = raw_field.strip()
            if not is_likely_normalized_metric_field(color_field):
                color_dataset_hint = _build_dataset_hint(color_dataset_ref, color_dataset_name)
                if "createDatasetWithNormalizedField" in tools_available:
                    next_step = (
                        f"Normalization objective detected but latest color field \"{color_field or 'unknown'}\" appears absolute. "
                        f"Call createDatasetWithNormalizedField{color_dataset_hint} to derive a per-capita/per-100k metric, "
                        "then call waitForQMapDataset and countQMapRows, and finally call setQMapLayerColorByField on the derived normalized field."
                    )
                else:
                    next_step = (
                        "Do not claim normalized coloring. Explicitly report that only absolute-metric coloring is currently applied "
                        "because no normalization tool is available in this runtime."
                    )
                candidates.append(
                    {
                        "ruleId": "normalized_color_requires_derived_metric",
                        "score": 154,
                        "guardrail": (
                            "User requested normalized/per-capita coloring, but the latest successful color step appears to use a non-normalized field."
                        ),
                        "next": next_step,
                    }
                )

        if last_tool == "setQMapLayerColorByField" and last_result.get("success") is False:
            if _is_low_distinct_color_failure(last_result.get("details")):
                if objective_requests_coloring(objective_text):
                    color_dataset_ref = _extract_dataset_ref_from_call(related_call)
                    color_dataset_name = _extract_dataset_name_from_call(related_call)
                    color_dataset_hint = _build_dataset_hint(color_dataset_ref, color_dataset_name)
                    fallback_step = (
                        "Do not run alternate heavy pipelines to fabricate variance. "
                        "Do not claim that color-by-field was applied."
                    )
                    if "setQMapLayerSolidColor" in tools_available:
                        fallback_step += (
                            f" Call setQMapLayerSolidColor{color_dataset_hint} with a visible fallback color, "
                            "then finalize by explicitly stating that metric-based gradient coloring is not possible "
                            "because the metric is flat (distinct=1)."
                        )
                    else:
                        fallback_step += (
                            " Finalize by explicitly stating that metric-based gradient coloring is not possible "
                            "because the metric is flat (distinct=1)."
                        )
                    candidates.append(
                        {
                            "ruleId": "color_low_distinct_no_false_success",
                            "score": 152,
                            "guardrail": (
                                "Color objective detected, but setQMapLayerColorByField failed with low-distinct metric. "
                                "Avoid repeated recompute loops and avoid false success claims."
                            ),
                            "next": fallback_step,
                        }
                    )
                rank_idx = _latest_successful_tool_index(results, {"rankQMapDatasetRows"})
                if _objective_requires_ranked_output(objective_text) and rank_idx >= 0:
                    next_step = (
                        "Do not launch alternate heavy pipelines just to force color variance. "
                        "Finalize using ordered rankQMapDatasetRows evidence (name + metric) and explicitly state ties if metric is flat. "
                        "Do not invent placeholders/approximate values; use exact values returned by tools."
                    )
                    if objective_requests_charts(objective_text) and "bubbleChartTool" in tools_available:
                        next_step += " If a chart is requested, use bubbleChartTool with a real metric axis (not name-only category bars)."
                    candidates.append(
                        {
                            "ruleId": "ranking_low_distinct_no_alt_pipeline",
                            "score": 146,
                            "guardrail": (
                                "Color metric is flat in a ranking objective. "
                                "Do not continue with alternate join/aggregation loops to fabricate variance."
                            ),
                            "next": next_step,
                        }
                    )
                field_name = ""
                if isinstance(related_call, dict):
                    args = related_call.get("args")
                    if isinstance(args, dict):
                        raw_field = args.get("fieldName")
                        if isinstance(raw_field, str) and raw_field.strip():
                            field_name = raw_field.strip()
                scoped = ", ".join(
                    item
                    for item in (
                        f'datasetName="{dataset_ref or dataset_name}"' if (dataset_ref or dataset_name) else "",
                        f'fieldName="{field_name}"' if field_name else "",
                    )
                    if item
                )
                scope_text = f" ({scoped})" if scoped else ""
                inspect_tools = [
                    name
                    for name in ("previewQMapDatasetRows", "distinctQMapFieldValues", "searchQMapFieldValues")
                    if name in tools_available
                ]
                next_step = "Inspect value distribution first."
                if inspect_tools:
                    next_step = (
                        "Inspect distribution first with "
                        + " / ".join(inspect_tools)
                        + ", then choose a different field/dataset or report that coloring is not possible."
                    )
                if _objective_targets_problem_metric(objective_text) and not _objective_explicit_population_metric(
                    objective_text
                ):
                    next_step += " Do not replace the analytical metric with population/name unless the user explicitly requests it."
                candidates.append(
                    {
                        "ruleId": "color_low_distinct_recovery",
                        "score": 100,
                        "guardrail": (
                            "Previous setQMapLayerColorByField failed due low distinct values. "
                            f"Do not retry the same color call with identical arguments{scope_text}."
                        ),
                        "next": next_step,
                    }
                )

        if objective_requests_coloring(objective_text):
            latest_style_failure_idx = _latest_failed_tool_index(results, _STYLE_EXECUTION_TOOLS)
            if latest_style_failure_idx >= 0 and not _has_successful_tool_after_index(
                results, latest_style_failure_idx, _STYLE_EXECUTION_TOOLS
            ):
                candidates.append(
                    {
                        "ruleId": "coloring_failure_no_false_success_claim",
                        "score": 140,
                        "guardrail": (
                            "Recent style/color operations failed and no subsequent successful style step is available. "
                            "Do not claim that value-based coloring has been applied."
                        ),
                        "next": (
                            "Retry styling only with exact dataset/layer identifiers from listQMapDatasets, "
                            "or return explicit failure. Do not state that coloring is completed without a successful style tool result."
                        ),
                    }
                )

        if _has_repeated_discovery_loop(results):
            query_tools = [
                name
                for name in ("queryQCumberTerritorialUnits", "queryQCumberDatasetSpatial", "queryQCumberDataset")
                if name in tools_available
            ]
            next_step = (
                "Stop repeating discovery calls and return one concise clarification if provider/dataset selection is still ambiguous."
            )
            if query_tools:
                next_step = (
                    "Stop repeating discovery calls. Reuse provider/dataset metadata already returned and execute one query step with "
                    + " / ".join(query_tools)
                    + "."
                )
            candidates.append(
                {
                    "ruleId": "discovery_loop_progress",
                    "score": 95,
                    "guardrail": (
                        "Repeated discovery-only loop detected "
                        "(listQCumberProviders/listQCumberDatasets called multiple times without progress)."
                    ),
                    "next": next_step,
                    "responseModeHint": "clarification" if not query_tools else "",
                }
            )

        clip_stats_candidate = _build_clip_stats_clip_required_candidate(
            objective_text=objective_text,
            results=results,
            tools_available=tools_available,
            needs_cross_geometry_clip_guardrail=_needs_cross_geometry_clip_guardrail,
            h3_clip_tools=_H3_CLIP_TOOLS,
            build_source_dataset_hint=_build_source_dataset_hint,
            dataset_ref=dataset_ref,
            dataset_name=dataset_name,
        )
        if clip_stats_candidate:
            candidates.append(clip_stats_candidate)

        boundary_clip_candidate = _build_boundary_clip_required_candidate(
            objective_text=objective_text,
            results=results,
            tools_available=tools_available,
            needs_boundary_clip_guardrail=_needs_boundary_clip_guardrail,
            h3_clip_tools=_H3_CLIP_TOOLS,
            h3_boundary_materialization_tools=_H3_BOUNDARY_MATERIALIZATION_TOOLS,
            latest_successful_tool_index=_latest_successful_tool_index,
            find_related_tool_call=_find_related_tool_call,
            assistant_calls=assistant_calls,
            extract_dataset_name_from_call=_extract_dataset_name_from_call,
            extract_dataset_ref_from_call=_extract_dataset_ref_from_call,
            build_source_dataset_hint=_build_source_dataset_hint,
        )
        if boundary_clip_candidate:
            candidates.append(boundary_clip_candidate)

        overlay_coverage_candidate = _build_perimeter_overlay_coverage_candidate(
            objective_text=objective_text,
            results=results,
            tools_available=tools_available,
            needs_overlay_coverage_guardrail=_needs_overlay_coverage_guardrail,
            latest_successful_tool_index=_latest_successful_tool_index,
            overlay_execution_tools=_OVERLAY_EXECUTION_TOOLS,
            h3_clip_tools=_H3_CLIP_TOOLS,
            find_related_tool_call=_find_related_tool_call,
            assistant_calls=assistant_calls,
            extract_dataset_name_from_call=_extract_dataset_name_from_call,
            extract_dataset_ref_from_call=_extract_dataset_ref_from_call,
        )
        if overlay_coverage_candidate:
            candidates.append(overlay_coverage_candidate)

        if workflow_state.ranking_active:
            rank_idx = _latest_successful_tool_index(results, {"rankQMapDatasetRows"})
            admin_query_idx = _latest_successful_tool_index(results, {"queryQCumberTerritorialUnits"})
            if (
                _objective_targets_admin_units(objective_text)
                and objective_requests_map_display(objective_text)
                and admin_query_idx < 0
                and "queryQCumberTerritorialUnits" in tools_available
            ):
                candidates.append(
                    {
                        "ruleId": "admin_superlative_requires_territorial_query",
                        "score": 167,
                        "guardrail": (
                            "Administrative superlative-to-map objective detected, but no successful territorial query step exists yet. "
                            "Do not rank or materialize from a generic dataset snapshot only."
                        ),
                        "next": (
                            "Call queryQCumberTerritorialUnits first to materialize the administrative dataset, "
                            "then rank the result, isolate the winner, and fit it on map."
                        ),
                        "forceToolChoice": "queryQCumberTerritorialUnits",
                    }
                )
            if last_success and last_tool == "previewQMapDatasetRows" and _is_preview_head_sample_details(
                last_result.get("details")
            ):
                candidates.append(
                    {
                        "ruleId": "ranking_preview_sample_not_evidence",
                        "score": 143,
                        "guardrail": (
                            "Superlative/ranking objective detected but latest evidence is an unsorted preview sample. "
                            "Sampled head rows are not valid ranking evidence."
                        ),
                        "next": (
                            f"Call rankQMapDatasetRows{dataset_hint} with an explicit numeric metric field, "
                            "then finalize from ordered rows only."
                        ),
                    }
                )

            latest_create_idx = _latest_successful_tool_index(results, _DATASET_CREATE_OR_UPDATE_TOOLS)
            if latest_create_idx >= 0 and "rankQMapDatasetRows" in tools_available:
                wait_after_create_idx = _next_successful_tool_index(results, latest_create_idx + 1, "waitForQMapDataset")
                count_after_wait_idx = (
                    _next_successful_tool_index(results, wait_after_create_idx + 1, "countQMapRows")
                    if wait_after_create_idx >= 0
                    else -1
                )
                rank_after_count_idx = (
                    _next_successful_tool_index(results, count_after_wait_idx + 1, "rankQMapDatasetRows")
                    if count_after_wait_idx >= 0
                    else -1
                )
                prior_rank_before_create_idx = -1
                for idx in range(latest_create_idx - 1, -1, -1):
                    row = results[idx]
                    if row.get("success") is True and str(row.get("toolName") or "").strip() == "rankQMapDatasetRows":
                        prior_rank_before_create_idx = idx
                        break
                latest_create_tool = str(results[latest_create_idx].get("toolName") or "").strip()
                is_isolated_superlative_winner = (
                    latest_create_tool in {"createDatasetFromFilter", "createDatasetFromCurrentFilters"}
                    and prior_rank_before_create_idx >= 0
                    and workflow_state.admin_superlative_map_workflow
                )
                if (
                    count_after_wait_idx >= 0
                    and is_isolated_superlative_winner
                    and "fitQMapToDataset" in tools_available
                    and _next_successful_tool_index(results, count_after_wait_idx + 1, "fitQMapToDataset") < 0
                ):
                    count_result = results[count_after_wait_idx]
                    _, _, post_count_dataset_hint = _resolve_dataset_hint_from_result(count_result, assistant_calls)
                    candidates.append(
                        {
                            "ruleId": "admin_superlative_isolated_winner_requires_fit",
                            "score": 159,
                            "guardrail": (
                                "Administrative superlative winner was already isolated after ranking, "
                                "but map focus is still missing. Do not finalize with 'shown on map' claims yet."
                            ),
                            "next": f"Call fitQMapToDataset{post_count_dataset_hint} before final response.",
                            "forceToolChoice": "fitQMapToDataset",
                        }
                    )
                elif count_after_wait_idx >= 0 and rank_after_count_idx < 0:
                    count_result = results[count_after_wait_idx]
                    _, _, post_count_dataset_hint = _resolve_dataset_hint_from_result(count_result, assistant_calls)
                    candidates.append(
                        {
                            "ruleId": "completion_contract_wait_count_rank",
                            "score": 151,
                            "guardrail": (
                                "Completion contract for ranking flow is incomplete: wait+count succeeded, "
                                "but rank step is still missing. Do not finalize yet."
                            ),
                            "next": (
                                f"Call rankQMapDatasetRows{post_count_dataset_hint} with explicit metric field before final response."
                            ),
                        }
                    )

            if rank_idx < 0 and "rankQMapDatasetRows" in tools_available:
                candidates.append(
                    {
                        "ruleId": "ranking_requires_rank_step",
                        "score": 114,
                        "guardrail": (
                            "Ranking/superlative objective detected but no successful rank step is available yet. "
                            "Do not finalize with generic narrative."
                        ),
                        "next": (
                            "Call rankQMapDatasetRows with an explicit numeric metric field and include name+metric fields."
                        ),
                    }
                )
            elif rank_idx >= 0:
                chart_name_only_by_result = _is_uninformative_category_name_chart_result(last_result.get("details"))
                if (
                    (last_tool == "categoryBarsTool" and _is_uninformative_category_name_chart_call(related_call))
                    or chart_name_only_by_result
                ):
                    candidates.append(
                        {
                            "ruleId": "ranking_chart_name_only_not_evidence",
                            "score": 141,
                            "guardrail": (
                                "Ranking objective detected, but the latest chart is category-only on `name` "
                                "without a metric axis. This is not valid ranking evidence."
                            ),
                            "next": (
                                "Do not finalize yet. Report ordered ranking rows from rankQMapDatasetRows "
                                "(name + metric). If charts are requested, use a metric-based chart."
                            ),
                        }
                    )
                tie_note = ""
                if _has_low_distinct_color_failure(results):
                    tie_note = (
                        " If the ranked metric is uniform/flat, explicitly state that regions are tied "
                        "instead of claiming a unique top region."
                    )
                candidates.append(
                    {
                        "ruleId": "ranking_evidence_in_final_answer",
                        "score": 109,
                        "guardrail": (
                            "Ranking/superlative objective detected. "
                            "Do not finalize with generic summary only."
                        ),
                        "next": (
                            "Before finalizing, report an ordered Top-N list from the latest successful "
                            "rankQMapDatasetRows output (name + metric)." + tie_note
                        ),
                    }
                )

            if (
                _objective_targets_forest_metric(objective_text)
                and _objective_targets_admin_units(objective_text)
                and not objective_requests_normalized_metric(objective_text)
            ):
                zonal_idx = _latest_successful_tool_index(results, {"zonalStatsByAdmin"})
                zonal_dataset_hint = ""
                zonal_metric_field = ""
                if zonal_idx >= 0:
                    zonal_result = results[zonal_idx]
                    zonal_call = _find_related_tool_call(zonal_result, assistant_calls)
                    zonal_args = zonal_call.get("args") if isinstance(zonal_call, dict) else {}
                    zonal_args = zonal_args if isinstance(zonal_args, dict) else {}
                    output_field = str(zonal_args.get("outputFieldName") or "").strip()
                    zonal_metric_field = output_field or "zonal_value"
                    zonal_dataset_name = str(zonal_args.get("newDatasetName") or "").strip()
                    zonal_dataset_ref = _extract_dataset_ref_from_call(zonal_call) or str(zonal_result.get("datasetRef") or "")
                    zonal_dataset_hint = _build_dataset_hint(zonal_dataset_ref, zonal_dataset_name)
                if zonal_dataset_hint and "createDatasetWithGeometryArea" in tools_available and "createDatasetWithNormalizedField" in tools_available:
                    normalized_field_name = f"{zonal_metric_field}_pct_area"
                    candidates.append(
                        {
                            "ruleId": "forest_superlative_dual_metric_no_size_bias",
                            "score": 148,
                            "guardrail": (
                                "Forest superlative on administrative units is vulnerable to area-size bias "
                                "if using only absolute forest area."
                            ),
                            "next": (
                                f"Compute dual evidence before final ranking: call createDatasetWithGeometryArea{zonal_dataset_hint} "
                                f'with areaFieldName="area_m2", then waitForQMapDataset and countQMapRows; '
                                f"then call createDatasetWithNormalizedField{zonal_dataset_hint} with "
                                f'numeratorFieldName="{zonal_metric_field}", denominatorFieldName="area_m2", '
                                f'outputFieldName="{normalized_field_name}", multiplier=100. '
                                "Then waitForQMapDataset, countQMapRows, and rank both absolute and normalized metrics. "
                                "In final answer report both rankings and avoid claiming a unique winner from absolute-only metric."
                            ),
                        }
                    )

        if candidates:
            best = max(candidates, key=lambda item: int(item.get("score") or 0))
            forced_tool_choice_name = str(best.get("forceToolChoice") or "").strip()
            if forced_tool_choice_name and forced_tool_choice_name in tools_available:
                outgoing["tool_choice"] = {"type": "function", "function": {"name": forced_tool_choice_name}}
            guidance_lines.append(
                f"{_RUNTIME_GUARDRAIL_PREFIX} Selected rule `{best.get('ruleId')}` (score={int(best.get('score') or 0)})."
            )
            guardrail_text = str(best.get("guardrail") or "").strip()
            next_text = str(best.get("next") or "").strip()
            if guardrail_text:
                guidance_lines.append(f"{_RUNTIME_GUARDRAIL_PREFIX} {guardrail_text}")
            if next_text:
                guidance_lines.append(f"{_RUNTIME_NEXT_STEP_PREFIX} {next_text}")
            response_mode_hint = str(best.get("responseModeHint") or "").strip().lower()
            if response_mode_hint in {"clarification", "limitation"}:
                guidance_lines.append(f"{_RUNTIME_RESPONSE_MODE_PREFIX} {response_mode_hint}")

    # ── Display-intent showOnMap enforcement ──
    # When the user objective starts with a display verb ("mostra", "show", …),
    # inject guidance on every sub-request so the model uses showOnMap=true for
    # the final data query that satisfies the user request.
    if objective_requests_map_display(objective_text):
        guidance_lines.append(
            f"{_RUNTIME_GUARDRAIL_PREFIX} "
            "Display objective detected. The final q-cumber data query (the one that "
            "satisfies the user request) MUST use showOnMap=true so the result is visible "
            "on the map. Use showOnMap=false only for intermediate/boundary datasets."
        )

    if not results:
        if "listQMapDatasets" in tools_available and not _objective_requests_dataset_discovery(objective_text):
            guidance_lines.append(
                f"{_RUNTIME_GUARDRAIL_PREFIX} "
                "Avoid listQMapDatasets as default first step. Reuse explicit dataset names from the user goal and"
                " call listQMapDatasets only when inventory/discovery is requested or after snapshot-related failures."
            )
        if _objective_targets_forest_metric(objective_text) and {
            "queryQCumberDatasetSpatial",
            "queryQCumberDataset",
        }.intersection(tools_available):
            guidance_lines.append(
                f"{_RUNTIME_GUARDRAIL_PREFIX} "
                "For q-cumber attribute filters, operator \"in\" requires `values` array. "
                "Do not pass array payloads under `value`."
            )
            guidance_lines.append(
                f"{_RUNTIME_NEXT_STEP_PREFIX} "
                "For CLC forest classes use filters like "
                '[{"field":"code_18","op":"in","values":["311","312","313"]}].'
            )
        if (
            _objective_targets_forest_metric(objective_text)
            and objective_requests_normalized_metric(objective_text)
            and "zonalStatsByAdmin" in tools_available
        ):
            guidance_lines.append(
                f"{_RUNTIME_GUARDRAIL_PREFIX} "
                "Forest percentage objective detected: do not finalize with absolute area sum as if it were percent."
            )
            guidance_lines.append(
                f"{_RUNTIME_NEXT_STEP_PREFIX} "
                "Materialize a normalized percent field first (e.g. forest_area / cell_area * 100), "
                "then style/rank using that percent metric."
            )
        if "queryQCumberTerritorialUnits" in tools_available and _objective_targets_admin_units(objective_text):
            guidance_lines.append(
                f"{_RUNTIME_GUARDRAIL_PREFIX} "
                "For queryQCumberTerritorialUnits keep filters atomized. "
                'Never encode operator+value in `op` (invalid example: `op=\"eq,value:7\"`).'
            )
            guidance_lines.append(
                f"{_RUNTIME_NEXT_STEP_PREFIX} "
                "Use canonical filters like "
                '[{"field":"name","op":"eq","value":"Brescia"},{"field":"lv","op":"eq","value":7}].'
            )
        if _objective_requires_ranked_output(objective_text) and "rankQMapDatasetRows" in tools_available:
            guidance_lines.append(
                f"{_RUNTIME_GUARDRAIL_PREFIX} "
                "For rankQMapDatasetRows use canonical args only: "
                "{datasetName, metricFieldName, topN?, sortDirection?}."
            )
        if "zonalStatsByAdmin" in tools_available and (
            _objective_targets_forest_metric(objective_text)
            or _objective_targets_admin_units(objective_text)
            or _objective_requires_ranked_output(objective_text)
        ):
            guidance_lines.append(
                f"{_RUNTIME_GUARDRAIL_PREFIX} "
                "For zonalStatsByAdmin use canonical args: "
                "{adminDatasetName, valueDatasetName, valueField?, aggregation?, weightMode?, outputFieldName?, showOnMap?, newDatasetName?}. "
                "Do not use non-canonical keys like targetDatasetName/adminNameField/targetValueFieldName/operations."
            )

    cleaned_messages: list[dict[str, Any]] = []
    for raw_msg in messages:
        if not isinstance(raw_msg, dict):
            continue
        msg = dict(raw_msg)
        role = str(msg.get("role") or "").strip().lower()
        if role != "system":
            cleaned_messages.append(msg)
            continue
        content = msg.get("content")
        if not isinstance(content, str):
            cleaned_messages.append(msg)
            continue
        lines = [
            line
            for line in content.splitlines()
            if not line.strip().startswith(_RUNTIME_GUARDRAIL_PREFIX)
            and not line.strip().startswith(_RUNTIME_NEXT_STEP_PREFIX)
        ]
        msg["content"] = "\n".join(lines).strip()
        cleaned_messages.append(msg)

    if not guidance_lines:
        outgoing["messages"] = cleaned_messages
        return outgoing

    guardrail_block = "\n".join(guidance_lines).strip()
    for idx, msg in enumerate(cleaned_messages):
        if str(msg.get("role") or "").strip().lower() != "system":
            continue
        content = str(msg.get("content") or "").strip()
        updated = dict(msg)
        updated["content"] = "\n".join(part for part in (content, guardrail_block) if part).strip()
        cleaned_messages[idx] = updated
        outgoing["messages"] = cleaned_messages
        return outgoing

    outgoing["messages"] = [{"role": "system", "content": guardrail_block}, *cleaned_messages]
    return outgoing
