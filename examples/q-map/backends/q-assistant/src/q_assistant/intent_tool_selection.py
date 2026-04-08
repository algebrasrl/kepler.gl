"""Intent-based tool selection for LLM provider requests.

Google Gemini Flash returns empty completions (HTTP 200, 0 tokens) when the
tool schema is too large (>20 tools).  This module reduces the tool set from
~99 to 18-25 per request by selecting tools based on the user's objective.

Pipeline position: runs AFTER ``_prune_forbidden_qmap_runtime_tools`` and
BEFORE fine-grained prune functions in ``main.py``.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Callable

from .objective_intent import (
    _objective_mentions_cloud_or_saved_maps,
    _objective_requests_chart_visualization,
    _objective_requests_exposure_assessment,
    _objective_requests_field_correlation,
    _objective_requests_geospatial_analysis,
    _objective_requests_h3_editing,
    _objective_requests_h3_tessellation,
    _objective_requests_linear_regression,
    _objective_requests_load_save,
    _objective_requests_mobility,
    _objective_requests_natural_break_classification,
    _objective_requests_regulatory_compliance,
    _objective_requests_regulatory_listing,
    _objective_requests_spatial_interpolation,
    _objective_requests_styling_full,
    _objective_requests_time_series,
)

# ---------------------------------------------------------------------------
# Core tools — always included regardless of user intent
# ---------------------------------------------------------------------------

CORE_TOOLS: frozenset[str] = frozenset({
    # Meta-discovery (allow model to see category structure)
    "listQMapToolCategories",
    "listQMapToolsByCategory",
    # Dataset discovery chain
    "listQMapDatasets",
    "listQCumberProviders",
    "listQCumberDatasets",
    "getQCumberDatasetHelp",
    # Workflow control
    "waitForQMapDataset",
    "countQMapRows",
    "setQMapFieldEqualsFilter",
    # Map interaction essentials
    "fitQMapToDataset",
})

# ---------------------------------------------------------------------------
# Balanced-lite — default query + styling for most requests
# ---------------------------------------------------------------------------

BALANCED_LITE: frozenset[str] = frozenset({
    # Query/inspection essentials
    "queryQCumberTerritorialUnits",
    "queryQCumberDatasetSpatial",
    "queryQCumberDataset",
    "previewQMapDatasetRows",
    "rankQMapDatasetRows",
    "deriveQMapDatasetBbox",
    # Styling essentials
    "basemap",
    "setQMapTooltipFields",
    "setQMapLayerColorByField",
    "setQMapLayerVisibility",
    "showOnlyQMapLayer",
    "setQMapLayerOrder",
})

# ---------------------------------------------------------------------------
# Intent rules — each maps a detector → additional tool names
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class IntentRule:
    name: str
    detector: Callable[[str], bool]
    tools: frozenset[str]


_CHART_TOOLS = frozenset({
    "wordCloudTool", "categoryBarsTool", "histogramTool", "boxplotTool",
    "bubbleChartTool", "pcpTool", "lineChartTool", "scatterplotTool",
    "listQMapChartTools",
})

_MOBILITY_TOOLS = frozenset({
    "geocoding", "routing", "isochrone", "roads",
})

_GEOSPATIAL_CORE_TOOLS = frozenset({
    "createDatasetFromFilter", "createDatasetFromCurrentFilters",
    "mergeQMapDatasets", "clipQMapDatasetByGeometry", "clipDatasetByBoundary",
    "spatialJoinByPredicate", "zonalStatsByAdmin", "dissolveQMapDatasetByField",
    "addComputedField", "createDatasetWithNormalizedField",
    "createDatasetWithGeometryArea", "bufferAndSummarize",
    "centroidOfDataset", "circleBufferFromPoint", "drawGeometryToDataset",
})

_GEOSPATIAL_H3_TOOLS = frozenset({
    "tassellateSelectedGeometry", "tassellateDatasetLayer",
    "aggregateDatasetToH3", "joinQMapDatasetsOnH3",
    "populateTassellationFromAdminUnits",
})

_GEOSPATIAL_OVERLAY_TOOLS = frozenset({
    "overlayDifference", "overlayUnion", "overlayIntersection",
    "overlaySymmetricDifference", "eraseQMapDatasetByGeometry",
})

_GEOSPATIAL_ADVANCED_TOOLS = frozenset({
    "nearestFeatureJoin", "adjacencyGraphFromPolygons",
    "coverageQualityReport", "computeQMapSpatialAutocorrelation",
    "computeQMapEquityIndices", "computeQMapBivariateCorrelation",
    "computeQMapHotspotAnalysis", "computeQMapCompositeIndex",
    "computeQMapDatasetDelta", "interpolateIDW",
    "reprojectQMapDatasetCrs", "simplifyQMapDatasetGeometry",
    "splitQMapPolygonByLine", "drawQMapBoundingBox",
})

_STATISTICAL_TOOLS = frozenset({
    "regressQMapFields", "classifyQMapFieldBreaks", "correlateQMapFields",
})

_REGULATORY_TOOLS = frozenset({
    "listRegulatoryThresholds", "checkRegulatoryCompliance",
    "assessPopulationExposure",
})

_CLOUD_MAP_TOOLS = frozenset({
    "loadData", "saveDataToMap", "loadQMapCloudMap", "loadCloudMapAndWait",
    "listQMapCloudMaps",
})

_STYLING_FULL_EXTRA = frozenset({
    "setQMapLayerSolidColor", "setQMapLayerHeightByField",
    "applyQMapStylePreset", "setQMapLayerColorByThresholds",
    "setQMapLayerColorByStatsThresholds", "openQMapPanel", "mapBoundary",
})

_H3_EDITING_TOOLS = frozenset({
    "paintQMapH3Cell", "paintQMapH3Cells", "paintQMapH3Ring",
})

_TIME_SERIES_TOOLS = frozenset({
    "summarizeQMapTimeSeries", "aggregateQMapTimeSeries",
    "grammarAnalyzeTool",
})

_EXTRA_INSPECTION_TOOLS = frozenset({
    "distinctQMapFieldValues", "searchQMapFieldValues",
    "describeQMapField", "computeQMapDataQualityReport",
    "debugQMapActiveFilters",
})


INTENT_RULES: tuple[IntentRule, ...] = (
    IntentRule("chart", _objective_requests_chart_visualization, _CHART_TOOLS),
    IntentRule("mobility", _objective_requests_mobility, _MOBILITY_TOOLS),
    IntentRule("geospatial-core", _objective_requests_geospatial_analysis, _GEOSPATIAL_CORE_TOOLS),
    IntentRule("geospatial-h3", _objective_requests_h3_tessellation, _GEOSPATIAL_H3_TOOLS),
    IntentRule("geospatial-overlay", _objective_requests_geospatial_analysis, _GEOSPATIAL_OVERLAY_TOOLS),
    IntentRule("geospatial-advanced", _objective_requests_geospatial_analysis, _GEOSPATIAL_ADVANCED_TOOLS),
    IntentRule(
        "statistical",
        lambda obj: (
            _objective_requests_linear_regression(obj)
            or _objective_requests_field_correlation(obj)
            or _objective_requests_natural_break_classification(obj)
        ),
        _STATISTICAL_TOOLS,
    ),
    IntentRule(
        "regulatory",
        lambda obj: (
            _objective_requests_regulatory_compliance(obj)
            or _objective_requests_regulatory_listing(obj)
            or _objective_requests_exposure_assessment(obj)
        ),
        _REGULATORY_TOOLS,
    ),
    IntentRule("cloud-maps", _objective_mentions_cloud_or_saved_maps, _CLOUD_MAP_TOOLS),
    IntentRule("styling-full", _objective_requests_styling_full, _STYLING_FULL_EXTRA),
    IntentRule("h3-editing", _objective_requests_h3_editing, _H3_EDITING_TOOLS),
    IntentRule("time-series", _objective_requests_time_series, _TIME_SERIES_TOOLS),
    IntentRule("spatial-interpolation", _objective_requests_spatial_interpolation, _GEOSPATIAL_ADVANCED_TOOLS),
    IntentRule("load-save", _objective_requests_load_save, _CLOUD_MAP_TOOLS),
)

# Category drop priority for hard-cap enforcement (lowest priority dropped first).
_DROP_PRIORITY: tuple[frozenset[str], ...] = (
    _H3_EDITING_TOOLS,
    _GEOSPATIAL_ADVANCED_TOOLS,
    _GEOSPATIAL_OVERLAY_TOOLS,
    _REGULATORY_TOOLS,
    _STATISTICAL_TOOLS,
    _TIME_SERIES_TOOLS,
    _MOBILITY_TOOLS,
    _CHART_TOOLS,
    _CLOUD_MAP_TOOLS,
    _STYLING_FULL_EXTRA,
    _EXTRA_INSPECTION_TOOLS,
    _GEOSPATIAL_H3_TOOLS,
    _GEOSPATIAL_CORE_TOOLS,
    # CORE_TOOLS and BALANCED_LITE are never dropped
)

# Environment variable for hard cap.
_MAX_TOOLS_DEFAULT = 20


def _read_max_tools_per_request() -> int:
    raw = os.environ.get("Q_ASSISTANT_MAX_TOOLS_PER_REQUEST", "")
    try:
        return max(10, int(str(raw).strip()))
    except (ValueError, TypeError):
        return _MAX_TOOLS_DEFAULT


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class IntentToolSelectionResult:
    matched_rules: tuple[str, ...]
    selected_tools: frozenset[str]
    total_before: int
    total_after: int


def select_tools_for_request(
    available_tools: set[str],
    objective_text: str,
    *,
    max_tools: int = 0,
) -> IntentToolSelectionResult:
    """Select tools for an LLM request based on the user's objective.

    Returns a result containing the tool names to KEEP (whitelist).
    """
    if max_tools <= 0:
        max_tools = _read_max_tools_per_request()

    total_before = len(available_tools)

    # Start with core + balanced-lite
    keep: set[str] = set(CORE_TOOLS) | set(BALANCED_LITE)

    # Run intent rules and add matched tool sets
    matched: list[str] = []
    for rule in INTENT_RULES:
        try:
            if rule.detector(objective_text):
                keep.update(rule.tools)
                matched.append(rule.name)
        except Exception:
            pass

    # If any geospatial intent matched, also add extra inspection tools
    if any(r.startswith("geospatial") for r in matched):
        keep.update(_EXTRA_INSPECTION_TOOLS)

    # Intersect with actually available tools (don't add tools the frontend didn't offer)
    keep.intersection_update(available_tools)

    # Hard cap: drop lowest-priority categories until within budget
    if len(keep) > max_tools:
        for drop_set in _DROP_PRIORITY:
            if len(keep) <= max_tools:
                break
            # Only drop tools that are NOT in core/balanced
            droppable = drop_set - CORE_TOOLS - BALANCED_LITE
            keep -= droppable

    # Final safety: if still over cap, truncate (should not happen with proper priority)
    if len(keep) > max_tools:
        protected = keep & (CORE_TOOLS | BALANCED_LITE)
        expendable = sorted(keep - protected)
        keep = protected | set(expendable[: max(0, max_tools - len(protected))])

    return IntentToolSelectionResult(
        matched_rules=tuple(matched),
        selected_tools=frozenset(keep),
        total_before=total_before,
        total_after=len(keep),
    )


def apply_intent_tool_selection(
    payload: dict[str, Any],
    *,
    objective_text: str,
    max_tools: int = 0,
) -> dict[str, Any]:
    """Filter tools in an OpenAI-compatible payload by user intent.

    Returns the payload with ``tools`` reduced to the intent-selected set.
    """
    tools = payload.get("tools")
    if not isinstance(tools, list) or not tools:
        return payload

    # Extract current tool names
    available: set[str] = set()
    for tool in tools:
        if isinstance(tool, dict):
            fn = tool.get("function")
            name = str(fn.get("name") or "").strip() if isinstance(fn, dict) else ""
            if name:
                available.add(name)

    result = select_tools_for_request(available, objective_text, max_tools=max_tools)

    # Filter tools list
    filtered = [
        tool
        for tool in tools
        if isinstance(tool, dict)
        and str((tool.get("function") or {}).get("name") or "").strip() in result.selected_tools
    ]
    payload["tools"] = filtered

    # If tool_choice points to a pruned tool, reset to auto
    tool_choice = payload.get("tool_choice")
    if isinstance(tool_choice, dict):
        fn = tool_choice.get("function")
        choice_name = str(fn.get("name") or "").strip() if isinstance(fn, dict) else ""
        if choice_name and choice_name not in result.selected_tools:
            payload["tool_choice"] = "auto"

    return payload
