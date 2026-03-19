from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Callable


def _normalize_runtime_token(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(value or "").strip().lower()).strip("_")


def has_admin_workflow_signal(results: list[dict[str, Any]]) -> bool:
    for row in reversed(results or []):
        if not isinstance(row, dict):
            continue
        tool_name = str(row.get("toolName") or "").strip()
        if row.get("success") is True and tool_name == "queryQCumberTerritorialUnits":
            return True
        routing_is_administrative = row.get("routingIsAdministrative")
        if isinstance(routing_is_administrative, bool) and routing_is_administrative:
            return True
        routing_preferred_tool = str(row.get("routingPreferredTool") or "").strip()
        if routing_preferred_tool == "queryQCumberTerritorialUnits":
            return True
        dataset_class = _normalize_runtime_token(row.get("datasetClass"))
        if dataset_class == "administrative":
            return True
    return False


def has_rank_workflow_signal(results: list[dict[str, Any]]) -> bool:
    for row in reversed(results or []):
        if not isinstance(row, dict):
            continue
        tool_name = str(row.get("toolName") or "").strip()
        if row.get("success") is True and tool_name == "rankQMapDatasetRows":
            return True
    return False


_MUTATION_TOOLS_FOR_FIT_PRESERVATION: frozenset[str] = frozenset(
    {
        "tassellateSelectedGeometry",
        "tassellateDatasetLayer",
        "aggregateDatasetToH3",
        "joinQMapDatasetsOnH3",
        "populateTassellationFromAdminUnits",
        "populateTassellationFromAdminUnitsAreaWeighted",
        "populateTassellationFromAdminUnitsDiscrete",
        "clipQMapDatasetByGeometry",
        "clipDatasetByBoundary",
        "overlayDifference",
        "spatialJoinByPredicate",
        "zonalStatsByAdmin",
        "bufferAndSummarize",
        "nearestFeatureJoin",
        "queryQCumberTerritorialUnits",
        "queryQCumberDataset",
        "queryQCumberDatasetSpatial",
    }
)


def has_mutation_workflow_signal(results: list[dict[str, Any]]) -> bool:
    """Detect successful dataset-creating tools that produce visual output."""
    for row in reversed(results or []):
        if not isinstance(row, dict):
            continue
        tool_name = str(row.get("toolName") or "").strip()
        if row.get("success") is True and tool_name in _MUTATION_TOOLS_FOR_FIT_PRESERVATION:
            return True
    return False


@dataclass(frozen=True)
class RuntimeWorkflowState:
    has_admin_workflow_signal: bool
    has_rank_workflow_signal: bool
    has_mutation_workflow_signal: bool
    ranking_objective_requested: bool
    admin_units_requested: bool
    map_display_requested: bool
    admin_map_display_objective: bool
    ranking_active: bool
    admin_superlative_map_workflow: bool
    preserve_fit_without_explicit_map_focus: bool


def build_runtime_workflow_state(
    *,
    results: list[dict[str, Any]],
    objective_text: str,
    objective_targets_admin_units: Callable[[str], bool],
    objective_requests_map_display: Callable[[str], bool],
    objective_requires_ranked_output: Callable[[str], bool],
) -> RuntimeWorkflowState:
    admin_signal = has_admin_workflow_signal(results)
    rank_signal = has_rank_workflow_signal(results)
    mutation_signal = has_mutation_workflow_signal(results)
    ranking_objective_requested = objective_requires_ranked_output(objective_text)
    admin_units_requested = objective_targets_admin_units(objective_text)
    map_display_requested = objective_requests_map_display(objective_text)
    admin_map_display_objective = admin_units_requested and map_display_requested
    ranking_active = ranking_objective_requested or rank_signal
    admin_superlative_map_workflow = ranking_active and (admin_map_display_objective or admin_signal)
    return RuntimeWorkflowState(
        has_admin_workflow_signal=admin_signal,
        has_rank_workflow_signal=rank_signal,
        has_mutation_workflow_signal=mutation_signal,
        ranking_objective_requested=ranking_objective_requested,
        admin_units_requested=admin_units_requested,
        map_display_requested=map_display_requested,
        admin_map_display_objective=admin_map_display_objective,
        ranking_active=ranking_active,
        admin_superlative_map_workflow=admin_superlative_map_workflow,
        preserve_fit_without_explicit_map_focus=(admin_signal and rank_signal) or mutation_signal,
    )
