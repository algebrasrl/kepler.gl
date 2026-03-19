import json
from functools import partial

from q_assistant.message_text import _extract_prompt_from_messages
from q_assistant.objective_intent import _objective_requires_ranked_output
from q_assistant.request_tool_results import _extract_request_tool_results
from q_assistant.runtime_guardrails import (
    _inject_runtime_guardrail_message,
    enforce_runtime_tool_loop_limits as _enforce_runtime_tool_loop_limits,
    is_likely_normalized_metric_field as _is_likely_normalized_metric_field,
    objective_requests_coloring as _objective_requests_coloring,
    objective_requests_normalized_metric as _objective_requests_normalized_metric,
    prune_forbidden_qmap_runtime_tools as _prune_forbidden_qmap_runtime_tools,
    prune_heavy_recompute_tools_after_low_distinct_color_failure as _prune_heavy_recompute_tools_after_low_distinct_color_failure,
    prune_open_panel_only_chart_navigation as _prune_open_panel_only_chart_navigation,
    prune_population_style_tools_for_unresolved_value_coloring as _prune_population_style_tools_for_unresolved_value_coloring,
    prune_repeated_discovery_tools as _prune_repeated_discovery_tools,
    prune_sampling_preview_tools_for_superlatives as _prune_sampling_preview_tools_for_superlatives,
    prune_uninformative_chart_tools_for_ranking as _prune_uninformative_chart_tools_for_ranking,
    summarize_runtime_tool_policy as _summarize_runtime_tool_policy,
)
from q_assistant.runtime_workflow_state import build_runtime_workflow_state
from q_assistant.services.request_processor import (
    _DISCOVERY_LOOP_PROGRESS_TOOLS,
    _DISCOVERY_LOOP_PRUNE_TOOLS,
    _FORBIDDEN_QMAP_RUNTIME_TOOLS,
    _has_recent_forest_clc_query_call,
    _has_repeated_discovery_loop,
    _has_unresolved_zonal_ui_freeze_failure,
    _is_low_distinct_color_failure,
    _is_metric_field_not_found_failure,
    _latest_successful_tool_index,
    _objective_explicit_category_distribution,
    _objective_explicit_population_metric,
    _runtime_guardrail_injection_bindings,
    _runtime_tool_loop_limit_bindings,
)
from q_assistant.tool_calls import _extract_assistant_tool_calls, _extract_request_tool_names

_prune_repeated_discovery_tools = partial(
    _prune_repeated_discovery_tools,
    extract_request_tool_results=_extract_request_tool_results,
    has_repeated_discovery_loop=_has_repeated_discovery_loop,
    extract_request_tool_names=_extract_request_tool_names,
    discovery_loop_progress_tools=_DISCOVERY_LOOP_PROGRESS_TOOLS,
    discovery_loop_prune_tools=_DISCOVERY_LOOP_PRUNE_TOOLS,
)
_prune_forbidden_qmap_runtime_tools = partial(
    _prune_forbidden_qmap_runtime_tools,
    forbidden_qmap_runtime_tools=_FORBIDDEN_QMAP_RUNTIME_TOOLS,
)
_prune_open_panel_only_chart_navigation = partial(
    _prune_open_panel_only_chart_navigation,
    extract_prompt_from_messages=_extract_prompt_from_messages,
)
_prune_uninformative_chart_tools_for_ranking = partial(
    _prune_uninformative_chart_tools_for_ranking,
    extract_prompt_from_messages=_extract_prompt_from_messages,
    objective_requires_ranked_output=_objective_requires_ranked_output,
    objective_explicit_category_distribution=_objective_explicit_category_distribution,
)
_prune_sampling_preview_tools_for_superlatives = partial(
    _prune_sampling_preview_tools_for_superlatives,
    extract_prompt_from_messages=_extract_prompt_from_messages,
    objective_requires_ranked_output=_objective_requires_ranked_output,
    extract_request_tool_names=_extract_request_tool_names,
    extract_request_tool_results=_extract_request_tool_results,
    latest_successful_tool_index=_latest_successful_tool_index,
    is_metric_field_not_found_failure=_is_metric_field_not_found_failure,
)
_prune_population_style_tools_for_unresolved_value_coloring = partial(
    _prune_population_style_tools_for_unresolved_value_coloring,
    extract_prompt_from_messages=_extract_prompt_from_messages,
    objective_requests_coloring=_objective_requests_coloring,
    objective_explicit_population_metric=_objective_explicit_population_metric,
    extract_request_tool_results=_extract_request_tool_results,
    has_unresolved_zonal_ui_freeze_failure=_has_unresolved_zonal_ui_freeze_failure,
    extract_assistant_tool_calls=_extract_assistant_tool_calls,
    has_recent_forest_clc_query_call=_has_recent_forest_clc_query_call,
)
_prune_heavy_recompute_tools_after_low_distinct_color_failure = partial(
    _prune_heavy_recompute_tools_after_low_distinct_color_failure,
    extract_prompt_from_messages=_extract_prompt_from_messages,
    objective_requests_coloring=_objective_requests_coloring,
    extract_request_tool_results=_extract_request_tool_results,
    is_low_distinct_color_failure=_is_low_distinct_color_failure,
)
_enforce_runtime_tool_loop_limits = partial(
    _enforce_runtime_tool_loop_limits,
    bindings=_runtime_tool_loop_limit_bindings(),
)
_inject_runtime_guardrail_message = partial(
    _inject_runtime_guardrail_message,
    bindings=_runtime_guardrail_injection_bindings(),
)


def _qmap_tool_result(*, success: bool, details: str, schema: str = "qmap.tool_result.v1") -> str:
    return json.dumps(
        {
            "qmapToolResult": {
                "schema": schema,
                "success": success,
                "details": details,
            }
        }
    )

