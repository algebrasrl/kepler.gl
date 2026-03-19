from __future__ import annotations

from dataclasses import dataclass
import json
import re
from typing import Any, Callable


@dataclass(frozen=True)
class RuntimeLoopRuleDecision:
    remove_tool_names: set[str]
    guidance_lines: list[str]
    forced_tool_choice_name: str = ""
    force_finalize_without_tools: bool = False


def apply_runtime_loop_rule_decision(
    *,
    remove_tool_names: set[str],
    guidance_lines: list[str],
    forced_tool_choice_name: str,
    force_finalize_without_tools: bool,
    decision: RuntimeLoopRuleDecision,
) -> tuple[str, bool]:
    if decision.remove_tool_names:
        remove_tool_names.update(decision.remove_tool_names)
    if decision.guidance_lines:
        guidance_lines.extend(decision.guidance_lines)
    return (
        decision.forced_tool_choice_name or forced_tool_choice_name,
        force_finalize_without_tools or decision.force_finalize_without_tools,
    )


def _first_successful_tool_index(
    results: list[dict[str, Any]],
    tool_names: set[str],
    *,
    start_idx: int = 0,
) -> int:
    if start_idx < 0:
        start_idx = 0
    for idx in range(start_idx, len(results)):
        row = results[idx]
        name = str(row.get("toolName") or "").strip()
        if row.get("success") is True and name in tool_names:
            return idx
    return -1


_DATASET_NOT_FOUND_RE = re.compile(r'dataset\s+"(?P<name>[^"]+)"\s+not found', re.IGNORECASE)
_NOT_MATERIALIZED_RE = re.compile(r'not materialized yet \("(?P<name>[^"]+)"\)', re.IGNORECASE)

_QCUMBER_MUTATION_TOOLS: frozenset[str] = frozenset(
    {
        "queryQCumberTerritorialUnits",
        "queryQCumberDatasetSpatial",
        "queryQCumberDataset",
    }
)


def _extract_missing_dataset_name(details: Any) -> str:
    text = str(details or "").strip()
    if not text:
        return ""
    match = _DATASET_NOT_FOUND_RE.search(text)
    if match:
        return str(match.group("name") or "").strip()
    match = _NOT_MATERIALIZED_RE.search(text)
    if match:
        return str(match.group("name") or "").strip()
    return ""


def build_dataset_not_found_recovery_decision(
    *,
    results: list[dict[str, Any]],
    request_tool_names: set[str],
    classify_runtime_error_kind: Callable[[Any], str],
    runtime_guardrail_prefix: str,
    runtime_next_step_prefix: str,
) -> RuntimeLoopRuleDecision:
    latest_dataset_not_found_idx = -1
    latest_dataset_not_found_tool = ""
    latest_missing_dataset_name = ""
    tracked_failure_tools = {
        "countQMapRows",
        "waitForQMapDataset",
        "fitQMapToDataset",
        "showOnlyQMapLayer",
        "setQMapLayerColorByField",
        "setQMapTooltipFields",
        "setQMapLayerVisibility",
    }
    for idx in range(len(results) - 1, -1, -1):
        row = results[idx]
        tool_name = str(row.get("toolName") or "").strip()
        if row.get("success") is not False or tool_name not in tracked_failure_tools:
            continue
        if classify_runtime_error_kind(row.get("details")) != "dataset_not_found":
            continue
        latest_dataset_not_found_idx = idx
        latest_dataset_not_found_tool = tool_name
        latest_missing_dataset_name = _extract_missing_dataset_name(row.get("details"))
        break

    if latest_dataset_not_found_idx < 0:
        return RuntimeLoopRuleDecision(remove_tool_names=set(), guidance_lines=[])

    has_materialization_progress = any(
        row.get("success") is True
        and str(row.get("toolName") or "").strip() in {"waitForQMapDataset", "saveDataToMap", "loadData"}
        for row in results[latest_dataset_not_found_idx + 1 :]
    )
    if has_materialization_progress:
        return RuntimeLoopRuleDecision(remove_tool_names=set(), guidance_lines=[])

    # Look back for the last successful queryQCumber* result with a canonical datasetRef.
    # When found, inject it explicitly so the LLM doesn't construct wrong names like "[skmbbz] (1)".
    latest_qcumber_dataset_ref = ""
    for idx in range(len(results) - 1, -1, -1):
        row = results[idx]
        if row.get("success") is not True:
            continue
        if str(row.get("toolName") or "").strip() not in _QCUMBER_MUTATION_TOOLS:
            continue
        ref = str(row.get("datasetRef") or "").strip()
        if ref.lower().startswith("id:"):
            latest_qcumber_dataset_ref = ref
            break

    # Prefer waitForQMapDataset: derived q-map datasets (clip/aggregate/join) are already in Redux state
    # and never go through saveDataToMap (ToolCache). Only fall back to loadData/saveDataToMap when
    # waitForQMapDataset is not available (truly external ToolCache-only results).
    forced_tool_choice_name = ""
    for candidate in ("waitForQMapDataset", "loadData", "saveDataToMap"):
        if candidate in request_tool_names:
            forced_tool_choice_name = candidate
            break
    if not forced_tool_choice_name:
        return RuntimeLoopRuleDecision(remove_tool_names=set(), guidance_lines=[])

    blocked_retry_tools = request_tool_names.intersection(
        {"countQMapRows", "fitQMapToDataset", "showOnlyQMapLayer", "setQMapLayerVisibility"}
    )
    dataset_hint = f' "{latest_missing_dataset_name}"' if latest_missing_dataset_name else ""
    if forced_tool_choice_name == "waitForQMapDataset":
        if latest_qcumber_dataset_ref:
            next_step = (
                f'Call waitForQMapDataset(datasetName="{latest_qcumber_dataset_ref}", timeoutMs=60000). '
                f'Use this exact canonical datasetRef — do NOT use constructed names'
                + (f' like "{latest_missing_dataset_name}"' if latest_missing_dataset_name else "")
                + ". Only retry countQMapRows/fit after waitForQMapDataset confirms the dataset is available."
            )
        else:
            next_step = (
                f"Call waitForQMapDataset now with the canonical name{dataset_hint} — derived q-map datasets "
                "(clip/aggregate/join outputs) are already in map state and do NOT require saveDataToMap. "
                "Only retry countQMapRows/fit after waitForQMapDataset confirms the dataset is available."
            )
    else:
        next_step = (
            f"Call {forced_tool_choice_name} now to materialize the missing dataset{dataset_hint}, "
            "then retry waitForQMapDataset/countQMapRows only after materialization succeeds."
        )
    return RuntimeLoopRuleDecision(
        remove_tool_names=blocked_retry_tools,
        guidance_lines=[
            (
                f"{runtime_guardrail_prefix} Selected rule `dataset_not_found_materialization_recovery` "
                f"(last failure={latest_dataset_not_found_tool or 'unknown'}:dataset_not_found)."
            ),
            (
                f"{runtime_guardrail_prefix} Do not loop wait/count/fit on a dataset that is not materialized in map state."
            ),
            (
                f"{runtime_next_step_prefix} {next_step}"
            ),
        ],
        forced_tool_choice_name=forced_tool_choice_name,
    )


def build_post_create_validation_decision(
    *,
    results: list[dict[str, Any]],
    request_tool_names: set[str],
    latest_successful_tool_index: Callable[[list[dict[str, Any]], set[str]], int],
    dataset_create_or_update_tools: set[str],
    post_create_validation_deferred_tools: set[str],
    runtime_guardrail_prefix: str,
    runtime_next_step_prefix: str,
) -> RuntimeLoopRuleDecision:
    latest_create_idx = latest_successful_tool_index(results, dataset_create_or_update_tools)
    wait_after_create_idx = (
        _first_successful_tool_index(results, {"waitForQMapDataset"}, start_idx=latest_create_idx + 1)
        if latest_create_idx >= 0
        else -1
    )
    count_after_wait_idx = (
        _first_successful_tool_index(results, {"countQMapRows"}, start_idx=wait_after_create_idx + 1)
        if wait_after_create_idx >= 0
        else -1
    )

    blocked_before_wait = post_create_validation_deferred_tools.union(
        dataset_create_or_update_tools,
        {"countQMapRows"},
    )
    if latest_create_idx >= 0 and wait_after_create_idx < 0 and "waitForQMapDataset" in request_tool_names:
        return RuntimeLoopRuleDecision(
            remove_tool_names=request_tool_names.intersection(blocked_before_wait),
            guidance_lines=[
                (
                    f"{runtime_guardrail_prefix} Selected rule `post_create_validation_wait_gate` "
                    "(dataset mutation already succeeded in this turn)."
                ),
                (
                    f"{runtime_guardrail_prefix} Validation chain is incomplete: do not style, rank, fit, isolate, "
                    "or count rows before waitForQMapDataset succeeds."
                ),
                (
                    f"{runtime_next_step_prefix} Call waitForQMapDataset now; defer count/style/focus steps until wait succeeds."
                ),
            ],
            forced_tool_choice_name="waitForQMapDataset",
        )

    blocked_before_count = post_create_validation_deferred_tools.union(
        dataset_create_or_update_tools,
        {"waitForQMapDataset"},
    )
    if wait_after_create_idx >= 0 and count_after_wait_idx < 0 and "countQMapRows" in request_tool_names:
        return RuntimeLoopRuleDecision(
            remove_tool_names=request_tool_names.intersection(blocked_before_count),
            guidance_lines=[
                (
                    f"{runtime_guardrail_prefix} Selected rule `post_create_validation_count_gate` "
                    "(waitForQMapDataset already succeeded after dataset mutation)."
                ),
                (
                    f"{runtime_guardrail_prefix} Validation chain is still incomplete: do not style, rank, fit, "
                    "or isolate the dataset before countQMapRows confirms materialization."
                ),
                (
                    f"{runtime_next_step_prefix} Call countQMapRows now and only then continue with styling/focus/final confirmation."
                ),
            ],
            forced_tool_choice_name="countQMapRows",
        )

    return RuntimeLoopRuleDecision(remove_tool_names=set(), guidance_lines=[])


def build_low_distinct_color_recovery_decision(
    *,
    results: list[dict[str, Any]],
    request_tool_names: set[str],
    current_forced_tool_choice_name: str,
    style_execution_tools: set[str],
    blocked_recompute_tools: set[str],
    is_low_distinct_color_failure: Callable[[Any], bool],
    runtime_guardrail_prefix: str,
    runtime_next_step_prefix: str,
) -> RuntimeLoopRuleDecision:
    latest_low_distinct_color_failure_idx = -1
    for idx in range(len(results) - 1, -1, -1):
        row = results[idx]
        if str(row.get("toolName") or "").strip() != "setQMapLayerColorByField":
            continue
        if row.get("success") is False and is_low_distinct_color_failure(row.get("details")):
            latest_low_distinct_color_failure_idx = idx
        break

    if latest_low_distinct_color_failure_idx < 0:
        return RuntimeLoopRuleDecision(remove_tool_names=set(), guidance_lines=[])

    has_style_recovery_after_failure = any(
        row.get("success") is True and str(row.get("toolName") or "").strip() in style_execution_tools
        for row in results[latest_low_distinct_color_failure_idx + 1 :]
    )
    if has_style_recovery_after_failure:
        return RuntimeLoopRuleDecision(remove_tool_names=set(), guidance_lines=[])

    forced_tool_choice_name = ""
    if not current_forced_tool_choice_name:
        for candidate in (
            "distinctQMapFieldValues",
            "previewQMapDatasetRows",
            "searchQMapFieldValues",
            "setQMapLayerSolidColor",
        ):
            if candidate in request_tool_names:
                forced_tool_choice_name = candidate
                break

    return RuntimeLoopRuleDecision(
        remove_tool_names=request_tool_names.intersection(blocked_recompute_tools.union({"setQMapLayerColorByField"})),
        guidance_lines=[
            (
                f"{runtime_guardrail_prefix} Selected rule `color_low_distinct_runtime_recovery_gate` "
                "(latest metric-color step failed with low-distinct values)."
            ),
            (
                f"{runtime_guardrail_prefix} Do not retry setQMapLayerColorByField with the same low-variance pattern "
                "and do not launch heavy recompute/query loops to fabricate variance."
            ),
            (
                f"{runtime_next_step_prefix} Inspect the metric distribution first (distinct/preview/search) "
                "or apply only an explicit non-metric fallback style; otherwise finalize with a clear limitation."
            ),
        ],
        forced_tool_choice_name=forced_tool_choice_name,
    )


def build_missing_field_color_recovery_decision(
    *,
    results: list[dict[str, Any]],
    request_tool_names: set[str],
    current_forced_tool_choice_name: str,
    style_execution_tools: set[str],
    blocked_recompute_tools: set[str],
    is_metric_field_not_found_failure: Callable[[Any], bool],
    runtime_guardrail_prefix: str,
    runtime_next_step_prefix: str,
) -> RuntimeLoopRuleDecision:
    latest_missing_field_color_failure_idx = -1
    for idx in range(len(results) - 1, -1, -1):
        row = results[idx]
        if str(row.get("toolName") or "").strip() != "setQMapLayerColorByField":
            continue
        if row.get("success") is False and is_metric_field_not_found_failure(row.get("details")):
            latest_missing_field_color_failure_idx = idx
        break

    if latest_missing_field_color_failure_idx < 0:
        return RuntimeLoopRuleDecision(remove_tool_names=set(), guidance_lines=[])

    has_style_recovery_after_failure = any(
        row.get("success") is True and str(row.get("toolName") or "").strip() in style_execution_tools
        for row in results[latest_missing_field_color_failure_idx + 1 :]
    )
    if has_style_recovery_after_failure:
        return RuntimeLoopRuleDecision(remove_tool_names=set(), guidance_lines=[])

    forced_tool_choice_name = ""
    if not current_forced_tool_choice_name:
        for candidate in ("previewQMapDatasetRows", "distinctQMapFieldValues", "searchQMapFieldValues"):
            if candidate in request_tool_names:
                forced_tool_choice_name = candidate
                break

    return RuntimeLoopRuleDecision(
        remove_tool_names=request_tool_names.intersection(blocked_recompute_tools),
        guidance_lines=[
            (
                f"{runtime_guardrail_prefix} Selected rule `color_missing_field_runtime_recovery_gate` "
                "(latest metric-color step failed because the field was not found)."
            ),
            (
                f"{runtime_guardrail_prefix} Do not rerun heavy query/join recompute loops while the current dataset already exists. "
                "Inspect the existing dataset fields first."
            ),
            (
                f"{runtime_next_step_prefix} Call previewQMapDatasetRows/distinct/search on the current dataset, "
                "then retry setQMapLayerColorByField only with a field that actually exists."
            ),
        ],
        forced_tool_choice_name=forced_tool_choice_name,
    )


def build_admin_superlative_layer_order_decision(
    *,
    results: list[dict[str, Any]],
    request_tool_names: set[str],
    admin_superlative_map_workflow: bool,
    latest_successful_tool_index: Callable[[list[dict[str, Any]], set[str]], int],
    runtime_guardrail_prefix: str,
    runtime_next_step_prefix: str,
) -> RuntimeLoopRuleDecision:
    if (
        "setQMapLayerOrder" not in request_tool_names
        or "fitQMapToDataset" not in request_tool_names
        or not admin_superlative_map_workflow
    ):
        return RuntimeLoopRuleDecision(remove_tool_names=set(), guidance_lines=[])

    latest_fit_idx = latest_successful_tool_index(results, {"fitQMapToDataset"})
    latest_isolation_idx = latest_successful_tool_index(
        results, {"createDatasetFromFilter", "createDatasetFromCurrentFilters"}
    )
    if latest_fit_idx >= 0 and latest_isolation_idx >= 0 and latest_fit_idx > latest_isolation_idx:
        guidance_lines = [
            (
                f"{runtime_guardrail_prefix} Selected rule `fit_completes_superlative_map_focus` "
                "(isolated winner already fit on map)."
            ),
            (
                f"{runtime_guardrail_prefix} After a successful fitQMapToDataset on the isolated administrative winner, "
                "do not spend an extra step on setQMapLayerOrder unless the user explicitly asked for manual layer ordering."
            ),
            (
                f"{runtime_next_step_prefix} Finalize from current ranking/materialization/map-focus evidence."
            ),
        ]
    else:
        guidance_lines = [
            (
                f"{runtime_guardrail_prefix} Selected rule `admin_superlative_prefers_fit_over_layer_order` "
                "(foreground display should be satisfied via fitQMapToDataset, not layer reordering)."
            ),
            (
                f"{runtime_guardrail_prefix} For administrative superlative-to-map objectives, "
                "do not use setQMapLayerOrder as a substitute for fitQMapToDataset."
            ),
            (
                f"{runtime_next_step_prefix} Isolate the winner dataset and use fitQMapToDataset to bring it into focus."
            ),
        ]

    return RuntimeLoopRuleDecision(
        remove_tool_names={"setQMapLayerOrder"},
        guidance_lines=guidance_lines,
    )


def build_post_filter_force_fit_decision(
    *,
    results: list[dict[str, Any]],
    request_tool_names: set[str],
    admin_superlative_map_workflow: bool,
    latest_successful_tool_index: Callable[[list[dict[str, Any]], set[str]], int],
    runtime_guardrail_prefix: str,
    runtime_next_step_prefix: str,
) -> RuntimeLoopRuleDecision:
    """Force fitQMapToDataset after superlative winner is isolated and validated (filter+wait+count).

    When the admin-superlative workflow has completed filter → wait → count
    but fit is still missing, force fitQMapToDataset and strip all other tools
    so that providers which ignore ``tool_choice`` still cannot escape the
    map-focus step.
    """
    if (
        "fitQMapToDataset" not in request_tool_names
        or not admin_superlative_map_workflow
    ):
        return RuntimeLoopRuleDecision(remove_tool_names=set(), guidance_lines=[])

    filter_tools = {"createDatasetFromFilter", "createDatasetFromCurrentFilters"}
    latest_filter_idx = latest_successful_tool_index(results, filter_tools)
    if latest_filter_idx < 0:
        return RuntimeLoopRuleDecision(remove_tool_names=set(), guidance_lines=[])

    prior_rank = False
    for idx in range(latest_filter_idx - 1, -1, -1):
        row = results[idx]
        if row.get("success") is True and str(row.get("toolName") or "").strip() == "rankQMapDatasetRows":
            prior_rank = True
            break
    if not prior_rank:
        return RuntimeLoopRuleDecision(remove_tool_names=set(), guidance_lines=[])

    wait_idx = _first_successful_tool_index(results, {"waitForQMapDataset"}, start_idx=latest_filter_idx + 1)
    if wait_idx < 0:
        return RuntimeLoopRuleDecision(remove_tool_names=set(), guidance_lines=[])

    count_idx = _first_successful_tool_index(results, {"countQMapRows"}, start_idx=wait_idx + 1)
    if count_idx < 0:
        return RuntimeLoopRuleDecision(remove_tool_names=set(), guidance_lines=[])

    fit_idx = _first_successful_tool_index(results, {"fitQMapToDataset"}, start_idx=count_idx + 1)
    if fit_idx >= 0:
        return RuntimeLoopRuleDecision(remove_tool_names=set(), guidance_lines=[])

    other_tools = request_tool_names - {"fitQMapToDataset"}
    return RuntimeLoopRuleDecision(
        remove_tool_names=other_tools,
        guidance_lines=[
            (
                f"{runtime_guardrail_prefix} Selected rule `post_filter_force_fit_for_superlative` "
                "(isolated superlative winner validated via wait+count, map focus is mandatory)."
            ),
            (
                f"{runtime_guardrail_prefix} The administrative superlative winner was isolated "
                "(createDatasetFromFilter), validated (waitForQMapDataset + countQMapRows), "
                "but map focus (fitQMapToDataset) has not been applied yet."
            ),
            (
                f"{runtime_next_step_prefix} Call fitQMapToDataset on the isolated winner dataset "
                "to complete the map display objective."
            ),
        ],
        forced_tool_choice_name="fitQMapToDataset",
    )


def build_clarification_required_decision(
    *,
    results: list[dict[str, Any]],
    classify_runtime_error_kind: Callable[[Any], str],
    discovery_tools: set[str],
    runtime_guardrail_prefix: str,
    runtime_next_step_prefix: str,
    runtime_response_mode_prefix: str,
) -> RuntimeLoopRuleDecision:
    latest_clarification_required_idx = -1
    latest_clarification_required_question = ""
    latest_clarification_required_reason = ""
    for idx in range(len(results) - 1, -1, -1):
        row = results[idx]
        if row.get("success") is True:
            continue
        error_kind = classify_runtime_error_kind(row.get("details"))
        if row.get("clarificationRequired") is True or error_kind == "ambiguous_admin_match":
            latest_clarification_required_idx = idx
            latest_clarification_required_question = str(row.get("clarificationQuestion") or "").strip()
            latest_clarification_required_reason = error_kind or "clarification_required"
            break

    if latest_clarification_required_idx < 0:
        return RuntimeLoopRuleDecision(remove_tool_names=set(), guidance_lines=[])

    has_successful_non_discovery_after_clarification = any(
        row.get("success") is True
        and str(row.get("toolName") or "").strip()
        and str(row.get("toolName") or "").strip() not in discovery_tools
        for row in results[latest_clarification_required_idx + 1 :]
    )
    if has_successful_non_discovery_after_clarification:
        return RuntimeLoopRuleDecision(remove_tool_names=set(), guidance_lines=[])

    question_suffix = (
        f" Use this clarification question verbatim or near-verbatim: {latest_clarification_required_question}"
        if latest_clarification_required_question
        else ""
    )
    return RuntimeLoopRuleDecision(
        remove_tool_names=set(),
        guidance_lines=[
            (
                f"{runtime_guardrail_prefix} Selected rule `clarification_required_finalize` "
                f"(reason={latest_clarification_required_reason or 'clarification_required'})."
            ),
            (
                f"{runtime_guardrail_prefix} The latest tool result requires explicit user clarification. "
                "Do not continue with more operational tool calls on guessed provider/dataset/level intent."
            ),
            (
                f"{runtime_next_step_prefix} Return one concise clarification now and stop."
                f"{question_suffix}"
            ),
            f"{runtime_response_mode_prefix} clarification",
        ],
        force_finalize_without_tools=True,
    )


def build_admin_level_validation_failure_decision(
    *,
    results: list[dict[str, Any]],
    classify_runtime_error_kind: Callable[[Any], str],
    discovery_tools: set[str],
    runtime_guardrail_prefix: str,
    runtime_next_step_prefix: str,
    runtime_response_mode_prefix: str,
) -> RuntimeLoopRuleDecision:
    latest_admin_level_failure_idx = -1
    for idx in range(len(results) - 1, -1, -1):
        row = results[idx]
        if row.get("success") is True:
            continue
        if classify_runtime_error_kind(row.get("details")) == "admin_level_validation_failure":
            latest_admin_level_failure_idx = idx
            break

    if latest_admin_level_failure_idx < 0:
        return RuntimeLoopRuleDecision(remove_tool_names=set(), guidance_lines=[])

    has_successful_non_discovery_after_failure = any(
        row.get("success") is True
        and str(row.get("toolName") or "").strip()
        and str(row.get("toolName") or "").strip() not in discovery_tools
        for row in results[latest_admin_level_failure_idx + 1 :]
    )
    if has_successful_non_discovery_after_failure:
        return RuntimeLoopRuleDecision(remove_tool_names=set(), guidance_lines=[])

    return RuntimeLoopRuleDecision(
        remove_tool_names=set(),
        guidance_lines=[
            (
                f"{runtime_guardrail_prefix} Selected rule `admin_level_validation_failure_finalize` "
                "(reason=admin_level_validation_failure)."
            ),
            (
                f"{runtime_guardrail_prefix} The latest tool result failed strict administrative level validation. "
                "Do not continue with relaxed queries that drop expectedAdminType/lv or auto-switch to a different level."
            ),
            (
                f"{runtime_next_step_prefix} Return one concise limitation now: report that the requested administrative "
                "level could not be validated with the current query/dataset and stop."
            ),
            f"{runtime_response_mode_prefix} limitation",
        ],
        force_finalize_without_tools=True,
    )


def build_error_retry_guardrail_decision(
    *,
    results: list[dict[str, Any]],
    runtime_failure_error_class: Callable[[dict[str, Any]], str],
    runtime_error_retry_policy: Callable[[str], dict[str, Any]],
    error_class_max_retries: int,
    runtime_guardrail_prefix: str,
    runtime_next_step_prefix: str,
) -> RuntimeLoopRuleDecision:
    failure_class_counts: dict[str, int] = {}
    last_failure_class = ""
    for row in results:
        error_class = runtime_failure_error_class(row)
        if not error_class:
            continue
        failure_class_counts[error_class] = failure_class_counts.get(error_class, 0) + 1
        last_failure_class = error_class

    if not last_failure_class:
        return RuntimeLoopRuleDecision(remove_tool_names=set(), guidance_lines=[])

    count = int(failure_class_counts.get(last_failure_class, 0))
    _, _, last_error_kind = last_failure_class.partition(":")
    policy = runtime_error_retry_policy(last_error_kind)
    allowed_retries = int(policy.get("allowedRetries") or error_class_max_retries)
    remediation_hint = str(policy.get("remediationHint") or "").strip()
    next_step_hint = str(policy.get("nextStep") or "").strip()
    limit = max(0, allowed_retries) + 1
    if count < limit:
        return RuntimeLoopRuleDecision(remove_tool_names=set(), guidance_lines=[])

    blocked_tool = last_failure_class.split(":", 1)[0].strip()
    return RuntimeLoopRuleDecision(
        remove_tool_names={blocked_tool} if blocked_tool else set(),
        guidance_lines=[
            (
                f"{runtime_guardrail_prefix} Selected rule `error_class_retry_cap` "
                f"(class={last_failure_class}, occurrences={count}, allowed={max(0, allowed_retries)} retries)."
            ),
            (
                f"{runtime_guardrail_prefix} Same error class repeated too many times. "
                f"Do not retry `{blocked_tool or 'the same tool'}` with the same failing pattern."
            ),
            (
                f"{runtime_guardrail_prefix} Retry policy `{last_error_kind or 'generic_failure'}`: "
                f"{remediation_hint or 'Use deterministic alternative remediation before retrying.'}"
            ),
            (
                f"{runtime_next_step_prefix} "
                f"{next_step_hint or 'Use an alternative validated path or finalize with a clear failure/limitation message.'}"
            ),
        ],
    )


def build_identical_tool_args_failure_decision(
    *,
    outgoing: dict[str, Any],
    results: list[dict[str, Any]],
    select_identical_tool_args_failure_circuit_breaker: Callable[..., dict[str, Any] | None],
    compact_signature_for_trace: Callable[[Any], str],
    runtime_guardrail_prefix: str,
    runtime_next_step_prefix: str,
) -> RuntimeLoopRuleDecision:
    identical_failure_breaker = select_identical_tool_args_failure_circuit_breaker(outgoing, results)
    if not identical_failure_breaker:
        return RuntimeLoopRuleDecision(remove_tool_names=set(), guidance_lines=[])

    blocked_tool = str(identical_failure_breaker.get("toolName") or "").strip()
    count = int(identical_failure_breaker.get("count") or 0)
    allowed_retries = int(identical_failure_breaker.get("allowedRetries") or 0)
    signature_trace = compact_signature_for_trace(identical_failure_breaker.get("signature"))
    trace_payload = {
        "rule": "identical_tool_args_circuit_breaker",
        "tool": blocked_tool or None,
        "signature": signature_trace,
        "failures": count,
        "allowedRetries": allowed_retries,
        "lastErrorKind": str(identical_failure_breaker.get("lastErrorKind") or ""),
    }
    return RuntimeLoopRuleDecision(
        remove_tool_names={blocked_tool} if blocked_tool else set(),
        guidance_lines=[
            (
                f"{runtime_guardrail_prefix} Selected rule `identical_tool_args_circuit_breaker` "
                f"(tool={blocked_tool or 'unknown'}, failures={count}, allowedRetries={allowed_retries})."
            ),
            (
                f"{runtime_guardrail_prefix} Repeated identical tool call detected (same tool + same arguments) "
                "without successful recovery. Stop retrying the same signature."
            ),
            (
                f"{runtime_guardrail_prefix} Fallback trace: "
                f"{json.dumps(trace_payload, ensure_ascii=False, separators=(',', ':'))}"
            ),
            (
                f"{runtime_next_step_prefix} Change at least one routing argument "
                "(dataset/field/bounds/resolution) or switch to an alternative validation/evidence tool; "
                "if no deterministic alternative exists, finalize with explicit limitation."
            ),
        ],
    )


def build_identical_tool_args_success_decision(
    *,
    outgoing: dict[str, Any],
    results: list[dict[str, Any]],
    select_identical_tool_args_success_guardrail: Callable[..., dict[str, Any] | None],
    compact_signature_for_trace: Callable[[Any], str],
    runtime_guardrail_prefix: str,
    runtime_next_step_prefix: str,
) -> RuntimeLoopRuleDecision:
    identical_success_guardrail = select_identical_tool_args_success_guardrail(outgoing, results)
    if not identical_success_guardrail:
        return RuntimeLoopRuleDecision(remove_tool_names=set(), guidance_lines=[])

    blocked_tool = str(identical_success_guardrail.get("toolName") or "").strip()
    count = int(identical_success_guardrail.get("count") or 0)
    allowed_repeats = int(identical_success_guardrail.get("allowedRepeats") or 0)
    signature_trace = compact_signature_for_trace(identical_success_guardrail.get("signature"))
    trace_payload = {
        "rule": "identical_tool_args_success_reuse",
        "tool": blocked_tool or None,
        "signature": signature_trace,
        "successes": count,
        "allowedRepeats": allowed_repeats,
    }
    return RuntimeLoopRuleDecision(
        remove_tool_names={blocked_tool} if blocked_tool else set(),
        guidance_lines=[
            (
                f"{runtime_guardrail_prefix} Selected rule `identical_tool_args_success_reuse` "
                f"(tool={blocked_tool or 'unknown'}, successes={count}, allowedRepeats={allowed_repeats})."
            ),
            (
                f"{runtime_guardrail_prefix} The same tool call already succeeded multiple times with identical arguments. "
                "Reuse existing evidence instead of repeating the same signature."
            ),
            (
                f"{runtime_guardrail_prefix} Fallback trace: "
                f"{json.dumps(trace_payload, ensure_ascii=False, separators=(',', ':'))}"
            ),
            (
                f"{runtime_next_step_prefix} Continue with a distinct downstream step or finalize from the current successful result. "
                "Do not reissue the same tool with the same arguments."
            ),
        ],
    )


def build_turn_state_discovery_decision(
    *,
    outgoing: dict[str, Any],
    results: list[dict[str, Any]],
    extract_request_tool_names: Callable[[dict[str, Any]], list[str]],
    is_turn_state_discovery_failure: Callable[[Any], bool],
    runtime_guardrail_prefix: str,
    runtime_next_step_prefix: str,
) -> RuntimeLoopRuleDecision:
    turn_state_gate_failures = [
        row for row in results if row.get("success") is False and is_turn_state_discovery_failure(row.get("details"))
    ]
    if len(turn_state_gate_failures) < 3:
        return RuntimeLoopRuleDecision(remove_tool_names=set(), guidance_lines=[])

    gate_failed_tools = sorted(
        {
            str(row.get("toolName") or "").strip()
            for row in turn_state_gate_failures
            if str(row.get("toolName") or "").strip()
            and str(row.get("toolName") or "").strip() != "listQMapDatasets"
        }
    )
    next_step = (
        "Call listQMapDatasets once and wait for success before calling operational tools again."
        if "listQMapDatasets" in set(extract_request_tool_names(outgoing))
        else "Retry operational tools only after a successful dataset discovery/snapshot step."
    )
    return RuntimeLoopRuleDecision(
        remove_tool_names=set(gate_failed_tools),
        guidance_lines=[
            (
                f"{runtime_guardrail_prefix} Selected rule `turn_state_discovery_retry_gate` "
                f"(failures={len(turn_state_gate_failures)})."
            ),
            (
                f"{runtime_guardrail_prefix} Repeated turn-state discovery gate failures detected. "
                "Do not keep retrying the same operational tools without a fresh discovery snapshot."
            ),
            (
                f"{runtime_next_step_prefix} {next_step}"
            ),
        ],
    )


def build_dataset_snapshot_reuse_decision(
    *,
    results: list[dict[str, Any]],
    request_tool_names: set[str],
    objective_text: str,
    objective_requests_dataset_discovery: Callable[[str], bool],
    discovery_tools: set[str],
    is_turn_state_discovery_failure: Callable[[Any], bool],
    classify_runtime_error_kind: Callable[[Any], str],
    runtime_guardrail_prefix: str,
    runtime_next_step_prefix: str,
) -> RuntimeLoopRuleDecision:
    list_snapshot_success_indices = [
        idx
        for idx, row in enumerate(results)
        if row.get("success") is True and str(row.get("toolName") or "").strip() == "listQMapDatasets"
    ]
    if (
        "listQMapDatasets" not in request_tool_names
        or not list_snapshot_success_indices
        or objective_requests_dataset_discovery(objective_text)
    ):
        return RuntimeLoopRuleDecision(remove_tool_names=set(), guidance_lines=[])

    latest_snapshot_idx = list_snapshot_success_indices[-1]
    repeated_snapshot_calls = len(list_snapshot_success_indices) >= 2
    has_operational_progress_after_snapshot = any(
        row.get("success") is True
        and str(row.get("toolName") or "").strip()
        and str(row.get("toolName") or "").strip() not in discovery_tools
        for row in results[latest_snapshot_idx + 1 :]
    )
    needs_fresh_snapshot_after_latest = any(
        row.get("success") is False
        and (
            is_turn_state_discovery_failure(row.get("details"))
            or classify_runtime_error_kind(row.get("details")) == "dataset_not_found"
        )
        for row in results[latest_snapshot_idx + 1 :]
    )
    has_operational_failure_after_snapshot = any(
        row.get("success") is False
        and str(row.get("toolName") or "").strip() not in discovery_tools
        for row in results[latest_snapshot_idx + 1 :]
    )
    if not (
        repeated_snapshot_calls
        or (
            has_operational_progress_after_snapshot
            and not needs_fresh_snapshot_after_latest
            and not has_operational_failure_after_snapshot
        )
    ):
        return RuntimeLoopRuleDecision(remove_tool_names=set(), guidance_lines=[])

    return RuntimeLoopRuleDecision(
        remove_tool_names={"listQMapDatasets"},
        guidance_lines=[
            (
                f"{runtime_guardrail_prefix} Selected rule `dataset_discovery_snapshot_reuse` "
                f"(snapshots={len(list_snapshot_success_indices)})."
            ),
            (
                f"{runtime_guardrail_prefix} A successful dataset snapshot is already available for this turn. "
                "Avoid redundant listQMapDatasets calls."
            ),
            (
                f"{runtime_next_step_prefix} Reuse the latest snapshot and continue with operational tools. "
                "Call listQMapDatasets again after snapshot-gate/dataset-not-found failures or unresolved operational errors."
            ),
        ],
    )


def build_tool_call_finalize_decision(
    *,
    tool_call_count: int,
    has_assistant_text: bool,
    tool_call_workflow_hard_cap: int,
    tool_only_no_text_watchdog_min_calls: int,
    runtime_guardrail_prefix: str,
    runtime_next_step_prefix: str,
) -> RuntimeLoopRuleDecision:
    if tool_call_count >= tool_call_workflow_hard_cap:
        return RuntimeLoopRuleDecision(
            remove_tool_names=set(),
            guidance_lines=[
                (
                    f"{runtime_guardrail_prefix} Selected rule `tool_call_hard_cap` "
                    f"(count={tool_call_count}, cap={tool_call_workflow_hard_cap})."
                ),
                (
                    f"{runtime_guardrail_prefix} Tool-call hard cap reached for the active request. "
                    "Stop tool execution to avoid loops."
                ),
                (
                    f"{runtime_next_step_prefix} Return one concise final user-facing response using only existing tool evidence "
                    "(success/failure + concrete stats/limits). Do not emit further tool calls."
                ),
            ],
            force_finalize_without_tools=True,
        )

    if tool_call_count >= tool_only_no_text_watchdog_min_calls and not has_assistant_text:
        return RuntimeLoopRuleDecision(
            remove_tool_names=set(),
            guidance_lines=[
                (
                    f"{runtime_guardrail_prefix} Selected rule `tool_only_no_final_text_watchdog` "
                    f"(toolCalls={tool_call_count}, min={tool_only_no_text_watchdog_min_calls})."
                ),
                (
                    f"{runtime_guardrail_prefix} Repeated tool-only turns detected without assistant final text. "
                    "Stop tool execution and finalize now."
                ),
                (
                    f"{runtime_next_step_prefix} Return final text now (completed steps, failures, and next actionable step). "
                    "Do not emit further tool calls."
                ),
            ],
            force_finalize_without_tools=True,
        )

    return RuntimeLoopRuleDecision(remove_tool_names=set(), guidance_lines=[])


def build_cloud_tools_require_explicit_request_decision(
    *,
    request_tool_names: set[str],
    objective_mentions_cloud_or_saved_maps: Callable[[str], bool],
    objective_text: str,
    runtime_guardrail_prefix: str,
    runtime_next_step_prefix: str,
) -> RuntimeLoopRuleDecision:
    """Prune cloud map tools when the user objective does not explicitly mention
    cloud, saved, or personal maps."""
    cloud_tools = {"listQMapCloudMaps", "loadQMapCloudMap", "loadCloudMapAndWait"}
    offered_cloud_tools = request_tool_names.intersection(cloud_tools)
    if not offered_cloud_tools:
        return RuntimeLoopRuleDecision(remove_tool_names=set(), guidance_lines=[])
    if objective_mentions_cloud_or_saved_maps(objective_text):
        return RuntimeLoopRuleDecision(remove_tool_names=set(), guidance_lines=[])
    return RuntimeLoopRuleDecision(
        remove_tool_names=offered_cloud_tools,
        guidance_lines=[
            (
                f"{runtime_guardrail_prefix} Selected rule `cloud_tools_require_explicit_request` "
                "(user objective does not mention cloud/saved/personal maps)."
            ),
            (
                f"{runtime_guardrail_prefix} Cloud map tools (listQMapCloudMaps, loadCloudMapAndWait, "
                "loadQMapCloudMap) are reserved for explicit user requests to load saved/personal maps. "
                "For territorial or analytical queries, use q-cumber dataset tools."
            ),
            (
                f"{runtime_next_step_prefix} Start from listQCumberProviders/listQCumberDatasets "
                "or queryQCumberTerritorialUnits for the current objective."
            ),
        ],
    )


def build_post_discovery_force_query_decision(
    *,
    results: list[dict[str, Any]],
    request_tool_names: set[str],
    runtime_guardrail_prefix: str,
    runtime_next_step_prefix: str,
) -> RuntimeLoopRuleDecision:
    """After discovery completes (listProviders + listDatasets both succeeded)
    but no query tool has been called yet, force the model to continue with
    a query tool instead of stopping.

    This implements the deterministic state-machine transition:
        discovery → query (forced via tool_choice: "required")

    Without this rule, some models (e.g. Gemini) stop after discovery and
    return an empty response, never proceeding to the actual data query.
    """
    discovery_tools = {"listQCumberProviders", "listQCumberDatasets"}
    query_tools = {
        "queryQCumberTerritorialUnits",
        "queryQCumberDataset",
        "queryQCumberDatasetSpatial",
        "getQCumberDatasetHelp",
    }

    # Check that both discovery steps succeeded
    has_providers = any(
        str(row.get("toolName") or "").strip() == "listQCumberProviders"
        and row.get("success") is True
        for row in results
    )
    has_datasets = any(
        str(row.get("toolName") or "").strip() == "listQCumberDatasets"
        and row.get("success") is True
        for row in results
    )
    if not has_providers or not has_datasets:
        return RuntimeLoopRuleDecision(remove_tool_names=set(), guidance_lines=[])

    # Check if a query or help tool has already been called
    has_query = any(
        str(row.get("toolName") or "").strip() in query_tools
        for row in results
    )
    if has_query:
        return RuntimeLoopRuleDecision(remove_tool_names=set(), guidance_lines=[])

    # Determine the best forced tool from available tools.
    # Prefer getQCumberDatasetHelp (safe, gives metadata for routing),
    # fall back to queryQCumberTerritorialUnits (most common query path).
    forced_tool = ""
    for candidate in ("getQCumberDatasetHelp", "queryQCumberTerritorialUnits"):
        if candidate in request_tool_names:
            forced_tool = candidate
            break

    if not forced_tool:
        # No query tools available in this request — cannot force
        return RuntimeLoopRuleDecision(remove_tool_names=set(), guidance_lines=[])

    return RuntimeLoopRuleDecision(
        remove_tool_names=set(),
        guidance_lines=[
            (
                f"{runtime_guardrail_prefix} Selected rule `post_discovery_force_query` "
                "(discovery completed but no query issued yet)."
            ),
            (
                f"{runtime_guardrail_prefix} Discovery phase is complete (providers and datasets "
                "resolved). Do not stop here — proceed with data query or dataset help."
            ),
            (
                f"{runtime_next_step_prefix} Call {forced_tool} with the appropriate "
                "providerId/datasetId from the discovery results, then continue with "
                "the analytical workflow."
            ),
        ],
        forced_tool_choice_name=forced_tool,
    )


def build_cloud_load_no_redundant_fallback_decision(
    *,
    results: list[dict[str, Any]],
    request_tool_names: set[str],
    objective_text: str,
    objective_requests_cloud_load_sequence: Callable[[str], bool],
    latest_successful_tool_index: Callable[[list[dict[str, Any]], set[str]], int],
    runtime_guardrail_prefix: str,
    runtime_next_step_prefix: str,
) -> RuntimeLoopRuleDecision:
    if "loadData" not in request_tool_names or not objective_requests_cloud_load_sequence(objective_text):
        return RuntimeLoopRuleDecision(remove_tool_names=set(), guidance_lines=[])

    cloud_success_idx = latest_successful_tool_index(results, {"loadCloudMapAndWait", "loadQMapCloudMap"})
    if cloud_success_idx < 0:
        return RuntimeLoopRuleDecision(remove_tool_names=set(), guidance_lines=[])

    has_cloud_failure_after_success = any(
        row.get("success") is False
        and str(row.get("toolName") or "").strip() in {"loadCloudMapAndWait", "loadQMapCloudMap"}
        for row in results[cloud_success_idx + 1 :]
    )
    if has_cloud_failure_after_success:
        return RuntimeLoopRuleDecision(remove_tool_names=set(), guidance_lines=[])

    return RuntimeLoopRuleDecision(
        remove_tool_names={"loadData"},
        guidance_lines=[
            (
                f"{runtime_guardrail_prefix} Selected rule `cloud_load_no_redundant_fallback_load` "
                "(cloud load already successful)."
            ),
            (
                f"{runtime_guardrail_prefix} Cloud map load succeeded. "
                "Do not run loadData fallback unless cloud load fails."
            ),
            (
                f"{runtime_next_step_prefix} Continue with validation/final response using cloud-load evidence; "
                "reserve loadData only for explicit cloud-load failure recovery."
            ),
        ],
    )


def build_cloud_no_validated_fallback_decision(
    *,
    results: list[dict[str, Any]],
    objective_text: str,
    objective_requests_cloud_load_sequence: Callable[[str], bool],
    classify_runtime_error_kind: Callable[[Any], str],
    runtime_guardrail_prefix: str,
    runtime_next_step_prefix: str,
    runtime_response_mode_prefix: str,
) -> RuntimeLoopRuleDecision:
    if not objective_requests_cloud_load_sequence(objective_text):
        return RuntimeLoopRuleDecision(remove_tool_names=set(), guidance_lines=[])

    latest_cloud_no_fallback_idx = -1
    for idx in range(len(results) - 1, -1, -1):
        row = results[idx]
        tool_name = str(row.get("toolName") or "").strip()
        if tool_name not in {"loadCloudMapAndWait", "loadQMapCloudMap"}:
            continue
        if row.get("success") is not False:
            continue
        if classify_runtime_error_kind(row.get("details")) != "cloud_no_validated_fallback":
            continue
        latest_cloud_no_fallback_idx = idx
        break

    if latest_cloud_no_fallback_idx < 0:
        return RuntimeLoopRuleDecision(remove_tool_names=set(), guidance_lines=[])

    has_validated_recovery_after_failure = False
    recovery_load_seen = False
    for row in results[latest_cloud_no_fallback_idx + 1 :]:
        tool_name = str(row.get("toolName") or "").strip()
        if row.get("success") is True and tool_name in {
            "loadCloudMapAndWait",
            "loadQMapCloudMap",
            "loadData",
            "saveDataToMap",
        }:
            recovery_load_seen = True
            continue
        if row.get("success") is True and tool_name == "waitForQMapDataset" and recovery_load_seen:
            has_validated_recovery_after_failure = True
            break

    if has_validated_recovery_after_failure:
        return RuntimeLoopRuleDecision(remove_tool_names=set(), guidance_lines=[])

    return RuntimeLoopRuleDecision(
        remove_tool_names=set(),
        guidance_lines=[
            (
                f"{runtime_guardrail_prefix} Selected rule `cloud_no_validated_fallback_finalize` "
                "(cloud retry/fallback path exhausted without validated recovery)."
            ),
            (
                f"{runtime_guardrail_prefix} Latest cloud-load failure already states that no validated fallback "
                "is available. Do not keep retrying cloud/bridge load paths in the same turn."
            ),
            (
                f"{runtime_next_step_prefix} Return one concise limitation now: cloud load could not be completed "
                "reliably because retry/fallback did not produce a validated dataset."
            ),
            f"{runtime_response_mode_prefix} limitation",
        ],
        force_finalize_without_tools=True,
    )


# ---------------------------------------------------------------------------
# Rule: force_statistical_tool_routing
# ---------------------------------------------------------------------------


_STATISTICAL_TOOL_ROUTING = {
    "regression": "regressQMapFields",
    "field_correlation": "correlateQMapFields",
    "natural_break_classification": "classifyQMapFieldBreaks",
    "regulatory_compliance": "checkRegulatoryCompliance",
    "regulatory_listing": "listRegulatoryThresholds",
    "exposure_assessment": "assessPopulationExposure",
    "spatial_interpolation": "interpolateIDW",
}

_STATISTICAL_TOOL_SET = set(_STATISTICAL_TOOL_ROUTING.values())


def build_force_statistical_tool_routing_decision(
    *,
    results: list[dict[str, Any]],
    request_tool_names: set[str],
    objective_text: str,
    objective_requests_linear_regression: Callable[[str], bool],
    objective_requests_field_correlation: Callable[[str], bool],
    objective_requests_natural_break_classification: Callable[[str], bool],
    objective_requests_regulatory_compliance: Callable[[str], bool],
    objective_requests_regulatory_listing: Callable[[str], bool],
    objective_requests_exposure_assessment: Callable[[str], bool],
    objective_requests_spatial_interpolation: Callable[[str], bool],
    runtime_guardrail_prefix: str,
    runtime_next_step_prefix: str,
) -> RuntimeLoopRuleDecision:
    """When the objective clearly requests a statistical/regulatory tool,
    force tool_choice to the correct tool — preventing the model from
    routing to older spatial tools that do something different.
    """
    # Skip if the correct tool was already called
    called_tools = {
        str(row.get("toolName") or "").strip()
        for row in results
        if isinstance(row, dict)
    }
    if called_tools & _STATISTICAL_TOOL_SET:
        return RuntimeLoopRuleDecision(remove_tool_names=set(), guidance_lines=[])

    # Detect intent → target tool
    target_tool = ""
    intent_label = ""

    if objective_requests_regulatory_listing(objective_text):
        target_tool = "listRegulatoryThresholds"
        intent_label = "regulatory_listing"
    elif objective_requests_regulatory_compliance(objective_text):
        # Only force after data is loaded (at least one query succeeded)
        has_query = any(
            str(row.get("toolName") or "").strip() in {
                "queryQCumberDatasetSpatial", "queryQCumberDataset", "queryQCumberTerritorialUnits"
            } and row.get("success") is True
            for row in results
        )
        if has_query:
            target_tool = "checkRegulatoryCompliance"
            intent_label = "regulatory_compliance"
    elif objective_requests_linear_regression(objective_text):
        target_tool = "regressQMapFields"
        intent_label = "regression"
    elif objective_requests_field_correlation(objective_text):
        target_tool = "correlateQMapFields"
        intent_label = "field_correlation"
    elif objective_requests_natural_break_classification(objective_text):
        target_tool = "classifyQMapFieldBreaks"
        intent_label = "natural_break_classification"
    elif objective_requests_exposure_assessment(objective_text):
        # Only force after both datasets are loaded
        has_two_queries = sum(
            1 for row in results
            if isinstance(row, dict) and row.get("success") is True
            and str(row.get("toolName") or "").strip() in {
                "queryQCumberDatasetSpatial", "queryQCumberDataset", "queryQCumberTerritorialUnits"
            }
        ) >= 2
        if has_two_queries:
            target_tool = "assessPopulationExposure"
            intent_label = "exposure_assessment"
    elif objective_requests_spatial_interpolation(objective_text):
        # Only force after data is loaded
        has_query = any(
            str(row.get("toolName") or "").strip() in {
                "queryQCumberDatasetSpatial", "queryQCumberDataset", "queryQCumberTerritorialUnits"
            } and row.get("success") is True
            for row in results
        )
        if has_query:
            target_tool = "interpolateIDW"
            intent_label = "spatial_interpolation"

    if not target_tool or target_tool not in request_tool_names:
        return RuntimeLoopRuleDecision(remove_tool_names=set(), guidance_lines=[])

    return RuntimeLoopRuleDecision(
        remove_tool_names=set(),
        guidance_lines=[
            (
                f"{runtime_guardrail_prefix} Selected rule `force_statistical_tool_routing` "
                f"(intent={intent_label}, target={target_tool})."
            ),
            (
                f"{runtime_guardrail_prefix} The user objective requires the dedicated tool "
                f"`{target_tool}`. Do NOT use spatial tools (computeQMapBivariateCorrelation, "
                "computeQMapSpatialAutocorrelation, setQMapLayerColorByStatsThresholds) for this."
            ),
            (
                f"{runtime_next_step_prefix} Call `{target_tool}` with the appropriate parameters."
            ),
        ],
        forced_tool_choice_name=target_tool,
    )


# ---------------------------------------------------------------------------
# Rule: zero_match_must_acknowledge
# ---------------------------------------------------------------------------


_QCUMBER_QUERY_TOOLS_FOR_ZERO_MATCH = {
    "queryQCumberTerritorialUnits",
    "queryQCumberDataset",
    "queryQCumberDatasetSpatial",
}


def build_zero_match_must_acknowledge_decision(
    *,
    results: list[dict[str, Any]],
    request_tool_names: set[str],
    runtime_guardrail_prefix: str,
    runtime_next_step_prefix: str,
    runtime_response_mode_prefix: str,
) -> RuntimeLoopRuleDecision:
    """When all filtered query tools returned 0 matching rows, force the model
    to acknowledge 'no data found' and stop.  Prevents hallucinated row counts
    when the underlying dataset simply has no rows for the requested filters.
    """
    has_any_query = False
    has_any_nonzero = False
    zero_match_count = 0
    filter_hints: list[str] = []

    for row in results:
        if not isinstance(row, dict):
            continue
        tool_name = str(row.get("toolName") or "").strip()
        if tool_name not in _QCUMBER_QUERY_TOOLS_FOR_ZERO_MATCH:
            continue
        if row.get("success") is not True:
            continue
        has_any_query = True
        details = str(row.get("details") or "").lower()
        # Detect zero matches from details string (the extractor does not
        # preserve 'returned'; check for explicit zero-match indicators).
        is_zero = "zero matches" in details or "with zero match" in details
        if is_zero:
            zero_match_count += 1
            filter_hints.append(f"{tool_name} -> 0 rows")
        else:
            has_any_nonzero = True

    if not has_any_query or has_any_nonzero or zero_match_count == 0:
        return RuntimeLoopRuleDecision(remove_tool_names=set(), guidance_lines=[])

    filter_summary = "; ".join(filter_hints[:3]) if filter_hints else "all queries returned 0"

    return RuntimeLoopRuleDecision(
        remove_tool_names=request_tool_names.intersection(_QCUMBER_QUERY_TOOLS_FOR_ZERO_MATCH),
        guidance_lines=[
            (
                f"{runtime_guardrail_prefix} Selected rule `zero_match_must_acknowledge` "
                f"({filter_summary})."
            ),
            (
                f"{runtime_guardrail_prefix} ALL query tools returned 0 matching rows for the "
                "requested filters. The data does not exist in the current database for this "
                "filter combination. Do NOT invent row counts, station names, or statistics. "
                "Do NOT claim data was loaded or is visible on the map."
            ),
            (
                f"{runtime_next_step_prefix} Acknowledge clearly that no data matches the "
                "requested filters. Suggest the user try different filters, a different region, "
                "or check data availability."
            ),
            f"{runtime_response_mode_prefix} limitation",
        ],
        force_finalize_without_tools=True,
    )
