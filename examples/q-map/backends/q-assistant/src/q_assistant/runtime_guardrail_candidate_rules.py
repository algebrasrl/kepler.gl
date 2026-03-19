from __future__ import annotations

from typing import Any, Callable


def build_post_create_wait_candidate(
    *,
    last_success: bool,
    last_tool: str,
    dataset_hint: str,
    tools_available: set[str],
    dataset_create_or_update_tools: set[str],
) -> dict[str, Any] | None:
    if not (last_success and last_tool in dataset_create_or_update_tools and "waitForQMapDataset" in tools_available):
        return None
    next_step = f"Call waitForQMapDataset{dataset_hint}"
    if "countQMapRows" in tools_available:
        next_step += ". Do not call countQMapRows in the same tool response; wait for waitForQMapDataset result first."
    return {
        "ruleId": "post_create_wait",
        "score": 120,
        "guardrail": (
            f"Last successful step `{last_tool}` created/updated a dataset. Do not return final text yet."
        ),
        "next": next_step,
    }


def build_post_wait_count_candidate(
    *,
    results: list[dict[str, Any]],
    last_idx: int,
    last_success: bool,
    last_tool: str,
    dataset_name: str,
    dataset_ref: str,
    dataset_hint: str,
    tools_available: set[str],
    dataset_create_or_update_tools: set[str],
    resolve_dataset_hint_from_result: Callable[..., tuple[str, str, str]],
    assistant_calls: list[dict[str, Any]],
) -> dict[str, Any] | None:
    if not (last_success and last_tool == "waitForQMapDataset" and "countQMapRows" in tools_available):
        return None
    previous_successful_create: dict[str, Any] | None = None
    for row in reversed(results[:last_idx]):
        if row.get("success") is True and str(row.get("toolName") or "").strip() in dataset_create_or_update_tools:
            previous_successful_create = row
            break
    if previous_successful_create is None:
        return None
    next_dataset_hint = dataset_hint
    if not dataset_name and not dataset_ref:
        _, _, next_dataset_hint = resolve_dataset_hint_from_result(previous_successful_create, assistant_calls)
    return {
        "ruleId": "post_wait_count",
        "score": 115,
        "guardrail": "waitForQMapDataset succeeded after a dataset-creation step. Validation is still incomplete.",
        "next": f"Call countQMapRows{next_dataset_hint} before styling or final confirmation.",
    }


def build_cloud_load_requires_wait_validation_candidate(
    *,
    results: list[dict[str, Any]],
    objective_text: str,
    tools_available: set[str],
    objective_requests_cloud_load_sequence: Callable[[str], bool],
    latest_successful_tool_index: Callable[[list[dict[str, Any]], set[str]], int],
    next_successful_tool_index: Callable[[list[dict[str, Any]], int, str], int],
    resolve_dataset_hint_from_result: Callable[..., tuple[str, str, str]],
    assistant_calls: list[dict[str, Any]],
) -> dict[str, Any] | None:
    if not objective_requests_cloud_load_sequence(objective_text):
        return None
    cloud_success_idx = latest_successful_tool_index(results, {"loadCloudMapAndWait", "loadQMapCloudMap"})
    wait_after_cloud_idx = (
        next_successful_tool_index(results, cloud_success_idx + 1, "waitForQMapDataset")
        if cloud_success_idx >= 0
        else -1
    )
    if not (cloud_success_idx >= 0 and wait_after_cloud_idx < 0 and "waitForQMapDataset" in tools_available):
        return None
    cloud_result = results[cloud_success_idx]
    _, _, cloud_dataset_hint = resolve_dataset_hint_from_result(cloud_result, assistant_calls)
    next_step = (
        f"Call waitForQMapDataset{cloud_dataset_hint} before final confirmation."
        if cloud_dataset_hint
        else (
            "Call waitForQMapDataset for the final cloud-loaded dataset before final confirmation. "
            "If datasetName is still ambiguous, reuse the latest cloud-map dataset snapshot first."
        )
    )
    return {
        "ruleId": "cloud_load_requires_wait_validation",
        "score": 171,
        "guardrail": (
            "Cloud-load objective detected, but no successful waitForQMapDataset step exists after the cloud load. "
            "Do not finalize with cloud-success claims yet."
        ),
        "next": next_step,
        "forceToolChoice": "waitForQMapDataset",
    }


def build_post_count_isolate_final_candidate(
    *,
    results: list[dict[str, Any]],
    last_idx: int,
    last_success: bool,
    last_tool: str,
    dataset_name: str,
    dataset_ref: str,
    dataset_hint: str,
    tools_available: set[str],
    objective_text: str,
    objective_prefers_visibility_isolation: Callable[[str], bool],
    dataset_create_or_update_tools: set[str],
    visibility_isolation_tools: set[str],
    resolve_dataset_hint_from_result: Callable[..., tuple[str, str, str]],
    assistant_calls: list[dict[str, Any]],
) -> dict[str, Any] | None:
    if not (
        last_success
        and last_tool == "countQMapRows"
        and "showOnlyQMapLayer" in tools_available
        and objective_prefers_visibility_isolation(objective_text)
    ):
        return None
    wait_idx = -1
    for idx in range(last_idx - 1, -1, -1):
        row = results[idx]
        if row.get("success") is True and str(row.get("toolName") or "").strip() == "waitForQMapDataset":
            wait_idx = idx
            break
    create_idx = -1
    if wait_idx >= 0:
        for idx in range(wait_idx - 1, -1, -1):
            row = results[idx]
            if row.get("success") is True and str(row.get("toolName") or "").strip() in dataset_create_or_update_tools:
                create_idx = idx
                break
    if create_idx < 0:
        return None
    already_isolated = any(
        row.get("success") is True and str(row.get("toolName") or "").strip() in visibility_isolation_tools
        for row in results[create_idx + 1 :]
    )
    if already_isolated:
        return None
    next_dataset_hint = dataset_hint
    if not dataset_name and not dataset_ref:
        _, _, next_dataset_hint = resolve_dataset_hint_from_result(results[wait_idx], assistant_calls)
    return {
        "ruleId": "post_count_isolate_final",
        "score": 127,
        "guardrail": (
            "Dataset validation is complete, but final layer isolation is still missing. "
            "Do not return final text yet."
        ),
        "next": f"Call showOnlyQMapLayer{next_dataset_hint} before final confirmation.",
    }


def build_map_display_requires_fit_candidate(
    *,
    results: list[dict[str, Any]],
    objective_text: str,
    tools_available: set[str],
    center_dataset_hint: str,
    objective_requests_map_display: Callable[[str], bool],
    latest_successful_tool_index: Callable[[list[dict[str, Any]], set[str]], int],
    dataset_create_or_update_tools: set[str],
) -> dict[str, Any] | None:
    relevant_map_load_tools = dataset_create_or_update_tools.union(
        {
            "queryQCumberDataset",
            "queryQCumberDatasetSpatial",
            "queryQCumberTerritorialUnits",
            "loadData",
            "loadCloudMapAndWait",
            "loadQMapCloudMap",
            "saveDataToMap",
        }
    )
    has_loaded_map_data = latest_successful_tool_index(results, relevant_map_load_tools) >= 0
    fit_success_idx = latest_successful_tool_index(results, {"fitQMapToDataset"})
    if not (
        objective_requests_map_display(objective_text)
        and fit_success_idx < 0
        and has_loaded_map_data
        and "fitQMapToDataset" in tools_available
    ):
        return None
    return {
        "ruleId": "map_display_requires_fit_evidence",
        "score": 146,
        "guardrail": (
            "Map-display objective detected, but there is no successful fit evidence yet. "
            "Do not finalize with 'shown on map' claims."
        ),
        "next": f"Call fitQMapToDataset{center_dataset_hint} before final confirmation.",
    }


def build_centering_fit_candidate(
    *,
    results: list[dict[str, Any]],
    objective_text: str,
    tools_available: set[str],
    center_dataset_hint: str,
    objective_requests_map_centering: Callable[[str], bool],
    latest_successful_tool_index: Callable[[list[dict[str, Any]], set[str]], int],
    latest_failed_tool_index: Callable[[list[dict[str, Any]], set[str]], int],
    dataset_create_or_update_tools: set[str],
) -> dict[str, Any] | None:
    if not objective_requests_map_centering(objective_text):
        return None
    relevant_map_load_tools = dataset_create_or_update_tools.union(
        {
            "queryQCumberDataset",
            "queryQCumberDatasetSpatial",
            "queryQCumberTerritorialUnits",
            "loadData",
            "loadCloudMapAndWait",
            "loadQMapCloudMap",
            "saveDataToMap",
        }
    )
    has_loaded_map_data = latest_successful_tool_index(results, relevant_map_load_tools) >= 0
    fit_failed_idx = latest_failed_tool_index(results, {"fitQMapToDataset"})
    fit_success_idx = latest_successful_tool_index(results, {"fitQMapToDataset"})
    fit_recovered = fit_failed_idx >= 0 and fit_success_idx > fit_failed_idx
    if fit_failed_idx >= 0 and not fit_recovered:
        if "fitQMapToDataset" in tools_available:
            next_step = (
                f"Call fitQMapToDataset{center_dataset_hint}. "
                "If fit fails again, finalize with explicit limitation text and do not claim centering success."
            )
        else:
            next_step = (
                "Finalize with explicit limitation text (map centering not confirmed); "
                "do not claim centering success."
            )
        return {
            "ruleId": "centering_requires_successful_fit",
            "score": 176,
            "guardrail": (
                "Centering objective detected, but the latest fit step failed. "
                "Do not claim that the map is centered."
            ),
            "next": next_step,
        }
    if fit_success_idx < 0 and has_loaded_map_data and "fitQMapToDataset" in tools_available:
        return {
            "ruleId": "centering_missing_fit_evidence",
            "score": 139,
            "guardrail": (
                "Centering objective detected, but there is no successful fit evidence yet. "
                "Do not finalize with centering claims."
            ),
            "next": f"Call fitQMapToDataset{center_dataset_hint} before final confirmation.",
        }
    return None


def build_turn_state_discovery_recovery_candidate(
    *,
    last_result: dict[str, Any],
    is_turn_state_discovery_failure: Callable[[Any], bool],
) -> dict[str, Any] | None:
    if not (
        last_result.get("success") is False
        and is_turn_state_discovery_failure(last_result.get("details"))
    ):
        return None
    return {
        "ruleId": "turn_state_discovery_recovery",
        "score": 168,
        "guardrail": (
            "Latest step failed due to turn-state discovery gate. "
            "Do not keep retrying operational tools before discovery."
        ),
        "next": (
            "Call listQMapDatasets once and wait for success, then continue with the next operational step."
        ),
    }


def build_metric_field_missing_recovery_candidate(
    *,
    results: list[dict[str, Any]],
    last_idx: int,
    last_result: dict[str, Any],
    last_tool: str,
    dataset_hint: str,
    objective_text: str,
    is_metric_field_not_found_failure: Callable[[Any], bool],
    extract_missing_metric_field: Callable[[Any], str],
    find_related_tool_call: Callable[..., dict[str, Any] | None],
    assistant_calls: list[dict[str, Any]],
    extract_metric_output_field_from_call: Callable[[Any], str],
    resolve_dataset_hint_from_result: Callable[..., tuple[str, str, str]],
    dataset_create_or_update_tools: set[str],
    objective_targets_problem_metric: Callable[[str], bool],
    objective_explicit_population_metric: Callable[[str], bool],
) -> dict[str, Any] | None:
    if not (
        last_result.get("success") is False
        and is_metric_field_not_found_failure(last_result.get("details"))
    ):
        return None
    missing_metric = extract_missing_metric_field(last_result.get("details"))
    suggestion_field = ""
    dataset_hint_for_metric = dataset_hint
    for prev_idx in range(last_idx - 1, -1, -1):
        prev = results[prev_idx]
        if prev.get("success") is not True:
            continue
        prev_tool = str(prev.get("toolName") or "").strip()
        if prev_tool not in dataset_create_or_update_tools:
            continue
        candidate_field = extract_metric_output_field_from_call(prev)
        if not candidate_field:
            prev_call = find_related_tool_call(prev, assistant_calls)
            candidate_field = extract_metric_output_field_from_call(prev_call)
        if candidate_field:
            suggestion_field = candidate_field
        if not dataset_hint_for_metric:
            _, _, dataset_hint_for_metric = resolve_dataset_hint_from_result(prev, assistant_calls)
        break
    objective_problem_mode = objective_targets_problem_metric(objective_text) and not objective_explicit_population_metric(
        objective_text
    )
    strict_note = (
        " Do not switch to population/name fallback metrics unless the user explicitly asks for those metrics."
        if objective_problem_mode
        else ""
    )
    if last_tool == "setQMapLayerColorByField":
        retry_part = (
            f'retry setQMapLayerColorByField with fieldName="{suggestion_field}".'
            if suggestion_field
            else "retry setQMapLayerColorByField with an existing numeric field from the dataset preview."
        )
        next_step = (
            f"Call previewQMapDatasetRows{dataset_hint_for_metric} with limit=1 to inspect real fields, then {retry_part} "
            "Do not rerun query/join recompute steps while the current dataset already exists."
        )
    else:
        retry_part = (
            f'retry rankQMapDatasetRows with metricFieldName="{suggestion_field}".'
            if suggestion_field
            else "retry rankQMapDatasetRows with an existing numeric metric field from the dataset preview."
        )
        next_step = (
            f"Call previewQMapDatasetRows{dataset_hint_for_metric} with limit=1 to inspect real fields, then {retry_part}"
        )
    return {
        "ruleId": "metric_field_missing_recovery",
        "score": 150,
        "guardrail": (
            f'Previous step failed because metric field "{missing_metric or "unknown"}" was not found.'
            " Do not finalize yet."
            + strict_note
        ),
        "next": next_step,
    }


def build_save_cached_dataset_before_wait_candidate(
    *,
    results: list[dict[str, Any]],
    last_idx: int,
    last_result: dict[str, Any],
    last_tool: str,
    dataset_name: str,
    related_call: dict[str, Any] | None,
    tools_available: set[str],
    classify_runtime_error_kind: Callable[[Any], str],
    extract_dataset_name_from_call: Callable[[Any], str],
    latest_successful_tool_for_dataset: Callable[..., tuple[int, str]],
    assistant_calls: list[dict[str, Any]],
    remote_cache_dataset_tools: set[str],
    has_successful_save_data_to_map_for_dataset: Callable[..., bool],
) -> dict[str, Any] | None:
    if not (
        last_result.get("success") is False
        and last_tool in {"waitForQMapDataset", "countQMapRows", "fitQMapToDataset"}
        and classify_runtime_error_kind(last_result.get("details")) == "dataset_not_found"
        and "saveDataToMap" in tools_available
    ):
        return None
    failing_dataset_name = extract_dataset_name_from_call(related_call) or dataset_name
    source_idx, source_dataset_name = latest_successful_tool_for_dataset(
        results,
        assistant_calls,
        tool_names=remote_cache_dataset_tools,
        until_idx=last_idx,
        preferred_dataset_name=failing_dataset_name,
    )
    chosen_dataset_name = source_dataset_name or failing_dataset_name
    if not chosen_dataset_name:
        return None
    already_saved = has_successful_save_data_to_map_for_dataset(
        results,
        assistant_calls,
        dataset_name=chosen_dataset_name,
        start_idx=max(0, source_idx),
    )
    if already_saved:
        return None
    return {
        "ruleId": "save_cached_dataset_before_wait",
        "score": 166,
        "guardrail": (
            f'`{last_tool}` failed because dataset "{chosen_dataset_name}" is not in map state yet. '
            "Do not retry wait/count/fit on an unsaved cached dataset."
        ),
        "next": (
            f'Call saveDataToMap(datasetNames=["{chosen_dataset_name}"]), '
            f'then call waitForQMapDataset(datasetName="{chosen_dataset_name}", timeoutMs=60000), '
            f'and then countQMapRows(datasetName="{chosen_dataset_name}"). '
            "Do not rerun isochrone unless saveDataToMap fails."
        ),
    }


def build_zonal_freeze_fallback_candidate(
    *,
    last_result: dict[str, Any],
    last_tool: str,
    related_call: dict[str, Any] | None,
    dataset_name: str,
    tools_available: set[str],
    is_zonal_ui_freeze_failure: Callable[[Any], bool],
    infer_h3_resolution_from_text: Callable[[Any], int | None],
    extract_zonal_values_count: Callable[[Any], int],
    replace_or_append_h3_resolution_suffix: Callable[[str, int], str],
) -> dict[str, Any] | None:
    if not (
        last_result.get("success") is False
        and last_tool == "zonalStatsByAdmin"
        and is_zonal_ui_freeze_failure(last_result.get("details"))
        and "aggregateDatasetToH3" in tools_available
        and "zonalStatsByAdmin" in tools_available
    ):
        return None

    zonal_args = related_call.get("args") if isinstance(related_call, dict) else {}
    zonal_args = zonal_args if isinstance(zonal_args, dict) else {}
    admin_dataset_name = str(zonal_args.get("adminDatasetName") or "").strip()
    value_dataset_name = str(zonal_args.get("valueDatasetName") or "").strip()
    value_field = str(zonal_args.get("valueField") or "").strip()
    aggregation = str(zonal_args.get("aggregation") or "").strip().lower()
    if aggregation not in {"count", "sum", "avg", "min", "max"}:
        aggregation = "sum" if value_field else "count"

    current_resolution = infer_h3_resolution_from_text(value_dataset_name)
    if current_resolution is None:
        current_resolution = infer_h3_resolution_from_text(last_result.get("details"))
    if current_resolution is None:
        values_count = extract_zonal_values_count(last_result.get("details")) or 0
        if values_count >= 12000:
            current_resolution = 6
        elif values_count >= 4000:
            current_resolution = 5
        else:
            current_resolution = 4

    if current_resolution <= 4:
        return {
            "ruleId": "zonal_freeze_fallback_floor_reached",
            "score": 168,
            "guardrail": (
                "Zonal workflow hit UI-freeze budget at H3 resolution r4. "
                "Do not ask for confirmation and do not keep retrying the same plan. "
                "Do not infer geographic ranking outcomes without computed ranked evidence."
            ),
            "next": (
                "Return explicit failure now: deterministic H3 fallback chain reached r4 and still exceeds budget. "
                "Report this as a computational limit and provide one concise next actionable option. "
                "Do not claim which areas have least/most forests unless rankQMapDatasetRows succeeded."
            ),
        }

    next_resolution = max(4, current_resolution - 1)
    rerouted_dataset_name = replace_or_append_h3_resolution_suffix(
        value_dataset_name or "value_dataset_h3",
        next_resolution,
    )
    aggregate_op = "count" if aggregation == "count" else aggregation
    rerouted_value_field = "count" if aggregation == "count" else aggregation
    next_step = (
        f'Call aggregateDatasetToH3(datasetName="{value_dataset_name or dataset_name}", '
        f'resolution={next_resolution}, operations=["{aggregate_op}"], '
        + (
            f'valueField="{value_field}", '
            if (value_field and aggregation != "count")
            else ""
        )
        + f'targetDatasetName="{rerouted_dataset_name}", showOnMap=false), then call waitForQMapDataset(datasetName="{rerouted_dataset_name}"), '
        f'then call zonalStatsByAdmin(adminDatasetName="{admin_dataset_name}", valueDatasetName="{rerouted_dataset_name}", '
        + (f'valueField="{rerouted_value_field}", ' if rerouted_value_field else "")
        + f'aggregation="{aggregation}"). '
        + (
            f"If this still fails with the same budget error, repeat deterministically with r{max(4, next_resolution - 1)} "
            "until r4, then return explicit failure."
        )
    )
    return {
        "ruleId": "zonal_freeze_deterministic_h3_fallback",
        "score": 170,
        "guardrail": (
            "zonalStatsByAdmin failed due UI-freeze budget. "
            "Use deterministic H3 coarsening fallback and continue automatically."
        ),
        "next": next_step,
    }


def build_clip_stats_clip_required_candidate(
    *,
    objective_text: str,
    results: list[dict[str, Any]],
    tools_available: set[str],
    needs_cross_geometry_clip_guardrail: Callable[..., bool],
    h3_clip_tools: set[str],
    build_source_dataset_hint: Callable[[Any, Any], str],
    dataset_ref: str,
    dataset_name: str,
) -> dict[str, Any] | None:
    if not (
        needs_cross_geometry_clip_guardrail(objective_text=objective_text, results=results)
        and h3_clip_tools.intersection(tools_available)
    ):
        return None
    clip_tool = "clipQMapDatasetByGeometry" if "clipQMapDatasetByGeometry" in tools_available else "clipDatasetByBoundary"
    clip_dataset_hint = build_source_dataset_hint(dataset_ref, dataset_name)
    return {
        "ruleId": "clip_stats_clip_required",
        "score": 161,
        "guardrail": (
            "Cross-geometry clip/statistics objective detected but no successful clip step is available yet. "
            "Do not finalize until a real clip output is produced."
        ),
        "next": (
            f"Call {clip_tool}{clip_dataset_hint}, then call waitForQMapDataset and countQMapRows "
            "on the clipped output before coverage/statistics confirmation."
        ),
    }


def build_boundary_clip_required_candidate(
    *,
    objective_text: str,
    results: list[dict[str, Any]],
    tools_available: set[str],
    needs_boundary_clip_guardrail: Callable[..., bool],
    h3_clip_tools: set[str],
    h3_boundary_materialization_tools: set[str],
    latest_successful_tool_index: Callable[[list[dict[str, Any]], set[str]], int],
    find_related_tool_call: Callable[..., dict[str, Any] | None],
    assistant_calls: list[dict[str, Any]],
    extract_dataset_name_from_call: Callable[[Any], str],
    extract_dataset_ref_from_call: Callable[[Any], str],
    build_source_dataset_hint: Callable[[Any, Any], str],
) -> dict[str, Any] | None:
    if not (
        needs_boundary_clip_guardrail(objective_text=objective_text, results=results)
        and h3_clip_tools.intersection(tools_available)
    ):
        return None
    last_h3_idx = latest_successful_tool_index(results, h3_boundary_materialization_tools)
    h3_dataset_hint = ""
    if last_h3_idx >= 0:
        h3_result = results[last_h3_idx]
        related_h3_call = find_related_tool_call(h3_result, assistant_calls)
        h3_dataset_name = extract_dataset_name_from_call(related_h3_call) or str(h3_result.get("datasetName") or "")
        h3_dataset_ref = extract_dataset_ref_from_call(related_h3_call) or str(h3_result.get("datasetRef") or "")
        h3_dataset_hint = build_source_dataset_hint(h3_dataset_ref, h3_dataset_name)
    clip_tool = "clipQMapDatasetByGeometry" if "clipQMapDatasetByGeometry" in tools_available else "clipDatasetByBoundary"
    return {
        "ruleId": "boundary_clip_required",
        "score": 105,
        "guardrail": "Boundary-constrained H3 workflow detected but no successful clip step yet. Do not return final text yet.",
        "next": (
            f"Call {clip_tool}{h3_dataset_hint}, then waitForQMapDataset for the clipped output. "
            "After wait succeeds, call countQMapRows before final confirmation."
        ),
    }


def build_perimeter_overlay_coverage_candidate(
    *,
    objective_text: str,
    results: list[dict[str, Any]],
    tools_available: set[str],
    needs_overlay_coverage_guardrail: Callable[..., bool],
    latest_successful_tool_index: Callable[[list[dict[str, Any]], set[str]], int],
    overlay_execution_tools: set[str],
    h3_clip_tools: set[str],
    find_related_tool_call: Callable[..., dict[str, Any] | None],
    assistant_calls: list[dict[str, Any]],
    extract_dataset_name_from_call: Callable[[Any], str],
    extract_dataset_ref_from_call: Callable[[Any], str],
) -> dict[str, Any] | None:
    if not (
        needs_overlay_coverage_guardrail(objective_text=objective_text, results=results)
        and "coverageQualityReport" in tools_available
    ):
        return None
    overlay_idx = latest_successful_tool_index(results, overlay_execution_tools)
    clip_idx = latest_successful_tool_index(results, h3_clip_tools)
    overlay_dataset = ""
    reference_dataset = ""
    if overlay_idx >= 0:
        overlay_result = results[overlay_idx]
        overlay_call = find_related_tool_call(overlay_result, assistant_calls)
        overlay_dataset = (
            extract_dataset_name_from_call(overlay_call)
            or str(overlay_result.get("datasetRef") or "")
            or str(overlay_result.get("datasetName") or "")
        ).strip()
        if isinstance(overlay_call, dict):
            args = overlay_call.get("args")
            if isinstance(args, dict):
                reference_dataset = str(args.get("datasetBName") or args.get("datasetAName") or "").strip()
        if not reference_dataset and clip_idx >= 0:
            clip_call = find_related_tool_call(results[clip_idx], assistant_calls)
            if isinstance(clip_call, dict):
                clip_args = clip_call.get("args")
                if isinstance(clip_args, dict):
                    reference_dataset = str(
                        clip_args.get("boundaryDatasetName")
                        or clip_args.get("clipDatasetName")
                        or clip_args.get("sourceDatasetName")
                        or ""
                    ).strip()
    elif clip_idx >= 0:
        clip_result = results[clip_idx]
        clip_call = find_related_tool_call(clip_result, assistant_calls)
        overlay_dataset = (
            extract_dataset_name_from_call(clip_call)
            or extract_dataset_ref_from_call(clip_call)
            or str(clip_result.get("datasetRef") or "")
            or str(clip_result.get("datasetName") or "")
        ).strip()
        if isinstance(clip_call, dict):
            clip_args = clip_call.get("args")
            if isinstance(clip_args, dict):
                reference_dataset = str(
                    clip_args.get("boundaryDatasetName")
                    or clip_args.get("clipDatasetName")
                    or clip_args.get("sourceDatasetName")
                    or ""
                ).strip()

    coverage_call = "Call coverageQualityReport"
    if overlay_dataset and reference_dataset:
        coverage_call += f'(leftDatasetName="{overlay_dataset}", rightDatasetName="{reference_dataset}")'
    elif overlay_dataset:
        coverage_call += f'(leftDatasetName="{overlay_dataset}", rightDatasetName="<boundary/reference dataset>")'
    else:
        coverage_call += "(leftDatasetName=<overlay output>, rightDatasetName=<boundary/reference dataset>)"

    return {
        "ruleId": "perimeter_overlay_coverage_required",
        "score": 158,
        "guardrail": (
            "Clip/intersection workflow detected but no coverage validation evidence is available yet. "
            "Do not finalize until coverage diagnostics are computed."
        ),
        "next": (
            coverage_call
            + ", then report `coveragePct` and `matchedRows` (plus `nullJoinPct` when available) before final confirmation."
        ),
    }
