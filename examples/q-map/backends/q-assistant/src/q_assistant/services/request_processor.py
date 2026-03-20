from __future__ import annotations

import hashlib
import json
import os
import re
import time
from typing import Any

from ..audit_logging import (
    _collect_response_tool_call_names,
    _configure_audit_runtime,
)
from ..chat_payload_compaction import _serialize_tool_call_args_for_signature
from ..message_text import _extract_prompt_from_messages
from ..objective_intent import (
    _objective_mentions_cloud_or_saved_maps,
    _objective_requests_cloud_load_sequence,
    _objective_requests_dataset_discovery,
    _objective_requests_field_correlation,
    _objective_requests_linear_regression,
    _objective_requests_natural_break_classification,
    _objective_requests_regulatory_compliance,
    _objective_requests_regulatory_listing,
    _objective_requests_exposure_assessment,
    _objective_requests_spatial_interpolation,
    _objective_requires_clip_stats_coverage_validation,
    _objective_requires_ranked_output,
)
from ..qmap_context import _sanitize_qmap_context_payload
from ..request_tool_results import (
    _extract_recent_tool_results_since_last_user,
    _extract_request_tool_results,
    _has_assistant_text_since_last_user,
    _messages_since_last_user,
)
from ..response_claims import (
    _response_claims_centering_success,
    _response_claims_operational_success,
    _response_claims_success,
)
from ..runtime_guardrails import (
    RuntimeToolLoopLimitBindings,
    objective_requests_map_centering as _objective_requests_map_centering,
    objective_requests_map_display as _objective_requests_map_display,
    objective_requests_provider_discovery as _objective_requests_provider_discovery,
)
from ..runtime_workflow_state import build_runtime_workflow_state
from ..tool_calls import (
    _extract_assistant_tool_calls,
    _extract_request_tool_names,
    _parse_tool_arguments,
)
from ..tool_result_parsing import (
    _build_dataset_hint,
    _build_source_dataset_hint,
    _extract_dataset_ref_from_call,
)


_MODEL_CONTEXT_LIMIT_HINTS: list[tuple[str, int]] = [
    ("gpt-4.1", 1_047_576),
    ("gpt-5", 400_000),
    ("gpt-4o", 128_000),
    ("gpt-4-turbo", 128_000),
    ("claude", 200_000),
    ("gemini-3", 1_048_576),
    ("gemini-2.5", 1_048_576),
    ("gemini-2", 1_048_576),
    ("gemini-1.5", 1_048_576),
    ("gemini", 1_048_576),
    ("llama", 128_000),
    ("qwen", 131_072),
]


def _with_context(prompt: str, context: dict | None) -> str:
    if not context:
        return prompt
    return f"{prompt}\n\nMap context:\n{context}"


def _build_openai_stream_request_id_chunk(request_id: str, model: str | None) -> bytes:
    payload = {
        "id": f"q-assistant-{request_id}",
        "object": "chat.completion.chunk",
        "created": int(time.time()),
        "model": str(model or ""),
        "choices": [
            {
                "index": 0,
                "delta": {"content": f"[requestId: {request_id}]\n"},
                "finish_reason": None,
            }
        ],
    }
    return f"data: {json.dumps(payload, ensure_ascii=False)}\n\n".encode("utf-8")


_RUNTIME_GUARDRAIL_PREFIX = "[RUNTIME_GUARDRAIL]"
_RUNTIME_NEXT_STEP_PREFIX = "[RUNTIME_NEXT_STEP]"
_DATASET_CREATE_OR_UPDATE_TOOLS: set[str] = {
    "tassellateSelectedGeometry",
    "tassellateDatasetLayer",
    "aggregateDatasetToH3",
    "joinQMapDatasetsOnH3",
    "populateTassellationFromAdminUnits",
    "populateTassellationFromAdminUnitsAreaWeighted",
    "populateTassellationFromAdminUnitsDiscrete",
    "createDatasetFromFilter",
    "createDatasetFromCurrentFilters",
    "createDatasetWithGeometryArea",
    "createDatasetWithNormalizedField",
    "reprojectQMapDatasetCrs",
    "clipQMapDatasetByGeometry",
    "clipDatasetByBoundary",
    "overlayDifference",
    "overlayIntersection",
    "overlayUnion",
    "overlaySymmetricDifference",
    "eraseQMapDatasetByGeometry",
    "spatialJoinByPredicate",
    "zonalStatsByAdmin",
    "bufferAndSummarize",
    "nearestFeatureJoin",
    "adjacencyGraphFromPolygons",
    "mergeQMapDatasets",
    "dissolveQMapDatasetByField",
    "simplifyQMapDatasetGeometry",
    "splitQMapPolygonByLine",
    "drawQMapBoundingBox",
    "paintQMapH3Cell",
    "paintQMapH3Cells",
    "paintQMapH3Ring",
    "addComputedField",
    "computeQMapBivariateCorrelation",
    "computeQMapCompositeIndex",
    "computeQMapDatasetDelta",
    "computeQMapEquityIndices",
    "computeQMapHotspotAnalysis",
    "computeQMapSpatialAutocorrelation",
}
_H3_BOUNDARY_MATERIALIZATION_TOOLS: set[str] = {
    "aggregateDatasetToH3",
    "joinQMapDatasetsOnH3",
}
_H3_CLIP_TOOLS: set[str] = {
    "clipQMapDatasetByGeometry",
    "clipDatasetByBoundary",
}
_OVERLAY_EXECUTION_TOOLS: set[str] = {
    "overlayDifference",
    "overlayIntersection",
    "overlayUnion",
    "overlaySymmetricDifference",
}
_REMOTE_CACHE_DATASET_TOOLS: set[str] = {
    "isochrone",
}
_VISIBILITY_ISOLATION_TOOLS: set[str] = {
    "showOnlyQMapLayer",
}
_DISCOVERY_TOOLS: set[str] = {
    "listQMapDatasets",
    "listQMapChartTools",
    "listQCumberProviders",
    "listQCumberDatasets",
    "getQCumberDatasetHelp",
    "queryQCumberDatasetSchema",
    "queryQCumberDatasetSchemaFlat",
}
_QCUMBER_DISCOVERY_TOOLS: set[str] = {
    "listQCumberProviders",
    "listQCumberDatasets",
    "getQCumberDatasetHelp",
    "queryQCumberDatasetSchema",
    "queryQCumberDatasetSchemaFlat",
}
_QCUMBER_PROVIDER_SCOPED_TOOLS: set[str] = {
    "listQCumberDatasets",
    "getQCumberDatasetHelp",
    "queryQCumberDatasetSchema",
    "queryQCumberDatasetSchemaFlat",
    "queryQCumberDataset",
    "queryQCumberDatasetSpatial",
    "queryQCumberTerritorialUnits",
}
_DISCOVERY_LOOP_PRUNE_TOOLS: set[str] = {
    "listQCumberProviders",
    "listQCumberDatasets",
}
_DISCOVERY_LOOP_PROGRESS_TOOLS: set[str] = {
    "queryQCumberTerritorialUnits",
    "queryQCumberDatasetSpatial",
    "queryQCumberDataset",
    "getQCumberDatasetHelp",
}
_FORBIDDEN_QMAP_RUNTIME_TOOLS: set[str] = {
    "tableTool",
    "mergeTablesTool",
}
_STYLE_EXECUTION_TOOLS: set[str] = {
    "applyQMapStylePreset",
    "setQMapLayerColorByField",
    "setQMapLayerColorByThresholds",
    "setQMapLayerColorByStatsThresholds",
    "setQMapLayerSolidColor",
    "setQMapLayerHeightByField",
}
_POST_CREATE_VALIDATION_DEFERRED_TOOLS: set[str] = _STYLE_EXECUTION_TOOLS.union(
    {
        "fitQMapToDataset",
        "showOnlyQMapLayer",
        "setQMapLayerVisibility",
        "rankQMapDatasetRows",
        "setQMapTooltipFields",
    }
)
_AUDIT_SUCCESS_EVIDENCE_TOOLS: set[str] = _DATASET_CREATE_OR_UPDATE_TOOLS.union(
    {
        "queryQCumberDataset",
        "queryQCumberDatasetSpatial",
        "queryQCumberTerritorialUnits",
        "loadData",
        "loadCloudMapAndWait",
        "loadQMapCloudMap",
        "saveDataToMap",
        "fitQMapToDataset",
    }
)
_BRIDGE_OPERATION_TOOLS: set[str] = {
    "loadData",
    "saveDataToMap",
}


def _read_env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return int(default)
    try:
        return int(str(raw).strip())
    except Exception:
        return int(default)


_TOOL_CALL_WORKFLOW_HARD_CAP = max(6, _read_env_int("Q_ASSISTANT_TOOL_CALL_HARD_CAP", 20))
_TOOL_ONLY_NO_TEXT_WATCHDOG_MIN_CALLS = max(
    4,
    _read_env_int("Q_ASSISTANT_TOOL_ONLY_NO_TEXT_WATCHDOG_MIN_CALLS", 8),
)
_ERROR_CLASS_MAX_RETRIES = max(0, _read_env_int("Q_ASSISTANT_ERROR_CLASS_MAX_RETRIES", 1))
_IDENTICAL_TOOL_ARGS_MAX_RETRIES = max(
    0,
    _read_env_int("Q_ASSISTANT_IDENTICAL_TOOL_ARGS_MAX_RETRIES", 1),
)
_IDENTICAL_TOOL_ARGS_SUCCESS_MAX_REPEATS = max(
    0,
    _read_env_int("Q_ASSISTANT_IDENTICAL_TOOL_ARGS_SUCCESS_MAX_REPEATS", 0),
)
_RUNTIME_RESPONSE_MODE_PREFIX = "[RUNTIME_RESPONSE_MODE]"


def _classify_runtime_error_kind(details: Any) -> str:
    text = " ".join(str(details or "").strip().lower().split())
    if not text:
        return "generic_failure"
    if _is_turn_state_discovery_failure(text):
        return "turn_state_discovery_gate"
    if "no validated fallback available" in text and "cloud" in text:
        return "cloud_no_validated_fallback"
    if "cloud map load timed out after retry" in text or "cloud load timed out after retry" in text:
        return "cloud_no_validated_fallback"
    if (
        "invalid providerid" in text
        or ("providerid" in text and "not found" in text)
        or ("provider id" in text and "not found" in text)
        or "provider not found" in text
    ):
        return "invalid_provider_id"
    if (
        "invalid datasetid" in text
        or ("datasetid" in text and "not found" in text)
        or ("dataset id" in text and "not found" in text)
        or ("dataset '" in text and "not found for provider" in text)
        or ('dataset "' in text and "not found for provider" in text)
        or "use an exact datasetid from listqcumberdatasets(providerid)" in text
        or "missing datasetid and unable to auto-select a unique dataset for provider" in text
    ):
        return "invalid_dataset_id"
    if (
        "ambiguous administrative match" in text
        or ("matched multiple levels" in text and "expectedadmintype" in text)
        or ("matched multiple levels" in text and "lv filter" in text)
        or ("exists on multiple levels" in text and "province" in text and "municipality" in text)
    ):
        return "ambiguous_admin_match"
    if _is_admin_level_validation_failure(text):
        return "admin_level_validation_failure"
    if _is_metric_field_not_found_failure(text):
        return "field_missing"
    if ("field" in text and "not found" in text) or "missing field" in text:
        return "field_missing"
    if _is_low_distinct_color_failure(text):
        return "low_distinct_color"
    if "timeout waiting for dataset" in text or (
        "timeout" in text and any(marker in text for marker in ("materializ", "waitforqmapdataset", "dataset"))
    ):
        return "materialization_timeout"
    if "aborted to prevent ui freeze" in text or "prevent ui freeze" in text:
        return "ui_freeze_budget"
    if "not materialized yet" in text or "requires canonical datasetref" in text:
        return "dataset_not_found"
    if "dataset" in text and "not found" in text:
        return "dataset_not_found"
    if "join" in text and any(marker in text for marker in ("mismatch", "no match", "not matching", "cannot join")):
        return "join_mismatch"
    if (
        "0 rows" in text
        or "zero rows" in text
        or "has no rows" in text
        or "empty dataset" in text
        or "no records" in text
    ):
        return "validation_zero_rows"
    return "generic_failure"


def _runtime_failure_error_class(result: dict[str, Any]) -> str:
    if not isinstance(result, dict):
        return ""
    if result.get("success") is not False:
        return ""
    tool_name = str(result.get("toolName") or "").strip() or "unknown_tool"
    details = result.get("details")
    error_kind = _classify_runtime_error_kind(details)
    return f"{tool_name}:{error_kind}"


def _runtime_error_retry_policy(error_kind: str) -> dict[str, Any]:
    kind = str(error_kind or "").strip().lower() or "generic_failure"
    default_allowed_retries = int(_ERROR_CLASS_MAX_RETRIES)
    policies: dict[str, dict[str, Any]] = {
        "materialization_timeout": {
            "allowedRetries": default_allowed_retries,
            "remediationHint": (
                "Retry once with bounded wait/materialization parameters; if it still fails, "
                "reduce geometry scope or resolution."
            ),
            "nextStep": (
                "Retry once with adjusted timeout/scope/resolution. "
                "If the same timeout repeats, switch to lighter materialization or finalize with computational limits."
            ),
        },
        "cloud_no_validated_fallback": {
            "allowedRetries": 0,
            "remediationHint": (
                "Cloud load already exhausted retry/fallback options without a validated result. "
                "Do not keep retrying cloud/bridge loads in the same turn."
            ),
            "nextStep": (
                "Return an explicit limitation unless a later validated fallback already succeeded "
                "(successful fallback load followed by waitForQMapDataset)."
            ),
        },
        "field_missing": {
            "allowedRetries": default_allowed_retries,
            "remediationHint": (
                "Do not retry with the same metric/field. Inspect real dataset fields and rerun with a valid field name."
            ),
            "nextStep": (
                "Use preview/distinct to discover existing fields, then rerun with a valid field. "
                "Do not keep retrying unknown field names."
            ),
        },
        "join_mismatch": {
            "allowedRetries": default_allowed_retries,
            "remediationHint": (
                "Join key mismatch detected. Validate join keys/cardinality before retrying join operations."
            ),
            "nextStep": (
                "Inspect both datasets for compatible join keys and data types, then retry with explicit validated key mapping."
            ),
        },
        "validation_zero_rows": {
            "allowedRetries": default_allowed_retries,
            "remediationHint": (
                "Validation produced zero rows. Recheck filters/spatial bounds before retrying the same pipeline."
            ),
            "nextStep": (
                "Inspect row counts and relax/fix filters or bounds, then rerun validation once with corrected constraints."
            ),
        },
        "dataset_not_found": {
            "allowedRetries": default_allowed_retries,
            "remediationHint": (
                "Dataset reference is missing from active map state. "
                "For derived datasets (clip/aggregate/join outputs) use waitForQMapDataset with the canonical name "
                "returned by the tool — they are already in map state and do NOT go through saveDataToMap. "
                "saveDataToMap is only for external tool results stored in ToolCache (isochrone, buffer, etc.)."
            ),
            "nextStep": (
                "Call waitForQMapDataset with the canonical dataset name from the prior tool result, "
                "or listQMapDatasets to discover the correct name. "
                "Do NOT call saveDataToMap for clip/aggregate/join derived datasets."
            ),
        },
        "invalid_provider_id": {
            "allowedRetries": default_allowed_retries,
            "remediationHint": (
                "Do not keep using a guessed or stale providerId. Refresh the provider catalog and pick an explicit providerId "
                "before dataset/help/query calls."
            ),
            "nextStep": (
                "Call listQCumberProviders, choose an explicit providerId from the returned catalog, then continue with "
                "listQCumberDatasets/help/query tools."
            ),
        },
        "invalid_dataset_id": {
            "allowedRetries": 0,
            "remediationHint": (
                "Do not keep using a guessed or stale datasetId. Refresh the dataset catalog for the selected provider "
                "and continue only with an explicit datasetId from that catalog."
            ),
            "nextStep": (
                "Call listQCumberDatasets(providerId), choose an explicit datasetId from the returned catalog, then continue "
                "with getQCumberDatasetHelp/query tools."
            ),
        },
        "ambiguous_admin_match": {
            "allowedRetries": 0,
            "remediationHint": (
                "The active named-place query matched multiple administrative levels. Do not keep routing with guessed "
                "level intent in the same turn."
            ),
            "nextStep": (
                "Ask one concise clarification for the intended administrative level "
                "(province/municipality/region/country) and stop."
            ),
        },
        "admin_level_validation_failure": {
            "allowedRetries": 0,
            "remediationHint": (
                "Strict expectedAdminType/lv validation failed. Do not keep querying with a relaxed administrative "
                "level or silently switch to another level in the same turn."
            ),
            "nextStep": (
                "Return an explicit mismatch/limitation for the requested administrative level and stop. "
                "Only continue after the user changes the requested level or filters."
            ),
        },
        "turn_state_discovery_gate": {
            "allowedRetries": default_allowed_retries,
            "remediationHint": (
                "Turn-state requires fresh dataset discovery snapshot before operational tools."
            ),
            "nextStep": (
                "Run listQMapDatasets once successfully, then continue with operational tools."
            ),
        },
        "low_distinct_color": {
            "allowedRetries": default_allowed_retries,
            "remediationHint": (
                "Color metric is non-informative (<=1 distinct). Choose a different numeric metric before styling retry."
            ),
            "nextStep": (
                "Inspect distinct values and select a meaningful numeric field before retrying color-by-field."
            ),
        },
        "ui_freeze_budget": {
            "allowedRetries": 0,
            "remediationHint": (
                "UI freeze budget reached. Avoid repeating heavy workflow at same complexity."
            ),
            "nextStep": (
                "Switch to a lighter fallback plan (lower resolution/aggregation) or finalize with explicit computational limit."
            ),
        },
        "generic_failure": {
            "allowedRetries": default_allowed_retries,
            "remediationHint": "Use an alternative deterministic path after repeated generic failures.",
            "nextStep": (
                "Change at least one critical routing argument or switch to an alternative evidence path; "
                "if no deterministic alternative exists, finalize with explicit limitation."
            ),
        },
    }
    selected = policies.get(kind) or policies["generic_failure"]
    allowed_retries = selected.get("allowedRetries")
    try:
        allowed_retries_int = int(allowed_retries)
    except Exception:
        allowed_retries_int = default_allowed_retries
    recovery_action_by_kind: dict[str, str] = {
        "materialization_timeout": "retry_materialization_with_adjusted_timeout",
        "cloud_no_validated_fallback": "finalize_cloud_limitation",
        "field_missing": "inspect_fields_then_retry",
        "join_mismatch": "validate_join_keys_then_retry",
        "validation_zero_rows": "review_filters_or_bounds",
        "dataset_not_found": "materialize_dataset_then_retry",
        "invalid_provider_id": "refresh_provider_catalog",
        "invalid_dataset_id": "refresh_dataset_catalog",
        "ambiguous_admin_match": "ask_admin_level_clarification",
        "admin_level_validation_failure": "finalize_admin_level_mismatch",
        "turn_state_discovery_gate": "refresh_dataset_snapshot",
        "low_distinct_color": "fallback_non_metric_or_finalize",
        "ui_freeze_budget": "switch_lighter_plan_or_finalize",
        "generic_failure": "switch_alternative_path_or_finalize",
    }
    next_allowed_tools_by_kind: dict[str, list[str]] = {
        "materialization_timeout": ["waitForQMapDataset", "countQMapRows"],
        "cloud_no_validated_fallback": [],
        "field_missing": ["previewQMapDatasetRows", "distinctQMapFieldValues", "searchQMapFieldValues"],
        "join_mismatch": ["previewQMapDatasetRows", "distinctQMapFieldValues"],
        "validation_zero_rows": ["countQMapRows", "previewQMapDatasetRows"],
        "dataset_not_found": ["waitForQMapDataset", "listQMapDatasets", "loadData"],
        "invalid_provider_id": ["listQCumberProviders"],
        "invalid_dataset_id": ["listQCumberDatasets"],
        "ambiguous_admin_match": [],
        "admin_level_validation_failure": [],
        "turn_state_discovery_gate": ["listQMapDatasets"],
        "low_distinct_color": ["distinctQMapFieldValues", "previewQMapDatasetRows", "setQMapLayerSolidColor"],
        "ui_freeze_budget": [],
        "generic_failure": [],
    }
    recovery_action = recovery_action_by_kind.get(kind) or recovery_action_by_kind["generic_failure"]
    raw_next_allowed = next_allowed_tools_by_kind.get(kind) or next_allowed_tools_by_kind["generic_failure"]
    next_allowed_tools = [
        str(name or "").strip()
        for name in (raw_next_allowed if isinstance(raw_next_allowed, list) else [])
        if str(name or "").strip()
    ]
    return {
        "errorKind": kind,
        "allowedRetries": max(0, allowed_retries_int),
        "remediationHint": str(selected.get("remediationHint") or "").strip(),
        "nextStep": str(selected.get("nextStep") or "").strip(),
        "recoveryAction": recovery_action,
        "nextAllowedTools": next_allowed_tools,
    }


def _compact_signature_for_trace(signature: Any, *, max_len: int = 180) -> str:
    text = " ".join(str(signature or "").split())
    if len(text) <= max_len:
        return text
    digest = hashlib.sha1(text.encode("utf-8", errors="ignore")).hexdigest()[:10]
    keep = max(60, max_len - 14)
    return f"{text[:keep]}...#{digest}"


def _build_tool_call_args_signature_by_id(payload: dict[str, Any], *, max_items: int = 192) -> dict[str, str]:
    messages = _messages_since_last_user(payload.get("messages"))
    assistant_calls = _extract_assistant_tool_calls(messages, max_items=max_items)
    out: dict[str, str] = {}
    for call in assistant_calls:
        call_id = str(call.get("id") or "").strip()
        tool_name = str(call.get("name") or "").strip()
        if not call_id or not tool_name:
            continue
        out[call_id] = _serialize_tool_call_args_for_signature(call.get("args"))
    return out


def _select_identical_tool_args_failure_circuit_breaker(
    payload: dict[str, Any],
    results: list[dict[str, Any]],
) -> dict[str, Any] | None:
    if not results:
        return None

    allowed_retries = int(_IDENTICAL_TOOL_ARGS_MAX_RETRIES)
    failures_to_trip = allowed_retries + 1
    if failures_to_trip <= 0:
        return None

    args_sig_by_call_id = _build_tool_call_args_signature_by_id(payload, max_items=192)
    by_signature: dict[str, dict[str, Any]] = {}

    for idx, row in enumerate(results):
        if row.get("success") is not False:
            continue
        tool_name = str(row.get("toolName") or "").strip()
        if not tool_name or tool_name in _DISCOVERY_TOOLS:
            continue
        tool_call_id = str(row.get("toolCallId") or "").strip()
        args_sig = args_sig_by_call_id.get(tool_call_id, "")
        signature = f"{tool_name}|{args_sig}"
        current = by_signature.get(signature)
        if not isinstance(current, dict):
            current = {
                "toolName": tool_name,
                "argsSig": args_sig,
                "signature": signature,
                "count": 0,
                "lastIndex": -1,
                "lastDetails": "",
                "lastErrorKind": "generic_failure",
            }
            by_signature[signature] = current
        current["count"] = int(current.get("count") or 0) + 1
        current["lastIndex"] = idx
        current["lastDetails"] = str(row.get("details") or "")
        current["lastErrorKind"] = _classify_runtime_error_kind(row.get("details"))

    candidates = [
        item
        for item in by_signature.values()
        if int(item.get("count") or 0) >= failures_to_trip
    ]
    if not candidates:
        return None

    selected = sorted(
        candidates,
        key=lambda item: (int(item.get("lastIndex") or -1), int(item.get("count") or 0)),
    )[-1]
    return {
        **selected,
        "allowedRetries": allowed_retries,
        "failuresToTrip": failures_to_trip,
    }


def _select_identical_tool_args_success_guardrail(
    payload: dict[str, Any],
    results: list[dict[str, Any]],
) -> dict[str, Any] | None:
    if not results:
        return None

    allowed_repeats = int(_IDENTICAL_TOOL_ARGS_SUCCESS_MAX_REPEATS)
    successes_to_trip = allowed_repeats + 2
    if successes_to_trip <= 1:
        return None

    args_sig_by_call_id = _build_tool_call_args_signature_by_id(payload, max_items=192)
    by_signature: dict[str, dict[str, Any]] = {}
    latest_non_discovery_success_idx = -1

    for idx, row in enumerate(results):
        if row.get("success") is not True:
            continue
        tool_name = str(row.get("toolName") or "").strip()
        if not tool_name or tool_name in _DISCOVERY_TOOLS:
            continue
        latest_non_discovery_success_idx = idx
        tool_call_id = str(row.get("toolCallId") or "").strip()
        args_sig = args_sig_by_call_id.get(tool_call_id, "")
        signature = f"{tool_name}|{args_sig}"
        current = by_signature.get(signature)
        if not isinstance(current, dict):
            current = {
                "toolName": tool_name,
                "argsSig": args_sig,
                "signature": signature,
                "count": 0,
                "lastIndex": -1,
                "lastDetails": "",
            }
            by_signature[signature] = current
        current["count"] = int(current.get("count") or 0) + 1
        current["lastIndex"] = idx
        current["lastDetails"] = str(row.get("details") or "")

    if latest_non_discovery_success_idx < 0:
        return None

    candidates = [
        item
        for item in by_signature.values()
        if int(item.get("count") or 0) >= successes_to_trip
        and int(item.get("lastIndex") or -1) == latest_non_discovery_success_idx
    ]
    if not candidates:
        return None

    selected = sorted(
        candidates,
        key=lambda item: (int(item.get("lastIndex") or -1), int(item.get("count") or 0)),
    )[-1]
    return {
        **selected,
        "allowedRepeats": allowed_repeats,
        "successesToTrip": successes_to_trip,
    }


def _find_related_tool_call(
    result: dict[str, Any],
    assistant_calls: list[dict[str, Any]],
) -> dict[str, Any] | None:
    if not assistant_calls:
        return None
    tool_call_id = str(result.get("toolCallId") or "").strip()
    tool_name = str(result.get("toolName") or "").strip()
    if tool_call_id:
        for call in reversed(assistant_calls):
            if str(call.get("id") or "").strip() == tool_call_id:
                return call
    if tool_name:
        for call in reversed(assistant_calls):
            if str(call.get("name") or "").strip() == tool_name:
                return call
    return None


def _extract_dataset_name_from_call(call: dict[str, Any] | None) -> str:
    if not isinstance(call, dict):
        return ""
    args = call.get("args")
    if not isinstance(args, dict):
        return ""
    for key in ("datasetName", "newDatasetName", "targetDatasetName", "outputDatasetName"):
        value = args.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _extract_dataset_names_from_call(call: dict[str, Any] | None) -> list[str]:
    if not isinstance(call, dict):
        return []
    args = call.get("args")
    if not isinstance(args, dict):
        return []

    names: list[str] = []
    dataset_name = _extract_dataset_name_from_call(call)
    if dataset_name:
        names.append(dataset_name)

    dataset_names = args.get("datasetNames")
    if isinstance(dataset_names, list):
        for value in dataset_names:
            if isinstance(value, str) and value.strip():
                names.append(value.strip())

    deduped: list[str] = []
    seen: set[str] = set()
    for value in names:
        if value in seen:
            continue
        seen.add(value)
        deduped.append(value)
    return deduped


def _latest_successful_tool_index(results: list[dict[str, Any]], tool_names: set[str]) -> int:
    for idx in range(len(results) - 1, -1, -1):
        row = results[idx]
        name = str(row.get("toolName") or "").strip()
        if row.get("success") is True and name in tool_names:
            return idx
    return -1


_QCUMBER_QUERY_TOOLS: set[str] = {
    "queryQCumberTerritorialUnits",
    "queryQCumberDatasetSpatial",
    "queryQCumberDataset",
}


def _latest_qcumber_load_index(results: list[dict[str, Any]]) -> int:
    """Return the index of the latest successful q-cumber query that loaded a
    dataset to the map (``loadedToMap=true`` in the tool result)."""
    for idx in range(len(results) - 1, -1, -1):
        row = results[idx]
        name = str(row.get("toolName") or "").strip()
        if row.get("success") is True and name in _QCUMBER_QUERY_TOOLS:
            # Check whether the tool actually loaded a dataset.
            if row.get("loadedToMap") is True:
                return idx
            # Also accept when a loadedDatasetRef is present (implies load).
            if row.get("datasetRef") or row.get("datasetName"):
                return idx
    return -1


def _needs_boundary_clip_guardrail(
    *,
    objective_text: str,
    results: list[dict[str, Any]],
) -> bool:
    prompt = str(objective_text or "").strip().lower()
    if "h3" not in prompt:
        return False
    last_h3_idx = _latest_successful_tool_index(results, _H3_BOUNDARY_MATERIALIZATION_TOOLS)
    if last_h3_idx < 0:
        return False
    has_boundary_resolution = any(
        row.get("success") is True and str(row.get("toolName") or "").strip() == "queryQCumberTerritorialUnits"
        for row in results[: last_h3_idx + 1]
    )
    if not has_boundary_resolution:
        return False
    has_clip_after = any(
        row.get("success") is True and str(row.get("toolName") or "").strip() in _H3_CLIP_TOOLS
        for row in results[last_h3_idx + 1 :]
    )
    return not has_clip_after


def _objective_requires_overlay_coverage_validation(objective_text: str) -> bool:
    text = str(objective_text or "").strip().lower()
    if not text:
        return False
    perimeter_markers = (
        "perimetri",
        "perimeter",
        "boundary",
        "giurisdiz",
        "jurisdiction",
    )
    overlay_markers = (
        "intersezion",
        "intersection",
        "overlay",
        "clip",
    )
    coverage_markers = (
        "copertura",
        "coverage",
        "consisten",
        "quality",
    )
    return (
        any(marker in text for marker in perimeter_markers)
        and any(marker in text for marker in overlay_markers)
        and any(marker in text for marker in coverage_markers)
    )


def _needs_overlay_coverage_guardrail(
    *,
    objective_text: str,
    results: list[dict[str, Any]],
) -> bool:
    requires_perimeter_overlay = _objective_requires_overlay_coverage_validation(objective_text)
    requires_clip_stats = _objective_requires_clip_stats_coverage_validation(objective_text)
    if not requires_perimeter_overlay and not requires_clip_stats:
        return False
    overlay_idx = _latest_successful_tool_index(results, _OVERLAY_EXECUTION_TOOLS)
    clip_idx = _latest_successful_tool_index(results, _H3_CLIP_TOOLS)

    validation_anchor_idx = -1
    if requires_perimeter_overlay:
        if overlay_idx < 0:
            return False
        has_clip_before_overlay = any(
            row.get("success") is True and str(row.get("toolName") or "").strip() in _H3_CLIP_TOOLS
            for row in results[: overlay_idx + 1]
        )
        if not has_clip_before_overlay:
            return False
        validation_anchor_idx = overlay_idx
    elif requires_clip_stats:
        validation_anchor_idx = max(overlay_idx, clip_idx)
        if validation_anchor_idx < 0:
            return False

    has_coverage_after_overlay = any(
        row.get("success") is True and str(row.get("toolName") or "").strip() == "coverageQualityReport"
        for row in results[validation_anchor_idx + 1 :]
    )
    return not has_coverage_after_overlay


def _needs_cross_geometry_clip_guardrail(
    *,
    objective_text: str,
    results: list[dict[str, Any]],
) -> bool:
    if not _objective_requires_clip_stats_coverage_validation(objective_text):
        return False
    clip_idx = _latest_successful_tool_index(results, _H3_CLIP_TOOLS)
    return clip_idx < 0


def _is_low_distinct_color_failure(details: Any) -> bool:
    text = str(details or "").lower()
    if not text:
        return False
    return (
        "<=1 distinct" in text
        or "distinct=1" in text
        or "would appear uniform" in text
        or "color scale would appear uniform" in text
    )


_METRIC_FIELD_NOT_FOUND_RE = re.compile(
    r'(?:metric\s+)?field\s+["\']?([^"\']+)["\']?\s+not\s+found(?:\s+in\s+dataset)?',
    re.IGNORECASE,
)


def _is_metric_field_not_found_failure(details: Any) -> bool:
    text = str(details or "").strip()
    if not text:
        return False
    return _METRIC_FIELD_NOT_FOUND_RE.search(text) is not None


def _is_admin_level_validation_failure(details: Any) -> bool:
    text = " ".join(str(details or "").strip().lower().split())
    if not text:
        return False
    return (
        "administrative level mismatch" in text
        or (
            "expected administrative type" in text
            and any(
                marker in text
                for marker in (
                    "not found",
                    "available sampled levels",
                    "no level field was found",
                )
            )
        )
    )


def _is_turn_state_discovery_failure(details: Any) -> bool:
    text = " ".join(str(details or "").strip().lower().split())
    if not text:
        return False
    return "hard-enforce turn state:" in text and "discovery step is mandatory" in text


def _extract_missing_metric_field(details: Any) -> str:
    text = str(details or "").strip()
    if not text:
        return ""
    match = _METRIC_FIELD_NOT_FOUND_RE.search(text)
    if not match:
        return ""
    return str(match.group(1) or "").strip()


def _objective_targets_problem_metric(objective_text: str) -> bool:
    text = str(objective_text or "").strip().lower()
    if not text:
        return False
    markers = (
        "piu problemi",
        "più problemi",
        "problemi ambientali",
        "pressione ambientale",
        "inquin",
        "contamin",
        "rischio",
        "most problems",
        "environmental pressure",
        "pollution",
        "contamination",
        "critical areas",
    )
    return any(marker in text for marker in markers)


def _objective_explicit_population_metric(objective_text: str) -> bool:
    text = str(objective_text or "").strip().lower()
    if not text:
        return False
    markers = ("population", "popolazione", "abitanti", "residenti", "per capita", "pro capite")
    return any(marker in text for marker in markers)


def _objective_requests_load_save_bridge(objective_text: str) -> bool:
    text = str(objective_text or "").strip().lower()
    if not text:
        return False
    if "bridge" in text:
        return True

    load_markers = ("load", "caric", "import")
    save_markers = ("save", "salv", "export", "esport")
    has_load_intent = any(marker in text for marker in load_markers)
    has_save_intent = any(marker in text for marker in save_markers)
    return has_load_intent and has_save_intent


def _should_prune_qcumber_discovery_for_bridge(
    *,
    objective_text: str,
    request_tool_names: set[str],
    results: list[dict[str, Any]],
) -> bool:
    if not request_tool_names.intersection(_BRIDGE_OPERATION_TOOLS):
        return False
    if not request_tool_names.intersection(_QCUMBER_DISCOVERY_TOOLS):
        return False
    if _objective_requests_dataset_discovery(objective_text):
        return False
    if not _objective_requests_load_save_bridge(objective_text):
        return False
    # Keep discovery tools available when a bridge step already failed and
    # recovery/disambiguation may be required.
    if _latest_failed_tool_index(results, _BRIDGE_OPERATION_TOOLS) >= 0:
        return False
    return True


def _should_prune_qcumber_provider_listing_without_discovery(
    *,
    objective_text: str,
    request_tool_names: set[str],
    results: list[dict[str, Any]],
) -> bool:
    if "listQCumberProviders" not in request_tool_names:
        return False
    if _objective_requests_dataset_discovery(objective_text):
        return False
    if _objective_requests_provider_discovery(objective_text):
        return False
    if _latest_failed_tool_index(results, _BRIDGE_OPERATION_TOOLS) >= 0:
        return False
    if _latest_failed_tool_index(results, _QCUMBER_DISCOVERY_TOOLS) >= 0:
        return False
    if not request_tool_names.intersection(
        {
            "listQCumberDatasets",
            "queryQCumberDataset",
            "queryQCumberDatasetSpatial",
            "queryQCumberTerritorialUnits",
        }
    ):
        return False
    return True


def _should_prune_fit_without_map_focus(
    *,
    objective_text: str,
    request_tool_names: set[str],
    results: list[dict[str, Any]],
) -> bool:
    if "fitQMapToDataset" not in request_tool_names:
        return False
    if _objective_requests_map_centering(objective_text) or _objective_requests_map_display(objective_text):
        return False
    workflow_state = build_runtime_workflow_state(
        results=results,
        objective_text=objective_text,
        objective_targets_admin_units=_objective_targets_admin_units,
        objective_requests_map_display=_objective_requests_map_display,
        objective_requires_ranked_output=_objective_requires_ranked_output,
    )
    if workflow_state.preserve_fit_without_explicit_map_focus:
        return False
    if _latest_failed_tool_index(results, {"fitQMapToDataset"}) >= 0:
        return False
    return True


def _objective_targets_forest_metric(objective_text: str) -> bool:
    text = str(objective_text or "").strip().lower()
    if not text:
        return False
    markers = (
        "boschi",
        "bosco",
        "forest",
        "foresta",
        "foreste",
        "woodland",
        "tree cover",
    )
    return any(marker in text for marker in markers)


def _objective_targets_admin_units(objective_text: str) -> bool:
    text = str(objective_text or "").strip().lower()
    if not text:
        return False
    markers = (
        "provincia",
        "province",
        "provincial",
        "regione",
        "regione",
        "region",
        "regionali",
        "comune",
        "comuni",
        "municip",
        "unita amministrativa",
        "unità amministrativa",
        "unita amministrative",
        "unità amministrative",
        "administrative unit",
        "administrative units",
    )
    return any(marker in text for marker in markers)


def _normalize_dataset_class_marker(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(value or "").strip().lower()).strip("_")


def _metadata_territorial_prune_decision(results: list[dict[str, Any]]) -> bool | None:
    if not results:
        return None

    for row in reversed(results):
        if not isinstance(row, dict):
            continue
        routing_is_administrative = row.get("routingIsAdministrative")
        if isinstance(routing_is_administrative, bool):
            return not routing_is_administrative
        routing_preferred_tool = str(row.get("routingPreferredTool") or "").strip()
        if routing_preferred_tool == "queryQCumberTerritorialUnits":
            return False
        if routing_preferred_tool in {"queryQCumberDatasetSpatial", "queryQCumberDataset"}:
            return True
        dataset_class = _normalize_dataset_class_marker(row.get("datasetClass"))
        if not dataset_class:
            continue
        if dataset_class == "administrative":
            return False
        if dataset_class in {"thematic_spatial", "land_cover", "events", "features"}:
            return True
    return None


def _should_prune_territorial_query_for_thematic_objective(
    *,
    objective_text: str,
    request_tool_names: set[str],
    results: list[dict[str, Any]],
) -> bool:
    if "queryQCumberTerritorialUnits" not in request_tool_names:
        return False
    if not {"queryQCumberDatasetSpatial", "queryQCumberDataset"}.intersection(request_tool_names):
        return False
    _ = objective_text
    metadata_decision = _metadata_territorial_prune_decision(results)
    return bool(metadata_decision)


def _objective_explicit_category_distribution(objective_text: str) -> bool:
    text = str(objective_text or "").strip().lower()
    if not text:
        return False
    markers = (
        "categorie",
        "category",
        "distribuzione",
        "frequenza",
        "frequency",
        "conta per nome",
    )
    return any(marker in text for marker in markers)


def _is_preview_head_sample_details(details: Any) -> bool:
    text = str(details or "").strip().lower()
    if not text:
        return False
    if not text.startswith("previewed"):
        return False
    return "ordered by" not in text


def _is_zonal_ui_freeze_failure(details: Any) -> bool:
    text = " ".join(str(details or "").strip().lower().split())
    if not text:
        return False
    return "zonalstatsbyadmin aborted to prevent ui freeze" in text or (
        "zonalstatsbyadmin aborted" in text and "prevent ui freeze" in text
    )


def _latest_failed_tool_index(results: list[dict[str, Any]], tool_names: set[str]) -> int:
    for idx in range(len(results) - 1, -1, -1):
        row = results[idx]
        name = str(row.get("toolName") or "").strip()
        if row.get("success") is False and name in tool_names:
            return idx
    return -1


def _has_successful_tool_after_index(results: list[dict[str, Any]], start_idx: int, tool_names: set[str]) -> bool:
    if start_idx < 0:
        return False
    for row in results[start_idx + 1 :]:
        name = str(row.get("toolName") or "").strip()
        if row.get("success") is True and name in tool_names:
            return True
    return False


def _latest_zonal_ui_freeze_failure_index(results: list[dict[str, Any]]) -> int:
    for idx in range(len(results) - 1, -1, -1):
        row = results[idx]
        if str(row.get("toolName") or "").strip() != "zonalStatsByAdmin":
            continue
        if row.get("success") is not False:
            continue
        if _is_zonal_ui_freeze_failure(row.get("details")):
            return idx
    return -1


def _has_unresolved_zonal_ui_freeze_failure(results: list[dict[str, Any]]) -> bool:
    freeze_idx = _latest_zonal_ui_freeze_failure_index(results)
    if freeze_idx < 0:
        return False
    if _has_successful_tool_after_index(results, freeze_idx, {"zonalStatsByAdmin", "rankQMapDatasetRows"}):
        return False
    return True


def _normalize_clc_code_token(value: Any) -> str:
    if value is None or isinstance(value, bool):
        return ""
    token = str(value).strip()
    if not token:
        return ""
    if re.fullmatch(r"0*[0-9]+", token):
        token = token.lstrip("0") or "0"
    return token


def _filters_include_forest_clc_codes(filters: Any) -> bool:
    if not isinstance(filters, list):
        return False
    required = {"311", "312", "313"}
    for row in filters:
        if not isinstance(row, dict):
            continue
        field_name = str(row.get("field") or "").strip().lower()
        if field_name != "code_18":
            continue
        op = str(row.get("op") or "").strip().lower()
        raw_values: list[Any] = []
        if op == "in":
            values = row.get("values")
            if isinstance(values, list):
                raw_values = values
            else:
                value_alias = row.get("value")
                if isinstance(value_alias, list):
                    raw_values = value_alias
                elif value_alias is not None:
                    raw_values = [value_alias]
        else:
            value = row.get("value")
            if value is not None:
                raw_values = [value]
        normalized = {
            token
            for token in (_normalize_clc_code_token(item) for item in raw_values)
            if token
        }
        if required.issubset(normalized):
            return True
    return False


def _has_recent_forest_clc_query_call(assistant_calls: list[dict[str, Any]], *, max_items: int = 48) -> bool:
    if not assistant_calls:
        return False
    inspected = 0
    for call in reversed(assistant_calls):
        if inspected >= max_items:
            break
        inspected += 1
        name = str(call.get("name") or "").strip()
        if name not in {"queryQCumberDatasetSpatial", "queryQCumberDataset"}:
            continue
        args = call.get("args")
        if not isinstance(args, dict):
            continue
        if _filters_include_forest_clc_codes(args.get("filters")):
            return True
    return False


_H3_RESOLUTION_RE = re.compile(r"(?:^|[_\s-])r(?P<res>\d{1,2})(?:$|[_\s-])", re.IGNORECASE)
_ZONAL_VALUES_COUNT_RE = re.compile(r"values=(?P<count>[0-9][0-9\.,]*)", re.IGNORECASE)


def _infer_h3_resolution_from_text(text: Any) -> int | None:
    raw = str(text or "").strip()
    if not raw:
        return None
    match = _H3_RESOLUTION_RE.search(raw)
    if not match:
        return None
    try:
        parsed = int(str(match.group("res") or "").strip())
    except Exception:
        return None
    if not (0 <= parsed <= 20):
        return None
    return parsed


def _extract_zonal_values_count(details: Any) -> int | None:
    text = str(details or "")
    if not text:
        return None
    match = _ZONAL_VALUES_COUNT_RE.search(text)
    if not match:
        return None
    token = str(match.group("count") or "").strip()
    if not token:
        return None
    try:
        return int(token.replace(".", "").replace(",", ""))
    except Exception:
        return None


def _replace_or_append_h3_resolution_suffix(name: str, next_resolution: int) -> str:
    raw = str(name or "").strip()
    if not raw:
        return f"h3_agg_r{next_resolution}"

    match = _H3_RESOLUTION_RE.search(raw)
    if match:
        start, end = match.span("res")
        return raw[:start] + str(next_resolution) + raw[end:]
    return f"{raw}_r{next_resolution}"


def _has_low_distinct_color_failure(results: list[dict[str, Any]]) -> bool:
    for row in results:
        if str(row.get("toolName") or "").strip() != "setQMapLayerColorByField":
            continue
        if row.get("success") is not False:
            continue
        if _is_low_distinct_color_failure(row.get("details")):
            return True
    return False


def _is_uninformative_category_name_chart_call(call: dict[str, Any] | None) -> bool:
    if not isinstance(call, dict):
        return False
    if str(call.get("name") or "").strip() != "categoryBarsTool":
        return False
    args = call.get("args")
    if not isinstance(args, dict):
        return False
    category_field = str(args.get("categoryFieldName") or "").strip().lower()
    if category_field != "name":
        return False
    value_field = str(args.get("valueFieldName") or "").strip()
    return not bool(value_field)


def _is_uninformative_category_name_chart_result(details: Any) -> bool:
    text = str(details or "").strip().lower()
    if not text:
        return False
    return "categories from \"name\"" in text or "categories from 'name'" in text


def _extract_metric_output_field_from_call(call: dict[str, Any] | None) -> str:
    if not isinstance(call, dict):
        return ""
    default_style_field = str(call.get("defaultStyleField") or "").strip()
    if default_style_field:
        return default_style_field
    aggregation_outputs = call.get("aggregationOutputs")
    if isinstance(aggregation_outputs, dict):
        for key in ("sum", "count", "avg", "max", "min"):
            candidate = aggregation_outputs.get(key)
            if isinstance(candidate, str) and candidate.strip():
                return candidate.strip()
    tool_name = str(call.get("name") or "").strip()
    args = call.get("args")
    if not isinstance(args, dict):
        return ""
    for key in ("outputFieldName", "targetValueFieldName", "outputAreaField", "metricFieldName"):
        value = args.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    if tool_name == "spatialJoinByPredicate":
        aggregations = args.get("aggregations")
        normalized_aggs = {
            str(item).strip().lower()
            for item in aggregations
            if isinstance(item, (str, int, float))
        } if isinstance(aggregations, list) else set()
        right_value_field = str(args.get("rightValueField") or "").strip()
        if "sum" in normalized_aggs and right_value_field:
            return "join_sum"
        if "count" in normalized_aggs:
            return "join_count"
    return ""


def _objective_prefers_visibility_isolation(objective_text: str) -> bool:
    text = str(objective_text or "").strip().lower()
    if not text:
        return False
    markers = (
        "mostra solo",
        "solo layer",
        "solo celle",
        "isola",
        "show only",
        "only layer",
        "only cells",
        "hide intermedi",
        "nascondi intermedi",
    )
    return any(marker in text for marker in markers)


def _next_successful_tool_index(results: list[dict[str, Any]], start_idx: int, tool_name: str) -> int:
    if start_idx < 0:
        start_idx = 0
    for idx in range(start_idx, len(results)):
        row = results[idx]
        if row.get("success") is True and str(row.get("toolName") or "").strip() == tool_name:
            return idx
    return -1


def _resolve_dataset_hint_from_result(
    result: dict[str, Any] | None,
    assistant_calls: list[dict[str, Any]],
) -> tuple[str, str, str]:
    if not isinstance(result, dict):
        return "", "", ""
    related_call = _find_related_tool_call(result, assistant_calls)
    dataset_name = _extract_dataset_name_from_call(related_call) or str(result.get("datasetName") or "")
    dataset_ref = _extract_dataset_ref_from_call(related_call) or str(result.get("datasetRef") or "")
    return dataset_name, dataset_ref, _build_dataset_hint(dataset_ref, dataset_name)


def _latest_successful_tool_for_dataset(
    results: list[dict[str, Any]],
    assistant_calls: list[dict[str, Any]],
    *,
    tool_names: set[str],
    until_idx: int,
    preferred_dataset_name: str = "",
) -> tuple[int, str]:
    if until_idx <= 0:
        return -1, ""
    preferred = str(preferred_dataset_name or "").strip()
    for idx in range(until_idx - 1, -1, -1):
        row = results[idx]
        if row.get("success") is not True:
            continue
        tool_name = str(row.get("toolName") or "").strip()
        if tool_name not in tool_names:
            continue
        related_call = _find_related_tool_call(row, assistant_calls)
        dataset_name = _extract_dataset_name_from_call(related_call) or str(row.get("datasetName") or "").strip()
        if not dataset_name:
            continue
        if preferred and dataset_name != preferred:
            continue
        return idx, dataset_name
    return -1, ""


def _has_successful_save_data_to_map_for_dataset(
    results: list[dict[str, Any]],
    assistant_calls: list[dict[str, Any]],
    *,
    dataset_name: str,
    start_idx: int,
) -> bool:
    target = str(dataset_name or "").strip()
    if not target:
        return False
    for idx in range(max(0, int(start_idx)), len(results)):
        row = results[idx]
        if row.get("success") is not True:
            continue
        if str(row.get("toolName") or "").strip() != "saveDataToMap":
            continue
        related_call = _find_related_tool_call(row, assistant_calls)
        dataset_names = _extract_dataset_names_from_call(related_call)
        if target in dataset_names:
            return True
        result_dataset = str(row.get("datasetName") or "").strip()
        if result_dataset == target:
            return True
    return False


def _derive_runtime_quality_metrics(
    request_tool_results: Any,
    response_tool_calls: Any,
    response_text: Any,
    request_payload: Any = None,
) -> dict[str, Any]:
    rows = request_tool_results if isinstance(request_tool_results, list) else []
    results = [row for row in rows if isinstance(row, dict)]
    response_calls = _collect_response_tool_call_names(response_tool_calls)

    latest_create_idx = _latest_successful_tool_index(results, _DATASET_CREATE_OR_UPDATE_TOOLS)
    # q-cumber query tools with loadedToMap=true also create datasets in the map.
    if latest_create_idx < 0:
        latest_create_idx = _latest_qcumber_load_index(results)
    has_dataset_mutation = latest_create_idx >= 0
    wait_idx = _next_successful_tool_index(results, latest_create_idx + 1, "waitForQMapDataset")
    count_idx = _next_successful_tool_index(results, wait_idx + 1, "countQMapRows") if wait_idx >= 0 else -1
    rank_idx = _next_successful_tool_index(results, count_idx + 1, "rankQMapDatasetRows") if count_idx >= 0 else -1
    isolation_idx = (
        _next_successful_tool_index(results, count_idx + 1, "showOnlyQMapLayer")
        if count_idx >= 0
        else -1
    )

    wait_timeouts = 0
    for row in results:
        if str(row.get("toolName") or "").strip() != "waitForQMapDataset":
            continue
        if row.get("success") is True:
            continue
        details = str(row.get("details") or "").lower()
        if "timeout waiting for dataset" in details:
            wait_timeouts += 1

    response_text_str = str(response_text or "").strip()
    failed_count = len([row for row in results if row.get("success") is False])
    any_success = any(row.get("success") is True for row in results)
    contract_schema_mismatch_count = len(
        [row for row in results if bool(row.get("contractSchemaMismatch"))]
    )
    contract_response_mismatch_count = len(
        [row for row in results if bool(row.get("contractResponseMismatch"))]
    )
    fit_success_idx = _latest_successful_tool_index(results, {"fitQMapToDataset"})
    fit_failed_idx = _latest_failed_tool_index(results, {"fitQMapToDataset"})
    latest_operational_success_idx = _latest_successful_tool_index(results, _AUDIT_SUCCESS_EVIDENCE_TOOLS)
    cloud_load_tools = {"loadCloudMapAndWait", "loadQMapCloudMap"}
    cloud_recovery_tools = cloud_load_tools.union({"loadData", "saveDataToMap"})
    latest_cloud_failure_idx = -1
    latest_cloud_failure_exhausted = False
    for idx in range(len(results) - 1, -1, -1):
        row = results[idx]
        tool_name = str(row.get("toolName") or "").strip()
        if tool_name not in cloud_load_tools or row.get("success") is not False:
            continue
        latest_cloud_failure_idx = idx
        latest_cloud_failure_exhausted = (
            _classify_runtime_error_kind(row.get("details")) == "cloud_no_validated_fallback"
        )
        break
    cloud_failure_seen = latest_cloud_failure_idx >= 0
    cloud_recovery_validated = False
    if cloud_failure_seen:
        recovery_load_seen = False
        for row in results[latest_cloud_failure_idx + 1 :]:
            tool_name = str(row.get("toolName") or "").strip()
            if row.get("success") is True and tool_name in cloud_recovery_tools:
                recovery_load_seen = True
                continue
            if row.get("success") is True and tool_name == "waitForQMapDataset" and recovery_load_seen:
                cloud_recovery_validated = True
                break
    response_mode_hint = _extract_runtime_response_mode_hint_from_payload(request_payload)
    clarification_pending = False
    clarification_reason = ""
    latest_clarification_required_idx = -1
    latest_clarification_required_question = ""
    latest_clarification_required_options: list[str] = []
    for idx in range(len(results) - 1, -1, -1):
        row = results[idx]
        if row.get("success") is True:
            continue
        error_kind = _classify_runtime_error_kind(row.get("details"))
        if row.get("clarificationRequired") is True or error_kind == "ambiguous_admin_match":
            latest_clarification_required_idx = idx
            clarification_reason = error_kind or "clarification_required"
            latest_clarification_required_question = str(row.get("clarificationQuestion") or "").strip()
            raw_options = row.get("clarificationOptions")
            if isinstance(raw_options, list):
                latest_clarification_required_options = [
                    str(value or "").strip() for value in raw_options if str(value or "").strip()
                ]
            break
    if latest_clarification_required_idx >= 0:
        clarification_pending = not any(
            row.get("success") is True
            and str(row.get("toolName") or "").strip()
            and str(row.get("toolName") or "").strip() not in _DISCOVERY_TOOLS
            for row in results[latest_clarification_required_idx + 1 :]
        )
        if clarification_pending and not response_mode_hint:
            response_mode_hint = "clarification"
    if not response_mode_hint:
        latest_selection_discovery_idx = _latest_successful_tool_index(
            results, {"listQCumberProviders", "listQCumberDatasets"}
        )
        if latest_selection_discovery_idx >= 0:
            has_successful_non_discovery_after_selection = any(
                row.get("success") is True
                and str(row.get("toolName") or "").strip()
                and str(row.get("toolName") or "").strip() not in _DISCOVERY_TOOLS
                for row in results[latest_selection_discovery_idx + 1 :]
            )
            if not has_successful_non_discovery_after_selection:
                response_mode_hint = "clarification"
    false_success_claim_rules: set[str] = set()
    latest_failed_error_kind = ""
    for row in reversed(results):
        if row.get("success") is not False:
            continue
        latest_failed_error_kind = _classify_runtime_error_kind(row.get("details"))
        if latest_failed_error_kind:
            break
    structured_recovery = _runtime_error_retry_policy(latest_failed_error_kind) if latest_failed_error_kind else {}
    recovery_action = str(structured_recovery.get("recoveryAction") or "").strip()
    raw_next_allowed_tools = structured_recovery.get("nextAllowedTools")
    next_allowed_tools = [
        str(value or "").strip()
        for value in (raw_next_allowed_tools if isinstance(raw_next_allowed_tools, list) else [])
        if str(value or "").strip()
    ]

    if _response_claims_centering_success(response_text_str):
        fit_recovered = fit_failed_idx >= 0 and fit_success_idx > fit_failed_idx
        if fit_success_idx < 0 or not fit_recovered and fit_failed_idx >= 0:
            false_success_claim_rules.add("centering_without_fit_success")

    if _response_claims_operational_success(response_text_str):
        if failed_count > 0 and latest_operational_success_idx < 0:
            false_success_claim_rules.add("operational_success_without_evidence")

    if _response_claims_success(response_text_str):
        if failed_count > 0 and not any_success:
            false_success_claim_rules.add("success_claim_with_all_tools_failed")

    quality: dict[str, Any] = {
        "responseToolCallCount": len(response_calls),
        "responseHasText": bool(response_text_str),
        "hasDatasetMutation": has_dataset_mutation,
        "postCreateWaitOk": bool(has_dataset_mutation and wait_idx >= 0),
        "postCreateWaitCountOk": bool(has_dataset_mutation and wait_idx >= 0 and count_idx >= 0),
        "postCreateWaitCountRankOk": bool(has_dataset_mutation and wait_idx >= 0 and count_idx >= 0 and rank_idx >= 0),
        "finalLayerIsolatedAfterCount": bool(has_dataset_mutation and count_idx >= 0 and isolation_idx >= 0),
        "pendingIsolationAfterCount": bool(has_dataset_mutation and count_idx >= 0 and isolation_idx < 0),
        "cloudFailureSeen": cloud_failure_seen,
        "cloudFailureExhausted": bool(cloud_failure_seen and latest_cloud_failure_exhausted),
        "cloudRecoveryValidated": cloud_recovery_validated,
        "clarificationPending": clarification_pending,
        "clarificationReason": clarification_reason or None,
        "clarificationQuestionSeen": bool(latest_clarification_required_question),
        "clarificationOptionsCount": len(latest_clarification_required_options),
        "responseModeHint": response_mode_hint,
        "hintVersion": "qmap.runtime.hints.v1",
        "errorKind": latest_failed_error_kind or None,
        "recoveryAction": recovery_action or None,
        "nextAllowedTools": next_allowed_tools,
        "waitTimeoutCount": wait_timeouts,
        "contractSchemaMismatchCount": contract_schema_mismatch_count,
        "contractResponseMismatchCount": contract_response_mismatch_count,
        "falseSuccessClaimCount": len(false_success_claim_rules),
        "falseSuccessClaimRules": sorted(false_success_claim_rules),
    }
    score = 100
    if has_dataset_mutation and wait_idx < 0:
        score -= 35
    if has_dataset_mutation and wait_idx >= 0 and count_idx < 0:
        score -= 20
    if has_dataset_mutation and count_idx >= 0 and isolation_idx < 0:
        score -= 15
    score -= min(wait_timeouts * 10, 30)
    if cloud_failure_seen and not cloud_recovery_validated and not latest_cloud_failure_exhausted:
        score -= 10
    score -= min(contract_schema_mismatch_count * 10, 30)
    score -= min(contract_response_mismatch_count * 5, 20)
    score -= min(len(false_success_claim_rules) * 20, 40)
    quality["workflowScore"] = max(0, min(100, score))
    return quality


def _extract_runtime_response_mode_hint_from_payload(payload: Any) -> str:
    if not isinstance(payload, dict):
        return ""
    messages = payload.get("messages")
    if not isinstance(messages, list):
        return ""
    for raw_msg in reversed(messages):
        if not isinstance(raw_msg, dict):
            continue
        if str(raw_msg.get("role") or "").strip().lower() != "system":
            continue
        content = str(raw_msg.get("content") or "")
        for line in reversed(content.splitlines()):
            normalized = " ".join(str(line or "").split())
            if not normalized.startswith(_RUNTIME_RESPONSE_MODE_PREFIX):
                continue
            hint = normalized[len(_RUNTIME_RESPONSE_MODE_PREFIX) :].strip().lower()
            if hint in {"clarification", "limitation"}:
                return hint
    return ""


def _recent_successful_discovery_tail(results: list[dict[str, Any]], *, max_items: int = 12) -> list[str]:
    tail: list[str] = []
    for row in reversed(results or []):
        if row.get("success") is not True:
            continue
        name = str(row.get("toolName") or "").strip()
        if not name:
            continue
        if name in _DISCOVERY_TOOLS:
            tail.append(name)
            if len(tail) >= max_items:
                break
            continue
        # Stop as soon as a successful non-discovery step appears in the recent trail.
        break
    return list(reversed(tail))


def _has_repeated_discovery_loop(results: list[dict[str, Any]]) -> bool:
    tail = _recent_successful_discovery_tail(results, max_items=12)
    if len(tail) < 6:
        return False

    qcumber_pair = {"listQCumberProviders", "listQCumberDatasets"}
    if set(tail).issubset(qcumber_pair):
        providers_count = tail.count("listQCumberProviders")
        datasets_count = tail.count("listQCumberDatasets")
        alternating = all(tail[idx] != tail[idx - 1] for idx in range(1, len(tail)))
        if providers_count >= 3 and datasets_count >= 3:
            return True
        if alternating and len(tail) >= 6:
            return True

    # Generic fallback: same discovery call repeated many times without progress.
    if len(set(tail)) == 1 and len(tail) >= 5:
        return True
    return False


def _runtime_guardrail_injection_bindings() -> dict[str, Any]:
    return {
        "_DATASET_CREATE_OR_UPDATE_TOOLS": _DATASET_CREATE_OR_UPDATE_TOOLS,
        "_H3_BOUNDARY_MATERIALIZATION_TOOLS": _H3_BOUNDARY_MATERIALIZATION_TOOLS,
        "_H3_CLIP_TOOLS": _H3_CLIP_TOOLS,
        "_OVERLAY_EXECUTION_TOOLS": _OVERLAY_EXECUTION_TOOLS,
        "_REMOTE_CACHE_DATASET_TOOLS": _REMOTE_CACHE_DATASET_TOOLS,
        "_RUNTIME_GUARDRAIL_PREFIX": _RUNTIME_GUARDRAIL_PREFIX,
        "_RUNTIME_NEXT_STEP_PREFIX": _RUNTIME_NEXT_STEP_PREFIX,
        "_STYLE_EXECUTION_TOOLS": _STYLE_EXECUTION_TOOLS,
        "_VISIBILITY_ISOLATION_TOOLS": _VISIBILITY_ISOLATION_TOOLS,
        "_build_dataset_hint": _build_dataset_hint,
        "_build_source_dataset_hint": _build_source_dataset_hint,
        "_classify_runtime_error_kind": _classify_runtime_error_kind,
        "_extract_assistant_tool_calls": _extract_assistant_tool_calls,
        "_extract_dataset_name_from_call": _extract_dataset_name_from_call,
        "_extract_dataset_ref_from_call": _extract_dataset_ref_from_call,
        "_extract_metric_output_field_from_call": _extract_metric_output_field_from_call,
        "_extract_missing_metric_field": _extract_missing_metric_field,
        "_extract_prompt_from_messages": _extract_prompt_from_messages,
        "_extract_request_tool_names": _extract_request_tool_names,
        "_extract_request_tool_results": _extract_request_tool_results,
        "_extract_zonal_values_count": _extract_zonal_values_count,
        "_find_related_tool_call": _find_related_tool_call,
        "_has_low_distinct_color_failure": _has_low_distinct_color_failure,
        "_has_recent_forest_clc_query_call": _has_recent_forest_clc_query_call,
        "_has_repeated_discovery_loop": _has_repeated_discovery_loop,
        "_has_successful_save_data_to_map_for_dataset": _has_successful_save_data_to_map_for_dataset,
        "_has_successful_tool_after_index": _has_successful_tool_after_index,
        "_has_unresolved_zonal_ui_freeze_failure": _has_unresolved_zonal_ui_freeze_failure,
        "_infer_h3_resolution_from_text": _infer_h3_resolution_from_text,
        "_is_low_distinct_color_failure": _is_low_distinct_color_failure,
        "_is_metric_field_not_found_failure": _is_metric_field_not_found_failure,
        "_is_preview_head_sample_details": _is_preview_head_sample_details,
        "_is_turn_state_discovery_failure": _is_turn_state_discovery_failure,
        "_is_uninformative_category_name_chart_call": _is_uninformative_category_name_chart_call,
        "_is_uninformative_category_name_chart_result": _is_uninformative_category_name_chart_result,
        "_is_zonal_ui_freeze_failure": _is_zonal_ui_freeze_failure,
        "_latest_failed_tool_index": _latest_failed_tool_index,
        "_latest_successful_tool_for_dataset": _latest_successful_tool_for_dataset,
        "_latest_successful_tool_index": _latest_successful_tool_index,
        "_needs_boundary_clip_guardrail": _needs_boundary_clip_guardrail,
        "_needs_cross_geometry_clip_guardrail": _needs_cross_geometry_clip_guardrail,
        "_needs_overlay_coverage_guardrail": _needs_overlay_coverage_guardrail,
        "_next_successful_tool_index": _next_successful_tool_index,
        "_objective_explicit_population_metric": _objective_explicit_population_metric,
        "_objective_prefers_visibility_isolation": _objective_prefers_visibility_isolation,
        "_objective_requests_cloud_load_sequence": _objective_requests_cloud_load_sequence,
        "_objective_requests_dataset_discovery": _objective_requests_dataset_discovery,
        "_objective_requires_ranked_output": _objective_requires_ranked_output,
        "_QCUMBER_PROVIDER_SCOPED_TOOLS": _QCUMBER_PROVIDER_SCOPED_TOOLS,
        "_objective_targets_admin_units": _objective_targets_admin_units,
        "_objective_targets_forest_metric": _objective_targets_forest_metric,
        "_objective_targets_problem_metric": _objective_targets_problem_metric,
        "_replace_or_append_h3_resolution_suffix": _replace_or_append_h3_resolution_suffix,
        "_resolve_dataset_hint_from_result": _resolve_dataset_hint_from_result,
        "_select_identical_tool_args_success_guardrail": _select_identical_tool_args_success_guardrail,
    }


def _runtime_tool_loop_limit_bindings() -> RuntimeToolLoopLimitBindings:
    return RuntimeToolLoopLimitBindings(
        _extract_recent_tool_results_since_last_user=_extract_recent_tool_results_since_last_user,
        _extract_prompt_from_messages=_extract_prompt_from_messages,
        _extract_request_tool_names=_extract_request_tool_names,
        _should_prune_qcumber_discovery_for_bridge=_should_prune_qcumber_discovery_for_bridge,
        _should_prune_territorial_query_for_thematic_objective=_should_prune_territorial_query_for_thematic_objective,
        _should_prune_qcumber_provider_listing_without_discovery=_should_prune_qcumber_provider_listing_without_discovery,
        _should_prune_fit_without_map_focus=_should_prune_fit_without_map_focus,
        _has_assistant_text_since_last_user=_has_assistant_text_since_last_user,
        _runtime_failure_error_class=_runtime_failure_error_class,
        _select_identical_tool_args_failure_circuit_breaker=_select_identical_tool_args_failure_circuit_breaker,
        _select_identical_tool_args_success_guardrail=_select_identical_tool_args_success_guardrail,
        _compact_signature_for_trace=_compact_signature_for_trace,
        _is_turn_state_discovery_failure=_is_turn_state_discovery_failure,
        _objective_requests_dataset_discovery=_objective_requests_dataset_discovery,
        _objective_requests_cloud_load_sequence=_objective_requests_cloud_load_sequence,
        _objective_mentions_cloud_or_saved_maps=_objective_mentions_cloud_or_saved_maps,
        _objective_requires_ranked_output=_objective_requires_ranked_output,
        _objective_targets_admin_units=_objective_targets_admin_units,
        _objective_requests_linear_regression=_objective_requests_linear_regression,
        _objective_requests_field_correlation=_objective_requests_field_correlation,
        _objective_requests_natural_break_classification=_objective_requests_natural_break_classification,
        _objective_requests_regulatory_compliance=_objective_requests_regulatory_compliance,
        _objective_requests_regulatory_listing=_objective_requests_regulatory_listing,
        _objective_requests_exposure_assessment=_objective_requests_exposure_assessment,
        _objective_requests_spatial_interpolation=_objective_requests_spatial_interpolation,
        _latest_successful_tool_index=_latest_successful_tool_index,
        _is_low_distinct_color_failure=_is_low_distinct_color_failure,
        _is_metric_field_not_found_failure=_is_metric_field_not_found_failure,
        _classify_runtime_error_kind=_classify_runtime_error_kind,
        _runtime_error_retry_policy=_runtime_error_retry_policy,
        _DATASET_CREATE_OR_UPDATE_TOOLS=_DATASET_CREATE_OR_UPDATE_TOOLS,
        _STYLE_EXECUTION_TOOLS=_STYLE_EXECUTION_TOOLS,
        _VISIBILITY_ISOLATION_TOOLS=_VISIBILITY_ISOLATION_TOOLS,
        _POST_CREATE_VALIDATION_DEFERRED_TOOLS=_POST_CREATE_VALIDATION_DEFERRED_TOOLS,
        _RUNTIME_GUARDRAIL_PREFIX=_RUNTIME_GUARDRAIL_PREFIX,
        _RUNTIME_NEXT_STEP_PREFIX=_RUNTIME_NEXT_STEP_PREFIX,
        _QCUMBER_DISCOVERY_TOOLS=_QCUMBER_DISCOVERY_TOOLS,
        _QCUMBER_PROVIDER_SCOPED_TOOLS=_QCUMBER_PROVIDER_SCOPED_TOOLS,
        _DISCOVERY_TOOLS=_DISCOVERY_TOOLS,
        _TOOL_CALL_WORKFLOW_HARD_CAP=_TOOL_CALL_WORKFLOW_HARD_CAP,
        _TOOL_ONLY_NO_TEXT_WATCHDOG_MIN_CALLS=_TOOL_ONLY_NO_TEXT_WATCHDOG_MIN_CALLS,
        _ERROR_CLASS_MAX_RETRIES=_ERROR_CLASS_MAX_RETRIES,
    )


_configure_audit_runtime(
    parse_tool_arguments=_parse_tool_arguments,
    derive_runtime_quality_metrics=_derive_runtime_quality_metrics,
    sanitize_qmap_context_payload=_sanitize_qmap_context_payload,
)
