from __future__ import annotations

import json
import re
from typing import Any


_FILTER_OP_VALUES: set[str] = {
    "eq",
    "ne",
    "gt",
    "gte",
    "lt",
    "lte",
    "in",
    "contains",
    "startswith",
    "endswith",
    "is_null",
    "not_null",
    "neq",
    "starts_with",
    "ends_with",
}
_FILTER_OP_INLINE_VALUE_RE = re.compile(
    r"^\s*(?P<op>[A-Za-z_]+)\s*[,;]\s*(?P<kind>value|values)\s*:\s*(?P<payload>.+?)\s*$",
    re.IGNORECASE,
)


def _coerce_filter_inline_payload(raw_value: str) -> Any:
    text = str(raw_value or "").strip()
    if not text:
        return None
    try:
        return json.loads(text)
    except Exception:
        return text


def _normalize_filter_row(row: Any) -> Any:
    if not isinstance(row, dict):
        return row
    out = dict(row)
    op_raw = str(out.get("op") or "").strip()
    if not op_raw:
        return out

    match = _FILTER_OP_INLINE_VALUE_RE.match(op_raw)
    if match:
        inline_op = str(match.group("op") or "").strip().lower()
        inline_kind = str(match.group("kind") or "").strip().lower()
        inline_payload = _coerce_filter_inline_payload(match.group("payload"))
        if inline_op in _FILTER_OP_VALUES:
            out["op"] = inline_op
            if "value" not in out and "values" not in out and inline_payload is not None:
                if inline_kind == "values" or inline_op == "in":
                    out["values"] = inline_payload if isinstance(inline_payload, list) else [inline_payload]
                else:
                    out["value"] = inline_payload
            op_raw = inline_op

    op_normalized = str(op_raw or "").strip().lower()
    if op_normalized in _FILTER_OP_VALUES:
        out["op"] = op_normalized
        if op_normalized == "in":
            if isinstance(out.get("value"), list) and "values" not in out:
                out["values"] = out.pop("value")
            elif "values" not in out and "value" in out:
                out["values"] = [out.pop("value")]
        return out

    op_prefix = re.split(r"[,;]", op_raw, maxsplit=1)[0].strip().lower()
    if op_prefix in _FILTER_OP_VALUES:
        out["op"] = op_prefix
        if op_prefix == "in":
            if isinstance(out.get("value"), list) and "values" not in out:
                out["values"] = out.pop("value")
            elif "values" not in out and "value" in out:
                out["values"] = [out.pop("value")]
    return out


def _normalize_tool_argument_object(arguments: dict[str, Any]) -> dict[str, Any]:
    out = dict(arguments or {})
    filters = out.get("filters")
    if isinstance(filters, list):
        out["filters"] = [_normalize_filter_row(item) for item in filters]
    filter_row = out.get("filter")
    if isinstance(filter_row, dict):
        out["filter"] = _normalize_filter_row(filter_row)
    return out


def _normalize_tool_call_arguments(arguments: Any) -> tuple[Any, bool]:
    if isinstance(arguments, dict):
        normalized = _normalize_tool_argument_object(arguments)
        return normalized, normalized != arguments
    if not isinstance(arguments, str):
        return arguments, False
    text = arguments.strip()
    if not text:
        return arguments, False
    try:
        parsed = json.loads(text)
    except Exception:
        return arguments, False
    if not isinstance(parsed, dict):
        return arguments, False
    normalized = _normalize_tool_argument_object(parsed)
    if normalized == parsed:
        return arguments, False
    return json.dumps(normalized, ensure_ascii=False), True


def _repair_tool_calls_list(tool_calls: Any) -> tuple[Any, bool]:
    if not isinstance(tool_calls, list):
        return tool_calls, False
    changed = False
    repaired: list[Any] = []
    for row in tool_calls:
        if not isinstance(row, dict):
            repaired.append(row)
            continue
        next_row = dict(row)
        fn = next_row.get("function")
        if isinstance(fn, dict):
            next_fn = dict(fn)
            repaired_args, args_changed = _normalize_tool_call_arguments(next_fn.get("arguments"))
            if args_changed:
                changed = True
                next_fn["arguments"] = repaired_args
                next_row["function"] = next_fn
        repaired.append(next_row)
    return repaired, changed


def _repair_message_tool_call_arguments(messages: Any) -> Any:
    if not isinstance(messages, list):
        return messages
    changed = False
    repaired_messages: list[Any] = []
    for row in messages:
        if not isinstance(row, dict):
            repaired_messages.append(row)
            continue
        next_row = dict(row)
        role = str(next_row.get("role") or "").strip().lower()
        if role == "assistant":
            repaired_tool_calls, tool_calls_changed = _repair_tool_calls_list(next_row.get("tool_calls"))
            if tool_calls_changed:
                changed = True
                next_row["tool_calls"] = repaired_tool_calls
        repaired_messages.append(next_row)
    return repaired_messages if changed else messages


def _repair_openai_response_tool_call_arguments(payload: Any) -> Any:
    if not isinstance(payload, dict):
        return payload
    outgoing = dict(payload)
    choices = outgoing.get("choices")
    if not isinstance(choices, list):
        return outgoing
    changed = False
    repaired_choices: list[Any] = []
    for choice in choices:
        if not isinstance(choice, dict):
            repaired_choices.append(choice)
            continue
        next_choice = dict(choice)
        message = next_choice.get("message")
        if isinstance(message, dict):
            next_message = dict(message)
            repaired_tool_calls, tool_calls_changed = _repair_tool_calls_list(next_message.get("tool_calls"))
            if tool_calls_changed:
                changed = True
                next_message["tool_calls"] = repaired_tool_calls
                next_choice["message"] = next_message
        repaired_choices.append(next_choice)
    if changed:
        outgoing["choices"] = repaired_choices
    return outgoing


def _latest_dataset_target_for_validation_tool(request_tool_results: Any) -> tuple[str, str]:
    if not isinstance(request_tool_results, list):
        return "", ""
    for row in reversed(request_tool_results):
        if not isinstance(row, dict):
            continue
        if row.get("success") is not True:
            continue
        dataset_ref = str(row.get("datasetRef") or "").strip()
        dataset_name = str(row.get("datasetName") or "").strip()
        if dataset_ref or dataset_name:
            return dataset_ref, dataset_name
    return "", ""


def _normalize_dataset_identity(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return text[3:].strip().lower() if text.lower().startswith("id:") else text.lower()


def _normalize_field_names(value: Any) -> set[str]:
    if not isinstance(value, list):
        return set()
    out: set[str] = set()
    for item in value:
        candidate = str(item or "").strip().lower()
        if candidate:
            out.add(candidate)
    return out


def _normalize_aggregation_outputs(value: Any) -> dict[str, str]:
    if not isinstance(value, dict):
        return {}
    out: dict[str, str] = {}
    for key, raw in value.items():
        normalized_key = str(key or "").strip().lower()
        normalized_value = str(raw or "").strip()
        if normalized_key and normalized_value:
            out[normalized_key] = normalized_value
    return out


def _normalize_field_aliases(value: Any) -> dict[str, str]:
    if not isinstance(value, dict):
        return {}
    out: dict[str, str] = {}
    for key, raw in value.items():
        normalized_key = str(key or "").strip().lower()
        normalized_value = str(raw or "").strip()
        if normalized_key and normalized_value:
            out[normalized_key] = normalized_value
    return out


def _metric_field_alias_candidates(value: Any) -> set[str]:
    raw = str(value or "").strip().lower()
    if not raw:
        return set()
    compact = re.sub(r"[\s\-]+", "_", raw)
    alnum_underscored = re.sub(r"[^a-z0-9_]+", "_", compact)
    normalized = re.sub(r"_+", "_", alnum_underscored).strip("_")
    return {candidate for candidate in {raw, compact, normalized} if candidate}


def _normalize_catalog_datasets(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    out: list[dict[str, Any]] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        out.append(
            {
                "datasetRef": item.get("datasetRef"),
                "datasetName": item.get("datasetName"),
                "fieldCatalog": item.get("fieldCatalog"),
                "numericFields": item.get("numericFields"),
                "styleableFields": item.get("styleableFields"),
                "defaultStyleField": item.get("defaultStyleField"),
                "aggregationOutputs": item.get("aggregationOutputs"),
                "fieldAliases": item.get("fieldAliases"),
            }
        )
    return out


def _candidate_metric_metadata_rows(row: dict[str, Any]) -> list[dict[str, Any]]:
    candidates = [row]
    candidates.extend(_normalize_catalog_datasets(row.get("catalogDatasets")))
    return candidates


def _resolve_metric_field_rewrite(
    *,
    dataset_name: str,
    requested_field_name: str,
    request_tool_results: Any,
) -> str:
    dataset_identity = _normalize_dataset_identity(dataset_name)
    requested_identity = str(requested_field_name or "").strip()
    requested_lower = requested_identity.lower()
    requested_alias_candidates = _metric_field_alias_candidates(requested_identity)
    if not dataset_identity or not requested_identity or not isinstance(request_tool_results, list):
        return ""

    for row in reversed(request_tool_results):
        if not isinstance(row, dict):
            continue
        if row.get("success") is not True:
            continue
        for candidate in _candidate_metric_metadata_rows(row):
            if not isinstance(candidate, dict):
                continue
            row_dataset_identities = {
                _normalize_dataset_identity(candidate.get("datasetRef")),
                _normalize_dataset_identity(candidate.get("datasetName")),
            }
            row_dataset_identities.discard("")
            if dataset_identity not in row_dataset_identities:
                continue

            known_fields = _normalize_field_names(candidate.get("fieldCatalog"))
            known_fields.update(_normalize_field_names(candidate.get("numericFields")))
            known_fields.update(_normalize_field_names(candidate.get("styleableFields")))
            if requested_lower in known_fields:
                return ""

            field_aliases = _normalize_field_aliases(candidate.get("fieldAliases"))
            for alias_candidate in requested_alias_candidates:
                replacement = field_aliases.get(alias_candidate)
                if replacement and replacement.lower() not in requested_alias_candidates:
                    return replacement

            aggregation_outputs = _normalize_aggregation_outputs(candidate.get("aggregationOutputs"))
            if (
                ("count_weighted" in requested_lower or "weighted_count" in requested_lower or "weighted count" in requested_lower)
                and aggregation_outputs.get("count_weighted")
            ):
                return aggregation_outputs["count_weighted"]
            if "distinct_count" in requested_lower and aggregation_outputs.get("distinct_count"):
                return aggregation_outputs["distinct_count"]
            if "sum" in requested_lower and aggregation_outputs.get("sum"):
                return aggregation_outputs["sum"]
            if "count" in requested_lower and aggregation_outputs.get("count"):
                return aggregation_outputs["count"]
            if "avg" in requested_lower and aggregation_outputs.get("avg"):
                return aggregation_outputs["avg"]
            if "min" in requested_lower and aggregation_outputs.get("min"):
                return aggregation_outputs["min"]
            if "max" in requested_lower and aggregation_outputs.get("max"):
                return aggregation_outputs["max"]

            default_style_field = str(candidate.get("defaultStyleField") or "").strip()
            if default_style_field and default_style_field.lower() not in {requested_lower, ""}:
                return default_style_field
    return ""


def _repair_qmap_validation_tool_call_arguments(
    payload: Any,
    *,
    request_tool_results: Any,
) -> Any:
    if not isinstance(payload, dict):
        return payload

    fallback_dataset_ref, fallback_dataset_name = _latest_dataset_target_for_validation_tool(request_tool_results)
    fallback_dataset = fallback_dataset_ref or fallback_dataset_name
    if not fallback_dataset:
        return payload

    outgoing = dict(payload)
    choices = outgoing.get("choices")
    if not isinstance(choices, list):
        return outgoing

    changed = False
    repaired_choices: list[Any] = []
    for choice in choices:
        if not isinstance(choice, dict):
            repaired_choices.append(choice)
            continue
        next_choice = dict(choice)
        message = next_choice.get("message")
        if not isinstance(message, dict):
            repaired_choices.append(choice)
            continue
        next_message = dict(message)
        tool_calls = next_message.get("tool_calls")
        if not isinstance(tool_calls, list):
            repaired_choices.append(choice)
            continue

        repaired_calls: list[Any] = []
        tool_calls_changed = False
        for call in tool_calls:
            if not isinstance(call, dict):
                repaired_calls.append(call)
                continue
            next_call = dict(call)
            function = next_call.get("function")
            function_dict = function if isinstance(function, dict) else {}
            tool_name = str(function_dict.get("name") or "").strip()
            if tool_name not in {"waitForQMapDataset", "countQMapRows"}:
                repaired_calls.append(call)
                continue

            raw_arguments = function_dict.get("arguments")
            if isinstance(raw_arguments, dict):
                parsed_args = _normalize_tool_argument_object(raw_arguments)
            elif isinstance(raw_arguments, str) and raw_arguments.strip():
                try:
                    parsed_json = json.loads(raw_arguments)
                except Exception:
                    parsed_json = {}
                parsed_args = _normalize_tool_argument_object(parsed_json) if isinstance(parsed_json, dict) else {}
            else:
                parsed_args = {}

            repaired_args = dict(parsed_args)
            existing_dataset_ref = str(repaired_args.get("datasetRef") or repaired_args.get("datasetId") or "").strip()
            existing_dataset_name = str(repaired_args.get("datasetName") or "").strip()
            existing_dataset_identity = existing_dataset_ref or existing_dataset_name

            # Some runtimes require canonical id:<datasetId> for waitForQMapDataset when
            # the dataset has just been materialized and the friendly name is not stable yet.
            if tool_name == "waitForQMapDataset" and fallback_dataset_ref and not existing_dataset_ref:
                normalized_existing = _normalize_dataset_identity(existing_dataset_identity)
                if normalized_existing in {
                    "",
                    _normalize_dataset_identity(fallback_dataset_ref),
                    _normalize_dataset_identity(fallback_dataset_name),
                }:
                    repaired_args["datasetRef"] = fallback_dataset_ref
                    if not existing_dataset_name:
                        repaired_args["datasetName"] = fallback_dataset_ref

            if not any(str(repaired_args.get(key) or "").strip() for key in ("datasetName", "datasetRef", "datasetId")):
                if fallback_dataset_ref:
                    repaired_args["datasetRef"] = fallback_dataset_ref
                    repaired_args["datasetName"] = fallback_dataset_ref
                else:
                    repaired_args["datasetName"] = fallback_dataset

            if repaired_args == parsed_args:
                repaired_calls.append(call)
                continue

            next_function = dict(function_dict)
            next_function["arguments"] = json.dumps(repaired_args, ensure_ascii=False)
            next_call["function"] = next_function
            repaired_calls.append(next_call)
            tool_calls_changed = True

        if tool_calls_changed:
            changed = True
            next_message["tool_calls"] = repaired_calls
            next_choice["message"] = next_message
            repaired_choices.append(next_choice)
        else:
            repaired_choices.append(choice)

    if changed:
        outgoing["choices"] = repaired_choices
    return outgoing


def _repair_qmap_metric_tool_call_arguments(
    payload: Any,
    *,
    request_tool_results: Any,
) -> Any:
    if not isinstance(payload, dict):
        return payload

    outgoing = dict(payload)
    choices = outgoing.get("choices")
    if not isinstance(choices, list):
        return outgoing

    changed = False
    repaired_choices: list[Any] = []
    metric_arg_by_tool = {
        "setQMapLayerColorByField": "fieldName",
        "setQMapLayerColorByThresholds": "fieldName",
        "setQMapLayerColorByStatsThresholds": "fieldName",
        "setQMapLayerHeightByField": "fieldName",
        "rankQMapDatasetRows": "metricFieldName",
    }

    for choice in choices:
        if not isinstance(choice, dict):
            repaired_choices.append(choice)
            continue
        next_choice = dict(choice)
        message = next_choice.get("message")
        if not isinstance(message, dict):
            repaired_choices.append(choice)
            continue
        next_message = dict(message)
        tool_calls = next_message.get("tool_calls")
        if not isinstance(tool_calls, list):
            repaired_choices.append(choice)
            continue

        repaired_calls: list[Any] = []
        tool_calls_changed = False
        for call in tool_calls:
            if not isinstance(call, dict):
                repaired_calls.append(call)
                continue
            next_call = dict(call)
            function = next_call.get("function")
            function_dict = function if isinstance(function, dict) else {}
            tool_name = str(function_dict.get("name") or "").strip()
            raw_arguments = function_dict.get("arguments")
            if isinstance(raw_arguments, dict):
                parsed_args = _normalize_tool_argument_object(raw_arguments)
            elif isinstance(raw_arguments, str) and raw_arguments.strip():
                try:
                    parsed_json = json.loads(raw_arguments)
                except Exception:
                    parsed_json = {}
                parsed_args = _normalize_tool_argument_object(parsed_json) if isinstance(parsed_json, dict) else {}
            else:
                parsed_args = {}

            if tool_name == "createDatasetWithNormalizedField":
                repaired_args = dict(parsed_args)
                alias_pairs = (
                    ("fieldName", "numeratorFieldName"),
                    ("normalizationFieldName", "denominatorFieldName"),
                    ("newFieldName", "outputFieldName"),
                )
                alias_changed = False
                for source_key, target_key in alias_pairs:
                    if str(repaired_args.get(target_key) or "").strip():
                        continue
                    source_value = repaired_args.get(source_key)
                    if source_value is None or not str(source_value).strip():
                        continue
                    repaired_args[target_key] = source_value
                    alias_changed = True
                if alias_changed:
                    next_function = dict(function_dict)
                    next_function["arguments"] = json.dumps(repaired_args, ensure_ascii=False)
                    next_call["function"] = next_function
                    repaired_calls.append(next_call)
                    tool_calls_changed = True
                else:
                    repaired_calls.append(call)
                continue

            metric_arg_name = metric_arg_by_tool.get(tool_name)
            if not metric_arg_name:
                repaired_calls.append(call)
                continue

            dataset_name = str(parsed_args.get("datasetName") or parsed_args.get("datasetRef") or "").strip()
            requested_field_name = str(parsed_args.get(metric_arg_name) or "").strip()
            replacement_field_name = _resolve_metric_field_rewrite(
                dataset_name=dataset_name,
                requested_field_name=requested_field_name,
                request_tool_results=request_tool_results,
            )
            if not replacement_field_name:
                repaired_calls.append(call)
                continue

            repaired_args = dict(parsed_args)
            repaired_args[metric_arg_name] = replacement_field_name
            next_function = dict(function_dict)
            next_function["arguments"] = json.dumps(repaired_args, ensure_ascii=False)
            next_call["function"] = next_function
            repaired_calls.append(next_call)
            tool_calls_changed = True

        if tool_calls_changed:
            changed = True
            next_message["tool_calls"] = repaired_calls
            next_choice["message"] = next_message
            repaired_choices.append(next_choice)
        else:
            repaired_choices.append(choice)

    if changed:
        outgoing["choices"] = repaired_choices
    return outgoing
