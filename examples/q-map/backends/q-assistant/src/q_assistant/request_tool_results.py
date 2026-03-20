from __future__ import annotations

from typing import Any

from .message_text import _text_from_message_content
from .tool_calls import _extract_tool_calls_from_assistant_message
from .tool_contracts import (
    _QMAP_TOOL_RESULT_SCHEMA,
    _get_qmap_tool_response_schema,
    _validate_qmap_tool_result_payload,
)
from .tool_result_parsing import (
    _extract_success_from_text,
    _normalize_dataset_ref,
    _read_tool_message_content,
)


def _extract_dataset_routing_metadata(payload: Any) -> tuple[bool | None, str, str]:
    if not isinstance(payload, dict):
        return None, "", ""

    routing_is_administrative: bool | None = None
    dataset_class = ""
    routing_preferred_tool = ""

    routing = payload.get("routing")
    if isinstance(routing, dict):
        if isinstance(routing.get("isAdministrative"), bool):
            routing_is_administrative = bool(routing.get("isAdministrative"))
        routing_class = str(routing.get("datasetClass") or "").strip()
        if routing_class:
            dataset_class = routing_class
        query_tool_hint = routing.get("queryToolHint")
        if isinstance(query_tool_hint, dict):
            preferred_tool = str(query_tool_hint.get("preferredTool") or "").strip()
            if preferred_tool:
                routing_preferred_tool = preferred_tool

    ai_hints = payload.get("aiHints")
    if isinstance(ai_hints, dict):
        profile = ai_hints.get("aiProfile")
        if isinstance(profile, dict):
            profile_class = str(profile.get("datasetClass") or profile.get("dataset_class") or "").strip()
            if profile_class:
                dataset_class = profile_class
            query_routing = profile.get("queryRouting")
            if isinstance(query_routing, dict):
                profile_preferred_tool = str(query_routing.get("preferredTool") or "").strip()
                if profile_preferred_tool and not routing_preferred_tool:
                    routing_preferred_tool = profile_preferred_tool
        hints_class = str(ai_hints.get("datasetClass") or "").strip()
        if hints_class:
            dataset_class = hints_class

    return routing_is_administrative, dataset_class, routing_preferred_tool


def _normalize_field_name_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    out: list[str] = []
    for item in value:
        if isinstance(item, dict):
            candidate = str(item.get("name") or item.get("field") or "").strip()
        else:
            candidate = str(item or "").strip()
        if candidate:
            out.append(candidate)
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


def _merge_field_name_lists(*values: Any) -> list[str]:
    merged: list[str] = []
    seen: set[str] = set()
    for value in values:
        for candidate in _normalize_field_name_list(value):
            lowered = candidate.lower()
            if lowered in seen:
                continue
            seen.add(lowered)
            merged.append(candidate)
    return merged


def _extract_catalog_dataset_metadata(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    raw_datasets = payload.get("datasets")
    if not isinstance(raw_datasets, list):
        return []
    raw_layers = payload.get("layers")
    layers = raw_layers if isinstance(raw_layers, list) else []

    def _match_layer_to_dataset(layer: Any, dataset_name: str, dataset_ref: str) -> bool:
        if not isinstance(layer, dict):
            return False
        layer_dataset_ref = _normalize_dataset_ref(layer.get("datasetRef")) or _normalize_dataset_ref(layer.get("datasetId"))
        if dataset_ref and layer_dataset_ref and dataset_ref == layer_dataset_ref:
            return True
        layer_dataset_name = str(layer.get("datasetName") or layer.get("name") or "").strip().lower()
        return bool(dataset_name and layer_dataset_name and dataset_name.lower() == layer_dataset_name)

    out: list[dict[str, Any]] = []
    for raw_dataset in raw_datasets:
        if not isinstance(raw_dataset, dict):
            continue
        dataset_name = str(
            raw_dataset.get("name")
            or raw_dataset.get("datasetName")
            or raw_dataset.get("label")
            or ""
        ).strip()
        dataset_ref = (
            _normalize_dataset_ref(raw_dataset.get("datasetRef"))
            or _normalize_dataset_ref(raw_dataset.get("id"))
            or _normalize_dataset_ref(raw_dataset.get("datasetId"))
        )
        if not dataset_name and not dataset_ref:
            continue

        matching_layers = [
            layer for layer in layers if _match_layer_to_dataset(layer, dataset_name, dataset_ref)
        ]
        active_fields = _merge_field_name_lists(
            *[layer.get("activeFields") for layer in matching_layers if isinstance(layer, dict)]
        )
        available_fields = _merge_field_name_lists(
            raw_dataset.get("fields"),
            raw_dataset.get("fieldCatalog"),
            raw_dataset.get("availableFields"),
            *[layer.get("availableFields") for layer in matching_layers if isinstance(layer, dict)],
        )
        field_catalog = _merge_field_name_lists(available_fields, active_fields)
        default_style_field = str(raw_dataset.get("defaultStyleField") or "").strip()
        if not default_style_field and active_fields:
            default_style_field = active_fields[0]

        out.append(
            {
                "datasetRef": dataset_ref or None,
                "datasetName": dataset_name or None,
                "fieldCatalog": field_catalog or None,
                "numericFields": None,
                "styleableFields": active_fields or None,
                "defaultStyleField": default_style_field or None,
                "aggregationOutputs": None,
                "fieldAliases": _normalize_field_aliases(raw_dataset.get("fieldAliases")) or None,
            }
        )
    return out


def _extract_tool_field_metadata(
    payload: Any,
) -> tuple[list[str], list[str], list[str], str, dict[str, str], dict[str, str]]:
    if not isinstance(payload, dict):
        return [], [], [], "", {}, {}
    field_catalog = _normalize_field_name_list(payload.get("fieldCatalog"))
    numeric_fields = _normalize_field_name_list(payload.get("numericFields"))
    styleable_fields = _normalize_field_name_list(payload.get("styleableFields"))
    default_style_field = str(payload.get("defaultStyleField") or "").strip()
    aggregation_outputs = _normalize_aggregation_outputs(payload.get("aggregationOutputs"))
    field_aliases = _normalize_field_aliases(payload.get("fieldAliases"))
    return field_catalog, numeric_fields, styleable_fields, default_style_field, aggregation_outputs, field_aliases


def _enrich_contract_validation_payload(
    *,
    tool_name: str,
    payload: dict[str, Any],
    dataset_name: str,
    dataset_ref: str,
    field_catalog: list[str],
    numeric_fields: list[str],
    styleable_fields: list[str],
    default_style_field: str,
    catalog_datasets: list[dict[str, Any]],
) -> dict[str, Any]:
    enriched = dict(payload)
    if dataset_name:
        enriched.setdefault("dataset", dataset_name)
        enriched.setdefault("datasetName", dataset_name)
    if dataset_ref:
        enriched.setdefault("datasetRef", dataset_ref)
    if field_catalog:
        enriched.setdefault("fieldCatalog", field_catalog)
    if numeric_fields:
        enriched.setdefault("numericFields", numeric_fields)
    if styleable_fields:
        enriched.setdefault("styleableFields", styleable_fields)
    if default_style_field:
        enriched.setdefault("defaultStyleField", default_style_field)
        enriched.setdefault("outputFieldName", default_style_field)

    if tool_name == "listQMapDatasets":
        if "datasets" not in enriched and catalog_datasets:
            enriched["datasets"] = [
                {
                    "datasetRef": item.get("datasetRef"),
                    "datasetName": item.get("datasetName"),
                }
                for item in catalog_datasets
                if isinstance(item, dict)
            ]
        if "layers" not in enriched and ("datasets" in enriched or catalog_datasets):
            enriched["layers"] = []

    return enriched


def _extract_request_tool_results(payload: Any, *, max_items: int = 48) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    messages = payload.get("messages")
    if not isinstance(messages, list):
        return []

    tool_name_by_call_id: dict[str, str] = {}
    results: list[dict[str, Any]] = []

    for message in messages:
        if not isinstance(message, dict):
            continue
        role = str(message.get("role") or "").strip().lower()

        if role == "assistant":
            for call in _extract_tool_calls_from_assistant_message(message):
                call_id = str(call.get("id") or "").strip()
                fn_name = str(call.get("name") or "").strip()
                if call_id and fn_name:
                    tool_name_by_call_id[call_id] = fn_name
            continue

        if role != "tool":
            continue

        tool_call_id = str(message.get("tool_call_id") or "").strip()
        tool_name = str(message.get("name") or "").strip() or tool_name_by_call_id.get(tool_call_id, "")
        content = message.get("content")
        details = ""
        success: bool | None = None
        result_schema = ""
        dataset_ref = ""
        dataset_name = ""
        clarification_required = False
        clarification_question = ""
        clarification_options: list[str] = []
        routing_is_administrative: bool | None = None
        dataset_class = ""
        routing_preferred_tool = ""
        field_catalog: list[str] = []
        numeric_fields: list[str] = []
        styleable_fields: list[str] = []
        default_style_field = ""
        aggregation_outputs: dict[str, str] = {}
        field_aliases: dict[str, str] = {}
        catalog_datasets: list[dict[str, Any]] = []
        contract_response_validation_errors: list[str] = []
        parsed, details = _read_tool_message_content(content)
        contract_validation_payload: dict[str, Any] | None = None

        if isinstance(parsed, dict):
            catalog_datasets = _extract_catalog_dataset_metadata(parsed)
            parsed_routing_is_administrative, parsed_dataset_class, parsed_routing_preferred_tool = (
                _extract_dataset_routing_metadata(parsed)
            )
            if parsed_routing_is_administrative is not None:
                routing_is_administrative = parsed_routing_is_administrative
            if parsed_dataset_class:
                dataset_class = parsed_dataset_class
            if parsed_routing_preferred_tool:
                routing_preferred_tool = parsed_routing_preferred_tool
            qmap_result = parsed.get("qmapToolResult")
            if isinstance(qmap_result, dict):
                # Merge qmapToolResult (envelope: success, details, schema …)
                # with llmResult (tool-specific fields: providers, datasets,
                # providerId, datasetId …).  llmResult fields take priority on
                # collision because they carry the actual response data.
                llm_for_contract = parsed.get("llmResult")
                if isinstance(llm_for_contract, dict):
                    merged = dict(qmap_result)
                    merged.update(llm_for_contract)
                    contract_validation_payload = merged
                else:
                    contract_validation_payload = dict(qmap_result)
                qmap_catalog_datasets = _extract_catalog_dataset_metadata(qmap_result)
                if qmap_catalog_datasets:
                    catalog_datasets = qmap_catalog_datasets
                (
                    qmap_field_catalog,
                    qmap_numeric_fields,
                    qmap_styleable_fields,
                    qmap_default_style_field,
                    qmap_aggregation_outputs,
                    qmap_field_aliases,
                ) = _extract_tool_field_metadata(qmap_result)
                if qmap_field_catalog:
                    field_catalog = qmap_field_catalog
                if qmap_numeric_fields:
                    numeric_fields = qmap_numeric_fields
                if qmap_styleable_fields:
                    styleable_fields = qmap_styleable_fields
                if qmap_default_style_field:
                    default_style_field = qmap_default_style_field
                if qmap_aggregation_outputs:
                    aggregation_outputs = qmap_aggregation_outputs
                if qmap_field_aliases:
                    field_aliases = qmap_field_aliases
                schema_value = qmap_result.get("schema")
                if isinstance(schema_value, str) and schema_value.strip():
                    result_schema = schema_value.strip()
                qmap_tool_name = qmap_result.get("toolName")
                if isinstance(qmap_tool_name, str) and qmap_tool_name.strip() and not tool_name:
                    tool_name = qmap_tool_name.strip()
                detail_value = qmap_result.get("details")
                if isinstance(detail_value, str) and detail_value.strip():
                    details = detail_value.strip()
                if isinstance(qmap_result.get("success"), bool):
                    success = bool(qmap_result.get("success"))
                dataset_ref = _normalize_dataset_ref(qmap_result.get("loadedDatasetRef")) or _normalize_dataset_ref(
                    qmap_result.get("datasetRef")
                )

            llm_result = parsed.get("llmResult")
            if isinstance(llm_result, dict):
                if contract_validation_payload is None:
                    contract_validation_payload = dict(llm_result)
                llm_catalog_datasets = _extract_catalog_dataset_metadata(llm_result)
                if llm_catalog_datasets:
                    catalog_datasets = llm_catalog_datasets
                (
                    llm_field_catalog,
                    llm_numeric_fields,
                    llm_styleable_fields,
                    llm_default_style_field,
                    llm_aggregation_outputs,
                    llm_field_aliases,
                ) = _extract_tool_field_metadata(llm_result)
                if llm_field_catalog:
                    field_catalog = llm_field_catalog
                if llm_numeric_fields:
                    numeric_fields = llm_numeric_fields
                if llm_styleable_fields:
                    styleable_fields = llm_styleable_fields
                if llm_default_style_field:
                    default_style_field = llm_default_style_field
                if llm_aggregation_outputs:
                    aggregation_outputs = llm_aggregation_outputs
                if llm_field_aliases:
                    field_aliases = llm_field_aliases
                llm_routing_is_administrative, llm_dataset_class, llm_routing_preferred_tool = (
                    _extract_dataset_routing_metadata(llm_result)
                )
                if llm_routing_is_administrative is not None:
                    routing_is_administrative = llm_routing_is_administrative
                if llm_dataset_class:
                    dataset_class = llm_dataset_class
                if llm_routing_preferred_tool:
                    routing_preferred_tool = llm_routing_preferred_tool
                detail_value = llm_result.get("details")
                if isinstance(detail_value, str) and detail_value.strip():
                    details = detail_value.strip()
                if isinstance(llm_result.get("success"), bool):
                    success = bool(llm_result.get("success"))
                if isinstance(llm_result.get("clarificationRequired"), bool):
                    clarification_required = bool(llm_result.get("clarificationRequired"))
                question_value = llm_result.get("clarificationQuestion")
                if isinstance(question_value, str) and question_value.strip():
                    clarification_question = question_value.strip()
                options_value = llm_result.get("clarificationOptions")
                if isinstance(options_value, list):
                    clarification_options = [
                        str(value or "").strip() for value in options_value if str(value or "").strip()
                    ]
                dataset_name = str(
                    llm_result.get("dataset")
                    or llm_result.get("datasetName")
                    or llm_result.get("loadedDatasetName")
                    or ""
                ).strip()
                dataset_ref = (
                    _normalize_dataset_ref(llm_result.get("loadedDatasetRef"))
                    or _normalize_dataset_ref(llm_result.get("datasetRef"))
                    or _normalize_dataset_ref(llm_result.get("datasetId"))
                    or dataset_ref
                )
            if not details:
                detail_value = parsed.get("details")
                if isinstance(detail_value, str) and detail_value.strip():
                    details = detail_value.strip()
            (
                parsed_field_catalog,
                parsed_numeric_fields,
                parsed_styleable_fields,
                parsed_default_style_field,
                parsed_aggregation_outputs,
                parsed_field_aliases,
            ) = _extract_tool_field_metadata(parsed)
            if parsed_field_catalog and not field_catalog:
                field_catalog = parsed_field_catalog
            if parsed_numeric_fields and not numeric_fields:
                numeric_fields = parsed_numeric_fields
            if parsed_styleable_fields and not styleable_fields:
                styleable_fields = parsed_styleable_fields
            if parsed_default_style_field and not default_style_field:
                default_style_field = parsed_default_style_field
            if parsed_aggregation_outputs and not aggregation_outputs:
                aggregation_outputs = parsed_aggregation_outputs
            if parsed_field_aliases and not field_aliases:
                field_aliases = parsed_field_aliases
            if success is None and isinstance(parsed.get("success"), bool):
                success = bool(parsed.get("success"))
            if isinstance(parsed.get("clarificationRequired"), bool):
                clarification_required = bool(parsed.get("clarificationRequired"))
            top_level_question = parsed.get("clarificationQuestion")
            if isinstance(top_level_question, str) and top_level_question.strip() and not clarification_question:
                clarification_question = top_level_question.strip()
            top_level_options = parsed.get("clarificationOptions")
            if isinstance(top_level_options, list) and not clarification_options:
                clarification_options = [
                    str(value or "").strip() for value in top_level_options if str(value or "").strip()
                ]
            if not dataset_name:
                dataset_name = str(
                    parsed.get("dataset")
                    or parsed.get("datasetName")
                    or parsed.get("savedDatasetName")
                    or ""
                ).strip()
            if not dataset_ref:
                dataset_ref = (
                    _normalize_dataset_ref(parsed.get("datasetRef"))
                    or _normalize_dataset_ref(parsed.get("loadedDatasetRef"))
                    or _normalize_dataset_ref(parsed.get("datasetId"))
                )
            if not result_schema and parsed.get("schema") == _QMAP_TOOL_RESULT_SCHEMA:
                result_schema = _QMAP_TOOL_RESULT_SCHEMA
            if contract_validation_payload is None:
                contract_validation_payload = dict(parsed)

        if success is None and details:
            success = _extract_success_from_text(details)

        if not tool_name and not tool_call_id and not details:
            continue

        expected_response_schema = _get_qmap_tool_response_schema(tool_name) if tool_name else ""
        if tool_name and isinstance(contract_validation_payload, dict):
            contract_validation_payload = _enrich_contract_validation_payload(
                tool_name=tool_name,
                payload=contract_validation_payload,
                dataset_name=dataset_name,
                dataset_ref=dataset_ref,
                field_catalog=field_catalog,
                numeric_fields=numeric_fields,
                styleable_fields=styleable_fields,
                default_style_field=default_style_field,
                catalog_datasets=catalog_datasets,
            )
            contract_response_validation_errors = _validate_qmap_tool_result_payload(
                tool_name,
                contract_validation_payload,
            )
        contract_schema_mismatch = bool(
            expected_response_schema and result_schema and expected_response_schema != result_schema
        )
        contract_response_mismatch = bool(contract_response_validation_errors)

        results.append(
            {
                "toolCallId": tool_call_id or None,
                "toolName": tool_name or None,
                "success": success,
                "details": details or None,
                "resultSchema": result_schema or None,
                "contractExpectedSchema": expected_response_schema or None,
                "contractSchemaMismatch": contract_schema_mismatch,
                "contractResponseMismatch": contract_response_mismatch,
                "contractResponseValidationErrors": contract_response_validation_errors or None,
                "datasetRef": dataset_ref or None,
                "datasetName": dataset_name or None,
                "clarificationRequired": clarification_required,
                "clarificationQuestion": clarification_question or None,
                "clarificationOptions": clarification_options or None,
                "routingIsAdministrative": routing_is_administrative,
                "datasetClass": dataset_class or None,
                "routingPreferredTool": routing_preferred_tool or None,
                "fieldCatalog": field_catalog or None,
                "numericFields": numeric_fields or None,
                "styleableFields": styleable_fields or None,
                "defaultStyleField": default_style_field or None,
                "aggregationOutputs": aggregation_outputs or None,
                "fieldAliases": field_aliases or None,
                "catalogDatasets": catalog_datasets or None,
            }
        )

    if max_items > 0 and len(results) > max_items:
        return results[-max_items:]
    return results


def _messages_since_last_user(messages: Any) -> list[dict[str, Any]]:
    if not isinstance(messages, list):
        return []
    last_user_idx = -1
    for idx in range(len(messages) - 1, -1, -1):
        msg = messages[idx]
        if not isinstance(msg, dict):
            continue
        if str(msg.get("role") or "").strip().lower() == "user":
            last_user_idx = idx
            break

    start_idx = last_user_idx if last_user_idx >= 0 else 0
    out: list[dict[str, Any]] = []
    for msg in messages[start_idx:]:
        if isinstance(msg, dict):
            out.append(dict(msg))
    return out


def _extract_recent_tool_results_since_last_user(
    payload: dict[str, Any],
    *,
    max_items: int = 192,
) -> list[dict[str, Any]]:
    messages = payload.get("messages")
    tail_messages = _messages_since_last_user(messages)
    if not tail_messages:
        return _extract_request_tool_results(payload, max_items=max_items)
    return _extract_request_tool_results({"messages": tail_messages}, max_items=max_items)


def _has_assistant_text_since_last_user(payload: dict[str, Any]) -> bool:
    messages = _messages_since_last_user(payload.get("messages"))
    if not messages:
        return False
    for msg in messages:
        if str(msg.get("role") or "").strip().lower() != "assistant":
            continue
        content_text = _text_from_message_content(msg.get("content"))
        if content_text.strip():
            return True
    return False
