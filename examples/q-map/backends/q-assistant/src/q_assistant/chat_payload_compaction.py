from __future__ import annotations

import json
from typing import Any

from .tool_calls import _extract_tool_calls_from_assistant_message, _parse_tool_arguments
from .tool_contracts import _QMAP_TOOL_RESULT_SCHEMA
from .tool_result_parsing import _extract_success_from_text, _read_tool_message_content


_DISCOVERY_TOOLS_DEFAULT: set[str] = {
    "listQMapDatasets",
    "listQMapChartTools",
    "listQCumberProviders",
    "listQCumberDatasets",
    "getQCumberDatasetHelp",
    "queryQCumberDatasetSchema",
    "queryQCumberDatasetSchemaFlat",
}


def _compact_message_content(content: Any, *, max_chars: int) -> Any:
    if isinstance(content, str):
        text = content.strip()
        if len(text) <= max_chars:
            return text
        return text[:max_chars] + "...[truncated]"
    if isinstance(content, list):
        compact_parts: list[Any] = []
        for item in content:
            if not isinstance(item, dict):
                continue
            next_item = dict(item)
            text_piece = next_item.get("text")
            if isinstance(text_piece, str):
                stripped = text_piece.strip()
                if len(stripped) > max_chars:
                    next_item["text"] = stripped[:max_chars] + "...[truncated]"
                else:
                    next_item["text"] = stripped
            compact_parts.append(next_item)
        return compact_parts
    return content


def _compact_tool_payload_value(
    value: Any,
    *,
    depth: int = 0,
    max_depth: int = 4,
    max_list_items: int = 8,
    max_dict_items: int = 24,
    max_string_chars: int = 300,
) -> Any:
    if isinstance(value, str):
        text = value.strip()
        if len(text) <= max_string_chars:
            return text
        return text[:max_string_chars] + "...[truncated]"
    if isinstance(value, (bool, int, float)) or value is None:
        return value
    if depth >= max_depth:
        if isinstance(value, dict):
            preferred_keys = (
                "id",
                "name",
                "providerId",
                "datasetId",
                "datasetName",
                "loadedDatasetName",
                "loadedDatasetRef",
                "layerId",
                "layerName",
                "fieldName",
                "returned",
                "totalMatched",
                "count",
                "success",
                "details",
            )
            summary: dict[str, Any] = {}
            for key in preferred_keys:
                if key not in value:
                    continue
                raw = value.get(key)
                if isinstance(raw, str):
                    txt = raw.strip()
                    summary[key] = txt if len(txt) <= max_string_chars else txt[:max_string_chars] + "...[truncated]"
                elif isinstance(raw, (bool, int, float)) or raw is None:
                    summary[key] = raw
            return summary or "[object]"
        if isinstance(value, list):
            return "[list]"
        return str(value)
    if isinstance(value, list):
        out = [
            _compact_tool_payload_value(
                item,
                depth=depth + 1,
                max_depth=max_depth,
                max_list_items=max_list_items,
                max_dict_items=max_dict_items,
                max_string_chars=max_string_chars,
            )
            for item in value[:max_list_items]
        ]
        if len(value) > max_list_items:
            out.append({"truncatedItems": len(value) - max_list_items})
        return out
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        keys = list(value.keys())
        for index, key in enumerate(keys[:max_dict_items]):
            out[str(key)] = _compact_tool_payload_value(
                value.get(key),
                depth=depth + 1,
                max_depth=max_depth,
                max_list_items=max_list_items,
                max_dict_items=max_dict_items,
                max_string_chars=max_string_chars,
            )
            if index + 1 >= max_dict_items:
                break
        if len(keys) > max_dict_items:
            out["_truncatedKeys"] = len(keys) - max_dict_items
        return out
    return str(value)


def _compact_tool_message_content(content: Any, *, max_chars: int) -> Any:
    parsed, details = _read_tool_message_content(content)
    if not isinstance(parsed, dict):
        return _compact_message_content(content, max_chars=max_chars)

    compacted_payload = _compact_tool_payload_value(parsed)
    try:
        compacted_text = json.dumps(compacted_payload, ensure_ascii=False)
    except Exception:
        compacted_text = ""
    if compacted_text and len(compacted_text) <= max_chars:
        return compacted_text

    success: bool | None = None
    detail_text = ""
    qmap_result = parsed.get("qmapToolResult")
    if isinstance(qmap_result, dict):
        if isinstance(qmap_result.get("success"), bool):
            success = bool(qmap_result.get("success"))
        if isinstance(qmap_result.get("details"), str) and str(qmap_result.get("details")).strip():
            detail_text = str(qmap_result.get("details")).strip()
    llm_result = parsed.get("llmResult")
    if isinstance(llm_result, dict):
        if success is None and isinstance(llm_result.get("success"), bool):
            success = bool(llm_result.get("success"))
        if not detail_text and isinstance(llm_result.get("details"), str) and str(llm_result.get("details")).strip():
            detail_text = str(llm_result.get("details")).strip()
    if success is None and isinstance(parsed.get("success"), bool):
        success = bool(parsed.get("success"))
    if not detail_text and isinstance(parsed.get("details"), str) and str(parsed.get("details")).strip():
        detail_text = str(parsed.get("details")).strip()
    if not detail_text:
        detail_text = details or "Tool result compacted."
    detail_text = _compact_tool_payload_value(detail_text, max_string_chars=max(80, max_chars // 2))

    llm_summary: dict[str, Any] = {}
    if isinstance(llm_result, dict):
        scalar_keys = (
            "providerId",
            "datasetId",
            "datasetName",
            "loadedDatasetName",
            "loadedDatasetRef",
            "returned",
            "totalMatched",
            "count",
            "field",
            "operator",
            "value",
        )
        for key in scalar_keys:
            if key not in llm_result:
                continue
            raw = llm_result.get(key)
            if isinstance(raw, str):
                txt = raw.strip()
                if txt:
                    llm_summary[key] = txt[:120]
            elif isinstance(raw, (bool, int, float)) or raw is None:
                llm_summary[key] = raw

        fields_raw = llm_result.get("fields")
        if isinstance(fields_raw, list):
            llm_summary["fields"] = [str(item)[:80] for item in fields_raw[:20]]
            if len(fields_raw) > 20:
                llm_summary["fieldsTruncated"] = len(fields_raw) - 20

        auto_retry = llm_result.get("autoRetry")
        if isinstance(auto_retry, dict):
            llm_summary["autoRetry"] = {
                "attempted": bool(auto_retry.get("attempted")),
                "fromTool": str(auto_retry.get("fromTool") or "")[:80],
                "toTool": str(auto_retry.get("toTool") or "")[:80],
                "success": auto_retry.get("success"),
            }

    summary_payload = {
        "qmapToolResult": {
            "schema": _QMAP_TOOL_RESULT_SCHEMA,
            "success": success,
            "details": detail_text,
        }
    }
    if llm_summary:
        summary_payload["llmResultSummary"] = llm_summary
    summary_text = json.dumps(summary_payload, ensure_ascii=False)
    if len(summary_text) <= max_chars:
        return summary_text
    minimal_payload = {
        "qmapToolResult": {
            "schema": _QMAP_TOOL_RESULT_SCHEMA,
            "success": success,
            "details": "Tool result compacted.",
        }
    }
    minimal_text = json.dumps(minimal_payload, ensure_ascii=False)
    if len(minimal_text) <= max_chars:
        return minimal_text

    if llm_summary:
        fallback_payload = {
            "qmapToolResult": minimal_payload["qmapToolResult"],
            "llmResultSummary": {
                key: llm_summary[key]
                for key in ("providerId", "datasetId", "datasetName", "loadedDatasetName", "count")
                if key in llm_summary
            },
        }
        fallback_text = json.dumps(fallback_payload, ensure_ascii=False)
        if len(fallback_text) <= max_chars:
            return fallback_text
        tiny_summary = {
            key: str(llm_summary.get(key))[:48]
            for key in ("providerId", "datasetId")
            if llm_summary.get(key) is not None
        }
        if tiny_summary:
            tiny_payload = {
                "qmapToolResult": {
                    "schema": _QMAP_TOOL_RESULT_SCHEMA,
                    "success": success,
                    "details": "compacted",
                },
                "llmResultSummary": tiny_summary,
            }
            tiny_text = json.dumps(tiny_payload, ensure_ascii=False)
            if len(tiny_text) <= max_chars:
                return tiny_text

    return minimal_text[:max_chars]


def _extract_tool_call_ids_from_assistant(message: dict[str, Any]) -> list[str]:
    ids: list[str] = []
    for call in _extract_tool_calls_from_assistant_message(message):
        call_id = str(call.get("id") or "").strip()
        if call_id:
            ids.append(call_id)
    return ids


def _repair_openai_tool_message_sequence(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    repaired: list[dict[str, Any]] = []
    i = 0
    while i < len(messages):
        raw_msg = messages[i]
        if not isinstance(raw_msg, dict):
            i += 1
            continue

        role = str(raw_msg.get("role") or "").strip().lower()
        if role == "tool":
            i += 1
            continue

        if role != "assistant":
            repaired.append(dict(raw_msg))
            i += 1
            continue

        call_ids = _extract_tool_call_ids_from_assistant(raw_msg)
        if not call_ids:
            repaired.append(dict(raw_msg))
            i += 1
            continue

        expected = set(call_ids)
        contiguous_tools: list[dict[str, Any]] = []
        seen_ids: set[str] = set()
        j = i + 1
        while j < len(messages):
            next_msg = messages[j]
            if not isinstance(next_msg, dict):
                break
            next_role = str(next_msg.get("role") or "").strip().lower()
            if next_role != "tool":
                break
            tool_call_id = str(next_msg.get("tool_call_id") or "").strip()
            if tool_call_id in expected and tool_call_id not in seen_ids:
                contiguous_tools.append(dict(next_msg))
                seen_ids.add(tool_call_id)
            j += 1

        if expected.issubset(seen_ids):
            kept_calls: list[dict[str, Any]] = []
            for call in raw_msg.get("tool_calls", []):
                if not isinstance(call, dict):
                    continue
                call_id = str(call.get("id") or "").strip()
                if call_id in expected:
                    kept_calls.append(call)
            next_assistant = dict(raw_msg)
            next_assistant["tool_calls"] = kept_calls
            repaired.append(next_assistant)
            repaired.extend(contiguous_tools)
        else:
            next_assistant = dict(raw_msg)
            next_assistant.pop("tool_calls", None)
            content = next_assistant.get("content")
            if content not in (None, "", []):
                repaired.append(next_assistant)

        i = j

    return repaired


def _compact_openai_tool_schema(tool: dict[str, Any], *, aggressive: bool) -> dict[str, Any]:
    def _compact_schema_value(value: Any, *, depth: int = 0, max_depth: int = 8) -> Any:
        if depth > max_depth:
            if isinstance(value, dict):
                return {}
            if isinstance(value, list):
                return []
            return value
        if isinstance(value, dict):
            out: dict[str, Any] = {}
            for raw_key, raw_value in value.items():
                key = str(raw_key)
                if key in {"description", "title", "example", "examples", "$comment", "default"}:
                    continue
                out[key] = _compact_schema_value(raw_value, depth=depth + 1, max_depth=max_depth)
            return out
        if isinstance(value, list):
            return [_compact_schema_value(item, depth=depth + 1, max_depth=max_depth) for item in value]
        return value

    def _normalize_schema_required(value: Any) -> Any:
        if isinstance(value, list):
            return [_normalize_schema_required(item) for item in value]
        if not isinstance(value, dict):
            return value

        out: dict[str, Any] = {}
        for key, raw in value.items():
            out[str(key)] = _normalize_schema_required(raw)

        properties = out.get("properties")
        required = out.get("required")
        if isinstance(required, list):
            if isinstance(properties, dict):
                allowed = {str(k) for k in properties.keys()}
                filtered = [item for item in required if isinstance(item, str) and item in allowed]
                if filtered:
                    out["required"] = filtered
                else:
                    out.pop("required", None)
            else:
                out.pop("required", None)

        return out

    next_tool = dict(tool or {})
    fn = next_tool.get("function")
    if not isinstance(fn, dict):
        return next_tool

    fn_name = str(fn.get("name") or "").strip()
    if not fn_name:
        return next_tool

    compact_fn: dict[str, Any] = {"name": fn_name}
    if not aggressive:
        description = fn.get("description")
        if isinstance(description, str) and description.strip():
            compact_fn["description"] = description.strip()[:200]

    parameters = fn.get("parameters")
    if isinstance(parameters, dict):
        compact_fn["parameters"] = _normalize_schema_required(_compact_schema_value(parameters))

    next_tool["function"] = compact_fn
    next_tool["type"] = "function"
    return next_tool


def _compact_chat_completions_payload(
    payload: dict[str, Any],
    *,
    max_messages: int = 24,
    max_tool_messages: int = 8,
    max_message_content_chars: int = 4000,
    keep_system_messages: int = 2,
    compact_tool_messages: bool = False,
    max_tool_content_chars: int = 4000,
    compact_tool_schemas: bool = False,
    aggressive_tool_schema_compaction: bool = False,
) -> dict[str, Any]:
    outgoing = dict(payload or {})
    messages = outgoing.get("messages")
    if isinstance(messages, list):
        message_limit = max(1, int(max_messages))
        tool_message_limit = max(0, int(max_tool_messages))
        system_message_limit = max(0, int(keep_system_messages))
        user_assistant_content_limit = max(400, int(max_message_content_chars))
        tool_content_limit = max(400, int(max_tool_content_chars))
        system_messages: list[dict[str, Any]] = []
        non_system_messages: list[dict[str, Any]] = []
        for msg in messages:
            if not isinstance(msg, dict):
                continue
            role = str(msg.get("role") or "").strip().lower()
            if role == "system" and len(system_messages) < system_message_limit:
                system_messages.append(dict(msg))
            else:
                non_system_messages.append(dict(msg))

        last_user_message: dict[str, Any] | None = None
        for msg in reversed(non_system_messages):
            if str(msg.get("role") or "").strip().lower() == "user":
                last_user_message = dict(msg)
                break

        tail_limit = max(0, message_limit - len(system_messages))
        tail = non_system_messages[-tail_limit:] if tail_limit else []

        tool_indices = [idx for idx, msg in enumerate(tail) if str(msg.get("role") or "").strip().lower() == "tool"]
        if len(tool_indices) > tool_message_limit:
            keep_tool_indices = set(tool_indices[-tool_message_limit:])
            tail = [
                msg
                for idx, msg in enumerate(tail)
                if not (str(msg.get("role") or "").strip().lower() == "tool" and idx not in keep_tool_indices)
            ]

        if last_user_message is not None:
            has_user_in_tail = any(str(msg.get("role") or "").strip().lower() == "user" for msg in tail)
            if not has_user_in_tail:
                if tail_limit <= 1:
                    tail = [last_user_message]
                else:
                    tail = [last_user_message, *tail[-(tail_limit - 1):]]

        compacted_messages: list[dict[str, Any]] = []
        for msg in [*system_messages, *tail]:
            role = str(msg.get("role") or "").strip().lower()
            next_msg = dict(msg)
            if role == "tool":
                if compact_tool_messages:
                    next_msg["content"] = _compact_tool_message_content(
                        next_msg.get("content"),
                        max_chars=tool_content_limit,
                    )
                else:
                    next_msg["content"] = next_msg.get("content")
            elif role == "system":
                next_msg["content"] = next_msg.get("content")
            else:
                next_msg["content"] = _compact_message_content(
                    next_msg.get("content"),
                    max_chars=user_assistant_content_limit,
                )
            compacted_messages.append(next_msg)

        outgoing["messages"] = _repair_openai_tool_message_sequence(compacted_messages)

    tools = outgoing.get("tools")
    if isinstance(tools, list):
        filtered_tools = [tool for tool in tools if isinstance(tool, dict)]
        if compact_tool_schemas:
            filtered_tools = [
                _compact_openai_tool_schema(
                    tool,
                    aggressive=bool(aggressive_tool_schema_compaction),
                )
                for tool in filtered_tools
            ]
        outgoing["tools"] = filtered_tools
    return outgoing


def _serialize_tool_call_args_for_signature(raw_arguments: Any) -> str:
    parsed_args = _parse_tool_arguments(raw_arguments, none_on_failure=True)
    if isinstance(parsed_args, dict):
        try:
            return json.dumps(parsed_args, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        except Exception:
            pass
    if isinstance(raw_arguments, str):
        return raw_arguments.strip()
    return ""


def _tool_message_discovery_fingerprint(content: Any) -> str:
    if isinstance(content, str):
        normalized = " ".join(content.split())
        return normalized[:1200]
    parsed, details = _read_tool_message_content(content)
    if isinstance(parsed, dict):
        summary: dict[str, Any] = {}
        qmap_result = parsed.get("qmapToolResult")
        if isinstance(qmap_result, dict):
            summary["schema"] = qmap_result.get("schema")
            summary["success"] = qmap_result.get("success")
        llm_result = parsed.get("llmResult")
        if isinstance(llm_result, dict):
            summary["success"] = summary.get("success", llm_result.get("success"))
            provider_id = str(llm_result.get("providerId") or "").strip()
            dataset_id = str(llm_result.get("datasetId") or "").strip()
            if provider_id:
                summary["providerId"] = provider_id[:120]
            if dataset_id:
                summary["datasetId"] = dataset_id[:160]
            summary["datasetCount"] = (
                len(llm_result.get("datasets"))
                if isinstance(llm_result.get("datasets"), list)
                else None
            )
            summary["providerCount"] = (
                len(llm_result.get("providers"))
                if isinstance(llm_result.get("providers"), list)
                else None
            )
            providers_raw = llm_result.get("providers")
            if isinstance(providers_raw, list):
                provider_ids = sorted(
                    {
                        str(item.get("id") or "").strip()
                        for item in providers_raw
                        if isinstance(item, dict) and str(item.get("id") or "").strip()
                    }
                )
                if provider_ids:
                    summary["providerIds"] = provider_ids[:12]
            datasets_raw = llm_result.get("datasets")
            if isinstance(datasets_raw, list):
                dataset_ids = sorted(
                    {
                        str(item.get("id") or "").strip()
                        for item in datasets_raw
                        if isinstance(item, dict) and str(item.get("id") or "").strip()
                    }
                )
                if dataset_ids:
                    summary["datasetIds"] = dataset_ids[:20]
        if not summary:
            summary = {
                "success": parsed.get("success"),
                "detailsHint": str(parsed.get("details") or details or "")[:120],
            }
        try:
            return json.dumps(summary, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        except Exception:
            return str(summary)
    return str(details or "")[:1200]


def _deduplicate_discovery_tool_turns(
    payload: dict[str, Any],
    *,
    discovery_tools: set[str] | None = None,
) -> dict[str, Any]:
    outgoing = dict(payload or {})
    messages = outgoing.get("messages")
    if not isinstance(messages, list) or not messages:
        return outgoing

    discovery_tool_names = set(discovery_tools or _DISCOVERY_TOOLS_DEFAULT)

    indexed: list[tuple[int, dict[str, Any]]] = [
        (idx, dict(message))
        for idx, message in enumerate(messages)
        if isinstance(message, dict)
    ]
    if not indexed:
        return outgoing

    to_drop: set[int] = set()
    latest_by_signature: dict[str, int] = {}
    i = 0
    while i < len(indexed):
        original_idx, message = indexed[i]
        if str(message.get("role") or "").strip().lower() != "assistant":
            i += 1
            continue
        tool_calls = _extract_tool_calls_from_assistant_message(message)
        if not tool_calls:
            i += 1
            continue
        call_lookup: dict[str, tuple[str, str]] = {}
        discovery_call_ids: list[str] = []
        non_discovery = False
        for call in tool_calls:
            call_id = str(call.get("id") or "").strip()
            tool_name = str(call.get("name") or "").strip()
            if not call_id or not tool_name:
                non_discovery = True
                break
            if tool_name not in discovery_tool_names:
                non_discovery = True
                break
            fn = call.get("function")
            fn_dict = fn if isinstance(fn, dict) else {}
            args_sig = _serialize_tool_call_args_for_signature(
                fn_dict.get("parsedArguments", fn_dict.get("arguments"))
            )
            call_lookup[call_id] = (tool_name, args_sig)
            discovery_call_ids.append(call_id)
        if non_discovery or not discovery_call_ids:
            i += 1
            continue

        tool_segment: list[tuple[int, dict[str, Any]]] = []
        j = i + 1
        while j < len(indexed):
            next_idx, next_msg = indexed[j]
            if str(next_msg.get("role") or "").strip().lower() != "tool":
                break
            tool_call_id = str(next_msg.get("tool_call_id") or "").strip()
            if tool_call_id in call_lookup:
                tool_segment.append((next_idx, next_msg))
            j += 1
        if len(tool_segment) < len(discovery_call_ids):
            i = j
            continue

        signatures: list[str] = []
        all_success = True
        seen_call_ids: set[str] = set()
        for _, tool_msg in tool_segment:
            tool_call_id = str(tool_msg.get("tool_call_id") or "").strip()
            if tool_call_id not in call_lookup or tool_call_id in seen_call_ids:
                continue
            seen_call_ids.add(tool_call_id)
            tool_name, args_sig = call_lookup[tool_call_id]
            parsed, details = _read_tool_message_content(tool_msg.get("content"))
            success: bool | None = None
            if isinstance(parsed, dict):
                qmap_result = parsed.get("qmapToolResult")
                if isinstance(qmap_result, dict) and isinstance(qmap_result.get("success"), bool):
                    success = bool(qmap_result.get("success"))
                llm_result = parsed.get("llmResult")
                if success is None and isinstance(llm_result, dict) and isinstance(llm_result.get("success"), bool):
                    success = bool(llm_result.get("success"))
                if success is None and isinstance(parsed.get("success"), bool):
                    success = bool(parsed.get("success"))
            if success is None:
                success = _extract_success_from_text(details)
            if success is not True:
                all_success = False
                break
            signatures.append(
                "|".join(
                    (
                        tool_name,
                        args_sig,
                        _tool_message_discovery_fingerprint(tool_msg.get("content")),
                    )
                )
            )
        if not all_success or not signatures:
            i = j
            continue

        turn_signature = "||".join(sorted(signatures))
        previous_assistant_idx = latest_by_signature.get(turn_signature)
        if previous_assistant_idx is not None:
            to_drop.add(previous_assistant_idx)
            for drop_idx in range(previous_assistant_idx + 1, len(messages)):
                raw = messages[drop_idx]
                if not isinstance(raw, dict):
                    continue
                if str(raw.get("role") or "").strip().lower() != "tool":
                    break
                to_drop.add(drop_idx)
        latest_by_signature[turn_signature] = original_idx
        i = j

    if not to_drop:
        return outgoing
    compacted_messages: list[Any] = []
    for idx, message in enumerate(messages):
        if idx in to_drop:
            continue
        if isinstance(message, dict):
            compacted_messages.append(dict(message))
        else:
            compacted_messages.append(message)
    outgoing["messages"] = compacted_messages
    return outgoing


def _sanitize_google_schema(schema: Any) -> Any:
    if isinstance(schema, list):
        return [_sanitize_google_schema(item) for item in schema]
    if not isinstance(schema, dict):
        return schema

    rename_keys = {
        "any_of": "anyOf",
        "one_of": "oneOf",
        "all_of": "allOf",
        "min_items": "minItems",
        "max_items": "maxItems",
        "min_length": "minLength",
        "max_length": "maxLength",
        "additional_properties": "additionalProperties",
    }
    current: dict[str, Any] = {}
    for key, value in schema.items():
        mapped_key = rename_keys.get(key, key)
        if mapped_key == "properties":
            current[mapped_key] = value
        else:
            current[mapped_key] = _sanitize_google_schema(value)

    def _as_string_name(value: Any) -> str | None:
        if isinstance(value, str):
            return value
        if isinstance(value, dict):
            for key in ("key", "name", "string_value", "stringValue"):
                candidate = value.get(key)
                if isinstance(candidate, str) and candidate:
                    return candidate
        return None

    props = current.get("properties")
    if isinstance(props, list):
        mapped_props: dict[str, Any] = {}
        for item in props:
            if not isinstance(item, dict):
                continue
            key = item.get("key")
            value = item.get("value")
            if isinstance(key, str):
                mapped_props[key] = _sanitize_google_schema(value)
        current["properties"] = mapped_props
    elif isinstance(props, dict):
        current["properties"] = {
            str(k): _sanitize_google_schema(v) for k, v in props.items() if isinstance(k, str)
        }

    union = current.get("anyOf") or current.get("oneOf") or current.get("allOf")
    if isinstance(union, list) and union:
        non_null = []
        has_null = False
        for option in union:
            if isinstance(option, dict) and option.get("type") == "null":
                has_null = True
            else:
                non_null.append(option)
        chosen = _sanitize_google_schema(non_null[0] if non_null else {"type": "string"})
        if isinstance(chosen, dict) and has_null:
            chosen["nullable"] = True
        return chosen

    value_type = current.get("type")
    if isinstance(value_type, list):
        filtered = [t for t in value_type if t != "null"]
        current["type"] = filtered[0] if filtered else "string"
        if "null" in value_type:
            current["nullable"] = True

    items = current.get("items")
    if isinstance(items, list):
        current["items"] = _sanitize_google_schema(items[0] if items else {"type": "string"})

    if current.get("type") is None:
        if isinstance(current.get("properties"), dict):
            current["type"] = "object"
        elif "items" in current:
            current["type"] = "array"

    required = current.get("required")
    if isinstance(required, list):
        normalized_required = []
        for entry in required:
            name = _as_string_name(entry)
            if name:
                normalized_required.append(name)
        properties_map = current.get("properties")
        if isinstance(properties_map, dict) and normalized_required:
            normalized_required = [name for name in normalized_required if name in properties_map]
        current["required"] = normalized_required

    allowed = {
        "type",
        "format",
        "description",
        "nullable",
        "enum",
        "items",
        "properties",
        "required",
        "minItems",
        "maxItems",
        "minimum",
        "maximum",
        "minLength",
        "maxLength",
        "title",
        "default",
        "additionalProperties",
    }
    return {k: v for k, v in current.items() if k in allowed}


def _sanitize_openai_tools_for_gemini_model(payload: dict[str, Any], *, model_hint: str | None) -> dict[str, Any]:
    model = str(model_hint or "").strip().lower()
    if "gemini" not in model:
        return payload

    outgoing = dict(payload or {})
    tools = outgoing.get("tools")
    if not isinstance(tools, list):
        return outgoing

    sanitized_tools: list[dict[str, Any]] = []
    for tool in tools:
        if not isinstance(tool, dict):
            continue
        next_tool = dict(tool)
        function_obj = next_tool.get("function")
        if isinstance(function_obj, dict):
            next_fn = dict(function_obj)
            params = next_fn.get("parameters")
            if isinstance(params, dict):
                next_fn["parameters"] = _sanitize_google_schema(params)
            next_tool["function"] = next_fn
        sanitized_tools.append(next_tool)
    outgoing["tools"] = sanitized_tools
    return outgoing
