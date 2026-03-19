from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any


_QMAP_TOOL_RESULT_SCHEMA = "qmap.tool_result.v1"
_QMAP_TOOL_CONTRACT_SCHEMA = "qmap.tool_contracts.v1"
_QMAP_TOOL_CONTRACTS_LOCAL_PATH = Path(__file__).with_name("qmap-tool-contracts.json")
_qmap_tool_contract_manifest_cache: dict[str, Any] | None = None
_SUCCESS_ONLY_REQUIRED_FIELDS = {
    "dataset",
    "datasetName",
    "datasetId",
    "datasetRef",
    "loadedDatasetName",
    "loadedDatasetRef",
    "outputFieldName",
    "fieldCatalog",
    "numericFields",
    "styleableFields",
    "defaultStyleField",
    "aggregationOutputs",
    "fieldAliases",
    "datasets",
    "layers",
}


def _load_qmap_tool_contract_manifest() -> dict[str, Any]:
    global _qmap_tool_contract_manifest_cache
    if isinstance(_qmap_tool_contract_manifest_cache, dict):
        return _qmap_tool_contract_manifest_cache

    fallback = {
        "schema": _QMAP_TOOL_CONTRACT_SCHEMA,
        "defaults": {
            "responseContract": {
                "schema": _QMAP_TOOL_RESULT_SCHEMA,
            }
        },
        "tools": {},
    }

    contract_path_raw = str(os.getenv("Q_ASSISTANT_TOOL_CONTRACTS_PATH") or "").strip()
    contract_path = Path(contract_path_raw) if contract_path_raw else _QMAP_TOOL_CONTRACTS_LOCAL_PATH
    if not contract_path_raw:
        module_path = Path(__file__).resolve()
        for parent in module_path.parents:
            shared_candidate = parent / "artifacts" / "tool-contracts" / "qmap-tool-contracts.json"
            if shared_candidate.is_file():
                contract_path = shared_candidate
                break

    try:
        raw = json.loads(contract_path.read_text(encoding="utf-8"))
    except Exception:
        _qmap_tool_contract_manifest_cache = fallback
        return _qmap_tool_contract_manifest_cache

    if str(raw.get("schema") or "").strip() != _QMAP_TOOL_CONTRACT_SCHEMA:
        _qmap_tool_contract_manifest_cache = fallback
        return _qmap_tool_contract_manifest_cache

    defaults = raw.get("defaults")
    defaults_dict = defaults if isinstance(defaults, dict) else {}
    tools_raw = raw.get("tools")
    tools_dict = tools_raw if isinstance(tools_raw, dict) else {}
    normalized_tools: dict[str, dict[str, Any]] = {}
    for name, row in tools_dict.items():
        tool_name = str(name or "").strip()
        if not tool_name:
            continue
        if isinstance(row, dict):
            normalized_tools[tool_name] = dict(row)
        else:
            normalized_tools[tool_name] = {}

    _qmap_tool_contract_manifest_cache = {
        "schema": _QMAP_TOOL_CONTRACT_SCHEMA,
        "defaults": defaults_dict,
        "tools": normalized_tools,
    }
    return _qmap_tool_contract_manifest_cache


def _get_qmap_tool_contract_entry(tool_name: Any) -> dict[str, Any] | None:
    normalized_name = str(tool_name or "").strip()
    if not normalized_name:
        return None
    manifest = _load_qmap_tool_contract_manifest()
    tools = manifest.get("tools")
    if not isinstance(tools, dict):
        return None
    row = tools.get(normalized_name)
    if isinstance(row, dict):
        return row
    return None


def _get_qmap_tool_response_schema(tool_name: Any) -> str:
    response_contract = _get_qmap_tool_response_contract(tool_name)
    if not isinstance(response_contract, dict):
        return ""
    return str(response_contract.get("schema") or "").strip()


def _normalize_qmap_tool_response_contract(raw: Any) -> dict[str, Any]:
    row = raw if isinstance(raw, dict) else {}
    properties = row.get("properties")
    return {
        "schema": str(row.get("schema") or _QMAP_TOOL_RESULT_SCHEMA).strip() or _QMAP_TOOL_RESULT_SCHEMA,
        "properties": properties if isinstance(properties, dict) else {},
        "required": [
            str(item or "").strip()
            for item in row.get("required", [])
            if str(item or "").strip()
        ]
        if isinstance(row.get("required"), list)
        else [],
        "allowAdditionalProperties": bool(row.get("allowAdditionalProperties", True)),
    }


def _get_qmap_tool_response_contract(tool_name: Any) -> dict[str, Any] | None:
    manifest = _load_qmap_tool_contract_manifest()
    defaults = manifest.get("defaults")
    defaults_dict = defaults if isinstance(defaults, dict) else {}
    default_response = defaults_dict.get("responseContract")
    default_response_dict = _normalize_qmap_tool_response_contract(default_response)

    row = _get_qmap_tool_contract_entry(tool_name)
    if not isinstance(row, dict):
        return default_response_dict
    response_contract = row.get("responseContract")
    if not isinstance(response_contract, dict):
        return default_response_dict
    normalized = _normalize_qmap_tool_response_contract(response_contract)
    if not normalized.get("properties") and default_response_dict.get("properties"):
        normalized["properties"] = dict(default_response_dict.get("properties") or {})
    if not normalized.get("required") and default_response_dict.get("required"):
        normalized["required"] = list(default_response_dict.get("required") or [])
    if "allowAdditionalProperties" not in normalized:
        normalized["allowAdditionalProperties"] = bool(default_response_dict.get("allowAdditionalProperties", True))
    return normalized


def _validate_qmap_tool_result_payload(tool_name: Any, payload: Any) -> list[str]:
    contract = _get_qmap_tool_response_contract(tool_name)
    if not isinstance(contract, dict):
        return []
    if not isinstance(payload, dict):
        return ["payload is not an object"]

    errors: list[str] = []
    required_fields = contract.get("required")
    is_failure_payload = payload.get("success") is False
    if isinstance(required_fields, list):
        for field_name in required_fields:
            normalized_name = str(field_name or "").strip()
            if is_failure_payload and normalized_name in _SUCCESS_ONLY_REQUIRED_FIELDS:
                continue
            if normalized_name and normalized_name not in payload:
                errors.append(f'missing required field "{normalized_name}"')

    properties = contract.get("properties")
    if not isinstance(properties, dict):
        return errors

    def _value_matches_schema(value: Any, schema: Any) -> bool:
        if not isinstance(schema, dict):
            return True
        schema_type = str(schema.get("type") or "").strip()
        if schema_type == "string":
            return isinstance(value, str)
        if schema_type == "boolean":
            return isinstance(value, bool)
        if schema_type == "array":
            if not isinstance(value, list):
                return False
            item_schema = schema.get("items")
            return all(_value_matches_schema(item, item_schema) for item in value)
        if schema_type == "object":
            if not isinstance(value, dict):
                return False
            additional_schema = schema.get("additionalProperties")
            if isinstance(additional_schema, dict):
                return all(_value_matches_schema(item, additional_schema) for item in value.values())
            return True
        return True

    for field_name, field_schema in properties.items():
        normalized_name = str(field_name or "").strip()
        if not normalized_name or normalized_name not in payload:
            continue
        if not _value_matches_schema(payload.get(normalized_name), field_schema):
            expected_type = str(field_schema.get("type") or "unknown").strip() if isinstance(field_schema, dict) else "unknown"
            errors.append(f'field "{normalized_name}" expected type {expected_type}')

    return errors
