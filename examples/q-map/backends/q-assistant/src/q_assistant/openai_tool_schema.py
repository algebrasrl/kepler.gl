from __future__ import annotations

from typing import Any


def _coerce_openai_tools(tools: Any) -> Any:
    def _as_required_name(entry: Any) -> str | None:
        if isinstance(entry, str):
            name = entry.strip()
            return name or None
        if isinstance(entry, dict):
            for key in ("key", "name", "string_value", "stringValue"):
                candidate = entry.get(key)
                if isinstance(candidate, str) and candidate.strip():
                    return candidate.strip()
        return None

    def _normalize_openai_schema_required(schema: Any) -> Any:
        if isinstance(schema, list):
            return [_normalize_openai_schema_required(item) for item in schema]
        if not isinstance(schema, dict):
            return schema

        out: dict[str, Any] = {
            str(key): _normalize_openai_schema_required(value) for key, value in schema.items()
        }

        properties = out.get("properties")
        if isinstance(properties, list):
            mapped: dict[str, Any] = {}
            for item in properties:
                if not isinstance(item, dict):
                    continue
                key = item.get("key")
                value = item.get("value")
                if isinstance(key, str) and key:
                    mapped[key] = _normalize_openai_schema_required(value)
            out["properties"] = mapped
        elif isinstance(properties, dict):
            out["properties"] = {
                str(key): _normalize_openai_schema_required(value)
                for key, value in properties.items()
                if isinstance(key, str)
            }

        required = out.get("required")
        if isinstance(required, list):
            normalized_required = []
            for entry in required:
                name = _as_required_name(entry)
                if name:
                    normalized_required.append(name)
            properties_map = out.get("properties")
            if isinstance(properties_map, dict) and normalized_required:
                normalized_required = [name for name in normalized_required if name in properties_map]
            if normalized_required:
                out["required"] = normalized_required
            else:
                out.pop("required", None)

        return out

    def _sanitize_openai_tool_schema_for_compat(tool: dict[str, Any]) -> dict[str, Any]:
        next_tool = dict(tool or {})
        fn = next_tool.get("function")
        if not isinstance(fn, dict):
            return next_tool
        next_fn = dict(fn)
        parameters = next_fn.get("parameters")
        if isinstance(parameters, (dict, list)):
            next_fn["parameters"] = _normalize_openai_schema_required(parameters)
        next_tool["function"] = next_fn
        return next_tool

    if isinstance(tools, list):
        return [
            _sanitize_openai_tool_schema_for_compat(tool) if isinstance(tool, dict) else tool
            for tool in tools
        ]
    if not isinstance(tools, dict):
        return tools
    normalized: list[dict[str, Any]] = []
    for name, tool in tools.items():
        if not isinstance(name, str) or not name:
            continue
        description = ""
        parameters = {"type": "object", "properties": {}, "additionalProperties": True}
        if isinstance(tool, dict):
            desc = tool.get("description")
            if isinstance(desc, str):
                description = desc
            candidate = tool.get("parameters")
            if isinstance(candidate, dict) and candidate.get("type") == "object":
                parameters = candidate
        normalized.append(
            _sanitize_openai_tool_schema_for_compat(
                {
                    "type": "function",
                    "function": {
                        "name": name,
                        "description": description,
                        "parameters": parameters,
                    },
                }
            )
        )
    return normalized
