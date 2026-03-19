import json

from tests.runtime_guardrails_test_support import (
    _enforce_runtime_tool_loop_limits,
    _qmap_tool_result,
)


def _superlative_payload_after_count(*, user_prompt, extra_tools=None):
    """Build a payload that has completed query→rank→filter→wait→count."""
    tools = [
        {"type": "function", "function": {"name": "fitQMapToDataset"}},
        {"type": "function", "function": {"name": "showOnlyQMapLayer"}},
        {"type": "function", "function": {"name": "rankQMapDatasetRows"}},
    ]
    if extra_tools:
        tools.extend(extra_tools)
    return {
        "messages": [
            {"role": "system", "content": "System"},
            {"role": "user", "content": user_prompt},
            {
                "role": "assistant",
                "tool_calls": [
                    {
                        "id": "call_query",
                        "type": "function",
                        "function": {
                            "name": "queryQCumberTerritorialUnits",
                            "arguments": json.dumps(
                                {
                                    "providerId": "local-assets-it",
                                    "datasetId": "kontur-boundaries-italia",
                                }
                            ),
                        },
                    }
                ],
            },
            {
                "role": "tool",
                "tool_call_id": "call_query",
                "content": json.dumps(
                    {
                        "success": True,
                        "datasetName": "Kontur boundaries Italia (query) [abc123]",
                        "datasetRef": "id:kontur-query-abc123",
                        "details": "Query completed.",
                    }
                ),
            },
            {
                "role": "assistant",
                "tool_calls": [
                    {
                        "id": "call_rank",
                        "type": "function",
                        "function": {
                            "name": "rankQMapDatasetRows",
                            "arguments": json.dumps(
                                {
                                    "datasetName": "id:kontur-query-abc123",
                                    "metricFieldName": "shape_area",
                                    "sortDirection": "asc",
                                    "topN": 1,
                                }
                            ),
                        },
                    }
                ],
            },
            {
                "role": "tool",
                "tool_call_id": "call_rank",
                "content": _qmap_tool_result(success=True, details="Ranking completed."),
            },
            {
                "role": "assistant",
                "tool_calls": [
                    {
                        "id": "call_filter",
                        "type": "function",
                        "function": {
                            "name": "createDatasetFromFilter",
                            "arguments": json.dumps(
                                {
                                    "sourceDatasetName": "id:kontur-query-abc123",
                                    "newDatasetName": "comune_piu_piccolo",
                                    "fieldName": "name",
                                    "op": "eq",
                                    "value": "Portobuffole",
                                    "showOnMap": True,
                                }
                            ),
                        },
                    }
                ],
            },
            {
                "role": "tool",
                "tool_call_id": "call_filter",
                "content": _qmap_tool_result(success=True, details="Dataset filtered."),
            },
            {
                "role": "assistant",
                "tool_calls": [
                    {
                        "id": "call_wait",
                        "type": "function",
                        "function": {
                            "name": "waitForQMapDataset",
                            "arguments": json.dumps({"datasetName": "comune_piu_piccolo"}),
                        },
                    }
                ],
            },
            {
                "role": "tool",
                "tool_call_id": "call_wait",
                "content": _qmap_tool_result(
                    success=True, details='Dataset "comune_piu_piccolo" is available (1 rows).'
                ),
            },
            {
                "role": "assistant",
                "tool_calls": [
                    {
                        "id": "call_count",
                        "type": "function",
                        "function": {
                            "name": "countQMapRows",
                            "arguments": json.dumps({"datasetName": "comune_piu_piccolo"}),
                        },
                    }
                ],
            },
            {
                "role": "tool",
                "tool_call_id": "call_count",
                "content": _qmap_tool_result(
                    success=True, details='Dataset "comune_piu_piccolo": 1 rows.'
                ),
            },
        ],
        "tools": tools,
        "tool_choice": "auto",
    }


class RuntimeGuardrailPostFilterForceFitMixin:
    def test_forces_fit_after_superlative_filter_wait_count(self):
        payload = _superlative_payload_after_count(
            user_prompt="mostra il comune piu piccolo della provincia di treviso sulla mappa",
        )
        out = _enforce_runtime_tool_loop_limits(payload)
        self.assertEqual(
            out.get("tool_choice"),
            {"type": "function", "function": {"name": "fitQMapToDataset"}},
        )
        remaining = {
            t["function"]["name"]
            for t in out.get("tools", [])
            if isinstance(t, dict) and isinstance(t.get("function"), dict)
        }
        self.assertIn("fitQMapToDataset", remaining)
        self.assertNotIn("showOnlyQMapLayer", remaining)
        self.assertNotIn("rankQMapDatasetRows", remaining)

    def test_does_not_force_fit_when_fit_already_succeeded(self):
        payload = _superlative_payload_after_count(
            user_prompt="mostra il comune piu piccolo della provincia di treviso sulla mappa",
        )
        payload["messages"].extend(
            [
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_fit",
                            "type": "function",
                            "function": {
                                "name": "fitQMapToDataset",
                                "arguments": json.dumps({"datasetName": "comune_piu_piccolo"}),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_fit",
                    "content": _qmap_tool_result(success=True, details="Map fitted."),
                },
            ]
        )
        out = _enforce_runtime_tool_loop_limits(payload)
        self.assertNotEqual(
            out.get("tool_choice"),
            {"type": "function", "function": {"name": "fitQMapToDataset"}},
        )

    def test_does_not_force_fit_without_rank_before_filter(self):
        """If there is no rank step before filter, the rule should not fire."""
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {"role": "user", "content": "filtra e mostra sulla mappa"},
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_filter",
                            "type": "function",
                            "function": {
                                "name": "createDatasetFromFilter",
                                "arguments": json.dumps(
                                    {
                                        "sourceDatasetName": "test",
                                        "newDatasetName": "filtered",
                                        "fieldName": "name",
                                        "op": "eq",
                                        "value": "X",
                                    }
                                ),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_filter",
                    "content": _qmap_tool_result(success=True, details="Filtered."),
                },
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_wait",
                            "type": "function",
                            "function": {
                                "name": "waitForQMapDataset",
                                "arguments": json.dumps({"datasetName": "filtered"}),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_wait",
                    "content": _qmap_tool_result(success=True, details="Available."),
                },
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_count",
                            "type": "function",
                            "function": {
                                "name": "countQMapRows",
                                "arguments": json.dumps({"datasetName": "filtered"}),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_count",
                    "content": _qmap_tool_result(success=True, details="1 rows."),
                },
            ],
            "tools": [
                {"type": "function", "function": {"name": "fitQMapToDataset"}},
                {"type": "function", "function": {"name": "showOnlyQMapLayer"}},
            ],
            "tool_choice": "auto",
        }
        out = _enforce_runtime_tool_loop_limits(payload)
        tc = out.get("tool_choice")
        if isinstance(tc, dict):
            fn = tc.get("function", {})
            self.assertNotEqual(fn.get("name"), "fitQMapToDataset")

    def test_forces_fit_with_workflow_signal_only(self):
        payload = _superlative_payload_after_count(
            user_prompt="continua",
        )
        out = _enforce_runtime_tool_loop_limits(payload)
        self.assertEqual(
            out.get("tool_choice"),
            {"type": "function", "function": {"name": "fitQMapToDataset"}},
        )
