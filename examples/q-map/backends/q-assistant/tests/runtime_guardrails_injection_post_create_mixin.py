import json

from tests.runtime_guardrails_test_support import (
    _inject_runtime_guardrail_message,
    _qmap_tool_result,
)


class RuntimeGuardrailInjectionPostCreateMixin:
    def test_injects_post_create_wait_and_count_guidance(self):
        payload = {
            "messages": [
                {"role": "system", "content": "Regole di sistema"},
                {"role": "user", "content": "tassella e integra CLC"},
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_join",
                            "type": "function",
                            "function": {
                                "name": "joinQMapDatasetsOnH3",
                                "arguments": json.dumps({"newDatasetName": "Regioni_H3_5_CLC_Joined"}),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_join",
                    "content": _qmap_tool_result(success=True, details="Join completed."),
                },
            ],
            "tools": [
                {"type": "function", "function": {"name": "waitForQMapDataset"}},
                {"type": "function", "function": {"name": "countQMapRows"}},
            ],
        }
        out = _inject_runtime_guardrail_message(payload)
        content = str(out["messages"][0]["content"])
        self.assertIn("[RUNTIME_GUARDRAIL]", content)
        self.assertIn("waitForQMapDataset", content)
        self.assertIn("countQMapRows", content)
        self.assertIn("Do not call countQMapRows in the same tool response", content)
        self.assertIn('datasetName="Regioni_H3_5_CLC_Joined"', content)

    def test_requires_count_after_wait_step(self):
        payload = {
            "messages": [
                {"role": "system", "content": "Regole di sistema"},
                {"role": "user", "content": "continua"},
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_join",
                            "type": "function",
                            "function": {"name": "joinQMapDatasetsOnH3", "arguments": "{}"},
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_join",
                    "content": _qmap_tool_result(success=True, details="Join completed."),
                },
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_wait",
                            "type": "function",
                            "function": {
                                "name": "waitForQMapDataset",
                                "arguments": json.dumps({"datasetName": "Regioni_H3_5_CLC_Joined"}),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_wait",
                    "content": _qmap_tool_result(
                        success=True, details='Dataset "Regioni_H3_5_CLC_Joined" is available (1406 rows).'
                    ),
                },
            ],
            "tools": [{"type": "function", "function": {"name": "countQMapRows"}}],
        }
        out = _inject_runtime_guardrail_message(payload)
        content = str(out["messages"][0]["content"])
        self.assertIn("[RUNTIME_GUARDRAIL]", content)
        self.assertIn("countQMapRows", content)
        self.assertIn('datasetName="Regioni_H3_5_CLC_Joined"', content)

    def test_recovers_cached_isochrone_with_save_data_before_wait_retry(self):
        payload = {
            "messages": [
                {"role": "system", "content": "Regole di sistema"},
                {"role": "user", "content": "puoi disegnare anche raggio?"},
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_isochrone",
                            "type": "function",
                            "function": {
                                "name": "isochrone",
                                "arguments": json.dumps(
                                    {
                                        "origin": {"longitude": 9.1896, "latitude": 45.4641},
                                        "distanceLimit": 1500,
                                        "profile": "walking",
                                    }
                                ),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_isochrone",
                    "content": json.dumps(
                        {
                            "success": True,
                            "datasetName": "isochrone_1m9ts56tl0z",
                            "details": 'Tool "isochrone" completed.',
                        }
                    ),
                },
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_wait",
                            "type": "function",
                            "function": {
                                "name": "waitForQMapDataset",
                                "arguments": json.dumps({"datasetName": "isochrone_1m9ts56tl0z"}),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_wait",
                    "content": _qmap_tool_result(
                        success=False,
                        details='Dataset "isochrone_1m9ts56tl0z" not found.',
                    ),
                },
            ],
            "tools": [
                {"type": "function", "function": {"name": "saveDataToMap"}},
                {"type": "function", "function": {"name": "waitForQMapDataset"}},
                {"type": "function", "function": {"name": "countQMapRows"}},
            ],
        }
        out = _inject_runtime_guardrail_message(payload)
        content = str(out["messages"][0]["content"])
        self.assertIn("save_cached_dataset_before_wait", content)
        self.assertIn('saveDataToMap(datasetNames=["isochrone_1m9ts56tl0z"])', content)
        self.assertIn('waitForQMapDataset(datasetName="isochrone_1m9ts56tl0z", timeoutMs=60000)', content)
        self.assertIn("Do not rerun isochrone unless saveDataToMap fails.", content)

    def test_requires_show_only_after_count_for_final_dataset(self):
        payload = {
            "messages": [
                {"role": "system", "content": "Regole di sistema"},
                {"role": "user", "content": "mostra solo il layer finale"},
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_join",
                            "type": "function",
                            "function": {
                                "name": "joinQMapDatasetsOnH3",
                                "arguments": json.dumps({"newDatasetName": "Celle_H3_Veneto_Perimetro"}),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_join",
                    "content": _qmap_tool_result(success=True, details="Join completed."),
                },
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_wait",
                            "type": "function",
                            "function": {
                                "name": "waitForQMapDataset",
                                "arguments": json.dumps({"datasetName": "Celle_H3_Veneto_Perimetro"}),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_wait",
                    "content": _qmap_tool_result(
                        success=True,
                        details='Dataset "Celle_H3_Veneto_Perimetro" is available (87603 rows).',
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
                                "arguments": json.dumps({"datasetName": "Celle_H3_Veneto_Perimetro"}),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_count",
                    "content": _qmap_tool_result(success=True, details='Dataset "Celle_H3_Veneto_Perimetro" has 87603 rows.'),
                },
            ],
            "tools": [{"type": "function", "function": {"name": "showOnlyQMapLayer"}}],
        }
        out = _inject_runtime_guardrail_message(payload)
        content = str(out["messages"][0]["content"])
        self.assertIn("final layer isolation is still missing", content)
        self.assertIn("showOnlyQMapLayer", content)
        self.assertIn('datasetName="Celle_H3_Veneto_Perimetro"', content)

    def test_does_not_require_show_only_after_count_when_isolation_not_requested(self):
        payload = {
            "messages": [
                {"role": "system", "content": "Regole di sistema"},
                {"role": "user", "content": "Tassella a H3, aggrega e fai join su H3 per ottenere un dataset finale."},
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_join",
                            "type": "function",
                            "function": {
                                "name": "joinQMapDatasetsOnH3",
                                "arguments": json.dumps({"newDatasetName": "H3_Join_Output"}),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_join",
                    "content": _qmap_tool_result(success=True, details='Dataset "H3_Join_Output" created.'),
                },
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_wait",
                            "type": "function",
                            "function": {
                                "name": "waitForQMapDataset",
                                "arguments": json.dumps({"datasetName": "H3_Join_Output"}),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_wait",
                    "content": _qmap_tool_result(success=True, details='Dataset "H3_Join_Output" is available (100 rows).'),
                },
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_count",
                            "type": "function",
                            "function": {"name": "countQMapRows", "arguments": json.dumps({"datasetName": "H3_Join_Output"})},
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_count",
                    "content": _qmap_tool_result(success=True, details='Dataset "H3_Join_Output" has 100 rows.'),
                },
            ],
            "tools": [{"type": "function", "function": {"name": "showOnlyQMapLayer"}}],
        }
        out = _inject_runtime_guardrail_message(payload)
        content = str(out["messages"][0]["content"])
        self.assertNotIn("post_count_isolate_final", content)
        self.assertNotIn("showOnlyQMapLayer", content)

    def test_prefers_dataset_ref_when_available(self):
        payload = {
            "messages": [
                {"role": "system", "content": "Regole di sistema"},
                {"role": "user", "content": "continua"},
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_join",
                            "type": "function",
                            "function": {"name": "joinQMapDatasetsOnH3", "arguments": "{}"},
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_join",
                    "content": json.dumps(
                        {
                            "qmapToolResult": {
                                "schema": "qmap.tool_result.v1",
                                "success": True,
                                "details": "Join completed.",
                            },
                            "llmResult": {
                                "success": True,
                                "loadedDatasetRef": "id:joined_h3_r6",
                                "loadedDatasetName": "Joined H3 r6",
                            },
                        }
                    ),
                },
            ],
            "tools": [
                {"type": "function", "function": {"name": "waitForQMapDataset"}},
                {"type": "function", "function": {"name": "countQMapRows"}},
            ],
        }
        out = _inject_runtime_guardrail_message(payload)
        content = str(out["messages"][0]["content"])
        self.assertIn('datasetName="id:joined_h3_r6"', content)
