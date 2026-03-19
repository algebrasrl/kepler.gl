import json

from tests.runtime_guardrails_test_support import (
    _inject_runtime_guardrail_message,
    _qmap_tool_result,
)


class RuntimeGuardrailInjectionMetricRecoveryMixin:
    def test_blocks_identical_color_retry_after_low_distinct_failure(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {"role": "user", "content": "colora per numero livelli"},
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_color",
                            "type": "function",
                            "function": {
                                "name": "setQMapLayerColorByField",
                                "arguments": json.dumps(
                                    {"datasetName": "query_jxx7313oayp", "fieldName": "num_livelli_clc"}
                                ),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_color",
                    "content": _qmap_tool_result(
                        success=False,
                        details=(
                            'Field "num_livelli_clc" has <=1 distinct numeric value in sampled rows, '
                            "so color scale would appear uniform."
                        ),
                    ),
                },
            ],
            "tools": [
                {"type": "function", "function": {"name": "previewQMapDatasetRows"}},
                {"type": "function", "function": {"name": "distinctQMapFieldValues"}},
                {"type": "function", "function": {"name": "setQMapLayerColorByField"}},
            ],
        }
        out = _inject_runtime_guardrail_message(payload)
        content = str(out["messages"][0]["content"])
        self.assertIn("[RUNTIME_GUARDRAIL]", content)
        self.assertIn("color_low_distinct_no_false_success", content)
        self.assertIn("Do not run alternate heavy pipelines", content)
        self.assertIn("Do not claim that color-by-field was applied", content)

    def test_recovers_missing_metric_field_with_dataset_preview_and_retry(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {"role": "user", "content": "mostra regioni con più problemi e grafici"},
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_zonal",
                            "type": "function",
                            "function": {
                                "name": "zonalStatsByAdmin",
                                "arguments": json.dumps(
                                    {
                                        "newDatasetName": "Regioni_Pressione_Ambientale_Zonal",
                                        "outputFieldName": "area_pressione_ha",
                                    }
                                ),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_zonal",
                    "content": _qmap_tool_result(success=True, details="Computing zonal stats completed."),
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
                                        "datasetName": "Regioni_Pressione_Ambientale_Zonal",
                                        "metricFieldName": "join_sum_area_ha",
                                    }
                                ),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_rank",
                    "content": _qmap_tool_result(
                        success=False,
                        details='Metric field "join_sum_area_ha" not found in dataset "Regioni_Pressione_Ambientale_Zonal".',
                    ),
                },
            ],
            "tools": [
                {"type": "function", "function": {"name": "previewQMapDatasetRows"}},
                {"type": "function", "function": {"name": "rankQMapDatasetRows"}},
            ],
        }
        out = _inject_runtime_guardrail_message(payload)
        content = str(out["messages"][0]["content"])
        self.assertIn('metric field "join_sum_area_ha" was not found', content)
        self.assertIn("previewQMapDatasetRows", content)
        self.assertIn('metricFieldName="area_pressione_ha"', content)
        self.assertIn("Do not switch to population/name fallback metrics", content)

    def test_problem_objective_blocks_population_name_fallback_on_color_recovery(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {"role": "user", "content": "mostrami la regione con più problemi ambientali"},
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_color",
                            "type": "function",
                            "function": {
                                "name": "setQMapLayerColorByField",
                                "arguments": json.dumps({"datasetName": "Regioni_Pressione_Ambientale", "fieldName": "name"}),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_color",
                    "content": _qmap_tool_result(
                        success=False,
                        details='Field "name" has <=1 distinct numeric value in sampled rows, so color scale would appear uniform.',
                    ),
                },
            ],
            "tools": [
                {"type": "function", "function": {"name": "previewQMapDatasetRows"}},
                {"type": "function", "function": {"name": "distinctQMapFieldValues"}},
            ],
        }
        out = _inject_runtime_guardrail_message(payload)
        content = str(out["messages"][0]["content"])
        self.assertIn("Do not replace the analytical metric with population/name", content)

    def test_color_missing_field_after_spatial_join_suggests_preview_and_join_sum_reuse(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {"role": "user", "content": "colorami le province italiane secondo i livelli pericolosi che intersecano"},
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_join",
                            "type": "function",
                            "function": {
                                "name": "spatialJoinByPredicate",
                                "arguments": json.dumps(
                                    {
                                        "leftDatasetName": "Kontur Boundaries Italia (query) [t7sj18]",
                                        "rightDatasetName": "CLC 2018 Italia (query) [izcnma]",
                                        "rightValueField": "area_ha",
                                        "aggregations": ["count", "sum"],
                                        "newDatasetName": "Province_Pressione_Ambientale",
                                    }
                                ),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_join",
                    "content": _qmap_tool_result(success=True, details="Spatial join completed."),
                },
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_color",
                            "type": "function",
                            "function": {
                                "name": "setQMapLayerColorByField",
                                "arguments": json.dumps(
                                    {
                                        "datasetName": "Province_Pressione_Ambientale",
                                        "fieldName": "sum_area_ha",
                                    }
                                ),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_color",
                    "content": _qmap_tool_result(
                        success=False,
                        details='Field "sum_area_ha" not found in dataset "Province_Pressione_Ambientale".',
                    ),
                },
            ],
            "tools": [
                {"type": "function", "function": {"name": "previewQMapDatasetRows"}},
                {"type": "function", "function": {"name": "setQMapLayerColorByField"}},
            ],
        }
        out = _inject_runtime_guardrail_message(payload)
        content = str(out["messages"][0]["content"])
        self.assertIn('metric field "sum_area_ha" was not found', content)
        self.assertIn("previewQMapDatasetRows", content)
        self.assertIn('fieldName="join_sum"', content)
        self.assertIn("Do not rerun query/join recompute steps", content)

    def test_normalized_coloring_requires_derived_metric_field(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {"role": "user", "content": "normalizza per popolazione e colora in scala di rossi"},
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_color",
                            "type": "function",
                            "function": {
                                "name": "setQMapLayerColorByField",
                                "arguments": json.dumps(
                                    {
                                        "datasetName": "Regioni_Pressione_Ambientale",
                                        "fieldName": "area_pressione_ha",
                                    }
                                ),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_color",
                    "content": _qmap_tool_result(
                        success=True,
                        details='Applying quantile color scale on layer "Regioni_Pressione_Ambientale" using field "area_pressione_ha".',
                    ),
                },
            ],
            "tools": [
                {"type": "function", "function": {"name": "createDatasetWithNormalizedField"}},
                {"type": "function", "function": {"name": "waitForQMapDataset"}},
                {"type": "function", "function": {"name": "countQMapRows"}},
                {"type": "function", "function": {"name": "setQMapLayerColorByField"}},
            ],
        }
        out = _inject_runtime_guardrail_message(payload)
        content = str(out["messages"][0]["content"])
        self.assertIn("normalized_color_requires_derived_metric", content)
        self.assertIn("createDatasetWithNormalizedField", content)
        self.assertIn("non-normalized field", content)

    def test_normalized_coloring_accepts_already_normalized_field(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {"role": "user", "content": "normalizza per popolazione e colora in scala di rossi"},
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_color",
                            "type": "function",
                            "function": {
                                "name": "setQMapLayerColorByField",
                                "arguments": json.dumps(
                                    {
                                        "datasetName": "Regioni_Pressione_Ambientale_norm",
                                        "fieldName": "area_pressione_ha_per_100k",
                                    }
                                ),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_color",
                    "content": _qmap_tool_result(
                        success=True,
                        details='Applying quantile color scale on layer "Regioni_Pressione_Ambientale_norm" using field "area_pressione_ha_per_100k".',
                    ),
                },
            ],
            "tools": [
                {"type": "function", "function": {"name": "createDatasetWithNormalizedField"}},
                {"type": "function", "function": {"name": "setQMapLayerColorByField"}},
            ],
        }
        out = _inject_runtime_guardrail_message(payload)
        content = str(out["messages"][0]["content"])
        self.assertNotIn("normalized_color_requires_derived_metric", content)
