import json

from tests.runtime_guardrails_test_support import (
    _inject_runtime_guardrail_message,
    _qmap_tool_result,
)


class RuntimeGuardrailInjectionRankingEvidenceMixin:
    def test_ranking_objective_requires_ranked_evidence_in_finalization(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {"role": "user", "content": "mostrami le regioni con più problemi e grafici"},
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
                                        "datasetName": "Regioni_Pressione_Ambientale",
                                        "metricFieldName": "area_pressione_ha",
                                        "topN": 10,
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
                        success=True,
                        details='Ranked top 10 rows from "Regioni_Pressione_Ambientale" by "area_pressione_ha" (desc).',
                    ),
                },
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_chart",
                            "type": "function",
                            "function": {
                                "name": "bubbleChartTool",
                                "arguments": json.dumps(
                                    {
                                        "datasetName": "Regioni_Pressione_Ambientale",
                                        "xAxisField": "population",
                                        "yAxisField": "area_pressione_ha",
                                    }
                                ),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_chart",
                    "content": _qmap_tool_result(success=True, details='Tool "bubbleChartTool" completed.'),
                },
            ],
            "tools": [{"type": "function", "function": {"name": "rankQMapDatasetRows"}}],
        }
        out = _inject_runtime_guardrail_message(payload)
        content = str(out["messages"][0]["content"])
        self.assertIn("ranking_evidence_in_final_answer", content)
        self.assertIn("ordered Top-N list", content)
        self.assertIn("rankQMapDatasetRows", content)

    def test_ranking_objective_with_flat_metric_requires_tie_disclaimer(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {"role": "user", "content": "mostrami le regioni con più problemi"},
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
                                        "datasetName": "Regioni_Pressione_Ambientale",
                                        "metricFieldName": "area_pressione_ha",
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
                        success=True,
                        details='Ranked top 10 rows from "Regioni_Pressione_Ambientale" by "area_pressione_ha" (desc).',
                    ),
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
                                    {"datasetName": "Regioni_Pressione_Ambientale", "fieldName": "area_pressione_ha"}
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
                            'Field "area_pressione_ha" has <=1 distinct numeric value in sampled rows, '
                            "so color scale would appear uniform. Stats: sampled=20, numeric=20, distinct=1."
                        ),
                    ),
                },
            ],
            "tools": [{"type": "function", "function": {"name": "rankQMapDatasetRows"}}],
        }
        out = _inject_runtime_guardrail_message(payload)
        content = str(out["messages"][0]["content"])
        self.assertIn("explicitly state ties", content)

    def test_ranking_name_only_category_chart_is_not_evidence(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {"role": "user", "content": "mostrami le regioni con più problemi e grafici"},
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
                                        "datasetName": "Pressione_Ambientale_Regioni",
                                        "metricFieldName": "area_pressione_ha",
                                        "topN": 10,
                                        "sortDirection": "desc",
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
                        success=True,
                        details='Ranked top 10 rows from "Pressione_Ambientale_Regioni" by "area_pressione_ha" (desc).',
                    ),
                },
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_chart",
                            "type": "function",
                            "function": {
                                "name": "categoryBarsTool",
                                "arguments": json.dumps(
                                    {
                                        "datasetName": "Pressione_Ambientale_Regioni",
                                        "categoryFieldName": "name",
                                        "topN": 10,
                                    }
                                ),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_chart",
                    "content": _qmap_tool_result(success=True, details='Rendered top 10 categories from "name".'),
                },
            ],
            "tools": [{"type": "function", "function": {"name": "rankQMapDatasetRows"}}],
        }
        out = _inject_runtime_guardrail_message(payload)
        content = str(out["messages"][0]["content"])
        self.assertIn("ranking_chart_name_only_not_evidence", content)
        self.assertIn("not valid ranking evidence", content)

    def test_ranking_name_only_category_chart_detected_from_tool_result_details(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {"role": "user", "content": "mostrami le regioni con più problemi e grafici"},
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
                                        "datasetName": "Regioni_Pressione_Ambientale",
                                        "metricFieldName": "area_pressione_ha",
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
                        success=True,
                        details='Ranked top 10 rows from "Regioni_Pressione_Ambientale" by "area_pressione_ha" (desc).',
                    ),
                },
                {
                    "role": "tool",
                    "tool_call_id": "orphan_chart_result",
                    "content": _qmap_tool_result(success=True, details='Rendered top 20 categories from "name".'),
                },
            ],
            "tools": [{"type": "function", "function": {"name": "rankQMapDatasetRows"}}],
        }
        out = _inject_runtime_guardrail_message(payload)
        content = str(out["messages"][0]["content"])
        self.assertIn("ranking_chart_name_only_not_evidence", content)
        self.assertIn("not valid ranking evidence", content)
