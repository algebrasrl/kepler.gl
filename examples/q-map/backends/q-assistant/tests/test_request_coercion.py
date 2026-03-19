import unittest
import json

from fastapi import HTTPException

from q_assistant.chat_request_coercion import _coerce_chat_request
from q_assistant.message_text import _extract_message_text
from q_assistant.objective_anchor import _inject_objective_anchor_message
from q_assistant.openai_chat_payload import _coerce_openai_chat_payload
from q_assistant.request_coercion import (
    _repair_qmap_metric_tool_call_arguments,
    _repair_qmap_validation_tool_call_arguments,
)


class ChatRequestCoercionTests(unittest.TestCase):
    def test_keeps_canonical_prompt_payload(self):
        request = _coerce_chat_request(
            {
                "prompt": "ciao",
                "context": {"k": "v"},
                "agent": {"provider": "ollama", "model": "qwen3-coder:30b"},
            }
        )
        self.assertEqual(request.prompt, "ciao")
        self.assertEqual(request.context, {"k": "v"})
        self.assertEqual(request.agent.provider, "ollama")

    def test_rejects_openai_style_messages_payload_without_prompt(self):
        with self.assertRaises(HTTPException) as ctx:
            _coerce_chat_request(
                {
                    "messages": [
                        {"role": "system", "content": "ignore"},
                        {"role": "user", "content": "first"},
                        {"role": "assistant", "content": "reply"},
                        {"role": "user", "content": "last user message"},
                    ],
                    "provider": "openai",
                    "model": "gpt-5.2",
                }
            )
        self.assertEqual(ctx.exception.status_code, 422)
        self.assertIn('Missing "prompt"', str(ctx.exception.detail))

    def test_rejects_single_message_payload_without_prompt(self):
        with self.assertRaises(HTTPException) as ctx:
            _coerce_chat_request(
                {
                    "message": {
                        "role": "user",
                        "content": "draw h3 over milan",
                    },
                    "provider": "openai",
                    "model": "gpt-4",
                }
            )
        self.assertEqual(ctx.exception.status_code, 422)
        self.assertIn('Missing "prompt"', str(ctx.exception.detail))

    def test_raises_422_when_prompt_and_messages_missing(self):
        with self.assertRaises(HTTPException) as ctx:
            _coerce_chat_request({})
        self.assertEqual(ctx.exception.status_code, 422)
        self.assertIn('Missing "prompt"', str(ctx.exception.detail))

    def test_extract_message_text_strips_control_characters(self):
        text = _extract_message_text({"role": "assistant", "content": "ok\x00\x07\x1fdone"})
        self.assertEqual(text, "okdone")

    def test_extract_message_text_falls_back_to_parts_when_content_is_only_controls(self):
        text = _extract_message_text(
            {
                "role": "assistant",
                "content": "\x00\x07\x1f",
                "parts": [{"text": "final plain text"}],
            }
        )
        self.assertEqual(text, "final plain text")

    def test_pipeline_keeps_clean_objective_when_messages_contain_dirty_noise(self):
        payload = {
            "messages": [
                {
                    "role": "user",
                    "content": "Analizza tema ambientale in area delimitata e ordina le priorita.",
                },
                {
                    "role": "assistant",
                    "content": "[requestId: dirty-1]\n[progress] steps=1/2\x00\x07",
                },
                {
                    "role": "user",
                    "content": "Tool execution complete. Provide a concise final answer in plain text without calling tools.",
                },
            ]
        }

        coerced = _coerce_openai_chat_payload(payload)
        anchored = _inject_objective_anchor_message(coerced)
        system_content = str(anchored.get("messages", [])[0].get("content") or "")

        self.assertIn("Analizza tema ambientale in area delimitata", system_content)
        self.assertNotIn("Tool execution complete.", system_content)
        self.assertNotIn("[requestId:", system_content)
        self.assertNotRegex(system_content, r"[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]")

    def test_coerce_openai_payload_repairs_combined_filter_operator_value_tokens(self):
        payload = {
            "messages": [
                {"role": "user", "content": "Mostrami i confini provinciali di Brescia"},
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_1",
                            "type": "function",
                            "function": {
                                "name": "queryQCumberTerritorialUnits",
                                "arguments": (
                                    '{"filters":[{"field":"name","op":"eq","value":"Brescia"},'
                                    '{"field":"lv","op":"eq,value:7"}]}'
                                ),
                            },
                        }
                    ],
                },
            ]
        }

        out = _coerce_openai_chat_payload(payload)
        tool_call = out["messages"][1]["tool_calls"][0]
        args = json.loads(tool_call["function"]["arguments"])
        self.assertEqual(args["filters"][1]["op"], "eq")
        self.assertEqual(args["filters"][1]["value"], 7)

    def test_repair_qmap_validation_tool_call_arguments_fills_missing_dataset_from_recent_result(self):
        payload = {
            "choices": [
                {
                    "message": {
                        "role": "assistant",
                        "tool_calls": [
                            {
                                "id": "call_wait",
                                "type": "function",
                                "function": {
                                    "name": "waitForQMapDataset",
                                    "arguments": "{}",
                                },
                            }
                        ],
                    }
                }
            ]
        }
        repaired = _repair_qmap_validation_tool_call_arguments(
            payload,
            request_tool_results=[
                {
                    "toolName": "loadCloudMapAndWait",
                    "success": True,
                    "datasetRef": "id:qmap-cloud-final",
                    "datasetName": "QMap Cloud Final",
                }
            ],
        )
        args = json.loads(repaired["choices"][0]["message"]["tool_calls"][0]["function"]["arguments"])
        self.assertEqual(args["datasetName"], "id:qmap-cloud-final")

    def test_repair_qmap_validation_tool_call_arguments_preserves_existing_dataset_name(self):
        payload = {
            "choices": [
                {
                    "message": {
                        "role": "assistant",
                        "tool_calls": [
                            {
                                "id": "call_wait",
                                "type": "function",
                                "function": {
                                    "name": "waitForQMapDataset",
                                    "arguments": '{"datasetName":"id:already-set"}',
                                },
                            }
                        ],
                    }
                }
            ]
        }
        repaired = _repair_qmap_validation_tool_call_arguments(
            payload,
            request_tool_results=[
                {
                    "toolName": "loadCloudMapAndWait",
                    "success": True,
                    "datasetRef": "id:qmap-cloud-final",
                    "datasetName": "QMap Cloud Final",
                }
            ],
        )
        args = json.loads(repaired["choices"][0]["message"]["tool_calls"][0]["function"]["arguments"])
        self.assertEqual(args["datasetName"], "id:already-set")

    def test_repair_qmap_validation_tool_call_arguments_adds_dataset_ref_for_wait_when_name_matches_recent_result(self):
        payload = {
            "choices": [
                {
                    "message": {
                        "role": "assistant",
                        "tool_calls": [
                            {
                                "id": "call_wait",
                                "type": "function",
                                "function": {
                                    "name": "waitForQMapDataset",
                                    "arguments": '{"datasetName":"QMap Cloud Final"}',
                                },
                            }
                        ],
                    }
                }
            ]
        }
        repaired = _repair_qmap_validation_tool_call_arguments(
            payload,
            request_tool_results=[
                {
                    "toolName": "loadCloudMapAndWait",
                    "success": True,
                    "datasetRef": "id:qmap-cloud-final",
                    "datasetName": "QMap Cloud Final",
                }
            ],
        )
        args = json.loads(repaired["choices"][0]["message"]["tool_calls"][0]["function"]["arguments"])
        self.assertEqual(args["datasetRef"], "id:qmap-cloud-final")
        self.assertEqual(args["datasetName"], "QMap Cloud Final")

    def test_repair_qmap_metric_tool_call_arguments_rewrites_style_field_from_recent_join_metadata(self):
        payload = {
            "choices": [
                {
                    "message": {
                        "role": "assistant",
                        "tool_calls": [
                            {
                                "id": "call_color",
                                "type": "function",
                                "function": {
                                    "name": "setQMapLayerColorByField",
                                    "arguments": '{"datasetName":"Province_Pressione_Ambientale","fieldName":"sum_area_ha","palette":"yellowRed"}',
                                },
                            }
                        ],
                    }
                }
            ]
        }
        repaired = _repair_qmap_metric_tool_call_arguments(
            payload,
            request_tool_results=[
                {
                    "toolName": "spatialJoinByPredicate",
                    "success": True,
                    "datasetName": "Province_Pressione_Ambientale",
                    "fieldCatalog": ["name", "join_count", "join_sum"],
                    "numericFields": ["join_count", "join_sum"],
                    "styleableFields": ["join_count", "join_sum"],
                    "defaultStyleField": "join_sum",
                    "aggregationOutputs": {"count": "join_count", "sum": "join_sum"},
                }
            ],
        )
        args = json.loads(repaired["choices"][0]["message"]["tool_calls"][0]["function"]["arguments"])
        self.assertEqual(args["fieldName"], "join_sum")

    def test_repair_qmap_metric_tool_call_arguments_preserves_existing_valid_field(self):
        payload = {
            "choices": [
                {
                    "message": {
                        "role": "assistant",
                        "tool_calls": [
                            {
                                "id": "call_color",
                                "type": "function",
                                "function": {
                                    "name": "setQMapLayerColorByField",
                                    "arguments": '{"datasetName":"Province_Pressione_Ambientale","fieldName":"join_count","palette":"yellowRed"}',
                                },
                            }
                        ],
                    }
                }
            ]
        }
        repaired = _repair_qmap_metric_tool_call_arguments(
            payload,
            request_tool_results=[
                {
                    "toolName": "spatialJoinByPredicate",
                    "success": True,
                    "datasetName": "Province_Pressione_Ambientale",
                    "fieldCatalog": ["name", "join_count", "join_sum"],
                    "numericFields": ["join_count", "join_sum"],
                    "styleableFields": ["join_count", "join_sum"],
                    "defaultStyleField": "join_sum",
                    "aggregationOutputs": {"count": "join_count", "sum": "join_sum"},
                }
            ],
        )
        args = json.loads(repaired["choices"][0]["message"]["tool_calls"][0]["function"]["arguments"])
        self.assertEqual(args["fieldName"], "join_count")

    def test_repair_qmap_metric_tool_call_arguments_uses_default_style_field_from_normalized_dataset(self):
        payload = {
            "choices": [
                {
                    "message": {
                        "role": "assistant",
                        "tool_calls": [
                            {
                                "id": "call_color",
                                "type": "function",
                                "function": {
                                    "name": "setQMapLayerColorByField",
                                    "arguments": '{"datasetName":"Province_Normalized","fieldName":"population_per_capita","palette":"yellowRed"}',
                                },
                            }
                        ],
                    }
                }
            ]
        }
        repaired = _repair_qmap_metric_tool_call_arguments(
            payload,
            request_tool_results=[
                {
                    "toolName": "createDatasetWithNormalizedField",
                    "success": True,
                    "datasetName": "Province_Normalized",
                    "fieldCatalog": ["name", "population", "population_per_100k"],
                    "numericFields": ["population_per_100k"],
                    "styleableFields": ["population_per_100k"],
                    "defaultStyleField": "population_per_100k",
                }
            ],
        )
        args = json.loads(repaired["choices"][0]["message"]["tool_calls"][0]["function"]["arguments"])
        self.assertEqual(args["fieldName"], "population_per_100k")

    def test_repair_qmap_metric_tool_call_arguments_uses_default_style_field_from_h3_join_metadata(self):
        payload = {
            "choices": [
                {
                    "message": {
                        "role": "assistant",
                        "tool_calls": [
                            {
                                "id": "call_color",
                                "type": "function",
                                "function": {
                                    "name": "setQMapLayerColorByField",
                                    "arguments": '{"datasetName":"Joined_H3","fieldName":"population_raw","palette":"yellowRed"}',
                                },
                            }
                        ],
                    }
                }
            ]
        }
        repaired = _repair_qmap_metric_tool_call_arguments(
            payload,
            request_tool_results=[
                {
                    "toolName": "joinQMapDatasetsOnH3",
                    "success": True,
                    "datasetName": "Joined_H3",
                    "fieldCatalog": ["h3_id", "left_metric", "population_2"],
                    "numericFields": ["population_2"],
                    "styleableFields": ["population_2"],
                    "defaultStyleField": "population_2",
                }
            ],
        )
        args = json.loads(repaired["choices"][0]["message"]["tool_calls"][0]["function"]["arguments"])
        self.assertEqual(args["fieldName"], "population_2")

    def test_repair_qmap_metric_tool_call_arguments_prefers_explicit_field_alias_over_default_style_field(self):
        payload = {
            "choices": [
                {
                    "message": {
                        "role": "assistant",
                        "tool_calls": [
                            {
                                "id": "call_color",
                                "type": "function",
                                "function": {
                                    "name": "setQMapLayerColorByField",
                                    "arguments": '{"datasetName":"Joined_H3","fieldName":"population","palette":"yellowRed"}',
                                },
                            }
                        ],
                    }
                }
            ]
        }
        repaired = _repair_qmap_metric_tool_call_arguments(
            payload,
            request_tool_results=[
                {
                    "toolName": "joinQMapDatasetsOnH3",
                    "success": True,
                    "datasetName": "Joined_H3",
                    "fieldCatalog": ["h3_id", "count_weighted", "population_2"],
                    "numericFields": ["count_weighted", "population_2"],
                    "styleableFields": ["count_weighted", "population_2"],
                    "defaultStyleField": "count_weighted",
                    "fieldAliases": {"population": "population_2"},
                }
            ],
        )
        args = json.loads(repaired["choices"][0]["message"]["tool_calls"][0]["function"]["arguments"])
        self.assertEqual(args["fieldName"], "population_2")

    def test_repair_qmap_metric_tool_call_arguments_rewrites_threshold_style_field_from_catalog_metadata(self):
        payload = {
            "choices": [
                {
                    "message": {
                        "role": "assistant",
                        "tool_calls": [
                            {
                                "id": "call_thresholds",
                                "type": "function",
                                "function": {
                                    "name": "setQMapLayerColorByStatsThresholds",
                                    "arguments": (
                                        '{"datasetName":"Province_Normalized","fieldName":"population_per_capita",'
                                        '"strategy":"quantiles","palette":"yellowRed"}'
                                    ),
                                },
                            }
                        ],
                    }
                }
            ]
        }
        repaired = _repair_qmap_metric_tool_call_arguments(
            payload,
            request_tool_results=[
                {
                    "toolName": "listQMapDatasets",
                    "success": True,
                    "catalogDatasets": [
                        {
                            "datasetName": "Province_Normalized",
                            "datasetRef": "id:province_normalized",
                            "fieldCatalog": ["name", "population", "population_per_100k"],
                            "styleableFields": ["population_per_100k"],
                            "defaultStyleField": "population_per_100k",
                        }
                    ],
                }
            ],
        )
        args = json.loads(repaired["choices"][0]["message"]["tool_calls"][0]["function"]["arguments"])
        self.assertEqual(args["fieldName"], "population_per_100k")

    def test_repair_qmap_metric_tool_call_arguments_normalizes_create_dataset_with_normalized_field_aliases(self):
        payload = {
            "choices": [
                {
                    "message": {
                        "role": "assistant",
                        "tool_calls": [
                            {
                                "id": "call_normalize",
                                "type": "function",
                                "function": {
                                    "name": "createDatasetWithNormalizedField",
                                    "arguments": (
                                        '{"datasetName":"Admin Boundaries","fieldName":"flat_metric",'
                                        '"normalizationFieldName":"population","newFieldName":"area_per_abitante"}'
                                    ),
                                },
                            }
                        ],
                    }
                }
            ]
        }
        repaired = _repair_qmap_metric_tool_call_arguments(payload, request_tool_results=[])
        args = json.loads(repaired["choices"][0]["message"]["tool_calls"][0]["function"]["arguments"])
        self.assertEqual(args["numeratorFieldName"], "flat_metric")
        self.assertEqual(args["denominatorFieldName"], "population")
        self.assertEqual(args["outputFieldName"], "area_per_abitante")

    def test_repair_qmap_metric_tool_call_arguments_keeps_existing_canonical_normalized_field_args(self):
        payload = {
            "choices": [
                {
                    "message": {
                        "role": "assistant",
                        "tool_calls": [
                            {
                                "id": "call_normalize",
                                "type": "function",
                                "function": {
                                    "name": "createDatasetWithNormalizedField",
                                    "arguments": (
                                        '{"datasetName":"Admin Boundaries","fieldName":"flat_metric",'
                                        '"numeratorFieldName":"area_m2","denominatorFieldName":"population",'
                                        '"outputFieldName":"area_per_abitante"}'
                                    ),
                                },
                            }
                        ],
                    }
                }
            ]
        }
        repaired = _repair_qmap_metric_tool_call_arguments(payload, request_tool_results=[])
        args = json.loads(repaired["choices"][0]["message"]["tool_calls"][0]["function"]["arguments"])
        self.assertEqual(args["numeratorFieldName"], "area_m2")
        self.assertEqual(args["denominatorFieldName"], "population")
        self.assertEqual(args["outputFieldName"], "area_per_abitante")

    def test_repair_qmap_metric_tool_call_arguments_normalizes_alias_spacing_before_field_alias_lookup(self):
        payload = {
            "choices": [
                {
                    "message": {
                        "role": "assistant",
                        "tool_calls": [
                            {
                                "id": "call_color",
                                "type": "function",
                                "function": {
                                    "name": "setQMapLayerColorByField",
                                    "arguments": '{"datasetName":"Boschi_Clipped","fieldName":"intersection pct","palette":"yellowRed"}',
                                },
                            }
                        ],
                    }
                }
            ]
        }
        repaired = _repair_qmap_metric_tool_call_arguments(
            payload,
            request_tool_results=[
                {
                    "toolName": "clipQMapDatasetByGeometry",
                    "success": True,
                    "datasetName": "Boschi_Clipped",
                    "fieldCatalog": ["name", "qmap_clip_intersection_pct"],
                    "numericFields": ["qmap_clip_intersection_pct"],
                    "styleableFields": ["qmap_clip_intersection_pct"],
                    "defaultStyleField": "qmap_clip_intersection_pct",
                    "fieldAliases": {"intersection_pct": "qmap_clip_intersection_pct"},
                }
            ],
        )
        args = json.loads(repaired["choices"][0]["message"]["tool_calls"][0]["function"]["arguments"])
        self.assertEqual(args["fieldName"], "qmap_clip_intersection_pct")

    def test_repair_qmap_metric_tool_call_arguments_uses_nearest_join_default_style_field(self):
        payload = {
            "choices": [
                {
                    "message": {
                        "role": "assistant",
                        "tool_calls": [
                            {
                                "id": "call_color",
                                "type": "function",
                                "function": {
                                    "name": "setQMapLayerColorByField",
                                    "arguments": '{"datasetName":"Comuni_Nearest_POI","fieldName":"distance_km","palette":"yellowRed"}',
                                },
                            }
                        ],
                    }
                }
            ]
        }
        repaired = _repair_qmap_metric_tool_call_arguments(
            payload,
            request_tool_results=[
                {
                    "toolName": "nearestFeatureJoin",
                    "success": True,
                    "datasetName": "Comuni_Nearest_POI",
                    "fieldCatalog": ["name", "nearest_count", "nearest_distance_km"],
                    "numericFields": ["nearest_count", "nearest_distance_km"],
                    "styleableFields": ["nearest_count", "nearest_distance_km"],
                    "defaultStyleField": "nearest_distance_km",
                }
            ],
        )
        args = json.loads(repaired["choices"][0]["message"]["tool_calls"][0]["function"]["arguments"])
        self.assertEqual(args["fieldName"], "nearest_distance_km")

    def test_repair_qmap_metric_tool_call_arguments_uses_zonal_aggregation_output_for_threshold_style(self):
        payload = {
            "choices": [
                {
                    "message": {
                        "role": "assistant",
                        "tool_calls": [
                            {
                                "id": "call_thresholds",
                                "type": "function",
                                "function": {
                                    "name": "setQMapLayerColorByStatsThresholds",
                                    "arguments": (
                                        '{"datasetName":"Province_Zonal_NO2","fieldName":"avg_no2",'
                                        '"strategy":"quantiles","palette":"yellowRed"}'
                                    ),
                                },
                            }
                        ],
                    }
                }
            ]
        }
        repaired = _repair_qmap_metric_tool_call_arguments(
            payload,
            request_tool_results=[
                {
                    "toolName": "zonalStatsByAdmin",
                    "success": True,
                    "datasetName": "Province_Zonal_NO2",
                    "fieldCatalog": ["name", "zonal_value"],
                    "numericFields": ["zonal_value"],
                    "styleableFields": ["zonal_value"],
                    "defaultStyleField": "zonal_value",
                    "aggregationOutputs": {"avg": "zonal_value"},
                }
            ],
        )
        args = json.loads(repaired["choices"][0]["message"]["tool_calls"][0]["function"]["arguments"])
        self.assertEqual(args["fieldName"], "zonal_value")

    def test_repair_qmap_metric_tool_call_arguments_uses_clip_default_style_field(self):
        payload = {
            "choices": [
                {
                    "message": {
                        "role": "assistant",
                        "tool_calls": [
                            {
                                "id": "call_color",
                                "type": "function",
                                "function": {
                                    "name": "setQMapLayerColorByField",
                                    "arguments": '{"datasetName":"Boschi_Clipped","fieldName":"intersection_pct","palette":"yellowRed"}',
                                },
                            }
                        ],
                    }
                }
            ]
        }
        repaired = _repair_qmap_metric_tool_call_arguments(
            payload,
            request_tool_results=[
                {
                    "toolName": "clipQMapDatasetByGeometry",
                    "success": True,
                    "datasetName": "Boschi_Clipped",
                    "fieldCatalog": [
                        "name",
                        "qmap_clip_match_count",
                        "qmap_clip_intersection_area_m2",
                        "qmap_clip_intersection_pct",
                    ],
                    "numericFields": [
                        "qmap_clip_match_count",
                        "qmap_clip_intersection_area_m2",
                        "qmap_clip_intersection_pct",
                    ],
                    "styleableFields": [
                        "qmap_clip_match_count",
                        "qmap_clip_intersection_area_m2",
                        "qmap_clip_intersection_pct",
                    ],
                    "defaultStyleField": "qmap_clip_intersection_pct",
                }
            ],
        )
        args = json.loads(repaired["choices"][0]["message"]["tool_calls"][0]["function"]["arguments"])
        self.assertEqual(args["fieldName"], "qmap_clip_intersection_pct")

    def test_repair_qmap_metric_tool_call_arguments_uses_buffer_aggregation_output(self):
        payload = {
            "choices": [
                {
                    "message": {
                        "role": "assistant",
                        "tool_calls": [
                            {
                                "id": "call_stats",
                                "type": "function",
                                "function": {
                                    "name": "setQMapLayerColorByStatsThresholds",
                                    "arguments": (
                                        '{"datasetName":"Comuni_Buffer_Summary","fieldName":"sum_area_ha",'
                                        '"strategy":"quantiles","palette":"yellowRed"}'
                                    ),
                                },
                            }
                        ],
                    }
                }
            ]
        }
        repaired = _repair_qmap_metric_tool_call_arguments(
            payload,
            request_tool_results=[
                {
                    "toolName": "bufferAndSummarize",
                    "success": True,
                    "datasetName": "Comuni_Buffer_Summary",
                    "fieldCatalog": ["name", "buffer_metric"],
                    "numericFields": ["buffer_metric"],
                    "styleableFields": ["buffer_metric"],
                    "defaultStyleField": "buffer_metric",
                    "aggregationOutputs": {"sum": "buffer_metric"},
                }
            ],
        )
        args = json.loads(repaired["choices"][0]["message"]["tool_calls"][0]["function"]["arguments"])
        self.assertEqual(args["fieldName"], "buffer_metric")

    def test_repair_qmap_metric_tool_call_arguments_uses_populated_tessellation_aggregation_output(self):
        payload = {
            "choices": [
                {
                    "message": {
                        "role": "assistant",
                        "tool_calls": [
                            {
                                "id": "call_color",
                                "type": "function",
                                "function": {
                                    "name": "setQMapLayerColorByField",
                                    "arguments": '{"datasetName":"Tassellazione_Popolata","fieldName":"sum_population","palette":"yellowRed"}',
                                },
                            }
                        ],
                    }
                }
            ]
        }
        repaired = _repair_qmap_metric_tool_call_arguments(
            payload,
            request_tool_results=[
                {
                    "toolName": "populateTassellationFromAdminUnits",
                    "success": True,
                    "datasetName": "Tassellazione_Popolata",
                    "fieldCatalog": ["h3_id", "population_2"],
                    "numericFields": ["population_2"],
                    "styleableFields": ["population_2"],
                    "defaultStyleField": "population_2",
                    "aggregationOutputs": {"sum": "population_2"},
                }
            ],
        )
        args = json.loads(repaired["choices"][0]["message"]["tool_calls"][0]["function"]["arguments"])
        self.assertEqual(args["fieldName"], "population_2")

    def test_repair_qmap_metric_tool_call_arguments_uses_h3_aggregation_output(self):
        payload = {
            "choices": [
                {
                    "message": {
                        "role": "assistant",
                        "tool_calls": [
                            {
                                "id": "call_color",
                                "type": "function",
                                "function": {
                                    "name": "setQMapLayerColorByField",
                                    "arguments": '{"datasetName":"CLC_h3_agg_r6","fieldName":"weighted_count","palette":"yellowRed"}',
                                },
                            }
                        ],
                    }
                }
            ]
        }
        repaired = _repair_qmap_metric_tool_call_arguments(
            payload,
            request_tool_results=[
                {
                    "toolName": "aggregateDatasetToH3",
                    "success": True,
                    "datasetName": "CLC_h3_agg_r6",
                    "fieldCatalog": ["h3_id", "h3_resolution", "count", "count_weighted", "sum"],
                    "numericFields": ["count", "count_weighted", "sum"],
                    "styleableFields": ["count", "count_weighted", "sum"],
                    "defaultStyleField": "sum",
                    "aggregationOutputs": {"count": "count", "count_weighted": "count_weighted", "sum": "sum"},
                }
            ],
        )
        args = json.loads(repaired["choices"][0]["message"]["tool_calls"][0]["function"]["arguments"])
        self.assertEqual(args["fieldName"], "count_weighted")


if __name__ == "__main__":
    unittest.main()
