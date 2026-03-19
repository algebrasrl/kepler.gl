import unittest
import time
import os
import tempfile
import sys
from unittest.mock import patch

from q_assistant.audit_logging import (
    _audit_context_for_log,
    _build_chat_audit_event,
    _prune_chat_audit_dir,
    _resolve_audit_session_id,
    _sanitize_for_audit,
)
from q_assistant.main import (
    _estimate_payload_token_usage,
    _extract_request_tool_results,
    _extract_response_tool_calls,
    _extract_upstream_usage,
    _parse_tool_arguments,
    _resolve_cloud_authorization_header,
)
from q_assistant.config import load_settings


class ChatAuditUtilsTests(unittest.TestCase):
    def test_resolve_cloud_authorization_header_prefers_configured_token(self):
        out = _resolve_cloud_authorization_header(
            "configured-token",
            "Bearer caller-token",
        )
        self.assertEqual(out, "Bearer configured-token")

    def test_resolve_cloud_authorization_header_falls_back_to_caller_bearer(self):
        out = _resolve_cloud_authorization_header("", "Bearer caller-token")
        self.assertEqual(out, "Bearer caller-token")

    def test_resolve_cloud_authorization_header_ignores_non_bearer_caller_auth(self):
        out = _resolve_cloud_authorization_header("", "Basic abc123")
        self.assertIsNone(out)

    def test_sanitize_for_audit_redacts_sensitive_keys(self):
        payload = {
            "apiKey": "secret-123",
            "authorization": "Bearer abc",
            "nested": {"token": "tkn", "value": 10},
        }
        out = _sanitize_for_audit(payload)
        self.assertEqual(out["apiKey"], "[REDACTED]")
        self.assertEqual(out["authorization"], "[REDACTED]")
        self.assertEqual(out["nested"]["token"], "[REDACTED]")
        self.assertEqual(out["nested"]["value"], 10)

    def test_parse_tool_arguments_repairs_inline_operator_value_token(self):
        out = _parse_tool_arguments(
            '{"filters":[{"field":"name","op":"eq","value":"Brescia"},{"field":"lv","op":"eq,value:7"}]}'
        )
        self.assertIsInstance(out, dict)
        filters = out.get("filters") or []
        self.assertEqual(filters[1].get("op"), "eq")
        self.assertEqual(filters[1].get("value"), 7)

    def test_parse_tool_arguments_keeps_non_canonical_normalized_field_aliases_unmapped(self):
        out = _parse_tool_arguments(
            '{"datasetName":"ds","numeratorField":"sum","denominatorField":"h3_area_ha"}'
        )
        self.assertIsInstance(out, dict)
        self.assertEqual(out.get("numeratorFieldName"), None)
        self.assertEqual(out.get("denominatorFieldName"), None)
        self.assertEqual(out.get("numeratorField"), "sum")
        self.assertEqual(out.get("denominatorField"), "h3_area_ha")

    def test_parse_tool_arguments_keeps_non_canonical_wait_dataset_aliases_unmapped(self):
        out = _parse_tool_arguments('{"datasetRef":"id:qmap_clip_boschi_treviso_clipped"}')
        self.assertIsInstance(out, dict)
        self.assertEqual(out.get("datasetName"), None)
        self.assertEqual(out.get("datasetRef"), "id:qmap_clip_boschi_treviso_clipped")

    def test_audit_context_for_log_sanitizes_context_json(self):
        previous_include_context = os.environ.get("Q_ASSISTANT_CHAT_AUDIT_INCLUDE_CONTEXT")
        try:
            os.environ["Q_ASSISTANT_CHAT_AUDIT_INCLUDE_CONTEXT"] = "true"
            settings = load_settings()
            out = _audit_context_for_log(
                settings,
                '{"sessionId":"sess-123","token":"secret-token","metadata":{"authorization":"Bearer x","ok":1}}',
            )
            self.assertIsInstance(out, dict)
            self.assertEqual(out.get("sessionId"), "sess-123")
            self.assertNotIn("token", out)
            self.assertEqual((out.get("metadata") or {}).get("ok"), 1)
            self.assertNotIn("authorization", out.get("metadata") or {})
        finally:
            if previous_include_context is None:
                os.environ.pop("Q_ASSISTANT_CHAT_AUDIT_INCLUDE_CONTEXT", None)
            else:
                os.environ["Q_ASSISTANT_CHAT_AUDIT_INCLUDE_CONTEXT"] = previous_include_context

    def test_extract_response_tool_calls(self):
        payload = {
            "choices": [
                {
                    "message": {
                        "tool_calls": [
                            {"function": {"name": "listQCumberProviders"}},
                            {"function": {"name": "queryQCumberDataset"}},
                        ]
                    }
                }
            ]
        }
        self.assertEqual(
            _extract_response_tool_calls(payload),
            ["listQCumberProviders", "queryQCumberDataset"],
        )

    def test_extract_request_tool_results_prefers_qmap_schema(self):
        payload = {
            "messages": [
                {
                    "role": "assistant",
                    "tool_calls": [
                        {"id": "call_1", "function": {"name": "queryQCumberDataset"}},
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_1",
                    "content": (
                        '{"qmapToolResult":{"schema":"qmap.tool_result.v1","toolName":"queryQCumberDataset",'
                        '"success":true,"details":"Loaded dataset."}}'
                    ),
                },
            ]
        }
        out = _extract_request_tool_results(payload)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["toolName"], "queryQCumberDataset")
        self.assertEqual(out[0]["success"], True)
        self.assertEqual(out[0]["details"], "Loaded dataset.")
        self.assertEqual(out[0]["resultSchema"], "qmap.tool_result.v1")
        self.assertEqual(out[0]["contractResponseMismatch"], False)

    def test_extract_request_tool_results_reads_success_from_details_json(self):
        payload = {
            "messages": [
                {
                    "role": "assistant",
                    "tool_calls": [
                        {"id": "call_2", "function": {"name": "queryQCumberTerritorialUnits"}},
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_2",
                    "content": '{"details":"{\\"success\\": true, \\"returned\\": 10}"}',
                },
            ]
        }
        out = _extract_request_tool_results(payload)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["toolName"], "queryQCumberTerritorialUnits")
        self.assertEqual(out[0]["success"], True)

    def test_extract_request_tool_results_flags_contract_response_mismatch_for_missing_required_field(self):
        payload = {
            "messages": [
                {
                    "role": "assistant",
                    "tool_calls": [
                        {"id": "call_list", "function": {"name": "listQMapDatasets"}},
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_list",
                    "content": '{"success":true,"datasets":[]}',
                },
            ]
        }
        out = _extract_request_tool_results(payload)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["contractResponseMismatch"], True)
        self.assertIn('missing required field "details"', out[0]["contractResponseValidationErrors"])

    def test_extract_request_tool_results_flags_contract_response_mismatch_for_declared_type_violation(self):
        payload = {
            "messages": [
                {
                    "role": "assistant",
                    "tool_calls": [
                        {"id": "call_join", "function": {"name": "spatialJoinByPredicate"}},
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_join",
                    "content": (
                        '{"llmResult":{"success":true,"dataset":"Province_Pressione_Ambientale",'
                        '"fieldCatalog":"join_sum",'
                        '"numericFields":["join_sum"],'
                        '"styleableFields":["join_sum"],'
                        '"defaultStyleField":"join_sum",'
                        '"details":"Join ok."}}'
                    ),
                },
            ]
        }
        out = _extract_request_tool_results(payload)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["contractResponseMismatch"], True)
        self.assertIn('field "fieldCatalog" expected type array', out[0]["contractResponseValidationErrors"])

    def test_extract_request_tool_results_flags_contract_response_mismatch_for_metric_tool_missing_required_metadata(self):
        payload = {
            "messages": [
                {
                    "role": "assistant",
                    "tool_calls": [
                        {"id": "call_join", "function": {"name": "spatialJoinByPredicate"}},
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_join",
                    "content": (
                        '{"llmResult":{"success":true,"dataset":"Province_Pressione_Ambientale",'
                        '"fieldCatalog":["name","join_sum"],'
                        '"numericFields":["join_sum"],'
                        '"styleableFields":["join_sum"],'
                        '"details":"Join ok."}}'
                    ),
                },
            ]
        }
        out = _extract_request_tool_results(payload)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["contractResponseMismatch"], True)
        self.assertIn('missing required field "defaultStyleField"', out[0]["contractResponseValidationErrors"])

    def test_extract_request_tool_results_skips_success_only_required_fields_for_failure_payload(self):
        payload = {
            "messages": [
                {
                    "role": "assistant",
                    "tool_calls": [
                        {"id": "call_norm", "function": {"name": "createDatasetWithNormalizedField"}},
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_norm",
                    "content": (
                        '{"success":false,"details":"Validation in progress. Retry after wait/count completion."}'
                    ),
                },
            ]
        }
        out = _extract_request_tool_results(payload)
        self.assertEqual(len(out), 1)
        self.assertFalse(out[0]["contractResponseMismatch"])
        self.assertIsNone(out[0]["contractResponseValidationErrors"])

    def test_extract_request_tool_results_reads_routing_metadata_fields(self):
        payload = {
            "messages": [
                {
                    "role": "assistant",
                    "tool_calls": [
                        {"id": "call_help", "function": {"name": "getQCumberDatasetHelp"}},
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_help",
                    "content": (
                        '{"qmapToolResult":{"schema":"qmap.tool_result.v1","success":true,"details":"Help ok."},'
                        '"llmResult":{"success":true,"routing":{"isAdministrative":false,"datasetClass":"thematic_spatial",'
                        '"queryToolHint":{"preferredTool":"queryQCumberDatasetSpatial"}}}}'
                    ),
                },
            ]
        }
        out = _extract_request_tool_results(payload)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["routingIsAdministrative"], False)
        self.assertEqual(out[0]["datasetClass"], "thematic_spatial")
        self.assertEqual(out[0]["routingPreferredTool"], "queryQCumberDatasetSpatial")

    def test_extract_request_tool_results_reads_metric_output_metadata(self):
        payload = {
            "messages": [
                {
                    "role": "assistant",
                    "tool_calls": [
                        {"id": "call_join", "function": {"name": "spatialJoinByPredicate"}},
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_join",
                    "content": (
                        '{"llmResult":{"success":true,"dataset":"Province_Pressione_Ambientale",'
                        '"fieldCatalog":["name","join_count","join_sum"],'
                        '"numericFields":["join_count","join_sum"],'
                        '"styleableFields":["join_count","join_sum"],'
                        '"defaultStyleField":"join_sum",'
                        '"aggregationOutputs":{"count":"join_count","sum":"join_sum"},'
                        '"details":"Join ok."}}'
                    ),
                },
            ]
        }
        out = _extract_request_tool_results(payload)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["defaultStyleField"], "join_sum")
        self.assertEqual(out[0]["aggregationOutputs"], {"count": "join_count", "sum": "join_sum"})
        self.assertEqual(out[0]["styleableFields"], ["join_count", "join_sum"])

    def test_extract_request_tool_results_reads_default_style_metadata_for_derived_field_tools(self):
        payload = {
            "messages": [
                {
                    "role": "assistant",
                    "tool_calls": [
                        {"id": "call_norm", "function": {"name": "createDatasetWithNormalizedField"}},
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_norm",
                    "content": (
                        '{"llmResult":{"success":true,"dataset":"Province_Normalized",'
                        '"fieldCatalog":["name","population","population_per_100k"],'
                        '"numericFields":["population_per_100k"],'
                        '"styleableFields":["population_per_100k"],'
                        '"defaultStyleField":"population_per_100k",'
                        '"outputFieldName":"population_per_100k",'
                        '"details":"Normalized dataset ok."}}'
                    ),
                },
            ]
        }
        out = _extract_request_tool_results(payload)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["defaultStyleField"], "population_per_100k")
        self.assertEqual(out[0]["fieldCatalog"], ["name", "population", "population_per_100k"])
        self.assertEqual(out[0]["numericFields"], ["population_per_100k"])

    def test_extract_request_tool_results_reads_h3_aggregation_outputs_metadata(self):
        payload = {
            "messages": [
                {
                    "role": "assistant",
                    "tool_calls": [
                        {"id": "call_h3", "function": {"name": "aggregateDatasetToH3"}},
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_h3",
                    "content": (
                        '{"llmResult":{"success":true,"dataset":"CLC_h3_agg_r6",'
                        '"fieldCatalog":["h3_id","h3_resolution","count","sum"],'
                        '"numericFields":["count","count_weighted","sum"],'
                        '"styleableFields":["count","count_weighted","sum"],'
                        '"defaultStyleField":"sum",'
                        '"aggregationOutputs":{"count":"count","count_weighted":"count_weighted","sum":"sum"},'
                        '"details":"H3 aggregation ok."}}'
                    ),
                },
            ]
        }
        out = _extract_request_tool_results(payload)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["defaultStyleField"], "sum")
        self.assertEqual(out[0]["aggregationOutputs"], {"count": "count", "count_weighted": "count_weighted", "sum": "sum"})

    def test_extract_request_tool_results_reads_field_aliases_metadata(self):
        payload = {
            "messages": [
                {
                    "role": "assistant",
                    "tool_calls": [
                        {"id": "call_h3_join", "function": {"name": "joinQMapDatasetsOnH3"}},
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_h3_join",
                    "content": (
                        '{"llmResult":{"success":true,"dataset":"Joined_H3",'
                        '"fieldCatalog":["h3_id","count_weighted","population_2"],'
                        '"numericFields":["count_weighted","population_2"],'
                        '"styleableFields":["count_weighted","population_2"],'
                        '"defaultStyleField":"count_weighted",'
                        '"fieldAliases":{"population":"population_2"},'
                        '"details":"H3 join ok."}}'
                    ),
                },
            ]
        }
        out = _extract_request_tool_results(payload)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["fieldAliases"], {"population": "population_2"})

    def test_extract_request_tool_results_reads_catalog_dataset_metadata_from_list_datasets(self):
        payload = {
            "messages": [
                {
                    "role": "assistant",
                    "tool_calls": [
                        {"id": "call_list", "function": {"name": "listQMapDatasets"}},
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_list",
                    "content": (
                        '{"success":true,"datasets":[{"id":"qmap_spatial_join_province","name":"Province_Pressione_Ambientale",'
                        '"datasetRef":"id:qmap_spatial_join_province","fields":["_geojson","name","join_count","join_sum"]}],'
                        '"layers":[{"id":"layer-province","name":"Province_Pressione_Ambientale",'
                        '"datasetId":"qmap_spatial_join_province","datasetRef":"id:qmap_spatial_join_province",'
                        '"datasetName":"Province_Pressione_Ambientale","activeFields":["join_sum"],'
                        '"availableFields":["_geojson","name","join_count","join_sum"]}],"details":"Found 1 dataset."}'
                    ),
                },
            ]
        }
        out = _extract_request_tool_results(payload)
        self.assertEqual(len(out), 1)
        catalog = out[0]["catalogDatasets"] or []
        self.assertEqual(len(catalog), 1)
        self.assertEqual(catalog[0]["datasetName"], "Province_Pressione_Ambientale")
        self.assertEqual(catalog[0]["defaultStyleField"], "join_sum")
        self.assertEqual(catalog[0]["styleableFields"], ["join_sum"])

    def test_extract_request_tool_results_reads_catalog_dataset_field_aliases(self):
        payload = {
            "messages": [
                {
                    "role": "assistant",
                    "tool_calls": [
                        {"id": "call_list", "function": {"name": "listQMapDatasets"}},
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_list",
                    "content": (
                        '{"success":true,"datasets":[{"id":"qmap_joined_h3","name":"Joined_H3",'
                        '"datasetRef":"id:qmap_joined_h3","fields":["h3_id","count_weighted","population_2"],'
                        '"fieldAliases":{"population":"population_2"}}],'
                        '"layers":[{"id":"layer-joined-h3","name":"Joined_H3",'
                        '"datasetId":"qmap_joined_h3","datasetRef":"id:qmap_joined_h3",'
                        '"datasetName":"Joined_H3","activeFields":["count_weighted"],'
                        '"availableFields":["h3_id","count_weighted","population_2"]}],"details":"Found 1 dataset."}'
                    ),
                },
            ]
        }
        out = _extract_request_tool_results(payload)
        self.assertEqual(len(out), 1)
        catalog = out[0]["catalogDatasets"] or []
        self.assertEqual(catalog[0]["fieldAliases"], {"population": "population_2"})

    def test_extract_request_tool_results_backfills_list_datasets_contract_payload_from_catalog(self):
        payload = {
            "messages": [
                {
                    "role": "assistant",
                    "tool_calls": [
                        {"id": "call_list", "function": {"name": "listQMapDatasets"}},
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_list",
                    "content": (
                        '{"qmapToolResult":{"schema":"qmap.tool_result.v1","success":true,'
                        '"details":"Found 1 dataset."},'
                        '"llmResult":{"datasets":[{"id":"ds-1","name":"Dataset 1","datasetRef":"id:ds-1",'
                        '"fields":["metric"]}],"layers":[{"id":"layer-1","name":"Dataset 1","datasetId":"ds-1",'
                        '"datasetRef":"id:ds-1","datasetName":"Dataset 1","activeFields":["metric"],'
                        '"availableFields":["metric"]}]}}'
                    ),
                },
            ]
        }
        out = _extract_request_tool_results(payload)
        self.assertEqual(len(out), 1)
        self.assertFalse(out[0]["contractResponseMismatch"])
        self.assertIsNone(out[0]["contractResponseValidationErrors"])

    def test_extract_request_tool_results_reads_nearest_join_metric_metadata(self):
        payload = {
            "messages": [
                {
                    "role": "assistant",
                    "tool_calls": [
                        {"id": "call_nearest", "function": {"name": "nearestFeatureJoin"}},
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_nearest",
                    "content": (
                        '{"llmResult":{"success":true,"dataset":"Comuni_Nearest_POI",'
                        '"fieldCatalog":["name","nearest_count","nearest_distance_km"],'
                        '"numericFields":["nearest_count","nearest_distance_km"],'
                        '"styleableFields":["nearest_count","nearest_distance_km"],'
                        '"defaultStyleField":"nearest_distance_km",'
                        '"details":"Nearest join ok."}}'
                    ),
                },
            ]
        }
        out = _extract_request_tool_results(payload)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["defaultStyleField"], "nearest_distance_km")
        self.assertEqual(out[0]["styleableFields"], ["nearest_count", "nearest_distance_km"])

    def test_extract_request_tool_results_reads_zonal_aggregation_outputs_metadata(self):
        payload = {
            "messages": [
                {
                    "role": "assistant",
                    "tool_calls": [
                        {"id": "call_zonal", "function": {"name": "zonalStatsByAdmin"}},
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_zonal",
                    "content": (
                        '{"llmResult":{"success":true,"dataset":"Province_Zonal_NO2",'
                        '"fieldCatalog":["name","zonal_value"],'
                        '"numericFields":["zonal_value"],'
                        '"styleableFields":["zonal_value"],'
                        '"defaultStyleField":"zonal_value",'
                        '"aggregationOutputs":{"avg":"zonal_value"},'
                        '"details":"Zonal stats ok."}}'
                    ),
                },
            ]
        }
        out = _extract_request_tool_results(payload)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["defaultStyleField"], "zonal_value")
        self.assertEqual(out[0]["aggregationOutputs"], {"avg": "zonal_value"})

    def test_extract_request_tool_results_reads_clip_metric_metadata(self):
        payload = {
            "messages": [
                {
                    "role": "assistant",
                    "tool_calls": [
                        {"id": "call_clip", "function": {"name": "clipQMapDatasetByGeometry"}},
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_clip",
                    "content": (
                        '{"llmResult":{"success":true,"dataset":"Boschi_Clipped",'
                        '"fieldCatalog":["name","qmap_clip_match_count","qmap_clip_intersection_area_m2","qmap_clip_intersection_pct"],'
                        '"numericFields":["qmap_clip_match_count","qmap_clip_intersection_area_m2","qmap_clip_intersection_pct"],'
                        '"styleableFields":["qmap_clip_match_count","qmap_clip_intersection_area_m2","qmap_clip_intersection_pct"],'
                        '"defaultStyleField":"qmap_clip_intersection_pct",'
                        '"details":"Clip ok."}}'
                    ),
                },
            ]
        }
        out = _extract_request_tool_results(payload)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["defaultStyleField"], "qmap_clip_intersection_pct")
        self.assertEqual(
            out[0]["styleableFields"],
            ["qmap_clip_match_count", "qmap_clip_intersection_area_m2", "qmap_clip_intersection_pct"],
        )

    def test_extract_request_tool_results_reads_buffer_aggregation_outputs_metadata(self):
        payload = {
            "messages": [
                {
                    "role": "assistant",
                    "tool_calls": [
                        {"id": "call_buffer", "function": {"name": "bufferAndSummarize"}},
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_buffer",
                    "content": (
                        '{"llmResult":{"success":true,"dataset":"Comuni_Buffer_Summary",'
                        '"fieldCatalog":["name","buffer_metric"],'
                        '"numericFields":["buffer_metric"],'
                        '"styleableFields":["buffer_metric"],'
                        '"defaultStyleField":"buffer_metric",'
                        '"aggregationOutputs":{"sum":"buffer_metric"},'
                        '"details":"Buffer summary ok."}}'
                    ),
                },
            ]
        }
        out = _extract_request_tool_results(payload)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["defaultStyleField"], "buffer_metric")
        self.assertEqual(out[0]["aggregationOutputs"], {"sum": "buffer_metric"})

    def test_extract_request_tool_results_reads_populated_tessellation_aggregation_outputs_metadata(self):
        payload = {
            "messages": [
                {
                    "role": "assistant",
                    "tool_calls": [
                        {"id": "call_pop", "function": {"name": "populateTassellationFromAdminUnits"}},
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_pop",
                    "content": (
                        '{"llmResult":{"success":true,"dataset":"Tassellazione_Popolata",'
                        '"fieldCatalog":["h3_id","population_2"],'
                        '"numericFields":["population_2"],'
                        '"styleableFields":["population_2"],'
                        '"defaultStyleField":"population_2",'
                        '"aggregationOutputs":{"sum":"population_2"},'
                        '"details":"Populate ok."}}'
                    ),
                },
            ]
        }
        out = _extract_request_tool_results(payload)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["defaultStyleField"], "population_2")
        self.assertEqual(out[0]["aggregationOutputs"], {"sum": "population_2"})

    def test_estimate_payload_token_usage_returns_metrics(self):
        payload = {
            "model": "openai/gpt-4.1",
            "messages": [
                {"role": "system", "content": "sys"},
                {"role": "user", "content": "carica i comuni veneti"},
            ],
            "tools": [{"type": "function", "function": {"name": "listQCumberProviders"}}],
        }
        out = _estimate_payload_token_usage(payload, model_hint="openai/gpt-4.1")
        self.assertGreater(out.get("serializedChars", 0), 0)
        self.assertEqual(out.get("messageCount"), 2)
        self.assertEqual(out.get("toolCount"), 1)
        self.assertIn("method", out)
        estimated = out.get("estimatedPromptTokens")
        if str(out.get("method") or "").startswith("unknown:"):
            self.assertIsNone(estimated)
        else:
            self.assertGreater(int(estimated or 0), 0)

    def test_estimate_payload_token_usage_uses_char_approx_when_tokenizer_unavailable(self):
        payload = {
            "messages": [{"role": "user", "content": "ciao"}],
            "tools": [{"type": "function", "function": {"name": "listQCumberProviders"}}],
        }
        with patch.dict(sys.modules, {"tiktoken": None}):
            out = _estimate_payload_token_usage(payload, model_hint="openai/gpt-4.1")
        self.assertEqual(out.get("method"), "approx:chars_div_4.0")
        estimated = out.get("estimatedPromptTokens")
        self.assertIsNotNone(estimated)
        self.assertGreater(estimated, 0)
        # Default ratio 4.0 for unknown models
        self.assertEqual(estimated, max(1, int(out["serializedChars"] / 4.0)))

    def test_estimate_payload_token_usage_uses_gemini_ratio_when_tokenizer_unavailable(self):
        payload = {
            "messages": [{"role": "user", "content": "ciao mondo"}],
            "tools": [{"type": "function", "function": {"name": "listQCumberProviders"}}],
        }
        with patch.dict(sys.modules, {"tiktoken": None}):
            out = _estimate_payload_token_usage(payload, model_hint="google/gemini-3-flash-preview")
        self.assertEqual(out.get("method"), "approx:chars_div_5.5")
        estimated = out.get("estimatedPromptTokens")
        self.assertIsNotNone(estimated)
        self.assertGreater(estimated, 0)
        # Gemini calibrated ratio 5.5
        self.assertEqual(estimated, max(1, int(out["serializedChars"] / 5.5)))

    def test_extract_upstream_usage_normalizes_openai_shape(self):
        payload = {
            "usage": {
                "prompt_tokens": 123,
                "completion_tokens": 45,
                "total_tokens": 168,
                "prompt_tokens_details": {"cached_tokens": 0, "audio_tokens": 0},
                "completion_tokens_details": {
                    "reasoning_tokens": 0,
                    "audio_tokens": 0,
                    "accepted_prediction_tokens": 0,
                    "rejected_prediction_tokens": 0,
                },
            }
        }
        out = _extract_upstream_usage(payload)
        self.assertEqual(out, {"promptTokens": 123, "completionTokens": 45, "totalTokens": 168})

    def test_extract_upstream_usage_openai_stream_chunk_with_usage(self):
        # OpenAI chat.completions streaming can emit usage in a final chunk.
        payload = {
            "id": "chatcmpl-123",
            "object": "chat.completion.chunk",
            "choices": [],
            "usage": {
                "prompt_tokens": 19,
                "completion_tokens": 10,
                "total_tokens": 29,
            },
        }
        out = _extract_upstream_usage(payload)
        self.assertEqual(out, {"promptTokens": 19, "completionTokens": 10, "totalTokens": 29})

    def test_extract_upstream_usage_normalizes_anthropic_shape(self):
        payload = {
            "usage": {
                "input_tokens": 200,
                "output_tokens": 50,
            }
        }
        out = _extract_upstream_usage(payload)
        self.assertEqual(out, {"promptTokens": 200, "completionTokens": 50, "totalTokens": 250})

    def test_extract_upstream_usage_normalizes_gemini_usage_metadata(self):
        # Gemini native and OpenRouter-proxied Gemini use usageMetadata at top level.
        payload = {
            "usageMetadata": {
                "promptTokenCount": 350,
                "candidates_token_count": 120,
                "totalTokenCount": 470,
            }
        }
        out = _extract_upstream_usage(payload)
        self.assertEqual(out, {"promptTokens": 350, "completionTokens": 120, "totalTokens": 470})

    def test_extract_upstream_usage_prefers_usage_over_usage_metadata(self):
        # Standard usage field takes priority over usageMetadata.
        payload = {
            "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15},
            "usageMetadata": {"promptTokenCount": 999, "candidates_token_count": 999},
        }
        out = _extract_upstream_usage(payload)
        self.assertEqual(out, {"promptTokens": 10, "completionTokens": 5, "totalTokens": 15})

    def test_build_chat_audit_event_marks_empty_completion_outcome(self):
        started = time.perf_counter() - 0.1
        event = _build_chat_audit_event(
            endpoint="/chat/completions",
            status=200,
            started_at=started,
            session_id="default",
            request_id="req-empty",
            responseToolCalls=None,
            responseText=None,
        )
        self.assertEqual(event["outcome"], "empty_completion")

    def test_build_chat_audit_event_marks_success_with_tool_calls(self):
        started = time.perf_counter() - 0.1
        event = _build_chat_audit_event(
            endpoint="/chat/completions",
            status=200,
            started_at=started,
            session_id="default",
            request_id="req-tools",
            responseToolCalls=[{"id": "1", "type": "function", "function": {"name": "listQCumberProviders", "arguments": "{}"}}],
            responseText=None,
        )
        self.assertEqual(event["outcome"], "success")

    def test_build_chat_audit_event_marks_success_with_text(self):
        started = time.perf_counter() - 0.1
        event = _build_chat_audit_event(
            endpoint="/chat/completions",
            status=200,
            started_at=started,
            session_id="default",
            request_id="req-text",
            responseToolCalls=None,
            responseText="Ecco i dataset disponibili.",
        )
        self.assertEqual(event["outcome"], "success")

    def test_build_chat_audit_event_includes_quality_metrics_for_complete_chain(self):
        started = time.perf_counter() - 0.2
        event = _build_chat_audit_event(
            endpoint="/chat/completions",
            status=200,
            started_at=started,
            session_id="default",
            request_id="req-1",
            requestToolResults=[
                {"toolName": "aggregateDatasetToH3", "success": True, "details": "ok"},
                {"toolName": "waitForQMapDataset", "success": True, "details": "ok"},
                {"toolName": "countQMapRows", "success": True, "details": "ok"},
                {"toolName": "showOnlyQMapLayer", "success": True, "details": "ok"},
            ],
            responseToolCalls=["showOnlyQMapLayer"],
            responseText=None,
        )
        metrics = event.get("qualityMetrics") or {}
        self.assertEqual(metrics.get("hasDatasetMutation"), True)
        self.assertEqual(metrics.get("postCreateWaitCountOk"), True)
        self.assertEqual(metrics.get("finalLayerIsolatedAfterCount"), True)
        self.assertEqual(metrics.get("pendingIsolationAfterCount"), False)
        self.assertEqual(metrics.get("waitTimeoutCount"), 0)
        self.assertGreaterEqual(metrics.get("workflowScore", 0), 80)

    def test_build_chat_audit_event_flags_pending_isolation_after_count(self):
        started = time.perf_counter() - 0.2
        event = _build_chat_audit_event(
            endpoint="/chat/completions",
            status=200,
            started_at=started,
            session_id="default",
            request_id="req-2",
            requestToolResults=[
                {"toolName": "clipQMapDatasetByGeometry", "success": True, "details": "ok"},
                {"toolName": "waitForQMapDataset", "success": True, "details": "ok"},
                {"toolName": "countQMapRows", "success": True, "details": "ok"},
            ],
            responseToolCalls=["waitForQMapDataset"],
            responseText=None,
        )
        metrics = event.get("qualityMetrics") or {}
        self.assertEqual(metrics.get("hasDatasetMutation"), True)
        self.assertEqual(metrics.get("postCreateWaitCountOk"), True)
        self.assertEqual(metrics.get("finalLayerIsolatedAfterCount"), False)
        self.assertEqual(metrics.get("pendingIsolationAfterCount"), True)

    def test_quality_metrics_counts_response_tool_calls_from_object_shape(self):
        started = time.perf_counter() - 0.2
        event = _build_chat_audit_event(
            endpoint="/chat/completions",
            status=200,
            started_at=started,
            session_id="default",
            request_id="req-3",
            requestToolResults=[],
            responseToolCalls=[
                {"id": "call_1", "type": "function", "function": {"name": "waitForQMapDataset"}},
                {"id": "call_2", "type": "function", "function": {"name": "countQMapRows"}},
            ],
            responseText=None,
        )
        metrics = event.get("qualityMetrics") or {}
        self.assertEqual(metrics.get("responseToolCallCount"), 2)

    def test_build_chat_audit_event_exposes_stable_parse_envelope(self):
        started = time.perf_counter() - 0.2
        event = _build_chat_audit_event(
            endpoint="/chat/completions",
            status=503,
            started_at=started,
            session_id="sess-1",
            request_id="req-parse-1",
            requestToolResults=[
                {"toolName": "waitForQMapDataset", "success": True, "details": "ok"},
                {"toolName": "countQMapRows", "success": False, "details": "timeout"},
            ],
            responseToolCalls=["countQMapRows"],
            error="upstream timeout",
        )
        self.assertEqual(event.get("auditSchema"), "qmap.chat_audit.v1")
        self.assertEqual(event.get("eventType"), "chat.audit")
        self.assertEqual(event.get("service"), "q-assistant")
        self.assertEqual(event.get("outcome"), "error")
        self.assertEqual(event.get("chatId"), "sess-1")
        self.assertEqual(event.get("responseToolCallNames"), ["countQMapRows"])
        normalized_calls = event.get("responseToolCallsNormalized") or []
        self.assertEqual(len(normalized_calls), 1)
        self.assertEqual(normalized_calls[0].get("function", {}).get("name"), "countQMapRows")
        summary = event.get("requestToolResultsSummary") or {}
        self.assertEqual(summary.get("total"), 2)
        self.assertEqual(summary.get("success"), 1)
        self.assertEqual(summary.get("failed"), 1)
        self.assertEqual(summary.get("unknown"), 0)

    def test_resolve_audit_session_id_falls_back_to_payload_chat_id(self):
        resolved = _resolve_audit_session_id(
            None,
            {"chatId": "chat-xyz"},
            None,
        )
        self.assertEqual(resolved, "chat-xyz")

    def test_quality_metrics_cloud_recovery_chain_after_cloud_failure(self):
        started = time.perf_counter() - 0.2
        event = _build_chat_audit_event(
            endpoint="/chat/completions",
            status=200,
            started_at=started,
            session_id="default",
            request_id="req-cloud-recovery-1",
            requestToolResults=[
                {
                    "toolName": "loadCloudMapAndWait",
                    "success": False,
                    "details": "Timeout waiting for cloud map data.",
                },
                {
                    "toolName": "loadData",
                    "success": True,
                    "details": 'Loaded dataset "Contaminazione_Provincia".',
                },
                {
                    "toolName": "waitForQMapDataset",
                    "success": True,
                    "details": 'Dataset "Contaminazione_Provincia" available.',
                },
                {
                    "toolName": "countQMapRows",
                    "success": True,
                    "details": "Counted 154 rows.",
                },
            ],
            responseToolCalls=[],
            responseText=(
                "Caricamento cloud con timeout gestito: completato fallback su loadData "
                "e validata disponibilita del dataset."
            ),
        )
        metrics = event.get("qualityMetrics") or {}
        self.assertEqual(metrics.get("hasDatasetMutation"), False)
        self.assertEqual(metrics.get("postCreateWaitOk"), False)
        self.assertEqual(metrics.get("postCreateWaitCountOk"), False)
        self.assertEqual(metrics.get("cloudFailureSeen"), True)
        self.assertEqual(metrics.get("cloudRecoveryValidated"), True)
        self.assertEqual(metrics.get("cloudFailureExhausted"), False)
        self.assertEqual(metrics.get("responseHasText"), True)
        self.assertEqual(metrics.get("falseSuccessClaimCount"), 0)
        self.assertGreaterEqual(metrics.get("workflowScore", 0), 90)

    def test_quality_metrics_cloud_failure_exhausted_without_validated_recovery(self):
        started = time.perf_counter() - 0.2
        event = _build_chat_audit_event(
            endpoint="/chat/completions",
            status=200,
            started_at=started,
            session_id="default",
            request_id="req-cloud-recovery-2",
            requestToolResults=[
                {
                    "toolName": "loadCloudMapAndWait",
                    "success": False,
                    "details": "Cloud map load timed out after retry; no validated fallback available.",
                }
            ],
            responseToolCalls=[],
            responseText="Non e stato possibile completare il caricamento cloud in modo affidabile.",
        )
        metrics = event.get("qualityMetrics") or {}
        self.assertEqual(metrics.get("cloudFailureSeen"), True)
        self.assertEqual(metrics.get("cloudFailureExhausted"), True)
        self.assertEqual(metrics.get("cloudRecoveryValidated"), False)

    def test_quality_metrics_extract_runtime_response_mode_hint_from_request_payload(self):
        started = time.perf_counter() - 0.2
        event = _build_chat_audit_event(
            endpoint="/chat/completions",
            status=200,
            started_at=started,
            session_id="default",
            request_id="req-mode-hint-1",
            requestPayload={
                "messages": [
                    {
                        "role": "system",
                        "content": "System\n[RUNTIME_RESPONSE_MODE] clarification",
                    }
                ]
            },
            requestToolResults=[
                {"toolName": "listQCumberDatasets", "success": True, "details": "Listed datasets."}
            ],
            responseToolCalls=[],
            responseText="Quale datasetId vuoi usare?",
        )
        metrics = event.get("qualityMetrics") or {}
        self.assertEqual(metrics.get("responseModeHint"), "clarification")

    def test_quality_metrics_marks_catalog_selection_pending_as_clarification(self):
        started = time.perf_counter() - 0.2
        event = _build_chat_audit_event(
            endpoint="/chat/completions",
            status=200,
            started_at=started,
            session_id="default",
            request_id="req-mode-hint-2",
            requestToolResults=[
                {"toolName": "listQCumberProviders", "success": True, "details": "Found providers."},
                {"toolName": "listQCumberDatasets", "success": True, "details": "Found datasets."},
            ],
            responseToolCalls=[],
            responseText="Quale datasetId vuoi usare?",
        )
        metrics = event.get("qualityMetrics") or {}
        self.assertEqual(metrics.get("responseModeHint"), "clarification")

    def test_quality_metrics_marks_ambiguous_admin_match_as_pending_clarification(self):
        started = time.perf_counter() - 0.2
        event = _build_chat_audit_event(
            endpoint="/chat/completions",
            status=200,
            started_at=started,
            session_id="default",
            request_id="req-mode-hint-3",
            requestToolResults=[
                {
                    "toolName": "queryQCumberTerritorialUnits",
                    "success": False,
                    "clarificationRequired": True,
                    "clarificationQuestion": "Vuoi provincia, comune, regione o stato?",
                    "clarificationOptions": ["province", "municipality", "region", "country"],
                    "details": (
                        "Ambiguous administrative match for name filter (Salerno). "
                        "Matched multiple levels (7, 9). "
                        "Retry with expectedAdminType (province/municipality/region/country) "
                        "or add explicit lv filter."
                    ),
                }
            ],
            responseToolCalls=[],
            responseText="Vuoi provincia, comune, regione o stato?",
        )
        metrics = event.get("qualityMetrics") or {}
        self.assertEqual(metrics.get("responseModeHint"), "clarification")
        self.assertEqual(metrics.get("clarificationPending"), True)
        self.assertEqual(metrics.get("clarificationReason"), "ambiguous_admin_match")
        self.assertEqual(metrics.get("clarificationQuestionSeen"), True)
        self.assertEqual(metrics.get("clarificationOptionsCount"), 4)

    def test_quality_metrics_does_not_treat_admin_level_validation_failure_as_clarification(self):
        started = time.perf_counter() - 0.2
        event = _build_chat_audit_event(
            endpoint="/chat/completions",
            status=200,
            started_at=started,
            session_id="default",
            request_id="req-mode-hint-4",
            requestToolResults=[
                {
                    "toolName": "queryQCumberTerritorialUnits",
                    "success": False,
                    "details": (
                        'Administrative level mismatch after strict filtering: expected municipality (lv=9) '
                        'on field "lv".'
                    ),
                }
            ],
            responseToolCalls=[],
            responseText="Non posso confermare il comune richiesto: la validazione del livello amministrativo non e riuscita.",
        )
        metrics = event.get("qualityMetrics") or {}
        self.assertNotEqual(metrics.get("responseModeHint"), "clarification")
        self.assertEqual(metrics.get("clarificationPending"), False)
        self.assertIsNone(metrics.get("clarificationReason"))

    def test_quality_metrics_ranking_evidence_chain_marks_rank_completion(self):
        started = time.perf_counter() - 0.2
        event = _build_chat_audit_event(
            endpoint="/chat/completions",
            status=200,
            started_at=started,
            session_id="default",
            request_id="req-ranking-evidence-1",
            requestToolResults=[
                {
                    "toolName": "createDatasetFromFilter",
                    "success": True,
                    "details": 'Created dataset "Aree_Critiche".',
                },
                {
                    "toolName": "waitForQMapDataset",
                    "success": True,
                    "details": 'Dataset "Aree_Critiche" available.',
                },
                {
                    "toolName": "countQMapRows",
                    "success": True,
                    "details": "Counted 42 rows.",
                },
                {
                    "toolName": "rankQMapDatasetRows",
                    "success": True,
                    "details": 'Ranked top 10 rows from "Aree_Critiche" by "indice_pressione" (desc).',
                },
            ],
            responseToolCalls=[],
            responseText="Top 3 aree critiche ordinate per indice_pressione: A, B, C.",
        )
        metrics = event.get("qualityMetrics") or {}
        self.assertEqual(metrics.get("hasDatasetMutation"), True)
        self.assertEqual(metrics.get("postCreateWaitCountRankOk"), True)
        self.assertEqual(metrics.get("responseHasText"), True)
        self.assertEqual(metrics.get("falseSuccessClaimCount"), 0)
        self.assertGreaterEqual(metrics.get("workflowScore", 0), 80)

    def test_quality_metrics_flags_false_centering_claim_without_fit_success(self):
        started = time.perf_counter() - 0.2
        event = _build_chat_audit_event(
            endpoint="/chat/completions",
            status=200,
            started_at=started,
            session_id="default",
            request_id="req-4",
            requestToolResults=[
                {"toolName": "queryQCumberDatasetSpatial", "success": True, "details": "Loaded dataset."},
                {
                    "toolName": "fitQMapToDataset",
                    "success": False,
                    "details": (
                        "Hard-enforce turn state: discovery step is mandatory. "
                        "Call listQMapDatasets first."
                    ),
                },
            ],
            responseToolCalls=[],
            responseText="Ho caricato i dati e la mappa è stata centrata sull'area richiesta.",
        )
        metrics = event.get("qualityMetrics") or {}
        self.assertEqual(metrics.get("falseSuccessClaimCount"), 1)
        self.assertIn("centering_without_fit_success", metrics.get("falseSuccessClaimRules") or [])

    def test_quality_metrics_allows_centering_claim_with_fit_success(self):
        started = time.perf_counter() - 0.2
        event = _build_chat_audit_event(
            endpoint="/chat/completions",
            status=200,
            started_at=started,
            session_id="default",
            request_id="req-5",
            requestToolResults=[
                {"toolName": "queryQCumberDatasetSpatial", "success": True, "details": "Loaded dataset."},
                {"toolName": "fitQMapToDataset", "success": True, "details": "Map centered to dataset extent."},
            ],
            responseToolCalls=[],
            responseText="Ho caricato i dati e la mappa è stata centrata sull'area richiesta.",
        )
        metrics = event.get("qualityMetrics") or {}
        self.assertEqual(metrics.get("falseSuccessClaimCount"), 0)
        self.assertEqual(metrics.get("falseSuccessClaimRules"), [])

    def test_quality_metrics_does_not_treat_hierarchical_framing_as_map_centering(self):
        started = time.perf_counter() - 0.2
        event = _build_chat_audit_event(
            endpoint="/chat/completions",
            status=200,
            started_at=started,
            session_id="default",
            request_id="req-5b",
            requestToolResults=[
                {
                    "toolName": "queryQCumberTerritorialUnits",
                    "success": True,
                    "details": "Loaded territorial units.",
                }
            ],
            responseToolCalls=[],
            responseText=(
                "La ricerca delle unita territoriali e stata completata con conferma del corretto "
                "inquadramento gerarchico nel risultato finale."
            ),
        )
        metrics = event.get("qualityMetrics") or {}
        self.assertEqual(metrics.get("falseSuccessClaimCount"), 0)
        self.assertNotIn("centering_without_fit_success", metrics.get("falseSuccessClaimRules") or [])

    def test_quality_metrics_flags_success_claim_with_all_tools_failed(self):
        started = time.perf_counter() - 0.2
        event = _build_chat_audit_event(
            endpoint="/chat/completions",
            status=200,
            started_at=started,
            session_id="default",
            request_id="req-6",
            requestToolResults=[
                {"toolName": "queryQCumberDataset", "success": False, "details": "Hard-enforce turn state..."},
                {"toolName": "waitForQMapDataset", "success": False, "details": "Dataset not found."},
            ],
            responseToolCalls=[],
            responseText="Ho completato il caricamento e visualizzato correttamente i risultati.",
        )
        metrics = event.get("qualityMetrics") or {}
        self.assertGreaterEqual(metrics.get("falseSuccessClaimCount", 0), 1)
        self.assertIn("success_claim_with_all_tools_failed", metrics.get("falseSuccessClaimRules") or [])

    def test_prune_chat_audit_dir_keeps_newest_files(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            base = os.path.abspath(tmp_dir)
            p1 = os.path.join(base, "session-a.jsonl")
            p2 = os.path.join(base, "session-b.jsonl")
            p3 = os.path.join(base, "session-c.jsonl")
            for path in (p1, p2, p3):
                with open(path, "w", encoding="utf-8") as handle:
                    handle.write("{}\n")
            now = time.time()
            os.utime(p1, (now - 300, now - 300))
            os.utime(p2, (now - 200, now - 200))
            os.utime(p3, (now - 100, now - 100))

            from pathlib import Path

            _prune_chat_audit_dir(Path(base), max_files=2, max_age_days=0)
            remaining = sorted(os.listdir(base))
            self.assertEqual(remaining, ["session-b.jsonl", "session-c.jsonl"])

    def test_prune_chat_audit_dir_removes_expired_files(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            base = os.path.abspath(tmp_dir)
            old_path = os.path.join(base, "session-old.jsonl")
            fresh_path = os.path.join(base, "session-fresh.jsonl")
            for path in (old_path, fresh_path):
                with open(path, "w", encoding="utf-8") as handle:
                    handle.write("{}\n")
            now = time.time()
            os.utime(old_path, (now - 3 * 86400, now - 3 * 86400))
            os.utime(fresh_path, (now - 60, now - 60))

            from pathlib import Path

            _prune_chat_audit_dir(Path(base), max_files=0, max_age_days=1)
            remaining = sorted(os.listdir(base))
            self.assertEqual(remaining, ["session-fresh.jsonl"])


if __name__ == "__main__":
    unittest.main()
