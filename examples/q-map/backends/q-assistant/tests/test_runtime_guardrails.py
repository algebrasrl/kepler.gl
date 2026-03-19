import json
import unittest

from tests.runtime_guardrails_injection_boundary_clip_coverage_mixin import (
    RuntimeGuardrailInjectionBoundaryClipCoverageMixin,
)
from tests.runtime_guardrails_injection_fit_focus_mixin import (
    RuntimeGuardrailInjectionFitFocusMixin,
)
from tests.runtime_guardrails_injection_post_create_mixin import (
    RuntimeGuardrailInjectionPostCreateMixin,
)
from tests.runtime_guardrails_post_filter_force_fit_mixin import (
    RuntimeGuardrailPostFilterForceFitMixin,
)
from tests.runtime_guardrails_injection_metric_recovery_mixin import (
    RuntimeGuardrailInjectionMetricRecoveryMixin,
)
from tests.runtime_guardrails_injection_ranking_evidence_mixin import (
    RuntimeGuardrailInjectionRankingEvidenceMixin,
)
from tests.runtime_guardrails_loop_limits_recovery_mixin import (
    RuntimeGuardrailLoopLimitsRecoveryMixin,
)
from tests.runtime_guardrails_loop_limits_routing_mixin import (
    RuntimeGuardrailLoopLimitsRoutingMixin,
)
from tests.runtime_guardrails_loop_limits_wait_tooltip_mixin import (
    RuntimeGuardrailLoopLimitsWaitTooltipMixin,
)
from q_assistant.objective_intent import _objective_requires_ranked_output
from q_assistant.runtime_guardrails import objective_requests_map_display as _objective_requests_map_display
from q_assistant.services.request_processor import (
    _classify_runtime_error_kind,
    _derive_runtime_quality_metrics,
    _objective_targets_admin_units,
    _runtime_error_retry_policy,
)
from tests.runtime_guardrails_test_support import (
    _enforce_runtime_tool_loop_limits,
    _extract_request_tool_results,
    _inject_runtime_guardrail_message,
    _is_likely_normalized_metric_field,
    _objective_requests_normalized_metric,
    _prune_forbidden_qmap_runtime_tools,
    _prune_heavy_recompute_tools_after_low_distinct_color_failure,
    _prune_open_panel_only_chart_navigation,
    _prune_population_style_tools_for_unresolved_value_coloring,
    _prune_repeated_discovery_tools,
    _prune_sampling_preview_tools_for_superlatives,
    _prune_uninformative_chart_tools_for_ranking,
    _qmap_tool_result,
    _summarize_runtime_tool_policy,
    build_runtime_workflow_state,
)


class RuntimeGuardrailInjectionTests(
    RuntimeGuardrailInjectionFitFocusMixin,
    RuntimeGuardrailInjectionBoundaryClipCoverageMixin,
    RuntimeGuardrailInjectionRankingEvidenceMixin,
    RuntimeGuardrailInjectionMetricRecoveryMixin,
    RuntimeGuardrailInjectionPostCreateMixin,
    RuntimeGuardrailPostFilterForceFitMixin,
    RuntimeGuardrailLoopLimitsRecoveryMixin,
    RuntimeGuardrailLoopLimitsRoutingMixin,
    RuntimeGuardrailLoopLimitsWaitTooltipMixin,
    unittest.TestCase,
):
    def test_runtime_workflow_state_detects_admin_superlative_from_metadata_and_rank(self):
        results = [
            {
                "toolName": "queryQCumberTerritorialUnits",
                "success": True,
                "routingIsAdministrative": True,
                "datasetClass": "administrative",
                "routingPreferredTool": "queryQCumberTerritorialUnits",
            },
            {"toolName": "rankQMapDatasetRows", "success": True},
        ]
        state = build_runtime_workflow_state(
            results=results,
            objective_text="continua",
            objective_targets_admin_units=_objective_targets_admin_units,
            objective_requests_map_display=_objective_requests_map_display,
            objective_requires_ranked_output=_objective_requires_ranked_output,
        )
        self.assertTrue(state.has_admin_workflow_signal)
        self.assertTrue(state.has_rank_workflow_signal)
        self.assertTrue(state.ranking_active)
        self.assertTrue(state.admin_superlative_map_workflow)
        self.assertTrue(state.preserve_fit_without_explicit_map_focus)

    def test_runtime_workflow_state_keeps_objective_only_admin_map_display(self):
        state = build_runtime_workflow_state(
            results=[],
            objective_text="mostra il comune piu piccolo della provincia di treviso sulla mappa",
            objective_targets_admin_units=_objective_targets_admin_units,
            objective_requests_map_display=_objective_requests_map_display,
            objective_requires_ranked_output=_objective_requires_ranked_output,
        )
        self.assertFalse(state.has_admin_workflow_signal)
        self.assertFalse(state.has_rank_workflow_signal)
        self.assertTrue(state.admin_map_display_objective)
        self.assertTrue(state.ranking_active)
        self.assertTrue(state.admin_superlative_map_workflow)

    def test_runtime_workflow_state_preserves_fit_for_tessellation(self):
        results = [
            {"toolName": "tassellateDatasetLayer", "success": True},
        ]
        state = build_runtime_workflow_state(
            results=results,
            objective_text="tassellami la provincia di treviso a risoluzione 8",
            objective_targets_admin_units=_objective_targets_admin_units,
            objective_requests_map_display=_objective_requests_map_display,
            objective_requires_ranked_output=_objective_requires_ranked_output,
        )
        self.assertTrue(state.has_mutation_workflow_signal)
        self.assertTrue(state.preserve_fit_without_explicit_map_focus)

    def test_runtime_workflow_state_preserves_fit_for_aggregate(self):
        results = [
            {"toolName": "aggregateDatasetToH3", "success": True},
        ]
        state = build_runtime_workflow_state(
            results=results,
            objective_text="aggrega a risoluzione 9",
            objective_targets_admin_units=_objective_targets_admin_units,
            objective_requests_map_display=_objective_requests_map_display,
            objective_requires_ranked_output=_objective_requires_ranked_output,
        )
        self.assertTrue(state.has_mutation_workflow_signal)
        self.assertTrue(state.preserve_fit_without_explicit_map_focus)

    def test_runtime_workflow_state_no_mutation_signal_without_results(self):
        state = build_runtime_workflow_state(
            results=[],
            objective_text="tassellami la provincia di treviso",
            objective_targets_admin_units=_objective_targets_admin_units,
            objective_requests_map_display=_objective_requests_map_display,
            objective_requires_ranked_output=_objective_requires_ranked_output,
        )
        self.assertFalse(state.has_mutation_workflow_signal)
        self.assertFalse(state.preserve_fit_without_explicit_map_focus)

    def test_extract_request_tool_results_includes_contract_expected_schema(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {"role": "user", "content": "ranking"},
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_rank",
                            "type": "function",
                            "function": {"name": "rankQMapDatasetRows", "arguments": "{}"},
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_rank",
                    "content": _qmap_tool_result(success=True, details="Ranking ok."),
                },
            ]
        }
        rows = _extract_request_tool_results(payload)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].get("toolName"), "rankQMapDatasetRows")
        self.assertEqual(rows[0].get("contractExpectedSchema"), "qmap.tool_result.v1")
        self.assertEqual(rows[0].get("contractSchemaMismatch"), False)

    def test_extract_request_tool_results_flags_contract_schema_mismatch(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {"role": "user", "content": "ranking"},
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_rank",
                            "type": "function",
                            "function": {"name": "rankQMapDatasetRows", "arguments": "{}"},
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_rank",
                    "content": _qmap_tool_result(
                        success=False,
                        details="Legacy envelope mismatch.",
                        schema="qmap.tool_result.v0",
                    ),
                },
            ]
        }
        rows = _extract_request_tool_results(payload)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].get("contractExpectedSchema"), "qmap.tool_result.v1")
        self.assertEqual(rows[0].get("contractSchemaMismatch"), True)

    def test_extract_request_tool_results_keeps_clarification_payload(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {"role": "user", "content": "Cerca Salerno senza assumere il livello"},
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_query",
                            "type": "function",
                            "function": {"name": "queryQCumberTerritorialUnits", "arguments": "{}"},
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_query",
                    "content": json.dumps(
                        {
                            "llmResult": {
                                "success": False,
                                "clarificationRequired": True,
                                "clarificationQuestion": "Vuoi provincia o comune?",
                                "clarificationOptions": ["province", "municipality"],
                                "details": (
                                    "Ambiguous administrative match for name filter (Salerno). "
                                    "Matched multiple levels (7, 9). "
                                    "Retry with expectedAdminType (province/municipality/region/country) "
                                    "or add explicit lv filter."
                                ),
                            }
                        }
                    ),
                },
            ]
        }
        rows = _extract_request_tool_results(payload)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].get("toolName"), "queryQCumberTerritorialUnits")
        self.assertEqual(rows[0].get("clarificationRequired"), True)
        self.assertEqual(rows[0].get("clarificationQuestion"), "Vuoi provincia o comune?")
        self.assertEqual(rows[0].get("clarificationOptions"), ["province", "municipality"])

    def test_blocks_repeated_discovery_loop_and_requests_query_progress(self):
        messages = [
            {"role": "system", "content": "System"},
            {"role": "user", "content": "mostra comuni e colorali per siti contaminati"},
        ]
        for idx in range(1, 4):
            provider_call_id = f"call_provider_{idx}"
            dataset_call_id = f"call_dataset_{idx}"
            messages.extend(
                [
                    {
                        "role": "assistant",
                        "tool_calls": [
                            {
                                "id": provider_call_id,
                                "type": "function",
                                "function": {"name": "listQCumberProviders", "arguments": "{}"},
                            }
                        ],
                    },
                    {
                        "role": "tool",
                        "tool_call_id": provider_call_id,
                        "content": _qmap_tool_result(success=True, details="Found providers."),
                    },
                    {
                        "role": "assistant",
                        "tool_calls": [
                            {
                                "id": dataset_call_id,
                                "type": "function",
                                "function": {
                                    "name": "listQCumberDatasets",
                                    "arguments": json.dumps({"providerId": "local-assets-it"}),
                                },
                            }
                        ],
                    },
                    {
                        "role": "tool",
                        "tool_call_id": dataset_call_id,
                        "content": _qmap_tool_result(success=True, details="Found datasets."),
                    },
                ]
            )

        payload = {
            "messages": messages,
            "tools": [
                {"type": "function", "function": {"name": "listQCumberProviders"}},
                {"type": "function", "function": {"name": "listQCumberDatasets"}},
                {"type": "function", "function": {"name": "queryQCumberDataset"}},
            ],
        }
        out = _inject_runtime_guardrail_message(payload)
        content = str(out["messages"][0]["content"])
        self.assertIn("Repeated discovery-only loop detected", content)
        self.assertIn("Stop repeating discovery calls", content)
        self.assertIn("queryQCumberDataset", content)

    def test_repeated_discovery_loop_without_query_tools_emits_clarification_mode_hint(self):
        messages = [
            {"role": "system", "content": "System"},
            {"role": "user", "content": "continua con il catalogo"},
        ]
        for idx in range(1, 4):
            provider_call_id = f"call_provider_only_{idx}"
            dataset_call_id = f"call_dataset_only_{idx}"
            messages.extend(
                [
                    {
                        "role": "assistant",
                        "tool_calls": [
                            {
                                "id": provider_call_id,
                                "type": "function",
                                "function": {"name": "listQCumberProviders", "arguments": "{}"},
                            }
                        ],
                    },
                    {
                        "role": "tool",
                        "tool_call_id": provider_call_id,
                        "content": _qmap_tool_result(success=True, details="Found providers."),
                    },
                    {
                        "role": "assistant",
                        "tool_calls": [
                            {
                                "id": dataset_call_id,
                                "type": "function",
                                "function": {
                                    "name": "listQCumberDatasets",
                                    "arguments": json.dumps({"providerId": "local-assets-it"}),
                                },
                            }
                        ],
                    },
                    {
                        "role": "tool",
                        "tool_call_id": dataset_call_id,
                        "content": _qmap_tool_result(success=True, details="Found datasets."),
                    },
                ]
            )

        payload = {
            "messages": messages,
            "tools": [
                {"type": "function", "function": {"name": "listQCumberProviders"}},
                {"type": "function", "function": {"name": "listQCumberDatasets"}},
            ],
        }
        out = _inject_runtime_guardrail_message(payload)
        content = str(out["messages"][0]["content"])
        self.assertIn("Repeated discovery-only loop detected", content)
        self.assertIn("[RUNTIME_RESPONSE_MODE] clarification", content)

    def test_loop_limits_finalize_when_tool_requires_named_place_clarification(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {"role": "user", "content": "Cerca Salerno ma non assumere se e comune o provincia"},
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_query",
                            "type": "function",
                            "function": {"name": "queryQCumberTerritorialUnits", "arguments": "{}"},
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_query",
                    "content": json.dumps(
                        {
                            "llmResult": {
                                "success": False,
                                "clarificationRequired": True,
                                "clarificationQuestion": "Vuoi provincia, comune, regione o stato?",
                                "clarificationOptions": [
                                    "province",
                                    "municipality",
                                    "region",
                                    "country",
                                ],
                                "details": (
                                    "Ambiguous administrative match for name filter (Salerno). "
                                    "Matched multiple levels (7, 9). "
                                    "Retry with expectedAdminType (province/municipality/region/country) "
                                    "or add explicit lv filter."
                                ),
                            }
                        }
                    ),
                },
            ],
            "tools": [
                {"type": "function", "function": {"name": "queryQCumberTerritorialUnits"}},
                {"type": "function", "function": {"name": "rankQMapDatasetRows"}},
                {"type": "function", "function": {"name": "fitQMapToDataset"}},
            ],
            "tool_choice": "auto",
        }
        out = _enforce_runtime_tool_loop_limits(payload)
        self.assertEqual(out.get("tools"), [])
        self.assertEqual(out.get("tool_choice"), "none")
        content = str(out["messages"][0]["content"])
        self.assertIn("clarification_required_finalize", content)
        self.assertIn("Vuoi provincia, comune, regione o stato?", content)
        self.assertIn("[RUNTIME_RESPONSE_MODE] clarification", content)

    def test_prunes_discovery_tools_after_repeated_loop_when_query_tools_exist(self):
        messages = [
            {"role": "system", "content": "System"},
            {"role": "user", "content": "trova siti contaminati in italia"},
        ]
        for idx in range(1, 4):
            provider_call_id = f"call_provider_{idx}"
            dataset_call_id = f"call_dataset_{idx}"
            messages.extend(
                [
                    {
                        "role": "assistant",
                        "tool_calls": [
                            {
                                "id": provider_call_id,
                                "type": "function",
                                "function": {"name": "listQCumberProviders", "arguments": "{}"},
                            }
                        ],
                    },
                    {
                        "role": "tool",
                        "tool_call_id": provider_call_id,
                        "content": _qmap_tool_result(success=True, details="Found providers."),
                    },
                    {
                        "role": "assistant",
                        "tool_calls": [
                            {
                                "id": dataset_call_id,
                                "type": "function",
                                "function": {
                                    "name": "listQCumberDatasets",
                                    "arguments": json.dumps({"providerId": "local-assets-it"}),
                                },
                            }
                        ],
                    },
                    {
                        "role": "tool",
                        "tool_call_id": dataset_call_id,
                        "content": _qmap_tool_result(success=True, details="Found datasets."),
                    },
                ]
            )

        payload = {
            "messages": messages,
            "tools": [
                {"type": "function", "function": {"name": "listQCumberProviders"}},
                {"type": "function", "function": {"name": "listQCumberDatasets"}},
                {"type": "function", "function": {"name": "queryQCumberDatasetSpatial"}},
            ],
        }
        out = _prune_repeated_discovery_tools(payload)
        names = [tool.get("function", {}).get("name") for tool in out.get("tools", [])]
        self.assertNotIn("listQCumberProviders", names)
        self.assertNotIn("listQCumberDatasets", names)
        self.assertIn("queryQCumberDatasetSpatial", names)

    def test_does_not_prune_when_no_discovery_loop(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {"role": "user", "content": "mostra comuni"},
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_provider",
                            "type": "function",
                            "function": {"name": "listQCumberProviders", "arguments": "{}"},
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_provider",
                    "content": _qmap_tool_result(success=True, details="Found providers."),
                },
            ],
            "tools": [
                {"type": "function", "function": {"name": "listQCumberProviders"}},
                {"type": "function", "function": {"name": "listQCumberDatasets"}},
                {"type": "function", "function": {"name": "queryQCumberDatasetSpatial"}},
            ],
        }
        out = _prune_repeated_discovery_tools(payload)
        names = [tool.get("function", {}).get("name") for tool in out.get("tools", [])]
        self.assertIn("listQCumberProviders", names)
        self.assertIn("listQCumberDatasets", names)
        self.assertIn("queryQCumberDatasetSpatial", names)

    def test_prunes_forbidden_qmap_runtime_tools(self):
        payload = {
            "messages": [{"role": "user", "content": "mostra siti contaminati"}],
            "tools": [
                {"type": "function", "function": {"name": "tableTool"}},
                {"type": "function", "function": {"name": "mergeTablesTool"}},
                {"type": "function", "function": {"name": "spatialJoinByPredicate"}},
            ],
        }
        out = _prune_forbidden_qmap_runtime_tools(payload)
        names = [tool.get("function", {}).get("name") for tool in out.get("tools", [])]
        self.assertNotIn("tableTool", names)
        self.assertNotIn("mergeTablesTool", names)
        self.assertIn("spatialJoinByPredicate", names)

    def test_prunes_name_only_category_chart_tool_for_ranking_objective(self):
        payload = {
            "messages": [{"role": "user", "content": "mostrami le regioni con più problemi e grafici"}],
            "tools": [
                {"type": "function", "function": {"name": "rankQMapDatasetRows"}},
                {"type": "function", "function": {"name": "bubbleChartTool"}},
                {"type": "function", "function": {"name": "categoryBarsTool"}},
            ],
        }
        out = _prune_uninformative_chart_tools_for_ranking(payload)
        names = [tool.get("function", {}).get("name") for tool in out.get("tools", [])]
        self.assertIn("rankQMapDatasetRows", names)
        self.assertIn("bubbleChartTool", names)
        self.assertNotIn("categoryBarsTool", names)

    def test_prunes_open_panel_when_chart_execution_tools_are_available(self):
        payload = {
            "messages": [{"role": "user", "content": "crea un grafico della distribuzione dei valori"}],
            "tools": [
                {"type": "function", "function": {"name": "openQMapPanel"}},
                {"type": "function", "function": {"name": "categoryBarsTool"}},
                {"type": "function", "function": {"name": "histogramTool"}},
            ],
        }
        out = _prune_open_panel_only_chart_navigation(payload)
        names = [tool.get("function", {}).get("name") for tool in out.get("tools", [])]
        self.assertNotIn("openQMapPanel", names)
        self.assertIn("categoryBarsTool", names)
        self.assertIn("histogramTool", names)

    def test_keeps_open_panel_when_no_chart_execution_tool_is_available(self):
        payload = {
            "messages": [{"role": "user", "content": "apri il pannello grafici"}],
            "tools": [
                {"type": "function", "function": {"name": "openQMapPanel"}},
                {"type": "function", "function": {"name": "listQMapChartTools"}},
            ],
        }
        out = _prune_open_panel_only_chart_navigation(payload)
        names = [tool.get("function", {}).get("name") for tool in out.get("tools", [])]
        self.assertIn("openQMapPanel", names)
        self.assertIn("listQMapChartTools", names)

    def test_prunes_open_panel_when_panel_navigation_was_not_requested(self):
        payload = {
            "messages": [{"role": "user", "content": "carica il dataset finale e centra la mappa"}],
            "tools": [
                {"type": "function", "function": {"name": "openQMapPanel"}},
                {"type": "function", "function": {"name": "fitQMapToDataset"}},
                {"type": "function", "function": {"name": "listQMapDatasets"}},
            ],
        }
        out = _prune_open_panel_only_chart_navigation(payload)
        names = [tool.get("function", {}).get("name") for tool in out.get("tools", [])]
        self.assertNotIn("openQMapPanel", names)
        self.assertIn("fitQMapToDataset", names)
        self.assertIn("listQMapDatasets", names)

    def test_keeps_category_chart_tool_when_user_explicitly_requests_category_distribution(self):
        payload = {
            "messages": [{"role": "user", "content": "mostra classifica per categorie e distribuzione"}],
            "tools": [
                {"type": "function", "function": {"name": "rankQMapDatasetRows"}},
                {"type": "function", "function": {"name": "categoryBarsTool"}},
            ],
        }
        out = _prune_uninformative_chart_tools_for_ranking(payload)
        names = [tool.get("function", {}).get("name") for tool in out.get("tools", [])]
        self.assertIn("categoryBarsTool", names)

    def test_prunes_heavy_recompute_tools_after_low_distinct_color_failure(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {"role": "user", "content": "colora per area critica"},
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_color",
                            "type": "function",
                            "function": {
                                "name": "setQMapLayerColorByField",
                                "arguments": json.dumps(
                                    {"datasetName": "Regioni_Pressione_CLC_Analisi", "fieldName": "pressione_ha"}
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
                            'Field "pressione_ha" has <=1 distinct numeric value in sampled rows, '
                            "so color scale would appear uniform. Stats: sampled=20, numeric=20, distinct=1."
                        ),
                    ),
                },
            ],
            "tools": [
                {"type": "function", "function": {"name": "queryQCumberDatasetSpatial"}},
                {"type": "function", "function": {"name": "zonalStatsByAdmin"}},
                {"type": "function", "function": {"name": "setQMapLayerSolidColor"}},
                {"type": "function", "function": {"name": "previewQMapDatasetRows"}},
            ],
        }
        out = _prune_heavy_recompute_tools_after_low_distinct_color_failure(payload)
        names = [tool.get("function", {}).get("name") for tool in out.get("tools", [])]
        self.assertNotIn("queryQCumberDatasetSpatial", names)
        self.assertNotIn("zonalStatsByAdmin", names)
        self.assertIn("setQMapLayerSolidColor", names)
        self.assertIn("previewQMapDatasetRows", names)

    def test_prunes_sampling_preview_tool_for_superlative_when_rank_available(self):
        payload = {
            "messages": [{"role": "user", "content": "mostra provincia italiana con meno boschi"}],
            "tools": [
                {"type": "function", "function": {"name": "previewQMapDatasetRows"}},
                {"type": "function", "function": {"name": "rankQMapDatasetRows"}},
            ],
        }
        out = _prune_sampling_preview_tools_for_superlatives(payload)
        names = [tool.get("function", {}).get("name") for tool in out.get("tools", [])]
        self.assertNotIn("previewQMapDatasetRows", names)
        self.assertIn("rankQMapDatasetRows", names)

    def test_keeps_sampling_preview_tool_when_metric_not_found_recovery_pending(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {"role": "user", "content": "mostra provincia italiana con meno boschi"},
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_rank",
                            "type": "function",
                            "function": {
                                "name": "rankQMapDatasetRows",
                                "arguments": json.dumps(
                                    {"datasetName": "Province_Boschi_Join", "metricFieldName": "area_ha_sum_sum"}
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
                        details='Metric field "area_ha_sum_sum" not found in dataset "Province_Boschi_Join".',
                    ),
                },
            ],
            "tools": [
                {"type": "function", "function": {"name": "previewQMapDatasetRows"}},
                {"type": "function", "function": {"name": "rankQMapDatasetRows"}},
            ],
        }
        out = _prune_sampling_preview_tools_for_superlatives(payload)
        names = [tool.get("function", {}).get("name") for tool in out.get("tools", [])]
        self.assertIn("previewQMapDatasetRows", names)
        self.assertIn("rankQMapDatasetRows", names)

    def test_low_distinct_color_guardrail_prevents_false_success_claims(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {"role": "user", "content": "colora per area critica"},
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_color",
                            "type": "function",
                            "function": {
                                "name": "setQMapLayerColorByField",
                                "arguments": json.dumps(
                                    {"datasetName": "Regioni_Pressione_CLC_Analisi", "fieldName": "pressione_ha"}
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
                            'Field "pressione_ha" has <=1 distinct numeric value in sampled rows, '
                            "so color scale would appear uniform. Stats: sampled=20, numeric=20, distinct=1."
                        ),
                    ),
                },
            ],
            "tools": [{"type": "function", "function": {"name": "setQMapLayerSolidColor"}}],
        }
        out = _inject_runtime_guardrail_message(payload)
        content = str(out["messages"][0]["content"])
        self.assertIn("color_low_distinct_no_false_success", content)
        self.assertIn("Do not claim that color-by-field was applied", content)
        self.assertIn("setQMapLayerSolidColor", content)

    def test_low_distinct_ranking_guardrail_blocks_alt_pipeline(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {"role": "user", "content": "mostrami le regioni con più problemi secondo CLC e mostra anche grafici"},
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
                            "id": "call_color",
                            "type": "function",
                            "function": {
                                "name": "setQMapLayerColorByField",
                                "arguments": json.dumps(
                                    {"datasetName": "Pressione_Ambientale_Regioni", "fieldName": "area_pressione_ha"}
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
                        details='Field "area_pressione_ha" has <=1 distinct numeric value in sampled rows, so color scale would appear uniform.',
                    ),
                },
            ],
            "tools": [{"type": "function", "function": {"name": "bubbleChartTool"}}],
        }
        out = _inject_runtime_guardrail_message(payload)
        content = str(out["messages"][0]["content"])
        self.assertIn("ranking_low_distinct_no_alt_pipeline", content)
        self.assertIn("Do not launch alternate heavy pipelines", content)

    def test_zonal_freeze_guardrail_requires_deterministic_h3_coarsening(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {"role": "user", "content": "mostra provincia italiana con meno boschi"},
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
                                        "adminDatasetName": "Kontur Boundaries Italia (query) [prov]",
                                        "valueDatasetName": "Boschi_H3_r6",
                                        "valueField": "sum",
                                        "aggregation": "sum",
                                    }
                                ),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_zonal",
                    "content": _qmap_tool_result(
                        success=False,
                        details=(
                            "zonalStatsByAdmin aborted to prevent UI freeze: estimated pair evaluations "
                            "767.250 exceed budget 300.000 (admin=110, values=6975, mode=area_weighted)."
                        ),
                    ),
                },
            ],
            "tools": [
                {"type": "function", "function": {"name": "aggregateDatasetToH3"}},
                {"type": "function", "function": {"name": "waitForQMapDataset"}},
                {"type": "function", "function": {"name": "zonalStatsByAdmin"}},
            ],
        }
        out = _inject_runtime_guardrail_message(payload)
        content = str(out["messages"][0]["content"])
        self.assertIn("zonal_freeze_deterministic_h3_fallback", content)
        self.assertIn("repeat deterministically", content)
        self.assertIn('aggregateDatasetToH3(datasetName="Boschi_H3_r6", resolution=5', content)

    def test_coloring_after_unresolved_forest_metric_blocks_population_fallback(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {"role": "user", "content": "mostra comune lombardo con meno boschi"},
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_clc",
                            "type": "function",
                            "function": {
                                "name": "queryQCumberDatasetSpatial",
                                "arguments": json.dumps(
                                    {
                                        "datasetId": "clc-2018-italia",
                                        "filters": [{"field": "code_18", "op": "in", "values": ["311", "312", "313"]}],
                                    }
                                ),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_clc",
                    "content": _qmap_tool_result(success=True, details="CLC forests loaded."),
                },
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
                                        "adminDatasetName": "Kontur Boundaries Italia (query) [lombardia]",
                                        "valueDatasetName": "CLC 2018 Italia (query) [forest]",
                                        "aggregation": "sum",
                                    }
                                ),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_zonal",
                    "content": _qmap_tool_result(
                        success=False,
                        details=(
                            "zonalStatsByAdmin aborted to prevent UI freeze: estimated pair evaluations "
                            "3.863.776 exceed budget 300.000 (admin=1504, values=2569, mode=area_weighted). "
                            "Deterministic H3 fallback reached minimum resolution r4; cannot proceed without truncation."
                        ),
                    ),
                },
                {"role": "user", "content": "mostra su mappa comuni colorati per valore"},
            ],
            "tools": [
                {"type": "function", "function": {"name": "applyQMapStylePreset"}},
                {"type": "function", "function": {"name": "setQMapLayerColorByField"}},
            ],
        }
        out = _inject_runtime_guardrail_message(payload)
        content = str(out["messages"][0]["content"])
        self.assertIn("coloring_after_unresolved_forest_metric_forbidden", content)
        self.assertIn("Do not switch to population/name fallback styling", content)
        self.assertIn("do not claim value-based coloring success", content.lower())

    def test_coloring_failure_without_success_cannot_claim_completion(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {"role": "user", "content": "mostra su mappa comuni colorati per valore"},
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_color",
                            "type": "function",
                            "function": {
                                "name": "setQMapLayerColorByField",
                                "arguments": json.dumps({"datasetName": "Comuni Lombardia", "fieldName": "zonal_value"}),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_color",
                    "content": _qmap_tool_result(success=False, details='No layer found for dataset "Comuni Lombardia".'),
                },
            ],
            "tools": [
                {"type": "function", "function": {"name": "listQMapDatasets"}},
                {"type": "function", "function": {"name": "setQMapLayerColorByField"}},
            ],
        }
        out = _inject_runtime_guardrail_message(payload)
        content = str(out["messages"][0]["content"])
        self.assertIn("coloring_failure_no_false_success_claim", content)
        self.assertIn("Do not claim that value-based coloring has been applied", content)

    def test_prunes_population_style_preset_after_unresolved_forest_value_workflow(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {"role": "user", "content": "mostra comune lombardo con meno boschi"},
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_clc",
                            "type": "function",
                            "function": {
                                "name": "queryQCumberDatasetSpatial",
                                "arguments": json.dumps(
                                    {
                                        "datasetId": "clc-2018-italia",
                                        "filters": [{"field": "code_18", "op": "in", "values": [311, 312, 313]}],
                                    }
                                ),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_clc",
                    "content": _qmap_tool_result(success=True, details="CLC forests loaded."),
                },
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_zonal",
                            "type": "function",
                            "function": {"name": "zonalStatsByAdmin", "arguments": json.dumps({"aggregation": "sum"})},
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_zonal",
                    "content": _qmap_tool_result(
                        success=False,
                        details=(
                            "zonalStatsByAdmin aborted to prevent UI freeze: estimated pair evaluations "
                            "3.863.776 exceed budget 300.000 (admin=1504, values=2569, mode=area_weighted). "
                            "Deterministic H3 fallback reached minimum resolution r4; cannot proceed without truncation."
                        ),
                    ),
                },
                {"role": "user", "content": "mostra su mappa comuni colorati per valore"},
            ],
            "tools": [
                {"type": "function", "function": {"name": "applyQMapStylePreset"}},
                {"type": "function", "function": {"name": "setQMapLayerColorByField"}},
            ],
        }
        out = _prune_population_style_tools_for_unresolved_value_coloring(payload)
        names = [tool.get("function", {}).get("name") for tool in out.get("tools", [])]
        self.assertNotIn("applyQMapStylePreset", names)
        self.assertIn("setQMapLayerColorByField", names)

    def test_completion_contract_wait_count_rank_required(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {"role": "user", "content": "mostra provincia italiana con meno boschi"},
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_join",
                            "type": "function",
                            "function": {
                                "name": "joinQMapDatasetsOnH3",
                                "arguments": json.dumps({"newDatasetName": "Province_Boschi_H3_Join"}),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_join",
                    "content": _qmap_tool_result(success=True, details='Join completed: "Province_Boschi_H3_Join".'),
                },
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_wait",
                            "type": "function",
                            "function": {
                                "name": "waitForQMapDataset",
                                "arguments": json.dumps({"datasetName": "Province_Boschi_H3_Join"}),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_wait",
                    "content": _qmap_tool_result(
                        success=True,
                        details='Dataset "Province_Boschi_H3_Join" is available (41556 rows).',
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
                                "arguments": json.dumps({"datasetName": "Province_Boschi_H3_Join"}),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_count",
                    "content": _qmap_tool_result(
                        success=True,
                        details='Dataset "Province_Boschi_H3_Join" has 41556 rows.',
                    ),
                },
            ],
            "tools": [{"type": "function", "function": {"name": "rankQMapDatasetRows"}}],
        }
        out = _inject_runtime_guardrail_message(payload)
        content = str(out["messages"][0]["content"])
        self.assertIn("completion_contract_wait_count_rank", content)
        self.assertIn("rankQMapDatasetRows", content)

    def test_forest_superlative_guardrail_requires_normalized_and_absolute_ranking(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {"role": "user", "content": "mostra provincia italiana con meno boschi"},
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
                                        "newDatasetName": "Province_Boschi_Final",
                                        "outputFieldName": "area_boschi_ha",
                                    }
                                ),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_zonal",
                    "content": _qmap_tool_result(success=True, details='Computed zonal dataset "Province_Boschi_Final".'),
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
                                        "datasetName": "Province_Boschi_Final",
                                        "metricFieldName": "area_boschi_ha",
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
                        details='Ranked top 10 rows from "Province_Boschi_Final" by "area_boschi_ha" (asc).',
                    ),
                },
            ],
            "tools": [
                {"type": "function", "function": {"name": "createDatasetWithGeometryArea"}},
                {"type": "function", "function": {"name": "createDatasetWithNormalizedField"}},
                {"type": "function", "function": {"name": "rankQMapDatasetRows"}},
            ],
        }
        out = _inject_runtime_guardrail_message(payload)
        content = str(out["messages"][0]["content"])
        self.assertIn("forest_superlative_dual_metric_no_size_bias", content)
        self.assertIn("createDatasetWithGeometryArea", content)
        self.assertIn("createDatasetWithNormalizedField", content)
        self.assertIn("avoid claiming a unique winner from absolute-only metric", content)

    def test_prefight_hint_enforces_in_operator_values_array_for_forest_queries(self):
        payload = {
            "messages": [{"role": "user", "content": "mostra provincia lombarda con meno boschi"}],
            "tools": [{"type": "function", "function": {"name": "queryQCumberDatasetSpatial"}}],
        }
        out = _inject_runtime_guardrail_message(payload)
        content = str(out["messages"][0]["content"])
        self.assertIn("operator \"in\" requires `values` array", content)
        self.assertIn('"op":"in","values":["311","312","313"]', content)

    def test_prefight_hint_enforces_canonical_rank_args(self):
        payload = {
            "messages": [{"role": "user", "content": "mostra su mappa la regione piu grande d'italia"}],
            "tools": [{"type": "function", "function": {"name": "rankQMapDatasetRows"}}],
        }
        out = _inject_runtime_guardrail_message(payload)
        content = str(out["messages"][0]["content"])
        self.assertIn("canonical args only", content)
        self.assertIn("metricFieldName", content)
        self.assertIn("sortDirection", content)

    def test_prefight_hint_enforces_canonical_zonal_args(self):
        payload = {
            "messages": [{"role": "user", "content": "mostra comune lombardo con meno boschi"}],
            "tools": [{"type": "function", "function": {"name": "zonalStatsByAdmin"}}],
        }
        out = _inject_runtime_guardrail_message(payload)
        content = str(out["messages"][0]["content"])
        self.assertIn("For zonalStatsByAdmin use canonical args", content)
        self.assertIn("valueDatasetName", content)
        self.assertIn("Do not use non-canonical keys like targetDatasetName", content)

    def test_preflight_hint_discourages_default_dataset_discovery_call(self):
        payload = {
            "messages": [{"role": "user", "content": "calcola ranking aree critiche e colora mappa"}],
            "tools": [{"type": "function", "function": {"name": "listQMapDatasets"}}],
        }
        out = _inject_runtime_guardrail_message(payload)
        content = str(out["messages"][0]["content"])
        self.assertIn("Avoid listQMapDatasets as default first step", content)
        self.assertIn("only when inventory/discovery is requested", content)

    def test_preflight_hint_allows_dataset_discovery_objectives(self):
        payload = {
            "messages": [{"role": "user", "content": "fai inventario dataset disponibili"}],
            "tools": [{"type": "function", "function": {"name": "listQMapDatasets"}}],
        }
        out = _inject_runtime_guardrail_message(payload)
        combined = "\n".join(
            str(message.get("content") or "")
            for message in out.get("messages", [])
            if isinstance(message, dict)
        )
        self.assertNotIn("Avoid listQMapDatasets as default first step", combined)

    def test_loop_limits_remove_repeated_error_class_tool(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {"role": "user", "content": "mostra provincia con meno boschi"},
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_rank_1",
                            "type": "function",
                            "function": {
                                "name": "rankQMapDatasetRows",
                                "arguments": json.dumps(
                                    {"datasetName": "Province_Boschi_Join", "metricFieldName": "area_ha_sum_sum"}
                                ),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_rank_1",
                    "content": _qmap_tool_result(
                        success=False,
                        details='Metric field "area_ha_sum_sum" not found in dataset "Province_Boschi_Join".',
                    ),
                },
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_rank_2",
                            "type": "function",
                            "function": {
                                "name": "rankQMapDatasetRows",
                                "arguments": json.dumps(
                                    {"datasetName": "Province_Boschi_Join", "metricFieldName": "area_ha_sum_sum"}
                                ),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_rank_2",
                    "content": _qmap_tool_result(
                        success=False,
                        details='Metric field "area_ha_sum_sum" not found in dataset "Province_Boschi_Join".',
                    ),
                },
            ],
            "tools": [
                {"type": "function", "function": {"name": "rankQMapDatasetRows"}},
                {"type": "function", "function": {"name": "previewQMapDatasetRows"}},
            ],
            "tool_choice": "auto",
        }

        out = _enforce_runtime_tool_loop_limits(payload)
        names = [tool.get("function", {}).get("name") for tool in out.get("tools", [])]
        self.assertNotIn("rankQMapDatasetRows", names)
        self.assertIn("previewQMapDatasetRows", names)
        content = str(out["messages"][0]["content"])
        self.assertIn("error_class_retry_cap", content)
        self.assertIn("rankQMapDatasetRows:field_missing", content)
        self.assertIn("Retry policy `field_missing`", content)

    def test_runtime_error_taxonomy_classifies_join_mismatch(self):
        details = 'Join failed: key mismatch between left and right datasets (cannot join on "h3_id").'
        self.assertEqual(_classify_runtime_error_kind(details), "join_mismatch")

    def test_runtime_error_taxonomy_classifies_admin_level_validation_failure(self):
        details = 'Administrative level mismatch after strict filtering: expected municipality (lv=9) on field "lv".'
        self.assertEqual(_classify_runtime_error_kind(details), "admin_level_validation_failure")

    def test_runtime_error_taxonomy_classifies_zero_rows_validation(self):
        details = 'Validation failed because filtered dataset has 0 rows after spatial clipping.'
        self.assertEqual(_classify_runtime_error_kind(details), "validation_zero_rows")

    def test_runtime_error_taxonomy_classifies_invalid_provider_id(self):
        details = "Invalid providerId."
        self.assertEqual(_classify_runtime_error_kind(details), "invalid_provider_id")

    def test_runtime_error_taxonomy_classifies_invalid_dataset_id(self):
        details = 'Invalid datasetId "stale-dataset". Use an exact datasetId from listQCumberDatasets(providerId).'
        self.assertEqual(_classify_runtime_error_kind(details), "invalid_dataset_id")

    def test_runtime_error_retry_policy_exposes_structured_hints_for_invalid_dataset_id(self):
        policy = _runtime_error_retry_policy("invalid_dataset_id")
        self.assertEqual(policy.get("errorKind"), "invalid_dataset_id")
        self.assertEqual(policy.get("recoveryAction"), "refresh_dataset_catalog")
        self.assertEqual(policy.get("nextAllowedTools"), ["listQCumberDatasets"])

    def test_runtime_quality_metrics_include_structured_backend_hints(self):
        quality = _derive_runtime_quality_metrics(
            [
                {
                    "toolName": "getQCumberDatasetHelp",
                    "success": False,
                    "details": 'Invalid datasetId "stale-dataset". Use an exact datasetId from listQCumberDatasets(providerId).',
                }
            ],
            [],
            "",
            {"messages": []},
        )
        self.assertEqual(quality.get("hintVersion"), "qmap.runtime.hints.v1")
        self.assertEqual(quality.get("errorKind"), "invalid_dataset_id")
        self.assertEqual(quality.get("recoveryAction"), "refresh_dataset_catalog")
        self.assertEqual(quality.get("nextAllowedTools"), ["listQCumberDatasets"])

    def test_runtime_error_taxonomy_classifies_cloud_no_validated_fallback(self):
        details = "Cloud map load timed out after retry; no validated fallback available."
        self.assertEqual(_classify_runtime_error_kind(details), "cloud_no_validated_fallback")

    def test_objective_requests_normalized_metric_detects_percentage_terms(self):
        self.assertTrue(_objective_requests_normalized_metric("colora per percentuale boschi per cella"))
        self.assertTrue(_objective_requests_normalized_metric("show forest coverage percentage by cell"))
        self.assertFalse(_objective_requests_normalized_metric("colora per area boschi assoluta"))

    def test_likely_normalized_metric_field_detects_pct_markers(self):
        self.assertTrue(_is_likely_normalized_metric_field("forest_pct"))
        self.assertTrue(_is_likely_normalized_metric_field("forest_percentage"))
        self.assertFalse(_is_likely_normalized_metric_field("forest_area_sum"))

    def test_objective_requests_map_display_detects_foreground_map_language(self):
        self.assertTrue(
            _objective_requests_map_display(
                "Materializza solo il risultato vincente e portalo in primo piano sulla mappa."
            )
        )


class RuntimeToolPolicySummaryTests(unittest.TestCase):
    def test_summarize_runtime_tool_policy_reports_pruned_tools(self):
        summary = _summarize_runtime_tool_policy(
            initial_tool_names=["fitQMapToDataset", "listQCumberProviders", "queryQCumberDataset"],
            final_tool_names=["queryQCumberDataset"],
        )
        self.assertIn("source=backend", summary)
        self.assertIn("allowed=1/3", summary)
        self.assertIn("pruned=fitQMapToDataset,listQCumberProviders", summary)

    def test_summarize_runtime_tool_policy_reports_none_when_unchanged(self):
        summary = _summarize_runtime_tool_policy(
            initial_tool_names=["queryQCumberDatasetSpatial"],
            final_tool_names=["queryQCumberDatasetSpatial"],
        )
        self.assertIn("allowed=1/1", summary)
        self.assertIn("pruned=none", summary)

    # ─── zero_match_must_acknowledge ─────────────────────────────────────

    def test_zero_match_guardrail_fires_when_all_queries_return_zero(self):
        """When all query tools returned 0 rows, the guardrail should force
        finalization with guidance to acknowledge no data found."""
        tool_result = {
            "toolName": "queryQCumberDatasetSpatial",
            "success": True,
            "details": "Query completed for dataset opas-stations with zero matches. No dataset loaded to map.",
        }
        payload = {
            "model": "test",
            "messages": [
                {"role": "user", "content": "mostrami le stazioni in Lombardia"},
                {"role": "assistant", "content": None, "tool_calls": [
                    {"id": "tc1", "type": "function", "function": {"name": "queryQCumberDatasetSpatial", "arguments": "{}"}}
                ]},
                {"role": "tool", "tool_call_id": "tc1", "content": json.dumps(tool_result)},
            ],
            "tools": [
                {"type": "function", "function": {"name": "queryQCumberDatasetSpatial", "parameters": {}}},
                {"type": "function", "function": {"name": "fitQMapToDataset", "parameters": {}}},
            ],
        }
        result = _enforce_runtime_tool_loop_limits(payload)
        # Should force finalization (tools=[], tool_choice=none)
        self.assertEqual(result.get("tools"), [])
        self.assertEqual(result.get("tool_choice"), "none")
        # Should contain guidance about zero matches
        messages = result.get("messages", [])
        system_msgs = [m for m in messages if m.get("role") == "system"]
        guidance_text = " ".join(str(m.get("content", "")) for m in system_msgs)
        self.assertIn("zero_match_must_acknowledge", guidance_text)

    def test_zero_match_guardrail_does_not_fire_when_query_has_rows(self):
        """When at least one query returned rows, the guardrail should not fire."""
        tool_result = {
            "toolName": "queryQCumberDatasetSpatial",
            "success": True,
            "details": "Query completed for dataset opas-stations.",
        }
        payload = {
            "model": "test",
            "messages": [
                {"role": "user", "content": "mostrami le stazioni in Veneto"},
                {"role": "assistant", "content": None, "tool_calls": [
                    {"id": "tc1", "type": "function", "function": {"name": "queryQCumberDatasetSpatial", "arguments": "{}"}}
                ]},
                {"role": "tool", "tool_call_id": "tc1", "content": json.dumps(tool_result)},
            ],
            "tools": [
                {"type": "function", "function": {"name": "queryQCumberDatasetSpatial", "parameters": {}}},
                {"type": "function", "function": {"name": "fitQMapToDataset", "parameters": {}}},
            ],
        }
        result = _enforce_runtime_tool_loop_limits(payload)
        # Tools should NOT be emptied
        self.assertTrue(len(result.get("tools", [])) > 0)


if __name__ == "__main__":
    unittest.main()
