import json

from tests.runtime_guardrails_test_support import (
    _enforce_runtime_tool_loop_limits,
    _qmap_tool_result,
)


class RuntimeGuardrailLoopLimitsWaitTooltipMixin:
    def test_loop_limits_circuit_breaker_for_identical_tool_args_failures(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {"role": "user", "content": "ordina province con meno boschi"},
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_rank_a",
                            "type": "function",
                            "function": {
                                "name": "rankQMapDatasetRows",
                                "arguments": json.dumps(
                                    {"datasetName": "Province_Boschi_Join", "metricFieldName": "area_bosco_ha"}
                                ),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_rank_a",
                    "content": _qmap_tool_result(
                        success=False,
                        details='Dataset "Province_Boschi_Join" not found.',
                    ),
                },
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_rank_b",
                            "type": "function",
                            "function": {
                                "name": "rankQMapDatasetRows",
                                "arguments": json.dumps(
                                    {"datasetName": "Province_Boschi_Join", "metricFieldName": "area_bosco_ha"}
                                ),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_rank_b",
                    "content": _qmap_tool_result(
                        success=False,
                        details='Timeout waiting for dataset "Province_Boschi_Join" after ranking request.',
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
        self.assertIn("identical_tool_args_circuit_breaker", content)
        self.assertIn("Fallback trace", content)
        self.assertIn('"tool":"rankQMapDatasetRows"', content)

    def test_loop_limits_force_wait_before_post_create_followups(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {"role": "user", "content": "tassella e colora"},
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_join",
                            "type": "function",
                            "function": {
                                "name": "joinQMapDatasetsOnH3",
                                "arguments": json.dumps({"newDatasetName": "Regioni_H3_Joined"}),
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
                {"type": "function", "function": {"name": "createDatasetWithNormalizedField"}},
                {"type": "function", "function": {"name": "waitForQMapDataset"}},
                {"type": "function", "function": {"name": "countQMapRows"}},
                {"type": "function", "function": {"name": "setQMapLayerColorByField"}},
                {"type": "function", "function": {"name": "fitQMapToDataset"}},
                {"type": "function", "function": {"name": "rankQMapDatasetRows"}},
            ],
            "tool_choice": {"type": "function", "function": {"name": "setQMapLayerColorByField"}},
        }

        out = _enforce_runtime_tool_loop_limits(payload)
        names = [tool.get("function", {}).get("name") for tool in out.get("tools", [])]
        self.assertIn("waitForQMapDataset", names)
        self.assertNotIn("createDatasetWithNormalizedField", names)
        self.assertNotIn("countQMapRows", names)
        self.assertNotIn("setQMapLayerColorByField", names)
        self.assertNotIn("fitQMapToDataset", names)
        self.assertNotIn("rankQMapDatasetRows", names)
        self.assertEqual(out.get("tool_choice", {}).get("function", {}).get("name"), "waitForQMapDataset")
        content = str(out["messages"][0]["content"])
        self.assertIn("post_create_validation_wait_gate", content)

    def test_loop_limits_force_count_after_successful_wait(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {"role": "user", "content": "continua"},
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_create",
                            "type": "function",
                            "function": {
                                "name": "createDatasetWithGeometryArea",
                                "arguments": json.dumps({"datasetName": "Province_Boschi"}),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_create",
                    "content": _qmap_tool_result(success=True, details='Created dataset "Province_Boschi_Area".'),
                },
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_wait",
                            "type": "function",
                            "function": {
                                "name": "waitForQMapDataset",
                                "arguments": json.dumps({"datasetName": "Province_Boschi_Area"}),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_wait",
                    "content": _qmap_tool_result(
                        success=True, details='Dataset "Province_Boschi_Area" is available (107 rows).'
                    ),
                },
            ],
            "tools": [
                {"type": "function", "function": {"name": "waitForQMapDataset"}},
                {"type": "function", "function": {"name": "countQMapRows"}},
                {"type": "function", "function": {"name": "showOnlyQMapLayer"}},
                {"type": "function", "function": {"name": "fitQMapToDataset"}},
                {"type": "function", "function": {"name": "setQMapTooltipFields"}},
            ],
            "tool_choice": {"type": "function", "function": {"name": "fitQMapToDataset"}},
        }

        out = _enforce_runtime_tool_loop_limits(payload)
        names = [tool.get("function", {}).get("name") for tool in out.get("tools", [])]
        self.assertIn("countQMapRows", names)
        self.assertNotIn("waitForQMapDataset", names)
        self.assertNotIn("showOnlyQMapLayer", names)
        self.assertNotIn("fitQMapToDataset", names)
        self.assertNotIn("setQMapTooltipFields", names)
        self.assertEqual(out.get("tool_choice", {}).get("function", {}).get("name"), "countQMapRows")
        content = str(out["messages"][0]["content"])
        self.assertIn("post_create_validation_count_gate", content)

    def test_loop_limits_do_not_force_tooltips_after_final_created_dataset_style(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {"role": "user", "content": "crea un dataset finale e coloralo per popolazione"},
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_create",
                            "type": "function",
                            "function": {
                                "name": "createDatasetWithNormalizedField",
                                "arguments": json.dumps(
                                    {
                                        "datasetName": "Province Italia",
                                        "numeratorFieldName": "population",
                                        "denominatorFieldName": "area_km2",
                                        "outputFieldName": "population_density",
                                    }
                                ),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_create",
                    "content": _qmap_tool_result(
                        success=True,
                        details='Created dataset "Province Italia Density" (id:province-italia-density).',
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
                                "arguments": json.dumps({"datasetName": "Province Italia Density"}),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_wait",
                    "content": _qmap_tool_result(
                        success=True,
                        details='Dataset "Province Italia Density" is ready.',
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
                                "arguments": json.dumps({"datasetName": "Province Italia Density"}),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_count",
                    "content": _qmap_tool_result(
                        success=True,
                        details='Dataset "Province Italia Density" contains 107 rows.',
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
                                    {"datasetName": "Province Italia Density", "fieldName": "population_density"}
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
                        details='Applying quantile color scale on layer "Province Italia Density" using field "population_density".',
                    ),
                },
            ],
            "tools": [
                {"type": "function", "function": {"name": "setQMapTooltipFields"}},
                {"type": "function", "function": {"name": "fitQMapToDataset"}},
                {"type": "function", "function": {"name": "showOnlyQMapLayer"}},
            ],
            "tool_choice": {"type": "function", "function": {"name": "fitQMapToDataset"}},
        }

        out = _enforce_runtime_tool_loop_limits(payload)
        tool_choice = out.get("tool_choice")
        if isinstance(tool_choice, dict):
            self.assertNotEqual(tool_choice.get("function", {}).get("name"), "setQMapTooltipFields")
        else:
            self.assertNotEqual(tool_choice, "setQMapTooltipFields")
        content = str(out["messages"][0]["content"])
        self.assertNotIn("final_dataset_requires_tooltips", content)

    def test_loop_limits_do_not_reopen_count_gate_after_late_wait_success(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {"role": "user", "content": "continua"},
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_create",
                            "type": "function",
                            "function": {
                                "name": "populateTassellationFromAdminUnitsAreaWeighted",
                                "arguments": json.dumps({"newDatasetName": "Tassellation_Brescia_r7_pop_final"}),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_create",
                    "content": _qmap_tool_result(
                        success=True,
                        details='Preparing populated tessellation "Tassellation_Brescia_r7_pop_final".',
                    ),
                },
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_wait_a",
                            "type": "function",
                            "function": {
                                "name": "waitForQMapDataset",
                                "arguments": json.dumps({"datasetName": "Tassellation_Brescia_r7_pop_final"}),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_wait_a",
                    "content": _qmap_tool_result(
                        success=True,
                        details='Dataset "Tassellation_Brescia_r7_pop_final" is available (1041 rows).',
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
                                "arguments": json.dumps({"datasetName": "Tassellation_Brescia_r7_pop_final"}),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_count",
                    "content": _qmap_tool_result(
                        success=True,
                        details='Dataset "Tassellation_Brescia_r7_pop_final" has 1041 rows.',
                    ),
                },
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_wait_b",
                            "type": "function",
                            "function": {
                                "name": "waitForQMapDataset",
                                "arguments": json.dumps({"datasetName": "Tassellation_Brescia_r7_pop_final"}),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_wait_b",
                    "content": _qmap_tool_result(
                        success=True,
                        details='Dataset "Tassellation_Brescia_r7_pop_final" is available (1041 rows).',
                    ),
                },
            ],
            "tools": [
                {"type": "function", "function": {"name": "countQMapRows"}},
                {"type": "function", "function": {"name": "setQMapLayerColorByField"}},
                {"type": "function", "function": {"name": "showOnlyQMapLayer"}},
            ],
            "tool_choice": "auto",
        }

        out = _enforce_runtime_tool_loop_limits(payload)
        tool_choice = out.get("tool_choice")
        if isinstance(tool_choice, dict):
            self.assertNotEqual(tool_choice.get("function", {}).get("name"), "countQMapRows")
        else:
            self.assertNotEqual(tool_choice, "countQMapRows")
        content = str(out["messages"][0]["content"])
        self.assertNotIn("post_create_validation_count_gate", content)

    def test_loop_limits_do_not_force_tooltips_after_query_style_only(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {"role": "user", "content": "colorami le province italiane per popolazione"},
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_query",
                            "type": "function",
                            "function": {
                                "name": "queryQCumberTerritorialUnits",
                                "arguments": json.dumps({"datasetName": "Province_Italia"}),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_query",
                    "content": _qmap_tool_result(
                        success=True,
                        details='Query completed and loaded dataset into map as "Province Italia" (id:province-italia).',
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
                                "arguments": json.dumps({"datasetName": "Province Italia", "fieldName": "population"}),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_color",
                    "content": _qmap_tool_result(
                        success=True,
                        details='Applying quantile color scale on layer "Province Italia" using field "population".',
                    ),
                },
            ],
            "tools": [
                {"type": "function", "function": {"name": "setQMapTooltipFields"}},
                {"type": "function", "function": {"name": "fitQMapToDataset"}},
                {"type": "function", "function": {"name": "showOnlyQMapLayer"}},
            ],
            "tool_choice": {"type": "function", "function": {"name": "fitQMapToDataset"}},
        }

        out = _enforce_runtime_tool_loop_limits(payload)
        tool_choice = out.get("tool_choice")
        if isinstance(tool_choice, dict):
            self.assertNotEqual(tool_choice.get("function", {}).get("name"), "setQMapTooltipFields")
        else:
            self.assertNotEqual(tool_choice, "setQMapTooltipFields")
        content = str(out["messages"][0]["content"])
        self.assertNotIn("final_dataset_requires_tooltips", content)

    def test_loop_limits_do_not_force_tooltips_before_pending_rank_step(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {"role": "user", "content": "mostrami la provincia con piu popolazione sulla mappa"},
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_query",
                            "type": "function",
                            "function": {
                                "name": "queryQCumberTerritorialUnits",
                                "arguments": json.dumps({"datasetName": "Province_Italia"}),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_query",
                    "content": _qmap_tool_result(
                        success=True,
                        details='Query completed and loaded dataset into map as "Province Italia" (id:province-italia).',
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
                                "arguments": json.dumps({"datasetName": "Province Italia", "fieldName": "population"}),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_color",
                    "content": _qmap_tool_result(
                        success=True,
                        details='Applying quantile color scale on layer "Province Italia" using field "population".',
                    ),
                },
            ],
            "tools": [
                {"type": "function", "function": {"name": "setQMapTooltipFields"}},
                {"type": "function", "function": {"name": "rankQMapDatasetRows"}},
                {"type": "function", "function": {"name": "fitQMapToDataset"}},
                {"type": "function", "function": {"name": "showOnlyQMapLayer"}},
            ],
            "tool_choice": {"type": "function", "function": {"name": "rankQMapDatasetRows"}},
        }

        out = _enforce_runtime_tool_loop_limits(payload)
        names = [tool.get("function", {}).get("name") for tool in out.get("tools", [])]
        self.assertIn("rankQMapDatasetRows", names)
        self.assertEqual(out.get("tool_choice", {}).get("function", {}).get("name"), "rankQMapDatasetRows")
        content = str(out["messages"][0]["content"])
        self.assertNotIn("final_dataset_requires_tooltips", content)

    def test_loop_limits_do_not_force_tooltips_before_isolating_admin_superlative_winner(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {
                    "role": "user",
                    "content": "Mostra il comune piu piccolo della provincia di Parma e mostrami direttamente il risultato sulla mappa",
                },
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_query",
                            "type": "function",
                            "function": {
                                "name": "queryQCumberTerritorialUnits",
                                "arguments": json.dumps({"filters": [{"field": "parent_name", "op": "eq", "value": "Parma"}]}),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_query",
                    "content": _qmap_tool_result(
                        success=True,
                        details='Query completed and loaded dataset into map as "EvalDataset [eval]" (id:evaldataset-eval).',
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
                                "arguments": json.dumps({"datasetName": "id:evaldataset-eval", "fieldName": "area_sqm", "order": "asc"}),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_rank",
                    "content": _qmap_tool_result(
                        success=True,
                        details='Ranking completed for dataset "id:evaldataset-eval".',
                    ),
                },
            ],
            "tools": [
                {"type": "function", "function": {"name": "createDatasetFromFilter"}},
                {"type": "function", "function": {"name": "setQMapTooltipFields"}},
                {"type": "function", "function": {"name": "fitQMapToDataset"}},
            ],
            "tool_choice": {"type": "function", "function": {"name": "createDatasetFromFilter"}},
        }

        out = _enforce_runtime_tool_loop_limits(payload)
        names = [tool.get("function", {}).get("name") for tool in out.get("tools", [])]
        self.assertIn("createDatasetFromFilter", names)
        self.assertIn("setQMapTooltipFields", names)
        self.assertEqual(out.get("tool_choice", {}).get("function", {}).get("name"), "createDatasetFromFilter")
        content = str(out["messages"][0]["content"])
        self.assertNotIn("final_dataset_requires_tooltips", content)

    def test_loop_limits_prune_low_distinct_color_retry_and_heavy_recompute(self):
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
                {"type": "function", "function": {"name": "setQMapLayerColorByField"}},
                {"type": "function", "function": {"name": "distinctQMapFieldValues"}},
                {"type": "function", "function": {"name": "previewQMapDatasetRows"}},
                {"type": "function", "function": {"name": "setQMapLayerSolidColor"}},
                {"type": "function", "function": {"name": "queryQCumberDatasetSpatial"}},
                {"type": "function", "function": {"name": "zonalStatsByAdmin"}},
            ],
            "tool_choice": {"type": "function", "function": {"name": "setQMapLayerColorByField"}},
        }

        out = _enforce_runtime_tool_loop_limits(payload)
        names = [tool.get("function", {}).get("name") for tool in out.get("tools", [])]
        self.assertNotIn("setQMapLayerColorByField", names)
        self.assertNotIn("queryQCumberDatasetSpatial", names)
        self.assertNotIn("zonalStatsByAdmin", names)
        self.assertIn("distinctQMapFieldValues", names)
        self.assertIn("previewQMapDatasetRows", names)
        self.assertIn("setQMapLayerSolidColor", names)
        self.assertEqual(out.get("tool_choice", {}).get("function", {}).get("name"), "distinctQMapFieldValues")
        content = str(out["messages"][0]["content"])
        self.assertIn("color_low_distinct_runtime_recovery_gate", content)
        self.assertIn("Do not retry setQMapLayerColorByField", content)

    def test_loop_limits_skip_low_distinct_gate_when_style_recovery_already_succeeded(self):
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
                        details='Field "pressione_ha" has <=1 distinct numeric value in sampled rows.',
                    ),
                },
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_solid",
                            "type": "function",
                            "function": {
                                "name": "setQMapLayerSolidColor",
                                "arguments": json.dumps(
                                    {"datasetName": "Regioni_Pressione_CLC_Analisi", "fillColor": [255, 128, 0]}
                                ),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_solid",
                    "content": _qmap_tool_result(success=True, details="Applied fallback solid color."),
                },
            ],
            "tools": [
                {"type": "function", "function": {"name": "setQMapLayerColorByField"}},
                {"type": "function", "function": {"name": "distinctQMapFieldValues"}},
            ],
            "tool_choice": "auto",
        }

        out = _enforce_runtime_tool_loop_limits(payload)
        names = [tool.get("function", {}).get("name") for tool in out.get("tools", [])]
        self.assertIn("setQMapLayerColorByField", names)
        content = str(out["messages"][0]["content"])
        self.assertNotIn("color_low_distinct_runtime_recovery_gate", content)

    def test_loop_limits_prune_heavy_recompute_after_color_missing_field_failure(self):
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
                {"type": "function", "function": {"name": "distinctQMapFieldValues"}},
                {"type": "function", "function": {"name": "setQMapLayerColorByField"}},
                {"type": "function", "function": {"name": "queryQCumberTerritorialUnits"}},
                {"type": "function", "function": {"name": "queryQCumberDatasetSpatial"}},
                {"type": "function", "function": {"name": "spatialJoinByPredicate"}},
            ],
            "tool_choice": {"type": "function", "function": {"name": "spatialJoinByPredicate"}},
        }

        out = _enforce_runtime_tool_loop_limits(payload)
        names = [tool.get("function", {}).get("name") for tool in out.get("tools", [])]
        self.assertIn("previewQMapDatasetRows", names)
        self.assertIn("distinctQMapFieldValues", names)
        self.assertIn("setQMapLayerColorByField", names)
        self.assertNotIn("queryQCumberTerritorialUnits", names)
        self.assertNotIn("queryQCumberDatasetSpatial", names)
        self.assertNotIn("spatialJoinByPredicate", names)
        self.assertEqual(out.get("tool_choice", {}).get("function", {}).get("name"), "previewQMapDatasetRows")
        content = str(out["messages"][0]["content"])
        self.assertIn("color_missing_field_runtime_recovery_gate", content)
        self.assertIn("Do not rerun heavy query/join recompute loops", content)
