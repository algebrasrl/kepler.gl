import json

from tests.runtime_guardrails_test_support import (
    _enforce_runtime_tool_loop_limits,
    _qmap_tool_result,
)


class RuntimeGuardrailLoopLimitsRecoveryMixin:
    def test_loop_limits_force_materialization_after_dataset_not_found_count_failure(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {"role": "user", "content": "crea layer rapporto superficie/popolazione"},
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_count_missing",
                            "type": "function",
                            "function": {
                                "name": "countQMapRows",
                                "arguments": json.dumps({"datasetName": "Comuni_Rapporto_Area_Popolazione"}),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_count_missing",
                    "content": _qmap_tool_result(
                        success=False,
                        details='Dataset "Comuni_Rapporto_Area_Popolazione" not found.',
                    ),
                },
            ],
            "tools": [
                {"type": "function", "function": {"name": "countQMapRows"}},
                {"type": "function", "function": {"name": "waitForQMapDataset"}},
                {"type": "function", "function": {"name": "saveDataToMap"}},
                {"type": "function", "function": {"name": "setQMapLayerColorByField"}},
            ],
            "tool_choice": {"type": "function", "function": {"name": "countQMapRows"}},
        }

        out = _enforce_runtime_tool_loop_limits(payload)
        names = [tool.get("function", {}).get("name") for tool in out.get("tools", [])]
        self.assertNotIn("countQMapRows", names)
        # waitForQMapDataset is now preferred over saveDataToMap for dataset_not_found recovery:
        # derived q-map datasets (clip/aggregate) are already in Redux state, not in ToolCache.
        self.assertIn("waitForQMapDataset", names)
        self.assertEqual(out.get("tool_choice", {}).get("function", {}).get("name"), "waitForQMapDataset")
        content = str(out["messages"][0]["content"])
        self.assertIn("dataset_not_found_materialization_recovery", content)
        self.assertIn("Do not loop wait/count/fit", content)

    def test_loop_limits_skip_materialization_force_when_recovery_already_started(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {"role": "user", "content": "crea layer rapporto superficie/popolazione"},
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_count_missing",
                            "type": "function",
                            "function": {
                                "name": "countQMapRows",
                                "arguments": json.dumps({"datasetName": "Comuni_Rapporto_Area_Popolazione"}),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_count_missing",
                    "content": _qmap_tool_result(
                        success=False,
                        details='Dataset "Comuni_Rapporto_Area_Popolazione" not found.',
                    ),
                },
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_save",
                            "type": "function",
                            "function": {
                                "name": "saveDataToMap",
                                "arguments": json.dumps({"datasetNames": ["Comuni_Rapporto_Area_Popolazione"]}),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_save",
                    "content": _qmap_tool_result(success=True, details="Saved dataset to map."),
                },
            ],
            "tools": [
                {"type": "function", "function": {"name": "countQMapRows"}},
                {"type": "function", "function": {"name": "saveDataToMap"}},
            ],
            "tool_choice": "auto",
        }

        out = _enforce_runtime_tool_loop_limits(payload)
        names = [tool.get("function", {}).get("name") for tool in out.get("tools", [])]
        self.assertIn("countQMapRows", names)
        content = str(out["messages"][0]["content"])
        self.assertNotIn("dataset_not_found_materialization_recovery", content)

    def test_loop_limits_keeps_tool_when_args_signature_changes(self):
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
                                    {"datasetName": "Province_Boschi_Join_A", "metricFieldName": "area_bosco_ha"}
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
                        details='Dataset "Province_Boschi_Join_A" not found.',
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
                                    {"datasetName": "Province_Boschi_Join_B", "metricFieldName": "area_bosco_ha"}
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
                        details='Ranking metric "area_bosco_ha" is invalid for selected dataset.',
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
        self.assertIn("rankQMapDatasetRows", names)
        content = str(out["messages"][0]["content"])
        self.assertNotIn("identical_tool_args_circuit_breaker", content)

    def test_loop_limits_reuse_successful_identical_tool_args(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {"role": "user", "content": "ordina e poi continua"},
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
                    "content": _qmap_tool_result(success=True, details="Ranking ok."),
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
                    "content": _qmap_tool_result(success=True, details="Ranking ok."),
                },
            ],
            "tools": [
                {"type": "function", "function": {"name": "rankQMapDatasetRows"}},
                {"type": "function", "function": {"name": "previewQMapDatasetRows"}},
            ],
            "tool_choice": {"type": "function", "function": {"name": "rankQMapDatasetRows"}},
        }

        out = _enforce_runtime_tool_loop_limits(payload)
        names = [tool.get("function", {}).get("name") for tool in out.get("tools", [])]
        self.assertNotIn("rankQMapDatasetRows", names)
        self.assertIn("previewQMapDatasetRows", names)
        self.assertEqual(out.get("tool_choice"), "auto")
        content = str(out["messages"][0]["content"])
        self.assertIn("identical_tool_args_success_reuse", content)
        self.assertIn('"tool":"rankQMapDatasetRows"', content)

    def test_loop_limits_keep_tool_after_duplicate_success_when_newer_distinct_progress_exists(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {"role": "user", "content": "ordina e poi centra"},
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
                    "content": _qmap_tool_result(success=True, details="Ranking ok."),
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
                    "content": _qmap_tool_result(success=True, details="Ranking ok."),
                },
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_fit",
                            "type": "function",
                            "function": {
                                "name": "fitQMapToDataset",
                                "arguments": json.dumps({"datasetName": "Province_Boschi_Join"}),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_fit",
                    "content": _qmap_tool_result(success=True, details="Fit completed."),
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
        self.assertIn("rankQMapDatasetRows", names)
        content = str(out["messages"][0]["content"])
        self.assertNotIn("identical_tool_args_success_reuse", content)

    def test_loop_limits_hard_cap_forces_final_text_mode(self):
        messages = [
            {"role": "system", "content": "System"},
            {"role": "user", "content": "workflow lungo"},
        ]
        for idx in range(1, 23):
            call_id = f"call_preview_{idx}"
            messages.extend(
                [
                    {
                        "role": "assistant",
                        "tool_calls": [
                            {
                                "id": call_id,
                                "type": "function",
                                "function": {"name": "previewQMapDatasetRows", "arguments": "{}"},
                            }
                        ],
                    },
                    {
                        "role": "tool",
                        "tool_call_id": call_id,
                        "content": _qmap_tool_result(success=True, details="Preview ok."),
                    },
                ]
            )

        payload = {
            "messages": messages,
            "tools": [
                {"type": "function", "function": {"name": "previewQMapDatasetRows"}},
                {"type": "function", "function": {"name": "rankQMapDatasetRows"}},
            ],
            "tool_choice": "auto",
        }
        out = _enforce_runtime_tool_loop_limits(payload)
        self.assertEqual(out.get("tools"), [])
        self.assertEqual(out.get("tool_choice"), "none")
        content = str(out["messages"][0]["content"])
        self.assertIn("tool_call_hard_cap", content)
        self.assertIn("Do not emit further tool calls", content)

    def test_loop_limits_tool_only_watchdog_forces_final_text_mode(self):
        messages = [
            {"role": "system", "content": "System"},
            {"role": "user", "content": "workflow senza risposta finale"},
        ]
        for idx in range(1, 10):
            call_id = f"call_wait_{idx}"
            messages.extend(
                [
                    {
                        "role": "assistant",
                        "tool_calls": [
                            {
                                "id": call_id,
                                "type": "function",
                                "function": {"name": "waitForQMapDataset", "arguments": "{}"},
                            }
                        ],
                    },
                    {
                        "role": "tool",
                        "tool_call_id": call_id,
                        "content": _qmap_tool_result(success=True, details="Wait ok."),
                    },
                ]
            )

        payload = {
            "messages": messages,
            "tools": [
                {"type": "function", "function": {"name": "waitForQMapDataset"}},
                {"type": "function", "function": {"name": "countQMapRows"}},
            ],
            "tool_choice": "auto",
        }
        out = _enforce_runtime_tool_loop_limits(payload)
        self.assertEqual(out.get("tools"), [])
        self.assertEqual(out.get("tool_choice"), "none")
        content = str(out["messages"][0]["content"])
        self.assertIn("tool_only_no_final_text_watchdog", content)
        self.assertIn("Return final text now", content)

    def test_loop_limits_prune_operational_tools_after_turn_state_discovery_failures(self):
        gate_error = (
            "Hard-enforce turn state: discovery step is mandatory. "
            "Call listQMapDatasets first to capture the current map snapshot, "
            "then continue with operational tools."
        )
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {"role": "user", "content": "mostra boschi appennino e centra la mappa"},
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_q_1",
                            "type": "function",
                            "function": {"name": "queryQCumberDatasetSpatial", "arguments": "{}"},
                        }
                    ],
                },
                {"role": "tool", "tool_call_id": "call_q_1", "content": _qmap_tool_result(success=False, details=gate_error)},
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_fit_1",
                            "type": "function",
                            "function": {"name": "fitQMapToDataset", "arguments": "{}"},
                        }
                    ],
                },
                {"role": "tool", "tool_call_id": "call_fit_1", "content": _qmap_tool_result(success=False, details=gate_error)},
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_agg_1",
                            "type": "function",
                            "function": {"name": "aggregateDatasetToH3", "arguments": "{}"},
                        }
                    ],
                },
                {"role": "tool", "tool_call_id": "call_agg_1", "content": _qmap_tool_result(success=False, details=gate_error)},
            ],
            "tools": [
                {"type": "function", "function": {"name": "listQMapDatasets"}},
                {"type": "function", "function": {"name": "queryQCumberDatasetSpatial"}},
                {"type": "function", "function": {"name": "fitQMapToDataset"}},
                {"type": "function", "function": {"name": "aggregateDatasetToH3"}},
            ],
            "tool_choice": "auto",
        }

        out = _enforce_runtime_tool_loop_limits(payload)
        names = [tool.get("function", {}).get("name") for tool in out.get("tools", [])]
        self.assertIn("listQMapDatasets", names)
        self.assertNotIn("queryQCumberDatasetSpatial", names)
        self.assertNotIn("fitQMapToDataset", names)
        self.assertNotIn("aggregateDatasetToH3", names)
        content = str(out["messages"][0]["content"])
        self.assertIn("turn_state_discovery_retry_gate", content)
        self.assertIn("Call listQMapDatasets once", content)

    def test_loop_limits_prune_redundant_list_snapshot_after_operational_progress(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {"role": "user", "content": "calcola ranking aree con piu pressione ambientale"},
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_list",
                            "type": "function",
                            "function": {"name": "listQMapDatasets", "arguments": "{}"},
                        }
                    ],
                },
                {"role": "tool", "tool_call_id": "call_list", "content": _qmap_tool_result(success=True, details="Snapshot ok.")},
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
                {"role": "tool", "tool_call_id": "call_rank", "content": _qmap_tool_result(success=True, details="Ranking ok.")},
            ],
            "tools": [
                {"type": "function", "function": {"name": "listQMapDatasets"}},
                {"type": "function", "function": {"name": "rankQMapDatasetRows"}},
                {"type": "function", "function": {"name": "searchQMapFieldValues"}},
            ],
            "tool_choice": "auto",
        }

        out = _enforce_runtime_tool_loop_limits(payload)
        names = [tool.get("function", {}).get("name") for tool in out.get("tools", [])]
        self.assertNotIn("listQMapDatasets", names)
        self.assertIn("rankQMapDatasetRows", names)
        content = str(out["messages"][0]["content"])
        self.assertIn("dataset_discovery_snapshot_reuse", content)
        self.assertIn("Avoid redundant listQMapDatasets calls", content)

    def test_loop_limits_keep_list_snapshot_for_inventory_objective(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {"role": "user", "content": "fai inventario dataset e provider disponibili"},
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_list",
                            "type": "function",
                            "function": {"name": "listQMapDatasets", "arguments": "{}"},
                        }
                    ],
                },
                {"role": "tool", "tool_call_id": "call_list", "content": _qmap_tool_result(success=True, details="Snapshot ok.")},
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
                {"role": "tool", "tool_call_id": "call_rank", "content": _qmap_tool_result(success=True, details="Ranking ok.")},
            ],
            "tools": [
                {"type": "function", "function": {"name": "listQMapDatasets"}},
                {"type": "function", "function": {"name": "rankQMapDatasetRows"}},
            ],
            "tool_choice": "auto",
        }

        out = _enforce_runtime_tool_loop_limits(payload)
        names = [tool.get("function", {}).get("name") for tool in out.get("tools", [])]
        self.assertIn("listQMapDatasets", names)
        content = str(out["messages"][0]["content"])
        self.assertNotIn("dataset_discovery_snapshot_reuse", content)

    def test_loop_limits_keep_list_snapshot_after_operational_failure(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {"role": "user", "content": "colora la regione piu piccola e la piu grande"},
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_list",
                            "type": "function",
                            "function": {"name": "listQMapDatasets", "arguments": "{}"},
                        }
                    ],
                },
                {"role": "tool", "tool_call_id": "call_list", "content": _qmap_tool_result(success=True, details="Snapshot ok.")},
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_style",
                            "type": "function",
                            "function": {"name": "setQMapLayerColorByField", "arguments": "{}"},
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_style",
                    "content": _qmap_tool_result(
                        success=False,
                        details='Field "area_m2" not found in dataset "Regioni_con_Area".',
                    ),
                },
            ],
            "tools": [
                {"type": "function", "function": {"name": "listQMapDatasets"}},
                {"type": "function", "function": {"name": "setQMapLayerColorByField"}},
                {"type": "function", "function": {"name": "rankQMapDatasetRows"}},
            ],
            "tool_choice": "auto",
        }

        out = _enforce_runtime_tool_loop_limits(payload)
        names = [tool.get("function", {}).get("name") for tool in out.get("tools", [])]
        self.assertIn("listQMapDatasets", names)
        content = str(out["messages"][0]["content"])
        self.assertNotIn("dataset_discovery_snapshot_reuse", content)

    def test_loop_limits_prune_load_data_after_successful_cloud_load(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {
                    "role": "user",
                    "content": "Gestisci sequenza cloud: lista mappe, load map, wait dataset e fallback error-safe.",
                },
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_cloud",
                            "type": "function",
                            "function": {"name": "loadCloudMapAndWait", "arguments": "{}"},
                        }
                    ],
                },
                {"role": "tool", "tool_call_id": "call_cloud", "content": _qmap_tool_result(success=True, details="Cloud load ok.")},
            ],
            "tools": [
                {"type": "function", "function": {"name": "loadCloudMapAndWait"}},
                {"type": "function", "function": {"name": "loadData"}},
                {"type": "function", "function": {"name": "countQMapRows"}},
            ],
            "tool_choice": "auto",
        }

        out = _enforce_runtime_tool_loop_limits(payload)
        names = [tool.get("function", {}).get("name") for tool in out.get("tools", [])]
        self.assertNotIn("loadData", names)
        self.assertIn("loadCloudMapAndWait", names)
        content = str(out["messages"][0]["content"])
        self.assertIn("cloud_load_no_redundant_fallback_load", content)
        self.assertIn("Do not run loadData fallback unless cloud load fails", content)

    def test_loop_limits_keep_load_data_when_cloud_load_failed(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {
                    "role": "user",
                    "content": "Gestisci sequenza cloud: lista mappe, load map, wait dataset e fallback error-safe.",
                },
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_cloud",
                            "type": "function",
                            "function": {"name": "loadCloudMapAndWait", "arguments": "{}"},
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_cloud",
                    "content": _qmap_tool_result(success=False, details="Cloud load failed."),
                },
            ],
            "tools": [
                {"type": "function", "function": {"name": "loadCloudMapAndWait"}},
                {"type": "function", "function": {"name": "loadData"}},
            ],
            "tool_choice": "auto",
        }

        out = _enforce_runtime_tool_loop_limits(payload)
        names = [tool.get("function", {}).get("name") for tool in out.get("tools", [])]
        self.assertIn("loadData", names)
        content = str(out["messages"][0]["content"])
        self.assertNotIn("cloud_load_no_redundant_fallback_load", content)

    def test_loop_limits_finalize_when_cloud_no_validated_fallback_exists(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {
                    "role": "user",
                    "content": "Gestisci sequenza cloud: lista mappe, load map, wait dataset e fallback error-safe.",
                },
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_cloud",
                            "type": "function",
                            "function": {
                                "name": "loadCloudMapAndWait",
                                "arguments": json.dumps({"mapId": "cloud-timeout-map"}),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_cloud",
                    "content": _qmap_tool_result(
                        success=False,
                        details="Cloud map load timed out after retry; no validated fallback available.",
                    ),
                },
            ],
            "tools": [
                {"type": "function", "function": {"name": "loadCloudMapAndWait"}},
                {"type": "function", "function": {"name": "loadData"}},
                {"type": "function", "function": {"name": "waitForQMapDataset"}},
            ],
            "tool_choice": "auto",
        }

        out = _enforce_runtime_tool_loop_limits(payload)
        self.assertEqual(out.get("tools"), [])
        self.assertEqual(out.get("tool_choice"), "none")
        content = str(out["messages"][0]["content"])
        self.assertIn("cloud_no_validated_fallback_finalize", content)
        self.assertIn("[RUNTIME_RESPONSE_MODE] limitation", content)
        self.assertIn("Return one concise limitation now", content)

    def test_loop_limits_finalize_when_admin_level_validation_fails(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {
                    "role": "user",
                    "content": "Cerca il comune di Parma e non rilassare il livello se fallisce la validazione.",
                },
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
                                        "filters": [{"field": "name", "op": "eq", "value": "Parma"}],
                                        "expectedAdminType": "municipality",
                                    }
                                ),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_query",
                    "content": _qmap_tool_result(
                        success=False,
                        details=(
                            'Administrative level mismatch after strict filtering: expected municipality (lv=9) '
                            'on field "lv".'
                        ),
                    ),
                },
            ],
            "tools": [
                {"type": "function", "function": {"name": "queryQCumberTerritorialUnits"}},
                {"type": "function", "function": {"name": "queryQCumberDataset"}},
                {"type": "function", "function": {"name": "fitQMapToDataset"}},
            ],
            "tool_choice": "auto",
        }

        out = _enforce_runtime_tool_loop_limits(payload)
        self.assertEqual(out.get("tools"), [])
        self.assertEqual(out.get("tool_choice"), "none")
        content = str(out["messages"][0]["content"])
        self.assertIn("admin_level_validation_failure_finalize", content)
        self.assertIn("[RUNTIME_RESPONSE_MODE] limitation", content)
        self.assertIn("Do not continue with relaxed queries", content)

    def test_loop_limits_skip_cloud_finalize_when_validated_recovery_exists(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {
                    "role": "user",
                    "content": "Gestisci sequenza cloud: lista mappe, load map, wait dataset e fallback error-safe.",
                },
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_cloud",
                            "type": "function",
                            "function": {
                                "name": "loadCloudMapAndWait",
                                "arguments": json.dumps({"mapId": "cloud-timeout-map"}),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_cloud",
                    "content": _qmap_tool_result(
                        success=False,
                        details="Cloud map load timed out after retry; no validated fallback available.",
                    ),
                },
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_fallback",
                            "type": "function",
                            "function": {"name": "loadData", "arguments": json.dumps({"source": "validated-backup"})},
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_fallback",
                    "content": _qmap_tool_result(success=True, details="Fallback dataset loaded."),
                },
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_wait",
                            "type": "function",
                            "function": {"name": "waitForQMapDataset", "arguments": json.dumps({"datasetName": "Fallback dataset"})},
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_wait",
                    "content": _qmap_tool_result(success=True, details='Dataset "Fallback dataset" is available.'),
                },
            ],
            "tools": [
                {"type": "function", "function": {"name": "countQMapRows"}},
                {"type": "function", "function": {"name": "loadData"}},
            ],
            "tool_choice": "auto",
        }

        out = _enforce_runtime_tool_loop_limits(payload)
        self.assertNotEqual(out.get("tool_choice"), "none")
        names = [tool.get("function", {}).get("name") for tool in out.get("tools", [])]
        self.assertIn("countQMapRows", names)
        content = str(out["messages"][0]["content"])
        self.assertNotIn("cloud_no_validated_fallback_finalize", content)
