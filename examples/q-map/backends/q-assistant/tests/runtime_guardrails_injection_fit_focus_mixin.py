import json

from tests.runtime_guardrails_test_support import (
    _inject_runtime_guardrail_message,
    _qmap_tool_result,
)


class RuntimeGuardrailInjectionFitFocusMixin:
    def test_centering_objective_requires_successful_fit(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {"role": "user", "content": "mostra i boschi dell'appennino e centra la mappa"},
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_query",
                            "type": "function",
                            "function": {
                                "name": "queryQCumberDatasetSpatial",
                                "arguments": json.dumps(
                                    {
                                        "providerId": "local-assets-it",
                                        "datasetId": "clc-2018-italia",
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
                            "datasetName": "CLC 2018 Italia (query) [abc123]",
                            "datasetRef": "id:local-assets-it-clc-2018-italia-query-abc123",
                            "details": "Query completed and loaded dataset.",
                        }
                    ),
                },
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_fit",
                            "type": "function",
                            "function": {
                                "name": "fitQMapToDataset",
                                "arguments": json.dumps(
                                    {"datasetName": "id:local-assets-it-clc-2018-italia-query-abc123"}
                                ),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_fit",
                    "content": _qmap_tool_result(
                        success=False,
                        details=(
                            "Hard-enforce turn state: discovery step is mandatory. "
                            "Call listQMapDatasets first to capture the current map snapshot, "
                            "then continue with operational tools."
                        ),
                    ),
                },
            ],
            "tools": [
                {"type": "function", "function": {"name": "fitQMapToDataset"}},
                {"type": "function", "function": {"name": "listQMapDatasets"}},
            ],
        }
        out = _inject_runtime_guardrail_message(payload)
        content = str(out["messages"][0]["content"])
        self.assertIn("centering_requires_successful_fit", content)
        self.assertIn("Do not claim that the map is centered", content)
        self.assertIn("fitQMapToDataset", content)

    def test_map_display_objective_requires_fit_even_without_centering_keywords(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {"role": "user", "content": "mostra il comune piu piccolo della provincia di treviso sulla mappa"},
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
                            "datasetRef": "id:local-assets-it-kontur-boundaries-italia-query-abc123",
                            "details": "Query completed and loaded dataset.",
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
                                        "datasetName": "id:local-assets-it-kontur-boundaries-italia-query-abc123",
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
                                        "sourceDatasetName": "id:local-assets-it-kontur-boundaries-italia-query-abc123",
                                        "newDatasetName": "comune_piu_piccolo_treviso",
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
                    "content": _qmap_tool_result(success=True, details="Dataset filtered and loaded on map."),
                },
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_wait",
                            "type": "function",
                            "function": {
                                "name": "waitForQMapDataset",
                                "arguments": json.dumps({"datasetName": "comune_piu_piccolo_treviso"}),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_wait",
                    "content": _qmap_tool_result(
                        success=True, details='Dataset "comune_piu_piccolo_treviso" is available (1 rows).'
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
                                "arguments": json.dumps({"datasetName": "comune_piu_piccolo_treviso"}),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_count",
                    "content": _qmap_tool_result(success=True, details='Dataset "comune_piu_piccolo_treviso": 1 rows.'),
                },
            ],
            "tools": [
                {"type": "function", "function": {"name": "fitQMapToDataset"}},
                {"type": "function", "function": {"name": "showOnlyQMapLayer"}},
            ],
        }
        out = _inject_runtime_guardrail_message(payload)
        content = str(out["messages"][0]["content"])
        self.assertIn("map_display_requires_fit_evidence", content)
        self.assertIn("fitQMapToDataset", content)

    def test_cloud_load_objective_forces_wait_validation_before_success_claim(self):
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
                                "arguments": json.dumps({"mapId": "cloud-map-123"}),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_cloud",
                    "content": json.dumps(
                        {
                            "qmapToolResult": {
                                "schema": "qmap.tool_result.v1",
                                "success": True,
                                "details": "Cloud map loaded.",
                            },
                            "datasetName": "Cloud map dataset [abc123]",
                            "datasetRef": "id:cloud-map-dataset-abc123",
                        }
                    ),
                },
            ],
            "tools": [
                {"type": "function", "function": {"name": "waitForQMapDataset"}},
                {"type": "function", "function": {"name": "countQMapRows"}},
            ],
            "tool_choice": "auto",
        }
        out = _inject_runtime_guardrail_message(payload)
        content = str(out["messages"][0]["content"])
        self.assertIn("cloud_load_requires_wait_validation", content)
        self.assertIn("waitForQMapDataset", content)
        self.assertEqual(
            out.get("tool_choice"),
            {"type": "function", "function": {"name": "waitForQMapDataset"}},
        )

    def test_admin_superlative_map_objective_forces_territorial_query_first(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {"role": "user", "content": "mostra il comune piu piccolo della provincia di treviso sulla mappa"},
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
                {
                    "role": "tool",
                    "tool_call_id": "call_list",
                    "content": _qmap_tool_result(success=True, details="Snapshot ok."),
                },
            ],
            "tools": [
                {"type": "function", "function": {"name": "queryQCumberTerritorialUnits"}},
                {"type": "function", "function": {"name": "rankQMapDatasetRows"}},
                {"type": "function", "function": {"name": "fitQMapToDataset"}},
            ],
            "tool_choice": "auto",
        }
        out = _inject_runtime_guardrail_message(payload)
        content = str(out["messages"][0]["content"])
        self.assertIn("admin_superlative_requires_territorial_query", content)
        self.assertIn("queryQCumberTerritorialUnits", content)
        self.assertEqual(
            out.get("tool_choice"),
            {"type": "function", "function": {"name": "queryQCumberTerritorialUnits"}},
        )

    def test_admin_superlative_heldout_prompt_forces_territorial_query_first(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {
                    "role": "user",
                    "content": (
                        "Senza fare assunzioni se il ranking e ambiguo, trova l'unita amministrativa "
                        "con il valore piu alto, materializza solo il risultato vincente e portalo in primo piano sulla mappa."
                    ),
                },
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
                {
                    "role": "tool",
                    "tool_call_id": "call_list",
                    "content": _qmap_tool_result(success=True, details="Snapshot ok."),
                },
            ],
            "tools": [
                {"type": "function", "function": {"name": "queryQCumberTerritorialUnits"}},
                {"type": "function", "function": {"name": "rankQMapDatasetRows"}},
                {"type": "function", "function": {"name": "fitQMapToDataset"}},
            ],
            "tool_choice": "auto",
        }
        out = _inject_runtime_guardrail_message(payload)
        content = str(out["messages"][0]["content"])
        self.assertIn("admin_superlative_requires_territorial_query", content)
        self.assertEqual(
            out.get("tool_choice"),
            {"type": "function", "function": {"name": "queryQCumberTerritorialUnits"}},
        )

    def test_admin_superlative_isolated_winner_forces_fit_after_count(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {"role": "user", "content": "mostra il comune piu piccolo della provincia di treviso sulla mappa"},
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
                            "datasetRef": "id:local-assets-it-kontur-boundaries-italia-query-abc123",
                            "details": "Query completed and loaded dataset.",
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
                                        "datasetName": "id:local-assets-it-kontur-boundaries-italia-query-abc123",
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
                                        "sourceDatasetName": "id:local-assets-it-kontur-boundaries-italia-query-abc123",
                                        "newDatasetName": "comune_piu_piccolo_treviso",
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
                    "content": _qmap_tool_result(success=True, details="Dataset filtered and loaded on map."),
                },
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_wait",
                            "type": "function",
                            "function": {
                                "name": "waitForQMapDataset",
                                "arguments": json.dumps({"datasetName": "comune_piu_piccolo_treviso"}),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_wait",
                    "content": _qmap_tool_result(
                        success=True, details='Dataset "comune_piu_piccolo_treviso" is available (1 rows).'
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
                                "arguments": json.dumps({"datasetName": "comune_piu_piccolo_treviso"}),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_count",
                    "content": _qmap_tool_result(success=True, details='Dataset "comune_piu_piccolo_treviso": 1 rows.'),
                },
            ],
            "tools": [
                {"type": "function", "function": {"name": "fitQMapToDataset"}},
                {"type": "function", "function": {"name": "rankQMapDatasetRows"}},
            ],
            "tool_choice": "auto",
        }
        out = _inject_runtime_guardrail_message(payload)
        content = str(out["messages"][0]["content"])
        self.assertIn("admin_superlative_isolated_winner_requires_fit", content)
        self.assertIn("fitQMapToDataset", content)
        self.assertEqual(
            out.get("tool_choice"),
            {"type": "function", "function": {"name": "fitQMapToDataset"}},
        )

    def test_admin_superlative_isolated_winner_forces_fit_from_workflow_signal(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {"role": "user", "content": "continua"},
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
                            "datasetRef": "id:local-assets-it-kontur-boundaries-italia-query-abc123",
                            "details": "Query completed and loaded dataset.",
                            "routing": {
                                "isAdministrative": True,
                                "datasetClass": "administrative",
                                "queryToolHint": {"preferredTool": "queryQCumberTerritorialUnits"},
                            },
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
                                        "datasetName": "id:local-assets-it-kontur-boundaries-italia-query-abc123",
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
                                        "sourceDatasetName": "id:local-assets-it-kontur-boundaries-italia-query-abc123",
                                        "newDatasetName": "comune_piu_piccolo_treviso",
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
                    "content": _qmap_tool_result(success=True, details="Dataset filtered and loaded on map."),
                },
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_wait",
                            "type": "function",
                            "function": {
                                "name": "waitForQMapDataset",
                                "arguments": json.dumps({"datasetName": "comune_piu_piccolo_treviso"}),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_wait",
                    "content": _qmap_tool_result(
                        success=True, details='Dataset "comune_piu_piccolo_treviso" is available (1 rows).'
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
                                "arguments": json.dumps({"datasetName": "comune_piu_piccolo_treviso"}),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_count",
                    "content": _qmap_tool_result(success=True, details='Dataset "comune_piu_piccolo_treviso": 1 rows.'),
                },
            ],
            "tools": [
                {"type": "function", "function": {"name": "fitQMapToDataset"}},
                {"type": "function", "function": {"name": "rankQMapDatasetRows"}},
            ],
            "tool_choice": "auto",
        }
        out = _inject_runtime_guardrail_message(payload)
        content = str(out["messages"][0]["content"])
        self.assertIn("admin_superlative_isolated_winner_requires_fit", content)
        self.assertIn("fitQMapToDataset", content)
        self.assertEqual(
            out.get("tool_choice"),
            {"type": "function", "function": {"name": "fitQMapToDataset"}},
        )
