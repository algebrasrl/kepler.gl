import json

from tests.runtime_guardrails_test_support import (
    _enforce_runtime_tool_loop_limits,
    _qmap_tool_result,
)


class RuntimeGuardrailLoopLimitsRoutingMixin:
    def test_loop_limits_prune_qcumber_discovery_for_bridge_preflight(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {
                    "role": "user",
                    "content": (
                        "Verifica bridge caricamento/salvataggio dati in mappa: "
                        "usa loadData o saveDataToMap e poi valida lo stato dataset."
                    ),
                },
            ],
            "tools": [
                {"type": "function", "function": {"name": "listQCumberProviders"}},
                {"type": "function", "function": {"name": "listQCumberDatasets"}},
                {"type": "function", "function": {"name": "getQCumberDatasetHelp"}},
                {"type": "function", "function": {"name": "loadData"}},
                {"type": "function", "function": {"name": "saveDataToMap"}},
                {"type": "function", "function": {"name": "countQMapRows"}},
            ],
            "tool_choice": "auto",
        }

        out = _enforce_runtime_tool_loop_limits(payload)
        names = [tool.get("function", {}).get("name") for tool in out.get("tools", [])]
        self.assertNotIn("listQCumberProviders", names)
        self.assertNotIn("listQCumberDatasets", names)
        self.assertNotIn("getQCumberDatasetHelp", names)
        self.assertIn("loadData", names)
        self.assertIn("saveDataToMap", names)
        content = str(out["messages"][0]["content"])
        self.assertIn("bridge_no_default_qcumber_discovery", content)

    def test_loop_limits_keep_qcumber_discovery_when_inventory_requested(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {
                    "role": "user",
                    "content": (
                        "Fai inventario dataset/provider disponibili e poi verifica bridge "
                        "caricamento/salvataggio in mappa."
                    ),
                },
            ],
            "tools": [
                {"type": "function", "function": {"name": "listQCumberProviders"}},
                {"type": "function", "function": {"name": "listQCumberDatasets"}},
                {"type": "function", "function": {"name": "loadData"}},
                {"type": "function", "function": {"name": "saveDataToMap"}},
            ],
            "tool_choice": "auto",
        }

        out = _enforce_runtime_tool_loop_limits(payload)
        names = [tool.get("function", {}).get("name") for tool in out.get("tools", [])]
        self.assertIn("listQCumberProviders", names)
        self.assertIn("listQCumberDatasets", names)
        content = str(out["messages"][0]["content"])
        self.assertNotIn("bridge_no_default_qcumber_discovery", content)

    def test_loop_limits_keep_qcumber_discovery_after_bridge_failure(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {
                    "role": "user",
                    "content": (
                        "Verifica bridge caricamento/salvataggio dati in mappa: "
                        "usa loadData o saveDataToMap e poi valida lo stato dataset."
                    ),
                },
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_load",
                            "type": "function",
                            "function": {
                                "name": "loadData",
                                "arguments": json.dumps({"providerId": "local-assets-it"}),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_load",
                    "content": _qmap_tool_result(success=False, details="Dataset not found."),
                },
            ],
            "tools": [
                {"type": "function", "function": {"name": "listQCumberProviders"}},
                {"type": "function", "function": {"name": "listQCumberDatasets"}},
                {"type": "function", "function": {"name": "loadData"}},
            ],
            "tool_choice": "auto",
        }

        out = _enforce_runtime_tool_loop_limits(payload)
        names = [tool.get("function", {}).get("name") for tool in out.get("tools", [])]
        self.assertIn("listQCumberProviders", names)
        self.assertIn("listQCumberDatasets", names)
        content = str(out["messages"][0]["content"])
        self.assertNotIn("bridge_no_default_qcumber_discovery", content)

    def test_loop_limits_keep_territorial_query_without_routing_metadata_preflight(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {
                    "role": "user",
                    "content": (
                        "Analizza un tema ambientale in un'area di interesse delimitata "
                        "e restituisci le zone prioritarie ordinate per valore dell'indicatore."
                    ),
                },
            ],
            "tools": [
                {"type": "function", "function": {"name": "queryQCumberTerritorialUnits"}},
                {"type": "function", "function": {"name": "queryQCumberDatasetSpatial"}},
                {"type": "function", "function": {"name": "queryQCumberDataset"}},
                {"type": "function", "function": {"name": "rankQMapDatasetRows"}},
            ],
            "tool_choice": "auto",
        }

        out = _enforce_runtime_tool_loop_limits(payload)
        names = [tool.get("function", {}).get("name") for tool in out.get("tools", [])]
        self.assertIn("queryQCumberTerritorialUnits", names)
        self.assertIn("queryQCumberDatasetSpatial", names)
        self.assertIn("queryQCumberDataset", names)
        content = str(out["messages"][0]["content"])
        self.assertNotIn("thematic_spatial_prefer_non_territorial_query", content)

    def test_loop_limits_keep_territorial_query_for_admin_objective(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {
                    "role": "user",
                    "content": (
                        "Mostra comuni e province piu critici, con dettaglio amministrativo "
                        "e ordinamento per indicatore."
                    ),
                },
            ],
            "tools": [
                {"type": "function", "function": {"name": "queryQCumberTerritorialUnits"}},
                {"type": "function", "function": {"name": "queryQCumberDatasetSpatial"}},
            ],
            "tool_choice": "auto",
        }

        out = _enforce_runtime_tool_loop_limits(payload)
        names = [tool.get("function", {}).get("name") for tool in out.get("tools", [])]
        self.assertIn("queryQCumberTerritorialUnits", names)
        content = str(out["messages"][0]["content"])
        self.assertNotIn("thematic_spatial_prefer_non_territorial_query", content)

    def test_loop_limits_prune_territorial_query_from_metadata_without_thematic_keywords(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {"role": "user", "content": "Procedi con la query migliore sul dataset selezionato."},
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_help",
                            "type": "function",
                            "function": {"name": "getQCumberDatasetHelp", "arguments": "{}"},
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_help",
                    "content": json.dumps(
                        {
                            "qmapToolResult": {
                                "schema": "qmap.tool_result.v1",
                                "success": True,
                                "details": "Dataset help ready.",
                            },
                            "llmResult": {
                                "success": True,
                                "routing": {"isAdministrative": False, "datasetClass": "thematic_spatial"},
                            },
                        }
                    ),
                },
            ],
            "tools": [
                {"type": "function", "function": {"name": "queryQCumberTerritorialUnits"}},
                {"type": "function", "function": {"name": "queryQCumberDatasetSpatial"}},
            ],
            "tool_choice": "auto",
        }

        out = _enforce_runtime_tool_loop_limits(payload)
        names = [tool.get("function", {}).get("name") for tool in out.get("tools", [])]
        self.assertNotIn("queryQCumberTerritorialUnits", names)
        self.assertIn("queryQCumberDatasetSpatial", names)
        content = str(out["messages"][0]["content"])
        self.assertIn("thematic_spatial_prefer_non_territorial_query", content)

    def test_loop_limits_prune_territorial_query_from_routing_preferred_tool_metadata(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {"role": "user", "content": "Continua con la query operativa."},
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_help",
                            "type": "function",
                            "function": {"name": "getQCumberDatasetHelp", "arguments": "{}"},
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_help",
                    "content": json.dumps(
                        {
                            "qmapToolResult": {
                                "schema": "qmap.tool_result.v1",
                                "success": True,
                                "details": "Dataset help ready.",
                            },
                            "llmResult": {
                                "success": True,
                                "routing": {"queryToolHint": {"preferredTool": "queryQCumberDatasetSpatial"}},
                            },
                        }
                    ),
                },
            ],
            "tools": [
                {"type": "function", "function": {"name": "queryQCumberTerritorialUnits"}},
                {"type": "function", "function": {"name": "queryQCumberDatasetSpatial"}},
            ],
            "tool_choice": "auto",
        }

        out = _enforce_runtime_tool_loop_limits(payload)
        names = [tool.get("function", {}).get("name") for tool in out.get("tools", [])]
        self.assertNotIn("queryQCumberTerritorialUnits", names)
        self.assertIn("queryQCumberDatasetSpatial", names)
        content = str(out["messages"][0]["content"])
        self.assertIn("thematic_spatial_prefer_non_territorial_query", content)

    def test_loop_limits_prune_fit_without_map_focus_preflight(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {
                    "role": "user",
                    "content": (
                        "Importa o salva dati sulla mappa, poi verifica che il dataset finale sia disponibile e consistente."
                    ),
                },
            ],
            "tools": [
                {"type": "function", "function": {"name": "fitQMapToDataset"}},
                {"type": "function", "function": {"name": "loadData"}},
                {"type": "function", "function": {"name": "waitForQMapDataset"}},
                {"type": "function", "function": {"name": "countQMapRows"}},
            ],
            "tool_choice": "auto",
        }

        out = _enforce_runtime_tool_loop_limits(payload)
        names = [tool.get("function", {}).get("name") for tool in out.get("tools", [])]
        self.assertNotIn("fitQMapToDataset", names)
        self.assertIn("loadData", names)
        content = str(out["messages"][0]["content"])
        self.assertIn("fit_requires_explicit_map_focus", content)

    def test_loop_limits_keep_fit_when_explicit_map_display_requested(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {"role": "user", "content": "mostra direttamente il risultato sulla mappa dopo il caricamento"},
            ],
            "tools": [
                {"type": "function", "function": {"name": "fitQMapToDataset"}},
                {"type": "function", "function": {"name": "loadData"}},
                {"type": "function", "function": {"name": "waitForQMapDataset"}},
            ],
            "tool_choice": "auto",
        }

        out = _enforce_runtime_tool_loop_limits(payload)
        names = [tool.get("function", {}).get("name") for tool in out.get("tools", [])]
        self.assertIn("fitQMapToDataset", names)
        content = str(out["messages"][0]["content"])
        self.assertNotIn("fit_requires_explicit_map_focus", content)

    def test_loop_limits_prune_layer_order_after_successful_fit_of_isolated_winner(self):
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
                            "id": "call_query",
                            "type": "function",
                            "function": {"name": "queryQCumberTerritorialUnits", "arguments": "{}"},
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_query",
                    "content": _qmap_tool_result(success=True, details="Administrative query ok."),
                },
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
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_filter",
                            "type": "function",
                            "function": {"name": "createDatasetFromFilter", "arguments": "{}"},
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_filter",
                    "content": _qmap_tool_result(success=True, details="Winner isolated."),
                },
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_fit",
                            "type": "function",
                            "function": {"name": "fitQMapToDataset", "arguments": "{}"},
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_fit",
                    "content": _qmap_tool_result(success=True, details="Map focus ok."),
                },
            ],
            "tools": [
                {"type": "function", "function": {"name": "fitQMapToDataset"}},
                {"type": "function", "function": {"name": "setQMapLayerOrder"}},
                {"type": "function", "function": {"name": "queryQCumberTerritorialUnits"}},
            ],
            "tool_choice": "auto",
        }

        out = _enforce_runtime_tool_loop_limits(payload)
        names = [tool.get("function", {}).get("name") for tool in out.get("tools", [])]
        self.assertNotIn("setQMapLayerOrder", names)
        self.assertIn("fitQMapToDataset", names)
        content = str(out["messages"][0]["content"])
        self.assertIn("fit_completes_superlative_map_focus", content)

    def test_loop_limits_prune_layer_order_before_fit_for_admin_superlative_map_objective(self):
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
                            "id": "call_query",
                            "type": "function",
                            "function": {"name": "queryQCumberTerritorialUnits", "arguments": "{}"},
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_query",
                    "content": _qmap_tool_result(success=True, details="Administrative query ok."),
                },
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
            ],
            "tools": [
                {"type": "function", "function": {"name": "fitQMapToDataset"}},
                {"type": "function", "function": {"name": "setQMapLayerOrder"}},
                {"type": "function", "function": {"name": "createDatasetFromFilter"}},
            ],
            "tool_choice": "auto",
        }

        out = _enforce_runtime_tool_loop_limits(payload)
        names = [tool.get("function", {}).get("name") for tool in out.get("tools", [])]
        self.assertNotIn("setQMapLayerOrder", names)
        self.assertIn("fitQMapToDataset", names)
        content = str(out["messages"][0]["content"])
        self.assertIn("admin_superlative_prefers_fit_over_layer_order", content)

    def test_loop_limits_prune_layer_order_before_fit_from_admin_workflow_signal(self):
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
                            "function": {"name": "queryQCumberTerritorialUnits", "arguments": "{}"},
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_query",
                    "content": json.dumps(
                        {
                            "success": True,
                            "details": "Administrative query ok.",
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
                            "function": {"name": "rankQMapDatasetRows", "arguments": "{}"},
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_rank",
                    "content": _qmap_tool_result(success=True, details="Ranking ok."),
                },
            ],
            "tools": [
                {"type": "function", "function": {"name": "fitQMapToDataset"}},
                {"type": "function", "function": {"name": "setQMapLayerOrder"}},
                {"type": "function", "function": {"name": "createDatasetFromFilter"}},
            ],
            "tool_choice": "auto",
        }

        out = _enforce_runtime_tool_loop_limits(payload)
        names = [tool.get("function", {}).get("name") for tool in out.get("tools", [])]
        self.assertNotIn("setQMapLayerOrder", names)
        self.assertIn("fitQMapToDataset", names)
        content = str(out["messages"][0]["content"])
        self.assertIn("admin_superlative_prefers_fit_over_layer_order", content)

    def test_loop_limits_prune_qcumber_providers_without_discovery_request(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {
                    "role": "user",
                    "content": (
                        "Analizza la coerenza tra perimetri tematici e amministrativi con clipping e intersezione."
                    ),
                },
            ],
            "tools": [
                {"type": "function", "function": {"name": "listQCumberProviders"}},
                {"type": "function", "function": {"name": "listQCumberDatasets"}},
                {"type": "function", "function": {"name": "queryQCumberDataset"}},
                {"type": "function", "function": {"name": "clipDatasetByBoundary"}},
            ],
            "tool_choice": "auto",
        }

        out = _enforce_runtime_tool_loop_limits(payload)
        names = [tool.get("function", {}).get("name") for tool in out.get("tools", [])]
        self.assertNotIn("listQCumberProviders", names)
        self.assertIn("listQCumberDatasets", names)
        content = str(out["messages"][0]["content"])
        self.assertIn("provider_listing_not_required_for_current_objective", content)

    def test_loop_limits_keep_qcumber_providers_for_explicit_provider_discovery(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {
                    "role": "user",
                    "content": "Indicami provider e dataset disponibili per analisi ambientale.",
                },
            ],
            "tools": [
                {"type": "function", "function": {"name": "listQCumberProviders"}},
                {"type": "function", "function": {"name": "listQCumberDatasets"}},
                {"type": "function", "function": {"name": "queryQCumberDataset"}},
            ],
            "tool_choice": "auto",
        }

        out = _enforce_runtime_tool_loop_limits(payload)
        names = [tool.get("function", {}).get("name") for tool in out.get("tools", [])]
        self.assertIn("listQCumberProviders", names)
        content = str(out["messages"][0]["content"])
        self.assertNotIn("provider_listing_not_required_for_current_objective", content)

    def test_loop_limits_force_provider_listing_after_invalid_provider_failure(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {"role": "user", "content": "analizza il dataset forestale disponibile"},
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_bad_provider",
                            "type": "function",
                            "function": {
                                "name": "listQCumberDatasets",
                                "arguments": json.dumps({"providerId": "stale-provider"}),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_bad_provider",
                    "content": _qmap_tool_result(success=False, details="Invalid providerId."),
                },
            ],
            "tools": [
                {"type": "function", "function": {"name": "listQCumberProviders"}},
                {"type": "function", "function": {"name": "listQCumberDatasets"}},
                {"type": "function", "function": {"name": "getQCumberDatasetHelp"}},
                {"type": "function", "function": {"name": "queryQCumberDataset"}},
            ],
            "tool_choice": {"type": "function", "function": {"name": "queryQCumberDataset"}},
        }

        out = _enforce_runtime_tool_loop_limits(payload)
        names = [tool.get("function", {}).get("name") for tool in out.get("tools", [])]
        self.assertIn("listQCumberProviders", names)
        self.assertNotIn("listQCumberDatasets", names)
        self.assertNotIn("getQCumberDatasetHelp", names)
        self.assertNotIn("queryQCumberDataset", names)
        self.assertEqual(out.get("tool_choice", {}).get("function", {}).get("name"), "listQCumberProviders")
        content = str(out["messages"][0]["content"])
        self.assertIn("provider_recovery_requires_explicit_listing", content)

    def test_loop_limits_force_dataset_listing_after_invalid_dataset_failure(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {"role": "user", "content": "analizza il dataset forestale disponibile"},
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_bad_dataset",
                            "type": "function",
                            "function": {
                                "name": "getQCumberDatasetHelp",
                                "arguments": json.dumps(
                                    {"providerId": "local-assets-it", "datasetId": "stale-dataset"}
                                ),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_bad_dataset",
                    "content": _qmap_tool_result(
                        success=False,
                        details=(
                            'Invalid datasetId "stale-dataset". '
                            "Use an exact datasetId from listQCumberDatasets(providerId)."
                        ),
                    ),
                },
            ],
            "tools": [
                {"type": "function", "function": {"name": "listQCumberDatasets"}},
                {"type": "function", "function": {"name": "getQCumberDatasetHelp"}},
                {"type": "function", "function": {"name": "queryQCumberDataset"}},
                {"type": "function", "function": {"name": "queryQCumberDatasetSpatial"}},
                {"type": "function", "function": {"name": "queryQCumberTerritorialUnits"}},
            ],
            "tool_choice": {"type": "function", "function": {"name": "queryQCumberDataset"}},
        }

        out = _enforce_runtime_tool_loop_limits(payload)
        names = [tool.get("function", {}).get("name") for tool in out.get("tools", [])]
        self.assertIn("listQCumberDatasets", names)
        self.assertNotIn("getQCumberDatasetHelp", names)
        self.assertNotIn("queryQCumberDataset", names)
        self.assertNotIn("queryQCumberDatasetSpatial", names)
        self.assertNotIn("queryQCumberTerritorialUnits", names)
        self.assertEqual(out.get("tool_choice", {}).get("function", {}).get("name"), "listQCumberDatasets")
        content = str(out["messages"][0]["content"])
        self.assertIn("dataset_recovery_requires_explicit_listing", content)

    def test_loop_limits_release_dataset_recovery_after_successful_listing(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {"role": "user", "content": "analizza il dataset forestale disponibile"},
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_bad_dataset",
                            "type": "function",
                            "function": {
                                "name": "getQCumberDatasetHelp",
                                "arguments": json.dumps(
                                    {"providerId": "local-assets-it", "datasetId": "stale-dataset"}
                                ),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_bad_dataset",
                    "content": _qmap_tool_result(
                        success=False,
                        details=(
                            'Invalid datasetId "stale-dataset". '
                            "Use an exact datasetId from listQCumberDatasets(providerId)."
                        ),
                    ),
                },
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_dataset_inventory",
                            "type": "function",
                            "function": {"name": "listQCumberDatasets", "arguments": '{"providerId":"local-assets-it"}'},
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_dataset_inventory",
                    "content": _qmap_tool_result(success=True, details="Found 3 datasets."),
                },
            ],
            "tools": [
                {"type": "function", "function": {"name": "listQCumberDatasets"}},
                {"type": "function", "function": {"name": "getQCumberDatasetHelp"}},
                {"type": "function", "function": {"name": "queryQCumberDataset"}},
            ],
            "tool_choice": "auto",
        }

        out = _enforce_runtime_tool_loop_limits(payload)
        names = [tool.get("function", {}).get("name") for tool in out.get("tools", [])]
        self.assertIn("listQCumberDatasets", names)
        self.assertIn("getQCumberDatasetHelp", names)
        self.assertIn("queryQCumberDataset", names)
        content = str(out["messages"][0]["content"])
        self.assertNotIn("dataset_recovery_requires_explicit_listing", content)

    def test_loop_limits_release_provider_recovery_after_successful_listing(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {"role": "user", "content": "analizza il dataset forestale disponibile"},
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_bad_provider",
                            "type": "function",
                            "function": {
                                "name": "listQCumberDatasets",
                                "arguments": json.dumps({"providerId": "stale-provider"}),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_bad_provider",
                    "content": _qmap_tool_result(success=False, details="Invalid providerId."),
                },
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_provider_inventory",
                            "type": "function",
                            "function": {"name": "listQCumberProviders", "arguments": "{}"},
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_provider_inventory",
                    "content": _qmap_tool_result(success=True, details="Found 2 providers."),
                },
            ],
            "tools": [
                {"type": "function", "function": {"name": "listQCumberProviders"}},
                {"type": "function", "function": {"name": "listQCumberDatasets"}},
                {"type": "function", "function": {"name": "queryQCumberDataset"}},
            ],
            "tool_choice": "auto",
        }

        out = _enforce_runtime_tool_loop_limits(payload)
        names = [tool.get("function", {}).get("name") for tool in out.get("tools", [])]
        self.assertIn("listQCumberProviders", names)
        self.assertIn("listQCumberDatasets", names)
        self.assertIn("queryQCumberDataset", names)
        content = str(out["messages"][0]["content"])
        self.assertNotIn("provider_recovery_requires_explicit_listing", content)
