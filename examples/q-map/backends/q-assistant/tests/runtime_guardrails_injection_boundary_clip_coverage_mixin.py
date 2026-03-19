import json

from tests.runtime_guardrails_test_support import (
    _inject_runtime_guardrail_message,
    _qmap_tool_result,
)


class RuntimeGuardrailInjectionBoundaryClipCoverageMixin:
    def test_requires_boundary_clip_before_final_h3_confirmation(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {"role": "user", "content": "Mostrami celle H3 risoluzione 6 del Veneto con copertura boschiva"},
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_boundary",
                            "type": "function",
                            "function": {
                                "name": "queryQCumberTerritorialUnits",
                                "arguments": json.dumps({"datasetId": "kontur-boundaries-italia"}),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_boundary",
                    "content": _qmap_tool_result(success=True, details="Loaded Veneto boundary."),
                },
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_agg",
                            "type": "function",
                            "function": {
                                "name": "aggregateDatasetToH3",
                                "arguments": json.dumps({"targetDatasetName": "H3_Veneto_Boschi_CLC"}),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_agg",
                    "content": _qmap_tool_result(success=True, details="Aggregation completed."),
                },
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_color",
                            "type": "function",
                            "function": {
                                "name": "setQMapLayerColorByField",
                                "arguments": json.dumps({"datasetName": "H3_Veneto_Boschi_CLC", "fieldName": "sum"}),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_color",
                    "content": _qmap_tool_result(success=True, details="Color applied."),
                },
            ],
            "tools": [
                {"type": "function", "function": {"name": "clipQMapDatasetByGeometry"}},
                {"type": "function", "function": {"name": "waitForQMapDataset"}},
                {"type": "function", "function": {"name": "countQMapRows"}},
            ],
        }
        out = _inject_runtime_guardrail_message(payload)
        content = str(out["messages"][0]["content"])
        self.assertIn("[RUNTIME_GUARDRAIL]", content)
        self.assertIn("no successful clip step yet", content)
        self.assertIn("clipQMapDatasetByGeometry", content)
        self.assertIn('sourceDatasetName="H3_Veneto_Boschi_CLC"', content)

    def test_requires_coverage_report_after_perimeter_overlay_flow(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {
                    "role": "user",
                    "content": "Esegui analisi tra perimetri con intersezione e verifica copertura del risultato finale",
                },
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_clip",
                            "type": "function",
                            "function": {
                                "name": "clipDatasetByBoundary",
                                "arguments": json.dumps(
                                    {
                                        "sourceDatasetName": "Stressor Events",
                                        "boundaryDatasetName": "Kontur Boundaries Italia",
                                        "newDatasetName": "Stressor Clipped",
                                    }
                                ),
                            },
                        }
                    ],
                },
                {"role": "tool", "tool_call_id": "call_clip", "content": _qmap_tool_result(success=True, details="Clip completed.")},
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_overlay",
                            "type": "function",
                            "function": {
                                "name": "overlayIntersection",
                                "arguments": json.dumps(
                                    {
                                        "datasetAName": "Stressor Clipped",
                                        "datasetBName": "Kontur Boundaries Italia",
                                        "newDatasetName": "Intersection Result",
                                    }
                                ),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_overlay",
                    "content": _qmap_tool_result(success=True, details="Overlay intersection completed."),
                },
            ],
            "tools": [{"type": "function", "function": {"name": "coverageQualityReport"}}],
        }
        out = _inject_runtime_guardrail_message(payload)
        content = str(out["messages"][0]["content"])
        self.assertIn("perimeter_overlay_coverage_required", content)
        self.assertIn("coverageQualityReport", content)
        self.assertIn('leftDatasetName="Intersection Result"', content)
        self.assertIn('rightDatasetName="Kontur Boundaries Italia"', content)
        self.assertIn("coveragePct", content)

    def test_skips_perimeter_coverage_guardrail_when_coverage_already_done(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {
                    "role": "user",
                    "content": "Esegui analisi tra perimetri con intersezione e verifica copertura del risultato finale",
                },
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_clip",
                            "type": "function",
                            "function": {
                                "name": "clipDatasetByBoundary",
                                "arguments": json.dumps(
                                    {
                                        "sourceDatasetName": "Stressor Events",
                                        "boundaryDatasetName": "Kontur Boundaries Italia",
                                        "newDatasetName": "Stressor Clipped",
                                    }
                                ),
                            },
                        }
                    ],
                },
                {"role": "tool", "tool_call_id": "call_clip", "content": _qmap_tool_result(success=True, details="Clip completed.")},
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_overlay",
                            "type": "function",
                            "function": {
                                "name": "overlayIntersection",
                                "arguments": json.dumps(
                                    {
                                        "datasetAName": "Stressor Clipped",
                                        "datasetBName": "Kontur Boundaries Italia",
                                        "newDatasetName": "Intersection Result",
                                    }
                                ),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_overlay",
                    "content": _qmap_tool_result(success=True, details="Overlay intersection completed."),
                },
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_cov",
                            "type": "function",
                            "function": {
                                "name": "coverageQualityReport",
                                "arguments": json.dumps(
                                    {
                                        "leftDatasetName": "Intersection Result",
                                        "rightDatasetName": "Kontur Boundaries Italia",
                                    }
                                ),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_cov",
                    "content": _qmap_tool_result(success=True, details="Coverage report: matchedRows=80 coveragePct=87."),
                },
            ],
            "tools": [{"type": "function", "function": {"name": "coverageQualityReport"}}],
        }
        out = _inject_runtime_guardrail_message(payload)
        combined = "\n".join(
            str(message.get("content") or "")
            for message in out.get("messages", [])
            if isinstance(message, dict)
        )
        self.assertNotIn("perimeter_overlay_coverage_required", combined)

    def test_requires_coverage_report_after_cross_geometry_clip_stats_flow(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {
                    "role": "user",
                    "content": (
                        "Esegui clip/intersezioni tra livelli diversi (shape e H3) e produci statistiche "
                        "confrontabili (percentuale area, conteggio elementi, aggregazioni principali)."
                    ),
                },
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_clip",
                            "type": "function",
                            "function": {
                                "name": "clipQMapDatasetByGeometry",
                                "arguments": json.dumps(
                                    {
                                        "sourceDatasetName": "stressor_events_h3",
                                        "clipDatasetName": "h3_grid_treviso_r8",
                                        "boundaryDatasetName": "provincia_treviso",
                                        "newDatasetName": "stressor_events_h3_clipped",
                                    }
                                ),
                            },
                        }
                    ],
                },
                {"role": "tool", "tool_call_id": "call_clip", "content": _qmap_tool_result(success=True, details="Clip completed.")},
            ],
            "tools": [{"type": "function", "function": {"name": "coverageQualityReport"}}],
        }
        out = _inject_runtime_guardrail_message(payload)
        content = str(out["messages"][0]["content"])
        self.assertIn("perimeter_overlay_coverage_required", content)
        self.assertIn("coverageQualityReport", content)
        self.assertIn('leftDatasetName="stressor_events_h3_clipped"', content)
        self.assertIn('rightDatasetName="provincia_treviso"', content)
        self.assertIn("coveragePct", content)

    def test_requires_clip_step_before_cross_geometry_stats_finalization(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {
                    "role": "user",
                    "content": (
                        "Esegui clip/intersezioni tra livelli diversi (shape e H3) e produci statistiche "
                        "confrontabili (percentuale area, conteggio elementi, aggregazioni principali)."
                    ),
                },
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_tess",
                            "type": "function",
                            "function": {
                                "name": "tassellateDatasetLayer",
                                "arguments": json.dumps(
                                    {
                                        "datasetName": "provincia_treviso",
                                        "resolution": 8,
                                        "newDatasetName": "h3_treviso_r8",
                                    }
                                ),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_tess",
                    "content": _qmap_tool_result(success=True, details='Created dataset "h3_treviso_r8".'),
                },
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_wait",
                            "type": "function",
                            "function": {
                                "name": "waitForQMapDataset",
                                "arguments": json.dumps({"datasetName": "h3_treviso_r8"}),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_wait",
                    "content": _qmap_tool_result(success=True, details='Dataset "h3_treviso_r8" is available.'),
                },
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_area",
                            "type": "function",
                            "function": {
                                "name": "createDatasetWithGeometryArea",
                                "arguments": json.dumps(
                                    {
                                        "datasetName": "h3_treviso_r8",
                                        "newDatasetName": "h3_treviso_r8_area",
                                    }
                                ),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_area",
                    "content": _qmap_tool_result(success=True, details='Created dataset "h3_treviso_r8_area".'),
                },
            ],
            "tools": [
                {"type": "function", "function": {"name": "clipQMapDatasetByGeometry"}},
                {"type": "function", "function": {"name": "waitForQMapDataset"}},
                {"type": "function", "function": {"name": "countQMapRows"}},
            ],
        }
        out = _inject_runtime_guardrail_message(payload)
        content = str(out["messages"][0]["content"])
        self.assertIn("clip_stats_clip_required", content)
        self.assertIn("no successful clip step is available yet", content)
        self.assertIn("clipQMapDatasetByGeometry", content)

    def test_skips_cross_geometry_clip_stats_guardrail_when_coverage_already_done(self):
        payload = {
            "messages": [
                {"role": "system", "content": "System"},
                {
                    "role": "user",
                    "content": (
                        "Esegui clip/intersezioni tra livelli diversi (shape e H3) e produci statistiche "
                        "confrontabili (percentuale area, conteggio elementi, aggregazioni principali)."
                    ),
                },
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_clip",
                            "type": "function",
                            "function": {
                                "name": "clipQMapDatasetByGeometry",
                                "arguments": json.dumps(
                                    {
                                        "sourceDatasetName": "stressor_events_h3",
                                        "clipDatasetName": "h3_grid_treviso_r8",
                                        "boundaryDatasetName": "provincia_treviso",
                                        "newDatasetName": "stressor_events_h3_clipped",
                                    }
                                ),
                            },
                        }
                    ],
                },
                {"role": "tool", "tool_call_id": "call_clip", "content": _qmap_tool_result(success=True, details="Clip completed.")},
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_cov",
                            "type": "function",
                            "function": {
                                "name": "coverageQualityReport",
                                "arguments": json.dumps(
                                    {
                                        "leftDatasetName": "stressor_events_h3_clipped",
                                        "rightDatasetName": "provincia_treviso",
                                    }
                                ),
                            },
                        }
                    ],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_cov",
                    "content": _qmap_tool_result(success=True, details="Coverage report: matchedRows=920 coveragePct=98."),
                },
            ],
            "tools": [{"type": "function", "function": {"name": "coverageQualityReport"}}],
        }
        out = _inject_runtime_guardrail_message(payload)
        combined = "\n".join(
            str(message.get("content") or "")
            for message in out.get("messages", [])
            if isinstance(message, dict)
        )
        self.assertNotIn("perimeter_overlay_coverage_required", combined)
