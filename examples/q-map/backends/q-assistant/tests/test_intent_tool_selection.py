"""Tests for intent-based tool selection."""

import unittest

from q_assistant.intent_tool_selection import (
    BALANCED_LITE,
    CORE_TOOLS,
    IntentToolSelectionResult,
    apply_intent_tool_selection,
    select_tools_for_request,
)

# Simulate a full tool set (~99 tools) by collecting all tool names from the module
from q_assistant.intent_tool_selection import (
    _CHART_TOOLS,
    _CLOUD_MAP_TOOLS,
    _EXTRA_INSPECTION_TOOLS,
    _GEOSPATIAL_ADVANCED_TOOLS,
    _GEOSPATIAL_CORE_TOOLS,
    _GEOSPATIAL_H3_TOOLS,
    _GEOSPATIAL_OVERLAY_TOOLS,
    _H3_EDITING_TOOLS,
    _MOBILITY_TOOLS,
    _REGULATORY_TOOLS,
    _STATISTICAL_TOOLS,
    _STYLING_FULL_EXTRA,
    _TIME_SERIES_TOOLS,
)

ALL_TOOLS = (
    set(CORE_TOOLS)
    | set(BALANCED_LITE)
    | set(_CHART_TOOLS)
    | set(_MOBILITY_TOOLS)
    | set(_GEOSPATIAL_CORE_TOOLS)
    | set(_GEOSPATIAL_H3_TOOLS)
    | set(_GEOSPATIAL_OVERLAY_TOOLS)
    | set(_GEOSPATIAL_ADVANCED_TOOLS)
    | set(_STATISTICAL_TOOLS)
    | set(_REGULATORY_TOOLS)
    | set(_CLOUD_MAP_TOOLS)
    | set(_STYLING_FULL_EXTRA)
    | set(_H3_EDITING_TOOLS)
    | set(_TIME_SERIES_TOOLS)
    | set(_EXTRA_INSPECTION_TOOLS)
)


class TestSelectToolsForRequest(unittest.TestCase):
    """Unit tests for select_tools_for_request."""

    def test_empty_objective_returns_core_and_balanced(self):
        result = select_tools_for_request(ALL_TOOLS, "", max_tools=50)
        self.assertTrue(CORE_TOOLS.issubset(result.selected_tools))
        self.assertTrue(BALANCED_LITE.issubset(result.selected_tools))
        self.assertEqual(len(result.matched_rules), 0)

    def test_core_tools_always_included(self):
        result = select_tools_for_request(ALL_TOOLS, "qualsiasi cosa", max_tools=50)
        for tool in CORE_TOOLS:
            self.assertIn(tool, result.selected_tools, f"Core tool {tool} missing")

    def test_chart_intent_adds_chart_tools(self):
        result = select_tools_for_request(ALL_TOOLS, "mostra un istogramma dei valori PM10", max_tools=50)
        self.assertIn("chart", result.matched_rules)
        self.assertIn("histogramTool", result.selected_tools)
        self.assertIn("boxplotTool", result.selected_tools)

    def test_mobility_intent_adds_routing_tools(self):
        result = select_tools_for_request(ALL_TOOLS, "calcola il percorso da Milano a Roma", max_tools=50)
        self.assertIn("mobility", result.matched_rules)
        self.assertIn("routing", result.selected_tools)
        self.assertIn("isochrone", result.selected_tools)

    def test_geospatial_intent_adds_spatial_tools(self):
        result = select_tools_for_request(ALL_TOOLS, "ritaglia il dataset per confini provinciali", max_tools=50)
        self.assertIn("geospatial-core", result.matched_rules)
        self.assertIn("clipQMapDatasetByGeometry", result.selected_tools)
        self.assertIn("zonalStatsByAdmin", result.selected_tools)

    def test_h3_intent_adds_tessellation_tools(self):
        result = select_tools_for_request(ALL_TOOLS, "tassella la provincia a risoluzione 8", max_tools=50)
        self.assertIn("geospatial-h3", result.matched_rules)
        self.assertIn("tassellateDatasetLayer", result.selected_tools)
        self.assertIn("aggregateDatasetToH3", result.selected_tools)

    def test_regulatory_intent_adds_compliance_tools(self):
        result = select_tools_for_request(ALL_TOOLS, "verifica conformita NO2 rispetto ai limiti di legge", max_tools=50)
        self.assertIn("regulatory", result.matched_rules)
        self.assertIn("checkRegulatoryCompliance", result.selected_tools)

    def test_statistical_intent_adds_regression_tools(self):
        result = select_tools_for_request(ALL_TOOLS, "calcola la regressione lineare tra temperatura e inquinamento", max_tools=50)
        self.assertIn("statistical", result.matched_rules)
        self.assertIn("regressQMapFields", result.selected_tools)

    def test_cloud_maps_intent(self):
        result = select_tools_for_request(ALL_TOOLS, "carica la mia mappa salvata", max_tools=50)
        self.assertIn("cloud-maps", result.matched_rules)
        self.assertIn("loadCloudMapAndWait", result.selected_tools)

    def test_time_series_intent(self):
        result = select_tools_for_request(ALL_TOOLS, "mostra le serie orarie di PM10", max_tools=50)
        self.assertIn("time-series", result.matched_rules)
        self.assertIn("summarizeQMapTimeSeries", result.selected_tools)

    def test_styling_full_intent(self):
        result = select_tools_for_request(ALL_TOOLS, "colora per popolazione con threshold", max_tools=50)
        self.assertIn("styling-full", result.matched_rules)
        self.assertIn("setQMapLayerColorByThresholds", result.selected_tools)

    def test_no_intent_stays_under_target(self):
        """Vague question should produce <= 25 tools."""
        result = select_tools_for_request(ALL_TOOLS, "che dataset conosci?", max_tools=25)
        self.assertLessEqual(result.total_after, 25)

    def test_hard_cap_enforced(self):
        """Even with multiple intents, hard cap is respected.
        Note: core+balanced = 22, so the minimum achievable is 22.
        The cap should trim intent-added tools to stay as close as possible."""
        result = select_tools_for_request(
            ALL_TOOLS,
            "ritaglia, overlay, tassella, calcola regressione, mostra istogramma",
            max_tools=25,
        )
        self.assertLessEqual(result.total_after, 25)
        # Core tools must survive the cap
        for tool in CORE_TOOLS:
            if tool in ALL_TOOLS:
                self.assertIn(tool, result.selected_tools, f"Core tool {tool} dropped by hard cap")

    def test_multi_intent_union(self):
        """Multiple intents should union their tool sets (with generous cap)."""
        result = select_tools_for_request(
            ALL_TOOLS,
            "ritaglia il dataset e mostra un grafico scatter",
            max_tools=60,
        )
        self.assertIn("geospatial-core", result.matched_rules)
        self.assertIn("chart", result.matched_rules)
        self.assertIn("clipQMapDatasetByGeometry", result.selected_tools)
        self.assertIn("scatterplotTool", result.selected_tools)

    def test_intersects_with_available_tools(self):
        """Tools not offered by frontend should not appear in selection."""
        small_set = {"fitQMapToDataset", "listQMapDatasets", "waitForQMapDataset"}
        result = select_tools_for_request(small_set, "", max_tools=50)
        self.assertTrue(result.selected_tools.issubset(small_set))

    def test_territorio_treviso_query(self):
        """Realistic territorial query should get query + styling + geo tools."""
        result = select_tools_for_request(
            ALL_TOOLS,
            "mostrami i siti rete natura 2000 nella provincia di treviso",
            max_tools=25,
        )
        self.assertLessEqual(result.total_after, 25)
        # Must have query tools for the workflow
        self.assertIn("queryQCumberTerritorialUnits", result.selected_tools)
        self.assertIn("queryQCumberDatasetSpatial", result.selected_tools)
        self.assertIn("deriveQMapDatasetBbox", result.selected_tools)


class TestApplyIntentToolSelection(unittest.TestCase):
    """Tests for the payload-level apply function."""

    def _make_payload(self, tool_names):
        return {
            "messages": [
                {"role": "user", "content": "test objective"},
            ],
            "tools": [
                {"type": "function", "function": {"name": name}}
                for name in tool_names
            ],
            "tool_choice": "auto",
        }

    def test_filters_payload_tools(self):
        payload = self._make_payload(ALL_TOOLS)
        result = apply_intent_tool_selection(
            payload, objective_text="che dataset conosci?", max_tools=25
        )
        self.assertLessEqual(len(result["tools"]), 25)

    def test_resets_forced_tool_choice_if_pruned(self):
        payload = self._make_payload(ALL_TOOLS)
        payload["tool_choice"] = {
            "type": "function",
            "function": {"name": "paintQMapH3Cell"},
        }
        result = apply_intent_tool_selection(
            payload, objective_text="che dataset conosci?", max_tools=25
        )
        # paintQMapH3Cell is h3-editing, not triggered by "che dataset conosci?"
        if "paintQMapH3Cell" not in {
            (t.get("function") or {}).get("name") for t in result["tools"]
        }:
            self.assertEqual(result["tool_choice"], "auto")

    def test_empty_tools_passthrough(self):
        payload = {"messages": [{"role": "user", "content": "test"}]}
        result = apply_intent_tool_selection(payload, objective_text="test", max_tools=20)
        self.assertNotIn("tools", result)


if __name__ == "__main__":
    unittest.main()
