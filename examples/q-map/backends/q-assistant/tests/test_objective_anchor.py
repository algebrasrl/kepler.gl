import unittest

from q_assistant.objective_anchor import (
    _build_objective_focus_terms,
    _inject_objective_anchor_message,
    _normalize_openai_response_final_text,
)
from q_assistant.objective_focus import _extract_objective_focus_terms
from q_assistant.openai_chat_payload import _coerce_openai_chat_payload


class ObjectiveAnchorTests(unittest.TestCase):
    def test_appends_anchor_to_existing_system_message(self):
        payload = {
            "messages": [
                {"role": "system", "content": "Regole di sistema"},
                {"role": "user", "content": "tassella veneto a risoluzione 6"},
            ]
        }
        out = _inject_objective_anchor_message(payload)
        messages = out.get("messages", [])
        self.assertEqual(len(messages), 2)
        self.assertEqual(messages[0].get("role"), "system")
        content = str(messages[0].get("content") or "")
        self.assertIn("[OBJECTIVE_ANCHOR]", content)
        self.assertIn("tassella veneto a risoluzione 6", content)
        self.assertIn("[OBJECTIVE_CRITERIA]", content)

    def test_prepends_system_message_when_missing(self):
        payload = {"messages": [{"role": "user", "content": "carica i comuni veneti"}]}
        out = _inject_objective_anchor_message(payload)
        messages = out.get("messages", [])
        self.assertTrue(messages)
        self.assertEqual(messages[0].get("role"), "system")
        self.assertIn("[OBJECTIVE_ANCHOR]", str(messages[0].get("content") or ""))

    def test_replaces_previous_anchor_instead_of_duplicating(self):
        payload = {
            "messages": [
                {
                    "role": "system",
                    "content": "Regole\n[OBJECTIVE_ANCHOR] Active user goal: vecchio\n[OBJECTIVE_CRITERIA] old",
                },
                {"role": "user", "content": "nuovo obiettivo"},
            ]
        }
        out = _inject_objective_anchor_message(payload)
        content = str(out.get("messages", [])[0].get("content") or "")
        self.assertEqual(content.count("[OBJECTIVE_ANCHOR]"), 1)
        self.assertEqual(content.count("[OBJECTIVE_CRITERIA]"), 1)
        self.assertIn("nuovo obiettivo", content)
        self.assertNotIn("vecchio", content)

    def test_adds_dataset_discovery_restraint_when_not_requested(self):
        payload = {
            "messages": [
                {"role": "system", "content": "Regole di sistema"},
                {"role": "user", "content": "calcola ranking aree critiche e colora mappa"},
            ]
        }
        out = _inject_objective_anchor_message(payload)
        content = str(out.get("messages", [])[0].get("content") or "")
        self.assertIn("Do not call listQMapDatasets as a default first step", content)

    def test_skips_dataset_discovery_restraint_for_inventory_objective(self):
        payload = {
            "messages": [
                {"role": "system", "content": "Regole di sistema"},
                {"role": "user", "content": "fai inventario dataset disponibili"},
            ]
        }
        out = _inject_objective_anchor_message(payload)
        content = str(out.get("messages", [])[0].get("content") or "")
        self.assertNotIn("Do not call listQMapDatasets as a default first step", content)

    def test_adds_keyword_coverage_template_with_focus_terms(self):
        payload = {
            "messages": [
                {"role": "system", "content": "Regole di sistema"},
                {"role": "user", "content": "esegui ranking e distinct su dataset finale"},
            ]
        }
        out = _inject_objective_anchor_message(payload)
        content = str(out.get("messages", [])[0].get("content") or "")
        self.assertIn("Copertura obiettivo:", content)
        self.assertIn("ranking", content)
        self.assertIn("distinct", content)

    def test_adds_required_phrases_for_cross_geometry_clip_stats_objective(self):
        payload = {
            "messages": [
                {"role": "system", "content": "Regole di sistema"},
                {
                    "role": "user",
                    "content": (
                        "Esegui clip/intersezioni tra livelli diversi (shape e H3) e produci statistiche "
                        "confrontabili (percentuale area, conteggio elementi, aggregazioni principali)."
                    ),
                },
            ]
        }
        out = _inject_objective_anchor_message(payload)
        content = str(out.get("messages", [])[0].get("content") or "")
        self.assertIn("percentuale area", content)
        self.assertIn("conteggio", content)
        self.assertIn("shape", content)
        self.assertIn("h3", content)

    def test_adds_required_phrases_for_geo_transform_chain_objective(self):
        payload = {
            "messages": [
                {"role": "system", "content": "Regole di sistema"},
                {
                    "role": "user",
                    "content": "Esegui una pipeline geospaziale con clipping, dissolve e overlay su dataset caricati.",
                },
            ]
        }
        out = _inject_objective_anchor_message(payload)
        content = str(out.get("messages", [])[0].get("content") or "")
        self.assertIn("clip", content)
        self.assertIn("dissolve", content)
        self.assertIn("overlay", content)

    def test_adds_required_phrases_for_cloud_load_sequence_objective(self):
        payload = {
            "messages": [
                {"role": "system", "content": "Regole di sistema"},
                {
                    "role": "user",
                    "content": "Elenca mappe cloud e gestisci in modo robusto il caricamento con attesa dataset.",
                },
            ]
        }
        out = _inject_objective_anchor_message(payload)
        content = str(out.get("messages", [])[0].get("content") or "")
        self.assertIn("cloud", content)
        self.assertIn("map", content)
        self.assertIn("dataset", content)

    def test_adds_required_phrases_for_direct_vs_derived_ranking_objective(self):
        payload = {
            "messages": [
                {"role": "system", "content": "Regole di sistema"},
                {
                    "role": "user",
                    "content": (
                        "Calcola ranking per proprieta dirette e per metrica derivata/normalizzata, "
                        "poi confronta i risultati e segnala eventuali differenze o ex-aequo."
                    ),
                },
            ]
        }
        out = _inject_objective_anchor_message(payload)
        content = str(out.get("messages", [])[0].get("content") or "")
        self.assertIn("metrica derivata", content)
        self.assertIn("normalizzata", content)
        self.assertIn("confronto", content)
        self.assertIn("ex-aequo", content)

    def test_extract_objective_focus_terms_prioritizes_domain_tokens(self):
        terms = _extract_objective_focus_terms(
            "Gestisci visibilità e ordine layer con isolamento finale e fit mappa", max_terms=3
        )
        self.assertTrue(terms)
        self.assertIn("visibilità", terms)
        self.assertIn("ordine", terms)

    def test_extract_objective_focus_terms_prioritizes_outcome_terms_in_thematic_prompt(self):
        terms = _extract_objective_focus_terms(
            "Analizza un tema ambientale in un'area di interesse delimitata e restituisci le zone prioritarie ordinate per valore dell'indicatore.",
            max_terms=3,
        )
        self.assertTrue(terms)
        self.assertIn("delimitata", terms)
        self.assertIn("prioritarie", terms)
        self.assertIn("indicatore", terms)

    def test_ignores_finalize_control_prompt_when_extracting_objective(self):
        payload = {
            "messages": [
                {"role": "system", "content": "Regole di sistema"},
                {"role": "user", "content": "valuta conformità e superamenti soglia per provincia"},
                {
                    "role": "user",
                    "content": "Tool execution complete. Provide a concise final answer in plain text without calling tools.",
                },
            ]
        }
        out = _inject_objective_anchor_message(payload)
        content = str(out.get("messages", [])[0].get("content") or "")
        self.assertIn("valuta conformità e superamenti soglia per provincia", content)
        self.assertNotIn("Tool execution complete.", content)

    def test_pipeline_anchor_ignores_dirty_assistant_noise_and_keeps_user_goal(self):
        payload = {
            "messages": [
                {"role": "system", "content": "Regole di sistema"},
                {"role": "user", "content": "Valuta copertura spaziale per tema ambientale e ordina aree prioritarie."},
                {
                    "role": "assistant",
                    "content": "[requestId: xyz123]\n[executionSummary] {\"status\":\"partial\"}\x00\x1f",
                },
                {
                    "role": "assistant",
                    "content": "[guardrail] runtime noisy line\n[progress] steps=2/4",
                },
            ]
        }
        coerced = _coerce_openai_chat_payload(payload)
        out = _inject_objective_anchor_message(coerced)
        content = str(out.get("messages", [])[0].get("content") or "")
        self.assertIn("Valuta copertura spaziale per tema ambientale", content)
        self.assertNotIn("[requestId:", content)
        self.assertNotIn("[executionSummary]", content)
        self.assertNotRegex(content, r"[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]")

    def test_build_objective_focus_terms_merges_required_and_priority_terms(self):
        terms = _build_objective_focus_terms(
            "Calcola ranking su metrica derivata normalizzata e confronta ex-aequo sul dataset finale.",
            max_terms=8,
        )
        self.assertIn("metrica derivata", terms)
        self.assertIn("normalizzata", terms)
        self.assertIn("confronto", terms)
        self.assertIn("ex-aequo", terms)

    def test_build_objective_focus_terms_includes_retry_fallback_for_cloud_timeout_objective(self):
        terms = _build_objective_focus_terms(
            "Gestisci un caricamento cloud con timeout: applica retry/fallback su alternativa valida.",
            max_terms=8,
        )
        self.assertIn("retry", terms)
        self.assertIn("fallback", terms)
        self.assertIn("cloud", terms)

    def test_build_objective_focus_terms_includes_population_modes_for_h3_flow(self):
        terms = _build_objective_focus_terms(
            (
                "Distribuisci la popolazione su griglia H3 confrontando criteri standard, "
                "proporzionale alla superficie e discreto."
            ),
            max_terms=8,
        )
        self.assertIn("proporzionale", terms)
        self.assertIn("discreto", terms)
        self.assertIn("popolazione", terms)

    def test_normalize_openai_response_final_text_strips_instruction_leak_and_adds_coverage(self):
        payload = {
            "choices": [
                {
                    "message": {
                        "role": "assistant",
                        "content": (
                            "Workflow completato.\n"
                            "In final text include one explicit line `Copertura obiettivo: ...` reusing these exact terms: ranking, distinct.\n"
                            "Use these terms only in final narrative; do not call extra tools only to satisfy lexical coverage."
                        ),
                    }
                }
            ]
        }
        out = _normalize_openai_response_final_text(
            payload,
            objective_text="Esegui ranking e distinct su dataset finale.",
        )
        message = out["choices"][0]["message"]
        content = str(message.get("content") or "")
        self.assertIn("Workflow completato.", content)
        self.assertIn("Copertura obiettivo:", content)
        self.assertIn("ranking", content)
        self.assertIn("distinct", content)
        self.assertNotIn("Include one explicit line", content)
        self.assertNotIn("Use these terms only in final narrative", content)

    def test_normalize_openai_response_final_text_strips_runtime_progress_envelope(self):
        payload = {
            "choices": [
                {
                    "message": {
                        "role": "assistant",
                        "content": (
                            "[progress] steps=3/3 failed=1\n"
                            "[executionSummary] {\"requestId\":\"abc\",\"status\":\"failed\"}\n"
                            "Operazione non completata per timeout del dataset."
                        ),
                    }
                }
            ]
        }
        out = _normalize_openai_response_final_text(
            payload,
            objective_text="Mostrami il risultato finale sulla mappa.",
        )
        content = str(out["choices"][0]["message"].get("content") or "")
        self.assertNotIn("[progress]", content)
        self.assertNotIn("[executionSummary]", content)
        self.assertIn("Operazione non completata", content)

    def test_normalize_openai_response_final_text_does_not_emit_coverage_for_progress_only_payload(self):
        payload = {
            "choices": [
                {
                    "message": {
                        "role": "assistant",
                        "content": "[progress] steps=2/2\n[executionSummary] {\"status\":\"success\"}",
                    }
                }
            ]
        }
        out = _normalize_openai_response_final_text(
            payload,
            objective_text="Esegui ranking e distinct su dataset finale.",
        )
        content = str(out["choices"][0]["message"].get("content") or "")
        self.assertEqual(content, "")
        self.assertNotIn("Copertura obiettivo:", content)

    def test_normalize_openai_response_final_text_skips_tool_call_messages(self):
        payload = {
            "choices": [
                {
                    "message": {
                        "role": "assistant",
                        "content": "Pensando alla prossima azione.",
                        "tool_calls": [
                            {
                                "id": "call_1",
                                "type": "function",
                                "function": {"name": "listQMapDatasets", "arguments": "{}"},
                            }
                        ],
                    }
                }
            ]
        }
        out = _normalize_openai_response_final_text(
            payload,
            objective_text="Esegui ranking e distinct su dataset finale.",
        )
        content = str(out["choices"][0]["message"].get("content") or "")
        self.assertEqual(content, "Pensando alla prossima azione.")
        self.assertNotIn("Copertura obiettivo:", content)


if __name__ == "__main__":
    unittest.main()
