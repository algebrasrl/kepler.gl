# q-map AI Governance Control Matrix

| Control | Objective | Primary Implementation Surface | Gate |
| --- | --- | --- | --- |
| `GOV-001` | Evidence-backed final claims only | q-assistant runtime guardrails + ai-eval functional cases | `ai-eval-functional` + `ai-governance-audit` |
| `GOV-002` | End-to-end traceability | `x-q-assistant-request-id` + tool audit trails + trace grading on critical functional cases | `ai-trace-grade-audit` + `ai-governance-audit` |
| `GOV-003` | Block forbidden policy routes | q-assistant runtime tool pruning/blocking | backend unittest + `ai-governance-audit` |
| `GOV-004` | Mandatory limitation/escalation on unresolved high-impact flows | runtime guardrail next-step contracts | `ai-eval-functional` + `ai-governance-audit` |
| `GOV-005` | Immutable audit log discipline | chat audit events + retention settings/tests | backend unittest + `ai-governance-audit` |
| `GOV-006` | Controlled prompt/model/provider changes | changelog discipline + test gate before merge | `changelog-audit` + `quality-gate` |

## Notes

- This matrix is baseline production governance for q-map loop integration.
- Control IDs are authoritative for `risk-register.yaml`.
