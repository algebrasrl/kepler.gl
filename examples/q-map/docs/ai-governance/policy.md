# q-map AI Governance Policy

This policy defines mandatory controls for production AI behavior in `examples/q-map`.

## Scope

- Runtime assistant behavior (`q-assistant`, frontend tool orchestration).
- Prompt/guardrail changes.
- Tooling and model-provider routing used for environmental, social, and jurisdictional analysis.

## Controls (mandatory)

- `GOV-001` no_evidence_no_claim: assistant final claims must be supported by successful tool evidence in the same turn context.
- `GOV-002` traceability_required: each AI response chain must expose request trace id + tool execution trail.
- `GOV-003` policy_blocking: forbidden tool/category calls must be blocked fail-closed.
- `GOV-004` human_escalation: high-impact or unresolved analytical outputs require explicit limitation/escalation path.
- `GOV-005` immutable_audit_log: request/response/tool outcomes must be audit-logged with retention policy.
- `GOV-006` change_control_for_models_and_prompts: provider/model/prompt guardrail changes require tests + changelog entry.

## Merge And Release Rules

- `make -C examples/q-map ai-governance-audit` MUST pass.
- `make -C examples/q-map quality-gate` MUST pass.
- Every technical change under `examples/q-map/*` MUST update `examples/q-map/CHANGELOG.md`.

## Exceptions

- Exceptions are temporary and require:
  - owner,
  - expiry date,
  - compensating controls,
  - dedicated follow-up issue.
