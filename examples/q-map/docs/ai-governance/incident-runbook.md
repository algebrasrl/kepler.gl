# q-map AI Incident Runbook

## Trigger

- False analytical claim.
- Policy bypass / forbidden tool usage.
- Missing traceability or broken audit chain.
- Repeated unresolved high-impact workflow behavior.

## Severity

- `SEV-1`: policy breach or unsupported critical claim with user impact.
- `SEV-2`: major degradation without confirmed harmful output.
- `SEV-3`: minor governance signal drift.

## Immediate Containment

1. Activate containment for the affected path (disable route/feature flag as needed).
2. Apply `kill switch` for model/provider/tool category when required.
3. Preserve request ids, tool traces, and audit payloads.

## Recovery

1. Patch runtime guardrail or tool policy.
2. Add or update deterministic test coverage.
3. Re-run `make -C examples/q-map quality-gate`.
4. Verify governance controls with `make -C examples/q-map ai-governance-audit`.

## Postmortem

1. Write postmortem with root cause, blast radius, and timeline.
2. Add at least one `regression case` to `tests/ai-eval/cases.functional.json` when behavior is reproducible.
3. Update `CHANGELOG.md` and control/risk docs if control semantics changed.
