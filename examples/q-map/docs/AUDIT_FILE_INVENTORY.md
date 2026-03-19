# Audit File Inventory

Reference inventory of q-map audit, KPI, and evidence files for local hardening and future pre-release assessment.

When the release target is the full Q-Hive platform, not only the standalone q-map app, this inventory
also tracks the root-platform files referenced from the parent repository (`../..` from `examples/q-map`).

Terminology note: the parent platform currently uses `hive.q-dev.it` as a
secondary public domain for pre-release validation. It is not being treated as a
progressive-traffic canary.

## Canonical Inputs

These files define the audit surface and should be treated as source-of-truth inputs.

| Area | Files | Purpose |
| --- | --- | --- |
| Eval cases | `tests/ai-eval/cases.sample.json`, `tests/ai-eval/cases.functional.json`, `tests/ai-eval/cases.adversarial.json` | Benchmark slices for sample, functional, and held-out/adversarial runs. |
| KPI matrix | `tests/ai-eval/architecture-matrix.json` | Area and KPI mapping used by `ai-matrix-audit`, report grading, and KPI summaries. |
| Governance | `docs/ai-governance/policy.md`, `docs/ai-governance/risk-register.yaml`, `docs/ai-governance/control-matrix.md`, `docs/ai-governance/incident-runbook.md` | Governance baseline enforced by `ai-governance-audit`. |
| Tool contract | `artifacts/tool-contracts/qmap-tool-contracts.json`, `backends/q-assistant/src/q_assistant/qmap-tool-contracts.json`, `src/features/qmap-ai/tool-contract.ts` | Shared FE/BE/eval tool schema and runtime contract surface. |

## Platform Publication Surface

If pre-release publication follows the full root-platform flow (`../../../Makefile` and Swarm/local platform assets),
these parent-repo files are part of the operational baseline and should be versioned/reviewed together with q-map KPIs.

| Area | Parent-repo files | Purpose |
| --- | --- | --- |
| Platform release targets | `../Makefile`, `../README.md`, `../CHANGELOG.md` | Root publication commands (`platform-up`, `platform-image-build`, `platform-image-push`, `platform-smoke`) and secondary-domain validation assumptions. |
| Stack definitions | `../docker-stack.platform.yml`, `../docker-compose.platform.local.yml`, `../docker-compose.production.yml`, `../compose/platform/platform.env.example` | Full-platform deploy/runtime manifests and environment surface. |
| Platform runtime scripts | `../compose/platform/scripts/docker-compose-platform.sh`, `../compose/platform/scripts/platform-smoke.sh` | Local platform orchestration and smoke verification for the integrated stack. |
| Platform ingress | `../compose/platform/nginx/local-platform.conf`, `../compose/production/nginx/default.conf`, `../compose/production/traefik/traefik.yml` | Public routing, reverse proxy, and ingress assumptions that affect q-map embedding and pre-release behavior. |
| q_hive audit trail | `../q_hive/audit/models.py`, `../q_hive/audit/services.py`, `../q_hive/audit/tests/test_models.py`, `../q_hive/organizations/domain/audit_state.py`, `../q_hive/organizations/management/commands/audit_membership_flow.py` | Platform-side audit state and membership/audit evidence generation beyond q-map traces. |

## Generated Evidence

These files and directories are the main evidence stores produced by the loop and by live audits.

| Path | Produced by | Notes |
| --- | --- | --- |
| `tests/ai-eval/results/report-*.json` | `make ai-eval*`, `make ai-eval-functional*`, `make ai-eval-adversarial*`, `make loop` | Canonical machine-readable KPI reports. |
| `tests/ai-eval/results/report-*.md` | Same eval commands | Human-readable report mirrors for the JSON reports. |
| `docs/KPI_WEEKLY_SUMMARY.md` | `make kpi-weekly-summary`, `make loop` | Rolling baseline delta summary derived from the latest compatible functional report; treat as local generated output, not stable versioned documentation. |
| `test-results/` | Playwright, assistant-live audit helpers | E2E artifacts, screenshots, JSON attachments, and live-assistant exports. |
| `playwright-report/` | Playwright | HTML/browser test report output. |
| `backends/logs/q-assistant/chat-audit/session-*.jsonl` | q-assistant runtime | Server-side request/trace evidence used by trace-grade and assistant-live audits. |
| `test-results/assistant-live/chat-audit/` | `make -C backends export-audit`, `make audit-assistant-live*` | Exported chat-audit subset for portable live-flow review. |
| `backends/logs/session-*.jsonl` | q-assistant/runtime logging | Additional host-side session evidence cleaned by `make clean-loop`. |

## Audit Scripts

These scripts are the current audit entrypoints used directly or through the q-map `Makefile`.

| Script | Primary make target(s) | Reads | Typical output |
| --- | --- | --- | --- |
| `scripts/validate-ai-eval-schema.mjs` | `ai-eval-schema-audit` | Eval case JSON + matrix | Console pass/fail for schema/linkage validity. |
| `scripts/audit-ai-matrix.mjs` | `ai-matrix-audit` | Matrix + case catalogs | Matrix coverage summary by area and tool. |
| `scripts/audit-ai-governance.mjs` | `ai-governance-audit` | Governance docs | Governance completeness and freshness check. |
| `scripts/audit-ai-eval-thresholds.mjs` | `ai-threshold-audit` | Functional cases | Static criticality floor enforcement. |
| `scripts/audit-ai-eval-variance.mjs` | `ai-variance-audit` | `tests/ai-eval/results/` | Multi-run baseline drift gate. |
| `scripts/audit-ai-eval-area-variance.mjs` | `ai-area-variance-audit` | `tests/ai-eval/results/`, matrix | Per-area KPI drift check. |
| `scripts/audit-ai-operational-kpis.mjs` | `ai-operational-audit` | Latest functional report JSON | Duration and transport KPI gate. |
| `scripts/audit-ai-cost-kpis.mjs` | `ai-cost-audit` | Latest functional report JSON | Token-budget and cost KPI gate. |
| `scripts/audit-ai-response-quality.mjs` | `ai-response-quality-audit` | Latest functional report JSON | False-success, groundedness, escalation metrics. |
| `scripts/audit-ai-trace-quality.mjs` | `ai-trace-quality-audit` | Functional report + chat-audit-backed metrics | Trace-backed quality verification. |
| `scripts/audit-ai-trace-grades.mjs` | `ai-trace-grade-audit` | Functional report + chat-audit lines | Critical-case workflow grading. |
| `scripts/audit-ai-passk-reliability.mjs` | `ai-passk-audit` | Adversarial report history | Repeated-run pass^k reliability. |
| `scripts/audit-tool-contracts.mjs` | `tool-contract-audit`, `tool-contract-sync` | Contract artifacts | FE/BE/eval contract alignment. |
| `scripts/audit-tool-coverage.mjs` | `tool-coverage-audit` | Runtime tool manifest + tests | Runtime vs e2e/ai-eval tool coverage. |
| `scripts/lint-ai-prompts.mjs` | `prompt-lint` | Case history + reports | Static/history prompt lint and warning surface. |
| `scripts/audit-assistant-live-chat-quality.mjs` | `audit-assistant-live`, `audit-assistant-live-strict` | `test-results/assistant-live/*.json`, chat-audit dirs | Live assistant trace gate over requestIds. |
| `scripts/update-kpi-weekly-summary.mjs` | `kpi-weekly-summary` | Functional reports + matrix | Updates `docs/KPI_WEEKLY_SUMMARY.md`. |

## Gate Composition

High-level targets compose the files above in the following way.

| Target | Main evidence consumed |
| --- | --- |
| `make quality-gate` | changelog, governance docs, eval catalogs, latest functional reports, tool contracts, Playwright results, worker/unit tests, backend tests |
| `make loop` | schema + matrix audits, sample eval, full `quality-gate`, KPI weekly summary |
| `make audit-assistant-live` | Playwright live assistant artifact plus chat-audit evidence |
| `make backend-audit` | backend Python unittest suites inside Docker |

For the full parent platform, the analogous operational checks live at root level:

| Root target | Main evidence consumed |
| --- | --- |
| `make platform-up` | local integrated compose stack for q_hive + q-map services |
| `make platform-smoke` | root platform smoke fixture plus `scripts/qhive-platform-smoke.mjs` from q-map |
| `make platform-image-build` | all custom q_hive and q-map production images |
| `make platform-image-push[-dev|-production]` | publishable image set for pre-release/production registries |

## Pre-release Baseline

For a new secondary-domain validation publication, the minimum artifacts worth preserving per iteration are:

1. One functional report pair in `tests/ai-eval/results/` (`report-<runId>.json` and `.md`).
2. The current `docs/KPI_WEEKLY_SUMMARY.md`.
3. Any `test-results/` evidence from failed or representative Playwright/live-assistant runs.
4. The matching `backends/logs/q-assistant/chat-audit/session-*.jsonl` lines for critical requestIds.
5. The governance baseline under `docs/ai-governance/`.
6. If shipping the full platform, the root publish/deploy inputs: `../Makefile`, `../docker-stack.platform.yml`, `../compose/platform/platform.env.example`, and the latest `make platform-smoke` outcome.

## Operational Notes

- `make clean-loop` preserves `tests/ai-eval/results/` by default; use it for routine cleanup when KPI history matters.
- `make clean-loop-hard` purges eval history and should be reserved for intentional resets.
- `docs/KPI_WEEKLY_SUMMARY.md` is intentionally kept out of normal version control flow; regenerate it locally when needed from the latest functional history.
- Local Playwright and local KPI loops should be bootstrapped from `make dev-local` or `make dev-local-prepare` so the q-map UI on `:8081` has the runtime JWT expected by backend-driven tool flows.
