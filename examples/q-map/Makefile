SHELL := /bin/bash

YARN ?= yarn
NODE ?= node
COMPOSE ?= docker compose
DOCKER ?= docker
PLAYWRIGHT_GREP ?= q-map tools functional coverage|q-map base tools smoke coverage|cloud tool coverage|assistant response exposes structured diagnostics envelope
VITE_MAPBOX_TOKEN ?=
VITE_QMAP_AI_PROXY_BASE ?= /api/q-assistant
VITE_QCUMBER_CLOUD_API_BASE ?= /api/q-cumber
VITE_QCUMBER_CLOUD_DISPLAY_NAME ?= Q-cumber
VITE_QCUMBER_CLOUD_MANAGEMENT_URL ?= /api/q-cumber/maps
VITE_QSTORAGE_CLOUD_API_BASE ?= /api/q-storage
VITE_QSTORAGE_CLOUD_DISPLAY_NAME ?= Le mie mappe
VITE_QSTORAGE_CLOUD_MANAGEMENT_URL ?= /api/q-storage/maps
VITE_QCUMBER_TILESET_BASE ?= /api/q-cumber
EVAL_BASE_URL ?= http://localhost:8000/api/q-assistant
EVAL_DIRECT_BASE_URL ?= http://localhost:3004
EVAL_TRANSPORT ?= edge
EVAL_MODEL ?= google/gemini-3-flash-preview
E2E_BASE_URL ?= http://127.0.0.1:8081
PW_SKIP_WEBSERVER ?= 1
DEV_LOCAL_DOMAIN ?= localhost
QMAP_ASSISTANT_AUDIT_REQUIRE_SERVER ?= $(if $(CI),true,false)
QMAP_ASSISTANT_AUDIT_EXPORT_DIR ?= ../test-results/assistant-live/chat-audit
QMAP_ASSISTANT_AUDIT_DIRS ?= test-results/assistant-live/chat-audit
RUN_ID ?=
EVAL_MIN_PASS_RATE ?=
EVAL_MIN_AVG_CASE_SCORE ?=
EVAL_MIN_P25_CASE_SCORE ?=
EVAL_MIN_MIN_CASE_SCORE ?=
EVAL_MIN_AREA_PASS_RATE ?=
EVAL_MIN_AREA_AVG_CASE_SCORE ?=
EVAL_MIN_AREA_P25_CASE_SCORE ?=
EVAL_MIN_AREA_MIN_CASE_SCORE ?=
EVAL_PREFLIGHT_TIMEOUT_MS ?= 5000
EVAL_PREFLIGHT_TIMEOUT_SEC ?= 5
EVAL_PREFLIGHT_RETRIES ?= 6
EVAL_PREFLIGHT_RETRY_DELAY_MS ?= 1500
BACKEND_READY_TIMEOUT_SEC ?= 180
BACKEND_READY_INTERVAL_MS ?= 2000
EVAL_RUN_TYPE ?= baseline
EVAL_VARIANCE_RUN_TYPE ?= baseline
KPI_SUMMARY_RUN_TYPE ?= baseline
QMAP_AI_EVAL_AREA_VARIANCE_STRICT ?= $(if $(CI),true,false)
CLEAN_LOOP_PURGE_EVAL_RESULTS ?= 0
QMAP_BACKENDS_ENV_FILE ?= backends/.env
COMPOSE_SAME_ORIGIN ?= $(COMPOSE) $(if $(wildcard $(QMAP_BACKENDS_ENV_FILE)),--env-file $(QMAP_BACKENDS_ENV_FILE),) $(if $(wildcard $(QMAP_SAME_ORIGIN_ENV_FILE)),--env-file $(QMAP_SAME_ORIGIN_ENV_FILE),) -f backends/docker-compose.yaml -f backends/docker-compose.edge-only.yaml -f backends/docker-compose.kong.yaml -f docker-compose.same-origin.yaml
QMAP_SAME_ORIGIN_ENV_FILE ?= $(firstword $(wildcard .env.development.local .env.development))
QMAP_SAME_ORIGIN_AI_PROXY_BASE ?= /api/q-assistant
QMAP_SAME_ORIGIN_QCUMBER_API_BASE ?= /api/q-cumber
QMAP_SAME_ORIGIN_QCUMBER_MANAGEMENT_URL ?= /api/q-cumber/maps
QMAP_SAME_ORIGIN_QSTORAGE_API_BASE ?= /api/q-storage
QMAP_SAME_ORIGIN_QSTORAGE_MANAGEMENT_URL ?= /api/q-storage/maps
QMAP_SAME_ORIGIN_QCUMBER_TILESET_BASE ?= /api/q-cumber
IMAGE_REGISTRY ?= quay.io
IMAGE_NAMESPACE ?= algebrasrl
IMAGE_TAG ?= latest
QMAP_IMAGE_UI ?= $(IMAGE_REGISTRY)/$(IMAGE_NAMESPACE)/q-map--ui:$(IMAGE_TAG)
QMAP_UI_BUILD_CONTEXT ?= ../..
QMAP_UI_DOCKERFILE ?= Dockerfile.ui

RUN_ID_SAMPLE_ARG := $(if $(strip $(RUN_ID)),--run-id $(RUN_ID)-sample,)
RUN_ID_FUNCTIONAL_ARG := $(if $(strip $(RUN_ID)),--run-id $(RUN_ID)-functional,)
RUN_ID_ADVERSARIAL_ARG := $(if $(strip $(RUN_ID)),--run-id $(RUN_ID)-adversarial,)
EVAL_RUN_TYPE_ARG := $(if $(strip $(EVAL_RUN_TYPE)),--run-type $(EVAL_RUN_TYPE),)
EVAL_GATE_ARGS := \
	$(if $(strip $(EVAL_MIN_PASS_RATE)),--min-pass-rate $(EVAL_MIN_PASS_RATE),) \
	$(if $(strip $(EVAL_MIN_AVG_CASE_SCORE)),--min-avg-case-score $(EVAL_MIN_AVG_CASE_SCORE),) \
	$(if $(strip $(EVAL_MIN_P25_CASE_SCORE)),--min-p25-case-score $(EVAL_MIN_P25_CASE_SCORE),) \
	$(if $(strip $(EVAL_MIN_MIN_CASE_SCORE)),--min-min-case-score $(EVAL_MIN_MIN_CASE_SCORE),) \
	$(if $(strip $(EVAL_MIN_AREA_PASS_RATE)),--min-area-pass-rate $(EVAL_MIN_AREA_PASS_RATE),) \
	$(if $(strip $(EVAL_MIN_AREA_AVG_CASE_SCORE)),--min-area-avg-case-score $(EVAL_MIN_AREA_AVG_CASE_SCORE),) \
	$(if $(strip $(EVAL_MIN_AREA_P25_CASE_SCORE)),--min-area-p25-case-score $(EVAL_MIN_AREA_P25_CASE_SCORE),) \
	$(if $(strip $(EVAL_MIN_AREA_MIN_CASE_SCORE)),--min-area-min-case-score $(EVAL_MIN_AREA_MIN_CASE_SCORE),)
EVAL_ACTIVE_BASE_URL := $(if $(filter direct,$(strip $(EVAL_TRANSPORT))),$(EVAL_DIRECT_BASE_URL),$(EVAL_BASE_URL))
EVAL_ACTIVE_ARGS := --base-url $(EVAL_ACTIVE_BASE_URL) --model $(EVAL_MODEL) $(EVAL_GATE_ARGS)
EVAL_ACTIVE_ARGS += --preflight-timeout-ms $(EVAL_PREFLIGHT_TIMEOUT_MS) --preflight-retries $(EVAL_PREFLIGHT_RETRIES) --preflight-retry-delay-ms $(EVAL_PREFLIGHT_RETRY_DELAY_MS)
EVAL_PREFLIGHT_HINT_TARGET := $(if $(filter direct,$(strip $(EVAL_TRANSPORT))),ai-eval-preflight-direct,ai-eval-preflight)

.PHONY: help registry-login ui-image-refs ui-image-build ui-image-push dev dev-local dev-local-prepare dev-local-stop same-origin-up same-origin-down same-origin-ps same-origin-logs backend-ready fix-test-perms clean-loop clean-loop-hard changelog-audit ai-governance-audit ai-eval-schema-audit ai-threshold-audit ai-variance-audit ai-area-variance-audit ai-trace-grade-audit ai-operational-audit ai-cost-audit ai-response-quality-audit ai-trace-quality-audit ai-passk-audit ai-eval-preflight ai-eval-preflight-direct tool-contract-audit tool-contract-sync tool-arg-audit prompt-lint typecheck test-workers test-eval-runner test-e2e-tools test-e2e-assistant-live audit-assistant-live audit-assistant-live-strict assistant-live-gate assistant-live-gate-strict tool-coverage-audit ai-matrix-audit backend-audit test-backends ai-eval ai-eval-direct ai-eval-functional ai-eval-functional-direct ai-eval-case-functional ai-eval-case-functional-direct ai-eval-adversarial ai-eval-adversarial-direct ai-eval-case-adversarial ai-eval-case-adversarial-direct ai-eval-all kpi-weekly-summary quality-gate loop

help:
	@echo "q-map Makefile helpers"
	@echo
	@echo "Core:"
	@echo "  make dev                          # start q-map UI"
	@echo "  make dev-local                    # bootstrap backend+JWT+env and start q-map UI"
	@echo "  make dev-local-prepare            # bootstrap backend+JWT+env only (no frontend process)"
	@echo "  make dev-local-stop               # stop backend docker stack started for dev-local"
	@echo "  make registry-login               # docker login to IMAGE_REGISTRY for manual q-map-ui image work"
	@echo "  make ui-image-refs                # print fully-qualified q-map-ui image tag"
	@echo "  make ui-image-build               # build q-map-ui from full kepler.gl repo context"
	@echo "  make ui-image-push                # push q-map-ui to registry (platform release policy stays in root Makefile)"
	@echo "  make backend-ready                # wait for running backend health on direct ports + Kong admin (start backends with: make dev-local)"
	@echo "  make fix-test-perms               # normalize permissions for test/audit artifact folders"
	@echo "  make clean-loop                   # clean audit/playwright artifacts, preserve ai-eval history by default"
	@echo "  make clean-loop-hard              # clean audit/playwright artifacts and purge ai-eval history"
	@echo "  make changelog-audit              # ensure q-map changes include CHANGELOG update"
	@echo "  make ai-governance-audit          # governance docs/control matrix baseline checks"
	@echo "  make ai-eval-schema-audit         # validate ai-eval cases/matrix JSON schema + linkage"
	@echo "  make ai-threshold-audit           # enforce per-criticality min_case_score floors on functional cases"
	@echo "  make ai-variance-audit            # check KPI stability span on latest functional reports (multi-run)"
	@echo "  make ai-area-variance-audit       # non-blocking per-area KPI drift audit on latest functional reports"
	@echo "  make ai-trace-grade-audit         # grade critical functional traces from latest ai-eval report against chat-audit evidence"
	@echo "  make ai-operational-audit         # enforce latency/transport KPI thresholds on latest functional report"
	@echo "  make ai-cost-audit                # enforce token-budget / cost KPI thresholds on latest functional report"
	@echo "  make ai-response-quality-audit    # enforce false-success / groundedness / escalation thresholds on latest functional report"
	@echo "  make ai-trace-quality-audit       # re-check groundedness / escalation on latest functional chat-audit traces"
	@echo "  make ai-passk-audit               # pass^k reliability audit over latest adversarial report window (skip until enough history)"
	@echo "  make ai-eval-preflight            # validate q-assistant /health reachability via curl and Node fetch"
	@echo "  make ai-eval-preflight-direct     # preflight against direct q-assistant backend on :3004 (no Kong bearer path)"
	@echo "  make tool-contract-audit          # validate shared FE/BE/eval tool args/response contract"
	@echo "  make tool-contract-sync           # regenerate shared tool contract from manifest + mutate flags"
	@echo "  make tool-arg-audit               # verify expected_tool_arguments keys exist in tool inputKeys"
	@echo "  make prompt-lint                  # lint ai-eval prompts (static + history stability)"
	@echo "  make typecheck                    # TypeScript check"
	@echo "  make test-workers                 # worker unit tests"
	@echo "  make test-eval-runner             # unit tests for ai-eval runner transport fail-fast"
	@echo "  make test-e2e-tools               # Playwright tools coverage test"
	@echo "  make test-e2e-assistant-live      # Playwright live assistant flow (real chat, no mock)"
	@echo "  make audit-assistant-live         # audit gate on latest live assistant flow requestIds (artifact fallback allowed)"
	@echo "  make audit-assistant-live-strict  # same audit, but requires server chat-audit lines"
	@echo "  make assistant-live-gate          # run live assistant flow + chat-audit quality gate"
	@echo "  make assistant-live-gate-strict   # live flow + strict server chat-audit gate"
	@echo "  make tool-coverage-audit          # runtime tools vs e2e/ai-eval coverage report"
	@echo "  make backend-audit                # backend unittest suite (docker)"
	@echo "  make test-backends                # backend unittest suite (docker)"
	@echo
	@echo "Demo/deployment:"
	@echo "  make same-origin-up               # start q-map UI + Kong/backends behind one same-origin shell on :8080"
	@echo "  make same-origin-down             # stop same-origin shell stack"
	@echo "  make same-origin-ps               # show status for same-origin shell stack"
	@echo "  make same-origin-logs [SERVICE=q-map-shell] # tail logs for same-origin shell stack"
	@echo
	@echo "Prompt-driven loop:"
	@echo "  make ai-eval                      # baseline prompt eval suite"
	@echo "  make ai-eval-direct               # baseline prompt eval suite against direct q-assistant backend on :3004"
	@echo "  make ai-eval-functional           # functional prompt eval suite"
	@echo "  make ai-eval-functional-direct    # functional prompt eval suite against direct q-assistant backend on :3004"
	@echo "  make ai-eval-case-functional CASE_ID=<id>  # single functional eval case by id"
	@echo "  make ai-eval-case-functional-direct CASE_ID=<id> # single functional eval case against direct backend"
	@echo "  make ai-eval-adversarial          # held-out/adversarial eval suite without deterministic case constraints"
	@echo "  make ai-eval-adversarial-direct   # held-out suite against direct q-assistant backend on :3004"
	@echo "  make ai-eval-case-adversarial CASE_ID=<id> # single held-out/adversarial eval case by id"
	@echo "  make ai-eval-case-adversarial-direct CASE_ID=<id> # single held-out case against direct backend on :3004"
	@echo "  make ai-matrix-audit              # area->KPI->cases matrix coverage audit"
	@echo "  make ai-eval-all                  # baseline + functional prompt eval suites"
	@echo "  make kpi-weekly-summary           # append compact KPI delta row from latest functional report"
	@echo "  make quality-gate                 # functional ai-eval + variance + typecheck + workers + e2e tools"
	@echo "  make loop                         # ai-eval sample + quality-gate + KPI weekly summary"
	@echo
	@echo "Optional vars:"
	@echo "  DEV_LOCAL_DOMAIN=<host>           # domain for local gateway endpoints (default localhost)"
	@echo "  QMAP_BACKENDS_ENV_FILE=<path>     # backend env file for same-origin stack (default backends/.env)"
	@echo "  QMAP_SAME_ORIGIN_ENV_FILE=<path>  # frontend env file for same-origin stack (default .env.development.local)"
	@echo "  IMAGE_REGISTRY=<registry>         # q-map-ui registry host (default quay.io)"
	@echo "  IMAGE_NAMESPACE=<namespace>       # q-map-ui registry namespace (default algebrasrl)"
	@echo "  IMAGE_TAG=<tag>                   # q-map-ui image tag (default latest)"
	@echo "  RUN_ID=<name>                     # attach run id to ai-eval reports"
	@echo "  EVAL_BASE_URL=<url>               # q-assistant eval base url (default localhost:8000/api/q-assistant)"
	@echo "  EVAL_DIRECT_BASE_URL=<url>        # direct q-assistant eval base url for local no-gateway runs (default localhost:3004)"
	@echo "  EVAL_TRANSPORT=edge|direct        # select eval transport for ai-eval/quality-gate/loop (default edge)"
	@echo "  QMAP_AI_EVAL_BEARER_TOKEN=<jwt>   # bearer token for ai-eval/preflight on default Kong gateway path"
	@echo "  EVAL_MODEL=<model>                # eval model override"
	@echo "  E2E_BASE_URL=<url>                # Playwright base URL/port for local webServer (default 127.0.0.1:8081)"
	@echo "  PW_SKIP_WEBSERVER=0|1             # 1=do not auto-start vite webServer in assistant suite (default 1)"
	@echo "  QMAP_ASSISTANT_AUDIT_REQUIRE_SERVER=true|false # strict server chat-audit requirement (default true in CI)"
	@echo "  QMAP_ASSISTANT_AUDIT_EXPORT_DIR=<path> # host path where backend export-audit writes session-*.jsonl"
	@echo "  QMAP_ASSISTANT_AUDIT_DIRS=<csv paths> # host dirs/files scanned by ai:audit:assistant-live"
	@echo "  EVAL_MIN_PASS_RATE=<num>          # override run gate min passRate"
	@echo "  EVAL_MIN_AVG_CASE_SCORE=<num>     # override run gate min avgCaseScore"
	@echo "  EVAL_MIN_P25_CASE_SCORE=<num>     # override run gate min p25CaseScore"
	@echo "  EVAL_MIN_MIN_CASE_SCORE=<num>     # override run gate min minCaseScore"
	@echo "  EVAL_MIN_AREA_PASS_RATE=<num>     # override per-area gate min passRate"
	@echo "  EVAL_MIN_AREA_AVG_CASE_SCORE=<num> # override per-area gate min avgCaseScore"
	@echo "  EVAL_MIN_AREA_P25_CASE_SCORE=<num> # override per-area gate min p25CaseScore"
	@echo "  EVAL_MIN_AREA_MIN_CASE_SCORE=<num> # override per-area gate min minCaseScore"
	@echo "  EVAL_PREFLIGHT_TIMEOUT_MS=<ms>    # run-ai-eval /health preflight timeout (default 5000)"
	@echo "  EVAL_PREFLIGHT_RETRIES=<n>        # run-ai-eval /health preflight retries (default 6)"
	@echo "  EVAL_PREFLIGHT_RETRY_DELAY_MS=<ms> # run-ai-eval /health preflight retry delay (default 1500)"
	@echo "  EVAL_PREFLIGHT_TIMEOUT_SEC=<sec>  # ai-eval-preflight timeout (default 5)"
	@echo "  BACKEND_READY_TIMEOUT_SEC=<sec>   # backend-ready max wait for health checks (default 180)"
	@echo "  BACKEND_READY_INTERVAL_MS=<ms>    # backend-ready polling interval (default 2000)"
	@echo "  EVAL_RUN_TYPE=baseline|stabilization|debug|heldout|adversarial # label ai-eval reports by run type (default baseline)"
	@echo "  EVAL_VARIANCE_RUN_TYPE=baseline|stabilization|debug|heldout|adversarial|all # variance audit filter (default baseline)"
	@echo "  KPI_SUMMARY_RUN_TYPE=baseline|stabilization|debug|heldout|adversarial|all # KPI weekly summary filter (default baseline)"
	@echo "  QMAP_AI_EVAL_AREA_VARIANCE_STRICT=true|false # fail quality-gate on area variance violations (default true in CI)"
	@echo "  QMAP_AI_EVAL_MAX_FALSE_SUCCESS_CLAIM_RATE=<num> # max allowed false-success case rate in response-quality audit (default 0)"
	@echo "  QMAP_AI_EVAL_MIN_GROUNDED_FINAL_ANSWER_RATE=<num> # min grounded final-answer pass rate on annotated functional cases (default 1)"
	@echo "  QMAP_AI_EVAL_MIN_ESCALATION_COMPLIANCE_RATE=<num> # min clarification/limitation compliance rate on annotated functional cases (default 1)"
	@echo "  QMAP_AI_EVAL_MIN_GROUNDED_ANSWER_EVALUATED_CASES=<n> # min groundedness-annotated cases required in latest functional report (default 1)"
	@echo "  QMAP_AI_EVAL_MIN_ESCALATION_EVALUATED_CASES=<n> # min escalation-annotated cases required in latest functional report (default 1)"
	@echo "  CLEAN_LOOP_PURGE_EVAL_RESULTS=0|1 # 1=delete tests/ai-eval/results during clean-loop (default 0)"
	@echo "  PLAYWRIGHT_GREP=<pattern>         # e2e tools grep filter"

registry-login:
	$(DOCKER) login $(IMAGE_REGISTRY)

ui-image-refs:
	@echo "QMAP_IMAGE_UI=$(QMAP_IMAGE_UI)"
	@echo "QMAP_UI_BUILD_CONTEXT=$(QMAP_UI_BUILD_CONTEXT)"
	@echo "QMAP_UI_DOCKERFILE=$(QMAP_UI_DOCKERFILE)"

ui-image-build:
	$(DOCKER) build \
		-t $(QMAP_IMAGE_UI) \
		-f $(QMAP_UI_DOCKERFILE) \
		--build-arg VITE_MAPBOX_TOKEN="$(VITE_MAPBOX_TOKEN)" \
		--build-arg VITE_QMAP_AI_PROXY_BASE="$(VITE_QMAP_AI_PROXY_BASE)" \
		--build-arg VITE_QCUMBER_CLOUD_API_BASE="$(VITE_QCUMBER_CLOUD_API_BASE)" \
		--build-arg VITE_QCUMBER_CLOUD_DISPLAY_NAME="$(VITE_QCUMBER_CLOUD_DISPLAY_NAME)" \
		--build-arg VITE_QCUMBER_CLOUD_MANAGEMENT_URL="$(VITE_QCUMBER_CLOUD_MANAGEMENT_URL)" \
		--build-arg VITE_QSTORAGE_CLOUD_API_BASE="$(VITE_QSTORAGE_CLOUD_API_BASE)" \
		--build-arg VITE_QSTORAGE_CLOUD_DISPLAY_NAME="$(VITE_QSTORAGE_CLOUD_DISPLAY_NAME)" \
		--build-arg VITE_QSTORAGE_CLOUD_MANAGEMENT_URL="$(VITE_QSTORAGE_CLOUD_MANAGEMENT_URL)" \
		--build-arg VITE_QCUMBER_TILESET_BASE="$(VITE_QCUMBER_TILESET_BASE)" \
		$(QMAP_UI_BUILD_CONTEXT)

ui-image-push: ui-image-build
	$(DOCKER) push $(QMAP_IMAGE_UI)

dev:
	$(YARN) dev

dev-local:
	$(NODE) scripts/dev-local.mjs --domain $(DEV_LOCAL_DOMAIN)

dev-local-prepare:
	$(NODE) scripts/dev-local.mjs --domain $(DEV_LOCAL_DOMAIN) --no-frontend

dev-local-stop:
	@$(MAKE) -C backends down

same-origin-up:
	@$(MAKE) -C backends doctor render-kong-config
	VITE_QMAP_AI_PROXY_BASE="$(QMAP_SAME_ORIGIN_AI_PROXY_BASE)" \
	VITE_QCUMBER_CLOUD_API_BASE="$(QMAP_SAME_ORIGIN_QCUMBER_API_BASE)" \
	VITE_QCUMBER_CLOUD_MANAGEMENT_URL="$(QMAP_SAME_ORIGIN_QCUMBER_MANAGEMENT_URL)" \
	VITE_QSTORAGE_CLOUD_API_BASE="$(QMAP_SAME_ORIGIN_QSTORAGE_API_BASE)" \
	VITE_QSTORAGE_CLOUD_MANAGEMENT_URL="$(QMAP_SAME_ORIGIN_QSTORAGE_MANAGEMENT_URL)" \
	VITE_QCUMBER_TILESET_BASE="$(QMAP_SAME_ORIGIN_QCUMBER_TILESET_BASE)" \
	$(COMPOSE_SAME_ORIGIN) --profile gateway up -d --build q-cumber-backend q-storage-backend q-assistant kong q-map-ui q-map-shell

same-origin-down:
	$(COMPOSE_SAME_ORIGIN) --profile gateway down

same-origin-ps:
	$(COMPOSE_SAME_ORIGIN) --profile gateway ps

same-origin-logs:
	$(COMPOSE_SAME_ORIGIN) --profile gateway logs -f $(if $(SERVICE),$(SERVICE),q-map-shell)

backend-ready:
	$(NODE) scripts/backend-ready.mjs --timeout-sec $(BACKEND_READY_TIMEOUT_SEC) --interval-ms $(BACKEND_READY_INTERVAL_MS)

fix-test-perms:
	@mkdir -p tests/ai-eval/results test-results playwright-report backends/logs/q-assistant/chat-audit
	@chmod -R u+rwX,go+rX tests/ai-eval/results test-results playwright-report backends/logs 2>/dev/null || true
	@find tests/ai-eval/results test-results playwright-report backends/logs -type d -exec chmod g+w {} + 2>/dev/null || true
	@$(MAKE) -C backends fix-audit-perms >/dev/null 2>&1 || true
	@echo "[fix-test-perms] OK"

clean-loop:
	@$(MAKE) fix-test-perms >/dev/null 2>&1 || true
	@purge="$$(printf '%s' "$(CLEAN_LOOP_PURGE_EVAL_RESULTS)" | tr '[:upper:]' '[:lower:]')"; \
	if [ "$$purge" = "1" ] || [ "$$purge" = "true" ] || [ "$$purge" = "yes" ] || [ "$$purge" = "on" ]; then \
		rm -f tests/ai-eval/results/report-*.json tests/ai-eval/results/report-*.md; \
		echo "[clean-loop] purged ai-eval history"; \
	else \
		echo "[clean-loop] preserved ai-eval history (set CLEAN_LOOP_PURGE_EVAL_RESULTS=1 to purge)"; \
	fi
	rm -f test-results/.last-run.json
	rm -rf test-results/* playwright-report/*
	rm -f backends/logs/session-*.jsonl backends/logs/session-default.jsonl
	@if [ -d backends/logs/q-assistant/chat-audit ]; then \
		find backends/logs/q-assistant/chat-audit -maxdepth 1 -type f -name '*.jsonl' -writable -delete 2>/dev/null || true; \
	fi
	@rm -f backends/logs/q-assistant/chat-audit.jsonl 2>/dev/null || true
	@rm -rf backends/logs/q-assistant/chat-audit.jsonl 2>/dev/null || true
	@if command -v docker >/dev/null 2>&1; then \
		$(MAKE) -C backends clean-audit >/dev/null 2>&1 || true; \
	fi

clean-loop-hard:
	@$(MAKE) clean-loop CLEAN_LOOP_PURGE_EVAL_RESULTS=1

changelog-audit:
	@changed="$$( { git diff --name-only HEAD --; git ls-files --others --exclude-standard --; } | sed '/^$$/d' | sort -u )"; \
	transient_re='^(examples/q-map/tests/ai-eval/results/|examples/q-map/test-results/|examples/q-map/playwright-report/|examples/q-map/backends/logs/|examples/q-map/\.env\.development$$|examples/q-map/\.env\.development\.local$$|examples/q-map/\.env\.local$$)'; \
	qmap_changed="$$(printf '%s\n' "$$changed" | grep '^examples/q-map/' | grep -Ev "$$transient_re" || true)"; \
	non_changelog="$$(printf '%s\n' "$$qmap_changed" | grep -v '^examples/q-map/CHANGELOG.md$$' || true)"; \
	has_changelog="$$(printf '%s\n' "$$qmap_changed" | grep -c '^examples/q-map/CHANGELOG.md$$' || true)"; \
	if [[ -n "$$non_changelog" && "$$has_changelog" -eq 0 ]]; then \
		echo "[changelog-audit] FAIL: q-map changes detected without CHANGELOG update."; \
		printf '%s\n' "$$non_changelog" | sed 's/^/[changelog-audit] changed: /'; \
		echo "[changelog-audit] required: examples/q-map/CHANGELOG.md"; \
		exit 2; \
	fi; \
	count="$$(printf '%s\n' "$$qmap_changed" | sed '/^$$/d' | wc -l | tr -d ' ')"; \
	echo "[changelog-audit] OK: qmapChanges=$$count changelogUpdated=$$has_changelog"

ai-governance-audit:
	$(NODE) scripts/audit-ai-governance.mjs

ai-eval-schema-audit:
	$(NODE) scripts/validate-ai-eval-schema.mjs

ai-threshold-audit:
	$(NODE) scripts/audit-ai-eval-thresholds.mjs

ai-variance-audit:
	$(NODE) scripts/audit-ai-eval-variance.mjs --results-dir tests/ai-eval/results --window 3 --run-type $(EVAL_VARIANCE_RUN_TYPE)

ai-area-variance-audit:
	@$(NODE) scripts/audit-ai-eval-area-variance.mjs --results-dir tests/ai-eval/results --matrix tests/ai-eval/architecture-matrix.json --window 3 --run-type $(EVAL_VARIANCE_RUN_TYPE); \
	status=$$?; \
	strict="$$(printf '%s' "$(QMAP_AI_EVAL_AREA_VARIANCE_STRICT)" | tr '[:upper:]' '[:lower:]')"; \
	if [ $$status -ne 0 ]; then \
		if [ "$$strict" = "1" ] || [ "$$strict" = "true" ] || [ "$$strict" = "yes" ] || [ "$$strict" = "on" ]; then \
			echo "[ai-area-variance-audit] FAIL: strict mode enabled (status=$$status)."; \
			exit $$status; \
		fi; \
		echo "[ai-area-variance-audit] WARN: non-blocking failure (status=$$status)."; \
	fi; \
	exit 0

ai-trace-grade-audit:
	$(NODE) scripts/audit-ai-trace-grades.mjs --results-dir tests/ai-eval/results

ai-operational-audit:
	$(NODE) scripts/audit-ai-operational-kpis.mjs --results-dir tests/ai-eval/results

ai-cost-audit:
	$(NODE) scripts/audit-ai-cost-kpis.mjs --results-dir tests/ai-eval/results

ai-response-quality-audit:
	$(NODE) scripts/audit-ai-response-quality.mjs --results-dir tests/ai-eval/results

ai-trace-quality-audit:
	$(NODE) scripts/audit-ai-trace-quality.mjs --results-dir tests/ai-eval/results

ai-passk-audit:
	$(NODE) scripts/audit-ai-passk-reliability.mjs --results-dir tests/ai-eval/results

ai-eval-preflight:
	$(NODE) scripts/preflight-ai-eval-runtime.mjs --base-url $(EVAL_BASE_URL) --timeout-sec $(EVAL_PREFLIGHT_TIMEOUT_SEC) --retries $(EVAL_PREFLIGHT_RETRIES) --retry-delay-ms $(EVAL_PREFLIGHT_RETRY_DELAY_MS)

ai-eval-preflight-direct:
	$(NODE) scripts/preflight-ai-eval-runtime.mjs --base-url $(EVAL_DIRECT_BASE_URL) --timeout-sec $(EVAL_PREFLIGHT_TIMEOUT_SEC) --retries $(EVAL_PREFLIGHT_RETRIES) --retry-delay-ms $(EVAL_PREFLIGHT_RETRY_DELAY_MS)

tool-contract-audit:
	$(NODE) scripts/audit-tool-contracts.mjs

tool-contract-sync:
	$(NODE) scripts/generate-tool-contracts.mjs
	$(NODE) scripts/audit-tool-contracts.mjs

tool-arg-audit:
	$(NODE) scripts/audit-tool-inputkeys.mjs

prompt-lint:
	$(NODE) scripts/lint-ai-prompts.mjs --max-reports 30 --min-history-runs 3 --min-keyword-hit-rate 0.7

typecheck:
	$(YARN) run tsc --noEmit

test-workers:
	$(YARN) test:unit:workers

test-eval-runner:
	bash scripts/test-ai-eval-runner.sh

test-e2e-tools:
	PW_BASE_URL=$(E2E_BASE_URL) $(YARN) test:e2e tests/e2e/tools.spec.ts --grep "$(PLAYWRIGHT_GREP)"

test-e2e-assistant-live:
	PW_BASE_URL=$(E2E_BASE_URL) PW_SKIP_WEBSERVER=$(PW_SKIP_WEBSERVER) $(YARN) test:e2e:assistant --grep "assistant live:"

audit-assistant-live:
	@$(MAKE) fix-test-perms >/dev/null 2>&1 || true
	@$(MAKE) -C backends export-audit AUDIT_EXPORT_DIR=$(QMAP_ASSISTANT_AUDIT_EXPORT_DIR) >/dev/null 2>&1 || true
	QMAP_ASSISTANT_AUDIT_DIRS=$(QMAP_ASSISTANT_AUDIT_DIRS) QMAP_ASSISTANT_AUDIT_REQUIRE_SERVER=$(QMAP_ASSISTANT_AUDIT_REQUIRE_SERVER) $(YARN) ai:audit:assistant-live

audit-assistant-live-strict:
	@$(MAKE) fix-test-perms >/dev/null 2>&1 || true
	@$(MAKE) -C backends export-audit AUDIT_EXPORT_DIR=$(QMAP_ASSISTANT_AUDIT_EXPORT_DIR) >/dev/null 2>&1 || true
	QMAP_ASSISTANT_AUDIT_DIRS=$(QMAP_ASSISTANT_AUDIT_DIRS) QMAP_ASSISTANT_AUDIT_REQUIRE_SERVER=true $(YARN) ai:audit:assistant-live

assistant-live-gate: test-e2e-assistant-live audit-assistant-live

assistant-live-gate-strict: test-e2e-assistant-live audit-assistant-live-strict

tool-coverage-audit:
	$(NODE) scripts/audit-tool-coverage.mjs --require-e2e-full

test-backends:
	$(MAKE) -C backends test-backends

backend-audit: test-backends

ai-eval:
	@tmp="$$(mktemp)"; \
	set -o pipefail; \
	if $(NODE) scripts/run-ai-eval.mjs --cases tests/ai-eval/cases.sample.json $(EVAL_ACTIVE_ARGS) $(EVAL_RUN_TYPE_ARG) $(RUN_ID_SAMPLE_ARG) 2>&1 | tee "$$tmp"; then \
		rm -f "$$tmp"; \
	else \
		status=$${PIPESTATUS[0]}; \
		if grep -Eiq 'preflight /health failed: fetch failed|connect eperm|operation not permitted' "$$tmp"; then \
			echo "[ai-eval][hint] Node localhost connectivity appears blocked (likely sandbox EPERM)."; \
			echo "[ai-eval][hint] Run with elevated/out-of-sandbox permissions or verify with: make $(EVAL_PREFLIGHT_HINT_TARGET)"; \
		fi; \
		if [ "$(strip $(EVAL_TRANSPORT))" != "direct" ] && grep -Eiq '401 unauthorized|requires bearer auth' "$$tmp"; then \
			echo "[ai-eval][hint] Local Kong path requires bearer auth; for direct backend eval use: make ai-eval-direct"; \
		fi; \
		rm -f "$$tmp"; \
		exit $$status; \
	fi

ai-eval-direct:
	@$(MAKE) ai-eval EVAL_TRANSPORT=direct RUN_ID="$(RUN_ID)" EVAL_RUN_TYPE="$(EVAL_RUN_TYPE)" \
		EVAL_MIN_PASS_RATE="$(EVAL_MIN_PASS_RATE)" EVAL_MIN_AVG_CASE_SCORE="$(EVAL_MIN_AVG_CASE_SCORE)" \
		EVAL_MIN_P25_CASE_SCORE="$(EVAL_MIN_P25_CASE_SCORE)" EVAL_MIN_MIN_CASE_SCORE="$(EVAL_MIN_MIN_CASE_SCORE)" \
		EVAL_MIN_AREA_PASS_RATE="$(EVAL_MIN_AREA_PASS_RATE)" EVAL_MIN_AREA_AVG_CASE_SCORE="$(EVAL_MIN_AREA_AVG_CASE_SCORE)" \
		EVAL_MIN_AREA_P25_CASE_SCORE="$(EVAL_MIN_AREA_P25_CASE_SCORE)" EVAL_MIN_AREA_MIN_CASE_SCORE="$(EVAL_MIN_AREA_MIN_CASE_SCORE)"

ai-eval-functional:
	@tmp="$$(mktemp)"; \
	set -o pipefail; \
	if $(NODE) scripts/run-ai-eval.mjs --cases tests/ai-eval/cases.functional.json $(EVAL_ACTIVE_ARGS) $(EVAL_RUN_TYPE_ARG) $(RUN_ID_FUNCTIONAL_ARG) 2>&1 | tee "$$tmp"; then \
		rm -f "$$tmp"; \
	else \
		status=$${PIPESTATUS[0]}; \
		if grep -Eiq 'preflight /health failed: fetch failed|connect eperm|operation not permitted' "$$tmp"; then \
			echo "[ai-eval-functional][hint] Node localhost connectivity appears blocked (likely sandbox EPERM)."; \
			echo "[ai-eval-functional][hint] Run with elevated/out-of-sandbox permissions or verify with: make $(EVAL_PREFLIGHT_HINT_TARGET)"; \
		fi; \
		if [ "$(strip $(EVAL_TRANSPORT))" != "direct" ] && grep -Eiq '401 unauthorized|requires bearer auth' "$$tmp"; then \
			echo "[ai-eval-functional][hint] Local Kong path requires bearer auth; for direct backend eval use: make ai-eval-functional-direct"; \
		fi; \
		rm -f "$$tmp"; \
		exit $$status; \
	fi

ai-eval-functional-direct:
	@$(MAKE) ai-eval-functional EVAL_TRANSPORT=direct RUN_ID="$(RUN_ID)" EVAL_RUN_TYPE="$(EVAL_RUN_TYPE)" \
		EVAL_MIN_PASS_RATE="$(EVAL_MIN_PASS_RATE)" EVAL_MIN_AVG_CASE_SCORE="$(EVAL_MIN_AVG_CASE_SCORE)" \
		EVAL_MIN_P25_CASE_SCORE="$(EVAL_MIN_P25_CASE_SCORE)" EVAL_MIN_MIN_CASE_SCORE="$(EVAL_MIN_MIN_CASE_SCORE)" \
		EVAL_MIN_AREA_PASS_RATE="$(EVAL_MIN_AREA_PASS_RATE)" EVAL_MIN_AREA_AVG_CASE_SCORE="$(EVAL_MIN_AREA_AVG_CASE_SCORE)" \
		EVAL_MIN_AREA_P25_CASE_SCORE="$(EVAL_MIN_AREA_P25_CASE_SCORE)" EVAL_MIN_AREA_MIN_CASE_SCORE="$(EVAL_MIN_AREA_MIN_CASE_SCORE)"

ai-eval-case-functional:
	@if [ -z "$(strip $(CASE_ID))" ]; then \
		echo "[ai-eval-case-functional] CASE_ID is required."; \
		echo "[ai-eval-case-functional] Example: make ai-eval-case-functional CASE_ID=arch_existing_join_metric_reuse_coloring"; \
		exit 2; \
	fi
	@case_file="$$(mktemp)"; \
	log_file="$$(mktemp)"; \
	jq '[.[] | select(.id == "$(strip $(CASE_ID))")]' tests/ai-eval/cases.functional.json > "$$case_file"; \
	case_count="$$(jq 'length' "$$case_file")"; \
	if [ "$$case_count" -ne 1 ]; then \
		echo "[ai-eval-case-functional] CASE_ID \"$(strip $(CASE_ID))\" not found in tests/ai-eval/cases.functional.json"; \
		rm -f "$$case_file" "$$log_file"; \
		exit 2; \
	fi; \
	set -o pipefail; \
	if $(NODE) scripts/run-ai-eval.mjs --cases "$$case_file" $(EVAL_ACTIVE_ARGS) $(EVAL_RUN_TYPE_ARG) $(RUN_ID_FUNCTIONAL_ARG) 2>&1 | tee "$$log_file"; then \
		rm -f "$$case_file" "$$log_file"; \
	else \
		status=$${PIPESTATUS[0]}; \
		if grep -Eiq 'preflight /health failed: fetch failed|connect eperm|operation not permitted' "$$log_file"; then \
			echo "[ai-eval-case-functional][hint] Node localhost connectivity appears blocked (likely sandbox EPERM)."; \
			echo "[ai-eval-case-functional][hint] Run with elevated/out-of-sandbox permissions or verify with: make $(EVAL_PREFLIGHT_HINT_TARGET)"; \
		fi; \
		if [ "$(strip $(EVAL_TRANSPORT))" != "direct" ] && grep -Eiq '401 unauthorized|requires bearer auth' "$$log_file"; then \
			echo "[ai-eval-case-functional][hint] Local Kong path requires bearer auth; for direct backend eval use: make ai-eval-case-functional-direct CASE_ID=$(strip $(CASE_ID))"; \
		fi; \
		rm -f "$$case_file" "$$log_file"; \
		exit $$status; \
	fi

ai-eval-case-functional-direct:
	@$(MAKE) ai-eval-case-functional EVAL_TRANSPORT=direct CASE_ID="$(CASE_ID)" RUN_ID="$(RUN_ID)" EVAL_RUN_TYPE="$(EVAL_RUN_TYPE)" \
		EVAL_MIN_PASS_RATE="$(EVAL_MIN_PASS_RATE)" EVAL_MIN_AVG_CASE_SCORE="$(EVAL_MIN_AVG_CASE_SCORE)" \
		EVAL_MIN_P25_CASE_SCORE="$(EVAL_MIN_P25_CASE_SCORE)" EVAL_MIN_MIN_CASE_SCORE="$(EVAL_MIN_MIN_CASE_SCORE)" \
		EVAL_MIN_AREA_PASS_RATE="$(EVAL_MIN_AREA_PASS_RATE)" EVAL_MIN_AREA_AVG_CASE_SCORE="$(EVAL_MIN_AREA_AVG_CASE_SCORE)" \
		EVAL_MIN_AREA_P25_CASE_SCORE="$(EVAL_MIN_AREA_P25_CASE_SCORE)" EVAL_MIN_AREA_MIN_CASE_SCORE="$(EVAL_MIN_AREA_MIN_CASE_SCORE)"

ai-eval-adversarial:
	@tmp="$$(mktemp)"; \
	set -o pipefail; \
	if $(NODE) scripts/run-ai-eval.mjs --cases tests/ai-eval/cases.adversarial.json $(EVAL_ACTIVE_ARGS) --run-type adversarial --disable-deterministic-constraints $(RUN_ID_ADVERSARIAL_ARG) 2>&1 | tee "$$tmp"; then \
		rm -f "$$tmp"; \
	else \
		status=$${PIPESTATUS[0]}; \
		if grep -Eiq 'preflight /health failed: fetch failed|connect eperm|operation not permitted' "$$tmp"; then \
			echo "[ai-eval-adversarial][hint] Node localhost connectivity appears blocked (likely sandbox EPERM)."; \
			echo "[ai-eval-adversarial][hint] Run with elevated/out-of-sandbox permissions or verify with: make $(EVAL_PREFLIGHT_HINT_TARGET)"; \
		fi; \
		if [ "$(strip $(EVAL_TRANSPORT))" != "direct" ] && grep -Eiq '401 unauthorized|requires bearer auth' "$$tmp"; then \
			echo "[ai-eval-adversarial][hint] Local Kong path requires bearer auth; for direct backend eval use: make ai-eval-adversarial-direct"; \
		fi; \
		rm -f "$$tmp"; \
		exit $$status; \
	fi

ai-eval-case-adversarial:
	@if [ -z "$(strip $(CASE_ID))" ]; then \
		echo "[ai-eval-case-adversarial] CASE_ID is required."; \
		echo "[ai-eval-case-adversarial] Example: make ai-eval-case-adversarial CASE_ID=adv_post_create_validation_normalized_no_retry_loop_heldout"; \
		exit 2; \
	fi
	@case_file="$$(mktemp)"; \
	log_file="$$(mktemp)"; \
	jq '[.[] | select(.id == "$(strip $(CASE_ID))")]' tests/ai-eval/cases.adversarial.json > "$$case_file"; \
	case_count="$$(jq 'length' "$$case_file")"; \
	if [ "$$case_count" -ne 1 ]; then \
		echo "[ai-eval-case-adversarial] CASE_ID \"$(strip $(CASE_ID))\" not found in tests/ai-eval/cases.adversarial.json"; \
		rm -f "$$case_file" "$$log_file"; \
		exit 2; \
	fi; \
	set -o pipefail; \
	if $(NODE) scripts/run-ai-eval.mjs --cases "$$case_file" $(EVAL_ACTIVE_ARGS) --run-type adversarial --disable-deterministic-constraints $(RUN_ID_ADVERSARIAL_ARG) 2>&1 | tee "$$log_file"; then \
		rm -f "$$case_file" "$$log_file"; \
	else \
		status=$${PIPESTATUS[0]}; \
		if grep -Eiq 'preflight /health failed: fetch failed|connect eperm|operation not permitted' "$$log_file"; then \
			echo "[ai-eval-case-adversarial][hint] Node localhost connectivity appears blocked (likely sandbox EPERM)."; \
			echo "[ai-eval-case-adversarial][hint] Run with elevated/out-of-sandbox permissions or verify with: make $(EVAL_PREFLIGHT_HINT_TARGET)"; \
		fi; \
		if [ "$(strip $(EVAL_TRANSPORT))" != "direct" ] && grep -Eiq '401 unauthorized|requires bearer auth' "$$log_file"; then \
			echo "[ai-eval-case-adversarial][hint] Local Kong path requires bearer auth; for direct backend eval use: make ai-eval-case-adversarial-direct CASE_ID=$(strip $(CASE_ID))"; \
		fi; \
		rm -f "$$case_file" "$$log_file"; \
		exit $$status; \
	fi

ai-eval-adversarial-direct:
	@$(MAKE) ai-eval-adversarial EVAL_TRANSPORT=direct RUN_ID="$(RUN_ID)" \
		EVAL_MIN_PASS_RATE="$(EVAL_MIN_PASS_RATE)" EVAL_MIN_AVG_CASE_SCORE="$(EVAL_MIN_AVG_CASE_SCORE)" \
		EVAL_MIN_P25_CASE_SCORE="$(EVAL_MIN_P25_CASE_SCORE)" EVAL_MIN_MIN_CASE_SCORE="$(EVAL_MIN_MIN_CASE_SCORE)" \
		EVAL_MIN_AREA_PASS_RATE="$(EVAL_MIN_AREA_PASS_RATE)" EVAL_MIN_AREA_AVG_CASE_SCORE="$(EVAL_MIN_AREA_AVG_CASE_SCORE)" \
		EVAL_MIN_AREA_P25_CASE_SCORE="$(EVAL_MIN_AREA_P25_CASE_SCORE)" EVAL_MIN_AREA_MIN_CASE_SCORE="$(EVAL_MIN_AREA_MIN_CASE_SCORE)"

ai-eval-case-adversarial-direct:
	@$(MAKE) ai-eval-case-adversarial EVAL_TRANSPORT=direct CASE_ID="$(CASE_ID)" RUN_ID="$(RUN_ID)" \
		EVAL_MIN_PASS_RATE="$(EVAL_MIN_PASS_RATE)" EVAL_MIN_AVG_CASE_SCORE="$(EVAL_MIN_AVG_CASE_SCORE)" \
		EVAL_MIN_P25_CASE_SCORE="$(EVAL_MIN_P25_CASE_SCORE)" EVAL_MIN_MIN_CASE_SCORE="$(EVAL_MIN_MIN_CASE_SCORE)" \
		EVAL_MIN_AREA_PASS_RATE="$(EVAL_MIN_AREA_PASS_RATE)" EVAL_MIN_AREA_AVG_CASE_SCORE="$(EVAL_MIN_AREA_AVG_CASE_SCORE)" \
		EVAL_MIN_AREA_P25_CASE_SCORE="$(EVAL_MIN_AREA_P25_CASE_SCORE)" EVAL_MIN_AREA_MIN_CASE_SCORE="$(EVAL_MIN_AREA_MIN_CASE_SCORE)"

ai-matrix-audit:
	$(NODE) scripts/audit-ai-matrix.mjs --require-complete-case-mapping

ai-eval-all: ai-eval-schema-audit ai-matrix-audit ai-eval ai-eval-functional

kpi-weekly-summary:
	$(NODE) scripts/update-kpi-weekly-summary.mjs --results-dir tests/ai-eval/results --out docs/KPI_WEEKLY_SUMMARY.md --run-type $(KPI_SUMMARY_RUN_TYPE)

quality-gate: changelog-audit ai-governance-audit ai-eval-schema-audit ai-threshold-audit ai-eval-functional ai-operational-audit ai-cost-audit ai-response-quality-audit ai-trace-quality-audit ai-trace-grade-audit ai-passk-audit ai-variance-audit ai-area-variance-audit tool-contract-audit tool-arg-audit prompt-lint typecheck test-workers test-eval-runner test-e2e-tools tool-coverage-audit backend-audit

loop: ai-eval-schema-audit ai-matrix-audit ai-eval quality-gate kpi-weekly-summary
