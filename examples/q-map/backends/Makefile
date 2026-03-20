SHELL := /bin/bash
PYTHON ?= python3

COMPOSE ?= docker compose
COMPOSE_GATEWAY ?= $(COMPOSE) -f docker-compose.yaml -f docker-compose.kong.yaml --profile gateway
DOCKER ?= docker
SERVICES ?= q-cumber-backend q-storage-backend q-assistant
DOCTOR_STRICT ?= 1
AUDIT_EXPORT_DIR ?= ../test-results/assistant-live/chat-audit
IMAGE_REGISTRY ?= quay.io
IMAGE_NAMESPACE ?= algebrasrl
IMAGE_TAG ?= latest
QMAP_IMAGE_QCUMBER_POSTGIS ?= $(IMAGE_REGISTRY)/$(IMAGE_NAMESPACE)/q-map--q-cumber-postgis:$(IMAGE_TAG)
QMAP_IMAGE_QCUMBER_BACKEND ?= $(IMAGE_REGISTRY)/$(IMAGE_NAMESPACE)/q-map--q-cumber-backend:$(IMAGE_TAG)
QMAP_IMAGE_QSTORAGE_BACKEND ?= $(IMAGE_REGISTRY)/$(IMAGE_NAMESPACE)/q-map--q-storage-backend:$(IMAGE_TAG)
QMAP_IMAGE_QASSISTANT ?= $(IMAGE_REGISTRY)/$(IMAGE_NAMESPACE)/q-map--q-assistant:$(IMAGE_TAG)

PROVIDER ?= openrouter
MODEL ?= google/gemini-3-flash-preview
BASE_URL ?=
KEEP_CHAIN ?= 0
BUILD ?= 0

SWITCH_ARGS := $(PROVIDER) $(MODEL)
ifneq ($(strip $(BASE_URL)),)
SWITCH_ARGS += $(BASE_URL)
endif
ifneq ($(strip $(KEEP_CHAIN)),0)
SWITCH_ARGS += --keep-chain
endif
ifneq ($(strip $(BUILD)),0)
SWITCH_ARGS += --build
endif

.PHONY: help init doctor render-kong-config registry-login image-refs image-build image-push up up-build restart up-direct up-direct-build down down-direct ps ps-direct logs logs-direct health fix-audit-perms clean-audit export-audit qa-switch qa-switch-dry \
        qa-switch-openai qa-switch-openrouter qa-switch-openrouter-nemotron qa-switch-openrouter-deepseek qa-switch-openrouter-gemini qa-switch-ollama \
        test-q-cumber test-q-assistant test-q-storage test-backends \
        pgdump-sync clean-postgis

help:
	@echo "q-map backends Makefile"
	@echo
	@echo "Setup:"
	@echo "  make init                            # create .env from .env.example if missing"
	@echo "  make doctor                          # validate host UID/GID vs compose UID/GID preflight"
	@echo "  make render-kong-config              # render kong/kong.yml from env (JWT issuer/secret rotation + aud checks)"
	@echo "  make registry-login [IMAGE_REGISTRY=quay.io] # docker login to target registry"
	@echo "  make image-refs                      # print fully-qualified image tags"
	@echo "  make image-build IMAGE_NAMESPACE=<org/project> IMAGE_TAG=<tag>"
	@echo "                                       # build/tag q-map backend images for a registry"
	@echo "  make image-push IMAGE_NAMESPACE=<org/project> IMAGE_TAG=<tag>"
	@echo "                                       # push q-map backend images after docker login (platform release policy stays in root Makefile)"
	@echo
	@echo "Compose:"
	@echo "  make up                              # start backends + Kong (direct ports + gateway exposed)"
	@echo "  make up-build                        # start backends + Kong with build for Python backends"
	@echo "  make restart                         # restart default stack (Kong + backends)"
	@echo "  make up-direct                       # start backends without Kong gateway (direct ports only)"
	@echo "  make up-direct-build                 # start direct backends with build (direct ports only)"
	@echo "  make down                            # stop default stack (Kong + backends)"
	@echo "  make down-direct                     # stop direct stack without Kong overlay"
	@echo "  make ps                              # show status for default stack (Kong + backends)"
	@echo "  make ps-direct                       # show status for direct stack"
	@echo "  make logs [SERVICE=kong]             # tail logs for default stack (default kong)"
	@echo "  make logs-direct [SERVICE=q-assistant] # tail logs for direct stack (default q-assistant)"
	@echo "  make health                          # quick health check for q-assistant"
	@echo "  make fix-audit-perms                 # normalize q-assistant chat-audit ownership/permissions"
	@echo "  make clean-audit                     # remove q-assistant chat audit JSONL logs"
	@echo "  make export-audit [AUDIT_EXPORT_DIR=../test-results/assistant-live/chat-audit]"
	@echo "                                       # copy q-assistant chat audit JSONL logs from container to host"
	@echo
	@echo "q-assistant switch:"
	@echo "  make qa-switch PROVIDER=openrouter MODEL=google/gemini-3-flash-preview"
	@echo "  make qa-switch PROVIDER=ollama MODEL=qwen3-coder:30b BASE_URL=http://host.docker.internal:11434"
	@echo "  make qa-switch PROVIDER=openrouter MODEL=google/gemini-3-flash-preview KEEP_CHAIN=1"
	@echo "  make qa-switch PROVIDER=openrouter MODEL=google/gemini-3-flash-preview BUILD=1"
	@echo "  make qa-switch-dry PROVIDER=openrouter MODEL=google/gemini-3-flash-preview   # update .env only"
	@echo "  make qa-switch-openai MODEL=gpt-4o-mini"
	@echo "  make qa-switch-openrouter MODEL=google/gemini-3-flash-preview"
	@echo "  make qa-switch-openrouter-nemotron [MODEL=nvidia/llama-3.1-nemotron-70b-instruct]"
	@echo "  make qa-switch-openrouter-deepseek [MODEL=deepseek/deepseek-v3.2]"
	@echo "  make qa-switch-openrouter-gemini [MODEL=google/gemini-3-flash-preview]"
	@echo "  make qa-switch-ollama MODEL=qwen3-coder:30b [BASE_URL=http://host.docker.internal:11434]"
	@echo
	@echo "PostGIS data:"
	@echo "  make pgdump-sync                     # restore any new pgdump files into running PostGIS"
	@echo "  make clean-postgis                   # remove PostGIS data volume (full re-init on next up)"
	@echo
	@echo "Backend tests (in running containers):"
	@echo "  make test-q-cumber"
	@echo "  make test-q-assistant"
	@echo "  make test-q-storage"
	@echo "  make test-backends"
	@echo
	@echo "Image vars:"
	@echo "  IMAGE_REGISTRY=$(IMAGE_REGISTRY)"
	@echo "  IMAGE_NAMESPACE=$(IMAGE_NAMESPACE)"
	@echo "  IMAGE_TAG=$(IMAGE_TAG)"

doctor:
	@host_uid="$$(id -u)"; \
	host_gid="$$(id -g)"; \
	cfg_uid="$${QMAP_DOCKER_UID:-}"; \
	cfg_gid="$${QMAP_DOCKER_GID:-}"; \
	if [[ -f .env ]]; then \
		file_uid="$$(awk -F= '/^[[:space:]]*QMAP_DOCKER_UID[[:space:]]*=/{print $$2}' .env | tail -n1 | tr -d '[:space:]')"; \
		file_gid="$$(awk -F= '/^[[:space:]]*QMAP_DOCKER_GID[[:space:]]*=/{print $$2}' .env | tail -n1 | tr -d '[:space:]')"; \
		if [[ -n "$$file_uid" ]]; then cfg_uid="$$file_uid"; fi; \
		if [[ -n "$$file_gid" ]]; then cfg_gid="$$file_gid"; fi; \
	fi; \
	cfg_uid="$${cfg_uid:-1000}"; \
	cfg_gid="$${cfg_gid:-1000}"; \
	echo "[doctor] host uid:gid=$$host_uid:$$host_gid"; \
	echo "[doctor] compose uid:gid=$$cfg_uid:$$cfg_gid"; \
	if [[ "$$host_uid" != "$$cfg_uid" || "$$host_gid" != "$$cfg_gid" ]]; then \
		echo "[doctor] mismatch: set QMAP_DOCKER_UID=$$host_uid and QMAP_DOCKER_GID=$$host_gid in .env"; \
		if [[ "$(DOCTOR_STRICT)" != "0" ]]; then exit 2; fi; \
	fi; \
	echo "[doctor] ok"

init:
	@if [[ ! -f .env ]]; then cp .env.example .env; echo "Created .env from .env.example"; else echo ".env already exists"; fi

render-kong-config:
	$(PYTHON) kong/scripts/render-kong-config.py --output kong/kong.yml

registry-login:
	$(DOCKER) login $(IMAGE_REGISTRY)

image-refs:
	@echo "QMAP_IMAGE_QCUMBER_POSTGIS=$(QMAP_IMAGE_QCUMBER_POSTGIS)"
	@echo "QMAP_IMAGE_QCUMBER_BACKEND=$(QMAP_IMAGE_QCUMBER_BACKEND)"
	@echo "QMAP_IMAGE_QSTORAGE_BACKEND=$(QMAP_IMAGE_QSTORAGE_BACKEND)"
	@echo "QMAP_IMAGE_QASSISTANT=$(QMAP_IMAGE_QASSISTANT)"

image-build:
	$(DOCKER) build -t $(QMAP_IMAGE_QCUMBER_POSTGIS) q-cumber-postgis
	$(DOCKER) build -t $(QMAP_IMAGE_QCUMBER_BACKEND) -f q-cumber-backend/Dockerfile .
	$(DOCKER) build -t $(QMAP_IMAGE_QSTORAGE_BACKEND) -f q-storage-backend/Dockerfile .
	$(DOCKER) build -t $(QMAP_IMAGE_QASSISTANT) -f q-assistant/Dockerfile .

image-push: image-build
	$(DOCKER) push $(QMAP_IMAGE_QCUMBER_POSTGIS)
	$(DOCKER) push $(QMAP_IMAGE_QCUMBER_BACKEND)
	$(DOCKER) push $(QMAP_IMAGE_QSTORAGE_BACKEND)
	$(DOCKER) push $(QMAP_IMAGE_QASSISTANT)

up: doctor render-kong-config
	$(COMPOSE_GATEWAY) up -d q-cumber-backend q-storage-backend q-assistant kong

up-build: doctor render-kong-config
	$(COMPOSE_GATEWAY) up -d --build q-cumber-backend q-storage-backend q-assistant kong

restart: doctor render-kong-config
	$(COMPOSE_GATEWAY) restart q-cumber-backend q-storage-backend q-assistant kong

up-direct: doctor
	$(COMPOSE) up -d $(SERVICES)

up-direct-build: doctor
	$(COMPOSE) up -d --build $(SERVICES)

down:
	$(COMPOSE_GATEWAY) down

down-direct:
	$(COMPOSE) down

ps:
	$(COMPOSE_GATEWAY) ps

ps-direct:
	$(COMPOSE) ps

logs:
	$(COMPOSE_GATEWAY) logs -f $(if $(SERVICE),$(SERVICE),kong)

logs-direct:
	$(COMPOSE) logs -f $(if $(SERVICE),$(SERVICE),q-assistant)

health:
	curl -sS -m 5 http://localhost:3004/health

clean-audit:
	@$(MAKE) fix-audit-perms >/dev/null 2>&1 || true
	@if $(COMPOSE) ps --status running --services | grep -qx "q-assistant"; then \
		$(COMPOSE) exec -T q-assistant sh -lc 'audit_path="$${Q_ASSISTANT_CHAT_AUDIT_LOG_PATH:-/var/log/q-assistant/chat-audit}"; case "$$audit_path" in *.jsonl) audit_dir="$$(dirname "$$audit_path")/$$(basename "$$audit_path" .jsonl)" ;; *) audit_dir="$$audit_path" ;; esac; mkdir -p "$$audit_dir"; deleted="$$(find "$$audit_dir" -maxdepth 1 -type f -name "*.jsonl" | wc -l | tr -d " ")"; find "$$audit_dir" -maxdepth 1 -type f -name "*.jsonl" -delete; echo "Cleaned $$audit_dir/*.jsonl ($$deleted files)"'; \
	else \
		$(COMPOSE) run --rm --no-deps q-assistant sh -lc 'audit_path="$${Q_ASSISTANT_CHAT_AUDIT_LOG_PATH:-/var/log/q-assistant/chat-audit}"; case "$$audit_path" in *.jsonl) audit_dir="$$(dirname "$$audit_path")/$$(basename "$$audit_path" .jsonl)" ;; *) audit_dir="$$audit_path" ;; esac; mkdir -p "$$audit_dir"; deleted="$$(find "$$audit_dir" -maxdepth 1 -type f -name "*.jsonl" | wc -l | tr -d " ")"; find "$$audit_dir" -maxdepth 1 -type f -name "*.jsonl" -delete; echo "Cleaned $$audit_dir/*.jsonl ($$deleted files)"'; \
	fi

fix-audit-perms:
	@uid="$${QMAP_DOCKER_UID:-$$(id -u)}"; \
	gid="$${QMAP_DOCKER_GID:-$$(id -g)}"; \
	fix_cmd='audit_path="$${Q_ASSISTANT_CHAT_AUDIT_LOG_PATH:-/var/log/q-assistant/chat-audit}"; case "$$audit_path" in *.jsonl) audit_dir="$$(dirname "$$audit_path")/$$(basename "$$audit_path" .jsonl)" ;; *) audit_dir="$$audit_path" ;; esac; mkdir -p "$$audit_dir"; chown -R '"$$uid"':'"$$gid"' "$$audit_dir"; find "$$audit_dir" -type d -exec chmod 775 {} +; find "$$audit_dir" -type f -name "*.jsonl" -exec chmod 664 {} +'; \
	if $(COMPOSE) ps --status running --services | grep -qx "q-assistant"; then \
		$(COMPOSE) exec -T -u 0 q-assistant sh -lc "$$fix_cmd"; \
	else \
		$(COMPOSE) run --rm --no-deps -u 0 q-assistant sh -lc "$$fix_cmd"; \
	fi; \
	echo "Normalized chat-audit permissions to uid:gid=$$uid:$$gid"

export-audit:
	@mkdir -p "$(AUDIT_EXPORT_DIR)"; \
	tmp_tar="$$(mktemp)"; \
	export_cmd='audit_path="$${Q_ASSISTANT_CHAT_AUDIT_LOG_PATH:-/var/log/q-assistant/chat-audit}"; case "$$audit_path" in *.jsonl) audit_dir="$$(dirname "$$audit_path")/$$(basename "$$audit_path" .jsonl)" ;; *) audit_dir="$$audit_path" ;; esac; if ls "$$audit_dir"/session-*.jsonl >/dev/null 2>&1; then (cd "$$audit_dir" && tar -cf - session-*.jsonl); fi'; \
	if $(COMPOSE) ps --status running --services | grep -qx "q-assistant"; then \
		$(COMPOSE) exec -T q-assistant sh -lc "$$export_cmd" > "$$tmp_tar" 2>/dev/null || true; \
	else \
		$(COMPOSE) run --rm --no-deps q-assistant sh -lc "$$export_cmd" > "$$tmp_tar" 2>/dev/null || true; \
	fi; \
	if [[ -s "$$tmp_tar" ]]; then \
		tar -xf "$$tmp_tar" -C "$(AUDIT_EXPORT_DIR)"; \
		count="$$(find "$(AUDIT_EXPORT_DIR)" -maxdepth 1 -type f -name 'session-*.jsonl' | wc -l | tr -d ' ')"; \
		echo "Exported $$count chat-audit files to $(AUDIT_EXPORT_DIR)"; \
	else \
		echo "No chat-audit files exported (container path empty/unavailable)."; \
	fi; \
	rm -f "$$tmp_tar"

qa-switch:
	./switch-q-assistant.sh $(SWITCH_ARGS)

qa-switch-dry:
	./switch-q-assistant.sh $(SWITCH_ARGS) --no-restart

qa-switch-openai:
	$(MAKE) qa-switch PROVIDER=openai MODEL="$(if $(filter command\ line,$(origin MODEL)),$(MODEL),gpt-4o-mini)" BASE_URL="$(if $(BASE_URL),$(BASE_URL),https://api.openai.com/v1)"

qa-switch-openrouter:
	$(MAKE) qa-switch PROVIDER=openrouter MODEL="$(if $(filter command\ line,$(origin MODEL)),$(MODEL),google/gemini-3-flash-preview)" BASE_URL="$(if $(BASE_URL),$(BASE_URL),https://openrouter.ai/api/v1)"

qa-switch-openrouter-nemotron:
	$(MAKE) qa-switch PROVIDER=openrouter MODEL="$(if $(filter command\ line,$(origin MODEL)),$(MODEL),nvidia/llama-3.1-nemotron-70b-instruct)" BASE_URL="$(if $(BASE_URL),$(BASE_URL),https://openrouter.ai/api/v1)"

qa-switch-openrouter-deepseek:
	$(MAKE) qa-switch PROVIDER=openrouter MODEL="$(if $(filter command\ line,$(origin MODEL)),$(MODEL),deepseek/deepseek-v3.2)" BASE_URL="$(if $(BASE_URL),$(BASE_URL),https://openrouter.ai/api/v1)"

qa-switch-openrouter-gemini:
	$(MAKE) qa-switch PROVIDER=openrouter MODEL="$(if $(filter command\ line,$(origin MODEL)),$(MODEL),google/gemini-3-flash-preview)" BASE_URL="$(if $(BASE_URL),$(BASE_URL),https://openrouter.ai/api/v1)"

qa-switch-ollama:
	$(MAKE) qa-switch PROVIDER=ollama MODEL="$(if $(filter command\ line,$(origin MODEL)),$(MODEL),llama3.1)" BASE_URL="$(if $(BASE_URL),$(BASE_URL),http://host.docker.internal:11434)"

test-q-cumber:
	docker exec q-map-q-cumber-backend python -m unittest -q \
		tests/test_provider_registry_and_storage.py \
		tests/test_dataset_adapters.py \
		tests/test_api_routing_metadata.py \
		tests/test_jwt_auth.py

test-q-assistant:
	docker exec q-map-q-assistant python -m unittest -q \
		tests/test_explicit_tool_routing.py \
		tests/test_agent_skip_policy.py \
		tests/test_chat_audit_utils.py \
		tests/test_request_coercion.py \
		tests/test_chat_payload_compaction.py \
		tests/test_token_budget_compaction.py \
		tests/test_objective_anchor.py \
		tests/test_runtime_guardrails.py \
		tests/test_openai_stream_normalization.py \
		tests/test_openrouter_provider.py

test-q-storage:
	docker exec q-map-q-storage-backend python -m unittest -q \
		tests/test_config_and_storage.py \
		tests/test_jwt_auth.py

test-backends: test-q-cumber test-q-assistant test-q-storage

pgdump-sync:
	@echo "[pgdump-sync] Running incremental dump restore on q-cumber-postgis..."
	docker exec q-map-q-cumber-postgis /usr/local/bin/restore-dumps.sh
	@echo "[pgdump-sync] Restarting q-cumber-backend to pick up new tables..."
	$(COMPOSE) restart q-cumber-backend

clean-postgis:
	@echo "[clean-postgis] Stopping q-cumber-postgis and removing data volume..."
	$(COMPOSE) stop q-cumber-postgis
	rm -rf q-cumber-postgis/data
	@echo "[clean-postgis] Done. Run 'make up' or 'make up-direct' to reinitialize."
