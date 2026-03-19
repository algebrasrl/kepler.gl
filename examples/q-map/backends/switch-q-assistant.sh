#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${SCRIPT_DIR}/.env"

usage() {
  cat <<'EOF'
Usage:
  switch-q-assistant.sh <provider> <model> [base_url] [--build] [--no-restart] [--keep-chain]

Examples:
  ./switch-q-assistant.sh openrouter google/gemini-3-flash-preview
  ./switch-q-assistant.sh openrouter deepseek/deepseek-v3.2
  ./switch-q-assistant.sh openrouter nvidia/llama-3.1-nemotron-70b-instruct
  ./switch-q-assistant.sh openai gpt-4o-mini https://api.openai.com/v1
  ./switch-q-assistant.sh ollama qwen3-coder:30b http://host.docker.internal:11434
  ./switch-q-assistant.sh openai gpt-4o-mini --build

Notes:
  - By default this script clears Q_ASSISTANT_AGENT_CHAIN so provider/model switch is deterministic.
  - Use --keep-chain if you want to keep the existing fallback chain.
EOF
}

default_base_url_for() {
  case "$1" in
    openai) echo "https://api.openai.com/v1" ;;
    openrouter) echo "https://openrouter.ai/api/v1" ;;
    ollama) echo "http://host.docker.internal:11434" ;;
    *)
      echo "Unsupported provider: $1" >&2
      exit 1
      ;;
  esac
}

set_env_value() {
  local key="$1"
  local value="$2"
  local tmp_file
  tmp_file="$(mktemp)"

  if [[ -f "${ENV_FILE}" ]]; then
    awk -v key="${key}" -v value="${value}" '
      BEGIN { updated = 0 }
      $0 ~ ("^" key "=") {
        print key "=" value
        updated = 1
        next
      }
      { print }
      END {
        if (updated == 0) {
          print key "=" value
        }
      }
    ' "${ENV_FILE}" > "${tmp_file}"
  else
    printf '%s=%s\n' "${key}" "${value}" > "${tmp_file}"
  fi

  mv "${tmp_file}" "${ENV_FILE}"
}

if [[ $# -eq 1 && ( "${1:-}" == "-h" || "${1:-}" == "--help" ) ]]; then
  usage
  exit 0
fi

if [[ $# -lt 2 ]]; then
  usage
  exit 1
fi

provider="$(echo "$1" | tr '[:upper:]' '[:lower:]')"
model="$2"
shift 2

base_url=""
restart_service=true
build_service=false
keep_chain=false

if [[ $# -gt 0 && "${1:-}" != --* ]]; then
  base_url="$1"
  shift
fi

while [[ $# -gt 0 ]]; do
  case "$1" in
    --no-restart)
      restart_service=false
      ;;
    --build)
      build_service=true
      ;;
    --no-build)
      build_service=false
      ;;
    --keep-chain)
      keep_chain=true
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
  shift
done

if [[ -z "${base_url}" ]]; then
  base_url="$(default_base_url_for "${provider}")"
fi

set_env_value "Q_ASSISTANT_PROVIDER" "${provider}"
set_env_value "Q_ASSISTANT_MODEL" "${model}"
set_env_value "Q_ASSISTANT_BASE_URL" "${base_url}"
if [[ "${keep_chain}" != "true" ]]; then
  set_env_value "Q_ASSISTANT_AGENT_CHAIN" ""
fi

echo "Updated ${ENV_FILE}:"
echo "  Q_ASSISTANT_PROVIDER=${provider}"
echo "  Q_ASSISTANT_MODEL=${model}"
echo "  Q_ASSISTANT_BASE_URL=${base_url}"
if [[ "${keep_chain}" != "true" ]]; then
  echo "  Q_ASSISTANT_AGENT_CHAIN=<cleared>"
fi

if [[ "${restart_service}" == "true" ]]; then
  if [[ "${build_service}" == "true" ]]; then
    (cd "${SCRIPT_DIR}" && docker compose up -d --build q-assistant)
  else
    (cd "${SCRIPT_DIR}" && docker compose up -d q-assistant)
  fi

  echo
  echo "Health check (waiting for q-assistant readiness):"
  health_url="http://localhost:3004/health"
  health_attempts=20
  health_sleep_seconds=1
  health_ok=false
  for ((i=1; i<=health_attempts; i++)); do
    if curl -sS -m 5 "${health_url}" >/dev/null 2>&1; then
      health_ok=true
      break
    fi
    sleep "${health_sleep_seconds}"
  done

  if [[ "${health_ok}" == "true" ]]; then
    curl -sS -m 5 "${health_url}"
    echo
  else
    echo "q-assistant did not become healthy within $((health_attempts * health_sleep_seconds))s" >&2
    exit 1
  fi
else
  echo "Restart skipped (--no-restart)."
fi
