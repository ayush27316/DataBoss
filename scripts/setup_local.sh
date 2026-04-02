#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${1:-$ROOT_DIR/.env.local}"

echo "[INFO] Starting local JDataBoss setup"

if ! command -v docker >/dev/null 2>&1; then
  echo "[ERROR] docker is required." >&2
  exit 1
fi

if ! docker compose version >/dev/null 2>&1; then
  echo "[ERROR] docker compose plugin is required." >&2
  exit 1
fi

if [[ ! -f "$ENV_FILE" ]]; then
  cp "$ROOT_DIR/.env.example" "$ENV_FILE"
  echo "[INFO] Created $ENV_FILE from .env.example"
fi

echo "[INFO] Starting local Postgres + Adminer (docker-compose.local.yml)"
docker compose -f "$ROOT_DIR/docker-compose.local.yml" up -d postgres adminer

echo "[INFO] Waiting for Postgres healthcheck"
for _ in {1..30}; do
  status="$(docker inspect --format='{{.State.Health.Status}}' jdboss-postgres 2>/dev/null || true)"
  if [[ "$status" == "healthy" ]]; then
    echo "[OK] Postgres is healthy"
    break
  fi
  sleep 2
done

if [[ "${status:-}" != "healthy" ]]; then
  echo "[ERROR] Postgres did not become healthy in time." >&2
  exit 1
fi

if [[ ! -d "$ROOT_DIR/.venv" ]]; then
  echo "[INFO] Creating Python virtual environment"
  python3 -m venv "$ROOT_DIR/.venv"
fi

echo "[INFO] Installing Python dependencies"
"$ROOT_DIR/.venv/bin/pip" install --upgrade pip >/dev/null
"$ROOT_DIR/.venv/bin/pip" install -r "$ROOT_DIR/requirements.txt"

cat <<EOF

[OK] Local setup complete.

Next:
  1) Edit $ENV_FILE and set at least:
     - DATABASE_URL (default already points to local Postgres)
     - XAI_API_KEY
     - GITHUB_REPOSITORY
     - Either GITHUB_ACCESS_TOKEN OR (GITHUB_APP_ID + GITHUB_APP_PRIVATE_KEY)
  2) Start backend:
     source "$ROOT_DIR/.venv/bin/activate"
     uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
  3) Health check:
     curl http://localhost:8000/health

EOF
