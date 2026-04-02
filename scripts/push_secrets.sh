#!/usr/bin/env bash
# =============================================================================
# Push secrets from .env.local → Railway variables + GitHub Actions secrets.
# Run this whenever you rotate a key or add a variable that was missing at setup.
#
# Usage:  bash scripts/push_secrets.sh [path/to/env/file]
# =============================================================================
set -euo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
info()    { echo -e "${CYAN}[INFO]${NC}  $*"; }
success() { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
die()     { echo -e "${RED}[ERROR]${NC} $*" >&2; exit 1; }

ENV_FILE="${1:-.env.local}"
[[ -f "$ENV_FILE" ]] || die "$ENV_FILE not found."

# Parse key=value pairs into associative array
declare -A SECRETS
while IFS= read -r line; do
  [[ -z "$line" || "$line" =~ ^[[:space:]]*# ]] && continue
  key="${line%%=*}"
  val="${line#*=}"
  # Strip inline comments (e.g. "value  # comment" → "value")
  val="${val%%#*}"
  # Trim trailing whitespace
  val="${val%"${val##*[![:space:]]}"}"
  SECRETS["$key"]="$val"
done < "$ENV_FILE"

GITHUB_REPO="${SECRETS[GITHUB_REPOSITORY]:-}"
if [[ -z "$GITHUB_REPO" ]]; then
  GITHUB_REPO=$(gh repo view --json nameWithOwner -q .nameWithOwner 2>/dev/null) \
    || die "Set GITHUB_REPOSITORY in $ENV_FILE"
fi

# ── Railway variables ─────────────────────────────────────────────────────────
RAILWAY_VAR_KEYS=(
  GCP_PROJECT_ID GCS_BUCKET_NAME GCP_SA_JSON
  DATABASE_URL
  XAI_API_KEY XAI_MODEL XAI_BASE_URL
  GITHUB_APP_ID GITHUB_APP_PRIVATE_KEY GITHUB_REPOSITORY GITHUB_BRANCH GITHUB_BASE_BRANCH
  STAGING_THRESHOLD PR_MERGE_MODE
  PUBSUB_VERIFICATION_TOKEN
)

info "Pushing Railway variables..."
for key in "${RAILWAY_VAR_KEYS[@]}"; do
  val="${SECRETS[$key]:-}"
  [[ -z "$val" ]] && { warn "Skipping Railway $key"; continue; }
  railway variables set "$key=$val"
  success "Railway: $key"
done

# ── GitHub Actions secrets ────────────────────────────────────────────────────
GITHUB_SECRET_KEYS=(
  DATABASE_URL
  GCP_SA_JSON GCS_BUCKET_NAME
)

info "Pushing GitHub Actions secrets → $GITHUB_REPO..."
for key in "${GITHUB_SECRET_KEYS[@]}"; do
  val="${SECRETS[$key]:-}"
  [[ -z "$val" ]] && { warn "Skipping GitHub $key"; continue; }
  printf '%s' "$val" | gh secret set "$key" --repo "$GITHUB_REPO"
  success "GitHub: $key"
done

echo ""
success "All secrets synced."
