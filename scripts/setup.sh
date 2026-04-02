#!/usr/bin/env bash
# =============================================================================
# JDataBoss – Initial Railway deployment + GitHub secrets setup
#
# What this script does:
#   1. Installs Railway CLI if missing
#   2. Authenticates Railway + GitHub
#   3. Creates Railway project with one PostgreSQL service (shared DB)
#   4. Deploys the app service
#   5. Reads .env.local and pushes every variable to:
#        • Railway (app environment variables)
#        • GitHub Actions secrets
#
# Prerequisites:
#   - gh CLI authenticated  (gh auth login)
#   - Node.js 18+ OR curl (for Railway CLI install)
#   - A .env.local file filled out from .env.example
#
# Usage:
#   cp .env.example .env.local   # fill in your values
#   bash scripts/setup.sh
# =============================================================================
set -euo pipefail

# ── Colours ──────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
info()    { echo -e "${CYAN}[INFO]${NC}  $*"; }
success() { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
die()     { echo -e "${RED}[ERROR]${NC} $*" >&2; exit 1; }

# ── Config ────────────────────────────────────────────────────────────────────
ENV_FILE="${1:-.env.local}"
PROJECT_NAME="jdboss"

# ── 1. Check .env.local ───────────────────────────────────────────────────────
[[ -f "$ENV_FILE" ]] || die "$ENV_FILE not found. Copy .env.example → $ENV_FILE and fill it in."

# Parse key=value pairs into associative array
declare -A SECRETS
while IFS= read -r line; do
  [[ -z "$line" || "$line" =~ ^[[:space:]]*# ]] && continue
  key="${line%%=*}"
  val="${line#*=}"
  # Strip inline comments and trailing whitespace
  val="${val%%#*}"
  val="${val%"${val##*[![:space:]]}"}"
  SECRETS["$key"]="$val"
done < "$ENV_FILE"

# ── 2. Install Railway CLI ────────────────────────────────────────────────────
if ! command -v railway &>/dev/null; then
  info "Installing Railway CLI..."
  if command -v npm &>/dev/null; then
    npm install -g @railway/cli
  else
    curl -fsSL https://railway.com/install.sh | sh
    export PATH="$HOME/.railway/bin:$PATH"
  fi
  success "Railway CLI installed: $(railway --version)"
else
  success "Railway CLI already installed: $(railway --version)"
fi

# ── 3. Authenticate ───────────────────────────────────────────────────────────
info "Logging in to Railway (browser will open)..."
railway login

info "Checking GitHub CLI auth..."
gh auth status || die "Run 'gh auth login' first, then re-run this script."

# ── 4. Detect GitHub repo ─────────────────────────────────────────────────────
GITHUB_REPO="${SECRETS[GITHUB_REPOSITORY]:-}"
if [[ -z "$GITHUB_REPO" ]]; then
  GITHUB_REPO=$(gh repo view --json nameWithOwner -q .nameWithOwner 2>/dev/null) \
    || die "Could not detect GitHub repo. Set GITHUB_REPOSITORY=owner/repo in $ENV_FILE"
fi
success "GitHub repo: $GITHUB_REPO"

# ── 5. Create Railway project ─────────────────────────────────────────────────
info "Creating Railway project '$PROJECT_NAME'..."
railway init --name "$PROJECT_NAME"
success "Railway project created."

# Link the CLI to the project so subsequent commands are authorized
info "Linking CLI to project..."
railway link
success "Project linked."

# ── 6. Add single shared PostgreSQL ──────────────────────────────────────────
info "Adding PostgreSQL (shared DB for PROD_/DEV_ tables + staging_raw)..."
railway add --database postgres
success "Database created."

# ── 7. Fetch DB connection string from Railway ───────────────────────────────
info "Fetching DATABASE_URL from Railway..."
DATABASE_URL=$(railway variables --service jdboss-db --json 2>/dev/null | \
  python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('DATABASE_URL',''))" 2>/dev/null || echo "")

if [[ -z "$DATABASE_URL" ]]; then
  warn "Could not auto-fetch DATABASE_URL. Copy it from the Railway dashboard,"
  warn "add it to $ENV_FILE, then re-run: bash scripts/push_secrets.sh"
else
  SECRETS["DATABASE_URL"]="$DATABASE_URL"
  success "DATABASE_URL fetched."
fi

# ── 8. Deploy app service ─────────────────────────────────────────────────────
info "Deploying app to Railway (this may take a few minutes)..."
railway up --detach
success "Deployment triggered. Track progress: railway logs --tail"

# Grab the public URL for reference
RAILWAY_SERVER_URL=$(railway domain 2>/dev/null | grep https | head -1 || echo "")
if [[ -n "$RAILWAY_SERVER_URL" ]]; then
  success "App URL: $RAILWAY_SERVER_URL"
else
  warn "Could not detect app URL yet. Check the Railway dashboard after deploy."
fi

# ── 9. Push env vars to Railway ───────────────────────────────────────────────
info "Setting Railway environment variables..."

RAILWAY_VARS=""
for key in \
  GCP_PROJECT_ID GCS_BUCKET_NAME GCP_SA_JSON \
  DATABASE_URL \
  XAI_API_KEY XAI_MODEL XAI_BASE_URL \
  GITHUB_APP_ID GITHUB_APP_PRIVATE_KEY GITHUB_REPOSITORY GITHUB_BRANCH GITHUB_BASE_BRANCH \
  STAGING_THRESHOLD PR_MERGE_MODE \
  PUBSUB_VERIFICATION_TOKEN; do
  val="${SECRETS[$key]:-}"
  [[ -z "$val" ]] && { warn "Skipping Railway var $key (not set in $ENV_FILE)"; continue; }
  RAILWAY_VARS+=" $key=$(printf '%q' "$val")"
done

# shellcheck disable=SC2086
railway variables set $RAILWAY_VARS
success "Railway variables set."

# ── 10. Push secrets to GitHub Actions ────────────────────────────────────────
info "Setting GitHub Actions secrets for $GITHUB_REPO..."

GITHUB_SECRET_KEYS=(
  DATABASE_URL
  GCP_SA_JSON
  GCS_BUCKET_NAME
)

for key in "${GITHUB_SECRET_KEYS[@]}"; do
  val="${SECRETS[$key]:-}"
  if [[ -z "$val" ]]; then
    warn "Skipping GitHub secret $key (not set in $ENV_FILE)"
    continue
  fi
  printf '%s' "$val" | gh secret set "$key" --repo "$GITHUB_REPO"
  success "GitHub secret set: $key"
done

# ── 11. Summary ───────────────────────────────────────────────────────────────
echo ""
echo -e "${GREEN}════════════════════════════════════════════${NC}"
echo -e "${GREEN}  Setup complete!${NC}"
echo -e "${GREEN}════════════════════════════════════════════${NC}"
echo ""
echo "Next steps:"
echo "  1. Configure your GCS Pub/Sub push subscription to:"
echo "     ${RAILWAY_SERVER_URL:-<your-railway-domain>}/webhook/pubsub?token=<PUBSUB_VERIFICATION_TOKEN>"
echo ""
echo "  2. If DATABASE_URL was not auto-fetched, copy it from the Railway dashboard,"
echo "     add it to $ENV_FILE, and run: bash scripts/push_secrets.sh"
echo ""
echo "  3. Monitor deployment: railway logs --tail"
echo "  4. Railway dashboard: https://railway.app/dashboard"
