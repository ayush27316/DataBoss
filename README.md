# JDataBoss

Local-first setup for testing the pipeline without deploying app or database to Railway.

## Local Testing Setup

1. Run local setup:
   - `bash scripts/setup_local.sh`
2. Fill local env:
   - Edit `.env.local`
   - Keep `DATABASE_URL=postgresql://jdboss:jdboss@localhost:5432/jdboss` for local Postgres.
3. Start backend:
   - `source .venv/bin/activate`
   - `uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload`
4. Verify:
   - `curl http://localhost:8000/health`

## GitHub Connection (PR creation still works)

To let the agent create branches and open PRs locally, configure one of:

- Personal access token (simplest for local):
  - Set `GITHUB_ACCESS_TOKEN` (token with `repo` access)
- GitHub App credentials:
  - Set both `GITHUB_APP_ID` and `GITHUB_APP_PRIVATE_KEY`

Also required:
- `GITHUB_REPOSITORY=owner/repo`
- `GITHUB_BRANCH=agent/migration` (or any working branch)
- `GITHUB_BASE_BRANCH=main`

The app will use whichever auth is provided and keep LangChain GitHubToolkit enabled for opening PRs.

## Local Postgres Operations

- Start DB: `docker compose -f docker-compose.local.yml up -d postgres`
- Stop DB: `docker compose -f docker-compose.local.yml down`
- Reset DB volume: `docker compose -f docker-compose.local.yml down -v`

## Notes

- `scripts/setup.sh` and `scripts/push_secrets.sh` are Railway-oriented workflows and can still be used for cloud deployment.
- For local iteration, prefer `.env.local` + `scripts/setup_local.sh`.
