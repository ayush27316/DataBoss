"""
GitHub operations using LangChain's GitHubToolkit.
https://python.langchain.com/docs/integrations/toolkits/github/

The toolkit covers: creating branches, committing files, opening PRs,
reading issues/comments, and more — no need to call the GitHub API directly.
"""

import os
from pathlib import Path
from typing import Mapping

from github import GithubException
from langchain_community.agent_toolkits.github.toolkit import GitHubToolkit
from langchain_community.utilities.github import GitHubAPIWrapper

from app.config import get_settings
from app.logging_config import get_logger

settings = get_settings()
log = get_logger("github")


def configure_github_environ() -> None:
    """Mirror Railway/local env vars expected by GitHubAPIWrapper."""
    os.environ.setdefault("GITHUB_REPOSITORY", settings.github_repository)
    os.environ.setdefault("GITHUB_BASE_BRANCH", settings.github_base_branch)
    os.environ.setdefault("GITHUB_BRANCH", settings.github_branch)

    if settings.github_access_token:
        os.environ.setdefault("GITHUB_ACCESS_TOKEN", settings.github_access_token)

    if settings.github_app_private_key:
        normalized_key = settings.github_app_private_key.replace("\\n", "\n")
        os.environ["GITHUB_APP_PRIVATE_KEY"] = normalized_key
    if settings.github_app_id:
        os.environ.setdefault("GITHUB_APP_ID", settings.github_app_id)


def github_api_wrapper() -> GitHubAPIWrapper:
    configure_github_environ()
    return GitHubAPIWrapper()


def _upsert_repo_file(repo, branch: str, path: str, content: str, message: str) -> str:
    """Create or update a single file on ``branch``. Returns 'created' or 'updated'."""
    try:
        existing = repo.get_contents(path, ref=branch)
        repo.update_file(path, message, content, existing.sha, branch=branch)
        return "updated"
    except GithubException as e:
        if getattr(e, "status", None) == 404:
            repo.create_file(path, message, content, branch=branch)
            return "created"
        raise


def get_github_toolkit() -> GitHubToolkit:
    """
    Returns a LangChain GitHubToolkit bound to the configured repo.
    The toolkit is passed directly to the LangChain agent as its tool list.

    Required env vars (set in Railway / .env):
      GITHUB_APP_ID, GITHUB_APP_PRIVATE_KEY, GITHUB_REPOSITORY,
      GITHUB_BRANCH (the working branch the agent will write to),
      GITHUB_BASE_BRANCH (the branch to PR into, i.e. "main")
    """
    return GitHubToolkit.from_github_api_wrapper(github_api_wrapper())


def sync_local_dbt_models_to_github(cycle_id: str, allowed_dirs: Mapping[str, Path]) -> str:
    """
    Push dbt model files from disk to ``agent/migration-{cycle_id}`` so the PR always
    includes staging / migrations / inject SQL (the agent often skips some paths).

    - ``staging/*.sql``: all files in the directory
    - ``migrations/*.sql``, ``inject/*.sql``: only files whose names contain ``cycle_id``
    """
    branch = f"agent/migration-{cycle_id}"

    try:
        wrapper = github_api_wrapper()
    except Exception as e:
        msg = f"github sync skipped (no client): {e}"
        log.warning(msg)
        return msg

    switched = wrapper.set_active_branch(branch)
    if switched.startswith("Error"):
        msg = f"github sync skipped — {switched}"
        log.warning(msg)
        return msg

    repo = wrapper.github_repo_instance
    synced = []
    for name, dir_path in allowed_dirs.items():
        path = Path(dir_path)
        if not path.is_dir():
            continue
        for filepath in sorted(path.glob("*.sql")):
            if name in ("migrations", "inject") and cycle_id not in filepath.name:
                continue
            rel = f"dbt/models/{name}/{filepath.name}"
            body = filepath.read_text(encoding="utf-8")
            action = _upsert_repo_file(
                repo,
                branch,
                rel,
                body,
                f"sync {rel} (cycle {cycle_id})",
            )
            synced.append(f"{action}:{rel}")
            log.info("github sync %s %s", action, rel)

    if not synced:
        msg = f"github dbt sync: no matching .sql files for cycle {cycle_id}"
        log.warning(msg)
        return msg
    return f"github dbt sync OK ({len(synced)} files): " + "; ".join(synced)


def push_dev_schema_artifact(cycle_id: str, ddl: str) -> str:
    """
    Create or update dbt/artifacts/dev_schema_{cycle_id}.sql on branch
    agent/migration-{cycle_id}. Best-effort: logs and returns on failure
    (e.g. branch not created yet if the inspector stopped early).
    """
    branch = f"agent/migration-{cycle_id}"
    path = f"dbt/artifacts/dev_schema_{cycle_id}.sql"
    if not ddl.strip():
        ddl = (
            f"-- dev_schema_{cycle_id}.sql — no public.dev_* tables at export time.\n"
        )

    try:
        wrapper = github_api_wrapper()
    except Exception as e:
        msg = f"github client unavailable: {e}"
        log.warning(msg)
        return msg

    switched = wrapper.set_active_branch(branch)
    if switched.startswith("Error"):
        msg = f"dev_schema push skipped — {switched}"
        log.warning(msg)
        return msg

    repo = wrapper.github_repo_instance
    message = f"Add dev schema DDL for cycle {cycle_id}"
    try:
        action = _upsert_repo_file(repo, branch, path, ddl, message)
        done = f"{action} {path} on {branch}"
        log.info("dev_schema artifact: %s", done)
        return done
    except Exception as e:
        msg = f"dev_schema push failed: {e}"
        log.warning(msg)
        return msg
