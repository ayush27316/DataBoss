"""
GitHub operations using LangChain's GitHubToolkit.
https://python.langchain.com/docs/integrations/toolkits/github/

The toolkit covers: creating branches, committing files, opening PRs,
reading issues/comments, and more — no need to call the GitHub API directly.
"""

import os

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
        try:
            existing = repo.get_contents(path, ref=branch)
            repo.update_file(path, message, ddl, existing.sha, branch=branch)
            done = f"updated {path} on {branch}"
        except GithubException as e:
            if getattr(e, "status", None) == 404:
                repo.create_file(path, message, ddl, branch=branch)
                done = f"created {path} on {branch}"
            else:
                raise
        log.info("dev_schema artifact: %s", done)
        return done
    except Exception as e:
        msg = f"dev_schema push failed: {e}"
        log.warning(msg)
        return msg
