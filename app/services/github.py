"""
GitHub operations using LangChain's GitHubToolkit.
https://python.langchain.com/docs/integrations/toolkits/github/

The toolkit covers: creating branches, committing files, opening PRs,
reading issues/comments, and more — no need to call the GitHub API directly.
"""

from langchain_community.agent_toolkits.github.toolkit import GitHubToolkit
from langchain_community.utilities.github import GitHubAPIWrapper
from app.config import get_settings
import os

settings = get_settings()


def get_github_toolkit() -> GitHubToolkit:
    """
    Returns a LangChain GitHubToolkit bound to the configured repo.
    The toolkit is passed directly to the LangChain agent as its tool list.

    Required env vars (set in Railway / .env):
      GITHUB_APP_ID, GITHUB_APP_PRIVATE_KEY, GITHUB_REPOSITORY,
      GITHUB_BRANCH (the working branch the agent will write to),
      GITHUB_BASE_BRANCH (the branch to PR into, i.e. "main")
    """
    os.environ.setdefault("GITHUB_REPOSITORY", settings.github_repository)
    os.environ.setdefault("GITHUB_BASE_BRANCH", settings.github_base_branch)
    os.environ.setdefault("GITHUB_BRANCH", settings.github_branch)

    # Local-friendly auth: support either a GitHub App or a PAT.
    if settings.github_access_token:
        os.environ.setdefault("GITHUB_ACCESS_TOKEN", settings.github_access_token)

    # Always assign (not setdefault): Railway/hosting injects this var before Python runs;
    # setdefault would skip and leave literal "\\n" in PEM, breaking PyJWT.
    if settings.github_app_private_key:
        normalized_key = settings.github_app_private_key.replace("\\n", "\n")
        os.environ["GITHUB_APP_PRIVATE_KEY"] = normalized_key
    if settings.github_app_id:
        os.environ.setdefault("GITHUB_APP_ID", settings.github_app_id)

    wrapper = GitHubAPIWrapper()
    return GitHubToolkit.from_github_api_wrapper(wrapper)
