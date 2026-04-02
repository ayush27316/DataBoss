from pydantic_settings import BaseSettings, SettingsConfigDict
from functools import lru_cache


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=(".env", ".env.local"), extra="ignore")

    # --- Google Cloud ---
    gcp_project_id: str = ""
    gcs_bucket_name: str = ""
    gcp_sa_json: str = ""

    # --- Database (single shared DB) ---
    database_url: str  # required — app cannot function without it

    # --- GitHub ---
    github_app_id: str = ""
    github_app_private_key: str = ""
    github_access_token: str = ""
    github_repository: str = ""
    github_branch: str = "agent/migration"
    github_base_branch: str = "main"

    # --- xAI Grok ---
    xai_api_key: str = ""
    xai_model: str = "grok-4-1-fast-non-reasoning"
    xai_base_url: str = "https://api.x.ai/v1"

    # --- Pipeline ---
    staging_threshold: int = 10
    pr_merge_mode: str = "manual"

    # --- Server ---
    pubsub_verification_token: str = ""
    port: int = 8000


@lru_cache
def get_settings() -> Settings:
    return Settings()
