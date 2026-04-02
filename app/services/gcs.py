import json
import tempfile
from pathlib import Path
from google.cloud import storage
from app.config import get_settings

settings = get_settings()


def _get_client() -> storage.Client:
    """
    Build a GCS client.
    - On Railway: set GCP_SA_JSON to the full service account JSON string.
    - Locally: set GOOGLE_APPLICATION_CREDENTIALS to the path of credentials.json.
    """
    if settings.gcp_sa_json:
        import google.oauth2.service_account as sa
        info = json.loads(settings.gcp_sa_json)
        credentials = sa.Credentials.from_service_account_info(info)
        return storage.Client(project=settings.gcp_project_id, credentials=credentials)
    return storage.Client.from_service_account_json("credentials.json")


def download_object(bucket_name: str, object_name: str) -> bytes:
    """Download a GCS object and return its raw bytes."""
    client = _get_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    return blob.download_as_bytes()


def delete_object(bucket_name: str, object_name: str) -> None:
    """Remove a processed object from GCS."""
    client = _get_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    blob.delete()
