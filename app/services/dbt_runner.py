"""
Run dbt commands against the single shared database.
profiles.yml is written at runtime — never hard-coded.
"""

import subprocess
import textwrap
from pathlib import Path
from urllib.parse import urlparse

from app.config import get_settings

settings = get_settings()

DBT_PROJECT_DIR = Path(__file__).resolve().parents[2] / "dbt"
PROFILES_PATH = DBT_PROJECT_DIR / "profiles.yml"


def _write_profiles() -> None:
    u = urlparse(settings.database_url)
    content = textwrap.dedent(f"""\
        jdboss:
          target: default
          outputs:
            default:
              type: postgres
              host: {u.hostname}
              port: {u.port or 5432}
              user: {u.username}
              password: {u.password}
              dbname: {u.path.lstrip('/')}
              schema: public
              threads: 4
    """)
    PROFILES_PATH.write_text(content)


def run_dbt(command: list[str]) -> tuple[int, str]:
    """
    Run `dbt <command>` against the shared DB.
    Returns (returncode, combined stdout+stderr).
    """
    _write_profiles()

    full_cmd = ["dbt"] + command + [
        "--project-dir", str(DBT_PROJECT_DIR),
        "--profiles-dir", str(DBT_PROJECT_DIR),
    ]

    result = subprocess.run(full_cmd, capture_output=True, text=True, cwd=str(DBT_PROJECT_DIR))
    return result.returncode, result.stdout + "\n" + result.stderr
